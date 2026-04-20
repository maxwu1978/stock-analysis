"""分形信号 → 期权策略推荐器

输入: 关注股票池
流程:
  1. 对每只股票拉 200 天历史, 算 MF-DFA 分形谱 + 常规技术
  2. 根据 asym / hq2 / RSI / MA20偏离 组合判断市场情境
  3. 映射到期权策略建议 (买 Call/Put / 跨式 / 空头)
  4. 查当前 ATM 期权链, 输出具体合约代号 + 下单命令行

重要: 此脚本仅生成建议和命令行. 不自动下单.
      所有买卖命令需由用户人工在终端执行:
        python trade_futu_sim.py buy <option_code> <qty> --confirm
"""

import sys
import numpy as np
import pandas as pd
from datetime import datetime

from fetch_futu import realtime_quotes, get_kline, find_atm_options, health_check
from fractal_survey import mfdfa_spectrum


WATCHLISTS = {
    "tech": ["US.NVDA", "US.AAPL", "US.MSFT", "US.GOOGL", "US.TSLA", "US.META", "US.AMZN"],
    "nvda_only": ["US.NVDA"],
    "default": ["US.NVDA", "US.AAPL", "US.TSLA"],
}


def analyze_underlying(code: str) -> dict:
    """对单只股票做分形 + 技术分析, 返回决策特征."""
    out = {"code": code}
    # 实时价
    rt = realtime_quotes([code])
    out["last_price"] = float(rt.iloc[0]["last_price"])
    out["chg_pct"] = float(rt.iloc[0]["change_rate"])

    # 拉 200 天历史
    kl = get_kline(code, days=200, ktype="K_DAY")
    closes = kl["close"].astype(float)
    log_ret = np.log(closes / closes.shift(1))

    # MF-DFA 谱
    if len(log_ret) >= 120:
        spec = mfdfa_spectrum(log_ret.iloc[-120:])
        out.update({"asym": spec.get("asym"), "hq2": spec.get("hq2"),
                    "delta_alpha": spec.get("delta_alpha"), "alpha0": spec.get("alpha0")})

    # 技术
    ma20 = closes.rolling(20).mean().iloc[-1]
    out["ma20_diff_pct"] = (closes.iloc[-1] / ma20 - 1) * 100 if pd.notna(ma20) else np.nan
    out["vol_20d_ann"] = log_ret.iloc[-20:].std() * np.sqrt(252) * 100

    # RSI6
    delta = closes.diff()
    gain = delta.where(delta > 0, 0).rolling(6).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(6).mean()
    rs = gain / loss
    out["rsi6"] = float((100 - 100 / (1 + rs)).iloc[-1])
    return out


def classify_regime(feat: dict) -> dict:
    """根据分形特征 + 技术指标分类市场情境, 给出期权策略建议.

    **v2 (基于 2026-04 回测修正)**:
      400天 × 7只科技股回测 n=545, 发现美股市场:
      - BUY_CALL @ strong_asym_oversold 胜率 67.9% (✓ 保留核心信号)
      - BUY_PUT @ strong_asym_overbought 胜率 47.9% (✗ 美股长期牛市 反转不成立)
      - trend_continuation 样本少且胜率低 (<35%)

    决策逻辑 (保守版):
      - asym > 0.3 + RSI<40 超卖 → BUY_CALL (反转买入, 胜率 67%) ⭐
      - asym > 0.3 + RSI>70 超买 → WAIT (不做空) 或降低 confidence
      - asym < -0.1 + RSI>60 强势 → BUY_CALL 延续 (样本少, 低 confidence)
      - |asym|弱 + Δα>0.6 → BUY_STRADDLE (波动放大)
      - 其他 → 观望
    """
    asym = feat.get("asym", 0) or 0
    rsi = feat.get("rsi6", 50) or 50
    ma20 = feat.get("ma20_diff_pct", 0) or 0
    delta_a = feat.get("delta_alpha", 0) or 0

    regime = None
    strategy = None
    days_to_expiry = 7
    confidence = None  # 基于回测的信号置信度

    if asym > 0.3:
        # 强反转股
        if rsi < 40 or ma20 < -6:
            # ⭐ 核心强信号: 胜率 67.9% (回测验证)
            regime = "strong_asym_oversold"
            strategy = "BUY_CALL"
            days_to_expiry = 14  # Call 持有期更长, 给反转空间
            confidence = "HIGH"
        elif rsi > 75 or ma20 > 10:
            # 超买反转信号 - 回测胜率仅 48%, 不推荐做空
            regime = "strong_asym_overbought_LOW_CONFIDENCE"
            strategy = "WAIT"  # 原为 BUY_PUT, 回测证伪后改为 WAIT
            confidence = "LOW"
        else:
            regime = "strong_asym_neutral"
            strategy = "OBSERVE"
    elif asym < -0.1:
        # 趋势股
        if rsi > 60 and ma20 > 3:
            regime = "trend_continuation_up"
            strategy = "BUY_CALL"
            days_to_expiry = 14
            confidence = "LOW"  # 回测样本少 (n=3, 胜率 33%)
        elif rsi < 40 and ma20 < -3:
            regime = "trend_continuation_down_LOW_CONFIDENCE"
            strategy = "WAIT"  # 回测 n=8 胜率 25%, 信号方向反
            confidence = "LOW"
        else:
            regime = "trend_neutral"
            strategy = "OBSERVE"
    else:
        # 弱分形
        if delta_a > 0.6:
            regime = "weak_asym_high_vol_complexity"
            strategy = "BUY_STRADDLE"
            days_to_expiry = 21
            confidence = "MEDIUM"
        else:
            regime = "weak_asym_low_signal"
            strategy = "OBSERVE"

    return {"regime": regime, "strategy": strategy, "days_to_expiry": days_to_expiry,
            "confidence": confidence,
            "asym": asym, "rsi6": rsi, "ma20_diff_pct": ma20, "delta_alpha": delta_a}


def pick_option_contract(underlying: str, strategy: str, days: int) -> pd.DataFrame:
    """根据策略选择推荐的期权合约."""
    if strategy in ("OBSERVE", "WAIT"):
        return pd.DataFrame()

    atm = find_atm_options(underlying, days_to_expiry=days, strike_band=0.03)
    if atm.empty:
        return pd.DataFrame()

    # 筛选对应方向
    if strategy == "BUY_CALL":
        picks = atm[atm["option_type"] == "CALL"].copy()
    elif strategy == "BUY_PUT":
        picks = atm[atm["option_type"] == "PUT"].copy()
    elif strategy == "BUY_STRADDLE":
        picks = atm.copy()  # Call + Put 都要
    else:
        return pd.DataFrame()

    # 推荐最接近 ATM 的合约 (moneyness 最接近 0 ± 1%)
    picks["abs_moneyness"] = picks["moneyness_pct"].abs()
    picks = picks.sort_values(["option_type", "abs_moneyness"]).groupby("option_type").head(1)
    return picks.reset_index(drop=True)


def format_recommendation(feat: dict, regime_info: dict, picks: pd.DataFrame) -> str:
    """打印建议 + 下单命令行."""
    code = feat["code"]
    lines = []
    lines.append(f"\n{'─' * 80}")
    lines.append(f"  {code}   现价=${feat['last_price']:.2f}   今日 {feat['chg_pct']:+.2f}%")
    lines.append(f"  分形:  asym={feat.get('asym', 0):+.3f}  h(q=2)={feat.get('hq2', 0):+.3f}  Δα={feat.get('delta_alpha', 0):.3f}")
    lines.append(f"  技术:  RSI6={feat['rsi6']:.1f}  MA20偏离={feat['ma20_diff_pct']:+.2f}%  年化σ={feat['vol_20d_ann']:.1f}%")
    lines.append(f"  情境:  {regime_info['regime']}")
    conf = regime_info.get("confidence")
    conf_str = f"  置信度: {conf}" if conf else ""
    lines.append(f"  建议:  {regime_info['strategy']}   到期天数目标={regime_info['days_to_expiry']}{conf_str}")

    if picks.empty:
        lines.append(f"  → 观望, 无期权建议")
    else:
        lines.append(f"")
        lines.append(f"  推荐期权合约:")
        for _, r in picks.iterrows():
            lines.append(
                f"    {r['option_type']:<4} {r['code']:<25} "
                f"strike=${r['strike_price']:<7.1f} premium=${r['last_price']:<6.2f} "
                f"Δ={r['delta']:+.3f} θ={r['theta']:+.3f} IV={r['iv']:.1f}% OI={r['open_interest']:,}"
            )
        lines.append(f"")
        lines.append(f"  下单命令 (你本人执行, 不会自动执行):")
        for _, r in picks.iterrows():
            # 每单买 1 张, 约 $100-500 成本
            premium = float(r["last_price"])
            cost_per_contract = premium * 100
            lines.append(
                f"    ./venv/bin/python trade_futu_sim.py buy {r['code']} 1 --confirm  "
                f"# 成本约 ${cost_per_contract:.0f}/张"
            )

    return "\n".join(lines)


def run(watchlist: list[str]) -> None:
    hc = health_check()
    if not hc.get("qot_logined"):
        print("Futu OpenD 未登录")
        return

    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"\n{'═' * 80}")
    print(f"  期权策略推荐  {ts}   US 市场: {hc.get('market_us', '?')}")
    print("═" * 80)

    for code in watchlist:
        try:
            feat = analyze_underlying(code)
            regime = classify_regime(feat)
            picks = pick_option_contract(code, regime["strategy"], regime["days_to_expiry"])
            print(format_recommendation(feat, regime, picks))
        except Exception as e:
            print(f"\n  {code}: 错误 {str(e)[:80]}")

    print(f"\n{'═' * 80}")
    print("  ⚠ 以上仅为分形信号衍生的策略建议, 不构成交易推荐")
    print("  ⚠ 期权有时间衰减 / 流动性 / IV 风险, 实盘前建议先模拟盘验证")
    print("═" * 80 + "\n")


if __name__ == "__main__":
    wl_name = sys.argv[1] if len(sys.argv) > 1 else "default"
    if wl_name not in WATCHLISTS:
        print(f"可用关注池: {list(WATCHLISTS)}")
        sys.exit(1)
    print(f"使用关注池: {wl_name}")
    run(WATCHLISTS[wl_name])
