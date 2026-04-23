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
from macro_events import get_risk_warnings, get_vix_level
from position_sizing import recommend_long_option_position, recommend_straddle_position
from exit_rules import build_long_option_exit, build_straddle_exit, format_exit_plan
from trade_plan import build_trade_plan_meta


WATCHLISTS = {
    "tech": ["US.NVDA", "US.AAPL", "US.MSFT", "US.GOOGL", "US.TSLA", "US.META", "US.AMZN"],
    "tech_plus": ["US.NVDA", "US.AAPL", "US.MSFT", "US.GOOGL", "US.TSLA", "US.META",
                   "US.AMZN", "US.AMD", "US.NFLX", "US.CRM", "US.PLTR", "US.UBER",
                   "US.FUTU"],  # 富途控股 (元: 用富途API 研究富途股票)
    "etf": ["US.SPY", "US.QQQ", "US.IWM", "US.DIA", "US.VOO"],
    "crypto_etf": ["US.IBIT", "US.FBTC", "US.BITB"],  # BTC 现货 ETF
    "all_watch": ["US.NVDA", "US.AAPL", "US.MSFT", "US.GOOGL", "US.TSLA", "US.META",
                   "US.AMZN", "US.AMD", "US.NFLX", "US.CRM", "US.PLTR",
                   "US.SPY", "US.QQQ", "US.IWM",
                   "US.IBIT"],  # 含 BTC 敞口
    "nvda_only": ["US.NVDA"],
    "btc_only": ["US.IBIT"],  # 研究用, 回测失败, 不建议实盘
    "default": ["US.NVDA", "US.AAPL", "US.TSLA"],  # 不含 IBIT: BTC 策略回测失败
}

REFERENCE_ACCOUNT_EQUITY = 1_000_000

# IBIT/FBTC/BITB 作为 BTC 现货 ETF, 分形特征用 BTC-USD 计算更准确
# 因为 ETF 日线样本少(上市不足2年) 且持有量大时可能有轻微溢价
BTC_ETF_UNDERLYING = {
    "US.IBIT": "BTC-USD",
    "US.FBTC": "BTC-USD",
    "US.BITB": "BTC-USD",
}


def _plan_exit_token(exit_plan) -> str:
    return (
        f"TP1_{exit_plan.take_profit_partial:.2f}_"
        f"TP2_{exit_plan.take_profit_full:.2f}_"
        f"SL_{exit_plan.hard_stop:.2f}_"
        f"TSTOP_{exit_plan.time_stop_days}d"
    )


def analyze_underlying(code: str) -> dict:
    """对单只股票做分形 + 技术分析, 返回决策特征.

    对 BTC ETF (IBIT/FBTC/BITB): 分形用 BTC-USD (yfinance) 计算更准确,
    其他技术指标 + 实时价仍用 ETF 自身.
    """
    out = {"code": code}
    # 实时价 (始终来自富途 ETF 自己)
    rt = realtime_quotes([code])
    out["last_price"] = float(rt.iloc[0]["last_price"])
    out["chg_pct"] = float(rt.iloc[0]["change_rate"])

    # 决定分形数据源: BTC ETF → yfinance BTC-USD; 其他 → 富途 ETF 自己
    fractal_source_code = BTC_ETF_UNDERLYING.get(code)
    if fractal_source_code:
        # 用 BTC-USD 做分形 (样本长, 更稳定)
        import yfinance as yf
        out["_fractal_source"] = fractal_source_code
        try:
            t = yf.Ticker(fractal_source_code)
            btc_hist = t.history(period="400d", interval="1d", auto_adjust=False)
            if btc_hist.index.tz is not None:
                btc_hist.index = btc_hist.index.tz_localize(None)
            closes_for_fractal = btc_hist["Close"].astype(float)
        except Exception as e:
            out["fractal_err"] = str(e)[:40]
            closes_for_fractal = pd.Series(dtype=float)
    else:
        # 其他标的: 富途拉 K 线
        kl = get_kline(code, days=200, ktype="K_DAY")
        closes_for_fractal = kl["close"].astype(float)

    log_ret = np.log(closes_for_fractal / closes_for_fractal.shift(1)) if not closes_for_fractal.empty else pd.Series(dtype=float)

    # MF-DFA 谱
    if len(log_ret) >= 120:
        spec = mfdfa_spectrum(log_ret.iloc[-120:])
        out.update({"asym": spec.get("asym"), "hq2": spec.get("hq2"),
                    "delta_alpha": spec.get("delta_alpha"), "alpha0": spec.get("alpha0")})

    # 技术指标 用标的自身 (ETF 价格, 因为下单是对 ETF)
    if fractal_source_code:
        kl = get_kline(code, days=200, ktype="K_DAY")
        closes = kl["close"].astype(float)
    else:
        closes = closes_for_fractal

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


def apply_macro_override(regime: dict, symbol: str) -> dict:
    """根据宏观事件风险覆盖分形信号建议.

    规则:
      - VIX >= 30 恐慌市 → 所有买方都降级 WAIT
      - 财报 ≤ 3天 → 买跨式 禁止 (IV crush), Call/Put 买方降级 WAIT
      - 财报 4-7 天 → 买跨式 降级 WEAK (IV 已含event premium)
      - FOMC/CPI/NFP ≤ 3天 → 买方降级 (IV 高)
    """
    strategy = regime.get("strategy", "")
    warnings = get_risk_warnings(symbol, days_ahead=10)

    override = None
    reason_add = []
    is_buy_signal = strategy.startswith("BUY_")
    is_straddle = "STRADDLE" in strategy

    for w in warnings:
        if "🔴" in w:
            # 财报 ≤ 3天: 禁止所有买方 (跨式会被 IV crush, 单腿风险也极高)
            if "财报" in w and is_buy_signal:
                override = "WAIT_EARNINGS"
                reason_add.append("财报临近(≤3天), 避免期权买方 IV crush")
            # VIX 高: 期权贵, 买方降级
            if "VIX" in w and is_buy_signal:
                override = "WAIT_VIX_HIGH"
                reason_add.append("VIX 恐慌, 期权买方极贵")
            # FOMC/CPI/NFP ≤ 3天: 买方降级 (IV 已上升)
            if any(ev in w for ev in ("FOMC", "CPI", "NFP")) and is_buy_signal:
                override = "WAIT_EVENT"
                reason_add.append("宏观事件临近(≤3天), 期权 IV 偏贵")
        elif "🟡" in w:
            # 4-7 天: 跨式降级 (IV 已含事件 premium), 单腿可继续但标低 confidence
            if "财报" in w and is_straddle:
                override = "WAIT_EARNINGS_SOON"
                reason_add.append("财报4-7天内, 跨式 IV 已定价事件")

    if override:
        regime = dict(regime)
        regime["strategy_original"] = strategy
        regime["strategy"] = override
        regime["reason"] = regime.get("reason", "") + " | " + " + ".join(reason_add)
        regime["confidence"] = None
        regime["macro_warnings"] = warnings
    else:
        regime["macro_warnings"] = warnings
    return regime


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
            # ⭐ 当前最可信的单腿信号: 胜率 67.9% (回测验证, 仍需生产样本闭环)
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
    # 宏观风险 - 按紧急度排序 (🔴 最先显示, 最多 3 条)
    warnings = regime_info.get("macro_warnings", [])
    if warnings:
        def _prio(w: str) -> int:
            return 0 if "🔴" in w else (1 if "🟡" in w else 2)
        sorted_w = sorted(warnings, key=_prio)
        lines.append(f"  宏观:  {'; '.join(sorted_w[:3])}")

    if picks.empty:
        lines.append(f"  → 观望, 无期权建议")
    else:
        macro_penalty = 0
        if any("🔴" in w for w in warnings):
            macro_penalty = 12
        elif any("🟡" in w for w in warnings):
            macro_penalty = 4
        lines.append(f"")
        lines.append(f"  推荐期权合约:")
        for _, r in picks.iterrows():
            lines.append(
                f"    {r['option_type']:<4} {r['code']:<25} "
                f"strike=${r['strike_price']:<7.1f} premium=${r['last_price']:<6.2f} "
                f"Δ={r['delta']:+.3f} θ={r['theta']:+.3f} IV={r['iv']:.1f}% OI={r['open_interest']:,}"
            )
            if regime_info["strategy"] in ("BUY_CALL", "BUY_PUT"):
                pos = recommend_long_option_position(
                    premium=float(r["last_price"]),
                    account_equity=REFERENCE_ACCOUNT_EQUITY,
                    reliability="中" if conf == "HIGH" else "弱",
                    confidence=conf,
                    macro_penalty=macro_penalty,
                )
                exit_plan = build_long_option_exit(
                    premium=float(r["last_price"]),
                    days_to_expiry=regime_info["days_to_expiry"],
                    confidence=conf,
                )
                lines.append(
                    f"      仓位: {pos.position_tier} · 建议 {pos.qty} 张 · 风险预算 ${pos.risk_budget:,.0f} · "
                    f"名义资金 ${pos.notional_value:,.0f}"
                )
                lines.append(f"      退出: {format_exit_plan(exit_plan)}")
                lines.append(f"      说明: {pos.sizing_note}")
            elif regime_info["strategy"] == "BUY_STRADDLE":
                pos = recommend_straddle_position(
                    total_premium=float(r["last_price"]),
                    account_equity=REFERENCE_ACCOUNT_EQUITY,
                    confidence=conf,
                    macro_penalty=macro_penalty,
                )
                exit_plan = build_straddle_exit(
                    total_premium=float(r["last_price"]),
                    days_to_expiry=regime_info["days_to_expiry"],
                    confidence=conf,
                )
                lines.append(
                    f"      仓位: {pos.position_tier} · 建议 {pos.qty} 套 · 风险预算 ${pos.risk_budget:,.0f} · "
                    f"名义资金 ${pos.notional_value:,.0f}"
                )
                lines.append(f"      退出: {format_exit_plan(exit_plan)}")
                lines.append(f"      说明: {pos.sizing_note}")
        lines.append(f"")
        lines.append(f"  下单命令 (你本人执行, 不会自动执行):")
        for _, r in picks.iterrows():
            # 每单买 1 张, 约 $100-500 成本
            premium = float(r["last_price"])
            cost_per_contract = premium * 100
            if regime_info["strategy"] in ("BUY_CALL", "BUY_PUT"):
                pos = recommend_long_option_position(
                    premium=float(r["last_price"]),
                    account_equity=REFERENCE_ACCOUNT_EQUITY,
                    reliability="中" if conf == "HIGH" else "弱",
                    confidence=conf,
                    macro_penalty=macro_penalty,
                )
                exit_plan = build_long_option_exit(
                    premium=float(r["last_price"]),
                    days_to_expiry=regime_info["days_to_expiry"],
                    confidence=conf,
                )
            else:
                pos = recommend_straddle_position(
                    total_premium=float(r["last_price"]),
                    account_equity=REFERENCE_ACCOUNT_EQUITY,
                    confidence=conf,
                    macro_penalty=macro_penalty,
                )
                exit_plan = build_straddle_exit(
                    total_premium=float(r["last_price"]),
                    days_to_expiry=regime_info["days_to_expiry"],
                    confidence=conf,
                )
            plan_flags = build_trade_plan_meta(
                symbol=code,
                signal=regime_info["strategy"],
                plan_tier=pos.position_tier,
                plan_risk=pos.risk_budget,
                plan_exit=_plan_exit_token(exit_plan),
            ).to_flags()
            lines.append(
                f"    ./venv/bin/python trade_futu_sim.py buy {r['code']} 1 --confirm {plan_flags}  "
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
            # 应用宏观事件覆盖
            regime = apply_macro_override(regime, code)
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
