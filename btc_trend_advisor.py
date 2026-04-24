"""BTC 条件策略 — 研究代码, 回测证明不可靠 ⚠️

**警告**: 本策略设计基于 BTC 分形理论, 但 400 天历史回测结果如下:
  总信号 32  胜率 37.5%  平均收益 -2.39%
  low_vol_uptrend (趋势跟踪做多):      22 次 胜率 40.9%  -1.45%  ❌
  high_vol_overextended_down (反弹):   8 次 胜率 25.0%  -5.72%  ❌
  **所有情境胜率都远低于 50%**, 策略设计失败.

可能失败原因 (供未来优化参考):
  1. 分形 h(-4)=0.988 的"趋势延续"时间尺度可能是几小时到 1 天,
     不是本策略假设的 14 天持有期
  2. BTC 24/7 交易, 日线压缩 vs 股票 6.5 小时/天 特征不同
  3. 回测期 BTC 震荡幅度极大 ($60k-$126k), 趋势策略被打爆

**请勿基于此策略实盘下单**. 保留代码仅作研究/教学用途.

未来优化方向:
  - 改用日内 4h/1h K 线 + 更短持有期 (1-3 天)
  - 替换 MA20/ROC20 为 布林带/Keltner/ATR 等
  - 考虑 on-chain 指标 (如 MVRV, NVT) 补充分形信号
  - 探索 BTC 的分形时间尺度 (自适应窗口)

**原始设计 (失败)**:
"""

import argparse
import numpy as np
import pandas as pd
import yfinance as yf
from datetime import datetime

from fractal_survey import mfdfa_spectrum
from fetch_futu import realtime_quotes, get_kline, find_atm_options, health_check


UNDERLYING_YF = "BTC-USD"    # 分形 + 历史数据源
UNDERLYING_OPT = "US.IBIT"   # 实盘交易标的 (富途期权)


def compute_features(closes: pd.Series) -> dict:
    """计算 BTC 策略特征."""
    log_ret = np.log(closes / closes.shift(1))

    # MF-DFA (最新 120 日)
    spec = mfdfa_spectrum(log_ret.iloc[-120:]) if len(log_ret) >= 120 else {}

    # 当前波动率 (20日年化)
    vol_20d = log_ret.iloc[-20:].std() * np.sqrt(365) * 100

    # 过去 252 天的 20日滚动波动率, 算分位
    vol_series = log_ret.rolling(20).std() * np.sqrt(365) * 100
    vol_252 = vol_series.dropna().tail(252)
    vol_rank = (vol_252 < vol_20d).mean() * 100 if len(vol_252) >= 30 else np.nan

    # 技术: MA20, ROC20, ROC5
    ma20 = closes.rolling(20).mean().iloc[-1]
    ma20_diff_pct = (closes.iloc[-1] / ma20 - 1) * 100 if pd.notna(ma20) else 0

    roc20 = (closes.iloc[-1] / closes.iloc[-21] - 1) * 100 if len(closes) > 21 else 0
    roc5 = (closes.iloc[-1] / closes.iloc[-6] - 1) * 100 if len(closes) > 6 else 0

    return {
        **spec,
        "close": float(closes.iloc[-1]),
        "vol_20d": vol_20d,
        "vol_rank": vol_rank,
        "ma20_diff_pct": ma20_diff_pct,
        "roc20": roc20,
        "roc5": roc5,
    }


def classify_btc_regime(feat: dict) -> dict:
    """BTC 条件策略分类.

    优先级:
    1. 高波动 + 急涨急跌 → 均值回归
    2. 低波动 + 方向明确 → 趋势跟踪
    3. 中波动/方向不清 → WAIT
    """
    vol_rank = feat.get("vol_rank")
    if vol_rank is None or pd.isna(vol_rank):
        return {"strategy": "WAIT", "regime": "insufficient_history", "confidence": None,
                "reason": "历史数据不足"}

    roc5 = feat.get("roc5", 0)
    roc20 = feat.get("roc20", 0)
    ma20_diff = feat.get("ma20_diff_pct", 0)
    vol_20d = feat.get("vol_20d", 0)

    # 高波动: 均值回归模式
    if vol_rank > 70:
        if roc5 > 10:
            return {
                "strategy": "BUY_PUT", "regime": "high_vol_overextended_up",
                "confidence": "MEDIUM", "days_to_expiry": 7,
                "reason": f"高波动({vol_rank:.0f}%分位)+5日急涨{roc5:+.1f}% → 回归下跌",
            }
        if roc5 < -10:
            return {
                "strategy": "BUY_CALL", "regime": "high_vol_overextended_down",
                "confidence": "MEDIUM", "days_to_expiry": 7,
                "reason": f"高波动({vol_rank:.0f}%分位)+5日急跌{roc5:+.1f}% → 反弹上涨",
            }
        return {"strategy": "WAIT", "regime": "high_vol_no_extreme",
                "confidence": None, "reason": f"高波动但5日变化{roc5:+.1f}%不极端, 等急涨急跌"}

    # 低波动: 趋势延续模式
    if vol_rank < 30:
        if ma20_diff > 3 and roc20 > 5:
            return {
                "strategy": "BUY_CALL", "regime": "low_vol_uptrend",
                "confidence": "MEDIUM", "days_to_expiry": 14,
                "reason": f"低波动({vol_rank:.0f}%分位)+MA20上{ma20_diff:+.1f}%+20日{roc20:+.1f}% → 趋势延续",
            }
        if ma20_diff < -3 and roc20 < -5:
            return {
                "strategy": "BUY_PUT", "regime": "low_vol_downtrend",
                "confidence": "MEDIUM", "days_to_expiry": 14,
                "reason": f"低波动({vol_rank:.0f}%分位)+MA20下{ma20_diff:+.1f}%+20日{roc20:+.1f}% → 趋势延续",
            }
        return {"strategy": "WAIT", "regime": "low_vol_no_direction",
                "confidence": None, "reason": f"低波动但方向不明 MA20{ma20_diff:+.1f}%/ROC20{roc20:+.1f}%"}

    # 中波动
    return {"strategy": "WAIT", "regime": "mid_vol_no_signal",
            "confidence": None, "reason": f"中波动({vol_rank:.0f}%分位), 等波动率到极端"}


def run_live() -> None:
    """实时跑 BTC 策略推荐."""
    print(f"\n{'═' * 72}")
    print(f"  BTC 条件趋势/回归策略  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("═" * 72)

    # 拉 BTC-USD 历史 (400天 给波动率分位足够样本)
    print("\n[1/3] 拉 BTC-USD 400 天历史...")
    t = yf.Ticker(UNDERLYING_YF)
    hist = t.history(period="400d", interval="1d", auto_adjust=False)
    if hist.index.tz is not None:
        hist.index = hist.index.tz_localize(None)
    closes = hist["Close"].astype(float)

    print(f"[2/3] 计算特征 + 分类...")
    feat = compute_features(closes)
    regime = classify_btc_regime(feat)

    print("\n  [BTC 当前状态]")
    print(f"    BTC-USD 现价:      ${feat['close']:,.0f}")
    print(f"    20日年化波动率:    {feat['vol_20d']:.1f}%")
    print(f"    波动率252日分位:   {feat['vol_rank']:.0f}%  "
          f"({'低' if feat['vol_rank']<30 else ('高' if feat['vol_rank']>70 else '中')})")
    print(f"    5日涨跌:           {feat['roc5']:+.2f}%")
    print(f"    20日涨跌:          {feat['roc20']:+.2f}%")
    print(f"    MA20 偏离:         {feat['ma20_diff_pct']:+.2f}%")
    print(f"    分形 asym:         {feat.get('asym', 0):+.3f}")
    print(f"    分形 Δα:           {feat.get('delta_alpha', 0):.3f}")

    print(f"\n  [策略判定]")
    print(f"    情境:     {regime['regime']}")
    print(f"    策略:     {regime['strategy']}")
    print(f"    置信度:   {regime.get('confidence', '-')}")
    print(f"    理由:     {regime['reason']}")

    if regime["strategy"] in ("WAIT",):
        print(f"\n  → 无操作建议")
        return

    # 找 IBIT 期权
    print(f"\n[3/3] 查找 IBIT 期权 (到期目标 {regime.get('days_to_expiry')} 天)...")
    days = regime.get("days_to_expiry", 14)
    atm = find_atm_options(UNDERLYING_OPT, days_to_expiry=days, strike_band=0.03)
    if atm.empty:
        print("  IBIT ATM 链为空, 无法推荐具体合约")
        return

    want_type = "CALL" if regime["strategy"] == "BUY_CALL" else "PUT"
    picks = atm[atm["option_type"] == want_type].copy()
    if picks.empty:
        print(f"  无 {want_type} 合约")
        return
    picks["abs_money"] = picks["moneyness_pct"].abs()
    atm_pick = picks.nsmallest(1, "abs_money").iloc[0]

    print(f"\n  [推荐合约]")
    print(f"    {atm_pick['code']}  {want_type} ${atm_pick['strike_price']:.2f}")
    print(f"    剩余天数: {int(atm_pick['days_to_expiry'])}")
    print(f"    权利金:   ${atm_pick['last_price']:.2f} (总成本 ${atm_pick['last_price']*100:.0f}/张)")
    print(f"    Δ={atm_pick['delta']:+.3f}  θ={atm_pick['theta']:+.3f}  IV={atm_pick['iv']:.1f}%  OI={int(atm_pick['open_interest']):,}")

    print(f"\n  [下单命令 (你本人执行)]")
    print(f"    ./venv/bin/python trade_futu_sim.py buy {atm_pick['code']} 1 --confirm")
    print()


def backtest(days: int = 400, lookback_min: int = 60) -> None:
    """历史回测 BTC 策略方向性."""
    print(f"\n{'═' * 72}")
    print(f"  BTC 条件策略 历史回测 (过去 {days} 天)")
    print("═" * 72)

    t = yf.Ticker(UNDERLYING_YF)
    hist = t.history(period=f"{days}d", interval="1d", auto_adjust=False)
    if hist.index.tz is not None:
        hist.index = hist.index.tz_localize(None)
    closes = hist["Close"].astype(float).reset_index(drop=True)
    dates = hist.index.to_series().reset_index(drop=True)

    results = []
    max_hold = 14
    for i in range(lookback_min, len(closes) - max_hold - 1):
        past_closes = closes.iloc[:i + 1]
        feat = compute_features(past_closes)
        regime = classify_btc_regime(feat)
        strategy = regime["strategy"]
        if strategy == "WAIT":
            continue

        hold = regime.get("days_to_expiry", 14)
        close_t = closes.iloc[i]
        if i + hold >= len(closes):
            continue
        close_end = closes.iloc[i + hold]
        fwd_ret = (close_end / close_t - 1) * 100

        correct = (strategy == "BUY_CALL" and fwd_ret > 0) or (strategy == "BUY_PUT" and fwd_ret < 0)

        results.append({
            "date": dates.iloc[i].strftime("%Y-%m-%d"),
            "close": close_t,
            "strategy": strategy,
            "regime": regime["regime"],
            "hold_days": hold,
            "fwd_ret_pct": fwd_ret,
            "correct": correct,
            "vol_rank": feat.get("vol_rank"),
            "roc5": feat.get("roc5"),
            "roc20": feat.get("roc20"),
            "ma20_diff": feat.get("ma20_diff_pct"),
        })

    df = pd.DataFrame(results)
    if df.empty:
        print("  无触发信号")
        return

    print(f"\n  总信号 {len(df)}  胜率 {df['correct'].sum()/len(df)*100:.1f}%  平均收益 {df['fwd_ret_pct'].mean():+.2f}%")
    print("\n  [按情境]")
    by_reg = df.groupby(["regime", "strategy"]).agg(
        n=("fwd_ret_pct", "size"),
        win_rate=("correct", lambda x: x.mean() * 100),
        mean_ret=("fwd_ret_pct", "mean"),
        median_ret=("fwd_ret_pct", "median"),
    )
    print(by_reg.to_string(float_format=lambda x: f"{x:+.2f}"))

    # 胜率 > 55% 的情境视为有效
    print("\n  [关键情境筛查]")
    strong = by_reg[by_reg["win_rate"] > 55]
    if len(strong) > 0:
        print("    ✓ 有效策略 (胜率 >55%):")
        print(strong.to_string(float_format=lambda x: f"{x:+.2f}"))
    else:
        print("    ✗ 所有情境胜率 ≤55%, 无可靠信号")

    # 保存
    out = f"btc_backtest_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
    df.to_csv(out, index=False)
    print(f"\n  详细: {out}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Research-only BTC trend/mean-reversion advisor")
    parser.add_argument("mode", nargs="?", default="live", choices=["live", "backtest"])
    parser.add_argument("--days", type=int, default=400)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.mode == "backtest":
        backtest(days=args.days)
    else:
        run_live()


if __name__ == "__main__":
    main()
