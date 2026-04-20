"""BTC 4 小时级分形分析 — 测试时间尺度假设

假设: BTC 日线级反转失败是因为时间尺度错误.
     h(-4)=0.988 的"趋势延续"可能是更短尺度 (4h-1d).
     在 4h K 线上重算 MF-DFA, 信号变"局部化"可能更准.

数据: yfinance BTC-USD 1h interval 聚合成 4h, 共 ~4340 条 4h K 线
时间窗口: 4h × 120 = 480 小时 = 20 天 (vs 日线 120 天)
持有期: 1-3 天 (vs 日线 4-14 天)

如果 4h 级回测胜率 >55% → 说明时间尺度猜对, 可部署
如果仍 <50% → 说明 BTC 根本不适用分形反转模型
"""

import sys
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import yfinance as yf

from fractal_survey import mfdfa_spectrum


def fetch_btc_4h(days: int = 730) -> pd.DataFrame:
    """拉 BTC 1h 历史聚合成 4h."""
    t = yf.Ticker("BTC-USD")
    # yfinance 1h interval 最多 730d
    hist = t.history(period=f"{min(days, 730)}d", interval="1h", auto_adjust=False)
    if hist.index.tz is not None:
        hist.index = hist.index.tz_localize(None)

    # 聚合到 4h (OHLCV)
    hist_4h = hist.resample("4h").agg({
        "Open": "first",
        "High": "max",
        "Low": "min",
        "Close": "last",
        "Volume": "sum",
    }).dropna()
    return hist_4h


def classify_4h_regime(feat: dict) -> dict:
    """4h 级策略 (同 option_fractal_advisor v2 风格, 阈值微调)."""
    asym = feat.get("asym", 0) or 0
    rsi = feat.get("rsi6", 50) or 50
    ma20 = feat.get("ma20_diff_pct", 0) or 0

    if asym > 0.3:
        if rsi < 40 or ma20 < -5:
            return {"strategy": "BUY_CALL", "regime": "oversold_bounce",
                    "hold_bars": 12}  # 12 × 4h = 2 天
        if rsi > 75 or ma20 > 8:
            return {"strategy": "BUY_PUT", "regime": "overbought_revert",
                    "hold_bars": 12}
    if asym < -0.1:
        if rsi > 60 and ma20 > 3:
            return {"strategy": "BUY_CALL", "regime": "trend_up",
                    "hold_bars": 18}  # 3 天
        if rsi < 40 and ma20 < -3:
            return {"strategy": "BUY_PUT", "regime": "trend_down",
                    "hold_bars": 18}
    return {"strategy": "WAIT", "regime": "no_signal", "hold_bars": 0}


def backtest_4h(days: int = 730, window: int = 120) -> pd.DataFrame:
    """4h 级回测."""
    print(f"[1/3] 拉 BTC-USD 1h × {days}天 聚合 4h...")
    hist = fetch_btc_4h(days=days)
    print(f"  {len(hist)} 根 4h K 线")

    closes = hist["Close"].astype(float).reset_index(drop=True)
    dates = hist.index.to_series().reset_index(drop=True)

    print(f"[2/3] 滚动 MF-DFA (窗口={window} 根 4h = {window*4/24:.0f}天)...")
    results = []
    step = 3  # 每 3 根 4h 采样一次 (减少冗余, 约 12h)
    for i in range(window, len(closes) - 18 - 1, step):
        past_closes = closes.iloc[:i + 1]
        log_ret = np.log(past_closes / past_closes.shift(1))
        window_lr = log_ret.iloc[-window:]
        if window_lr.isna().any() or (window_lr == 0).all():
            continue

        spec = mfdfa_spectrum(window_lr)
        if not spec:
            continue

        # 技术指标 (用 4h K 线)
        ma20 = past_closes.iloc[-20:].mean()
        ma20_diff = (past_closes.iloc[-1] / ma20 - 1) * 100 if pd.notna(ma20) else 0

        delta = past_closes.diff()
        gain = delta.where(delta > 0, 0).rolling(6).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(6).mean()
        rs = gain / loss
        rsi_series = 100 - 100 / (1 + rs)
        rsi6 = float(rsi_series.iloc[-1]) if pd.notna(rsi_series.iloc[-1]) else 50

        feat = {**spec, "rsi6": rsi6, "ma20_diff_pct": ma20_diff}
        regime = classify_4h_regime(feat)
        if regime["strategy"] == "WAIT":
            continue

        hold_bars = regime["hold_bars"]
        if i + hold_bars >= len(closes):
            break
        close_t = closes.iloc[i]
        close_end = closes.iloc[i + hold_bars]
        fwd_ret = (close_end / close_t - 1) * 100

        correct = (regime["strategy"] == "BUY_CALL" and fwd_ret > 0) or \
                  (regime["strategy"] == "BUY_PUT" and fwd_ret < 0)

        results.append({
            "timestamp": dates.iloc[i],
            "close": close_t,
            "strategy": regime["strategy"],
            "regime": regime["regime"],
            "hold_bars": hold_bars,
            "hold_hours": hold_bars * 4,
            "fwd_ret_pct": fwd_ret,
            "correct": correct,
            "asym": spec["asym"],
            "hq2": spec["hq2"],
            "rsi6": rsi6,
            "ma20_diff": ma20_diff,
        })

    df = pd.DataFrame(results)
    print(f"[3/3] 汇总 — {len(df)} 个信号")
    return df


def report(df: pd.DataFrame) -> None:
    if df.empty:
        print("无信号.")
        return
    print()
    print("═" * 80)
    print(f"  BTC 4h 级分形策略回测  n={len(df)}")
    print("═" * 80)
    print()

    total = len(df)
    wins = df["correct"].sum()
    print(f"  [整体]")
    print(f"    信号 {total}  正确 {wins}  胜率 {wins/total*100:.1f}%")
    print(f"    平均 4h-持有期收益 {df['fwd_ret_pct'].mean():+.2f}%")
    print()

    print(f"  [按情境+策略]")
    by = df.groupby(["regime", "strategy"]).agg(
        n=("fwd_ret_pct", "size"),
        win_rate=("correct", lambda x: x.mean() * 100),
        mean_ret=("fwd_ret_pct", "mean"),
        median_ret=("fwd_ret_pct", "median"),
        hold_h=("hold_hours", "first"),
    )
    print(by.to_string(float_format=lambda x: f"{x:+.2f}"))
    print()

    # 关键筛查
    strong = by[by["win_rate"] > 55]
    print(f"  [胜率 > 55% 情境]")
    if len(strong) > 0:
        print(strong.to_string(float_format=lambda x: f"{x:+.2f}"))
        print("  ✓ 4h 级别有可用信号")
    else:
        print("  ✗ 所有情境胜率 ≤55%")
        print("  ✗ 4h 时间尺度仍不 work")
    print()


if __name__ == "__main__":
    print(f"\n{'═' * 80}")
    print(f"  BTC 4 小时级分形回测  {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("═" * 80)

    df = backtest_4h(days=730, window=120)
    report(df)

    out = f"btc_4h_backtest_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
    df.to_csv(out, index=False)
    print(f"  详细: {out}\n")
