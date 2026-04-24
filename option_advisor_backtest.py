"""期权策略信号方向性回测

思路:
  真期权 PnL 涉及 IV / theta / 行权价等非线性因素, 难精确模拟.
  更实用的代理: 验证分形信号对**底层股票未来方向**是否准确.
  如果信号方向准 (胜率 >55%), 期权才值得做.

流程:
  1. 对每只股票拉 400 天历史日线 (yfinance, 因为富途 request_history_kline 偶尔返回受限)
  2. 对每一天 t (从第 121 天开始), 用 t 之前 120 天算 MF-DFA + 技术指标
  3. 按 classify_regime 给出信号 (BUY_PUT / BUY_CALL / OBSERVE / ...)
  4. 看未来 N 天的收益率方向 (Put 对应 N=4, Call 对应 N=14)
  5. 统计胜率 / 平均收益 / 盈亏比 / 最大连续亏损

运行: python option_advisor_backtest.py [watchlist_name]
"""

import argparse
import numpy as np
import pandas as pd
from datetime import datetime

from fractal_survey import mfdfa_spectrum
from option_fractal_advisor import classify_regime


WATCHLISTS = {
    "tech": ["AAPL", "NVDA", "TSLA", "MSFT", "GOOGL", "META", "AMZN"],
    "default": ["NVDA", "TSLA", "AAPL"],
    "nvda_only": ["NVDA"],
    "btc": ["BTC-USD"],
    "crypto": ["BTC-USD", "ETH-USD"],
    "crypto_strong": ["ETH-USD", "XRP-USD", "DOT-USD"],  # 最佳带宽候选 asym 0.47-0.51
    "eth_only": ["ETH-USD"],
    "mix": ["BTC-USD", "NVDA", "TSLA", "META"],  # 强分形资产 vs 中等分形
}

WINDOW = 120  # MF-DFA 窗口
HOLD_PUT = 4  # Put 信号的检验周期
HOLD_CALL = 14  # Call 信号的检验周期


def compute_features_at_t(prices: pd.Series, t: int) -> dict:
    """在索引 t 处计算分形+技术特征 (仅用 t 之前的数据)."""
    if t < WINDOW:
        return None
    log_ret = np.log(prices / prices.shift(1))
    window_lr = log_ret.iloc[t - WINDOW + 1:t + 1]

    spec = mfdfa_spectrum(window_lr)
    if not spec:
        return None

    # 简单技术
    close_t = prices.iloc[t]
    ma20 = prices.iloc[t - 20 + 1:t + 1].mean() if t >= 20 else np.nan
    ma20_diff_pct = (close_t / ma20 - 1) * 100 if pd.notna(ma20) else 0

    # RSI6
    deltas = prices.diff()
    gain = deltas.where(deltas > 0, 0).rolling(6).mean()
    loss = (-deltas.where(deltas < 0, 0)).rolling(6).mean()
    rs = gain / loss
    rsi_series = 100 - 100 / (1 + rs)
    rsi6 = float(rsi_series.iloc[t]) if pd.notna(rsi_series.iloc[t]) else 50

    return {
        **spec,
        "rsi6": rsi6,
        "ma20_diff_pct": ma20_diff_pct,
    }


def backtest_symbol(symbol: str, days: int = 400) -> pd.DataFrame:
    """对单只股票回测. 返回每个信号点的 DataFrame."""
    import yfinance as yf
    try:
        t = yf.Ticker(symbol)
        hist = t.history(period=f"{max(days, 400)}d", interval="1d", auto_adjust=False)
        if hist is None or hist.empty:
            return pd.DataFrame()
        if hist.index.tz is not None:
            hist.index = hist.index.tz_localize(None)
    except Exception as e:
        print(f"  {symbol}: yfinance FAIL: {str(e)[:50]}")
        return pd.DataFrame()

    closes = hist["Close"].astype(float).reset_index(drop=True)
    dates = hist.index.to_series().reset_index(drop=True)

    results = []
    max_t = len(closes) - max(HOLD_PUT, HOLD_CALL) - 1
    for i in range(WINDOW, max_t):
        feat = compute_features_at_t(closes, i)
        if feat is None:
            continue
        regime = classify_regime(feat)
        strategy = regime["strategy"]
        if strategy == "OBSERVE":
            continue

        # 未来收益率
        close_t = closes.iloc[i]
        # 根据 strategy 选不同的持有期
        if strategy == "BUY_PUT":
            hold = HOLD_PUT
        elif strategy == "BUY_CALL":
            hold = HOLD_CALL
        elif strategy == "BUY_STRADDLE":
            hold = HOLD_CALL  # 跨式一般持有更久
        else:
            continue

        if i + hold >= len(closes):
            break
        fwd_ret = (closes.iloc[i + hold] / close_t - 1) * 100

        # 信号方向判定
        if strategy == "BUY_PUT":
            direction_correct = fwd_ret < 0
        elif strategy == "BUY_CALL":
            direction_correct = fwd_ret > 0
        elif strategy == "BUY_STRADDLE":
            direction_correct = abs(fwd_ret) > feat.get("delta_alpha", 0) * 5  # 跨式: 大幅度波动
        else:
            direction_correct = False

        results.append({
            "symbol": symbol,
            "date": dates.iloc[i].strftime("%Y-%m-%d"),
            "strategy": strategy,
            "regime": regime["regime"],
            "close": close_t,
            "fwd_ret_pct": fwd_ret,
            "hold_days": hold,
            "direction_correct": direction_correct,
            "asym": feat.get("asym"),
            "hq2": feat.get("hq2"),
            "rsi6": feat.get("rsi6"),
            "ma20_diff_pct": feat.get("ma20_diff_pct"),
        })
    return pd.DataFrame(results)


def report(all_df: pd.DataFrame) -> None:
    """打印回测汇总报告."""
    if all_df.empty:
        print("无信号.")
        return

    print()
    print("═" * 92)
    print(f"  分形期权策略 方向性回测 (总信号 n={len(all_df)})")
    print("═" * 92)

    # 1. 整体胜率
    print()
    print("  [整体]")
    total = len(all_df)
    wins = all_df["direction_correct"].sum()
    print(f"    信号数 {total}  正确 {wins}  胜率 {wins/total*100:.1f}%")
    print(f"    平均未来收益 {all_df['fwd_ret_pct'].mean():+.2f}%")

    # 2. 按策略分
    print()
    print("  [按策略]")
    by_strat = all_df.groupby("strategy").agg(
        n=("fwd_ret_pct", "size"),
        win_rate=("direction_correct", lambda x: x.mean() * 100),
        mean_fwd_ret=("fwd_ret_pct", "mean"),
        std_fwd_ret=("fwd_ret_pct", "std"),
        median_fwd_ret=("fwd_ret_pct", "median"),
    )
    print(by_strat.to_string(float_format=lambda x: f"{x:+.2f}"))

    # 3. 按股票分
    print()
    print("  [按股票]")
    by_sym = all_df.groupby("symbol").agg(
        n=("fwd_ret_pct", "size"),
        win_rate=("direction_correct", lambda x: x.mean() * 100),
        mean_fwd_ret=("fwd_ret_pct", "mean"),
    )
    print(by_sym.sort_values("win_rate", ascending=False).to_string(float_format=lambda x: f"{x:+.2f}"))

    # 4. 按情境分
    print()
    print("  [按情境]")
    by_reg = all_df.groupby("regime").agg(
        n=("fwd_ret_pct", "size"),
        win_rate=("direction_correct", lambda x: x.mean() * 100),
        mean_fwd_ret=("fwd_ret_pct", "mean"),
    )
    print(by_reg.sort_values("win_rate", ascending=False).to_string(float_format=lambda x: f"{x:+.2f}"))

    # 5. 期权简化 PnL 估算
    # 假设: Put 信号正确, 4 天内股票跌 1%, Put Delta -0.5 → 期权涨约 0.5% 名义金额 × 杠杆 ~10x
    # 粗略: 期权 PnL ≈ fwd_ret × delta × leverage (杠杆约 10x ATM)
    print()
    print("  [极简期权 PnL 估算 (ATM 4 天 Put Delta=-0.5, 杠杆 ~10x)]")
    # Put 信号: PnL_per_trade ≈ -fwd_ret × 5 (-因为 Put 反向, 5 = |Δ|×杠杆简化)
    # Call 信号: PnL_per_trade ≈ fwd_ret × 5
    put_mask = all_df["strategy"] == "BUY_PUT"
    call_mask = all_df["strategy"] == "BUY_CALL"
    put_pnl = -all_df.loc[put_mask, "fwd_ret_pct"] * 5
    call_pnl = all_df.loc[call_mask, "fwd_ret_pct"] * 5
    combined_pnl = pd.concat([put_pnl, call_pnl])
    if len(combined_pnl) > 0:
        wins = (combined_pnl > 0).sum()
        print(f"    总期权交易 {len(combined_pnl)}")
        print(f"    盈利交易 {wins}  胜率 {wins/len(combined_pnl)*100:.1f}%")
        print(f"    平均 PnL/笔 {combined_pnl.mean():+.2f}%")
        print(f"    中位数 PnL {combined_pnl.median():+.2f}%")
        gains = combined_pnl[combined_pnl > 0]
        losses = combined_pnl[combined_pnl < 0]
        if len(gains) > 0 and len(losses) > 0:
            pr = gains.mean() / abs(losses.mean())
            print(f"    盈亏比 {pr:.2f}")
    print()


def run(symbols: list[str]) -> pd.DataFrame:
    all_results = []
    print(f"[1/2] 拉 yfinance 历史 + 计算 MF-DFA 信号 (共 {len(symbols)} 只)")
    for sym in symbols:
        print(f"  {sym}...", end="", flush=True)
        df = backtest_symbol(sym, days=400)
        if not df.empty:
            all_results.append(df)
            sig_count = len(df)
            print(f" {sig_count} 个信号")
        else:
            print(" 无信号")
    combined = pd.concat(all_results, ignore_index=True) if all_results else pd.DataFrame()
    return combined


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backtest option-advisor direction signals on underlying prices")
    parser.add_argument("watchlist", nargs="?", default="default", choices=sorted(WATCHLISTS))
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    wl_name = args.watchlist
    symbols = WATCHLISTS[wl_name]

    df = run(symbols)
    report(df)

    out = f"advisor_backtest_{wl_name}_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
    df.to_csv(out, index=False)
    print(f"  详细信号: {out}\n")


if __name__ == "__main__":
    main()
