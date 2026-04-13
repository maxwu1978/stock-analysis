#!/usr/bin/env python3
"""美股因子有效性分析"""

import time
import numpy as np
import pandas as pd
from fetch_us import US_STOCKS, fetch_us_history, fetch_us_financials
from indicators import compute_all

FACTORS = [
    "RSI6", "RSI12",
    "DIF", "DEA", "MACD",
    "ADX", "+DI", "-DI",
    "ROC5", "ROC10", "ROC20",
    "autocorr",
    "vol_price_div",
    "price_position",
    "BOLL_pos", "vol_ratio", "ma5_slope", "ma20_diff", "ma60_diff",
    # 财报 (美股可能稀疏)
    "roe", "rev_growth", "profit_growth", "gross_margin", "debt_ratio",
]


def prepare_factors(df):
    boll_spread = df["BOLL_UP"] - df["BOLL_DN"]
    df["BOLL_pos"] = (df["close"] - df["BOLL_DN"]) / boll_spread.replace(0, np.nan) * 100
    if "VOL_MA5" in df.columns:
        df["vol_ratio"] = df["volume"] / df["VOL_MA5"].replace(0, np.nan)
    df["ma5_slope"] = df["MA5"].pct_change(3) * 100
    df["ma20_diff"] = (df["close"] / df["MA20"] - 1) * 100
    df["ma60_diff"] = (df["close"] / df["MA60"] - 1) * 100
    return df


def compute_factor_ic(df, factor, horizons=[5, 10, 30]):
    results = {}
    for h in horizons:
        fwd = df["close"].shift(-h) / df["close"] - 1
        valid = pd.DataFrame({"factor": df[factor], "fwd": fwd}).dropna()
        if len(valid) < 30:
            continue
        ic = valid["factor"].corr(valid["fwd"])
        rank_ic = valid["factor"].rank().corr(valid["fwd"].rank())
        results[h] = {"IC": ic, "RankIC": rank_ic, "n": len(valid)}
    return results


def main():
    print(f"\n{'=' * 76}")
    print(f"  美股因子有效性分析")
    print(f"  IC > 0: 因子越大->未来涨(动量)  IC < 0: 因子越大->未来跌(反转)")
    print(f"  |RankIC| > 0.03: 弱有效, > 0.05: 中等, > 0.08: 强有效")
    print(f"{'=' * 76}")

    all_fund = fetch_us_financials()
    all_scores = {}

    for ticker, name in US_STOCKS.items():
        df = fetch_us_history(ticker, "10y")
        fund_df = all_fund.get(ticker)
        df = compute_all(df, fund_df)
        df = prepare_factors(df)
        df = df.iloc[80:]  # 去掉热身期

        print(f"\n  === {name}({ticker})  {len(df)}天 ===\n")
        print(f"  {'因子':<16} {'5日IC':>8} {'10日IC':>8} {'30日IC':>8} {'5日RkIC':>8} {'10日RkIC':>9} {'30日RkIC':>9}")
        print(f"  {'-' * 72}")

        stock_scores = {}
        for factor in FACTORS:
            if factor not in df.columns:
                continue
            nan_pct = df[factor].isna().mean()
            if nan_pct > 0.7:
                continue

            ics = compute_factor_ic(df, factor)
            if not ics:
                continue

            ic5 = ics.get(5, {}).get("IC", 0)
            ic10 = ics.get(10, {}).get("IC", 0)
            ic30 = ics.get(30, {}).get("IC", 0)
            rkic5 = ics.get(5, {}).get("RankIC", 0)
            rkic10 = ics.get(10, {}).get("RankIC", 0)
            rkic30 = ics.get(30, {}).get("RankIC", 0)

            best_rk = max(abs(rkic5), abs(rkic10), abs(rkic30))
            if best_rk > 0.08:
                tag = " ***"
            elif best_rk > 0.05:
                tag = "  **"
            elif best_rk > 0.03:
                tag = "   *"
            else:
                tag = ""

            print(f"  {factor:<16} {ic5:>+8.4f} {ic10:>+8.4f} {ic30:>+8.4f} {rkic5:>+8.4f} {rkic10:>+9.4f} {rkic30:>+9.4f}{tag}")
            stock_scores[factor] = {"rkic5": rkic5, "rkic10": rkic10, "rkic30": rkic30}

        all_scores[ticker] = stock_scores
        time.sleep(0.3)

    # 汇总
    print(f"\n{'=' * 76}")
    print(f"  跨股票汇总 (30日RankIC)")
    print(f"{'=' * 76}")
    print(f"\n  {'因子':<16}", end="")
    for name in US_STOCKS.values():
        print(f" {name:>8}", end="")
    print(f" {'平均|IC|':>8} {'一致?':>5}")
    print(f"  {'-' * 60}")

    for factor in FACTORS:
        vals = []
        print(f"  {factor:<16}", end="")
        for ticker in US_STOCKS:
            v = all_scores.get(ticker, {}).get(factor, {}).get("rkic30", 0)
            vals.append(v)
            print(f" {v:>+8.4f}", end="")

        avg_abs = np.mean([abs(v) for v in vals]) if vals else 0
        signs = [np.sign(v) for v in vals if abs(v) > 0.02]
        consistent = "YES" if signs and (all(s > 0 for s in signs) or all(s < 0 for s in signs)) else "no"
        print(f" {avg_abs:>8.4f} {consistent:>5}")


if __name__ == "__main__":
    main()
