#!/usr/bin/env python3
"""美股回测验证 — 5年数据, 含分段验证"""

import time
import numpy as np
import pandas as pd
from datetime import datetime

from fetch_us import US_STOCKS, fetch_us_history, fetch_us_financials
from indicators import compute_all
from probability_us import score_trend_us as score_trend, IC_WINDOW, HORIZON

HORIZONS = [5, 10, 30, 180]
WARMUP = IC_WINDOW + HORIZON + 20


def run_backtest(ticker, name):
    print(f"  {name}({ticker}): 获取数据...", end="", flush=True)
    df = fetch_us_history(ticker, "10y")
    print(f" {len(df)}天", end="", flush=True)

    try:
        from fetch_us import fetch_us_financials as _ff
        all_fund = _ff()
        fund_df = all_fund.get(ticker)
    except:
        fund_df = None

    df = compute_all(df, fund_df)

    for h in HORIZONS:
        df[f"fwd_{h}d"] = df["close"].shift(-h) / df["close"] - 1

    records = []
    test_end = len(df) - max(HORIZONS)

    for i in range(WARMUP, test_end):
        window = df.iloc[:i + 1].copy()
        try:
            result = score_trend(window)
        except:
            continue
        if "error" in result:
            continue

        row = df.iloc[i]
        # Keep the US backtest aligned with the production model output:
        # positive score means bullish, negative score means bearish.
        outlook = result["score"]

        record = {"date": row.name, "close": row["close"], "outlook": outlook}
        for h in HORIZONS:
            fwd = row[f"fwd_{h}d"]
            if pd.notna(fwd):
                record[f"fwd_{h}d_ret"] = fwd
                record[f"fwd_{h}d_up"] = 1 if fwd > 0 else 0
        records.append(record)

    print(f" -> {len(records)}天回测")
    return pd.DataFrame(records)


def analyze(results, name, ticker, label=""):
    if len(results) < 30:
        print(f"  {name}: 样本不足\n")
        return

    tag = f" [{label}]" if label else ""
    print(f"\n  --- {name}({ticker}){tag}  {results['date'].iloc[0].date()} ~ {results['date'].iloc[-1].date()}  n={len(results)}")

    bins = [(-101, -40), (-40, -10), (-10, 10), (10, 40), (40, 101)]
    labels_list = ["强看跌", " 偏跌 ", " 震荡 ", " 偏涨 ", "强看涨"]

    print(f"  {'评分区间':<16} {'n':>5}", end="")
    for h in HORIZONS:
        print(f"  {h:>3}日涨率", end="")
    print()
    print(f"  {'-' * 60}")

    for (lo, hi), lbl in zip(bins, labels_list):
        mask = (results["outlook"] > lo) & (results["outlook"] <= hi)
        sub = results[mask]
        if len(sub) < 3:
            continue
        print(f"  {lbl}({lo:+d}~{hi:+d}) {len(sub):>5}", end="")
        for h in HORIZONS:
            col = f"fwd_{h}d_up"
            if col in sub.columns:
                valid = sub[col].dropna()
                if len(valid) > 0:
                    rate = valid.mean() * 100
                    tag_s = "*" if rate >= 60 or rate <= 40 else " "
                    print(f"  {rate:5.0f}%{tag_s}", end="")
                else:
                    print(f"     -  ", end="")
            else:
                print(f"     -  ", end="")
        print()

    # Q5 vs Q1
    bull = results[results["outlook"] > 40]
    bear = results[results["outlook"] <= -40]
    if len(bull) > 5 and len(bear) > 5:
        print(f"\n  强看涨 vs 强看跌:")
        for h in HORIZONS:
            col_up = f"fwd_{h}d_up"
            col_ret = f"fwd_{h}d_ret"
            if col_up in bull.columns:
                b_rate = bull[col_up].dropna().mean() * 100
                s_rate = bear[col_up].dropna().mean() * 100
                b_ret = bull[col_ret].dropna().mean() * 100
                s_ret = bear[col_ret].dropna().mean() * 100
                diff = b_rate - s_rate
                sig = "***" if abs(diff) > 15 else ("**" if abs(diff) > 10 else ("*" if abs(diff) > 5 else ""))
                print(f"    {h}日: 涨率差{diff:+.0f}pp  收益差{b_ret - s_ret:+.1f}%  {sig}")


def main():
    print(f"\n{'=' * 60}")
    print(f"  美股模型回测验证  {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"  数据: 最长10年  周期: {HORIZONS}日")
    print(f"{'=' * 60}\n")

    # 只获取一次财报
    all_results = {}
    for ticker, name in US_STOCKS.items():
        try:
            bt = run_backtest(ticker, name)
            if len(bt) > 0:
                all_results[ticker] = bt
                analyze(bt, name, ticker, "全量")

                mid = len(bt) // 2
                analyze(bt.iloc[:mid].copy(), name, ticker, "前半段")
                analyze(bt.iloc[mid:].copy(), name, ticker, "后半段")
        except Exception as e:
            print(f"  [!] {name}({ticker}) 失败: {e}")
        time.sleep(0.3)

    # 汇总
    if all_results:
        combined = pd.concat(all_results.values(), ignore_index=True)
        print(f"\n{'=' * 60}")
        print(f"  汇总  n={len(combined)}")
        print(f"{'=' * 60}")

        try:
            combined["q"] = pd.qcut(combined["outlook"], 5,
                                    labels=["Q1最跌", "Q2", "Q3中性", "Q4", "Q5最涨"],
                                    duplicates="drop")
        except:
            combined["q"] = pd.cut(combined["outlook"], 5,
                                   labels=["Q1最跌", "Q2", "Q3中性", "Q4", "Q5最涨"])

        print(f"\n  {'分位':<8} {'n':>5} {'均分':>6}", end="")
        for h in HORIZONS:
            print(f"  {h:>3}日涨率", end="")
        print(f"  180日均值")
        print(f"  {'-' * 60}")

        for q in ["Q1最跌", "Q2", "Q3中性", "Q4", "Q5最涨"]:
            sub = combined[combined["q"] == q]
            if len(sub) == 0:
                continue
            avg_score = sub["outlook"].mean()
            print(f"  {q:<8} {len(sub):>5} {avg_score:>+6.0f}", end="")
            for h in HORIZONS:
                col = f"fwd_{h}d_up"
                if col in sub.columns:
                    valid = sub[col].dropna()
                    rate = valid.mean() * 100 if len(valid) > 0 else 0
                    print(f"  {rate:5.0f}%", end="")
                else:
                    print(f"     -", end="")
            col_180 = "fwd_180d_ret"
            if col_180 in sub.columns:
                valid = sub[col_180].dropna()
                avg = valid.mean() * 100 if len(valid) > 0 else 0
                print(f"  {avg:+.1f}%")
            else:
                print()

        # IC
        print(f"\n  评分与收益相关系数:")
        for h in HORIZONS:
            col = f"fwd_{h}d_ret"
            if col in combined.columns:
                valid = combined[["outlook", col]].dropna()
                if len(valid) > 30:
                    r = valid["outlook"].corr(valid[col])
                    rk = valid["outlook"].rank().corr(valid[col].rank())
                    sig = "***" if abs(rk) > 0.08 else ("**" if abs(rk) > 0.05 else ("*" if abs(rk) > 0.03 else ""))
                    print(f"    {h:>3}日: IC={r:+.4f}  RankIC={rk:+.4f}  {sig}")

    print(f"\n{'=' * 60}")
    print(f"  说明: 历史回测, 过去不代表未来")
    print(f"{'=' * 60}\n")


if __name__ == "__main__":
    main()
