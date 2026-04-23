#!/usr/bin/env python3
"""回测验证 - 用过去3年历史数据检验评分模型的预测准确性

方法：
1. 获取每只股票约800个交易日的历史数据
2. 从第120天开始，每天计算综合评分
3. 记录评分后实际的1日/3日/5日/10日收益率
4. 按评分区间统计预测准确率
"""

import time
import pandas as pd
import numpy as np
from datetime import datetime

from fetch_data import STOCKS, _sina_symbol
from indicators import compute_all
from probability import score_trend

import requests

HORIZONS = [1, 3, 5, 10]
WARMUP = 80  # 前80天用于计算指标，不参与回测


def fetch_long_history(symbol: str, count: int = 800) -> pd.DataFrame:
    """获取长周期历史数据"""
    sina_sym = _sina_symbol(symbol)
    url = f"https://web.ifzq.gtimg.cn/appstock/app/fqkline/get?param={sina_sym},day,,,{count},qfq"
    resp = requests.get(url, timeout=15)
    data = resp.json()
    klines = data["data"][sina_sym].get("qfqday") or data["data"][sina_sym].get("day", [])
    rows = []
    for k in klines:
        rows.append({
            "date": k[0],
            "open": float(k[1]),
            "close": float(k[2]),
            "high": float(k[3]),
            "low": float(k[4]),
            "volume": int(float(k[5])) if len(k) > 5 else 0,
        })
    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"])
    df.set_index("date", inplace=True)
    df["pct_chg"] = df["close"].pct_change() * 100
    df["change"] = df["close"].diff()
    return df


def run_backtest_one(code: str, name: str) -> pd.DataFrame:
    """对单只股票进行回测"""
    print(f"  {name}({code}): 获取数据...", end="", flush=True)
    df = fetch_long_history(code, 800)
    print(f" {len(df)}天", end="", flush=True)

    # 计算所有指标
    df = compute_all(df)

    # 计算未来收益率
    for h in HORIZONS:
        df[f"fwd_{h}d"] = df["close"].shift(-h) / df["close"] - 1

    records = []
    test_start = WARMUP
    test_end = len(df) - max(HORIZONS)  # 需要留出未来收益率的空间

    for i in range(test_start, test_end):
        window = df.iloc[:i + 1].copy()
        try:
            result = score_trend(window)
        except Exception:
            continue

        if "error" in result:
            continue

        row_data = df.iloc[i]
        record = {
            "date": row_data.name,
            "close": row_data["close"],
            "score": result["score"],
            "direction": result["direction"],
            "n_bullish": len(result["bullish_factors"]),
            "n_bearish": len(result["bearish_factors"]),
        }
        for h in HORIZONS:
            record[f"fwd_{h}d_ret"] = row_data[f"fwd_{h}d"]
            record[f"fwd_{h}d_up"] = 1 if row_data[f"fwd_{h}d"] > 0 else 0

        records.append(record)

    print(f" -> 回测{len(records)}天")
    return pd.DataFrame(records)


def analyze_results(results: pd.DataFrame, name: str, code: str):
    """分析单只股票的回测结果"""
    print(f"\n  {'=' * 68}")
    print(f"  {name}({code})  回测期间: {results['date'].iloc[0].date()} ~ {results['date'].iloc[-1].date()}")
    print(f"  总样本: {len(results)}天")
    print(f"  {'=' * 68}")

    # 1. 按评分区间统计
    bins = [(-101, -30), (-30, -10), (-10, 10), (10, 30), (30, 101)]
    labels = ["强空(<-30)", "偏空(-30~-10)", "中性(-10~10)", "偏多(10~30)", "强多(>30)"]

    print(f"\n  {'评分区间':<16} {'样本':>6} ", end="")
    for h in HORIZONS:
        print(f"{'':>3}{h}日涨率  {h}日均值", end="")
    print()
    print(f"  {'─' * 66}")

    for (lo, hi), label in zip(bins, labels):
        mask = (results["score"] > lo) & (results["score"] <= hi)
        subset = results[mask]
        if len(subset) == 0:
            continue

        print(f"  {label:<16} {len(subset):>5}  ", end="")
        for h in HORIZONS:
            up_rate = subset[f"fwd_{h}d_up"].mean() * 100
            avg_ret = subset[f"fwd_{h}d_ret"].mean() * 100
            print(f"  {up_rate:5.1f}% {avg_ret:+6.2f}%", end="")
        print()

    # 2. 方向准确率
    print(f"\n  方向预测准确率:")
    for direction in ["偏多", "中性偏多", "中性", "中性偏空", "偏空"]:
        mask = results["direction"] == direction
        subset = results[mask]
        if len(subset) < 5:
            continue

        print(f"    {direction:<8} (n={len(subset):>4})  ", end="")
        for h in HORIZONS:
            up_rate = subset[f"fwd_{h}d_up"].mean() * 100
            avg_ret = subset[f"fwd_{h}d_ret"].mean() * 100
            print(f"  {h}日: {up_rate:.0f}%涨/{avg_ret:+.2f}%", end="")
        print()

    # 3. 极端评分的表现
    print(f"\n  极端评分表现:")
    for threshold, label in [(40, "评分>=40"), (50, "评分>=50"), (-40, "评分<=-40"), (-50, "评分<=-50")]:
        if threshold > 0:
            mask = results["score"] >= threshold
        else:
            mask = results["score"] <= threshold
        subset = results[mask]
        if len(subset) < 3:
            continue
        print(f"    {label:<12} (n={len(subset):>3})  ", end="")
        for h in HORIZONS:
            up_rate = subset[f"fwd_{h}d_up"].mean() * 100
            avg_ret = subset[f"fwd_{h}d_ret"].mean() * 100
            print(f"  {h}日: {up_rate:.0f}%/{avg_ret:+.2f}%", end="")
        print()

    return results


def compute_overall_stats(all_results: dict):
    """汇总所有股票的回测结果"""
    combined = pd.concat(all_results.values(), ignore_index=True)

    print(f"\n{'═' * 72}")
    print(f"  汇 总 统 计 (全部{len(combined)}个样本)")
    print(f"{'═' * 72}")

    # 评分与收益率相关性
    print(f"\n  评分与未来收益率的相关系数:")
    for h in HORIZONS:
        corr = combined["score"].corr(combined[f"fwd_{h}d_ret"])
        print(f"    评分 vs {h}日收益: r = {corr:.4f}")

    # 按评分五分位统计
    combined["score_quintile"] = pd.qcut(combined["score"], 5, labels=["Q1最空", "Q2", "Q3中性", "Q4", "Q5最多"], duplicates="drop")
    print(f"\n  五分位统计:")
    print(f"  {'分位':<10} {'样本':>6}  {'均分':>6}", end="")
    for h in HORIZONS:
        print(f"  {h}日涨率  {h}日均值", end="")
    print()
    print(f"  {'─' * 70}")

    for q in ["Q1最空", "Q2", "Q3中性", "Q4", "Q5最多"]:
        mask = combined["score_quintile"] == q
        subset = combined[mask]
        if len(subset) == 0:
            continue
        avg_score = subset["score"].mean()
        print(f"  {q:<10} {len(subset):>5}  {avg_score:>+6.1f}", end="")
        for h in HORIZONS:
            up_rate = subset[f"fwd_{h}d_up"].mean() * 100
            avg_ret = subset[f"fwd_{h}d_ret"].mean() * 100
            print(f"  {up_rate:5.1f}% {avg_ret:+6.2f}%", end="")
        print()

    # 模型有效性判断
    print(f"\n  ── 模型有效性评估 ──")
    q1 = combined[combined["score_quintile"] == "Q1最空"]
    q5 = combined[combined["score_quintile"] == "Q5最多"]
    for h in HORIZONS:
        q5_ret = q5[f"fwd_{h}d_ret"].mean() * 100
        q1_ret = q1[f"fwd_{h}d_ret"].mean() * 100
        spread = q5_ret - q1_ret
        q5_up = q5[f"fwd_{h}d_up"].mean() * 100
        q1_up = q1[f"fwd_{h}d_up"].mean() * 100
        print(f"    {h}日: Q5最多均值{q5_ret:+.2f}% vs Q1最空{q1_ret:+.2f}%  "
              f"差值={spread:+.2f}%  涨率差={q5_up - q1_up:+.1f}pp")

    # 盈亏比
    print(f"\n  ── 按信号操作的盈亏比 ──")
    for threshold, label in [(30, "评分>30做多"), (50, "评分>50做多"), (-30, "评分<-30做空")]:
        if threshold > 0:
            mask = combined["score"] >= threshold
        else:
            mask = combined["score"] <= threshold
        subset = combined[mask]
        if len(subset) < 10:
            continue
        for h in HORIZONS:
            rets = subset[f"fwd_{h}d_ret"]
            if threshold > 0:
                wins = rets[rets > 0]
                losses = rets[rets <= 0]
            else:
                wins = rets[rets < 0]
                losses = rets[rets >= 0]
            win_rate = len(wins) / len(rets) * 100
            avg_win = abs(wins.mean()) * 100 if len(wins) > 0 else 0
            avg_loss = abs(losses.mean()) * 100 if len(losses) > 0 else 0
            pnl_ratio = avg_win / avg_loss if avg_loss > 0 else float("inf")
            print(f"    {label} {h}日: 胜率{win_rate:.0f}%  "
                  f"均盈{avg_win:.2f}%/均亏{avg_loss:.2f}%  盈亏比{pnl_ratio:.2f}")


def main():
    print(f"\n{'═' * 72}")
    print(f"  评分模型回测验证  |  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  回测周期: ~3年 (~800交易日)  前{WARMUP}天热身  验证{HORIZONS}日收益")
    print(f"{'═' * 72}\n")

    all_results = {}
    for code, name in STOCKS.items():
        try:
            bt = run_backtest_one(code, name)
            if len(bt) > 0:
                all_results[code] = analyze_results(bt, name, code)
        except Exception as e:
            print(f"  [!] {name}({code}) 回测失败: {e}")
        time.sleep(0.3)

    if all_results:
        compute_overall_stats(all_results)

    print(f"\n{'═' * 72}")
    print(f"  说明: 以上为历史回测统计结果，过去表现不代表未来。")
    print(f"{'═' * 72}\n")


if __name__ == "__main__":
    main()
