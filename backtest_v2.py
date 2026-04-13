#!/usr/bin/env python3
"""回测 v2 — 5年数据, 含30日/180日周期, 分段验证稳定性

改进:
1. 使用新浪API获取1500+交易日数据(~6年)
2. 增加30日和180日预测周期
3. 分前半/后半两段验证模型稳定性
4. 输出更精简, 聚焦可操作的结论
"""

import time
import json
import numpy as np
import pandas as pd
import requests
from datetime import datetime

from fetch_data import STOCKS, _sina_symbol
from indicators import compute_all
from probability import score_trend, IC_WINDOW, HORIZON
from fundamental import fetch_financial

HORIZONS = [1, 3, 5, 10, 30, 180]
WARMUP = IC_WINDOW + HORIZON + 20  # 模型需要的最小数据量


def fetch_sina_history(symbol: str, count: int = 1500) -> pd.DataFrame:
    """从新浪获取长周期日线数据"""
    sina_sym = _sina_symbol(symbol)
    url = (f"https://money.finance.sina.com.cn/quotes_service/api/json_v2.php/"
           f"CN_MarketData.getKLineData?symbol={sina_sym}&scale=240&ma=no&datalen={count}")
    resp = requests.get(url, timeout=15, headers={"Referer": "https://finance.sina.com.cn"})
    data = json.loads(resp.text)
    rows = []
    for item in data:
        rows.append({
            "date": item["day"],
            "open": float(item["open"]),
            "close": float(item["close"]),
            "high": float(item["high"]),
            "low": float(item["low"]),
            "volume": int(float(item["volume"])),
        })
    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"])
    df.set_index("date", inplace=True)
    df["pct_chg"] = df["close"].pct_change() * 100
    df["change"] = df["close"].diff()
    return df


def run_backtest(code: str, name: str) -> pd.DataFrame:
    """单只股票回测"""
    print(f"  {name}({code}): 获取数据...", end="", flush=True)
    df = fetch_sina_history(code, 1500)
    print(f" {len(df)}天", end="", flush=True)

    # 获取财报数据
    try:
        fund_df = fetch_financial(code)
        print(f" +{len(fund_df)}期财报", end="", flush=True)
    except Exception:
        fund_df = None

    df = compute_all(df, fund_df)

    # 计算未来收益率
    for h in HORIZONS:
        df[f"fwd_{h}d"] = df["close"].shift(-h) / df["close"] - 1

    records = []
    test_end = len(df) - max(HORIZONS)

    for i in range(WARMUP, test_end):
        window = df.iloc[:i + 1].copy()
        try:
            result = score_trend(window)
        except Exception:
            continue
        if "error" in result:
            continue

        row = df.iloc[i]
        # 直接使用原始评分 (6年验证正向有效)
        outlook = result["score"]

        record = {"date": row.name, "close": row["close"], "outlook": outlook}
        for h in HORIZONS:
            fwd = row[f"fwd_{h}d"]
            if pd.notna(fwd):
                record[f"fwd_{h}d_ret"] = fwd
                record[f"fwd_{h}d_up"] = 1 if fwd > 0 else 0
        records.append(record)

    print(f" -> {len(records)}天回测完成")
    return pd.DataFrame(records)


def analyze(results: pd.DataFrame, name: str, code: str, label: str = ""):
    """分析回测结果"""
    if len(results) < 30:
        print(f"  {name}: 样本不足\n")
        return

    tag = f" [{label}]" if label else ""
    print(f"\n  ┏━━ {name}({code}){tag}  {results['date'].iloc[0].date()} ~ {results['date'].iloc[-1].date()}  n={len(results)}")

    # 按评分分5档
    bins = [(-101, -40), (-40, -10), (-10, 10), (10, 40), (40, 101)]
    labels_list = ["强看跌", " 偏跌 ", " 震荡 ", " 偏涨 ", "强看涨"]

    # 表头
    print(f"  ┃ {'评分区间':<8}", end="")
    print(f"  {'n':>4}", end="")
    for h in HORIZONS:
        print(f"  {h}日涨率", end="")
    print()
    print(f"  ┃ {'─' * 68}")

    for (lo, hi), lbl in zip(bins, labels_list):
        mask = (results["outlook"] > lo) & (results["outlook"] <= hi)
        sub = results[mask]
        if len(sub) < 3:
            continue
        print(f"  ┃ {lbl}({lo:+d}~{hi:+d})", end="")
        print(f" {len(sub):>4}", end="")
        for h in HORIZONS:
            col = f"fwd_{h}d_up"
            if col in sub.columns:
                valid = sub[col].dropna()
                if len(valid) > 0:
                    rate = valid.mean() * 100
                    tag_str = "*" if rate >= 60 or rate <= 40 else " "
                    print(f"  {rate:5.0f}%{tag_str}", end="")
                else:
                    print(f"     -  ", end="")
            else:
                print(f"     -  ", end="")
        print()

    # 最关键指标: 强看涨 vs 强看跌的差异
    bull = results[results["outlook"] > 40]
    bear = results[results["outlook"] <= -40]
    print(f"  ┃")
    print(f"  ┃ 强看涨vs强看跌 差异:")
    for h in HORIZONS:
        col_up = f"fwd_{h}d_up"
        col_ret = f"fwd_{h}d_ret"
        if col_up in bull.columns and len(bull) > 3 and len(bear) > 3:
            b_valid = bull[col_up].dropna()
            s_valid = bear[col_up].dropna()
            b_ret = bull[col_ret].dropna()
            s_ret = bear[col_ret].dropna()
            if len(b_valid) > 0 and len(s_valid) > 0:
                diff_rate = b_valid.mean() * 100 - s_valid.mean() * 100
                diff_ret = b_ret.mean() * 100 - s_ret.mean() * 100
                sig = "***" if abs(diff_rate) > 15 else ("**" if abs(diff_rate) > 10 else ("*" if abs(diff_rate) > 5 else ""))
                print(f"  ┃   {h}日: 涨率差{diff_rate:+.0f}pp  收益差{diff_ret:+.1f}%  {sig}")
    print(f"  ┗{'━' * 60}")


def main():
    print(f"\n{'═' * 72}")
    print(f"  模型回测验证 v2  |  {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"  数据: ~6年(1500交易日)  周期: {HORIZONS}日")
    print(f"  含分段验证(前半/后半)检查模型稳定性")
    print(f"{'═' * 72}\n")

    all_results = {}
    for code, name_cn in STOCKS.items():
        try:
            bt = run_backtest(code, name_cn)
            if len(bt) > 0:
                all_results[code] = bt
                analyze(bt, name_cn, code, "全量")

                # 分段验证
                mid = len(bt) // 2
                analyze(bt.iloc[:mid].copy(), name_cn, code, "前半段")
                analyze(bt.iloc[mid:].copy(), name_cn, code, "后半段")
        except Exception as e:
            print(f"  [!] {name_cn}({code}) 失败: {e}")
        time.sleep(0.3)

    # 汇总
    if all_results:
        combined = pd.concat(all_results.values(), ignore_index=True)
        print(f"\n{'═' * 72}")
        print(f"  全部汇总  n={len(combined)}")
        print(f"{'═' * 72}")

        # 五分位
        try:
            combined["q"] = pd.qcut(combined["outlook"], 5,
                                    labels=["Q1最跌", "Q2", "Q3中性", "Q4", "Q5最涨"],
                                    duplicates="drop")
        except Exception:
            combined["q"] = pd.cut(combined["outlook"], 5,
                                   labels=["Q1最跌", "Q2", "Q3中性", "Q4", "Q5最涨"])

        print(f"\n  {'分位':<8} {'n':>5} {'均分':>6}", end="")
        for h in HORIZONS:
            print(f"  {h}日涨率", end="")
        print(f"  {180}日均值")
        print(f"  {'─' * 72}")

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
                print(f"     -")

        # 相关系数
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

        # 盈亏比
        print(f"\n  按信号操作盈亏比:")
        for threshold, action in [(40, "看涨>40做多"), (-40, "看跌<-40做空")]:
            sub = combined[combined["outlook"] >= threshold] if threshold > 0 else combined[combined["outlook"] <= threshold]
            if len(sub) < 10:
                continue
            for h in [5, 10, 30]:
                col = f"fwd_{h}d_ret"
                if col not in sub.columns:
                    continue
                rets = sub[col].dropna()
                if threshold > 0:
                    wins = rets[rets > 0]
                    losses = rets[rets <= 0]
                else:
                    wins = rets[rets < 0]  # 做空时, 跌=赢
                    losses = rets[rets >= 0]
                wr = len(wins) / len(rets) * 100 if len(rets) > 0 else 0
                aw = abs(wins.mean()) * 100 if len(wins) > 0 else 0
                al = abs(losses.mean()) * 100 if len(losses) > 0 else 0
                ratio = aw / al if al > 0 else float("inf")
                print(f"    {action} {h}日: 胜率{wr:.0f}%  均盈{aw:.2f}%/均亏{al:.2f}%  盈亏比{ratio:.2f}")

    print(f"\n{'═' * 72}")
    print(f"  说明: 历史回测结果, 过去不代表未来")
    print(f"{'═' * 72}\n")


if __name__ == "__main__":
    main()
