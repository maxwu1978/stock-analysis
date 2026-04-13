#!/usr/bin/env python3
"""因子有效性分析 — 找出哪些指标真正能预测未来收益

对每个因子计算:
1. IC (Information Coefficient): 因子值与未来收益的相关系数
2. IC方向: 正=动量有效, 负=反转有效
3. 分组单调性: 因子分5组, 收益是否单调递增/递减
4. 每只股票的最优因子组合
"""

import time
import numpy as np
import pandas as pd
from fetch_data import STOCKS, _sina_symbol
from indicators import compute_all
import requests


def fetch_long_history(symbol, count=800):
    sina_sym = _sina_symbol(symbol)
    url = f"https://web.ifzq.gtimg.cn/appstock/app/fqkline/get?param={sina_sym},day,,,{count},qfq"
    resp = requests.get(url, timeout=15)
    data = resp.json()
    klines = data["data"][sina_sym].get("qfqday") or data["data"][sina_sym].get("day", [])
    rows = [{"date": k[0], "open": float(k[1]), "close": float(k[2]),
             "high": float(k[3]), "low": float(k[4]),
             "volume": int(float(k[5])) if len(k) > 5 else 0} for k in klines]
    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"])
    df.set_index("date", inplace=True)
    df["pct_chg"] = df["close"].pct_change() * 100
    df["change"] = df["close"].diff()
    return df


# 要测试的因子
FACTORS = [
    "RSI6", "RSI12",
    "DIF", "DEA", "MACD",
    "ADX", "+DI", "-DI",
    "ROC5", "ROC10", "ROC20",
    "autocorr",
    "vol_price_div",
    "price_position",
    "BOLL_pos",      # 布林带位置 (需计算)
    "vol_ratio",     # 量比 (需计算)
    "ma5_slope",     # MA5斜率
    "ma20_diff",     # 价格偏离MA20的百分比
    "ma60_diff",     # 价格偏离MA60的百分比
    "high52w_pos",   # 52周高低位置 (George & Hwang 2004)
]


def prepare_factors(df):
    """计算所有候选因子"""
    df = compute_all(df)

    # 布林带位置
    spread = df["BOLL_UP"] - df["BOLL_DN"]
    df["BOLL_pos"] = (df["close"] - df["BOLL_DN"]) / spread.replace(0, np.nan) * 100

    # 量比
    df["vol_ratio"] = df["volume"] / df["VOL_MA5"].replace(0, np.nan)

    # MA5斜率
    df["ma5_slope"] = df["MA5"].pct_change(3) * 100

    # 偏离均线
    df["ma20_diff"] = (df["close"] / df["MA20"] - 1) * 100
    df["ma60_diff"] = (df["close"] / df["MA60"] - 1) * 100

    return df


def compute_factor_ic(df, factor, horizons=[1, 3, 5, 10]):
    """计算单个因子的IC值"""
    results = {}
    for h in horizons:
        fwd = df["close"].shift(-h) / df["close"] - 1
        valid = pd.DataFrame({"factor": df[factor], "fwd": fwd}).dropna()
        if len(valid) < 30:
            continue
        ic = valid["factor"].corr(valid["fwd"])
        # Rank IC (更稳健)
        rank_ic = valid["factor"].rank().corr(valid["fwd"].rank())

        # 分5组测试单调性
        try:
            valid["q"] = pd.qcut(valid["factor"], 5, labels=False, duplicates="drop")
            group_means = valid.groupby("q")["fwd"].mean()
            monotonic_score = group_means.corr(pd.Series(range(len(group_means))))
        except Exception:
            monotonic_score = 0

        results[h] = {
            "IC": ic,
            "RankIC": rank_ic,
            "monotonic": monotonic_score,
            "n": len(valid),
        }
    return results


def analyze_one_stock(code, name):
    """分析单只股票的所有因子"""
    df = fetch_long_history(code, 800)
    df = prepare_factors(df)

    # 去掉前80天热身期
    df = df.iloc[80:]

    print(f"\n  {'=' * 74}")
    print(f"  {name}({code})  样本: {len(df)}天")
    print(f"  {'=' * 74}")

    all_ics = {}
    for factor in FACTORS:
        if factor not in df.columns:
            continue
        ics = compute_factor_ic(df, factor)
        if ics:
            all_ics[factor] = ics

    # 按5日RankIC排序显示
    print(f"\n  {'因子':<14} {'1日IC':>8} {'3日IC':>8} {'5日IC':>8} {'10日IC':>8} {'5日RkIC':>8} {'单调性':>8}")
    print(f"  {'─' * 72}")

    sorted_factors = sorted(all_ics.items(),
                           key=lambda x: abs(x[1].get(5, {}).get("RankIC", 0)),
                           reverse=True)

    factor_scores = {}
    for factor, ics in sorted_factors:
        ic1 = ics.get(1, {}).get("IC", 0)
        ic3 = ics.get(3, {}).get("IC", 0)
        ic5 = ics.get(5, {}).get("IC", 0)
        ic10 = ics.get(10, {}).get("IC", 0)
        rkic5 = ics.get(5, {}).get("RankIC", 0)
        mono = ics.get(5, {}).get("monotonic", 0)

        # 标记方向
        direction = "+" if rkic5 > 0 else "-"
        strength = abs(rkic5)
        if strength > 0.08:
            tag = f" *** {direction}"
        elif strength > 0.05:
            tag = f"  ** {direction}"
        elif strength > 0.03:
            tag = f"   * {direction}"
        else:
            tag = ""

        print(f"  {factor:<14} {ic1:>+8.4f} {ic3:>+8.4f} {ic5:>+8.4f} {ic10:>+8.4f} {rkic5:>+8.4f} {mono:>+8.2f}{tag}")

        factor_scores[factor] = {
            "rank_ic_5d": rkic5,
            "rank_ic_10d": ics.get(10, {}).get("RankIC", 0),
            "direction": 1 if rkic5 > 0 else -1,
            "strength": strength,
        }

    return factor_scores


def main():
    print(f"\n{'═' * 76}")
    print(f"  因子有效性分析")
    print(f"  IC > 0: 因子越大 -> 未来涨  (动量方向)")
    print(f"  IC < 0: 因子越大 -> 未来跌  (反转方向)")
    print(f"  |RankIC| > 0.03: 弱有效, > 0.05: 中等, > 0.08: 强有效")
    print(f"{'═' * 76}")

    all_scores = {}
    for code, name in STOCKS.items():
        try:
            scores = analyze_one_stock(code, name)
            all_scores[code] = scores
        except Exception as e:
            print(f"  [!] {name}({code}) 失败: {e}")
        time.sleep(0.3)

    # 汇总: 哪些因子在多数股票上有效
    print(f"\n{'═' * 76}")
    print(f"  跨股票因子有效性汇总 (5日RankIC)")
    print(f"{'═' * 76}")
    print(f"\n  {'因子':<14}", end="")
    for name in STOCKS.values():
        print(f" {name:>10}", end="")
    print(f" {'平均|IC|':>10} {'一致方向':>10}")
    print(f"  {'─' * 74}")

    for factor in FACTORS:
        vals = []
        print(f"  {factor:<14}", end="")
        for code in STOCKS:
            v = all_scores.get(code, {}).get(factor, {}).get("rank_ic_5d", 0)
            vals.append(v)
            tag = "  " if abs(v) < 0.03 else "++" if v > 0 else "--"
            print(f" {v:>+8.4f}{tag}", end="")

        avg_abs = np.mean([abs(v) for v in vals])
        # 方向一致性: 全正或全负
        signs = [np.sign(v) for v in vals if abs(v) > 0.02]
        consistent = "YES" if signs and (all(s > 0 for s in signs) or all(s < 0 for s in signs)) else "no"
        print(f" {avg_abs:>10.4f} {consistent:>10}")


if __name__ == "__main__":
    main()
