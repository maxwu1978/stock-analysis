#!/usr/bin/env python3
"""肥尾效应研究 — 能否识别极端收益阶段？

问题: 股市收益的大部分来自极少数的"肥尾"交易日。
     如果错过最好的10天, 十年收益可能腰斩。
     能否在这些极端日到来之前识别出前兆？

方法:
1. 找出历史上收益最大的5%交易日 (正肥尾)
2. 分析这些日子的前兆特征: 波动率、成交量、布林带、ADX等
3. 看这些前兆是否有统计意义上的预测力
"""

import json
import numpy as np
import pandas as pd
from datetime import datetime
from scipy import stats

from fetch_data import STOCKS, _sina_symbol
from fetch_us import US_STOCKS, fetch_us_history
from indicators import compute_all
import requests


def fetch_sina_history(symbol, count=1500):
    sina_sym = _sina_symbol(symbol)
    url = (f"https://money.finance.sina.com.cn/quotes_service/api/json_v2.php/"
           f"CN_MarketData.getKLineData?symbol={sina_sym}&scale=240&ma=no&datalen={count}")
    resp = requests.get(url, timeout=15, headers={"Referer": "https://finance.sina.com.cn"})
    data = json.loads(resp.text)
    rows = [{"date": item["day"], "open": float(item["open"]), "close": float(item["close"]),
             "high": float(item["high"]), "low": float(item["low"]),
             "volume": int(float(item["volume"]))} for item in data]
    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"])
    df.set_index("date", inplace=True)
    df["pct_chg"] = df["close"].pct_change() * 100
    df["change"] = df["close"].diff()
    return df


def analyze_fat_tails(df, name):
    """分析肥尾事件的前兆"""
    df = compute_all(df)

    # 添加衍生指标
    # 布林带宽度 (波动率收缩/扩张)
    df["boll_width"] = (df["BOLL_UP"] - df["BOLL_DN"]) / df["BOLL_MID"] * 100
    # 滚动波动率 (5日 vs 20日)
    df["vol_5"] = df["pct_chg"].rolling(5).std()
    df["vol_20"] = df["pct_chg"].rolling(20).std()
    df["vol_ratio"] = df["vol_5"] / df["vol_20"].replace(0, np.nan)
    # 滚动偏度和峰度
    df["skew_20"] = df["pct_chg"].rolling(20).skew()
    df["kurt_20"] = df["pct_chg"].rolling(20).apply(lambda x: x.kurtosis(), raw=False)
    # 成交量变化
    df["vol_ma5"] = df["volume"].rolling(5).mean()
    df["vol_ma20"] = df["volume"].rolling(20).mean()
    df["vol_surge"] = df["vol_ma5"] / df["vol_ma20"].replace(0, np.nan)
    # 连续涨跌天数
    df["streak"] = df["pct_chg"].apply(lambda x: 1 if x > 0 else -1)
    # ADX变化
    df["adx_chg"] = df["ADX"].diff(5)

    # 未来5日收益
    df["fwd_5d"] = df["close"].shift(-5) / df["close"] - 1

    valid = df.dropna(subset=["fwd_5d", "boll_width", "vol_ratio", "ADX"]).copy()

    # 定义肥尾: 未来5日收益在top 5% (正肥尾) 和 bottom 5% (负肥尾)
    top_5 = valid["fwd_5d"].quantile(0.95)
    bot_5 = valid["fwd_5d"].quantile(0.05)

    valid["tail_type"] = "normal"
    valid.loc[valid["fwd_5d"] >= top_5, "tail_type"] = "positive_tail"  # 大涨
    valid.loc[valid["fwd_5d"] <= bot_5, "tail_type"] = "negative_tail"  # 大跌

    n_total = len(valid)
    n_pos = (valid["tail_type"] == "positive_tail").sum()
    n_neg = (valid["tail_type"] == "negative_tail").sum()
    n_norm = (valid["tail_type"] == "normal").sum()

    print(f"\n{'=' * 70}")
    print(f"  {name}  样本={n_total}天")
    print(f"  正肥尾(top5%): {n_pos}天, 阈值={top_5*100:+.1f}%")
    print(f"  负肥尾(bot5%): {n_neg}天, 阈值={bot_5*100:+.1f}%")
    print(f"  正常(中间90%): {n_norm}天")
    print(f"{'=' * 70}")

    # 收益占比分析
    total_ret = valid["fwd_5d"].sum()
    pos_tail_ret = valid[valid["tail_type"] == "positive_tail"]["fwd_5d"].sum()
    neg_tail_ret = valid[valid["tail_type"] == "negative_tail"]["fwd_5d"].sum()
    norm_ret = valid[valid["tail_type"] == "normal"]["fwd_5d"].sum()
    print(f"\n  收益占比:")
    print(f"    正肥尾(5%天数)贡献收益: {pos_tail_ret/total_ret*100:.0f}%  平均每次{valid[valid['tail_type']=='positive_tail']['fwd_5d'].mean()*100:+.1f}%")
    print(f"    负肥尾(5%天数)贡献收益: {neg_tail_ret/total_ret*100:.0f}%  平均每次{valid[valid['tail_type']=='negative_tail']['fwd_5d'].mean()*100:+.1f}%")
    print(f"    正常(90%天数)贡献收益: {norm_ret/total_ret*100:.0f}%")

    # 各前兆指标在肥尾事件前的值 vs 正常时期
    indicators = [
        ("boll_width", "布林带宽度(%)"),
        ("vol_ratio", "短期/长期波动比"),
        ("ADX", "趋势强度ADX"),
        ("adx_chg", "ADX 5日变化"),
        ("RSI6", "RSI6"),
        ("vol_surge", "量能比(5日/20日)"),
        ("kurt_20", "20日峰度"),
        ("skew_20", "20日偏度"),
        ("autocorr", "自相关系数"),
    ]

    print(f"\n  前兆指标对比 (肥尾事件发生前的指标值):")
    print(f"  {'指标':<22} {'正肥尾前':>10} {'正常时':>10} {'负肥尾前':>10} {'正-常差异':>10} {'p值':>8}")
    print(f"  {'-' * 72}")

    useful_indicators = []

    for col, label in indicators:
        if col not in valid.columns:
            continue

        pos_vals = valid[valid["tail_type"] == "positive_tail"][col].dropna()
        norm_vals = valid[valid["tail_type"] == "normal"][col].dropna()
        neg_vals = valid[valid["tail_type"] == "negative_tail"][col].dropna()

        if len(pos_vals) < 5 or len(norm_vals) < 10:
            continue

        pos_mean = pos_vals.mean()
        norm_mean = norm_vals.mean()
        neg_mean = neg_vals.mean()

        # t检验: 正肥尾前 vs 正常
        t_stat, p_val = stats.ttest_ind(pos_vals, norm_vals)
        sig = "***" if p_val < 0.01 else ("**" if p_val < 0.05 else ("*" if p_val < 0.1 else ""))

        print(f"  {label:<22} {pos_mean:>10.2f} {norm_mean:>10.2f} {neg_mean:>10.2f} {pos_mean - norm_mean:>+10.2f} {p_val:>7.4f} {sig}")

        if p_val < 0.1:
            useful_indicators.append((col, label, pos_mean - norm_mean, p_val))

    # 组合信号测试: 能否用前兆指标预测肥尾?
    print(f"\n  有预测价值的前兆信号:")
    if useful_indicators:
        for col, label, diff, p in useful_indicators:
            direction = "高" if diff > 0 else "低"
            print(f"    {label}: 正肥尾前显著偏{direction} (差异={diff:+.2f}, p={p:.4f})")
    else:
        print(f"    (无统计显著的前兆)")

    # 实际预测测试: 用组合条件能否提高命中率
    # 条件: 布林带收窄(低波动) + ADX低(无趋势) = "蓄力"状态
    if "boll_width" in valid.columns and "ADX" in valid.columns:
        bw_low = valid["boll_width"] < valid["boll_width"].quantile(0.3)
        adx_low = valid["ADX"] < valid["ADX"].quantile(0.3)
        squeeze = bw_low & adx_low  # 同时满足=极度收缩

        squeeze_days = valid[squeeze]
        normal_days = valid[~squeeze]

        if len(squeeze_days) > 10:
            sq_pos_rate = (squeeze_days["fwd_5d"] >= top_5).mean() * 100
            sq_neg_rate = (squeeze_days["fwd_5d"] <= bot_5).mean() * 100
            nm_pos_rate = (normal_days["fwd_5d"] >= top_5).mean() * 100
            nm_neg_rate = (normal_days["fwd_5d"] <= bot_5).mean() * 100

            print(f"\n  '蓄力'状态测试 (布林带窄+ADX低, n={len(squeeze_days)}):")
            print(f"    正肥尾命中率: 蓄力时{sq_pos_rate:.1f}% vs 非蓄力{nm_pos_rate:.1f}%  (基准5%)")
            print(f"    负肥尾命中率: 蓄力时{sq_neg_rate:.1f}% vs 非蓄力{nm_neg_rate:.1f}%  (基准5%)")
            sq_avg = squeeze_days["fwd_5d"].mean() * 100
            nm_avg = normal_days["fwd_5d"].mean() * 100
            print(f"    平均5日收益: 蓄力时{sq_avg:+.2f}% vs 非蓄力{nm_avg:+.2f}%")

    return valid


def main():
    print(f"{'#' * 70}")
    print(f"  肥尾效应研究  {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"  问题: 能否识别即将发生极端收益(正/负肥尾)的前兆?")
    print(f"{'#' * 70}")

    # A股
    print(f"\n  === A股 ===")
    for code, name in STOCKS.items():
        try:
            df = fetch_sina_history(code, 1500)
            analyze_fat_tails(df, f"A股-{name}")
        except Exception as e:
            print(f"  [!] {name}: {e}")

    # 美股
    print(f"\n  === 美股 ===")
    for ticker, name in US_STOCKS.items():
        try:
            df = fetch_us_history(ticker, "10y")
            analyze_fat_tails(df, f"美股-{name}")
        except Exception as e:
            print(f"  [!] {name}: {e}")

    print(f"\n{'#' * 70}")
    print(f"  结论: 看各指标在正肥尾前是否显著偏离正常值")
    print(f"  如果'蓄力'状态(低波动+低ADX)命中率显著高于5%基准,")
    print(f"  则可以作为肥尾预警信号加入模型")
    print(f"{'#' * 70}\n")


if __name__ == "__main__":
    main()
