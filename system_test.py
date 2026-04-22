#!/usr/bin/env python3
"""系统有效性验证 — 测试经理视角

测试维度:
1. 基准对比: 模型 vs 买入持有 vs 随机猜测
2. 样本外测试: 用前70%训练, 后30%纯样本外验证
3. 滚动窗口测试: 每季度重新验证, 看IC是否持续有效
4. 市场环境测试: 牛市/熊市/震荡分别验证
5. 统计显著性: t检验确认信号不是运气
6. 过拟合检测: 随机打乱评分, 看结果是否消失
7. 实际可操作性: 加入交易成本后是否仍然盈利
"""

import argparse
import time
import json
import numpy as np
import pandas as pd
from datetime import datetime
from scipy import stats

from fetch_data import STOCKS, _sina_symbol
from fetch_us import US_STOCKS, fetch_us_history, fetch_us_financials
from indicators import compute_all
from probability import score_trend
from probability_us import score_trend_us
from fundamental import fetch_financial
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


WARMUP = 145  # IC_WINDOW + HORIZON + buffer


def generate_signals(df, score_fn, fund_df=None):
    """对整个时间序列生成评分信号"""
    df = compute_all(df, fund_df)
    records = []
    for i in range(WARMUP, len(df)):
        window = df.iloc[:i + 1].copy()
        try:
            result = score_fn(window)
        except:
            continue
        if "error" in result:
            continue
        row = df.iloc[i]
        outlook = -result["score"]
        records.append({"date": row.name, "close": row["close"], "outlook": outlook})

    out = pd.DataFrame(records)
    if not out.empty:
        out = out.set_index("date")
        # 计算未来收益
        for h in [5, 10, 30]:
            out[f"fwd_{h}d"] = df["close"].reindex(out.index).shift(-h) / out["close"] - 1
    return out


def test_1_vs_baseline(signals, name, horizons=[5, 10, 30]):
    """测试1: 模型 vs 基准"""
    print(f"\n  [测试1] {name}: 模型 vs 基准 (买入持有 / 随机)")
    print(f"  {'策略':<20}", end="")
    for h in horizons:
        print(f" {h}日胜率  {h}日均值", end="")
    print()
    print(f"  {'-' * 60}")

    for h in horizons:
        col = f"fwd_{h}d"
        if col not in signals.columns:
            continue
        valid = signals[col].dropna()
        # 买入持有 (每天都看涨)
        bh_wr = (valid > 0).mean() * 100
        bh_avg = valid.mean() * 100

        if h == horizons[0]:
            print(f"  {'买入持有':<20} {bh_wr:5.1f}% {bh_avg:+6.2f}%", end="")
        else:
            print(f" {bh_wr:5.1f}% {bh_avg:+6.2f}%", end="")

    print()

    # 模型: 只在看涨(>30)时操作
    for threshold, label in [(30, "模型>30做多"), (0, "模型>0做多")]:
        bull = signals[signals["outlook"] > threshold]
        if len(bull) < 10:
            continue
        print(f"  {label:<20}", end="")
        for h in horizons:
            col = f"fwd_{h}d"
            if col not in bull.columns:
                continue
            valid = bull[col].dropna()
            if len(valid) == 0:
                print(f"    -      -  ", end="")
                continue
            wr = (valid > 0).mean() * 100
            avg = valid.mean() * 100
            print(f" {wr:5.1f}% {avg:+6.2f}%", end="")
        print()

    # 模型: 避开看跌(<-30)
    bear = signals[signals["outlook"] <= -30]
    if len(bear) >= 10:
        print(f"  {'模型<-30(应避开)':<20}", end="")
        for h in horizons:
            col = f"fwd_{h}d"
            if col not in bear.columns:
                continue
            valid = bear[col].dropna()
            if len(valid) == 0:
                print(f"    -      -  ", end="")
                continue
            wr = (valid > 0).mean() * 100
            avg = valid.mean() * 100
            print(f" {wr:5.1f}% {avg:+6.2f}%", end="")
        print(f"  <- 应低于买入持有")


def test_2_out_of_sample(signals, name, horizon=30):
    """测试2: 样本外验证 (前70%训练, 后30%测试)"""
    n = len(signals)
    split = int(n * 0.7)
    train = signals.iloc[:split]
    test = signals.iloc[split:]

    col = f"fwd_{horizon}d"
    if col not in signals.columns:
        return

    print(f"\n  [测试2] {name}: 样本外验证 ({horizon}日)")
    print(f"  训练集: {len(train)}天  测试集: {len(test)}天")

    for label, subset in [("训练集(样本内)", train), ("测试集(样本外)", test)]:
        bull = subset[subset["outlook"] > 30]
        bear = subset[subset["outlook"] <= -30]
        all_valid = subset[col].dropna()

        if len(bull) > 5:
            bull_wr = (bull[col].dropna() > 0).mean() * 100
            bull_avg = bull[col].dropna().mean() * 100
        else:
            bull_wr = bull_avg = 0

        if len(bear) > 5:
            bear_wr = (bear[col].dropna() > 0).mean() * 100
            bear_avg = bear[col].dropna().mean() * 100
        else:
            bear_wr = bear_avg = 0

        base_wr = (all_valid > 0).mean() * 100
        diff = bull_wr - bear_wr
        print(f"  {label:<16} 基准{base_wr:.0f}%  看涨{bull_wr:.0f}%(n={len(bull)})  "
              f"看跌{bear_wr:.0f}%(n={len(bear)})  差值{diff:+.0f}pp")


def test_3_rolling_quarterly(signals, name, horizon=30):
    """测试3: 滚动季度验证"""
    col = f"fwd_{horizon}d"
    if col not in signals.columns:
        return

    signals = signals.copy()
    signals["quarter"] = signals.index.to_period("Q")
    quarters = signals["quarter"].unique()

    print(f"\n  [测试3] {name}: 滚动季度IC ({horizon}日)")
    print(f"  {'季度':<10} {'IC':>8} {'RankIC':>8} {'看涨胜率':>8} {'看跌胜率':>8} {'差值':>8}")
    print(f"  {'-' * 55}")

    positive_quarters = 0
    total_quarters = 0

    for q in quarters[-12:]:  # 最近12个季度
        qdata = signals[signals["quarter"] == q]
        valid = qdata[["outlook", col]].dropna()
        if len(valid) < 15:
            continue

        ic = valid["outlook"].corr(valid[col])
        rkic = valid["outlook"].rank().corr(valid[col].rank())

        bull = qdata[qdata["outlook"] > 20]
        bear = qdata[qdata["outlook"] <= -20]
        bull_wr = (bull[col].dropna() > 0).mean() * 100 if len(bull) > 3 else 0
        bear_wr = (bear[col].dropna() > 0).mean() * 100 if len(bear) > 3 else 0
        diff = bull_wr - bear_wr

        total_quarters += 1
        if diff > 0:
            positive_quarters += 1
        sig = "+" if diff > 5 else ("-" if diff < -5 else " ")

        print(f"  {str(q):<10} {ic:>+8.3f} {rkic:>+8.3f} {bull_wr:>7.0f}% {bear_wr:>7.0f}% {diff:>+7.0f}pp {sig}")

    if total_quarters > 0:
        win_rate = positive_quarters / total_quarters * 100
        print(f"  季度胜率: {positive_quarters}/{total_quarters} = {win_rate:.0f}%")


def test_4_market_regime(signals, name, horizon=30):
    """测试4: 不同市场环境下的表现"""
    col = f"fwd_{horizon}d"
    if col not in signals.columns:
        return

    # 用60日收益率划分市场环境
    signals = signals.copy()
    ret_60 = signals["close"].pct_change(60) * 100
    signals["regime"] = pd.cut(ret_60, bins=[-999, -10, 10, 999], labels=["熊市", "震荡", "牛市"])

    print(f"\n  [测试4] {name}: 不同市场环境 ({horizon}日)")
    print(f"  {'环境':<6} {'n':>5} {'基准胜率':>8} {'看涨胜率':>8} {'看跌胜率':>8} {'差值':>8}")
    print(f"  {'-' * 50}")

    for regime in ["熊市", "震荡", "牛市"]:
        sub = signals[signals["regime"] == regime]
        if len(sub) < 20:
            continue

        base_wr = (sub[col].dropna() > 0).mean() * 100
        bull = sub[sub["outlook"] > 20]
        bear = sub[sub["outlook"] <= -20]
        bull_wr = (bull[col].dropna() > 0).mean() * 100 if len(bull) > 5 else 0
        bear_wr = (bear[col].dropna() > 0).mean() * 100 if len(bear) > 5 else 0

        print(f"  {regime:<6} {len(sub):>5} {base_wr:>7.0f}% {bull_wr:>7.0f}% {bear_wr:>7.0f}% {bull_wr - bear_wr:>+7.0f}pp")


def test_5_statistical_significance(signals, name, horizon=30):
    """测试5: 统计显著性 (t检验)"""
    col = f"fwd_{horizon}d"
    if col not in signals.columns:
        return

    bull = signals[signals["outlook"] > 30][col].dropna()
    bear = signals[signals["outlook"] <= -30][col].dropna()

    if len(bull) < 10 or len(bear) < 10:
        return

    t_stat, p_value = stats.ttest_ind(bull, bear)

    print(f"\n  [测试5] {name}: 统计显著性 ({horizon}日)")
    print(f"  看涨组: n={len(bull)}, 均值={bull.mean()*100:+.2f}%, 标准差={bull.std()*100:.2f}%")
    print(f"  看跌组: n={len(bear)}, 均值={bear.mean()*100:+.2f}%, 标准差={bear.std()*100:.2f}%")
    print(f"  t统计量={t_stat:.3f}, p值={p_value:.4f}", end="")
    if p_value < 0.01:
        print("  *** 极显著 (p<0.01)")
    elif p_value < 0.05:
        print("  ** 显著 (p<0.05)")
    elif p_value < 0.10:
        print("  * 弱显著 (p<0.10)")
    else:
        print("  不显著")


def test_6_overfit_check(signals, name, horizon=30, n_shuffles=100):
    """测试6: 过拟合检测 — 随机打乱评分, 看差值是否消失"""
    col = f"fwd_{horizon}d"
    if col not in signals.columns:
        return

    valid = signals[["outlook", col]].dropna()
    real_ic = valid["outlook"].rank().corr(valid[col].rank())

    # 随机打乱N次
    random_ics = []
    for _ in range(n_shuffles):
        shuffled = valid["outlook"].sample(frac=1, replace=False).values
        fake_ic = pd.Series(shuffled).rank().corr(valid[col].rank().reset_index(drop=True))
        random_ics.append(fake_ic)

    random_ics = np.array(random_ics)
    percentile = (random_ics < real_ic).mean() * 100

    print(f"\n  [测试6] {name}: 过拟合检测 ({horizon}日, {n_shuffles}次随机)")
    print(f"  真实RankIC: {real_ic:+.4f}")
    print(f"  随机RankIC: 均值={random_ics.mean():+.4f}, 标准差={random_ics.std():.4f}")
    print(f"  真实IC位于随机分布的第{percentile:.0f}百分位", end="")
    if percentile > 95:
        print("  -> 模型显著优于随机 (>95%)")
    elif percentile > 90:
        print("  -> 模型优于随机 (>90%)")
    elif percentile > 75:
        print("  -> 模型略优于随机 (>75%)")
    else:
        print("  -> 模型无显著优势")


def test_7_with_costs(signals, name, horizon=30, cost_pct=0.15):
    """测试7: 加入交易成本"""
    col = f"fwd_{horizon}d"
    if col not in signals.columns:
        return

    print(f"\n  [测试7] {name}: 交易成本影响 ({horizon}日, 单边{cost_pct}%)")

    # 无成本
    bull = signals[signals["outlook"] > 30]
    if len(bull) < 10:
        return
    gross = bull[col].dropna()
    gross_avg = gross.mean() * 100
    net_avg = gross_avg - cost_pct * 2  # 买入+卖出
    gross_wr = (gross > 0).mean() * 100
    net_wr = (gross > cost_pct * 2 / 100).mean() * 100

    print(f"  无成本: 胜率{gross_wr:.0f}%, 均值{gross_avg:+.2f}%")
    print(f"  扣成本: 胜率{net_wr:.0f}%, 均值{net_avg:+.2f}%", end="")
    if net_avg > 0:
        print("  -> 扣费后仍盈利")
    else:
        print("  -> 扣费后亏损!")


def run_full_test(name, signals, n_shuffles=100):
    """对一只股票运行所有测试"""
    print(f"\n{'=' * 60}")
    print(f"  {name}  样本: {len(signals)}天  {signals.index[0].date()} ~ {signals.index[-1].date()}")
    print(f"{'=' * 60}")

    test_1_vs_baseline(signals, name)
    test_2_out_of_sample(signals, name)
    test_3_rolling_quarterly(signals, name)
    test_4_market_regime(signals, name)
    test_5_statistical_significance(signals, name)
    test_6_overfit_check(signals, name, n_shuffles=n_shuffles)
    test_7_with_costs(signals, name)


def parse_args():
    parser = argparse.ArgumentParser(description="系统有效性验证")
    parser.add_argument("--quick", action="store_true", help="快速模式：仅抽样少量A股/美股做冒烟验证")
    parser.add_argument("--shuffles", type=int, default=100, help="过拟合检测的随机次数")
    return parser.parse_args()


def main():
    args = parse_args()
    a_universe = STOCKS
    us_universe = US_STOCKS
    if args.quick:
        # 保持覆盖面，但把执行时间控制到适合作为日常回归
        a_universe = dict(list(STOCKS.items())[:2])
        us_universe = dict(list(US_STOCKS.items())[:2])

    print(f"\n{'#' * 60}")
    print(f"  系统有效性验证报告  {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    if args.quick:
        print(f"  快速模式 · 7项测试 x 4只股票 (2只A股 + 2只美股)")
    else:
        print(f"  7项测试 x 11只股票 (5只A股 + 6只美股)")
    print(f"{'#' * 60}")

    # A股
    print(f"\n{'=' * 60}")
    print(f"  第一部分: A股")
    print(f"{'=' * 60}")

    a_fund = {}
    for code, name in a_universe.items():
        try:
            a_fund[code] = fetch_financial(code)
        except:
            pass
        time.sleep(0.3)

    for code, name in a_universe.items():
        try:
            df = fetch_sina_history(code, 1500)
            signals = generate_signals(df, score_trend, a_fund.get(code))
            if len(signals) > 100:
                run_full_test(f"A股-{name}({code})", signals, n_shuffles=args.shuffles)
        except Exception as e:
            print(f"  [!] {name} 失败: {e}")
        time.sleep(0.3)

    # 美股
    print(f"\n{'=' * 60}")
    print(f"  第二部分: 美股")
    print(f"{'=' * 60}")

    us_fund = {}
    try:
        us_fund = fetch_us_financials()
    except:
        pass

    for ticker, name in us_universe.items():
        try:
            df = fetch_us_history(ticker, "10y")
            signals = generate_signals(df, score_trend_us, us_fund.get(ticker))
            if len(signals) > 100:
                run_full_test(f"美股-{name}({ticker})", signals, n_shuffles=args.shuffles)
        except Exception as e:
            print(f"  [!] {name} 失败: {e}")
        time.sleep(0.3)

    # 总结
    print(f"\n{'#' * 60}")
    print(f"  验证完成")
    print(f"  判定标准:")
    print(f"  - 测试1: 看涨组应高于买入持有; 看跌组应低于买入持有")
    print(f"  - 测试2: 样本外效果应接近样本内 (否则过拟合)")
    print(f"  - 测试3: 季度胜率 >60% 为合格")
    print(f"  - 测试4: 至少在2种环境下有区分度")
    print(f"  - 测试5: p值 <0.05 为统计显著")
    print(f"  - 测试6: 真实IC应在随机的90%以上")
    print(f"  - 测试7: 扣成本后仍盈利")
    print(f"{'#' * 60}\n")


if __name__ == "__main__":
    main()
