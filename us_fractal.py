"""美股分形结构调研 — 验证 MF-DFA 非对称是否为 A 股独有特征

背景:
  A 股沪深300 抽样 50 只的结论:
    h(q=-4) 均值 0.683, h(q=+4) 均值 0.461, asym = +0.222
    asym > 0 占比 92%, asym > 0.1 显著占比 76%
  即"小波动段强持续 + 大波动段反持续" 是 A 股的横截面普遍规律.

本脚本在美股大样本上做同样计算, 回答:
  1. 美股是否也有 h(-4) > h(+4)?
  2. 强度是否和 A 股类似?
  3. 中概股 (BABA/PDD/TCOM) 的分形特征接近美股还是 A 股?

运行: venv/bin/python us_fractal.py
"""

import time
import numpy as np
import pandas as pd

from fetch_us import fetch_us_history
from fractal_survey import mfdfa_spectrum, WINDOW


# 跨行业 + 跨市值 的美股样本池
US_POOL = {
    # 大型科技 (7)
    "AAPL": ("苹果", "科技", "US"),
    "MSFT": ("微软", "科技", "US"),
    "GOOGL": ("谷歌", "科技", "US"),
    "NVDA": ("英伟达", "科技", "US"),
    "META": ("Meta", "科技", "US"),
    "TSLA": ("特斯拉", "科技", "US"),
    "AMZN": ("亚马逊", "科技", "US"),
    # 金融 (5)
    "JPM": ("摩根大通", "金融", "US"),
    "BAC": ("美银", "金融", "US"),
    "GS": ("高盛", "金融", "US"),
    "MS": ("摩根士丹利", "金融", "US"),
    "WFC": ("富国", "金融", "US"),
    # 医疗 (4)
    "JNJ": ("强生", "医疗", "US"),
    "UNH": ("联合健康", "医疗", "US"),
    "PFE": ("辉瑞", "医疗", "US"),
    "LLY": ("礼来", "医疗", "US"),
    # 消费 (5)
    "KO":  ("可口可乐", "消费", "US"),
    "PG":  ("宝洁", "消费", "US"),
    "WMT": ("沃尔玛", "消费", "US"),
    "COST": ("Costco", "消费", "US"),
    "NKE": ("耐克", "消费", "US"),
    # 工业/能源 (4)
    "CAT": ("卡特彼勒", "工业", "US"),
    "BA":  ("波音", "工业", "US"),
    "XOM": ("埃克森", "能源", "US"),
    "CVX": ("雪佛龙", "能源", "US"),
    # 电信/公共事业 (3)
    "T":   ("AT&T", "电信", "US"),
    "VZ":  ("Verizon", "电信", "US"),
    "DUK": ("杜克能源", "公用事业", "US"),
    # 中概股 (3) — 关键对比组
    "BABA": ("阿里巴巴", "中概股", "CN_ADR"),
    "PDD":  ("拼多多", "中概股", "CN_ADR"),
    "TCOM": ("携程", "中概股", "CN_ADR"),
}


def run_us_survey(history_period: str = "2y") -> pd.DataFrame:
    """对美股池逐只拉历史 + 算 MF-DFA 谱."""
    rows = []
    failures = []
    tickers = list(US_POOL.keys())
    print(f"[1/2] 美股样本池: {len(tickers)} 只")

    for i, ticker in enumerate(tickers, 1):
        name, sector, region = US_POOL[ticker]
        try:
            df = fetch_us_history(ticker, period=history_period)
            if df is None or len(df) < WINDOW + 10:
                failures.append((ticker, f"数据不足({0 if df is None else len(df)}天)"))
                time.sleep(0.3)
                continue
            log_ret = np.log(df["close"] / df["close"].shift(1))
            window = log_ret.iloc[-WINDOW:]
            spec = mfdfa_spectrum(window)
            if not spec:
                failures.append((ticker, "MF-DFA失败"))
                time.sleep(0.3)
                continue
            spec["ticker"] = ticker
            spec["name"] = name
            spec["sector"] = sector
            spec["region"] = region
            rows.append(spec)
            print(f"  {i:2d}/{len(tickers)} {ticker:6s} {name:10s} "
                  f"h(-4)={spec['hq_neg4']:.3f} h(+4)={spec['hq_pos4']:.3f} "
                  f"asym={spec['asym']:+.3f}")
        except Exception as e:
            failures.append((ticker, str(e)[:60]))
            print(f"  {i:2d}/{len(tickers)} {ticker:6s} 失败: {str(e)[:50]}")
        time.sleep(0.3)

    print(f"\n[2/2] 完成. 成功{len(rows)}, 失败{len(failures)}")
    for c, err in failures:
        print(f"  失败 {c}: {err}")
    return pd.DataFrame(rows)


# A 股基准 (来自 fractal_survey 的 50 只沪深300 抽样)
CN_BENCHMARK = {
    "hq_neg4_mean": 0.683,
    "hq_pos4_mean": 0.461,
    "asym_mean":    0.222,
    "asym_pos_pct": 92.0,
    "asym_strong_pct": 76.0,
}


def report(results: pd.DataFrame) -> None:
    if results.empty:
        print("无结果.")
        return

    n = len(results)
    print()
    print("=" * 78)
    print(f"  美股 MF-DFA 谱横截面 (n={n})  vs  A 股基准 (n=50 沪深300 抽样)")
    print("=" * 78)

    # ==== 全样本 ====
    hq_neg4_us = results["hq_neg4"].mean()
    hq_pos4_us = results["hq_pos4"].mean()
    asym_us    = results["asym"].mean()
    asym_pos   = (results["asym"] > 0).sum()
    asym_str   = (results["asym"] > 0.1).sum()
    asym_pos_pct = asym_pos / n * 100
    asym_str_pct = asym_str / n * 100

    print()
    print(f"  [全样本 US n={n}]")
    print(f"  {'指标':<30} {'A股':>10} {'美股':>10} {'差异':>10}")
    print(f"  {'-'*62}")
    print(f"  {'h(q=-4) 均值':<30} {CN_BENCHMARK['hq_neg4_mean']:>10.3f} "
          f"{hq_neg4_us:>10.3f} {hq_neg4_us - CN_BENCHMARK['hq_neg4_mean']:>+10.3f}")
    print(f"  {'h(q=+4) 均值':<30} {CN_BENCHMARK['hq_pos4_mean']:>10.3f} "
          f"{hq_pos4_us:>10.3f} {hq_pos4_us - CN_BENCHMARK['hq_pos4_mean']:>+10.3f}")
    print(f"  {'asym=h(-4)-h(+4) 均值':<30} {CN_BENCHMARK['asym_mean']:>+10.3f} "
          f"{asym_us:>+10.3f} {asym_us - CN_BENCHMARK['asym_mean']:>+10.3f}")
    print(f"  {'asym > 0 占比 %':<30} {CN_BENCHMARK['asym_pos_pct']:>10.1f} "
          f"{asym_pos_pct:>10.1f} {asym_pos_pct - CN_BENCHMARK['asym_pos_pct']:>+10.1f}")
    print(f"  {'asym > 0.1 显著占比 %':<30} {CN_BENCHMARK['asym_strong_pct']:>10.1f} "
          f"{asym_str_pct:>10.1f} {asym_str_pct - CN_BENCHMARK['asym_strong_pct']:>+10.1f}")

    # ==== 分组 (美股本土 vs 中概股) ====
    us_native = results[results["region"] == "US"]
    us_cn_adr = results[results["region"] == "CN_ADR"]

    print()
    print(f"  [分组] 美股本土 n={len(us_native)}  |  中概股 ADR n={len(us_cn_adr)}")
    print(f"  {'指标':<30} {'美本土':>10} {'中概股':>10} {'A股基准':>10}")
    print(f"  {'-'*62}")

    def _fmt(series, pos_thresh=None):
        if len(series) == 0:
            return float("nan")
        if pos_thresh is None:
            return series.mean()
        return (series > pos_thresh).sum() / len(series) * 100

    print(f"  {'h(q=-4) 均值':<30} "
          f"{_fmt(us_native['hq_neg4']):>10.3f} "
          f"{_fmt(us_cn_adr['hq_neg4']):>10.3f} "
          f"{CN_BENCHMARK['hq_neg4_mean']:>10.3f}")
    print(f"  {'h(q=+4) 均值':<30} "
          f"{_fmt(us_native['hq_pos4']):>10.3f} "
          f"{_fmt(us_cn_adr['hq_pos4']):>10.3f} "
          f"{CN_BENCHMARK['hq_pos4_mean']:>10.3f}")
    print(f"  {'asym 均值':<30} "
          f"{_fmt(us_native['asym']):>+10.3f} "
          f"{_fmt(us_cn_adr['asym']):>+10.3f} "
          f"{CN_BENCHMARK['asym_mean']:>+10.3f}")
    print(f"  {'asym > 0 占比 %':<30} "
          f"{_fmt(us_native['asym'], 0):>10.1f} "
          f"{_fmt(us_cn_adr['asym'], 0):>10.1f} "
          f"{CN_BENCHMARK['asym_pos_pct']:>10.1f}")
    print(f"  {'asym > 0.1 显著占比 %':<30} "
          f"{_fmt(us_native['asym'], 0.1):>10.1f} "
          f"{_fmt(us_cn_adr['asym'], 0.1):>10.1f} "
          f"{CN_BENCHMARK['asym_strong_pct']:>10.1f}")

    # ==== 按行业 ====
    print()
    print("  [按行业] asym 均值 / asym>0 占比")
    print(f"  {'行业':<15} {'n':>4} {'asym均值':>12} {'h(-4)':>10} {'h(+4)':>10} {'asym>0 %':>10}")
    print(f"  {'-'*70}")
    for sector, g in results.groupby("sector"):
        print(f"  {sector:<15} {len(g):>4} {g['asym'].mean():>+12.3f} "
              f"{g['hq_neg4'].mean():>10.3f} {g['hq_pos4'].mean():>10.3f} "
              f"{(g['asym']>0).sum()/len(g)*100:>10.1f}")

    # ==== 逐只 Top/Bottom ====
    print()
    print("  [Top5 asym 最高]")
    top = results.nlargest(5, "asym")[["ticker", "name", "sector", "hq_neg4", "hq_pos4", "asym"]]
    print(top.to_string(index=False, float_format=lambda x: f"{x:.3f}"))

    print()
    print("  [Bottom5 asym 最低 (负值=反 A 股模式)]")
    bot = results.nsmallest(5, "asym")[["ticker", "name", "sector", "hq_neg4", "hq_pos4", "asym"]]
    print(bot.to_string(index=False, float_format=lambda x: f"{x:.3f}"))


if __name__ == "__main__":
    df = run_us_survey(history_period="2y")
    report(df)
    out_path = "us_fractal_results.csv"
    df.to_csv(out_path, index=False)
    print(f"\n详细结果已保存: {out_path}")
