"""行业维度分形谱调研 — 跨行业 asym 稳定性检验

目的: 验证 A 股分形非对称 (h(-4) > h(+4)) 是否为跨行业普适现象,
      还是某些行业更显著. 覆盖 6 个典型行业 x ~20 只.

流程:
  1. 用 pywencai 分别拉 6 个行业代表股
  2. 对每只取最新 300 天历史, 在最新 120 日 log_ret 窗口跑 MF-DFA
  3. 分组统计各行业 delta_alpha/alpha0/hq2/asym 均值与 std
  4. 关键检验: 各行业 asym 均值差异, 行业内一致性, 组内 vs 跨行业 std

运行: python industry_fractal.py
"""

import time
import numpy as np
import pandas as pd

from fetch_data import fetch_history
from fetch_wencai import get_stock_pool
from fractal_survey import mfdfa_spectrum, WINDOW


INDUSTRIES = [
    ("白酒",       "白酒行业股票"),
    ("银行",       "银行股票"),
    ("新能源电池", "动力电池概念股"),
    ("半导体",     "半导体芯片股票"),
    ("创新药",     "创新药概念股票"),
    ("房地产",     "房地产行业股票"),
]

MAX_PER_INDUSTRY = 20
HISTORY_DAYS = 300


def collect_industry(name: str, prompt: str, max_n: int = MAX_PER_INDUSTRY) -> pd.DataFrame:
    """拉一个行业的股票池, 计算每只的 MF-DFA 谱."""
    print(f"\n[行业] {name}  query={prompt!r}")
    try:
        pool = get_stock_pool(prompt)
    except Exception as e:
        print(f"  问财拉取失败: {e}")
        return pd.DataFrame()
    print(f"  池大小={len(pool)}, 取前 {min(max_n, len(pool))}")
    pool = pool[:max_n]

    rows = []
    fails = 0
    for i, code in enumerate(pool, 1):
        try:
            df = fetch_history(code, days=HISTORY_DAYS)
            if df is None or len(df) < WINDOW + 10:
                fails += 1
                time.sleep(0.15)
                continue
            log_ret = np.log(df["close"] / df["close"].shift(1))
            window = log_ret.iloc[-WINDOW:]
            spec = mfdfa_spectrum(window)
            if not spec:
                fails += 1
                time.sleep(0.15)
                continue
            spec["code"] = code
            spec["industry"] = name
            rows.append(spec)
        except Exception:
            fails += 1
        time.sleep(0.15)
        if i % 5 == 0 or i == len(pool):
            print(f"    {i}/{len(pool)} 完成 (失败 {fails})")
    return pd.DataFrame(rows)


def industry_stats(results: pd.DataFrame) -> pd.DataFrame:
    """按行业聚合基础统计."""
    metrics = ["delta_alpha", "alpha0", "hq2", "asym"]
    agg = results.groupby("industry")[metrics].agg(["mean", "std", "count"])
    return agg


def asym_sign_ratio(results: pd.DataFrame) -> pd.DataFrame:
    """各行业 asym > 0 占比 / asym > 0.1 显著占比."""
    def _row(g: pd.DataFrame) -> pd.Series:
        n = len(g)
        return pd.Series({
            "n": n,
            "asym_mean": g["asym"].mean(),
            "asym_std": g["asym"].std(),
            "asym>0_pct": (g["asym"] > 0).sum() / n * 100.0 if n else np.nan,
            "asym>0.1_pct": (g["asym"] > 0.1).sum() / n * 100.0 if n else np.nan,
        })
    return results.groupby("industry").apply(_row).round(3)


def one_way_anova_like(results: pd.DataFrame, metric: str = "asym") -> dict:
    """简单 F 检验: 组间 / 组内 方差比.

    F = (between-group MS) / (within-group MS)
    大的 F 表示行业间差异显著.
    """
    groups = [g[metric].values for _, g in results.groupby("industry")]
    groups = [g for g in groups if len(g) >= 2]
    all_vals = np.concatenate(groups)
    grand_mean = all_vals.mean()
    k = len(groups)
    n_total = len(all_vals)
    ss_between = sum(len(g) * (g.mean() - grand_mean) ** 2 for g in groups)
    ss_within = sum(((g - g.mean()) ** 2).sum() for g in groups)
    df_between = k - 1
    df_within = n_total - k
    ms_between = ss_between / df_between if df_between > 0 else np.nan
    ms_within = ss_within / df_within if df_within > 0 else np.nan
    f_stat = ms_between / ms_within if ms_within and ms_within > 0 else np.nan
    return {
        "metric": metric,
        "k_groups": k,
        "n_total": n_total,
        "ms_between": ms_between,
        "ms_within": ms_within,
        "F": f_stat,
        "grand_mean": grand_mean,
        "grand_std": all_vals.std(ddof=1),
    }


def report(results: pd.DataFrame) -> None:
    if results.empty:
        print("无有效数据.")
        return

    print()
    print("=" * 78)
    print(f"  行业分形谱调研报告  (总样本 n={len(results)})")
    print("=" * 78)

    # 1. 行业覆盖
    ind_counts = results["industry"].value_counts()
    print("\n[1] 行业覆盖:")
    for ind, cnt in ind_counts.items():
        print(f"    {ind:<10s}  n={cnt}")

    # 2. 各行业谱特征均值/std
    print("\n[2] 各行业谱特征 (mean / std):")
    stats = industry_stats(results)
    print(stats.to_string(float_format=lambda x: f"{x:.3f}"))

    # 3. asym 专项: 均值排序 + 正向占比
    print("\n[3] asym 分行业关键指标 (按均值降序):")
    asy = asym_sign_ratio(results).sort_values("asym_mean", ascending=False)
    print(asy.to_string(float_format=lambda x: f"{x:.3f}"))

    # 4. 组间 / 组内方差对比 (F-like)
    print("\n[4] 跨行业差异检验 (F-like):")
    for metric in ["asym", "delta_alpha", "alpha0", "hq2"]:
        r = one_way_anova_like(results, metric)
        if not np.isnan(r["F"]):
            print(
                f"    {metric:<11s}  F={r['F']:.2f}  "
                f"MS_between={r['ms_between']:.5f}  MS_within={r['ms_within']:.5f}  "
                f"grand_mean={r['grand_mean']:+.3f}"
            )

    # 5. 组内一致性: 均值行业内 std vs 跨行业 std
    print("\n[5] 组内一致性 (asym):")
    within_std_mean = results.groupby("industry")["asym"].std().mean()
    cross_std = results["asym"].std()
    industry_means = results.groupby("industry")["asym"].mean()
    between_std = industry_means.std()
    print(f"    行业内 std 均值  = {within_std_mean:.3f}")
    print(f"    行业均值 之 std  = {between_std:.3f}  (跨行业离散)")
    print(f"    全体 asym std     = {cross_std:.3f}")
    if within_std_mean < cross_std:
        print(f"    => 行业内比全体更一致 (within={within_std_mean:.3f} < total={cross_std:.3f})")
    else:
        print(f"    => 行业内未显著更一致")

    # 6. 最强/最弱
    top = asy.iloc[0]
    bot = asy.iloc[-1]
    print("\n[6] 极值行业:")
    print(f"    非对称最强: {top.name}  asym均值={top['asym_mean']:+.3f}  "
          f"asym>0占比={top['asym>0_pct']:.1f}%")
    print(f"    非对称最弱: {bot.name}  asym均值={bot['asym_mean']:+.3f}  "
          f"asym>0占比={bot['asym>0_pct']:.1f}%")


if __name__ == "__main__":
    all_rows = []
    t0 = time.time()
    for name, prompt in INDUSTRIES:
        df = collect_industry(name, prompt, max_n=MAX_PER_INDUSTRY)
        if not df.empty:
            all_rows.append(df)

    if not all_rows:
        print("所有行业均无数据.")
        raise SystemExit(1)

    results = pd.concat(all_rows, ignore_index=True)
    out_path = "industry_fractal_results.csv"
    results.to_csv(out_path, index=False)
    print(f"\n原始明细已保存: {out_path}  ({len(results)} rows)")

    report(results)
    print(f"\n耗时: {time.time() - t0:.1f}s")
