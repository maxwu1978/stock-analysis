"""分形结构稳健性调研 — 大样本 MF-DFA 谱验证

目的: 验证之前5只A股的分形结论 (h(-4)>h(+4) 非对称) 在更大样本上是否稳健.

流程:
  1. 用 pywencai 拉沪深300成分股
  2. 抽样 N 只 (默认50)
  3. 对每只取最新300天历史
  4. 计算最新一期 MF-DFA 谱 (Δα/α_0/h(q=2)/h(q=-4)/h(q=+4)/asym)
  5. 横截面统计 + 非对称稳健性检验

运行: python fractal_survey.py
"""

import time
import random
import numpy as np
import pandas as pd

from fetch_data import fetch_history
from fetch_wencai import get_stock_pool


Q_LIST = np.array([-4, -2, 2, 4])
SUB_LENS = [10, 20, 30, 40]
WINDOW = 120


def mfdfa_spectrum(log_ret_window: pd.Series) -> dict:
    """在长度=WINDOW的对数收益窗口上计算完整MF-DFA谱特征."""
    x = log_ret_window.dropna()
    if len(x) < 50:
        return {}
    y = (x - x.mean()).cumsum().values
    h_q = []
    for q in Q_LIST:
        log_F = []
        for n in SUB_LENS:
            if len(y) < 2 * n:
                continue
            num_seg = len(y) // n
            segs = y[: num_seg * n].reshape(num_seg, n)
            t = np.arange(n)
            F2 = []
            for seg in segs:
                coef = np.polyfit(t, seg, 1)
                trend = np.polyval(coef, t)
                resid = seg - trend
                F2.append(np.mean(resid ** 2))
            F2 = np.array(F2)
            if q == 0:
                F_q = np.exp(0.5 * np.mean(np.log(F2 + 1e-12)))
            else:
                F_q = np.mean(F2 ** (q / 2.0)) ** (1.0 / q)
            if F_q > 0:
                log_F.append((np.log(n), np.log(F_q)))
        if len(log_F) < 2:
            return {}
        xs = np.array([p[0] for p in log_F])
        ys = np.array([p[1] for p in log_F])
        h_q.append(np.polyfit(xs, ys, 1)[0])
    h_q = np.array(h_q)
    # Legendre α(q)
    alphas = []
    for i, q in enumerate(Q_LIST):
        if i == 0:
            dh = (h_q[1] - h_q[0]) / (Q_LIST[1] - Q_LIST[0])
        elif i == len(Q_LIST) - 1:
            dh = (h_q[-1] - h_q[-2]) / (Q_LIST[-1] - Q_LIST[-2])
        else:
            dh = (h_q[i + 1] - h_q[i - 1]) / (Q_LIST[i + 1] - Q_LIST[i - 1])
        alphas.append(h_q[i] + q * dh)
    alphas = np.array(alphas)
    a_max, a_min = alphas.max(), alphas.min()
    return {
        "delta_alpha": a_max - a_min,
        "alpha0": (a_max + a_min) / 2.0,
        "hq2": h_q[2],      # q=2
        "hq_neg4": h_q[0],  # q=-4
        "hq_pos4": h_q[3],  # q=+4
        "asym": h_q[0] - h_q[3],  # h(-4) - h(+4), 正=小波动段更持续
    }


def survey(n_sample: int = 50, history_days: int = 300, seed: int = 42,
           pool_query: str = "沪深300成分股") -> pd.DataFrame:
    """对N只抽样股票做MF-DFA谱调研."""
    random.seed(seed)

    print(f"[1/3] 从问财获取股票池: {pool_query!r}")
    pool = get_stock_pool(pool_query)
    print(f"  池大小: {len(pool)}")

    if len(pool) > n_sample:
        pool = random.sample(pool, n_sample)
    print(f"  抽样: {len(pool)} 只")

    rows = []
    failures = []
    print(f"[2/3] 逐只拉历史+算MF-DFA谱 (每只~1-2秒)...")
    for i, code in enumerate(pool, 1):
        try:
            df = fetch_history(code, days=history_days)
            if df is None or len(df) < WINDOW + 10:
                failures.append((code, "数据不足"))
                continue
            log_ret = np.log(df["close"] / df["close"].shift(1))
            window = log_ret.iloc[-WINDOW:]
            spec = mfdfa_spectrum(window)
            if not spec:
                failures.append((code, "MF-DFA失败"))
                continue
            spec["code"] = code
            rows.append(spec)
            if i % 10 == 0 or i == len(pool):
                print(f"  {i}/{len(pool)} done  (失败{len(failures)})")
        except Exception as e:
            failures.append((code, str(e)[:40]))
        time.sleep(0.15)  # 腾讯接口限频

    print(f"[3/3] 完成. 成功{len(rows)}, 失败{len(failures)}")
    if failures and len(failures) <= 5:
        for c, err in failures:
            print(f"  失败 {c}: {err}")
    return pd.DataFrame(rows)


def report(results: pd.DataFrame) -> None:
    """打印横截面统计报告."""
    if results.empty:
        print("无数据.")
        return

    n = len(results)
    print()
    print("═" * 72)
    print(f"  MF-DFA 谱横截面调研 (n={n})")
    print("═" * 72)
    print()

    # 基础统计
    stats = results[["delta_alpha", "alpha0", "hq2", "hq_neg4", "hq_pos4", "asym"]].describe().T
    stats = stats[["mean", "std", "min", "25%", "50%", "75%", "max"]]
    print("  特征分布:")
    print(stats.to_string(float_format=lambda x: f"{x:.3f}"))
    print()

    # 核心结论验证
    print("  分形非对称稳健性检验:")
    print(f"    h(q=-4) 均值: {results['hq_neg4'].mean():.3f} (5只样本: 0.68)")
    print(f"    h(q=+4) 均值: {results['hq_pos4'].mean():.3f} (5只样本: 0.40)")
    print(f"    asym = h(-4) - h(+4) 均值: {results['asym'].mean():+.3f} (5只样本: +0.20~+0.39)")
    print()

    asym_pos = (results["asym"] > 0).sum()
    asym_strong = (results["asym"] > 0.1).sum()
    print(f"    asym > 0 股票占比: {asym_pos}/{n} = {asym_pos / n * 100:.1f}%")
    print(f"    asym > 0.1 (显著): {asym_strong}/{n} = {asym_strong / n * 100:.1f}%")
    print()

    # 持续性 vs 反持续性比例
    hq2_persist = (results["hq2"] > 0.55).sum()
    hq2_revert = (results["hq2"] < 0.45).sum()
    print("  h(q=2) 分布 (持续/反持续):")
    print(f"    H>0.55 持续性 : {hq2_persist}/{n} = {hq2_persist / n * 100:.1f}%")
    print(f"    H<0.45 反持续 : {hq2_revert}/{n} = {hq2_revert / n * 100:.1f}%")
    print(f"    0.45-0.55 随机 : {n - hq2_persist - hq2_revert}/{n}")
    print()

    # Δα vs α_0 的关系
    corr = results[["delta_alpha", "alpha0", "hq2", "asym"]].corr()
    print("  特征相关性矩阵:")
    print(corr.to_string(float_format=lambda x: f"{x:+.2f}"))
    print()


if __name__ == "__main__":
    df = survey(n_sample=50)
    report(df)
    out_path = "fractal_survey_results.csv"
    df.to_csv(out_path, index=False)
    print(f"详细结果已保存: {out_path}")
