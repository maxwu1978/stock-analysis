"""分形非对称分组信号强度对比

假设: asym = h(q=-4) - h(q=+4) > 0.1 的股票(分形非对称强)上,
      传统动量/反转因子的IC/RankIC应系统性强于 asym <= 0.1 的股票.

流程:
  1. 沪深300 抽 60 只 (pywencai)
  2. 每只拉 600 天, 最新一期 MF-DFA 谱 -> asym
  3. 每只 compute_all 获得技术因子日面板 + 未来5/30日收益
  4. 按 asym 分组 (A: asym>0.1, B: asym<=0.1)
  5. 在每组内池化所有 (date,code) 样本, 算 5 因子 IC 和 RankIC:
       ROC20 -> 未来5日/30日
       RSI6  -> 未来5日
       ma20_diff -> 未来30日
       amihud_20d -> 未来30日
       autocorr -> 未来5日
  6. 核心对比: Group A 的 |RankIC| 平均是否比 B 高 10% 以上
  7. 结果保存 fractal_strategy_results.csv

运行: python fractal_strategy.py
"""

import time
import random
import numpy as np
import pandas as pd
from scipy.stats import pearsonr, spearmanr

from fetch_data import fetch_history
from fetch_wencai import get_stock_pool
from fractal_survey import mfdfa_spectrum
from indicators import compute_all


N_SAMPLE = 60
HISTORY_DAYS = 600
WINDOW = 120          # MF-DFA 窗口
ASYM_THR = 0.1        # 分组阈值
HORIZONS = {5: "ret_fwd_5", 30: "ret_fwd_30"}

# 因子 -> 预测目标
FACTOR_SPECS = [
    ("ROC20", 5),
    ("ROC20", 30),
    ("RSI6", 5),
    ("ma20_diff", 30),
    ("amihud_20d", 30),
    ("autocorr", 5),
]


def build_panel(code: str) -> tuple[pd.DataFrame | None, float | None]:
    """返回 (日面板含因子+未来收益, asym). 失败返回 (None, None)."""
    try:
        df = fetch_history(code, days=HISTORY_DAYS)
    except Exception:
        return None, None
    if df is None or len(df) < WINDOW + 60:
        return None, None

    # MF-DFA 基于最近 WINDOW 天, 作为该股的"分形状态"标签
    log_ret = np.log(df["close"] / df["close"].shift(1))
    spec = mfdfa_spectrum(log_ret.iloc[-WINDOW:])
    if not spec:
        return None, None
    asym = spec["asym"]

    # 全量因子
    try:
        df = compute_all(df.copy())
    except Exception:
        return None, None

    # ma20_diff 手动补算 (compute_all 未包含)
    if "MA20" in df.columns:
        df["ma20_diff"] = (df["close"] / df["MA20"] - 1) * 100

    # 未来收益
    for h, col in HORIZONS.items():
        df[col] = df["close"].shift(-h) / df["close"] - 1

    df["code"] = code
    return df, asym


def compute_ic(values: pd.Series, forward_ret: pd.Series) -> tuple[int, float, float]:
    """返回 (n, pearson_ic, spearman_rankic)."""
    s = pd.concat([values, forward_ret], axis=1).dropna()
    if len(s) < 30:
        return 0, np.nan, np.nan
    x = s.iloc[:, 0].values
    y = s.iloc[:, 1].values
    if np.std(x) == 0 or np.std(y) == 0:
        return len(s), np.nan, np.nan
    ic, _ = pearsonr(x, y)
    rankic, _ = spearmanr(x, y)
    return len(s), ic, rankic


def main():
    random.seed(42)

    print(f"[1/4] 拉取沪深300成分股 (max_n={N_SAMPLE}).")
    pool = get_stock_pool("沪深300成分股", max_n=N_SAMPLE)
    print(f"  池大小: {len(pool)}")

    print(f"[2/4] 对每只股票算 MF-DFA 谱 + compute_all 因子 ({HISTORY_DAYS}天).")
    panels: dict[str, pd.DataFrame] = {}
    asyms: dict[str, float] = {}
    failures = []

    for i, code in enumerate(pool, 1):
        df_p, asym = build_panel(code)
        if df_p is None:
            failures.append(code)
        else:
            panels[code] = df_p
            asyms[code] = asym
        if i % 10 == 0 or i == len(pool):
            print(f"  {i}/{len(pool)} 成功 {len(panels)} 失败 {len(failures)}")
        time.sleep(0.12)

    if not panels:
        print("没有任何股票成功, 退出.")
        return

    print(f"  最终成功 {len(panels)} / {len(pool)}")

    # 分组
    group_a = [c for c, a in asyms.items() if a > ASYM_THR]
    group_b = [c for c, a in asyms.items() if a <= ASYM_THR]
    print(f"[3/4] 分组: Group A (asym>{ASYM_THR}) = {len(group_a)}只,"
          f" Group B (asym<={ASYM_THR}) = {len(group_b)}只")

    def pool_group(codes: list[str]) -> pd.DataFrame:
        if not codes:
            return pd.DataFrame()
        return pd.concat([panels[c] for c in codes], axis=0, ignore_index=True)

    pool_a = pool_group(group_a)
    pool_b = pool_group(group_b)
    print(f"  Group A 行数 {len(pool_a)}, Group B 行数 {len(pool_b)}")

    print(f"[4/4] 计算因子 IC/RankIC.")
    rows = []
    for factor, horizon in FACTOR_SPECS:
        ret_col = HORIZONS[horizon]
        for gname, g_df in [("A_asym_strong", pool_a), ("B_asym_weak", pool_b)]:
            if g_df.empty or factor not in g_df.columns:
                rows.append({"factor": factor, "horizon": horizon, "group": gname,
                             "n": 0, "ic": np.nan, "rankic": np.nan})
                continue
            n, ic, rankic = compute_ic(g_df[factor], g_df[ret_col])
            rows.append({"factor": factor, "horizon": horizon, "group": gname,
                         "n": n, "ic": ic, "rankic": rankic})

    result = pd.DataFrame(rows)

    # 对比汇总: 每个 factor+horizon 一行, A/B 的 |rankic| 和差值%
    pivot = (result
             .pivot_table(index=["factor", "horizon"], columns="group",
                          values="rankic", aggfunc="first")
             .reset_index())
    pivot["abs_rankic_A"] = pivot["A_asym_strong"].abs()
    pivot["abs_rankic_B"] = pivot["B_asym_weak"].abs()
    pivot["delta_pct"] = (pivot["abs_rankic_A"] - pivot["abs_rankic_B"]) / pivot["abs_rankic_B"].replace(0, np.nan) * 100

    print()
    print("=" * 72)
    print("  分组 RankIC 对比 (A=asym>0.1, B=asym<=0.1)")
    print("=" * 72)
    show = pivot[["factor", "horizon", "abs_rankic_A", "abs_rankic_B", "delta_pct"]]
    print(show.to_string(index=False,
                        float_format=lambda x: f"{x:+.4f}" if abs(x) < 100 else f"{x:+.1f}"))
    print()
    mean_a = pivot["abs_rankic_A"].mean()
    mean_b = pivot["abs_rankic_B"].mean()
    avg_delta = (mean_a - mean_b) / mean_b * 100 if mean_b else np.nan
    print(f"  |RankIC| 平均: A={mean_a:.4f} vs B={mean_b:.4f},"
          f" 相对差 {avg_delta:+.1f}%")

    # 保存
    out_path = "fractal_strategy_results.csv"
    result.to_csv(out_path, index=False)
    pivot_path = "fractal_strategy_pivot.csv"
    pivot.to_csv(pivot_path, index=False)
    print(f"\n明细: {out_path}  汇总: {pivot_path}")


if __name__ == "__main__":
    main()
