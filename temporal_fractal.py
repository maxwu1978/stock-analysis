"""分形非对称时序演化调研 — 滚动窗口 MF-DFA 谱特征

对 5 只主力股拉 1200 天历史, 用 120 日滚动窗口 + 步长=10 计算
MF-DFA 谱, 观察 asym = h(-4) - h(+4) 在牛/熊/震荡周期中是否翻转方向.

输出:
  1) 打印每只股票时序统计 + 三段时期对比
  2) CSV: temporal_fractal_results.csv
"""
from __future__ import annotations

import time
from pathlib import Path

import numpy as np
import pandas as pd

from fetch_data import fetch_history
from fractal_survey import mfdfa_spectrum, WINDOW  # 复用 (WINDOW=120)


STOCKS = {
    "300750": "宁德时代",
    "600519": "贵州茅台",
    "601600": "中国铝业",
    "300274": "阳光电源",
    "600745": "闻泰科技",
}

HISTORY_DAYS = 1200
STEP = 10  # 每 10 天一个样本点, 控制计算量


def rolling_mfdfa(df: pd.DataFrame, window: int = WINDOW, step: int = STEP) -> pd.DataFrame:
    """对价格序列做滚动 MF-DFA 谱计算.

    参数:
        df: 含 'close' 列, 以 date 为索引的日线 DataFrame
        window: 窗口长度 (默认 120)
        step: 步长 (默认 10)
    返回: 每个窗口末端日期 + 谱特征的 DataFrame
    """
    log_ret = np.log(df["close"] / df["close"].shift(1)).dropna()
    rows = []
    n = len(log_ret)
    for end in range(window, n + 1, step):
        win = log_ret.iloc[end - window : end]
        spec = mfdfa_spectrum(win)
        if not spec:
            continue
        rows.append({
            "date": log_ret.index[end - 1],
            **spec,
        })
    return pd.DataFrame(rows)


def summarize_one(code: str, name: str, series: pd.DataFrame) -> dict:
    """对单只股票的时序谱数据计算统计量."""
    asym = series["asym"].values
    n = len(asym)

    # 三等分时间段
    third = n // 3
    seg_early = asym[:third]
    seg_mid = asym[third : 2 * third]
    seg_late = asym[2 * third :]

    # 翻转天数占比: asym 符号变化次数 / 总样本
    signs = np.sign(asym)
    flips = int((signs[1:] * signs[:-1] < 0).sum())

    return {
        "code": code,
        "name": name,
        "n_windows": n,
        "date_first": series["date"].iloc[0],
        "date_last": series["date"].iloc[-1],
        "asym_mean": float(np.mean(asym)),
        "asym_std": float(np.std(asym, ddof=1)),
        "asym_min": float(np.min(asym)),
        "asym_max": float(np.max(asym)),
        "asym_pos_ratio": float((asym > 0).mean()),
        "asym_early_mean": float(np.mean(seg_early)),
        "asym_mid_mean": float(np.mean(seg_mid)),
        "asym_late_mean": float(np.mean(seg_late)),
        "flip_count": flips,
        "flip_ratio": flips / max(n - 1, 1),
    }


def main() -> None:
    all_rows = []
    summaries = []

    print(f"[1/3] 拉 {len(STOCKS)} 只 x {HISTORY_DAYS} 天历史")
    hist = {}
    for code, name in STOCKS.items():
        try:
            df = fetch_history(code, days=HISTORY_DAYS)
            print(f"  {code} {name}: {len(df)} 根K线 ({df.index.min().date()} -> {df.index.max().date()})")
            hist[code] = df
            time.sleep(0.2)
        except Exception as e:
            print(f"  [!] {code} {name} 拉取失败: {e}")

    print(f"[2/3] 滚动 MF-DFA (window={WINDOW}, step={STEP})")
    for code, name in STOCKS.items():
        if code not in hist:
            continue
        t0 = time.time()
        series = rolling_mfdfa(hist[code])
        dt = time.time() - t0
        if series.empty:
            print(f"  [!] {code} {name}: 无有效谱")
            continue
        series["code"] = code
        series["name"] = name
        print(f"  {code} {name}: {len(series)} 个窗口 ({dt:.1f}s)")
        all_rows.append(series)
        summaries.append(summarize_one(code, name, series))

    if not all_rows:
        print("[!] 所有股票都失败, 退出")
        return

    full = pd.concat(all_rows, ignore_index=True)
    out_cols = ["code", "date", "delta_alpha", "alpha0", "hq2", "hq_neg4", "hq_pos4", "asym"]
    csv_path = Path(__file__).resolve().parent / "temporal_fractal_results.csv"
    full[out_cols].to_csv(csv_path, index=False)
    print(f"[3/3] 时序数据已写入 {csv_path}  (rows={len(full)})")

    # ---------- 报告 ----------
    sdf = pd.DataFrame(summaries)

    print("\n============ 时序 asym 统计 ============")
    print(sdf[[
        "code", "name", "n_windows",
        "asym_mean", "asym_std", "asym_min", "asym_max",
        "asym_pos_ratio",
    ]].to_string(index=False, float_format=lambda x: f"{x:.4f}"))

    print("\n============ 三段时期 asym 均值 ============")
    print("(early/mid/late 各占时间 1/3; mean 下降或翻负 => 非对称方向翻转)")
    print(sdf[[
        "code", "name",
        "asym_early_mean", "asym_mid_mean", "asym_late_mean",
    ]].to_string(index=False, float_format=lambda x: f"{x:+.4f}"))

    print("\n============ 翻转统计 (asym 符号切换) ============")
    print(sdf[["code", "name", "flip_count", "flip_ratio"]].to_string(
        index=False, float_format=lambda x: f"{x:.4f}"))

    # 合计跨股票占比
    overall_pos = float((full["asym"] > 0).mean())
    overall_mean = float(full["asym"].mean())
    print(f"\n[全样本] asym 均值={overall_mean:+.4f}  |  asym>0 占比={overall_pos:.2%}  |  总样本={len(full)}")

    # 找出 asym 显著为负的子时段
    neg_ratio_by_stock = (full.assign(neg=full["asym"] < 0)
                          .groupby("code")["neg"].mean())
    print("\n[各股 asym<0 时段占比]")
    for code, r in neg_ratio_by_stock.items():
        print(f"  {code} {STOCKS.get(code, '')}: {r:.2%}")


if __name__ == "__main__":
    main()
