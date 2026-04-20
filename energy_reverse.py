"""美股能源股反向分形策略调研

背景:
  前轮美股 31 只调研发现大部分股票遵循 "asym>0" 模式
  (h(-4)>h(+4), 小波动段更持续), 但能源板块反向:
    CVX asym=-0.417, XOM -0.112, LLY -0.188, TSLA -0.171
    能源板块 asym 均值 -0.264
  意味着这些股票: 大波动段更持续, 小波动段反转.

任务:
  1. 扩大样本到 10-15 只能源股 + 2-3 只反向参照
  2. 对每只计算:
     - 120 日 MF-DFA 谱 (最新窗口 + 过去3次滚动平均)
     - 最近 1 年 ROC20 vs 未来 30 日收益 RankIC
  3. 按 asym 符号分组, 验证反向模式假说:
     - asym<0 组: RankIC 应为 "正" (高 ROC 后继续涨 = 趋势延续)
     - asym>0 组: RankIC 应为 "负" (高 ROC 后回落 = 均值回归)

运行: venv/bin/python energy_reverse.py
"""

import time
import numpy as np
import pandas as pd
from scipy.stats import spearmanr

from fetch_us import fetch_us_history
from fractal_survey import mfdfa_spectrum, WINDOW


# 10-15 能源股 + 2-3 反向参照
ENERGY_POOL = {
    # 已知反向能源 (2)
    "CVX":  ("雪佛龙", "能源"),
    "XOM":  ("埃克森", "能源"),
    # 扩展能源 (石油/天然气综合)
    "OXY":  ("西方石油", "能源"),
    "COP":  ("康菲石油", "能源"),
    "EOG":  ("EOG", "能源"),
    "PSX":  ("菲利普66", "能源"),
    "MPC":  ("马拉松石油", "能源"),
    "VLO":  ("瓦莱罗", "能源"),
    "HES":  ("赫斯", "能源"),
    "SLB":  ("斯伦贝谢", "能源"),  # 油服
    "HAL":  ("哈里伯顿", "能源"),  # 油服
    "BKR":  ("贝克休斯", "能源"),  # 油服
    # 反向参照 (非能源)
    "TSLA": ("特斯拉", "反向参照"),
    "LLY":  ("礼来",   "反向参照"),
    # 正向参照 (已知 asym>0 美股)
    "AAPL": ("苹果",   "正向参照"),
    "MSFT": ("微软",   "正向参照"),
    "JPM":  ("摩根大通", "正向参照"),
}


def rolling_asym(log_ret: pd.Series, n_windows: int = 4) -> dict:
    """计算最新窗口 + 过去 (n_windows-1) 个滚动窗口的 asym 平均."""
    if len(log_ret) < WINDOW + (n_windows - 1) * 20:
        # 样本不足, 只算最新窗口
        spec = mfdfa_spectrum(log_ret.iloc[-WINDOW:])
        if not spec:
            return {}
        return {
            "asym_latest": spec["asym"],
            "hq_neg4_latest": spec["hq_neg4"],
            "hq_pos4_latest": spec["hq_pos4"],
            "asym_avg": spec["asym"],
            "asym_std": 0.0,
            "n_rolling": 1,
        }

    asyms = []
    hq_neg4s = []
    hq_pos4s = []
    # 每 20 日滚动一次, 最新窗口在最右
    for offset in range(n_windows):
        end = len(log_ret) - offset * 20
        start = end - WINDOW
        if start < 0:
            continue
        spec = mfdfa_spectrum(log_ret.iloc[start:end])
        if spec:
            asyms.append(spec["asym"])
            hq_neg4s.append(spec["hq_neg4"])
            hq_pos4s.append(spec["hq_pos4"])

    if len(asyms) == 0:
        return {}

    return {
        "asym_latest": asyms[0],
        "hq_neg4_latest": hq_neg4s[0],
        "hq_pos4_latest": hq_pos4s[0],
        "asym_avg": float(np.mean(asyms)),
        "asym_std": float(np.std(asyms)),
        "n_rolling": len(asyms),
    }


def compute_rankic(df: pd.DataFrame, roc_win: int = 20, fwd_win: int = 30) -> dict:
    """计算最近 1 年 ROC20 与未来 30 日收益的 Spearman 相关."""
    close = df["close"]
    roc = close.pct_change(roc_win)
    fwd = close.shift(-fwd_win) / close - 1
    # 取最近 252 日 (约 1 年) 的配对样本
    tail = pd.DataFrame({"roc": roc, "fwd": fwd}).dropna()
    if len(tail) < 60:
        return {"rankic_1y": np.nan, "n_pairs": len(tail),
                "rankic_p": np.nan}
    tail = tail.tail(252) if len(tail) > 252 else tail
    if len(tail) < 60:
        return {"rankic_1y": np.nan, "n_pairs": len(tail),
                "rankic_p": np.nan}
    rho, p = spearmanr(tail["roc"], tail["fwd"])
    return {
        "rankic_1y": float(rho) if not np.isnan(rho) else np.nan,
        "rankic_p": float(p) if not np.isnan(p) else np.nan,
        "n_pairs": int(len(tail)),
    }


def run_study(history_period: str = "3y") -> pd.DataFrame:
    """对能源股池逐只: 分形 asym + RankIC."""
    rows = []
    failures = []
    tickers = list(ENERGY_POOL.keys())
    print(f"[1/2] 样本池: {len(tickers)} 只 "
          f"(能源 {sum(1 for v in ENERGY_POOL.values() if v[1]=='能源')}, "
          f"反向参照 {sum(1 for v in ENERGY_POOL.values() if v[1]=='反向参照')}, "
          f"正向参照 {sum(1 for v in ENERGY_POOL.values() if v[1]=='正向参照')})")

    for i, ticker in enumerate(tickers, 1):
        name, group = ENERGY_POOL[ticker]
        try:
            df = fetch_us_history(ticker, period=history_period)
            if df is None or len(df) < WINDOW + 60:
                failures.append((ticker, f"数据不足({0 if df is None else len(df)}天)"))
                time.sleep(0.3)
                continue
            log_ret = np.log(df["close"] / df["close"].shift(1)).dropna()

            asym_info = rolling_asym(log_ret, n_windows=4)
            if not asym_info:
                failures.append((ticker, "MF-DFA失败"))
                time.sleep(0.3)
                continue

            ic_info = compute_rankic(df, roc_win=20, fwd_win=30)

            row = {
                "ticker": ticker,
                "name": name,
                "group": group,
                **asym_info,
                **ic_info,
            }
            rows.append(row)
            print(f"  {i:2d}/{len(tickers)} {ticker:6s} {name:10s} [{group:6s}]"
                  f" asym_latest={asym_info['asym_latest']:+.3f}"
                  f" asym_avg={asym_info['asym_avg']:+.3f}"
                  f" RankIC={ic_info['rankic_1y']:+.3f}"
                  f" (n={ic_info['n_pairs']})")
        except Exception as e:
            failures.append((ticker, str(e)[:60]))
            print(f"  {i:2d}/{len(tickers)} {ticker:6s} 失败: {str(e)[:50]}")
        time.sleep(0.3)

    print(f"\n[2/2] 完成. 成功 {len(rows)}, 失败 {len(failures)}")
    for c, err in failures:
        print(f"  失败 {c}: {err}")
    return pd.DataFrame(rows)


def report(results: pd.DataFrame) -> None:
    if results.empty:
        print("无结果.")
        return

    results = results.copy()
    results["sign_group"] = np.where(results["asym_avg"] < 0, "asym<0", "asym>=0")

    n = len(results)
    print()
    print("=" * 90)
    print(f"  美股能源反向分形策略 (n={n})")
    print("=" * 90)

    # ========= 1. 按 asym 符号分组 =========
    print()
    print("  [按 asym 符号分组] ROC20 -> 未来 30 日 RankIC")
    print(f"  {'组':<12} {'n':>4} {'asym_avg均值':>14} {'RankIC均值':>14}"
          f" {'RankIC>0 占比%':>16} {'显著(|IC|>0.1)%':>18}")
    print(f"  {'-'*80}")
    for g_name, g in results.groupby("sign_group"):
        ic_mean = g["rankic_1y"].mean()
        pos_pct = (g["rankic_1y"] > 0).sum() / len(g) * 100
        strong_pct = (g["rankic_1y"].abs() > 0.1).sum() / len(g) * 100
        print(f"  {g_name:<12} {len(g):>4} {g['asym_avg'].mean():>+14.3f}"
              f" {ic_mean:>+14.3f}"
              f" {pos_pct:>16.1f} {strong_pct:>18.1f}")

    # ========= 2. 按池内 group =========
    print()
    print("  [按 group 分类]")
    print(f"  {'group':<12} {'n':>4} {'asym_avg均值':>14}"
          f" {'RankIC均值':>14} {'RankIC>0 占比%':>16}")
    print(f"  {'-'*70}")
    for grp, g in results.groupby("group"):
        ic_mean = g["rankic_1y"].mean()
        pos_pct = (g["rankic_1y"] > 0).sum() / len(g) * 100
        print(f"  {grp:<12} {len(g):>4} {g['asym_avg'].mean():>+14.3f}"
              f" {ic_mean:>+14.3f} {pos_pct:>16.1f}")

    # ========= 3. 单股列表 (按 asym 排序) =========
    print()
    print("  [逐只 - 按 asym_avg 升序]")
    cols = ["ticker", "name", "group", "asym_latest", "asym_avg",
            "asym_std", "rankic_1y", "rankic_p", "n_pairs"]
    sorted_res = results.sort_values("asym_avg").reset_index(drop=True)
    print(sorted_res[cols].to_string(index=False,
                                      float_format=lambda x: f"{x:+.3f}"
                                      if isinstance(x, float) else str(x)))

    # ========= 4. 反向模式假说检验 =========
    print()
    print("  [反向模式假说检验]")
    neg_group = results[results["asym_avg"] < 0]
    pos_group = results[results["asym_avg"] >= 0]
    if len(neg_group) >= 2 and len(pos_group) >= 2:
        ic_neg = neg_group["rankic_1y"].mean()
        ic_pos = pos_group["rankic_1y"].mean()
        print(f"    假说: asym<0 组 RankIC 应为正 (高 ROC -> 趋势继续)")
        print(f"         asym>=0 组 RankIC 应为负 (高 ROC -> 回落)")
        print(f"    asym<0  组 RankIC 均值: {ic_neg:+.3f} "
              f"({'符合' if ic_neg > 0 else '不符合'}假说)")
        print(f"    asym>=0 组 RankIC 均值: {ic_pos:+.3f} "
              f"({'符合' if ic_pos < 0 else '不符合'}假说)")
        print(f"    两组 RankIC 差距: {ic_neg - ic_pos:+.3f}")

    # ========= 5. 最强反向 / 最意外 =========
    print()
    print("  [最强反向 - asym_avg 最负 且 RankIC 最正]")
    # 组合得分: asym 越负 (<0) 且 rankic 越正 越像反向
    results_valid = results.dropna(subset=["rankic_1y"]).copy()
    results_valid["reverse_score"] = (-results_valid["asym_avg"]) * results_valid["rankic_1y"]
    top_rev = results_valid.nlargest(5, "reverse_score")[
        ["ticker", "name", "group", "asym_avg", "rankic_1y", "reverse_score"]
    ]
    print(top_rev.to_string(index=False, float_format=lambda x: f"{x:+.3f}"))


if __name__ == "__main__":
    df = run_study(history_period="3y")
    report(df)
    out_path = "energy_reverse_results.csv"
    df.to_csv(out_path, index=False)
    print(f"\n详细结果已保存: {out_path}")
