"""行业维度因子回测调研 — 3行业对比 39 因子模型表现差异

动机: industry_fractal.py 已发现不同行业分形非对称度差异巨大
  (房地产 asym +0.502, 白酒 +0.329, 半导体 +0.233)
  行业内 asym std (0.278) < 全体 std (0.341) -> 同行聚集

假设: 既然行业间分形结构差异大, 39 因子模型在不同行业信号强度应该不同.

流程 (纯调研, 不改 main 模型):
  1. 用 pywencai 分别拉 房地产 / 白酒 / 半导体 各 15 只股票
  2. 对每只: fetch_history(code, 1200) + compute_all(df) + 滚动 score_trend
  3. 聚合每行业样本, 分行业统计:
     - 5/10/30 日 RankIC
     - Q5 vs Q1 涨率差 (30日)
     - 看涨>40做多 5/30 日盈亏比
  4. 输出对比表 + CSV

运行: ./venv/bin/python industry_backtest.py
"""

import time
import numpy as np
import pandas as pd
from datetime import datetime

from fetch_data import fetch_history
from fetch_wencai import get_stock_pool
from indicators import compute_all
from probability import score_trend, IC_WINDOW, HORIZON
from fundamental import fetch_financial


INDUSTRIES = [
    ("房地产",  "房地产行业股票"),
    ("白酒",    "白酒行业股票"),
    ("半导体",  "半导体芯片股票"),
]

MAX_PER_INDUSTRY = 15
HISTORY_DAYS = 1200
HORIZONS = [5, 10, 30]
WARMUP = IC_WINDOW + HORIZON + 20   # = 145


def backtest_one(code: str, industry: str) -> pd.DataFrame:
    """单只股票回测 -> 每日 outlook + 未来收益率 DataFrame."""
    df = fetch_history(code, days=HISTORY_DAYS)
    if df is None or len(df) < WARMUP + 60:
        return pd.DataFrame()

    # 基本面 (失败就继续, 模型会自动跳过缺失因子)
    try:
        fund_df = fetch_financial(code)
    except Exception:
        fund_df = None

    df = compute_all(df, fund_df)

    for h in HORIZONS:
        df[f"fwd_{h}d"] = df["close"].shift(-h) / df["close"] - 1

    records = []
    test_end = len(df) - max(HORIZONS)
    # 为控制耗时, 每 3 天采样一次 (1200 天 -> ~350 评分)
    for i in range(WARMUP, test_end, 3):
        window = df.iloc[:i + 1]
        try:
            res = score_trend(window)
        except Exception:
            continue
        if "error" in res:
            continue
        row = df.iloc[i]
        rec = {
            "code": code,
            "industry": industry,
            "date": row.name,
            "outlook": res["score"],
        }
        for h in HORIZONS:
            fwd = row[f"fwd_{h}d"]
            if pd.notna(fwd):
                rec[f"fwd_{h}d"] = fwd
        records.append(rec)

    return pd.DataFrame(records)


def collect_industry(name: str, prompt: str, max_n: int = MAX_PER_INDUSTRY) -> pd.DataFrame:
    """拉一个行业的股票池, 对每只做回测."""
    print(f"\n[行业] {name}  query={prompt!r}")
    try:
        pool = get_stock_pool(prompt)
    except Exception as e:
        print(f"  问财拉取失败: {e}")
        return pd.DataFrame()
    print(f"  池大小={len(pool)}, 取前 {min(max_n, len(pool))}")
    pool = pool[:max_n]

    frames = []
    fails = 0
    t_start = time.time()
    for i, code in enumerate(pool, 1):
        try:
            df = backtest_one(code, name)
            if df.empty:
                fails += 1
            else:
                frames.append(df)
                print(f"    [{i}/{len(pool)}] {code} n={len(df)}")
        except Exception as e:
            fails += 1
            print(f"    [{i}/{len(pool)}] {code} 失败: {e}")
        time.sleep(0.15)

    elapsed = time.time() - t_start
    print(f"  {name}: 成功 {len(frames)}/{len(pool)}, 失败 {fails}, 耗时 {elapsed:.1f}s")
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def _industry_metrics(df: pd.DataFrame) -> dict:
    """聚合行业指标."""
    out = {"n_samples": len(df), "n_stocks": df["code"].nunique()}

    # 1) 各周期 RankIC (Spearman)
    for h in HORIZONS:
        col = f"fwd_{h}d"
        if col not in df.columns:
            out[f"rankic_{h}d"] = np.nan
            continue
        sub = df[["outlook", col]].dropna()
        if len(sub) < 30:
            out[f"rankic_{h}d"] = np.nan
            continue
        rk = sub["outlook"].rank().corr(sub[col].rank())
        out[f"rankic_{h}d"] = rk

    # 2) Q5 - Q1 涨率差 (30 日)
    col30 = "fwd_30d"
    if col30 in df.columns:
        sub = df[["outlook", col30]].dropna()
        if len(sub) >= 50:
            try:
                sub = sub.assign(q=pd.qcut(sub["outlook"], 5,
                                           labels=["Q1", "Q2", "Q3", "Q4", "Q5"],
                                           duplicates="drop"))
                q1 = sub[sub["q"] == "Q1"][col30]
                q5 = sub[sub["q"] == "Q5"][col30]
                if len(q1) > 5 and len(q5) > 5:
                    out["q5_up_rate_30d"] = (q5 > 0).mean() * 100
                    out["q1_up_rate_30d"] = (q1 > 0).mean() * 100
                    out["q5_q1_diff_30d"] = out["q5_up_rate_30d"] - out["q1_up_rate_30d"]
                    out["q5_avg_ret_30d"] = q5.mean() * 100
                    out["q1_avg_ret_30d"] = q1.mean() * 100
                    out["q5_q1_ret_diff_30d"] = out["q5_avg_ret_30d"] - out["q1_avg_ret_30d"]
            except Exception:
                pass

    # 3) 看涨>40 做多 5d / 30d 盈亏比
    bull = df[df["outlook"] >= 40]
    for h in [5, 30]:
        col = f"fwd_{h}d"
        if col not in bull.columns or len(bull) < 10:
            continue
        rets = bull[col].dropna()
        if len(rets) < 10:
            continue
        wins = rets[rets > 0]
        losses = rets[rets <= 0]
        wr = len(wins) / len(rets) * 100
        aw = wins.mean() * 100 if len(wins) > 0 else 0
        al = abs(losses.mean()) * 100 if len(losses) > 0 else 0
        ratio = aw / al if al > 0 else float("inf")
        out[f"bull40_{h}d_n"] = len(rets)
        out[f"bull40_{h}d_winrate"] = wr
        out[f"bull40_{h}d_avg_win"] = aw
        out[f"bull40_{h}d_avg_loss"] = al
        out[f"bull40_{h}d_profit_loss_ratio"] = ratio

    return out


def build_report(all_df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for ind, sub in all_df.groupby("industry"):
        m = _industry_metrics(sub)
        m["industry"] = ind
        rows.append(m)
    report = pd.DataFrame(rows).set_index("industry")
    # 整理列顺序
    cols = ["n_stocks", "n_samples",
            "rankic_5d", "rankic_10d", "rankic_30d",
            "q5_up_rate_30d", "q1_up_rate_30d", "q5_q1_diff_30d",
            "q5_avg_ret_30d", "q1_avg_ret_30d", "q5_q1_ret_diff_30d",
            "bull40_5d_n", "bull40_5d_winrate", "bull40_5d_avg_win",
            "bull40_5d_avg_loss", "bull40_5d_profit_loss_ratio",
            "bull40_30d_n", "bull40_30d_winrate", "bull40_30d_avg_win",
            "bull40_30d_avg_loss", "bull40_30d_profit_loss_ratio"]
    cols = [c for c in cols if c in report.columns]
    return report[cols]


def print_compare_table(report: pd.DataFrame) -> None:
    print("\n" + "=" * 82)
    print("  三行业 39 因子模型表现对比")
    print("=" * 82)

    def fmt(v, pct=False, dec=3):
        if pd.isna(v):
            return "  n/a"
        if pct:
            return f"{v:+.1f}%"
        return f"{v:+.{dec}f}"

    header = f"\n{'指标':<26}" + "".join(f"{ind:>14}" for ind in report.index)
    print(header)
    print("-" * len(header))

    for label, key, pct, dec in [
        ("股票数",              "n_stocks",              False, 0),
        ("样本数 (day×stock)",  "n_samples",             False, 0),
        ("RankIC 5d",           "rankic_5d",             False, 4),
        ("RankIC 10d",          "rankic_10d",            False, 4),
        ("RankIC 30d",          "rankic_30d",            False, 4),
        ("Q5-Q1 涨率差 30d",    "q5_q1_diff_30d",        True,  1),
        ("Q5-Q1 收益差 30d",    "q5_q1_ret_diff_30d",    True,  2),
        ("Q5 均收益 30d",       "q5_avg_ret_30d",        True,  2),
        ("Q1 均收益 30d",       "q1_avg_ret_30d",        True,  2),
        ("看涨>40 5d 胜率",     "bull40_5d_winrate",     True,  1),
        ("看涨>40 5d 盈亏比",   "bull40_5d_profit_loss_ratio", False, 2),
        ("看涨>40 30d 胜率",    "bull40_30d_winrate",    True,  1),
        ("看涨>40 30d 盈亏比",  "bull40_30d_profit_loss_ratio", False, 2),
    ]:
        if key not in report.columns:
            continue
        line = f"{label:<26}"
        for ind in report.index:
            v = report.loc[ind, key]
            if isinstance(v, (int, np.integer)) or (key in ("n_stocks", "n_samples", "bull40_5d_n", "bull40_30d_n") and not pd.isna(v)):
                line += f"{int(v):>14d}"
            else:
                line += f"{fmt(v, pct, dec):>14}"
        print(line)

    # 排名
    print("\n" + "-" * len(header))
    print("  排名 (各指标按高到低):")
    for label, key, higher_better in [
        ("RankIC 5d",         "rankic_5d",                   True),
        ("RankIC 30d",        "rankic_30d",                  True),
        ("Q5-Q1 涨率差 30d",  "q5_q1_diff_30d",              True),
        ("看涨>40 30d 盈亏比","bull40_30d_profit_loss_ratio",True),
    ]:
        if key not in report.columns:
            continue
        ordered = report[key].dropna().sort_values(ascending=not higher_better).index.tolist()
        print(f"    {label:<24} : {' > '.join(ordered)}")


def main():
    print(f"\n{'=' * 72}")
    print(f"  行业因子回测  |  {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"  3 行业 x {MAX_PER_INDUSTRY} 股 x ~{HISTORY_DAYS} 天历史")
    print(f"{'=' * 72}")

    t0 = time.time()
    all_frames = []
    for name, prompt in INDUSTRIES:
        df = collect_industry(name, prompt, max_n=MAX_PER_INDUSTRY)
        if not df.empty:
            all_frames.append(df)
    if not all_frames:
        print("\n所有行业均无数据.")
        return

    combined = pd.concat(all_frames, ignore_index=True)
    print(f"\n合并总样本数: {len(combined)}  股票数: {combined['code'].nunique()}")

    report = build_report(combined)

    # CSV: 原始样本 + 行业汇总
    combined.to_csv("industry_backtest_raw.csv", index=False)
    report.to_csv("industry_backtest_results.csv")
    print(f"\n明细保存: industry_backtest_raw.csv  ({len(combined)} rows)")
    print(f"汇总保存: industry_backtest_results.csv")

    print_compare_table(report)

    print(f"\n耗时: {time.time() - t0:.1f}s")


if __name__ == "__main__":
    main()
