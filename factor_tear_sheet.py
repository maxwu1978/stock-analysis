#!/usr/bin/env python3
"""轻量版因子评估报告

目标:
1. 对当前 A股 / 美股主模型的底层因子做结构化评估
2. 输出类似 Alphalens 的核心结论:
   - 样本覆盖
   - IC / RankIC
   - 分层收益
   - 时段稳定性

不依赖外部框架, 直接复用当前仓库的数据与因子计算链.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass

import numpy as np
import pandas as pd

from backtest_v2 import fetch_sina_history
from factor_weighting import infer_factor_family
from fetch_data import STOCKS, fetch_history
from fetch_us import US_STOCKS, fetch_us_history
from fundamental import fetch_financial
from indicators import compute_all
from probability import FACTOR_COLS
from probability_us import US_FACTOR_COLS


A_HORIZONS = [5, 10, 30]
US_HORIZONS = [5, 10, 30]


@dataclass
class MarketConfig:
    market: str
    universe: dict[str, str]
    factors: list[str]
    horizons: list[int]


CONFIGS = {
    "a": MarketConfig("a", STOCKS, FACTOR_COLS, A_HORIZONS),
    "us": MarketConfig("us", US_STOCKS, US_FACTOR_COLS, US_HORIZONS),
}

def _fetch_series(cfg: MarketConfig, symbol: str) -> pd.DataFrame | None:
    if cfg.market == "a":
        try:
            df = fetch_history(symbol, days=1500)
        except Exception:
            df = None
        if df is None or len(df) < 180:
            try:
                df = fetch_sina_history(symbol, 1500)
            except Exception:
                df = None
        if df is None or len(df) < 180:
            return None
        try:
            fund_df = fetch_financial(symbol)
        except Exception:
            fund_df = None
        return compute_all(df, fund_df)

    df = fetch_us_history(symbol, period="10y")
    if df is None or len(df) < 180:
        return None
    return compute_all(df, None)


def collect_factor_panel(cfg: MarketConfig) -> pd.DataFrame:
    frames = []
    for symbol, name in cfg.universe.items():
        try:
            df = _fetch_series(cfg, symbol)
        except Exception as exc:
            print(f"[!] {name}({symbol}) 失败: {exc}")
            continue
        if df is None or df.empty:
            continue

        sub = df.copy()
        sub["symbol"] = symbol
        sub["name"] = name
        for h in cfg.horizons:
            sub[f"fwd_{h}d_ret"] = sub["close"].shift(-h) / sub["close"] - 1
            sub[f"fwd_{h}d_up"] = (sub[f"fwd_{h}d_ret"] > 0).astype(float)

        keep_cols = ["symbol", "name", "close"] + [c for c in cfg.factors if c in sub.columns]
        keep_cols += [f"fwd_{h}d_ret" for h in cfg.horizons] + [f"fwd_{h}d_up" for h in cfg.horizons]
        sub = sub[keep_cols].copy()
        sub.index.name = "date"
        sub = sub.reset_index()
        frames.append(sub)
        print(f"[+] {name}({symbol}) {len(sub)} rows")

    if not frames:
        return pd.DataFrame()
    panel = pd.concat(frames, ignore_index=True)
    return panel


def _calc_factor_metrics(panel: pd.DataFrame, factor: str, horizon: int) -> dict:
    ret_col = f"fwd_{horizon}d_ret"
    up_col = f"fwd_{horizon}d_up"
    sub = panel[[factor, ret_col, up_col, "date"]].dropna()
    n = len(sub)
    if n < 80:
        return {
            "factor": factor,
            "family": infer_factor_family(factor),
            "horizon": horizon,
            "n": n,
            "coverage": round(panel[factor].notna().mean() * 100, 1),
            "ic": None,
            "rankic": None,
            "q5_q1_ret_diff_pct": None,
            "q5_q1_up_diff_pp": None,
            "stable_halves": False,
        }

    ic = float(sub[factor].corr(sub[ret_col]))
    rankic = float(sub[factor].rank().corr(sub[ret_col].rank()))

    try:
        sub = sub.assign(q=pd.qcut(sub[factor], 5, labels=["Q1", "Q2", "Q3", "Q4", "Q5"], duplicates="drop"))
        q1 = sub[sub["q"] == "Q1"]
        q5 = sub[sub["q"] == "Q5"]
        q5_q1_ret_diff_pct = float((q5[ret_col].mean() - q1[ret_col].mean()) * 100) if len(q1) and len(q5) else None
        q5_q1_up_diff_pp = float((q5[up_col].mean() - q1[up_col].mean()) * 100) if len(q1) and len(q5) else None
    except Exception:
        q5_q1_ret_diff_pct = None
        q5_q1_up_diff_pp = None

    dated = sub.sort_values("date")
    mid = len(dated) // 2
    first = dated.iloc[:mid]
    second = dated.iloc[mid:]
    first_rankic = first[factor].rank().corr(first[ret_col].rank()) if len(first) >= 40 else np.nan
    second_rankic = second[factor].rank().corr(second[ret_col].rank()) if len(second) >= 40 else np.nan
    stable_halves = bool(
        pd.notna(first_rankic)
        and pd.notna(second_rankic)
        and np.sign(first_rankic) == np.sign(second_rankic)
        and abs(first_rankic) >= 0.02
        and abs(second_rankic) >= 0.02
    )

    return {
        "factor": factor,
        "family": infer_factor_family(factor),
        "horizon": horizon,
        "n": n,
        "coverage": round(panel[factor].notna().mean() * 100, 1),
        "ic": round(ic, 4),
        "rankic": round(rankic, 4),
        "q5_q1_ret_diff_pct": round(q5_q1_ret_diff_pct, 2) if q5_q1_ret_diff_pct is not None else None,
        "q5_q1_up_diff_pp": round(q5_q1_up_diff_pp, 2) if q5_q1_up_diff_pp is not None else None,
        "first_half_rankic": round(float(first_rankic), 4) if pd.notna(first_rankic) else None,
        "second_half_rankic": round(float(second_rankic), 4) if pd.notna(second_rankic) else None,
        "stable_halves": stable_halves,
    }


def build_factor_report(cfg: MarketConfig) -> tuple[pd.DataFrame, pd.DataFrame]:
    panel = collect_factor_panel(cfg)
    if panel.empty:
        return panel, pd.DataFrame()

    rows = []
    usable_factors = [f for f in cfg.factors if f in panel.columns]
    for factor in usable_factors:
        for horizon in cfg.horizons:
            rows.append(_calc_factor_metrics(panel, factor, horizon))
    report = pd.DataFrame(rows)
    return panel, report


def build_summary_report(report: pd.DataFrame) -> pd.DataFrame:
    if report.empty:
        return pd.DataFrame()
    summary = (
        report.groupby(["factor", "family"], as_index=False)
        .agg(
            best_rankic=("rankic", lambda s: s.dropna().max() if len(s.dropna()) else np.nan),
            worst_rankic=("rankic", lambda s: s.dropna().min() if len(s.dropna()) else np.nan),
            mean_abs_rankic=("rankic", lambda s: s.dropna().abs().mean() if len(s.dropna()) else np.nan),
            best_q5_q1_ret_diff_pct=("q5_q1_ret_diff_pct", lambda s: s.dropna().max() if len(s.dropna()) else np.nan),
            stable_h_count=("stable_halves", "sum"),
            max_coverage=("coverage", "max"),
        )
    )
    summary["quality_score"] = (
        summary["mean_abs_rankic"].fillna(0) * 100
        + summary["stable_h_count"].fillna(0) * 2
        + summary["max_coverage"].fillna(0) / 10
    ).round(2)
    return summary.sort_values(["quality_score", "best_rankic"], ascending=False).reset_index(drop=True)


def print_factor_report(cfg: MarketConfig, panel: pd.DataFrame, report: pd.DataFrame) -> None:
    if report.empty:
        print("无有效因子评估结果.")
        return

    print("\n" + "=" * 92)
    print(f"  Factor Tear Sheet · {cfg.market.upper()} · n={len(panel)}")
    print("=" * 92)

    best_rankic = report.dropna(subset=["rankic"]).sort_values("rankic", ascending=False).head(8)
    print("\n[1] 正向最强因子")
    print(
        best_rankic[["factor", "horizon", "coverage", "rankic", "q5_q1_ret_diff_pct", "stable_halves"]]
        .to_string(index=False)
    )

    worst_rankic = report.dropna(subset=["rankic"]).sort_values("rankic", ascending=True).head(8)
    print("\n[2] 反向最强因子")
    print(
        worst_rankic[["factor", "horizon", "coverage", "rankic", "q5_q1_ret_diff_pct", "stable_halves"]]
        .to_string(index=False)
    )

    stable = report[report["stable_halves"]].sort_values(["horizon", "rankic"], ascending=[True, False]).head(12)
    print("\n[3] 时段稳定因子")
    if stable.empty:
        print("无满足稳定性阈值的因子")
    else:
        print(
            stable[["factor", "horizon", "rankic", "first_half_rankic", "second_half_rankic"]]
            .to_string(index=False)
        )

    cov = report.groupby("factor")["coverage"].max().sort_values(ascending=False)
    print("\n[4] 覆盖率最低的因子")
    print(cov.tail(8).to_string())

    family = (
        report.groupby("family", as_index=False)
        .agg(
            mean_abs_rankic=("rankic", lambda s: round(s.dropna().abs().mean(), 4) if len(s.dropna()) else np.nan),
            stable_count=("stable_halves", "sum"),
            mean_coverage=("coverage", "mean"),
        )
        .sort_values("mean_abs_rankic", ascending=False)
    )
    print("\n[5] 因子族概览")
    print(family.to_string(index=False))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--market", choices=["a", "us"], default="a")
    parser.add_argument("--csv-prefix", default="factor_tear_sheet")
    args = parser.parse_args()

    cfg = CONFIGS[args.market]
    panel, report = build_factor_report(cfg)
    if report.empty:
        raise SystemExit(1)
    summary = build_summary_report(report)

    panel_path = f"{args.csv_prefix}_{args.market}_panel.csv"
    report_path = f"{args.csv_prefix}_{args.market}_report.csv"
    summary_path = f"{args.csv_prefix}_{args.market}_summary.csv"
    panel.to_csv(panel_path, index=False)
    report.to_csv(report_path, index=False)
    summary.to_csv(summary_path, index=False)
    print_factor_report(cfg, panel, report)
    print(f"\n输出: {panel_path}")
    print(f"输出: {report_path}")
    print(f"输出: {summary_path}")


if __name__ == "__main__":
    main()
