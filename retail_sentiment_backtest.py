#!/usr/bin/env python3
"""Backtest A-share retail attention as a contrarian proxy.

The available free historical field is Eastmoney popularity rank history. This
script joins that rank to daily prices and checks whether high retail attention
is followed by weaker 5/10/20-day forward returns.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd

from cn_retail_sentiment import fetch_hot_rank_history
from fetch_data import STOCKS, fetch_history


def _retail_heat_from_rank(rank: pd.Series, max_rank: int = 600) -> pd.Series:
    rank = pd.to_numeric(rank, errors="coerce")
    return ((1.0 - rank.clip(lower=1, upper=max_rank) / max_rank) * 100.0).clip(0, 100)


def _rankic(frame: pd.DataFrame, factor: str, ret_col: str) -> float | None:
    sub = frame[[factor, ret_col]].dropna()
    if len(sub) < 20:
        return None
    val = sub[factor].rank().corr(sub[ret_col].rank())
    return None if pd.isna(val) else round(float(val), 4)


def build_panel(days: int, use_cache: bool) -> pd.DataFrame:
    frames = []
    for code, name in STOCKS.items():
        try:
            hist = fetch_history(code, days=days)
            hot = fetch_hot_rank_history(code, use_cache=use_cache)
        except Exception as exc:
            print(f"[!] {name}({code}) 失败: {exc}")
            continue
        if hist.empty or hot.empty or "排名" not in hot.columns:
            continue

        price = hist.reset_index()[["date", "close"]].copy()
        price["date"] = pd.to_datetime(price["date"], errors="coerce").dt.normalize()
        hot = hot.copy()
        hot["date"] = pd.to_datetime(hot.get("时间"), errors="coerce").dt.normalize()
        hot["retail_heat"] = _retail_heat_from_rank(hot["排名"])
        merged = pd.merge(price, hot[["date", "排名", "retail_heat"]], on="date", how="inner")
        if merged.empty:
            continue
        merged = merged.sort_values("date")
        for horizon in (5, 10, 20):
            merged[f"fwd_{horizon}d_ret"] = merged["close"].shift(-horizon) / merged["close"] - 1
        merged["code"] = code
        merged["name"] = name
        frames.append(merged)
        print(f"[+] {name}({code}) {len(merged)} rows")
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def summarize(panel: pd.DataFrame) -> pd.DataFrame:
    rows = []
    if panel.empty:
        return pd.DataFrame()
    panel = panel.copy()
    panel["bucket"] = pd.cut(
        panel["retail_heat"],
        bins=[-0.01, 35, 65, 100],
        labels=["低关注", "中性", "高关注"],
    )
    for horizon in (5, 10, 20):
        ret_col = f"fwd_{horizon}d_ret"
        valid = panel.dropna(subset=[ret_col, "retail_heat"])
        rankic = _rankic(valid, "retail_heat", ret_col)
        bucket_mean = valid.groupby("bucket", observed=False)[ret_col].mean()
        bucket_count = valid.groupby("bucket", observed=False)[ret_col].count()
        high = float(bucket_mean.get("高关注", np.nan))
        low = float(bucket_mean.get("低关注", np.nan))
        rows.append(
            {
                "horizon": horizon,
                "rankic": rankic,
                "contrarian_rankic": None if rankic is None else round(-rankic, 4),
                "high_attention_avg_ret": round(high * 100, 2) if not np.isnan(high) else np.nan,
                "low_attention_avg_ret": round(low * 100, 2) if not np.isnan(low) else np.nan,
                "high_minus_low": round((high - low) * 100, 2) if not np.isnan(high) and not np.isnan(low) else np.nan,
                "high_n": int(bucket_count.get("高关注", 0)),
                "low_n": int(bucket_count.get("低关注", 0)),
                "sample_n": int(len(valid)),
            }
        )
    return pd.DataFrame(rows)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backtest retail attention as a contrarian A-share proxy")
    parser.add_argument("--days", type=int, default=500, help="price history days")
    parser.add_argument("--no-cache", action="store_true", help="ignore cached popularity rank files")
    parser.add_argument("--panel-csv", default="retail_sentiment_backtest_panel.csv")
    parser.add_argument("--summary-csv", default="retail_sentiment_backtest_summary.csv")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    panel = build_panel(days=args.days, use_cache=not args.no_cache)
    summary = summarize(panel)
    if panel.empty or summary.empty:
        raise SystemExit("无有效散户情绪回测样本")
    panel.to_csv(Path(args.panel_csv), index=False)
    summary.to_csv(Path(args.summary_csv), index=False)
    print("\n散户热度反向验证:")
    print(summary.to_string(index=False))
    print(f"\n输出: {args.panel_csv}, {args.summary_csv}")


if __name__ == "__main__":
    main()
