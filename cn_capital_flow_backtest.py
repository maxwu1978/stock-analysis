#!/usr/bin/env python3
"""Backtest the A-share capital-flow intent layer.

This validates the explanatory capital-flow labels by replaying each historical
day available from Eastmoney fund-flow history and measuring later returns.
"""

from __future__ import annotations

import argparse
from dataclasses import asdict
from datetime import datetime

import pandas as pd

from cn_capital_flow import (
    CapitalFlowView,
    _classify_intent,
    _confidence,
    _num,
    _ret_tail,
    _score_flow,
    _sum_tail,
    fetch_stock_capital_flow,
)
from fetch_data import STOCKS


HORIZONS = [5, 10, 20]
MIN_LOOKBACK = 30
BULLISH_INTENTS = {"吸筹", "拉升", "流入确认"}
BEARISH_INTENTS = {"派发", "撤退", "流出确认"}


def _window_view(code: str, name: str, window: pd.DataFrame) -> CapitalFlowView:
    latest = window.iloc[-1]
    main_net = _num(window, "主力净流入-净额")
    super_large = _num(window, "超大单净流入-净额")
    large = _num(window, "大单净流入-净额")
    small = _num(window, "小单净流入-净额")
    close = _num(window, "收盘价")

    view_args = {
        "main_net": float(latest.get("主力净流入-净额", 0) or 0),
        "main_ratio": float(latest.get("主力净流入-净占比", 0) or 0),
        "main_net_3d": _sum_tail(main_net, 3),
        "main_net_5d": _sum_tail(main_net, 5),
        "main_net_10d": _sum_tail(main_net, 10),
        "super_large_net_5d": _sum_tail(super_large, 5),
        "large_net_5d": _sum_tail(large, 5),
        "small_net_5d": _sum_tail(small, 5),
        "price_ret_5d": _ret_tail(close, 5),
    }
    score = _score_flow(window)
    intent, explanation = _classify_intent(**view_args, capital_score=score)
    return CapitalFlowView(
        code=code,
        name=name,
        date=str(latest.get("日期", "")),
        close=float(latest.get("收盘价", 0) or 0),
        change_pct=float(latest.get("涨跌幅", 0) or 0),
        capital_score=score,
        intent=intent,
        confidence=_confidence(score, view_args["main_ratio"], view_args["main_net_5d"]),
        explanation=explanation,
        **view_args,
    )


def _expected_direction(intent: str) -> int:
    if intent in BULLISH_INTENTS:
        return 1
    if intent in BEARISH_INTENTS:
        return -1
    return 0


def backtest_stock(code: str, name: str, *, use_cache: bool) -> pd.DataFrame:
    raw = fetch_stock_capital_flow(code, use_cache=use_cache)
    if raw.empty:
        return pd.DataFrame()
    df = raw.copy()
    df["日期"] = pd.to_datetime(df["日期"], errors="coerce")
    df = df.dropna(subset=["日期"]).sort_values("日期").reset_index(drop=True)
    for col in [
        "收盘价",
        "涨跌幅",
        "主力净流入-净额",
        "主力净流入-净占比",
        "超大单净流入-净额",
        "大单净流入-净额",
        "小单净流入-净额",
    ]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    records = []
    max_h = max(HORIZONS)
    for i in range(MIN_LOOKBACK - 1, len(df) - max_h):
        view = _window_view(code, name, df.iloc[: i + 1].copy())
        expected = _expected_direction(view.intent)
        if expected == 0:
            group = "neutral"
        elif expected > 0:
            group = "bullish"
        else:
            group = "bearish"
        close_now = float(df.loc[i, "收盘价"])
        for h in HORIZONS:
            close_future = float(df.loc[i + h, "收盘价"])
            fwd_ret_pct = (close_future / close_now - 1.0) * 100 if close_now else 0.0
            direction_hit = None if expected == 0 else int(fwd_ret_pct * expected > 0)
            signed_ret_pct = fwd_ret_pct * expected if expected else 0.0
            rec = asdict(view)
            rec.update(
                {
                    "horizon": h,
                    "expected_group": group,
                    "expected_direction": expected,
                    "fwd_ret_pct": fwd_ret_pct,
                    "signed_ret_pct": signed_ret_pct,
                    "direction_hit": direction_hit,
                    "future_date": str(df.loc[i + h, "日期"].date()),
                }
            )
            records.append(rec)
    return pd.DataFrame(records)


def summarize(details: pd.DataFrame) -> pd.DataFrame:
    if details.empty:
        return pd.DataFrame()
    rows = []
    groupings = [
        ("overall_directional", ["horizon"]),
        ("by_group", ["expected_group", "horizon"]),
        ("by_intent", ["intent", "horizon"]),
        ("by_symbol", ["symbol", "horizon"]),
    ]
    df = details.rename(columns={"code": "symbol"})
    for scope, cols in groupings:
        for keys, sub in df.groupby(cols, dropna=False):
            if not isinstance(keys, tuple):
                keys = (keys,)
            row = {"scope": scope}
            row.update(dict(zip(cols, keys)))
            directional = sub[sub["expected_direction"] != 0]
            hit_series = directional["direction_hit"].dropna()
            row.update(
                {
                    "n": int(len(sub)),
                    "directional_n": int(len(directional)),
                    "hit_rate_pct": round(float(hit_series.mean() * 100), 2) if len(hit_series) else None,
                    "mean_fwd_ret_pct": round(float(sub["fwd_ret_pct"].mean()), 3),
                    "median_fwd_ret_pct": round(float(sub["fwd_ret_pct"].median()), 3),
                    "mean_signed_ret_pct": round(float(directional["signed_ret_pct"].mean()), 3) if len(directional) else None,
                    "avg_capital_score": round(float(sub["capital_score"].mean()), 2),
                }
            )
            rows.append(row)
    return pd.DataFrame(rows)


def run(use_cache: bool) -> tuple[pd.DataFrame, pd.DataFrame]:
    frames = []
    coverage = []
    for code, name in STOCKS.items():
        print(f"  {name}({code})...", end="", flush=True)
        raw = fetch_stock_capital_flow(code, use_cache=use_cache)
        if raw.empty:
            print(" no data")
            continue
        start = pd.to_datetime(raw["日期"], errors="coerce").min()
        end = pd.to_datetime(raw["日期"], errors="coerce").max()
        coverage.append({"code": code, "name": name, "rows": len(raw), "start": start, "end": end})
        bt = backtest_stock(code, name, use_cache=True)
        print(f" {len(raw)} rows, {len(bt)} validation rows")
        if not bt.empty:
            frames.append(bt)
    details = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    summary = summarize(details)
    coverage_df = pd.DataFrame(coverage)
    if not coverage_df.empty:
        coverage_df.to_csv("cn_capital_flow_backtest_coverage.csv", index=False)
    return details, summary


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backtest CN capital-flow intent labels")
    parser.add_argument("--no-cache", action="store_true")
    parser.add_argument("--details-output", default="cn_capital_flow_backtest_details.csv")
    parser.add_argument("--summary-output", default="cn_capital_flow_backtest_summary.csv")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    print("\n══ CN Capital Flow Intent Backtest ══")
    print(f"run_at: {datetime.now():%Y-%m-%d %H:%M:%S}")
    print("requested: 2 years; source coverage is reported separately")
    details, summary = run(use_cache=not args.no_cache)
    details.to_csv(args.details_output, index=False)
    summary.to_csv(args.summary_output, index=False)
    print(f"\ndetails: {args.details_output}")
    print(f"summary: {args.summary_output}")
    if not summary.empty:
        show = summary[(summary["scope"] == "overall_directional") & summary["horizon"].isin(HORIZONS)]
        print("\n[overall directional]")
        print(show[["horizon", "directional_n", "hit_rate_pct", "mean_signed_ret_pct", "mean_fwd_ret_pct"]].to_string(index=False))


if __name__ == "__main__":
    main()
