#!/usr/bin/env python3
"""Two-year validation for the current A-share page signal algorithm.

This replays the same production direction rule used on the page:
30-day historical upside probability decides 看涨/偏涨/震荡/偏跌/看跌.
"""

from __future__ import annotations

import argparse
from datetime import datetime

import pandas as pd

from analyze import direction_from_prob
from backtest_v2 import fetch_sina_history
from fetch_data import STOCKS
from fundamental import fetch_financial
from indicators import compute_all
from position_sizing import recommend_model_action
from probability import HORIZON, IC_WINDOW, score_trend
from reliability import get_reliability_label, load_reliability_labels


HORIZONS = [5, 10, 30, 60]
WARMUP = IC_WINDOW + HORIZON + 20
BULLISH = {"看涨", "偏涨"}
BEARISH = {"看跌", "偏跌"}


def expected_direction(direction: str) -> int:
    if direction in BULLISH:
        return 1
    if direction in BEARISH:
        return -1
    return 0


def backtest_stock(code: str, name: str, *, years: int, count: int, labels: dict) -> pd.DataFrame:
    print(f"  {name}({code})...", end="", flush=True)
    df = fetch_sina_history(code, count)
    fund_df = fetch_financial(code)
    enriched = compute_all(df, fund_df)
    latest_date = enriched.index.max()
    start_date = latest_date - pd.DateOffset(years=years)

    for h in HORIZONS:
        enriched[f"fwd_{h}d_ret"] = enriched["close"].shift(-h) / enriched["close"] - 1.0

    reliability = get_reliability_label(labels, "a_share", code)
    records = []
    test_end = len(enriched) - max(HORIZONS)
    for i in range(WARMUP, test_end):
        row = enriched.iloc[i]
        if row.name < start_date:
            continue
        window = enriched.iloc[: i + 1].copy()
        try:
            result = score_trend(window)
        except Exception:
            continue
        if "error" in result:
            continue

        hp = result.get("historical_prob", {})
        direction = direction_from_prob(hp)
        expected = expected_direction(direction)
        decision = recommend_model_action(
            direction=direction,
            entry_price=float(row["close"]),
            score=result.get("score"),
            reliability=reliability,
            macro_penalty=0,
        )

        for h in HORIZONS:
            fwd = row.get(f"fwd_{h}d_ret")
            if pd.isna(fwd):
                continue
            fwd_ret_pct = float(fwd) * 100.0
            records.append(
                {
                    "date": row.name.date().isoformat(),
                    "symbol": code,
                    "name": name,
                    "close": float(row["close"]),
                    "score": float(result.get("score", 0) or 0),
                    "direction": direction,
                    "expected_direction": expected,
                    "reliability": reliability,
                    "action": decision.action,
                    "tier": decision.plan.position_tier,
                    "horizon": h,
                    "fwd_ret_pct": fwd_ret_pct,
                    "signed_ret_pct": fwd_ret_pct * expected if expected else 0.0,
                    "direction_hit": None if expected == 0 else int(fwd_ret_pct * expected > 0),
                    "p30": hp.get("30日", {}).get("上涨概率"),
                    "p30_avg": hp.get("30日", {}).get("平均收益"),
                }
            )
    print(f" {len(records)} rows")
    return pd.DataFrame(records)


def summarize(details: pd.DataFrame) -> pd.DataFrame:
    if details.empty:
        return pd.DataFrame()
    rows = []
    groupings = [
        ("overall_directional", ["horizon"]),
        ("by_direction", ["direction", "horizon"]),
        ("by_action", ["action", "horizon"]),
        ("by_symbol", ["symbol", "horizon"]),
        ("by_reliability", ["reliability", "horizon"]),
    ]
    for scope, cols in groupings:
        for keys, sub in details.groupby(cols, dropna=False):
            if not isinstance(keys, tuple):
                keys = (keys,)
            row = {"scope": scope}
            row.update(dict(zip(cols, keys)))
            directional = sub[sub["expected_direction"] != 0]
            hits = directional["direction_hit"].dropna()
            row.update(
                {
                    "n": int(len(sub)),
                    "directional_n": int(len(directional)),
                    "hit_rate_pct": round(float(hits.mean() * 100), 2) if len(hits) else None,
                    "mean_fwd_ret_pct": round(float(sub["fwd_ret_pct"].mean()), 3),
                    "median_fwd_ret_pct": round(float(sub["fwd_ret_pct"].median()), 3),
                    "mean_signed_ret_pct": round(float(directional["signed_ret_pct"].mean()), 3) if len(directional) else None,
                    "avg_score": round(float(sub["score"].mean()), 2),
                }
            )
            rows.append(row)
    return pd.DataFrame(rows)


def run(years: int, count: int) -> tuple[pd.DataFrame, pd.DataFrame]:
    labels = load_reliability_labels()
    frames = []
    for code, name in STOCKS.items():
        try:
            bt = backtest_stock(code, name, years=years, count=count, labels=labels)
            if not bt.empty:
                frames.append(bt)
        except Exception as exc:
            print(f" failed: {exc}")
    details = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    return details, summarize(details)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate current A-share signal algorithm over recent years")
    parser.add_argument("--years", type=int, default=2)
    parser.add_argument("--count", type=int, default=1000)
    parser.add_argument("--details-output", default="a_share_signal_validation_2y_details.csv")
    parser.add_argument("--summary-output", default="a_share_signal_validation_2y_summary.csv")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    print("\n══ A-Share Signal Validation ══")
    print(f"run_at: {datetime.now():%Y-%m-%d %H:%M:%S}")
    print(f"period: recent {args.years} calendar years")
    details, summary = run(years=args.years, count=args.count)
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
