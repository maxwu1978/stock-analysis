#!/usr/bin/env python3
"""Small backtest for using Kronos as a confirmation layer on A-share signals."""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from backtest_v2 import fetch_sina_history
from fetch_data import STOCKS
from fundamental import fetch_financial
from indicators import compute_all
from kronos_us_experiment import (
    DEFAULT_KRONOS_REPO,
    _build_predictor,
    _ensure_kronos_repo,
)
from position_sizing import recommend_model_action
from probability import HORIZON, IC_WINDOW, score_trend
from reliability import get_reliability_label, load_reliability_labels


REPO_ROOT = Path(__file__).resolve().parent
DETAIL_PATH = REPO_ROOT / "kronos_confirmation_backtest_a_details.csv"
SUMMARY_PATH = REPO_ROOT / "kronos_confirmation_backtest_a_summary.csv"
ACTIONABLE_ACTIONS = {"PROBE_LONG", "BUILD_LONG"}


def _direction_from_prob(hp: dict) -> str:
    p30 = hp.get("30日")
    if not p30:
        return "--"
    pct = int(p30["上涨概率"].replace("%", ""))
    if pct >= 60:
        return "看涨"
    if pct >= 55:
        return "偏涨"
    if pct <= 35:
        return "看跌"
    if pct <= 45:
        return "偏跌"
    return "震荡"


def _bool_rate(series: pd.Series) -> float | None:
    valid = series.dropna()
    if len(valid) == 0:
        return None
    return float(valid.mean() * 100)


def _factor_sidecar(hist_df: pd.DataFrame, code: str, reliability_labels: dict) -> dict | None:
    result = score_trend(hist_df)
    if "error" in result:
        return None
    direction = _direction_from_prob(result.get("historical_prob", {}))
    reliability = get_reliability_label(reliability_labels, "a_share", code)
    decision = recommend_model_action(
        direction=direction,
        entry_price=float(hist_df["close"].iloc[-1]),
        score=result.get("score"),
        reliability=reliability,
        macro_penalty=0,
    )
    return {
        "factor_score": float(result.get("score", 0) or 0),
        "factor_direction": direction,
        "factor_action": decision.action,
        "factor_tier": decision.plan.position_tier,
        "factor_risk_budget": float(decision.plan.risk_budget),
        "reliability": reliability,
    }


def run_backtest(
    *,
    codes: list[str],
    history_count: int,
    lookback: int,
    pred_len: int,
    step: int,
    model_size: str,
    repo_path: Path,
    bootstrap: bool,
    device: str,
    kronos_confirm_min_ret: float,
    detail_output: Path,
    summary_output: Path,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    repo_path = _ensure_kronos_repo(repo_path, bootstrap)
    predictor = _build_predictor(repo_path, model_size, device)
    reliability_labels = load_reliability_labels()

    detail_rows = []
    warmup = max(lookback, IC_WINDOW + HORIZON + 20)

    for code in codes:
        raw = fetch_sina_history(code, count=history_count).copy()
        raw = raw.sort_index()
        raw.index = pd.to_datetime(raw.index).tz_localize(None)
        raw = raw[["open", "high", "low", "close", "volume"]].dropna()
        if len(raw) < warmup + pred_len + 5:
            continue

        try:
            fund_df = fetch_financial(code)
        except Exception:
            fund_df = None

        full_factor = compute_all(raw.copy(), fund_df)

        for i in range(warmup - 1, len(raw) - pred_len, step):
            hist_factor = full_factor.iloc[: i + 1].copy()
            hist_raw = raw.iloc[: i + 1].copy()
            future = raw.iloc[i + 1 : i + pred_len + 1].copy()

            sidecar = _factor_sidecar(hist_factor, code, reliability_labels)
            if not sidecar:
                continue

            x_df = hist_raw.iloc[-lookback:].copy()
            x_timestamp = pd.Series(pd.to_datetime(x_df.index))
            y_timestamp = pd.Series(pd.to_datetime(future.index))

            pred_df = predictor.predict(
                df=x_df,
                x_timestamp=x_timestamp,
                y_timestamp=y_timestamp,
                pred_len=pred_len,
                T=1.0,
                top_p=0.9,
                sample_count=1,
                verbose=False,
            )

            last_close = float(x_df["close"].iloc[-1])
            pred_close_n = float(pred_df["close"].iloc[-1])
            actual_close_n = float(future["close"].iloc[-1])
            pred_ret_nd = (pred_close_n / last_close - 1) * 100
            actual_ret_nd = (actual_close_n / last_close - 1) * 100
            kronos_bullish = pred_ret_nd >= kronos_confirm_min_ret
            actionable = sidecar["factor_action"] in ACTIONABLE_ACTIONS

            detail_rows.append(
                {
                    "code": code,
                    "name": STOCKS.get(code, code),
                    "signal_date": hist_raw.index[-1],
                    "pred_end_date": future.index[-1],
                    "factor_score": round(sidecar["factor_score"], 2),
                    "factor_direction": sidecar["factor_direction"],
                    "factor_action": sidecar["factor_action"],
                    "factor_tier": sidecar["factor_tier"],
                    "factor_risk_budget": round(sidecar["factor_risk_budget"], 2),
                    "reliability": sidecar["reliability"],
                    "kronos_pred_ret_nd_pct": round(pred_ret_nd, 2),
                    "kronos_confirm": int(kronos_bullish),
                    "actual_ret_nd_pct": round(actual_ret_nd, 2),
                    "base_hit": int(actual_ret_nd > 0) if actionable else None,
                    "confirmed_hit": int(actual_ret_nd > 0) if (actionable and kronos_bullish) else None,
                    "actionable": int(actionable),
                    "confirmed_actionable": int(actionable and kronos_bullish),
                }
            )

    details = pd.DataFrame(detail_rows)
    if details.empty:
        details.to_csv(detail_output, index=False, encoding="utf-8-sig")
        pd.DataFrame().to_csv(summary_output, index=False, encoding="utf-8-sig")
        return details, pd.DataFrame()

    def _summarize(group: pd.DataFrame, label: str) -> dict:
        actionable = group[group["actionable"] == 1]
        confirmed = group[group["confirmed_actionable"] == 1]
        return {
            "scope": label,
            "signals_total": int(len(group)),
            "actionable_n": int(len(actionable)),
            "actionable_hit_rate_pct": round(_bool_rate(actionable["base_hit"]) or 0, 2) if len(actionable) else None,
            "actionable_avg_ret_pct": round(float(actionable["actual_ret_nd_pct"].mean()), 2) if len(actionable) else None,
            "confirmed_n": int(len(confirmed)),
            "confirmed_hit_rate_pct": round(_bool_rate(confirmed["confirmed_hit"]) or 0, 2) if len(confirmed) else None,
            "confirmed_avg_ret_pct": round(float(confirmed["actual_ret_nd_pct"].mean()), 2) if len(confirmed) else None,
            "coverage_pct": round(len(confirmed) / len(actionable) * 100, 2) if len(actionable) else None,
            "hit_rate_lift_pp": round((_bool_rate(confirmed["confirmed_hit"]) or 0) - (_bool_rate(actionable["base_hit"]) or 0), 2) if len(actionable) and len(confirmed) else None,
        }

    summary_rows = [_summarize(details, "ALL")]
    for code in details["code"].dropna().unique():
        summary_rows.append(_summarize(details[details["code"] == code].copy(), code))

    summary = pd.DataFrame(summary_rows)
    details.to_csv(detail_output, index=False, encoding="utf-8-sig")
    summary.to_csv(summary_output, index=False, encoding="utf-8-sig")
    return details, summary


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Kronos A股二次确认小回测")
    parser.add_argument("--codes", nargs="+", default=list(STOCKS.keys()))
    parser.add_argument("--history-count", type=int, default=1200)
    parser.add_argument("--lookback", type=int, default=180)
    parser.add_argument("--pred-len", type=int, default=5)
    parser.add_argument("--step", type=int, default=20)
    parser.add_argument("--model", choices=["mini", "small", "base"], default="mini")
    parser.add_argument("--repo-path", type=Path, default=DEFAULT_KRONOS_REPO)
    parser.add_argument("--bootstrap", action="store_true")
    parser.add_argument("--device", default="auto")
    parser.add_argument("--kronos-confirm-min-ret", type=float, default=1.0)
    parser.add_argument("--detail-output", type=Path, default=DETAIL_PATH)
    parser.add_argument("--summary-output", type=Path, default=SUMMARY_PATH)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    details, summary = run_backtest(
        codes=args.codes,
        history_count=args.history_count,
        lookback=args.lookback,
        pred_len=args.pred_len,
        step=args.step,
        model_size=args.model,
        repo_path=args.repo_path,
        bootstrap=args.bootstrap,
        device=args.device,
        kronos_confirm_min_ret=args.kronos_confirm_min_ret,
        detail_output=args.detail_output,
        summary_output=args.summary_output,
    )
    if summary.empty:
        print("没有产出有效回测结果。")
        return 1

    display_cols = [
        "scope",
        "actionable_n",
        "actionable_hit_rate_pct",
        "confirmed_n",
        "confirmed_hit_rate_pct",
        "coverage_pct",
        "hit_rate_lift_pp",
    ]
    print(summary[display_cols].to_string(index=False))
    print(f"\n详情已写出: {args.detail_output}")
    print(f"摘要已写出: {args.summary_output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
