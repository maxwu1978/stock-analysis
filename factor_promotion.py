#!/usr/bin/env python3
"""Convert candidate factor lab outputs into a promotion queue."""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path(__file__).resolve().parent
QUEUE_PATH = ROOT / "factor_promotion_queue.json"


@dataclass(slots=True)
class PromotionThresholds:
    min_coverage: float = 50.0
    min_watch_rankic: float = 0.025
    min_promote_rankic: float = 0.040
    min_promote_stable_count: int = 2
    min_consistency: float = 55.0
    min_promote_orthogonality: float = 0.15
    reject_duplicate_orthogonality: float = 0.10


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        val = float(value)
    except Exception:
        return None
    if pd.isna(val):
        return None
    return val


def _best_horizon_row(report: pd.DataFrame, factor: str) -> pd.Series | None:
    sub = report[report["factor"] == factor].copy()
    if sub.empty:
        return None
    sub["_abs_rankic"] = sub["rankic"].astype(float).abs()
    sub["_stable"] = sub["stable_halves"].astype(bool).astype(int)
    sub = sub.sort_values(["_abs_rankic", "_stable", "coverage"], ascending=[False, False, False])
    return sub.iloc[0]


def decide_factor(summary_row: pd.Series, best_row: pd.Series | None, thresholds: PromotionThresholds) -> tuple[str, str]:
    coverage = _safe_float(summary_row.get("max_coverage")) or 0.0
    best_abs_rankic = _safe_float(summary_row.get("best_abs_rankic"))
    stable_count = int(summary_row.get("stable_count") or 0)
    consistency = _safe_float(summary_row.get("mean_consistency"))
    orthogonality = _safe_float(summary_row.get("best_orthogonality"))
    closest = summary_row.get("closest_active_factor")

    if best_abs_rankic is None or coverage < thresholds.min_coverage:
        return "REJECT", "coverage不足或缺少有效rankIC"

    if orthogonality is not None and orthogonality < thresholds.reject_duplicate_orthogonality:
        return "REJECT_DUPLICATE", f"与active因子 {closest or '-'} 高度重叠"

    if (
        best_abs_rankic >= thresholds.min_promote_rankic
        and stable_count >= thresholds.min_promote_stable_count
        and (consistency is None or consistency >= thresholds.min_consistency)
        and (orthogonality is None or orthogonality >= thresholds.min_promote_orthogonality)
    ):
        reason = "rankIC、稳定性和正交性均达标，可进入试验池"
        return "PROMOTE_TO_TRIAL", reason

    if best_abs_rankic >= thresholds.min_watch_rankic:
        reason = "局部有效，但稳定性或正交性不足，进入观察名单"
        if best_row is not None and bool(best_row.get("stable_halves")):
            reason = "局部有效且单周期稳定，但整体晋升证据不足，进入观察名单"
        return "WATCH", reason

    return "REJECT", "信号强度不足"


def _build_item(
    market: str,
    summary_row: pd.Series,
    best_row: pd.Series | None,
    thresholds: PromotionThresholds,
) -> dict[str, Any]:
    decision, reason = decide_factor(summary_row, best_row, thresholds)
    factor = str(summary_row["factor"])
    family = str(summary_row["family"])
    closest = summary_row.get("closest_active_factor")

    item = {
        "market": market,
        "factor": factor,
        "family": family,
        "status": str(summary_row.get("status") or "candidate"),
        "decision": decision,
        "reason": reason,
        "closest_active_factor": None if pd.isna(closest) else closest,
        "best_abs_rankic": _safe_float(summary_row.get("best_abs_rankic")),
        "mean_abs_rankic": _safe_float(summary_row.get("mean_abs_rankic")),
        "stable_count": int(summary_row.get("stable_count") or 0),
        "mean_consistency": _safe_float(summary_row.get("mean_consistency")),
        "best_orthogonality": _safe_float(summary_row.get("best_orthogonality")),
        "max_coverage": _safe_float(summary_row.get("max_coverage")),
        "quality_score": _safe_float(summary_row.get("quality_score")),
        "recommended_action": {
            "PROMOTE_TO_TRIAL": "加入试验回测池，不修改 active 因子池",
            "WATCH": "保留候选状态，继续观察后续样本",
            "REJECT_DUPLICATE": "记录为重复因子，不建议再次变体试验",
            "REJECT": "淘汰，不进入晋升队列",
        }[decision],
    }

    if best_row is not None:
        item["best_horizon"] = int(best_row["horizon"])
        item["best_rankic"] = _safe_float(best_row.get("rankic"))
        item["best_ic"] = _safe_float(best_row.get("ic"))
        item["q5_q1_ret_diff_pct"] = _safe_float(best_row.get("q5_q1_ret_diff_pct"))
        item["q5_q1_up_diff_pp"] = _safe_float(best_row.get("q5_q1_up_diff_pp"))
        item["stable_halves"] = bool(best_row.get("stable_halves"))
        item["first_half_rankic"] = _safe_float(best_row.get("first_half_rankic"))
        item["second_half_rankic"] = _safe_float(best_row.get("second_half_rankic"))

    return item


def build_market_queue(
    market: str,
    report: pd.DataFrame,
    summary: pd.DataFrame,
    thresholds: PromotionThresholds,
) -> dict[str, Any]:
    decisions = []
    for _, row in summary.iterrows():
        best_row = _best_horizon_row(report, str(row["factor"]))
        decisions.append(_build_item(market, row, best_row, thresholds))

    promote = [item for item in decisions if item["decision"] == "PROMOTE_TO_TRIAL"]
    watch = [item for item in decisions if item["decision"] == "WATCH"]
    reject = [item for item in decisions if item["decision"].startswith("REJECT")]

    return {
        "market": market,
        "promote_to_trial": promote,
        "watch": watch,
        "reject": reject,
        "counts": {
            "promote_to_trial": len(promote),
            "watch": len(watch),
            "reject": len(reject),
            "total": len(decisions),
        },
        "all": decisions,
    }


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(path)
    return pd.read_csv(path)


def build_promotion_queue(
    *,
    a_report_path: Path | None,
    a_summary_path: Path | None,
    us_report_path: Path | None,
    us_summary_path: Path | None,
    thresholds: PromotionThresholds | None = None,
) -> dict[str, Any]:
    thresholds = thresholds or PromotionThresholds()
    payload: dict[str, Any] = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "thresholds": asdict(thresholds),
        "markets": {},
        "promotion_queue": [],
    }

    for market, report_path, summary_path in (
        ("a", a_report_path, a_summary_path),
        ("us", us_report_path, us_summary_path),
    ):
        if report_path is None or summary_path is None:
            continue
        report = _read_csv(report_path)
        summary = _read_csv(summary_path)
        market_payload = build_market_queue(market, report, summary, thresholds)
        payload["markets"][market] = market_payload
        payload["promotion_queue"].extend(market_payload["promote_to_trial"])

    payload["promotion_queue"] = sorted(
        payload["promotion_queue"],
        key=lambda item: (item.get("quality_score") or 0, item.get("best_abs_rankic") or 0),
        reverse=True,
    )
    return payload


def save_queue(payload: dict[str, Any], path: Path = QUEUE_PATH) -> Path:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def _default_report_paths(prefix: str, market: str) -> tuple[Path, Path]:
    return ROOT / f"{prefix}_{market}_report.csv", ROOT / f"{prefix}_{market}_summary.csv"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build promotion queue from factor lab outputs")
    parser.add_argument("--prefix", default="factor_candidate")
    parser.add_argument("--a-report", type=Path)
    parser.add_argument("--a-summary", type=Path)
    parser.add_argument("--us-report", type=Path)
    parser.add_argument("--us-summary", type=Path)
    parser.add_argument("--output", type=Path, default=QUEUE_PATH)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    a_report, a_summary = args.a_report, args.a_summary
    us_report, us_summary = args.us_report, args.us_summary
    if a_report is None and a_summary is None:
        a_report, a_summary = _default_report_paths(args.prefix, "a")
    if us_report is None and us_summary is None:
        us_report, us_summary = _default_report_paths(args.prefix, "us")

    payload = build_promotion_queue(
        a_report_path=a_report,
        a_summary_path=a_summary,
        us_report_path=us_report,
        us_summary_path=us_summary,
    )
    save_queue(payload, args.output)

    print("\n══ Factor Promotion Queue ══")
    for market, data in payload["markets"].items():
        counts = data["counts"]
        print(f"{market.upper()}: promote={counts['promote_to_trial']} watch={counts['watch']} reject={counts['reject']} total={counts['total']}")
        for item in data["promote_to_trial"][:10]:
            print(
                f"  + {item['factor']:<20} {item['decision']:<18} "
                f"rankic={item.get('best_abs_rankic', 0):.4f} "
                f"orth={item.get('best_orthogonality', 0) if item.get('best_orthogonality') is not None else float('nan'):.4f}"
            )
    print(f"\n输出: {args.output}")


if __name__ == "__main__":
    main()
