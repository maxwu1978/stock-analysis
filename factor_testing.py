#!/usr/bin/env python3
"""Evaluate the formal candidate pool and update research-stage statuses."""

from __future__ import annotations

import argparse
import json
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from factor_lab import CandidateRun, build_candidate_report
from factor_promotion import QUEUE_PATH, build_promotion_queue, save_queue
from factor_registry import CANDIDATE_PATH, FactorSpec, dump_factor_specs, load_candidate_specs


ROOT = Path(__file__).resolve().parent
DEFAULT_PREFIX = "factor_candidate"
DEFAULT_REPORT_PATH = ROOT / "factor_test_decisions.md"
DEFAULT_RUNTIME_OVERLAY_PATH = ROOT / "factor_runtime_overlay.json"


def _write_lab_outputs(prefix: str, market: str, panel: pd.DataFrame, report: pd.DataFrame, summary: pd.DataFrame) -> tuple[Path, Path, Path]:
    panel_path = ROOT / f"{prefix}_{market}_panel.csv"
    report_path = ROOT / f"{prefix}_{market}_report.csv"
    summary_path = ROOT / f"{prefix}_{market}_summary.csv"
    panel.to_csv(panel_path, index=False)
    report.to_csv(report_path, index=False)
    summary.to_csv(summary_path, index=False)
    return panel_path, report_path, summary_path


def _symbols_for_market(args: argparse.Namespace, market: str) -> list[str] | None:
    if market == "a":
        return args.symbols_a
    if market == "us":
        return args.symbols_us
    return None


def _decision_map(queue: dict[str, Any]) -> dict[tuple[str, str], str]:
    out: dict[tuple[str, str], str] = {}
    for market, payload in queue.get("markets", {}).items():
        for item in payload.get("all", []):
            out[(market, str(item["factor"]))] = str(item["decision"])
    return out


def _apply_registry_decisions(
    *,
    queue: dict[str, Any],
    candidate_path: Path,
    apply_rejections: bool,
    dry_run: bool,
) -> list[dict[str, str]]:
    specs = load_candidate_specs(candidate_path)
    decisions = _decision_map(queue)
    updated: list[FactorSpec] = []
    changes: list[dict[str, str]] = []

    for spec in specs:
        decision = decisions.get((spec.market, spec.name))
        new_status = spec.status
        if decision == "PROMOTE_TO_TRIAL":
            new_status = "trial"
        elif decision == "WATCH":
            new_status = "watch"
        elif apply_rejections and decision in {"REJECT", "REJECT_DUPLICATE"}:
            new_status = "rejected"

        if new_status != spec.status:
            changes.append({
                "market": spec.market,
                "factor": spec.name,
                "from": spec.status,
                "to": new_status,
                "decision": decision or "",
            })
            updated.append(replace(spec, status=new_status))
        else:
            updated.append(spec)

    if changes and not dry_run:
        dump_factor_specs(updated, candidate_path)
    return changes


def _write_runtime_overlay(candidate_path: Path, output_path: Path) -> Path:
    specs = load_candidate_specs(candidate_path)
    payload: dict[str, Any] = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source": "factor_testing.py",
        "activation_rule": "status == trial",
        "note": "Runtime decision models read these trial factors dynamically; FACTOR_COLS / US_FACTOR_COLS are not edited.",
        "markets": {"a": [], "us": []},
    }
    for spec in specs:
        if spec.status != "trial":
            continue
        payload["markets"].setdefault(spec.market, []).append({
            "factor": spec.name,
            "family": spec.family,
            "formula_type": spec.formula_type,
            "inputs": spec.inputs,
            "params": spec.params,
            "source": spec.source,
            "source_score": spec.source_score,
            "evidence_type": spec.evidence_type,
        })
    output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return output_path


def _write_decision_report(path: Path, queue: dict[str, Any], changes: list[dict[str, str]], dry_run: bool, apply_rejections: bool) -> None:
    lines = [
        "# Factor Test Decisions",
        "",
        f"- generated_at: {datetime.now(timezone.utc).isoformat()}",
        f"- mode: {'dry-run' if dry_run else 'write'}",
        f"- apply_rejections: {apply_rejections}",
        "",
        "## Market Summary",
    ]
    for market, payload in queue.get("markets", {}).items():
        counts = payload.get("counts", {})
        lines.append(f"- {market.upper()}: promote={counts.get('promote_to_trial', 0)}, watch={counts.get('watch', 0)}, reject={counts.get('reject', 0)}, total={counts.get('total', 0)}")

    lines.extend(["", "## Status Changes"])
    if changes:
        for item in changes:
            lines.append(f"- {item['market'].upper()} {item['factor']}: {item['from']} -> {item['to']} ({item['decision']})")
    else:
        lines.append("- none")

    lines.extend(["", "## Promote To Trial"])
    promoted = queue.get("promotion_queue", [])
    if promoted:
        for item in promoted[:20]:
            lines.append(f"- {item['market'].upper()} {item['factor']}: rankic={item.get('best_abs_rankic')}, orth={item.get('best_orthogonality')}, reason={item.get('reason')}")
    else:
        lines.append("- none")

    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Test formal candidate factors and update trial/watch statuses")
    parser.add_argument("--markets", nargs="+", choices=["a", "us"], default=["a", "us"])
    parser.add_argument("--statuses", nargs="+", default=["candidate", "watch", "trial"])
    parser.add_argument("--max-candidates", type=int)
    parser.add_argument("--symbols-a", nargs="*", help="Optional A-share subset for faster test runs.")
    parser.add_argument("--symbols-us", nargs="*", help="Optional US subset for faster test runs.")
    parser.add_argument("--candidate-path", type=Path, default=CANDIDATE_PATH)
    parser.add_argument("--prefix", default=DEFAULT_PREFIX)
    parser.add_argument("--queue-path", type=Path, default=QUEUE_PATH)
    parser.add_argument("--report-path", type=Path, default=DEFAULT_REPORT_PATH)
    parser.add_argument("--runtime-overlay-path", type=Path, default=DEFAULT_RUNTIME_OVERLAY_PATH)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--apply-rejections", action="store_true", help="Mark REJECT/REJECT_DUPLICATE as rejected in factor_candidates.yaml.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    print("\n══ Factor Testing ══")
    print(f"markets: {' '.join(args.markets)}")
    print(f"statuses: {' '.join(args.statuses)}")
    print(f"candidate_path: {args.candidate_path}")

    report_paths: dict[str, tuple[Path, Path]] = {}
    for market in args.markets:
        run = CandidateRun(
            market=market,
            statuses=tuple(args.statuses),
            max_candidates=args.max_candidates,
            symbols=_symbols_for_market(args, market),
            csv_prefix=args.prefix,
            candidate_path=args.candidate_path,
        )
        panel, report, summary = build_candidate_report(run)
        if report.empty:
            print(f"[{market.upper()}] no valid candidate report")
            continue
        _, report_path, summary_path = _write_lab_outputs(args.prefix, market, panel, report, summary)
        report_paths[market] = (report_path, summary_path)
        print(f"[{market.upper()}] summary_rows={len(summary)} report={report_path}")

    if not report_paths:
        raise SystemExit("no candidate reports generated")

    queue = build_promotion_queue(
        a_report_path=report_paths.get("a", (None, None))[0],
        a_summary_path=report_paths.get("a", (None, None))[1],
        us_report_path=report_paths.get("us", (None, None))[0],
        us_summary_path=report_paths.get("us", (None, None))[1],
    )
    save_queue(queue, args.queue_path)
    changes = _apply_registry_decisions(
        queue=queue,
        candidate_path=args.candidate_path,
        apply_rejections=args.apply_rejections,
        dry_run=args.dry_run,
    )
    overlay_path = None
    if not args.dry_run:
        overlay_path = _write_runtime_overlay(args.candidate_path, args.runtime_overlay_path)
    _write_decision_report(args.report_path, queue, changes, args.dry_run, args.apply_rejections)

    print("\n输出:")
    print(f"- queue: {args.queue_path}")
    print(f"- report: {args.report_path}")
    print(f"- status_changes: {len(changes)}")
    if not args.dry_run:
        print(f"- registry: {args.candidate_path}")
        print(f"- runtime_overlay: {overlay_path}")


if __name__ == "__main__":
    main()
