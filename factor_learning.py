#!/usr/bin/env python3
"""Run a bounded factor-learning loop.

The learning loop generates draft factor ideas, runs a lightweight idea-only
screen, and imports promising ideas into the formal candidate registry. It never
modifies active production factor lists.
"""

from __future__ import annotations

import argparse
import json
import time
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from factor_idea_generator import DEFAULT_OUTPUT as DEFAULT_IDEAS_PATH
from factor_idea_generator import generate_ideas
from factor_lab import CandidateRun, build_candidate_report
from factor_learning_state import (
    DEFAULT_STATE_PATH,
    already_screened,
    factor_key,
    idea_learning_score,
    ingest_test_queue,
    load_learning_state,
    mark_screened,
    save_learning_state,
    spec_map_from_paths,
)
from factor_promotion import QUEUE_PATH as DEFAULT_TEST_QUEUE_PATH
from factor_promotion import build_promotion_queue, save_queue
from factor_registry import CANDIDATE_PATH, FactorSpec, dump_factor_specs, load_candidate_specs


ROOT = Path(__file__).resolve().parent
DEFAULT_PREFIX = "factor_learning"
DEFAULT_QUEUE_PATH = ROOT / "factor_learning_queue.json"
DEFAULT_REPORT_PATH = ROOT / "factor_learning_report.md"
DEFAULT_HISTORY_PATH = ROOT / "factor_learning_history.jsonl"


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


def _spec_key(spec: FactorSpec) -> tuple[str, str]:
    return spec.market, spec.name


def _items_sorted(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(
        items,
        key=lambda item: (item.get("quality_score") or 0, item.get("best_abs_rankic") or 0),
        reverse=True,
    )


def _existing_keys(candidate_path: Path, ideas_path: Path | None = None) -> set[str]:
    keys = {factor_key(spec.market, spec.name) for spec in load_candidate_specs(candidate_path)}
    if ideas_path is not None and ideas_path.exists():
        keys.update(factor_key(spec.market, spec.name) for spec in load_candidate_specs(ideas_path))
    return keys


def _variant_name(spec: FactorSpec, params: dict[str, Any]) -> str | None:
    if spec.formula_type == "rolling_mean" and spec.inputs:
        window = int(params.get("window", 0))
        if window <= 0:
            return None
        return f"{spec.inputs[0].replace('_1d', '')}_{window}d"
    if spec.formula_type == "rolling_zscore" and spec.inputs:
        window = int(params.get("window", 0))
        if window <= 0:
            return None
        return f"{spec.inputs[0]}_z{window}"
    if spec.formula_type == "close_roc":
        period = int(params.get("period", 0))
        if period <= 0:
            return None
        return f"roc{period}"
    return None


def _adaptive_windows(base: int) -> list[int]:
    candidates = {max(3, base // 2), max(3, int(round(base * 1.5))), max(3, base + 10)}
    return sorted(w for w in candidates if 3 <= w <= 120 and w != base)


def _generate_adaptive_variants(queue_path: Path, candidate_path: Path, ideas_path: Path) -> list[FactorSpec]:
    """Create nearby variants around factors that recent tests liked."""
    if not queue_path.exists():
        return []
    try:
        queue = json.loads(queue_path.read_text(encoding="utf-8"))
    except Exception:
        return []

    specs = spec_map_from_paths(candidate_path, ideas_path)
    existing = _existing_keys(candidate_path)
    variants: list[FactorSpec] = []
    seen: set[str] = set()
    liked_decisions = {"PROMOTE_TO_TRIAL", "WATCH"}

    for market, payload in (queue.get("markets") or {}).items():
        liked = list(payload.get("promote_to_trial", [])) + list(payload.get("watch", []))
        for item in liked:
            if item.get("decision") not in liked_decisions:
                continue
            base = specs.get(factor_key(market, str(item.get("factor") or "")))
            if base is None:
                continue
            if base.formula_type in {"rolling_mean", "rolling_zscore"}:
                current_window = int(base.params.get("window", 0) or 0)
                for window in _adaptive_windows(current_window):
                    params = dict(base.params)
                    params["window"] = window
                    params["min_periods"] = max(3, window // 2)
                    name = _variant_name(base, params)
                    if not name:
                        continue
                    key = factor_key(base.market, name)
                    if key in existing or key in seen:
                        continue
                    seen.add(key)
                    variants.append(replace(
                        base,
                        name=name,
                        status="idea",
                        source="adaptive",
                        params=params,
                        notes=f"Adaptive variant from {base.name} after {item.get('decision')}",
                        source_score=min(5.0, (base.source_score or 2.0) + 0.5),
                        source_notes=f"Generated from tested factor {base.name}; latest decision={item.get('decision')}.",
                    ))
            elif base.formula_type == "close_roc":
                current_period = int(base.params.get("period", 0) or 0)
                for period in _adaptive_windows(current_period):
                    params = dict(base.params)
                    params["period"] = period
                    name = _variant_name(base, params)
                    if not name:
                        continue
                    key = factor_key(base.market, name)
                    if key in existing or key in seen:
                        continue
                    seen.add(key)
                    variants.append(replace(
                        base,
                        name=name,
                        status="idea",
                        source="adaptive",
                        params=params,
                        notes=f"Adaptive ROC variant from {base.name} after {item.get('decision')}",
                        source_score=min(5.0, (base.source_score or 2.0) + 0.5),
                        source_notes=f"Generated from tested factor {base.name}; latest decision={item.get('decision')}.",
                    ))
    return variants


def _select_learning_ideas(
    ideas: list[FactorSpec],
    state: dict[str, Any],
    limit_per_market: int | None,
    allow_rescreen: bool,
) -> list[FactorSpec]:
    fresh = [spec for spec in ideas if allow_rescreen or not already_screened(spec, state)]
    fresh = sorted(fresh, key=lambda spec: idea_learning_score(spec, state), reverse=True)
    if limit_per_market is None:
        return fresh

    selected: list[FactorSpec] = []
    counts: dict[str, int] = {}
    for spec in fresh:
        count = counts.get(spec.market, 0)
        if count >= limit_per_market:
            continue
        selected.append(spec)
        counts[spec.market] = count + 1
    return selected


def _import_learning_decisions(
    *,
    queue: dict[str, Any],
    ideas_path: Path,
    candidate_path: Path,
    max_promote_import: int,
    max_watch_import: int,
    import_watch: bool,
    dry_run: bool,
) -> list[dict[str, str]]:
    current = load_candidate_specs(candidate_path)
    ideas = load_candidate_specs(ideas_path)
    existing = {_spec_key(spec) for spec in current}
    idea_map = {_spec_key(spec): spec for spec in ideas}
    selected: list[FactorSpec] = []
    changes: list[dict[str, str]] = []

    for market, market_payload in queue.get("markets", {}).items():
        promote_items = _items_sorted(market_payload.get("promote_to_trial", []))[:max_promote_import]
        watch_items = _items_sorted(market_payload.get("watch", []))[:max_watch_import] if import_watch else []
        for item, target_status in [(item, "candidate") for item in promote_items] + [(item, "watch") for item in watch_items]:
            key = (market, str(item["factor"]))
            if key in existing or key not in idea_map:
                continue
            spec = replace(idea_map[key], status=target_status)
            selected.append(spec)
            existing.add(key)
            changes.append({
                "market": market,
                "factor": spec.name,
                "status": target_status,
                "decision": str(item.get("decision") or ""),
            })

    if selected and not dry_run:
        dump_factor_specs(current + selected, candidate_path)
    return changes


def _write_report(
    *,
    path: Path,
    generated_at: str,
    cycles: list[dict[str, Any]],
    latest_queue: dict[str, Any] | None,
    imported: list[dict[str, str]],
    dry_run: bool,
) -> None:
    lines = [
        "# Factor Learning Report",
        "",
        f"- generated_at: {generated_at}",
        f"- mode: {'dry-run' if dry_run else 'write'}",
        f"- cycles: {len(cycles)}",
        "",
        "## Latest Decisions",
    ]

    if latest_queue:
        for market, payload in latest_queue.get("markets", {}).items():
            counts = payload.get("counts", {})
            lines.append(f"- {market.upper()}: promote={counts.get('promote_to_trial', 0)}, watch={counts.get('watch', 0)}, reject={counts.get('reject', 0)}, total={counts.get('total', 0)}")
            for item in _items_sorted(payload.get("promote_to_trial", []))[:8]:
                lines.append(f"- {market.upper()} PROMOTE {item['factor']}: rankic={item.get('best_abs_rankic')}, orth={item.get('best_orthogonality')}")
            for item in _items_sorted(payload.get("watch", []))[:8]:
                lines.append(f"- {market.upper()} WATCH {item['factor']}: rankic={item.get('best_abs_rankic')}, reason={item.get('reason')}")
    else:
        lines.append("- no queue generated")

    section_title = "## Selected For Candidate Import" if dry_run else "## Imported Into Candidate Pool"
    lines.extend(["", section_title])
    if imported:
        for item in imported:
            lines.append(f"- {item['market'].upper()} {item['factor']} -> {item['status']} ({item['decision']})")
    else:
        lines.append("- none")

    lines.extend(["", "## Cycle Log"])
    for cycle in cycles:
        label = "selected" if dry_run else "imported"
        lines.append(f"- cycle={cycle['cycle']} started_at={cycle['started_at']} ideas={cycle['ideas']} {label}={len(cycle.get('imported', []))}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Learn candidate factors for a bounded time window")
    parser.add_argument("--duration-min", type=float, default=60.0, help="Total learning window. Default: 60 minutes.")
    parser.add_argument("--markets", nargs="+", choices=["a", "us"], default=["a", "us"])
    parser.add_argument("--limit-per-market", type=int, default=60)
    parser.add_argument("--max-candidates", type=int, default=60, help="Max idea factors to screen per market per cycle.")
    parser.add_argument("--cycle-sleep-sec", type=float, default=300.0)
    parser.add_argument("--max-cycles", type=int)
    parser.add_argument("--symbols-a", nargs="*", help="Optional A-share subset for faster learning runs.")
    parser.add_argument("--symbols-us", nargs="*", help="Optional US subset for faster learning runs.")
    parser.add_argument("--ideas-path", type=Path, default=DEFAULT_IDEAS_PATH)
    parser.add_argument("--candidate-path", type=Path, default=CANDIDATE_PATH)
    parser.add_argument("--state-path", type=Path, default=DEFAULT_STATE_PATH)
    parser.add_argument("--test-queue-path", type=Path, default=DEFAULT_TEST_QUEUE_PATH)
    parser.add_argument("--prefix", default=DEFAULT_PREFIX)
    parser.add_argument("--queue-path", type=Path, default=DEFAULT_QUEUE_PATH)
    parser.add_argument("--report-path", type=Path, default=DEFAULT_REPORT_PATH)
    parser.add_argument("--history-path", type=Path, default=DEFAULT_HISTORY_PATH)
    parser.add_argument("--max-promote-import", type=int, default=8)
    parser.add_argument("--max-watch-import", type=int, default=8)
    parser.add_argument("--no-import", action="store_true", help="Do not write selected ideas into factor_candidates.yaml.")
    parser.add_argument("--no-import-watch", action="store_true", help="Only import promote-grade ideas.")
    parser.add_argument("--allow-rescreen", action="store_true", help="Allow factors already screened in factor_learning_state.json.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    deadline = time.monotonic() + max(args.duration_min, 0.0) * 60
    cycles: list[dict[str, Any]] = []
    latest_queue: dict[str, Any] | None = None
    all_imported: list[dict[str, str]] = []
    cycle = 0
    state = load_learning_state(args.state_path)
    specs_for_feedback = spec_map_from_paths(args.candidate_path, args.ideas_path)
    state = ingest_test_queue(state, args.test_queue_path, specs_for_feedback)
    save_learning_state(state, args.state_path)

    print("\n══ Factor Learning ══")
    print(f"duration_min: {args.duration_min}")
    print(f"markets: {' '.join(args.markets)}")
    print(f"candidate_path: {args.candidate_path}")

    while cycle == 0 or time.monotonic() < deadline:
        cycle += 1
        started_at = datetime.now(timezone.utc).isoformat()
        base_ideas = generate_ideas(args.markets, limit_per_market=None)
        adaptive_ideas = _generate_adaptive_variants(args.test_queue_path, args.candidate_path, args.ideas_path)
        combined: dict[str, FactorSpec] = {}
        for spec in base_ideas + adaptive_ideas:
            if spec.market not in args.markets:
                continue
            combined[factor_key(spec.market, spec.name)] = spec
        ideas = _select_learning_ideas(
            list(combined.values()),
            state=state,
            limit_per_market=args.limit_per_market,
            allow_rescreen=args.allow_rescreen,
        )
        dump_factor_specs(ideas, args.ideas_path)
        cycle_event: dict[str, Any] = {
            "cycle": cycle,
            "started_at": started_at,
            "base_ideas": len(base_ideas),
            "adaptive_ideas": len(adaptive_ideas),
            "ideas": len(ideas),
            "markets": {},
            "imported": [],
        }
        print(f"\n[cycle {cycle}] ideas={len(ideas)} base={len(base_ideas)} adaptive={len(adaptive_ideas)} output={args.ideas_path}")

        if not ideas:
            print(f"[cycle {cycle}] no fresh ideas after duplicate filter")
            cycles.append(cycle_event)
            save_learning_state(state, args.state_path)
            break

        report_paths: dict[str, tuple[Path, Path]] = {}
        for market in args.markets:
            market_ideas = [spec for spec in ideas if spec.market == market]
            if not market_ideas:
                print(f"[cycle {cycle}] {market.upper()} no new ideas")
                continue
            run = CandidateRun(
                market=market,
                statuses=("idea",),
                max_candidates=args.max_candidates,
                symbols=_symbols_for_market(args, market),
                csv_prefix=args.prefix,
                candidate_path=args.ideas_path,
            )
            panel, report, summary = build_candidate_report(run)
            if report.empty:
                print(f"[cycle {cycle}] {market.upper()} no valid report")
                continue
            _, report_path, summary_path = _write_lab_outputs(args.prefix, market, panel, report, summary)
            report_paths[market] = (report_path, summary_path)
            cycle_event["markets"][market] = {
                "panel_rows": len(panel),
                "report_rows": len(report),
                "summary_rows": len(summary),
                "report_path": str(report_path),
                "summary_path": str(summary_path),
            }
            print(f"[cycle {cycle}] {market.upper()} summary_rows={len(summary)}")

        if report_paths:
            latest_queue = build_promotion_queue(
                a_report_path=report_paths.get("a", (None, None))[0],
                a_summary_path=report_paths.get("a", (None, None))[1],
                us_report_path=report_paths.get("us", (None, None))[0],
                us_summary_path=report_paths.get("us", (None, None))[1],
            )
            save_queue(latest_queue, args.queue_path)
            imported = _import_learning_decisions(
                queue=latest_queue,
                ideas_path=args.ideas_path,
                candidate_path=args.candidate_path,
                max_promote_import=args.max_promote_import,
                max_watch_import=args.max_watch_import,
                import_watch=not args.no_import_watch,
                dry_run=args.no_import,
            )
            cycle_event["imported"] = imported
            all_imported.extend(imported)
            action_label = "selected" if args.no_import else "imported"
            print(f"[cycle {cycle}] queue={args.queue_path} {action_label}={len(imported)}")
            learning_specs = spec_map_from_paths(args.candidate_path, args.ideas_path)
            mark_screened(state, latest_queue, learning_specs)
            save_learning_state(state, args.state_path)

        cycles.append(cycle_event)
        with args.history_path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(cycle_event, ensure_ascii=False) + "\n")
        _write_report(
            path=args.report_path,
            generated_at=datetime.now(timezone.utc).isoformat(),
            cycles=cycles,
            latest_queue=latest_queue,
            imported=all_imported,
            dry_run=args.no_import,
        )

        if args.max_cycles and cycle >= args.max_cycles:
            break
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        time.sleep(min(args.cycle_sleep_sec, remaining))

    print("\n输出:")
    print(f"- ideas: {args.ideas_path}")
    print(f"- queue: {args.queue_path}")
    print(f"- report: {args.report_path}")
    print(f"- history: {args.history_path}")
    print(f"- state: {args.state_path}")


if __name__ == "__main__":
    main()
