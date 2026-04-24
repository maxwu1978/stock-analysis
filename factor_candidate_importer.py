#!/usr/bin/env python3
"""Import reviewed factor ideas into the formal candidate registry."""

from __future__ import annotations

import argparse
from pathlib import Path

from factor_registry import CANDIDATE_PATH, FactorSpec, dump_factor_specs, load_candidate_specs


ROOT = Path(__file__).resolve().parent
DEFAULT_IDEAS_PATH = ROOT / "factor_candidate_ideas.yaml"


def _spec_key(spec: FactorSpec) -> tuple[str, str]:
    return spec.market, spec.name


def import_ideas(
    *,
    ideas_path: Path,
    candidate_path: Path,
    markets: set[str] | None,
    names: set[str] | None,
    status: str,
    dry_run: bool,
) -> tuple[list[FactorSpec], list[FactorSpec]]:
    current = load_candidate_specs(candidate_path)
    ideas = load_candidate_specs(ideas_path)
    existing = {_spec_key(spec) for spec in current}

    selected: list[FactorSpec] = []
    for idea in ideas:
        if markets and idea.market not in markets:
            continue
        if names and idea.name not in names:
            continue
        if _spec_key(idea) in existing:
            continue
        selected.append(FactorSpec(
            name=idea.name,
            market=idea.market,
            family=idea.family,
            status=status,
            source=idea.source,
            formula_type=idea.formula_type,
            inputs=idea.inputs,
            params=idea.params,
            notes=idea.notes,
            owner=idea.owner,
            source_url=idea.source_url,
            evidence_type=idea.evidence_type,
            source_score=idea.source_score,
            source_notes=idea.source_notes,
        ))

    if not dry_run and selected:
        dump_factor_specs(current + selected, candidate_path)

    return current, selected


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Import factor ideas into factor_candidates.yaml")
    parser.add_argument("--ideas", type=Path, default=DEFAULT_IDEAS_PATH)
    parser.add_argument("--candidate-path", type=Path, default=CANDIDATE_PATH)
    parser.add_argument("--market", choices=["a", "us"], action="append")
    parser.add_argument("--name", action="append", help="Import only the named factor. Can be repeated.")
    parser.add_argument("--status", choices=["candidate", "watch"], default="candidate")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    _, selected = import_ideas(
        ideas_path=args.ideas,
        candidate_path=args.candidate_path,
        markets=set(args.market) if args.market else None,
        names=set(args.name) if args.name else None,
        status=args.status,
        dry_run=args.dry_run,
    )

    print("\n══ Factor Candidate Importer ══")
    print(f"ideas: {args.ideas}")
    print(f"target: {args.candidate_path}")
    print(f"selected: {len(selected)}")
    print(f"mode: {'dry-run' if args.dry_run else 'write'}")
    for spec in selected:
        print(f"- {spec.market} {spec.name} -> {spec.status}")


if __name__ == "__main__":
    main()
