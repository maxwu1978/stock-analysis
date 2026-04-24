#!/usr/bin/env python3
"""Registry helpers for active and candidate factors."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from factor_weighting import infer_factor_family


ROOT = Path(__file__).resolve().parent
CANDIDATE_PATH = ROOT / "factor_candidates.yaml"


@dataclass(slots=True)
class FactorSpec:
    name: str
    market: str
    family: str
    status: str
    source: str = "manual"
    formula_type: str = "custom"
    inputs: list[str] = field(default_factory=list)
    params: dict[str, Any] = field(default_factory=dict)
    notes: str = ""
    owner: str = "research"
    source_url: str = ""
    evidence_type: str = ""
    source_score: float | None = None
    source_notes: str = ""

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "FactorSpec":
        name = str(data["name"])
        market = str(data["market"]).lower()
        family = str(data.get("family") or infer_factor_family(name))
        status = str(data.get("status") or "candidate").lower()
        raw_score = data.get("source_score")
        source_score = None
        if raw_score not in (None, ""):
            try:
                source_score = float(raw_score)
            except (TypeError, ValueError):
                source_score = None
        return cls(
            name=name,
            market=market,
            family=family,
            status=status,
            source=str(data.get("source") or "manual"),
            formula_type=str(data.get("formula_type") or "custom"),
            inputs=[str(v) for v in data.get("inputs", [])],
            params=dict(data.get("params") or {}),
            notes=str(data.get("notes") or ""),
            owner=str(data.get("owner") or "research"),
            source_url=str(data.get("source_url") or ""),
            evidence_type=str(data.get("evidence_type") or ""),
            source_score=source_score,
            source_notes=str(data.get("source_notes") or ""),
        )

    def to_dict(self) -> dict[str, Any]:
        out: dict[str, Any] = {
            "name": self.name,
            "market": self.market,
            "family": self.family,
            "status": self.status,
            "source": self.source,
            "formula_type": self.formula_type,
            "inputs": self.inputs,
            "params": self.params,
            "notes": self.notes,
            "owner": self.owner,
        }
        if self.source_url:
            out["source_url"] = self.source_url
        if self.evidence_type:
            out["evidence_type"] = self.evidence_type
        if self.source_score is not None:
            out["source_score"] = self.source_score
        if self.source_notes:
            out["source_notes"] = self.source_notes
        return out


def active_factor_names(market: str) -> list[str]:
    market = market.lower()
    if market == "a":
        from probability import FACTOR_COLS

        return list(FACTOR_COLS)
    if market == "us":
        from probability_us import US_FACTOR_COLS

        return list(US_FACTOR_COLS)
    raise ValueError(f"unknown market: {market}")


def active_factor_specs(market: str) -> list[FactorSpec]:
    return [
        FactorSpec(
            name=name,
            market=market.lower(),
            family=infer_factor_family(name),
            status="active",
            source="production",
            formula_type="precomputed",
            notes="Current active model factor",
            owner="model",
        )
        for name in active_factor_names(market)
    ]


def load_candidate_specs(path: Path = CANDIDATE_PATH) -> list[FactorSpec]:
    if not path.exists():
        return []
    raw = yaml.safe_load(path.read_text(encoding="utf-8")) or []
    if not isinstance(raw, list):
        raise ValueError("factor_candidates.yaml must contain a top-level list")
    return [FactorSpec.from_dict(item) for item in raw if isinstance(item, dict)]


def list_factor_specs(
    market: str,
    statuses: tuple[str, ...] | list[str] | None = None,
    *,
    include_active: bool = False,
    path: Path = CANDIDATE_PATH,
) -> list[FactorSpec]:
    market = market.lower()
    wanted_status = {s.lower() for s in statuses} if statuses else None

    specs: list[FactorSpec] = []
    if include_active:
        specs.extend(active_factor_specs(market))

    for spec in load_candidate_specs(path):
        if spec.market != market:
            continue
        if wanted_status and spec.status not in wanted_status:
            continue
        specs.append(spec)
    return specs


def candidate_factor_names(
    market: str,
    statuses: tuple[str, ...] | list[str] | None = None,
    *,
    path: Path = CANDIDATE_PATH,
) -> list[str]:
    return [spec.name for spec in list_factor_specs(market, statuses=statuses, include_active=False, path=path)]


def dump_factor_specs(specs: list[FactorSpec], path: Path) -> Path:
    """Write factor specs to YAML in registry format."""
    payload = [spec.to_dict() for spec in specs]
    path.write_text(yaml.safe_dump(payload, allow_unicode=True, sort_keys=False), encoding="utf-8")
    return path
