#!/usr/bin/env python3
"""Persistent state for adaptive factor learning."""

from __future__ import annotations

import json
import hashlib
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from factor_registry import FactorSpec, load_candidate_specs


ROOT = Path(__file__).resolve().parent
DEFAULT_STATE_PATH = ROOT / "factor_learning_state.json"


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def factor_key(market: str, factor: str) -> str:
    return f"{market.lower()}:{factor}"


def empty_state() -> dict[str, Any]:
    return {
        "version": 1,
        "updated_at": utc_now(),
        "screened": {},
        "direction_scores": {
            "market": {},
            "family": {},
            "formula_type": {},
            "evidence_type": {},
        },
        "last_test_queue": None,
        "last_test_queue_fingerprint": None,
    }


def load_learning_state(path: Path = DEFAULT_STATE_PATH) -> dict[str, Any]:
    if not path.exists():
        return empty_state()
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return empty_state()
    state = empty_state()
    state.update(raw if isinstance(raw, dict) else {})
    state.setdefault("screened", {})
    state.setdefault("direction_scores", {})
    for bucket in ("market", "family", "formula_type", "evidence_type"):
        state["direction_scores"].setdefault(bucket, {})
    return state


def save_learning_state(state: dict[str, Any], path: Path = DEFAULT_STATE_PATH) -> Path:
    state["updated_at"] = utc_now()
    path.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def _bump(scores: dict[str, dict[str, float]], bucket: str, name: str | None, delta: float) -> None:
    if not name:
        return
    current = float(scores.setdefault(bucket, {}).get(name, 0.0))
    scores[bucket][name] = round(max(-12.0, min(12.0, current + delta)), 4)


def decision_delta(decision: str) -> float:
    if decision == "PROMOTE_TO_TRIAL":
        return 3.0
    if decision == "WATCH":
        return 1.0
    if decision == "REJECT_DUPLICATE":
        return -2.0
    if decision == "REJECT":
        return -1.0
    return 0.0


def spec_map_from_paths(*paths: Path) -> dict[str, FactorSpec]:
    out: dict[str, FactorSpec] = {}
    for path in paths:
        for spec in load_candidate_specs(path):
            out[factor_key(spec.market, spec.name)] = spec
    return out


def ingest_test_queue(state: dict[str, Any], queue_path: Path, specs: dict[str, FactorSpec]) -> dict[str, Any]:
    if not queue_path.exists():
        return state
    try:
        raw = queue_path.read_text(encoding="utf-8")
        fingerprint = hashlib.sha256(raw.encode("utf-8")).hexdigest()
        if state.get("last_test_queue_fingerprint") == fingerprint:
            return state
        queue = json.loads(raw)
    except Exception:
        return state

    scores = state.setdefault("direction_scores", {})
    for market, payload in (queue.get("markets") or {}).items():
        for item in payload.get("all", []):
            factor = str(item.get("factor") or "")
            if not factor:
                continue
            key = factor_key(market, factor)
            decision = str(item.get("decision") or "")
            delta = decision_delta(decision)
            spec = specs.get(key)
            family = str(item.get("family") or (spec.family if spec else ""))
            formula_type = spec.formula_type if spec else ""
            evidence_type = spec.evidence_type if spec else ""
            _bump(scores, "market", market, delta * 0.5)
            _bump(scores, "family", family, delta)
            _bump(scores, "formula_type", formula_type, delta * 0.8)
            _bump(scores, "evidence_type", evidence_type, delta * 0.5)
    state["last_test_queue"] = str(queue_path)
    state["last_test_queue_fingerprint"] = fingerprint
    return state


def mark_screened(state: dict[str, Any], queue: dict[str, Any], specs: dict[str, FactorSpec]) -> None:
    screened = state.setdefault("screened", {})
    scores = state.setdefault("direction_scores", {})
    now = utc_now()
    for market, payload in (queue.get("markets") or {}).items():
        for item in payload.get("all", []):
            factor = str(item.get("factor") or "")
            if not factor:
                continue
            key = factor_key(market, factor)
            decision = str(item.get("decision") or "")
            spec = specs.get(key)
            entry = screened.setdefault(key, {
                "market": market,
                "factor": factor,
                "first_seen": now,
                "times_screened": 0,
            })
            entry["last_seen"] = now
            entry["times_screened"] = int(entry.get("times_screened") or 0) + 1
            entry["last_decision"] = decision
            entry["family"] = str(item.get("family") or (spec.family if spec else ""))
            entry["formula_type"] = spec.formula_type if spec else ""
            entry["quality_score"] = item.get("quality_score")

            delta = decision_delta(decision)
            _bump(scores, "market", market, delta * 0.25)
            _bump(scores, "family", entry.get("family"), delta * 0.5)
            _bump(scores, "formula_type", entry.get("formula_type"), delta * 0.4)
            _bump(scores, "evidence_type", spec.evidence_type if spec else "", delta * 0.25)


def idea_learning_score(spec: FactorSpec, state: dict[str, Any]) -> float:
    scores = state.get("direction_scores") or {}
    source_score = spec.source_score if spec.source_score is not None else 2.0
    return float(source_score) + float(scores.get("market", {}).get(spec.market, 0.0)) + float(scores.get("family", {}).get(spec.family, 0.0)) + float(scores.get("formula_type", {}).get(spec.formula_type, 0.0)) + float(scores.get("evidence_type", {}).get(spec.evidence_type, 0.0))


def already_screened(spec: FactorSpec, state: dict[str, Any]) -> bool:
    return factor_key(spec.market, spec.name) in (state.get("screened") or {})
