#!/usr/bin/env python3
"""Batch-evaluate candidate factors without touching the active model."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd

from candidate_factor_engine import build_candidate_factors
from factor_registry import CANDIDATE_PATH, active_factor_names, list_factor_specs
from factor_tear_sheet import CONFIGS, _calc_factor_metrics, _fetch_series


@dataclass
class CandidateRun:
    market: str
    statuses: tuple[str, ...]
    max_candidates: int | None
    symbols: list[str] | None
    csv_prefix: str
    candidate_path: Path


def _filter_universe(universe: dict[str, str], symbols: list[str] | None) -> dict[str, str]:
    if not symbols:
        return universe
    wanted = {s.upper() for s in symbols}
    return {symbol: name for symbol, name in universe.items() if symbol.upper() in wanted}


def collect_candidate_panel(run: CandidateRun) -> tuple[pd.DataFrame, list[str], list[str]]:
    cfg = CONFIGS[run.market]
    specs = list_factor_specs(run.market, statuses=run.statuses, path=run.candidate_path)
    if run.max_candidates:
        specs = specs[: run.max_candidates]

    candidate_names = [spec.name for spec in specs]
    active_names = list(active_factor_names(run.market))

    frames = []
    for symbol, name in _filter_universe(cfg.universe, run.symbols).items():
        try:
            df = _fetch_series(cfg, symbol)
        except Exception as exc:
            print(f"[!] {name}({symbol}) 失败: {exc}")
            continue
        if df is None or df.empty:
            continue

        df, _ = build_candidate_factors(df, specs, run.market)
        sub = df.copy()
        sub["symbol"] = symbol
        sub["name"] = name
        for h in cfg.horizons:
            sub[f"fwd_{h}d_ret"] = sub["close"].shift(-h) / sub["close"] - 1
            sub[f"fwd_{h}d_up"] = (sub[f"fwd_{h}d_ret"] > 0).astype(float)

        keep = ["symbol", "name", "close"] + [c for c in active_names if c in sub.columns]
        keep += [c for c in candidate_names if c in sub.columns]
        keep += [f"fwd_{h}d_ret" for h in cfg.horizons] + [f"fwd_{h}d_up" for h in cfg.horizons]
        sub = sub[keep].copy()
        sub.index.name = "date"
        frames.append(sub.reset_index())
        print(f"[+] {name}({symbol}) {len(sub)} rows")

    if not frames:
        return pd.DataFrame(), candidate_names, active_names
    return pd.concat(frames, ignore_index=True), candidate_names, active_names


def _max_active_corr(panel: pd.DataFrame, factor: str, active_names: list[str]) -> tuple[float | None, str | None]:
    best_corr = None
    best_name = None
    for active in active_names:
        if active not in panel.columns:
            continue
        sub = panel[[factor, active]].dropna()
        if len(sub) < 80:
            continue
        corr = abs(float(sub[factor].corr(sub[active])))
        if np.isnan(corr):
            continue
        if best_corr is None or corr > best_corr:
            best_corr = corr
            best_name = active
    return best_corr, best_name


def _cross_symbol_consistency(panel: pd.DataFrame, factor: str, horizon: int, pooled_rankic: float | None) -> float | None:
    ret_col = f"fwd_{horizon}d_ret"
    signs = []
    pooled_sign = np.sign(pooled_rankic) if pooled_rankic is not None and not pd.isna(pooled_rankic) else 0
    if pooled_sign == 0:
        return None

    for _, group in panel.groupby("symbol"):
        sub = group[[factor, ret_col]].dropna()
        if len(sub) < 50:
            continue
        rk = sub[factor].rank().corr(sub[ret_col].rank())
        if pd.isna(rk) or abs(rk) < 0.02:
            continue
        signs.append(np.sign(rk))

    if len(signs) < 2:
        return None
    return round(float(np.mean(np.array(signs) == pooled_sign) * 100), 1)


def _decision_hint(row: dict) -> str:
    coverage = row.get("coverage") or 0
    rankic = row.get("rankic")
    stable = bool(row.get("stable_halves"))
    max_corr = row.get("max_corr_active")
    consistency = row.get("cross_symbol_consistency")

    if coverage < 50 or rankic is None or pd.isna(rankic) or abs(rankic) < 0.01:
        return "REJECT"
    if max_corr is not None and max_corr > 0.85 and abs(rankic) < 0.04:
        return "REJECT_DUPLICATE"
    if stable and abs(rankic) >= 0.04 and (consistency is None or consistency >= 55):
        return "PROMOTE_TO_TRIAL"
    if abs(rankic) >= 0.02:
        return "WATCH"
    return "REJECT"


def build_candidate_report(run: CandidateRun) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    cfg = CONFIGS[run.market]
    panel, candidate_names, active_names = collect_candidate_panel(run)
    if panel.empty:
        return panel, pd.DataFrame(), pd.DataFrame()

    spec_map = {spec.name: spec for spec in list_factor_specs(run.market, statuses=run.statuses, path=run.candidate_path)}
    rows = []
    for factor in candidate_names:
        if factor not in panel.columns:
            continue
        for horizon in cfg.horizons:
            row = _calc_factor_metrics(panel, factor, horizon)
            spec = spec_map.get(factor)
            if spec is not None:
                row["family"] = spec.family
                row["status"] = spec.status
            max_corr, closest = _max_active_corr(panel, factor, active_names)
            orthogonality = None if max_corr is None else round(1 - max_corr, 4)
            consistency = _cross_symbol_consistency(panel, factor, horizon, row.get("rankic"))
            row["status"] = row.get("status") or "candidate"
            row["cross_symbol_consistency"] = consistency
            row["max_corr_active"] = round(max_corr, 4) if max_corr is not None else None
            row["closest_active_factor"] = closest
            row["orthogonality_score"] = orthogonality
            row["decision_hint"] = _decision_hint(row)
            rows.append(row)

    report = pd.DataFrame(rows)
    if report.empty:
        return panel, report, pd.DataFrame()

    summary = (
        report.groupby(["factor", "family", "status"], as_index=False)
        .agg(
            best_abs_rankic=("rankic", lambda s: round(float(s.dropna().abs().max()), 4) if len(s.dropna()) else np.nan),
            mean_abs_rankic=("rankic", lambda s: round(float(s.dropna().abs().mean()), 4) if len(s.dropna()) else np.nan),
            stable_count=("stable_halves", "sum"),
            mean_consistency=("cross_symbol_consistency", lambda s: round(float(s.dropna().mean()), 1) if len(s.dropna()) else np.nan),
            best_orthogonality=("orthogonality_score", lambda s: round(float(s.dropna().max()), 4) if len(s.dropna()) else np.nan),
            closest_active_factor=("closest_active_factor", lambda s: next((v for v in s if isinstance(v, str) and v), None)),
            best_decision=("decision_hint", lambda s: max(s, key=lambda v: {"PROMOTE_TO_TRIAL": 3, "WATCH": 2, "REJECT_DUPLICATE": 1, "REJECT": 0}.get(v, 0))),
            max_coverage=("coverage", "max"),
        )
    )
    decision_rank = {"PROMOTE_TO_TRIAL": 3, "WATCH": 2, "REJECT_DUPLICATE": 1, "REJECT": 0}
    summary["_decision_rank"] = summary["best_decision"].map(decision_rank).fillna(-1)
    summary["quality_score"] = (
        summary["mean_abs_rankic"].fillna(0) * 100
        + summary["stable_count"].fillna(0) * 2
        + summary["best_orthogonality"].fillna(0) * 5
        + summary["max_coverage"].fillna(0) / 10
    ).round(2)
    summary = summary.sort_values(["_decision_rank", "best_abs_rankic"], ascending=[False, False]).drop(columns="_decision_rank").reset_index(drop=True)
    return panel, report, summary


def print_candidate_report(report: pd.DataFrame, summary: pd.DataFrame) -> None:
    if report.empty:
        print("无有效候选因子报告.")
        return
    print("\n" + "=" * 96)
    print("  Candidate Factor Lab")
    print("=" * 96)

    promoted = summary[summary["best_decision"] == "PROMOTE_TO_TRIAL"].head(10)
    print("\n[1] 晋升试验池候选")
    if promoted.empty:
        print("无")
    else:
        print(promoted[["factor", "family", "best_abs_rankic", "stable_count", "best_orthogonality", "closest_active_factor"]].to_string(index=False))

    watch = summary[summary["best_decision"] == "WATCH"].head(12)
    print("\n[2] 观察名单")
    if watch.empty:
        print("无")
    else:
        print(watch[["factor", "family", "best_abs_rankic", "mean_consistency", "closest_active_factor"]].to_string(index=False))

    rejects = summary[summary["best_decision"].str.startswith("REJECT", na=False)].head(12)
    print("\n[3] 明显应淘汰")
    if rejects.empty:
        print("无")
    else:
        print(rejects[["factor", "family", "best_abs_rankic", "best_orthogonality", "closest_active_factor", "best_decision"]].to_string(index=False))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Batch-evaluate candidate factors")
    parser.add_argument("--market", choices=["a", "us"], default="a")
    parser.add_argument("--statuses", nargs="*", default=["candidate"])
    parser.add_argument("--max-candidates", type=int)
    parser.add_argument("--symbols", nargs="*")
    parser.add_argument("--csv-prefix", default="factor_candidate")
    parser.add_argument("--candidate-path", type=Path, default=CANDIDATE_PATH)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    run = CandidateRun(
        market=args.market,
        statuses=tuple(args.statuses),
        max_candidates=args.max_candidates,
        symbols=args.symbols,
        csv_prefix=args.csv_prefix,
        candidate_path=args.candidate_path,
    )

    panel, report, summary = build_candidate_report(run)
    if report.empty:
        raise SystemExit(1)

    panel_path = f"{run.csv_prefix}_{run.market}_panel.csv"
    report_path = f"{run.csv_prefix}_{run.market}_report.csv"
    summary_path = f"{run.csv_prefix}_{run.market}_summary.csv"
    panel.to_csv(panel_path, index=False)
    report.to_csv(report_path, index=False)
    summary.to_csv(summary_path, index=False)
    print_candidate_report(report, summary)
    print(f"\n输出: {panel_path}")
    print(f"输出: {report_path}")
    print(f"输出: {summary_path}")


if __name__ == "__main__":
    main()
