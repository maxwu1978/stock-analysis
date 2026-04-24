#!/usr/bin/env python3
"""Generate draft candidate-factor ideas from active factors and research themes."""

from __future__ import annotations

import argparse
from datetime import datetime
from pathlib import Path

from factor_registry import (
    FactorSpec,
    active_factor_names,
    dump_factor_specs,
    load_candidate_specs,
)
from factor_weighting import infer_factor_family


ROOT = Path(__file__).resolve().parent
DEFAULT_OUTPUT = ROOT / "factor_candidate_ideas.yaml"


MARKET_BASE_INPUTS = {
    "a": {
        "rolling_mean": {
            "gap_ret_1d": [3, 5, 20, 30],
            "intraday_ret_1d": [3, 5, 10],
            "amihud_daily": [10, 30, 60],
        },
        "rolling_zscore": {
            "price_position": [20, 60],
            "vol_surge": [20, 60],
            "gap_ret_10d": [20],
            "amihud_20d": [20],
        },
        "close_roc": [15, 30, 60],
    },
    "us": {
        "rolling_mean": {
            "gap_ret_1d": [3, 5, 10],
            "intraday_ret_1d": [3, 5],
        },
        "rolling_zscore": {
            "atr_pct": [20, 60],
            "vol_change": [20],
            "high_low_range": [20],
            "vix_z20": [20],
        },
        "close_roc": [15, 30, 60],
    },
}


THEME_INTERACTIONS = {
    "a": [
        ("industry", "ai_mfg_heat_proxy", "ROC20", "vol_surge", "AI/制造链热度：动量与放量共振"),
        ("industry", "ai_mfg_leader_proxy", "high52w_pos", "ROC20", "AI/制造链龙头强势：年度位置与中期动量"),
        ("industry", "quality_flow_proxy", "cash_flow_ps", "vol_surge", "现金流质量与资金活跃共振"),
        ("industry", "earnings_accel_proxy", "roe_chg", "rev_growth_accel", "业绩改善与营收加速度共振"),
        ("macro", "policy_beta_proxy", "price_position", "vol_surge", "政策风险偏好代理"),
        ("macro", "liquidity_stress_proxy", "gap_ret_10d", "amihud_20d", "流动性压力与跳空收益共振"),
        ("fractal", "mfdfa_x_vol_surge", "mfdfa_width_centered_120", "vol_surge", "分形复杂度与量能异动共振"),
    ],
    "us": [
        ("industry", "ai_chain_heat_proxy", "ROC20", "vol_surge", "AI链热度：动量与放量共振"),
        ("industry", "ai_chain_leader_proxy", "high52w_pos", "ROC20", "AI链龙头强势：年度位置与中期动量"),
        ("industry", "ai_chain_vol_proxy", "atr_pct", "high52w_pos", "AI链波动成本与年度位置"),
        ("industry", "mfdfa_x_high52w", "mfdfa_width_centered_120", "high52w_pos", "分形复杂度与龙头位置"),
        ("industry", "vol_change_x_high52w", "vol_change", "high52w_pos", "成交变化与龙头位置"),
        ("macro", "vix_z20_x_high52w", "vix_z20", "high52w_pos", "VIX偏离与年度位置"),
        ("macro", "vix_z20_x_atr_pct", "vix_z20", "atr_pct", "VIX偏离与个股波动"),
        ("macro", "vix_roc5_x_vol_surge", "vix_roc5", "vol_surge", "VIX变化与量能异动"),
        ("macro", "vix_ma20_diff_x_roc20", "vix_ma20_diff", "ROC20", "VIX风险溢价与中期动量"),
    ],
}


def _existing_names() -> set[tuple[str, str]]:
    names = {(spec.market, spec.name) for spec in load_candidate_specs()}
    for market in ("a", "us"):
        names.update((market, name) for name in active_factor_names(market))
    return names


def _spec(
    *,
    name: str,
    market: str,
    family: str | None = None,
    formula_type: str,
    inputs: list[str] | None = None,
    params: dict | None = None,
    notes: str,
    source: str = "generated",
    source_url: str = "",
    evidence_type: str = "template",
    source_score: float | None = None,
    source_notes: str = "",
) -> FactorSpec:
    return FactorSpec(
        name=name,
        market=market,
        family=family or infer_factor_family(name),
        status="idea",
        source=source,
        formula_type=formula_type,
        inputs=inputs or [],
        params=params or {},
        notes=notes,
        owner="idea_generator",
        source_url=source_url,
        evidence_type=evidence_type,
        source_score=source_score,
        source_notes=source_notes,
    )


def _source_meta(market: str, formula_type: str, inputs: list[str] | None = None) -> dict:
    inputs = inputs or []
    joined = " ".join(inputs).lower()
    if "amihud" in joined:
        return {
            "source_url": "https://doi.org/10.1016/S0304-405X(01)00024-6",
            "evidence_type": "academic_formula",
            "source_score": 5.0,
            "source_notes": "Amihud illiquidity family; local test still required.",
        }
    if "vix" in joined:
        return {
            "source_url": "https://fred.stlouisfed.org/docs/api/fred/",
            "evidence_type": "macro_data_proxy",
            "source_score": 4.0,
            "source_notes": "Macro risk proxy built from VIX-derived features.",
        }
    if any(token in joined for token in ("gap_ret", "intraday_ret")):
        return {
            "source_url": "",
            "evidence_type": "academic_formula",
            "source_score": 4.0,
            "source_notes": "Overnight/intraday return decomposition hypothesis.",
        }
    if formula_type == "close_roc":
        return {
            "source_url": "https://qlib.readthedocs.io/",
            "evidence_type": "open_source_template",
            "source_score": 4.0,
            "source_notes": "Common momentum template; verify under local universe.",
        }
    if formula_type == "interaction":
        return {
            "source_url": "",
            "evidence_type": "theme_proxy",
            "source_score": 2.0,
            "source_notes": f"{market.upper()} theme interaction; requires shadow validation.",
        }
    return {
        "source_url": "https://arxiv.org/abs/1601.00991",
        "evidence_type": "formula_variant",
        "source_score": 3.0,
        "source_notes": "Formulaic alpha style transformation; local evidence required.",
    }


def generate_ideas(markets: list[str], limit_per_market: int | None = None, include_existing: bool = False) -> list[FactorSpec]:
    existing = set() if include_existing else _existing_names()
    out: list[FactorSpec] = []

    for market in markets:
        market = market.lower()
        generated: list[FactorSpec] = []

        cfg = MARKET_BASE_INPUTS[market]
        for src, windows in cfg["rolling_mean"].items():
            for window in windows:
                meta = _source_meta(market, "rolling_mean", [src])
                generated.append(_spec(
                    name=f"{src.replace('_1d', '')}_{window}d",
                    market=market,
                    family="behavior",
                    formula_type="rolling_mean",
                    inputs=[src],
                    params={"window": window, "min_periods": max(3, window // 2)},
                    notes=f"{src} 的 {window} 日滚动均值变体",
                    **meta,
                ))

        for src, windows in cfg["rolling_zscore"].items():
            for window in windows:
                meta = _source_meta(market, "rolling_zscore", [src])
                generated.append(_spec(
                    name=f"{src}_z{window}",
                    market=market,
                    formula_type="rolling_zscore",
                    inputs=[src],
                    params={"window": window, "min_periods": max(3, window // 2)},
                    notes=f"{src} 的 {window} 日滚动标准化变体",
                    **meta,
                ))

        for period in cfg["close_roc"]:
            meta = _source_meta(market, "close_roc", [])
            generated.append(_spec(
                name=f"roc{period}",
                market=market,
                family="trend",
                formula_type="close_roc",
                params={"period": period},
                notes=f"{period} 日价格动量候选",
                **meta,
            ))

        for family, name, left, right, notes in THEME_INTERACTIONS[market]:
            meta = _source_meta(market, "interaction", [left, right])
            generated.append(_spec(
                name=name,
                market=market,
                family=family,
                formula_type="interaction",
                inputs=[left, right],
                notes=notes,
                **meta,
            ))

        deduped = []
        seen_names: set[str] = set()
        for item in generated:
            key = (item.market, item.name)
            if key in existing or item.name in seen_names:
                continue
            seen_names.add(item.name)
            deduped.append(item)
        if limit_per_market:
            deduped = deduped[:limit_per_market]
        out.extend(deduped)

    return out


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate draft factor candidate ideas")
    parser.add_argument("--markets", nargs="+", choices=["a", "us"], default=["a", "us"])
    parser.add_argument("--limit-per-market", type=int)
    parser.add_argument("--include-existing", action="store_true", help="Also emit ideas already present in active/candidate pools")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    ideas = generate_ideas(
        markets=args.markets,
        limit_per_market=args.limit_per_market,
        include_existing=args.include_existing,
    )
    dump_factor_specs(ideas, args.output)
    print("\n══ Factor Idea Generator ══")
    print(f"generated_at: {datetime.now():%Y-%m-%d %H:%M:%S}")
    print(f"ideas: {len(ideas)}")
    print(f"output: {args.output}")
    for spec in ideas[:20]:
        print(f"- {spec.market} {spec.name} [{spec.family}] {spec.formula_type}")


if __name__ == "__main__":
    main()
