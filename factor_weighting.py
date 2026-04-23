#!/usr/bin/env python3
"""Load lightweight factor-family priors from local tear-sheet reports."""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parent
REPORT_PATHS = {
    "a": ROOT / "factor_eval_a_report.csv",
    "us": ROOT / "factor_eval_us_report.csv",
}


def infer_factor_family(factor: str) -> str:
    if factor.startswith("mfdfa_") or factor.startswith("hq2"):
        return "fractal"
    if factor in {"boll_width", "vol_compress", "vol_surge", "adx_accel", "kurt_20", "atr_pct", "high_low_range"}:
        return "volatility"
    if factor.startswith("ROC") or factor in {"DIF", "DEA", "MACD", "RSI6", "RSI12", "ADX", "+DI", "-DI", "ma5_slope", "ma20_diff", "ma60_diff", "high52w_pos"}:
        return "trend"
    if factor in {"autocorr", "vol_price_div", "price_position", "max_ret_20d", "gap_ret_10d", "amihud_20d", "vol_change"}:
        return "behavior"
    if factor in {"roe", "rev_growth", "profit_growth", "gross_margin", "debt_ratio", "cash_flow_ps", "roe_chg", "rev_growth_accel"}:
        return "fundamental"
    if factor.startswith("vix_") or factor == "vix_close":
        return "macro"
    return "other"


@lru_cache(maxsize=4)
def load_family_weight_multipliers(market: str) -> dict[str, float]:
    """Map factor family -> weight multiplier derived from tear-sheet quality.

    Missing files or malformed reports gracefully degrade to neutral multipliers.
    """
    path = REPORT_PATHS.get(market)
    if path is None or not path.exists():
        return {}

    try:
        report = pd.read_csv(path)
    except Exception:
        return {}

    required = {"family", "rankic", "stable_halves", "coverage"}
    if report.empty or not required.issubset(report.columns):
        return {}

    family = (
        report.groupby("family", as_index=False)
        .agg(
            mean_abs_rankic=("rankic", lambda s: s.dropna().abs().mean() if len(s.dropna()) else np.nan),
            stable_ratio=("stable_halves", "mean"),
            max_coverage=("coverage", "max"),
        )
        .fillna({"mean_abs_rankic": 0.0, "stable_ratio": 0.0, "max_coverage": 0.0})
    )
    if family.empty:
        return {}

    family["quality"] = (
        family["mean_abs_rankic"] * 100
        + family["stable_ratio"] * 8
        + family["max_coverage"] / 20
    )

    baseline = float(family["quality"].mean()) if len(family) else 0.0
    dispersion = float(family["quality"].std(ddof=0)) if len(family) > 1 else 0.0
    if baseline <= 0 or dispersion <= 0:
        return {row["family"]: 1.0 for _, row in family.iterrows()}

    out: dict[str, float] = {}
    for _, row in family.iterrows():
        z = (float(row["quality"]) - baseline) / dispersion
        # Keep the overlay modest: this is a prior, not a replacement for IC.
        out[str(row["family"])] = float(np.clip(1.0 + z * 0.12, 0.80, 1.20))
    return out
