#!/usr/bin/env python3
"""Build candidate factor columns on top of the existing indicator panel."""

from __future__ import annotations

from collections.abc import Iterable

import numpy as np
import pandas as pd

from factor_registry import FactorSpec
from macro_events import add_us_macro_factors


def _rolling_zscore(series: pd.Series, window: int, min_periods: int | None = None) -> pd.Series:
    min_periods = min_periods or max(3, window // 2)
    mean = series.rolling(window=window, min_periods=min_periods).mean()
    std = series.rolling(window=window, min_periods=min_periods).std()
    return (series - mean) / std.replace(0, np.nan)


def _ensure_common_inputs(df: pd.DataFrame, market: str) -> pd.DataFrame:
    out = df.copy()
    if market == "us":
        out = add_us_macro_factors(out)

    if "daily_ret_1d" not in out.columns:
        out["daily_ret_1d"] = out["close"].pct_change() * 100
    if "gap_ret_1d" not in out.columns and "open" in out.columns:
        out["gap_ret_1d"] = (out["open"] - out["close"].shift(1)) / out["close"].shift(1) * 100
    if "intraday_ret_1d" not in out.columns and "open" in out.columns:
        out["intraday_ret_1d"] = (out["close"] - out["open"]) / out["open"].replace(0, np.nan) * 100
    if "amihud_daily" not in out.columns and "volume" in out.columns:
        daily_ret = out["close"].pct_change().abs()
        dollar_vol = (out["close"] * out["volume"]).replace(0, np.nan)
        out["amihud_daily"] = daily_ret / dollar_vol * 1e8
    if "mfdfa_width_centered_120" not in out.columns and "mfdfa_width_120d" in out.columns:
        rolling_med = out["mfdfa_width_120d"].rolling(window=120, min_periods=60).median()
        out["mfdfa_width_centered_120"] = out["mfdfa_width_120d"] - rolling_med
    return out


def _build_from_spec(df: pd.DataFrame, spec: FactorSpec) -> pd.Series:
    formula = spec.formula_type
    params = spec.params

    if formula == "rolling_mean":
        src = spec.inputs[0]
        window = int(params.get("window", 5))
        min_periods = int(params.get("min_periods", max(3, window // 2)))
        return df[src].rolling(window=window, min_periods=min_periods).mean()

    if formula == "rolling_zscore":
        src = spec.inputs[0]
        window = int(params.get("window", 20))
        min_periods = int(params.get("min_periods", max(3, window // 2)))
        return _rolling_zscore(df[src], window=window, min_periods=min_periods)

    if formula == "ratio":
        num = df[spec.inputs[0]]
        den = df[spec.inputs[1]].replace(0, np.nan)
        scale = float(params.get("scale", 1.0))
        return num / den * scale

    if formula == "interaction":
        left = df[spec.inputs[0]]
        right = df[spec.inputs[1]]
        scale = float(params.get("scale", 1.0))
        return left * right * scale

    if formula == "close_roc":
        period = int(params.get("period", 20))
        return (df["close"] / df["close"].shift(period) - 1) * 100

    raise ValueError(f"unsupported formula_type: {formula}")


def build_candidate_factors(df: pd.DataFrame, specs: Iterable[FactorSpec], market: str) -> tuple[pd.DataFrame, list[str]]:
    out = _ensure_common_inputs(df, market.lower())
    built: list[str] = []

    for spec in specs:
        try:
            series = _build_from_spec(out, spec)
        except Exception:
            out[spec.name] = np.nan
            continue
        out[spec.name] = series
        built.append(spec.name)

    return out, built
