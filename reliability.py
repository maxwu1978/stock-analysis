#!/usr/bin/env python3
"""Build and read auto-generated reliability labels for report rendering."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from backtest_us import run_backtest as run_backtest_us
from backtest_v2 import run_backtest as run_backtest_a
from fetch_data import STOCKS
from fetch_us import US_STOCKS


RELIABILITY_PATH = Path(__file__).with_name("reliability_labels.json")


def _half_diff_up(sub: pd.DataFrame, threshold: int = 40) -> float | None:
    bull = sub[sub["outlook"] > threshold]
    bear = sub[sub["outlook"] <= -threshold]
    if len(bull) < 5 or len(bear) < 5:
        return None
    bull_rate = bull["fwd_30d_up"].dropna().mean() * 100
    bear_rate = bear["fwd_30d_up"].dropna().mean() * 100
    if pd.isna(bull_rate) or pd.isna(bear_rate):
        return None
    return float(bull_rate - bear_rate)


def summarize_reliability(results: pd.DataFrame) -> dict:
    """Convert structured backtest output into a compact reliability label."""
    if results is None or len(results) < 30 or "outlook" not in results.columns:
        return {"label": "?", "reason": "样本不足"}

    bull = results[results["outlook"] > 40]
    bear = results[results["outlook"] <= -40]
    if len(bull) < 5 or len(bear) < 5:
        return {"label": "?", "reason": "极端分位样本不足", "sample_size": len(results)}

    bull_30 = bull["fwd_30d_up"].dropna()
    bear_30 = bear["fwd_30d_up"].dropna()
    bull_30_ret = bull["fwd_30d_ret"].dropna()
    bear_30_ret = bear["fwd_30d_ret"].dropna()
    valid_30 = results[["outlook", "fwd_30d_ret"]].dropna()

    diff_up_30d_pp = float(bull_30.mean() * 100 - bear_30.mean() * 100) if len(bull_30) and len(bear_30) else None
    diff_ret_30d_pct = float((bull_30_ret.mean() - bear_30_ret.mean()) * 100) if len(bull_30_ret) and len(bear_30_ret) else None
    rankic_30d = float(valid_30["outlook"].rank().corr(valid_30["fwd_30d_ret"].rank())) if len(valid_30) >= 30 else None

    mid = len(results) // 2
    first_diff = _half_diff_up(results.iloc[:mid].copy())
    second_diff = _half_diff_up(results.iloc[mid:].copy())
    stable_halves = bool(first_diff is not None and second_diff is not None and first_diff > 0 and second_diff > 0)

    min_extreme_n = min(len(bull), len(bear))
    if (
        min_extreme_n >= 20
        and diff_up_30d_pp is not None and diff_up_30d_pp >= 10
        and rankic_30d is not None and rankic_30d >= 0.05
        and stable_halves
    ):
        label = "强"
    elif (
        min_extreme_n >= 10
        and diff_up_30d_pp is not None and diff_up_30d_pp >= 5
        and rankic_30d is not None and rankic_30d >= 0.02
    ):
        label = "中"
    else:
        label = "弱"

    return {
        "label": label,
        "sample_size": int(len(results)),
        "bull_n_30d": int(len(bull_30)),
        "bear_n_30d": int(len(bear_30)),
        "diff_up_30d_pp": round(diff_up_30d_pp, 2) if diff_up_30d_pp is not None else None,
        "diff_ret_30d_pct": round(diff_ret_30d_pct, 2) if diff_ret_30d_pct is not None else None,
        "rankic_30d": round(rankic_30d, 4) if rankic_30d is not None else None,
        "stable_halves": stable_halves,
        "first_half_diff_up_30d_pp": round(first_diff, 2) if first_diff is not None else None,
        "second_half_diff_up_30d_pp": round(second_diff, 2) if second_diff is not None else None,
    }


def build_reliability_labels() -> dict:
    data = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "a_share": {},
        "us": {},
    }

    for code, name in STOCKS.items():
        try:
            data["a_share"][code] = summarize_reliability(run_backtest_a(code, name))
        except Exception as exc:
            data["a_share"][code] = {"label": "?", "reason": str(exc)[:120]}

    for ticker, name in US_STOCKS.items():
        try:
            data["us"][ticker] = summarize_reliability(run_backtest_us(ticker, name))
        except Exception as exc:
            data["us"][ticker] = {"label": "?", "reason": str(exc)[:120]}

    return data


def save_reliability_labels(data: dict, path: Path = RELIABILITY_PATH) -> Path:
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def load_reliability_labels(path: Path = RELIABILITY_PATH) -> dict:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def get_reliability_label(labels: dict, market: str, symbol: str) -> str:
    return labels.get(market, {}).get(symbol, {}).get("label", "?")
