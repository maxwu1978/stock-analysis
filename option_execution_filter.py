"""Execution gates for long-option strategy candidates.

These gates are deliberately simple and account-size aware.  The signal model
can find directional opportunities, but the trade should be blocked when the
contract is too expensive or too wide to execute cleanly.
"""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd


MAX_SINGLE_DEBIT_PER_CONTRACT = 1_500.0
MAX_STRADDLE_DEBIT_PER_CONTRACT = 1_800.0
MAX_ENTRY_SPREAD_PCT_MID = 15.0


@dataclass(frozen=True)
class ExecutionGate:
    allowed: bool
    reason: str
    debit_per_contract: float
    spread_pct_mid: float | None


def _price_series(rows: pd.DataFrame, column: str) -> pd.Series:
    if column not in rows.columns:
        return pd.Series([pd.NA] * len(rows), index=rows.index)
    return pd.to_numeric(rows[column], errors="coerce")


def _entry_prices(rows: pd.DataFrame) -> pd.Series:
    ask = _price_series(rows, "ask")
    mid = _price_series(rows, "mid")
    last = _price_series(rows, "last_price")
    return ask.where(ask > 0, mid.where(mid > 0, last))


def entry_spread_pct_mid(rows: pd.DataFrame) -> float | None:
    bid = _price_series(rows, "bid")
    ask = _price_series(rows, "ask")
    valid = (bid > 0) & (ask > 0)
    if not valid.all():
        return None
    spread = (ask - bid).sum()
    mid = ((ask + bid) / 2).sum()
    if pd.isna(mid) or float(mid) <= 0:
        return None
    return float(spread / mid * 100.0)


def evaluate_long_option_entry(
    rows: pd.DataFrame,
    *,
    kind: str,
    max_single_debit: float = MAX_SINGLE_DEBIT_PER_CONTRACT,
    max_straddle_debit: float = MAX_STRADDLE_DEBIT_PER_CONTRACT,
    max_spread_pct_mid: float | None = MAX_ENTRY_SPREAD_PCT_MID,
) -> ExecutionGate:
    if rows is None or rows.empty:
        return ExecutionGate(False, "no_option_chain", 0.0, None)

    prices = _entry_prices(rows).dropna()
    if prices.empty or not (prices > 0).all():
        return ExecutionGate(False, "no_executable_quote", 0.0, None)

    debit = float(prices.sum() * 100.0)
    spread_pct = entry_spread_pct_mid(rows)
    max_debit = max_single_debit if kind == "single" else max_straddle_debit
    if debit > max_debit:
        return ExecutionGate(False, f"debit_too_high>{max_debit:.0f}", debit, spread_pct)
    if max_spread_pct_mid is not None and spread_pct is not None and spread_pct > max_spread_pct_mid:
        return ExecutionGate(False, f"spread_too_wide>{max_spread_pct_mid:.0f}%", debit, spread_pct)
    return ExecutionGate(True, "pass", debit, spread_pct)
