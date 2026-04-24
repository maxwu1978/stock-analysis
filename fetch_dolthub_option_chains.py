#!/usr/bin/env python3
"""Fetch free historical option-chain slices from DoltHub.

The DoltHub ``post-no-preference/options`` database contains a large historical
US option-chain table.  This script only downloads the small ATM-near slices
needed by the strategy backtests and stores them in the local snapshot schema.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import timedelta
import math
from pathlib import Path
import time

import pandas as pd
import requests

from fetch_us import fetch_us_history
from historical_option_pnl_review import (
    SINGLE_DTE_DAYS,
    SINGLE_TIME_STOP_DAYS,
    STRADDLE_DTE_DAYS,
    STRADDLE_TIME_STOP_DAYS,
    _mark_episodes,
)
from option_chain_snapshot import SNAPSHOT_COLUMNS, SNAPSHOT_PATH, _load_snapshots, append_snapshots


ROOT = Path(__file__).resolve().parent
DOLTHUB_SQL_URL = "https://www.dolthub.com/api/v1alpha1/post-no-preference/options/master"


@dataclass(frozen=True)
class FetchTarget:
    symbol: str
    kind: str
    entry_date: str
    quote_date: str
    spot: float
    target_dte: int
    exp_start: str
    exp_end: str
    strike_low: float
    strike_high: float


def _sql_quote(text: str) -> str:
    return "'" + str(text).replace("\\", "\\\\").replace("'", "''") + "'"


def _dte_for_kind(kind: str) -> int:
    return SINGLE_DTE_DAYS if kind == "single" else STRADDLE_DTE_DAYS


def _time_stop_for_kind(kind: str) -> int:
    return SINGLE_TIME_STOP_DAYS if kind == "single" else STRADDLE_TIME_STOP_DAYS


def _nearest_pos_on_or_after(index: pd.DatetimeIndex, date: pd.Timestamp) -> int | None:
    normalized = pd.DatetimeIndex(index).normalize()
    pos = int(normalized.searchsorted(date.normalize(), side="left"))
    if pos >= len(index):
        return None
    return pos


def _prepare_signal_rows(signals_path: Path, episode_only: bool, symbols: set[str] | None) -> pd.DataFrame:
    signals = pd.read_csv(signals_path, parse_dates=["date"])
    strong = signals[signals["strength"] == "强机会"].copy()
    if strong.empty:
        return strong
    strong = _mark_episodes(strong)
    if episode_only:
        strong = strong[strong["is_episode_entry"]].copy()
    if symbols:
        strong = strong[strong["symbol"].astype(str).str.upper().isin(symbols)].copy()
    return strong.sort_values(["date", "symbol", "kind"]).reset_index(drop=True)


def _history_for_symbols(symbols: list[str], period: str) -> dict[str, pd.DataFrame]:
    histories: dict[str, pd.DataFrame] = {}
    for symbol in symbols:
        try:
            hist = fetch_us_history(symbol, period=period)
        except Exception as exc:
            print(f"[!] {symbol}: history failed: {exc}", flush=True)
            continue
        if hist is None or hist.empty:
            print(f"[!] {symbol}: empty history", flush=True)
            continue
        hist = hist.sort_index().copy()
        hist.index = pd.to_datetime(hist.index).tz_localize(None).normalize()
        histories[symbol] = hist
    return histories


def _build_targets(
    rows: pd.DataFrame,
    histories: dict[str, pd.DataFrame],
    *,
    include_holding_window: bool,
    dte_window: int,
    strike_band: float,
) -> list[FetchTarget]:
    targets: list[FetchTarget] = []
    seen: set[tuple[str, str, str, int, str, str, int, int]] = set()
    for _, row in rows.iterrows():
        symbol = str(row["symbol"]).upper()
        kind = str(row["kind"])
        hist = histories.get(symbol)
        if hist is None or hist.empty:
            continue
        entry_ts = pd.Timestamp(row["date"]).normalize()
        pos = _nearest_pos_on_or_after(hist.index, entry_ts)
        if pos is None:
            continue

        close = float(hist["close"].iloc[pos])
        if not math.isfinite(close) or close <= 0:
            continue

        target_dte = _dte_for_kind(kind)
        time_stop = _time_stop_for_kind(kind)
        exp_start_ts = hist.index[pos] + timedelta(days=max(1, target_dte - dte_window))
        exp_end_ts = hist.index[pos] + timedelta(days=target_dte + dte_window)
        exp_start = exp_start_ts.strftime("%Y-%m-%d")
        exp_end = exp_end_ts.strftime("%Y-%m-%d")
        strike_low = close * (1.0 - strike_band)
        strike_high = close * (1.0 + strike_band)

        max_pos = pos + time_stop if include_holding_window else pos
        max_pos = min(max_pos, len(hist) - 1)
        for quote_pos in range(pos, max_pos + 1):
            quote_date = hist.index[quote_pos].strftime("%Y-%m-%d")
            key = (
                symbol,
                kind,
                quote_date,
                target_dte,
                exp_start,
                exp_end,
                round(strike_low * 100),
                round(strike_high * 100),
            )
            if key in seen:
                continue
            seen.add(key)
            targets.append(FetchTarget(
                symbol=symbol,
                kind=kind,
                entry_date=hist.index[pos].strftime("%Y-%m-%d"),
                quote_date=quote_date,
                spot=close,
                target_dte=target_dte,
                exp_start=exp_start,
                exp_end=exp_end,
                strike_low=strike_low,
                strike_high=strike_high,
            ))
    return targets


def _query_dolthub(session: requests.Session, target: FetchTarget, *, timeout: int, limit: int) -> list[dict]:
    query = f"""
        SELECT
            date, act_symbol, expiration, strike, call_put,
            bid, ask, vol, delta, gamma, theta, vega, rho
        FROM option_chain
        WHERE act_symbol = {_sql_quote(target.symbol)}
          AND date = {_sql_quote(target.quote_date)}
          AND expiration >= {_sql_quote(target.exp_start)}
          AND expiration <= {_sql_quote(target.exp_end)}
          AND strike >= {target.strike_low:.2f}
          AND strike <= {target.strike_high:.2f}
        ORDER BY expiration, strike, call_put
        LIMIT {int(limit)}
    """
    response = session.get(DOLTHUB_SQL_URL, params={"q": query}, timeout=timeout)
    response.raise_for_status()
    payload = response.json()
    status = payload.get("query_execution_status")
    if status != "Success":
        raise RuntimeError(payload.get("query_execution_message") or f"DoltHub query failed: {status}")
    return payload.get("rows") or []


def _rows_to_snapshot(rows: list[dict], target: FetchTarget) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame(columns=SNAPSHOT_COLUMNS)

    raw = pd.DataFrame(rows)
    out = pd.DataFrame()
    out["timestamp"] = pd.Timestamp.utcnow().isoformat(timespec="seconds")
    out["date"] = pd.to_datetime(raw["date"], errors="coerce").dt.date.astype(str)
    out["underlying"] = "US." + raw["act_symbol"].astype(str).str.upper().str.strip()
    out["spot"] = target.spot
    out["target_dte"] = target.target_dte
    option_type = raw["call_put"].astype(str).str.upper().map({"CALL": "CALL", "PUT": "PUT"})
    expiry = pd.to_datetime(raw["expiration"], errors="coerce").dt.date.astype(str)
    strike = pd.to_numeric(raw["strike"], errors="coerce")
    marker = option_type.map({"CALL": "C", "PUT": "P"}).fillna("")
    out["code"] = (
        raw["act_symbol"].astype(str).str.upper().str.strip()
        + "_"
        + expiry
        + "_"
        + marker
        + "_"
        + strike.map(lambda x: "" if pd.isna(x) else f"{float(x):g}")
    )
    out["name"] = out["code"]
    out["option_type"] = option_type
    out["expiry"] = expiry
    quote_date = pd.to_datetime(out["date"], errors="coerce")
    expiry_ts = pd.to_datetime(out["expiry"], errors="coerce")
    out["days_to_expiry"] = (expiry_ts - quote_date).dt.days
    out["strike_price"] = strike
    out["moneyness_pct"] = (strike / target.spot - 1.0) * 100.0
    out["last_price"] = pd.NA
    out["bid"] = pd.to_numeric(raw["bid"], errors="coerce")
    out["ask"] = pd.to_numeric(raw["ask"], errors="coerce")
    out["mid"] = (out["bid"] + out["ask"]) / 2.0
    out["prev_close"] = pd.NA
    out["volume"] = pd.NA
    out["open_interest"] = pd.NA
    out["iv"] = pd.to_numeric(raw["vol"], errors="coerce") * 100.0
    out["delta"] = pd.to_numeric(raw["delta"], errors="coerce")
    out["gamma"] = pd.to_numeric(raw["gamma"], errors="coerce")
    out["theta"] = pd.to_numeric(raw["theta"], errors="coerce")
    out["vega"] = pd.to_numeric(raw["vega"], errors="coerce")
    valid = out[
        out["option_type"].isin(["CALL", "PUT"])
        & out["expiry"].notna()
        & out["strike_price"].notna()
        & ((out["bid"].fillna(0) > 0) | (out["ask"].fillna(0) > 0) | (out["mid"].fillna(0) > 0))
    ].copy()
    return valid[SNAPSHOT_COLUMNS]


def _existing_keys(path: Path) -> set[tuple[str, str, int]]:
    df = _load_snapshots(path)
    if df.empty:
        return set()
    target_dte = pd.to_numeric(df.get("target_dte"), errors="coerce")
    keys = set()
    for underlying, date, dte in zip(df.get("underlying", []), df.get("date", []), target_dte):
        if pd.isna(dte):
            continue
        keys.add((str(underlying), str(date), int(dte)))
    return keys


def run(args: argparse.Namespace) -> pd.DataFrame:
    symbols = {s.strip().upper() for s in args.symbols.split(",") if s.strip()} if args.symbols else None
    rows = _prepare_signal_rows(args.signals, args.episode_only, symbols)
    if rows.empty:
        print("no strong signal rows", flush=True)
        return pd.DataFrame(columns=SNAPSHOT_COLUMNS)

    histories = _history_for_symbols(sorted(rows["symbol"].astype(str).str.upper().unique()), args.period)
    targets = _build_targets(
        rows,
        histories,
        include_holding_window=args.include_holding_window,
        dte_window=args.dte_window,
        strike_band=args.strike_band,
    )
    if args.max_targets:
        targets = targets[: args.max_targets]

    if args.replace and args.output.exists():
        args.output.unlink()
    existing = set() if args.replace else _existing_keys(args.output)
    session = requests.Session()
    frames: list[pd.DataFrame] = []

    print("\n══ DoltHub Option Chain Fetch ══", flush=True)
    print(f"signals: {args.signals}", flush=True)
    print(f"targets: {len(targets)}", flush=True)
    print(f"output: {args.output}", flush=True)

    for i, target in enumerate(targets, start=1):
        key = (f"US.{target.symbol}", target.quote_date, target.target_dte)
        if key in existing:
            print(f"[-] {i}/{len(targets)} {target.symbol} {target.quote_date} {target.target_dte}D: cached", flush=True)
            continue
        try:
            rows = _query_dolthub(session, target, timeout=args.timeout, limit=args.limit)
            snapshot = _rows_to_snapshot(rows, target)
            if snapshot.empty:
                print(f"[ ] {i}/{len(targets)} {target.symbol} {target.quote_date} {target.target_dte}D: empty", flush=True)
            else:
                append_snapshots(snapshot, args.output)
                frames.append(snapshot)
                existing.add(key)
                print(f"[+] {i}/{len(targets)} {target.symbol} {target.quote_date} {target.target_dte}D: {len(snapshot)} rows", flush=True)
        except Exception as exc:
            print(f"[!] {i}/{len(targets)} {target.symbol} {target.quote_date} {target.target_dte}D: {str(exc)[:180]}", flush=True)
        time.sleep(args.sleep_sec)

    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=SNAPSHOT_COLUMNS)


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch strategy-needed option-chain slices from DoltHub")
    parser.add_argument("--signals", type=Path, default=ROOT / "historical_option_signal_review.csv")
    parser.add_argument("--output", type=Path, default=SNAPSHOT_PATH)
    parser.add_argument("--period", default="3y")
    parser.add_argument("--symbols", help="optional comma-separated ticker filter")
    parser.add_argument("--episode-only", action="store_true", help="fetch only first day of each strong-signal episode")
    parser.add_argument("--include-holding-window", action="store_true", help="also fetch quote dates through the strategy time stop")
    parser.add_argument("--dte-window", type=int, default=10)
    parser.add_argument("--strike-band", type=float, default=0.18)
    parser.add_argument("--limit", type=int, default=300)
    parser.add_argument("--timeout", type=int, default=45)
    parser.add_argument("--sleep-sec", type=float, default=0.2)
    parser.add_argument("--max-targets", type=int, help="cap targets for smoke tests")
    parser.add_argument("--replace", action="store_true")
    args = parser.parse_args()

    out = run(args)
    print(f"new rows: {len(out)}", flush=True)


if __name__ == "__main__":
    main()
