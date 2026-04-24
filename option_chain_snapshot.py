"""Collect and query local option-chain snapshots.

Futu OpenD exposes current option chains, not historical option chains.  This
module starts building a local history by saving current ATM-near snapshots.
Backtests can later use these snapshots before falling back to model prices.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path
import time

import pandas as pd

from fetch_futu import find_atm_options
from option_fractal_advisor import WATCHLISTS


ROOT = Path(__file__).parent
SNAPSHOT_PATH = ROOT / "option_chain_snapshots.csv"

SNAPSHOT_COLUMNS = [
    "timestamp",
    "date",
    "underlying",
    "spot",
    "target_dte",
    "code",
    "name",
    "option_type",
    "expiry",
    "days_to_expiry",
    "strike_price",
    "moneyness_pct",
    "last_price",
    "bid",
    "ask",
    "mid",
    "prev_close",
    "volume",
    "open_interest",
    "iv",
    "delta",
    "gamma",
    "theta",
    "vega",
]


def _ticker(code: str) -> str:
    return code.split(".")[-1].upper()


def _load_snapshots(path: Path = SNAPSHOT_PATH) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=SNAPSHOT_COLUMNS)
    df = pd.read_csv(path)
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date.astype(str)
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    return df


def append_snapshots(rows: pd.DataFrame, path: Path = SNAPSHOT_PATH) -> None:
    if rows.empty:
        return
    rows = rows.copy()
    for col in SNAPSHOT_COLUMNS:
        if col not in rows.columns:
            rows[col] = pd.NA
    rows = rows[SNAPSHOT_COLUMNS]
    header = not path.exists()
    rows.to_csv(path, mode="a", index=False, header=header)


def collect_snapshot(
    *,
    watchlist: str = "tech",
    dtes: list[int] | None = None,
    strike_band: float = 0.08,
    max_contracts_per_side: int = 8,
    sleep_sec: float = 3.2,
    output: Path = SNAPSHOT_PATH,
) -> pd.DataFrame:
    dtes = dtes or [14, 21]
    codes = WATCHLISTS.get(watchlist)
    if not codes:
        raise ValueError(f"unknown watchlist: {watchlist}")

    timestamp = datetime.now(timezone.utc).isoformat(timespec="seconds")
    local_date = datetime.now().date().isoformat()
    frames: list[pd.DataFrame] = []

    for underlying in codes:
        for dte in dtes:
            try:
                chain = find_atm_options(
                    underlying,
                    days_to_expiry=dte,
                    strike_band=strike_band,
                    max_contracts_per_side=max_contracts_per_side,
                )
            except Exception as exc:
                print(f"[!] {underlying} {dte}D: {str(exc)[:120]}")
                time.sleep(sleep_sec)
                continue
            if chain.empty:
                print(f"[-] {underlying} {dte}D: empty")
                time.sleep(sleep_sec)
                continue

            chain = chain.copy()
            chain["timestamp"] = timestamp
            chain["date"] = local_date
            chain["underlying"] = underlying
            chain["target_dte"] = dte
            if "mid" not in chain.columns:
                bid = pd.to_numeric(chain.get("bid"), errors="coerce")
                ask = pd.to_numeric(chain.get("ask"), errors="coerce")
                last = pd.to_numeric(chain.get("last_price"), errors="coerce")
                chain["mid"] = ((bid + ask) / 2).where((bid > 0) & (ask > 0), last)
            frames.append(chain)
            print(f"[+] {underlying} {dte}D: {len(chain)} contracts")
            time.sleep(sleep_sec)

    out = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=SNAPSHOT_COLUMNS)
    append_snapshots(out, output)
    return out


def select_snapshot_contract(
    *,
    underlying: str,
    trade_date: str | pd.Timestamp,
    kind: str,
    target_dte: int,
    snapshots: pd.DataFrame | None = None,
    max_date_gap_days: int = 1,
) -> pd.DataFrame:
    """Select ATM contract(s) from the nearest local snapshot.

    For ``kind=single`` returns one CALL.  For ``kind=straddle`` returns one
    CALL and one PUT with the same/nearest ATM strike when available.
    """
    df = snapshots.copy() if snapshots is not None else _load_snapshots()
    if df.empty:
        return pd.DataFrame()

    df["date_ts"] = pd.to_datetime(df["date"], errors="coerce")
    trade_ts = pd.Timestamp(trade_date).normalize()
    underlying = underlying if underlying.startswith("US.") else f"US.{underlying}"
    sub = df[df["underlying"] == underlying].copy()
    if sub.empty:
        return pd.DataFrame()

    sub["date_gap"] = (sub["date_ts"] - trade_ts).abs().dt.days
    sub = sub[sub["date_gap"] <= max_date_gap_days]
    if sub.empty:
        return pd.DataFrame()
    sub["dte_gap"] = (pd.to_numeric(sub["days_to_expiry"], errors="coerce") - target_dte).abs()
    sub = sub.sort_values(["date_gap", "dte_gap", "timestamp"])
    best_date_gap = sub.iloc[0]["date_gap"]
    best_dte_gap = sub.iloc[0]["dte_gap"]
    sub = sub[(sub["date_gap"] == best_date_gap) & (sub["dte_gap"] == best_dte_gap)].copy()
    sub["abs_moneyness"] = pd.to_numeric(sub["moneyness_pct"], errors="coerce").abs()

    if kind == "single":
        calls = sub[sub["option_type"] == "CALL"].sort_values("abs_moneyness")
        return calls.head(1).copy()
    if kind == "straddle":
        rows = []
        for option_type in ("CALL", "PUT"):
            side = sub[sub["option_type"] == option_type].sort_values("abs_moneyness")
            if side.empty:
                return pd.DataFrame()
            rows.append(side.head(1))
        return pd.concat(rows, ignore_index=True)
    return pd.DataFrame()


def main() -> None:
    parser = argparse.ArgumentParser(description="Collect local option-chain snapshots")
    parser.add_argument("--watchlist", default="tech", choices=sorted(WATCHLISTS))
    parser.add_argument("--dtes", default="14,21", help="comma-separated target DTEs")
    parser.add_argument("--strike-band", type=float, default=0.08)
    parser.add_argument("--max-contracts-per-side", type=int, default=8)
    parser.add_argument("--sleep-sec", type=float, default=3.2)
    parser.add_argument("--output", type=Path, default=SNAPSHOT_PATH)
    args = parser.parse_args()

    dtes = [int(x.strip()) for x in args.dtes.split(",") if x.strip()]
    print("\n══ Option Chain Snapshot Collector ══")
    print(f"watchlist: {args.watchlist}")
    print(f"dtes: {dtes}")
    out = collect_snapshot(
        watchlist=args.watchlist,
        dtes=dtes,
        strike_band=args.strike_band,
        max_contracts_per_side=args.max_contracts_per_side,
        sleep_sec=args.sleep_sec,
        output=args.output,
    )
    print(f"saved rows: {len(out)}")
    print(f"output: {args.output}")


if __name__ == "__main__":
    main()
