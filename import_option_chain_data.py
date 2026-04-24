"""Import third-party historical option-chain CSV data.

The importer normalizes vendor-specific columns into the local
``option_chain_snapshots.csv`` schema used by backtests.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import re

import pandas as pd

from option_chain_snapshot import SNAPSHOT_COLUMNS, SNAPSHOT_PATH, append_snapshots


ROOT = Path(__file__).parent

DEFAULT_ALIASES = {
    "date": ["date", "quote_date", "quotedate", "trade_date", "timestamp", "time", "datetime"],
    "timestamp": ["timestamp", "datetime", "quote_datetime", "quote_time", "time"],
    "underlying": ["underlying", "root", "symbol", "ticker", "underlying_symbol", "act_symbol"],
    "spot": [
        "spot",
        "underlying_price",
        "underlying_last",
        "stock_price",
        "price_underlying",
        "active_underlying_price_1545",
        "active_underlying_price_eod",
        "implied_underlying_price_1545",
    ],
    "code": ["code", "option_symbol", "contract", "contract_symbol", "occ_symbol", "option"],
    "name": ["name", "description"],
    "option_type": ["option_type", "type", "right", "call_put", "cp", "put_call"],
    "expiry": ["expiry", "expiration", "expiration_date", "exp_date", "expire_date"],
    "days_to_expiry": ["days_to_expiry", "dte", "days_to_expiration", "option_expiry_date_distance"],
    "strike_price": ["strike_price", "strike", "option_strike_price"],
    "last_price": ["last_price", "last", "mark", "close", "option_close"],
    "bid": ["bid", "bid_1545", "bid_eod", "bid_price", "best_bid"],
    "ask": ["ask", "ask_1545", "ask_eod", "ask_price", "best_ask"],
    "mid": ["mid", "mark", "midpoint"],
    "prev_close": ["prev_close", "previous_close", "prev_close_price"],
    "volume": ["volume", "trade_volume"],
    "open_interest": ["open_interest", "openint", "oi"],
    "iv": ["iv", "vol", "implied_volatility_1545", "implied_volatility", "option_implied_volatility", "impliedvol"],
    "delta": ["delta_1545", "delta", "option_delta"],
    "gamma": ["gamma_1545", "gamma", "option_gamma"],
    "theta": ["theta_1545", "theta", "option_theta"],
    "vega": ["vega_1545", "vega", "option_vega"],
}


def _norm_col(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", str(name).strip().lower()).strip("_")


def _parse_mapping(raw: str | None) -> dict[str, str]:
    if not raw:
        return {}
    path = Path(raw)
    if path.exists():
        return json.loads(path.read_text(encoding="utf-8"))
    return json.loads(raw)


def _auto_mapping(columns: list[str], manual: dict[str, str]) -> dict[str, str]:
    normalized = {_norm_col(c): c for c in columns}
    mapping: dict[str, str] = {}
    for target, aliases in DEFAULT_ALIASES.items():
        if target in manual:
            mapping[target] = manual[target]
            continue
        for alias in aliases:
            key = _norm_col(alias)
            if key in normalized:
                mapping[target] = normalized[key]
                break
    return mapping


def _parse_occ_contract(code: str) -> dict[str, object]:
    """Parse common OCC-style symbols when vendor fields are absent.

    Example: AAPL240621C00195000.
    """
    text = str(code or "").strip().replace(" ", "")
    match = re.match(r"^(?P<root>[A-Z]{1,6})(?P<date>\d{6})(?P<type>[CP])(?P<strike>\d{8})$", text)
    if not match:
        return {}
    yymmdd = match.group("date")
    year = int("20" + yymmdd[:2])
    month = int(yymmdd[2:4])
    day = int(yymmdd[4:6])
    strike = int(match.group("strike")) / 1000
    return {
        "underlying": match.group("root"),
        "expiry": f"{year:04d}-{month:02d}-{day:02d}",
        "option_type": "CALL" if match.group("type") == "C" else "PUT",
        "strike_price": strike,
    }


def _normalize_option_type(series: pd.Series) -> pd.Series:
    raw = series.astype(str).str.upper().str.strip()
    return raw.replace({
        "C": "CALL",
        "CALLS": "CALL",
        "CALL": "CALL",
        "P": "PUT",
        "PUTS": "PUT",
        "PUT": "PUT",
    })


def normalize_vendor_csv(
    input_path: Path,
    *,
    mapping: dict[str, str] | None = None,
    default_underlying_prefix: str = "US.",
) -> pd.DataFrame:
    raw = pd.read_csv(input_path)
    manual = mapping or {}
    colmap = _auto_mapping(list(raw.columns), manual)

    out = pd.DataFrame()
    for target in SNAPSHOT_COLUMNS:
        source = colmap.get(target)
        if source and source in raw.columns:
            out[target] = raw[source]
        else:
            out[target] = pd.NA

    if out["code"].notna().any():
        parsed = out["code"].map(_parse_occ_contract)
        parsed_df = pd.DataFrame(parsed.tolist())
        for col in ["underlying", "expiry", "option_type", "strike_price"]:
            if col in parsed_df.columns:
                out[col] = out[col].where(out[col].notna(), parsed_df[col])

    out["date"] = pd.to_datetime(out["date"], errors="coerce").dt.date.astype(str)
    if out["timestamp"].isna().all():
        out["timestamp"] = pd.to_datetime(out["date"], errors="coerce")
    else:
        out["timestamp"] = pd.to_datetime(out["timestamp"], errors="coerce")
        out["timestamp"] = out["timestamp"].fillna(pd.to_datetime(out["date"], errors="coerce"))

    out["underlying"] = out["underlying"].astype(str).str.upper().str.strip()
    prefix = default_underlying_prefix
    out["underlying"] = out["underlying"].where(out["underlying"].str.contains(r"\."), prefix + out["underlying"])

    out["option_type"] = _normalize_option_type(out["option_type"])
    out["expiry"] = pd.to_datetime(out["expiry"], errors="coerce").dt.date.astype(str)

    for col in [
        "spot",
        "target_dte",
        "days_to_expiry",
        "strike_price",
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
    ]:
        out[col] = pd.to_numeric(out[col], errors="coerce")

    # Vendors such as Cboe DataShop report IV as a decimal fraction; the local
    # strategy code uses percentage points, matching Futu snapshots.
    positive_iv = out["iv"][out["iv"] > 0].dropna()
    if not positive_iv.empty and positive_iv.median() <= 3:
        out["iv"] = out["iv"] * 100

    if out["code"].isna().all():
        option_marker = out["option_type"].map({"CALL": "C", "PUT": "P"}).fillna("")
        out["code"] = (
            out["underlying"].astype(str).str.replace("US.", "", regex=False)
            + "_"
            + out["expiry"].astype(str)
            + "_"
            + option_marker
            + "_"
            + out["strike_price"].map(lambda x: "" if pd.isna(x) else f"{float(x):g}")
        )

    if out["mid"].isna().all() or ((out["mid"] <= 0) | out["mid"].isna()).any():
        mid = (out["bid"] + out["ask"]) / 2
        fallback = out["last_price"]
        out["mid"] = out["mid"].where(out["mid"] > 0, mid.where((out["bid"] > 0) & (out["ask"] > 0), fallback))

    if out["days_to_expiry"].isna().any():
        quote_date = pd.to_datetime(out["date"], errors="coerce")
        expiry = pd.to_datetime(out["expiry"], errors="coerce")
        dte = (expiry - quote_date).dt.days
        out["days_to_expiry"] = out["days_to_expiry"].fillna(dte)

    if out["target_dte"].isna().any():
        out["target_dte"] = out["target_dte"].fillna(out["days_to_expiry"])

    if out["moneyness_pct"].isna().all() and out["spot"].notna().any():
        out["moneyness_pct"] = (out["strike_price"] / out["spot"] - 1) * 100
    else:
        out["moneyness_pct"] = pd.to_numeric(out["moneyness_pct"], errors="coerce")

    valid = out[
        out["date"].notna()
        & out["underlying"].notna()
        & out["option_type"].isin(["CALL", "PUT"])
        & out["expiry"].notna()
        & out["strike_price"].notna()
        & ((out["bid"].fillna(0) > 0) | (out["ask"].fillna(0) > 0) | (out["mid"].fillna(0) > 0) | (out["last_price"].fillna(0) > 0))
    ].copy()

    return valid[SNAPSHOT_COLUMNS]


def main() -> None:
    parser = argparse.ArgumentParser(description="Import third-party option-chain CSV into local snapshot schema")
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output", type=Path, default=SNAPSHOT_PATH)
    parser.add_argument("--mapping", help="JSON string or JSON file path: local_field -> vendor_column")
    parser.add_argument("--replace", action="store_true", help="Replace output instead of appending")
    parser.add_argument("--prefix", default="US.", help="Underlying prefix when vendor uses bare tickers")
    args = parser.parse_args()

    mapping = _parse_mapping(args.mapping)
    normalized = normalize_vendor_csv(args.input, mapping=mapping, default_underlying_prefix=args.prefix)
    if args.replace and args.output.exists():
        args.output.unlink()
    append_snapshots(normalized, args.output)

    print("\n══ Import Option Chain Data ══")
    print(f"input: {args.input}")
    print(f"output: {args.output}")
    print(f"imported rows: {len(normalized)}")
    if not normalized.empty:
        print(f"date range: {normalized['date'].min()} → {normalized['date'].max()}")
        print(f"underlyings: {', '.join(sorted(normalized['underlying'].dropna().unique())[:20])}")


if __name__ == "__main__":
    main()
