"""Backtest proxy PnL for historical strong option signals.

This script intentionally does not mutate the live strategy.  It replays the
strong signals emitted by ``historical_option_signal_review.py``.  When local
historical option-chain snapshots are available it uses ask-side entry quotes
and bid-side exit marks for the selected contracts; otherwise it falls back to
a Black-Scholes proxy.
"""

from __future__ import annotations

import argparse
import math
from dataclasses import asdict, dataclass
from pathlib import Path

import numpy as np
import pandas as pd

from fetch_us import fetch_us_history
from option_execution_filter import (
    MAX_ENTRY_SPREAD_PCT_MID,
    MAX_SINGLE_DEBIT_PER_CONTRACT,
    MAX_STRADDLE_DEBIT_PER_CONTRACT,
    entry_spread_pct_mid,
    evaluate_long_option_entry,
)
from option_chain_snapshot import _load_snapshots, select_snapshot_contract


ROOT = Path(__file__).parent

SINGLE_DTE_DAYS = 14
SINGLE_TIME_STOP_DAYS = 5
SINGLE_TAKE_PROFIT_MULT = 1.50
SINGLE_HARD_STOP_MULT = 0.45

STRADDLE_DTE_DAYS = 21
STRADDLE_TIME_STOP_DAYS = 7
STRADDLE_TAKE_PROFIT_MULT = 1.30
STRADDLE_HARD_STOP_MULT = 0.50


@dataclass
class TradeResult:
    episode_id: int
    is_episode_entry: bool
    date: str
    symbol: str
    kind: str
    signal: str
    entry_close: float
    strike: float
    dte_days: int
    time_stop_days: int
    iv_used_pct: float
    entry_debit: float
    entry_debit_per_contract: float
    breakeven_move_pct: float
    exit_date: str
    exit_reason: str
    exit_value: float
    pnl_pct: float
    pnl_per_contract: float
    underlying_ret_to_exit_pct: float
    dte_exit_date: str
    dte_underlying_ret_pct: float
    expiry_proxy_pnl_pct: float
    pricing_source: str
    contract_codes: str
    entry_quote: str
    entry_spread_pct_mid: float | None


def _norm_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def _bs_call(S: float, K: float, T: float, sigma: float, r: float) -> float:
    if T <= 0:
        return max(S - K, 0.0)
    if sigma <= 0:
        return max(S - K * math.exp(-r * T), 0.0)
    vol_sqrt = sigma * math.sqrt(T)
    d1 = (math.log(S / K) + (r + 0.5 * sigma * sigma) * T) / vol_sqrt
    d2 = d1 - vol_sqrt
    return S * _norm_cdf(d1) - K * math.exp(-r * T) * _norm_cdf(d2)


def _bs_put(S: float, K: float, T: float, sigma: float, r: float) -> float:
    if T <= 0:
        return max(K - S, 0.0)
    if sigma <= 0:
        return max(K * math.exp(-r * T) - S, 0.0)
    vol_sqrt = sigma * math.sqrt(T)
    d1 = (math.log(S / K) + (r + 0.5 * sigma * sigma) * T) / vol_sqrt
    d2 = d1 - vol_sqrt
    return K * math.exp(-r * T) * _norm_cdf(-d2) - S * _norm_cdf(-d1)


def _option_value(kind: str, S: float, K: float, T: float, sigma: float, r: float) -> float:
    if kind == "single":
        return _bs_call(S, K, T, sigma, r)
    if kind == "straddle":
        return _bs_call(S, K, T, sigma, r) + _bs_put(S, K, T, sigma, r)
    raise ValueError(f"Unsupported kind: {kind}")


def _snapshot_contract_value(
    snapshots: pd.DataFrame,
    *,
    underlying: str,
    quote_date: pd.Timestamp,
    contract_codes: list[str],
    side: str,
) -> float | None:
    if snapshots is None or snapshots.empty or not contract_codes:
        return None

    underlying = underlying if underlying.startswith("US.") else f"US.{underlying}"
    date_str = quote_date.strftime("%Y-%m-%d")
    sub = snapshots[
        (snapshots["underlying"].astype(str) == underlying)
        & (snapshots["date"].astype(str) == date_str)
        & (snapshots["code"].astype(str).isin(contract_codes))
    ].copy()
    if sub.empty:
        return None

    prices = []
    preferred_col = "ask" if side == "entry" else "bid"
    for code in contract_codes:
        row = sub[sub["code"].astype(str) == str(code)]
        if row.empty:
            return None
        candidate = pd.to_numeric(row.iloc[0].get(preferred_col), errors="coerce")
        if pd.isna(candidate) or float(candidate) <= 0:
            candidate = pd.to_numeric(row.iloc[0].get("mid"), errors="coerce")
        if pd.isna(candidate) or float(candidate) <= 0:
            candidate = pd.to_numeric(row.iloc[0].get("last_price"), errors="coerce")
        if pd.isna(candidate) or float(candidate) <= 0:
            return None
        prices.append(float(candidate))
    return float(sum(prices))


def _expiry_payoff(kind: str, S: float, K: float) -> float:
    if kind == "single":
        return max(S - K, 0.0)
    if kind == "straddle":
        return max(S - K, 0.0) + max(K - S, 0.0)
    raise ValueError(f"Unsupported kind: {kind}")


def _nearest_pos_on_or_after(index: pd.DatetimeIndex, date: pd.Timestamp) -> int | None:
    normalized = pd.DatetimeIndex(index).normalize()
    pos = int(normalized.searchsorted(date.normalize(), side="left"))
    if pos >= len(index):
        return None
    return pos


def _realized_vol_fallback(closes: pd.Series, pos: int) -> float:
    ret = np.log(closes / closes.shift(1)).iloc[max(0, pos - 20 + 1):pos + 1]
    vol = float(ret.std() * math.sqrt(252)) if ret.notna().sum() >= 10 else 0.35
    return vol if math.isfinite(vol) and vol > 0 else 0.35


def _mark_episodes(strong: pd.DataFrame, max_gap_days: int = 4) -> pd.DataFrame:
    out = strong.sort_values(["symbol", "kind", "date"]).copy()
    out["episode_id"] = -1
    out["is_episode_entry"] = False
    episode_id = 0
    for (_symbol, _kind), group in out.groupby(["symbol", "kind"], sort=False):
        prev_date = None
        for idx, row in group.iterrows():
            current = pd.Timestamp(row["date"])
            if prev_date is None or (current - prev_date).days > max_gap_days:
                episode_id += 1
                out.at[idx, "is_episode_entry"] = True
            out.at[idx, "episode_id"] = episode_id
            prev_date = current
    return out


def _prepare_history(symbols: list[str], period: str) -> dict[str, pd.DataFrame]:
    histories: dict[str, pd.DataFrame] = {}
    for symbol in symbols:
        df = fetch_us_history(symbol, period=period)
        if df is None or df.empty:
            continue
        hist = df.sort_index().copy()
        hist.index = pd.to_datetime(hist.index).tz_localize(None).normalize()
        histories[symbol] = hist
        print(f"[+] {symbol}: {len(hist)} rows")
    return histories


def _simulate_trade(
    row: pd.Series,
    hist: pd.DataFrame,
    risk_free: float,
    snapshots: pd.DataFrame | None = None,
    use_snapshots: bool = True,
    execution_filter: bool = False,
    require_snapshot_entry: bool = False,
    max_single_debit_per_contract: float = MAX_SINGLE_DEBIT_PER_CONTRACT,
    max_straddle_debit_per_contract: float = MAX_STRADDLE_DEBIT_PER_CONTRACT,
    max_entry_spread_pct_mid: float | None = MAX_ENTRY_SPREAD_PCT_MID,
) -> TradeResult | None:
    signal_date = pd.Timestamp(row["date"])
    pos = _nearest_pos_on_or_after(hist.index, signal_date)
    if pos is None:
        return None

    closes = hist["close"].astype(float)
    entry_close = float(closes.iloc[pos])
    if not math.isfinite(entry_close) or entry_close <= 0:
        return None

    kind = str(row["kind"])
    if kind == "single":
        dte_days = SINGLE_DTE_DAYS
        time_stop_days = SINGLE_TIME_STOP_DAYS
        take_profit_mult = SINGLE_TAKE_PROFIT_MULT
        hard_stop_mult = SINGLE_HARD_STOP_MULT
    elif kind == "straddle":
        dte_days = STRADDLE_DTE_DAYS
        time_stop_days = STRADDLE_TIME_STOP_DAYS
        take_profit_mult = STRADDLE_TAKE_PROFIT_MULT
        hard_stop_mult = STRADDLE_HARD_STOP_MULT
    else:
        return None

    iv_raw = row.get("realized_vol")
    if pd.isna(iv_raw) or float(iv_raw) <= 0:
        sigma = _realized_vol_fallback(closes, pos)
    else:
        sigma = float(iv_raw) / 100.0
    sigma = min(max(sigma, 0.08), 2.50)

    pricing_source = "proxy_bs"
    selected_contract_codes: list[str] = []
    entry_quote = ""
    entry_spread = None

    strike = entry_close
    entry_value = _option_value(kind, entry_close, strike, dte_days / 365.0, sigma, risk_free)
    if use_snapshots and snapshots is not None and not snapshots.empty:
        picked = select_snapshot_contract(
            underlying=str(row["symbol"]),
            trade_date=signal_date,
            kind=kind,
            target_dte=dte_days,
            snapshots=snapshots,
            max_date_gap_days=0,
        )
        if not picked.empty:
            ask = pd.to_numeric(picked.get("ask"), errors="coerce")
            mid = pd.to_numeric(picked.get("mid"), errors="coerce")
            last = pd.to_numeric(picked.get("last_price"), errors="coerce")
            entry_prices = ask.where(ask > 0, mid.where(mid > 0, last))
            entry_prices = entry_prices.dropna()
            if not entry_prices.empty and (entry_prices > 0).all():
                entry_value = float(entry_prices.sum())
                strike_vals = pd.to_numeric(picked.get("strike_price"), errors="coerce").dropna()
                if not strike_vals.empty:
                    strike = float(strike_vals.mean())
                iv_vals = pd.to_numeric(picked.get("iv"), errors="coerce").dropna()
                if not iv_vals.empty and float(iv_vals.mean()) > 0:
                    sigma = float(iv_vals.mean()) / 100.0
                pricing_source = "snapshot_entry_proxy_exit"
                selected_contract_codes = picked["code"].astype(str).tolist()
                entry_quote = "ask"
                entry_spread = entry_spread_pct_mid(picked)
                if execution_filter:
                    gate = evaluate_long_option_entry(
                        picked,
                        kind=kind,
                        max_single_debit=max_single_debit_per_contract,
                        max_straddle_debit=max_straddle_debit_per_contract,
                        max_spread_pct_mid=max_entry_spread_pct_mid,
                    )
                    if not gate.allowed:
                        return None
    if execution_filter:
        if require_snapshot_entry and not selected_contract_codes:
            return None
        max_debit = max_single_debit_per_contract if kind == "single" else max_straddle_debit_per_contract
        if entry_value * 100.0 > max_debit:
            return None
    if not math.isfinite(entry_value) or entry_value <= 0:
        return None

    take_profit = entry_value * take_profit_mult
    hard_stop = entry_value * hard_stop_mult

    exit_pos = pos
    exit_reason = "time_stop"
    exit_value = entry_value
    snapshot_exit_observations = 0
    proxy_exit_observations = 0
    max_pos = min(len(closes) - 1, pos + time_stop_days)
    if max_pos <= pos:
        return None

    for j in range(pos + 1, max_pos + 1):
        days_elapsed = max(0, (hist.index[j] - hist.index[pos]).days)
        remaining = max((dte_days - days_elapsed) / 365.0, 0.0)
        snapshot_value = None
        if selected_contract_codes:
            snapshot_value = _snapshot_contract_value(
                snapshots,
                underlying=str(row["symbol"]),
                quote_date=hist.index[j],
                contract_codes=selected_contract_codes,
                side="exit",
            )
        if snapshot_value is not None:
            value = snapshot_value
            snapshot_exit_observations += 1
        else:
            value = _option_value(kind, float(closes.iloc[j]), strike, remaining, sigma, risk_free)
            proxy_exit_observations += 1
        if value >= take_profit:
            exit_pos = j
            exit_value = value
            exit_reason = "take_profit"
            break
        if value <= hard_stop:
            exit_pos = j
            exit_value = value
            exit_reason = "hard_stop"
            break
        exit_pos = j
        exit_value = value

    if selected_contract_codes and snapshot_exit_observations > 0:
        if proxy_exit_observations == 0:
            pricing_source = "snapshot_entry_snapshot_exit"
        else:
            pricing_source = "snapshot_entry_mixed_exit"

    dte_target_date = hist.index[pos] + pd.Timedelta(days=dte_days)
    dte_pos = _nearest_pos_on_or_after(hist.index, dte_target_date)
    if dte_pos is None:
        return None

    dte_close = float(closes.iloc[dte_pos])
    payoff = _expiry_payoff(kind, dte_close, strike)

    pnl_pct = (exit_value / entry_value - 1.0) * 100.0
    expiry_proxy_pnl_pct = (payoff / entry_value - 1.0) * 100.0
    underlying_ret_to_exit = (float(closes.iloc[exit_pos]) / entry_close - 1.0) * 100.0
    dte_underlying_ret = (dte_close / entry_close - 1.0) * 100.0

    return TradeResult(
        episode_id=int(row["episode_id"]),
        is_episode_entry=bool(row["is_episode_entry"]),
        date=hist.index[pos].strftime("%Y-%m-%d"),
        symbol=str(row["symbol"]),
        kind=kind,
        signal=str(row["signal"]),
        entry_close=entry_close,
        strike=strike,
        dte_days=dte_days,
        time_stop_days=time_stop_days,
        iv_used_pct=sigma * 100.0,
        entry_debit=entry_value,
        entry_debit_per_contract=entry_value * 100.0,
        breakeven_move_pct=entry_value / entry_close * 100.0,
        exit_date=hist.index[exit_pos].strftime("%Y-%m-%d"),
        exit_reason=exit_reason,
        exit_value=exit_value,
        pnl_pct=pnl_pct,
        pnl_per_contract=(exit_value - entry_value) * 100.0,
        underlying_ret_to_exit_pct=underlying_ret_to_exit,
        dte_exit_date=hist.index[dte_pos].strftime("%Y-%m-%d"),
        dte_underlying_ret_pct=dte_underlying_ret,
        expiry_proxy_pnl_pct=expiry_proxy_pnl_pct,
        pricing_source=pricing_source,
        contract_codes=",".join(selected_contract_codes),
        entry_quote=entry_quote,
        entry_spread_pct_mid=entry_spread,
    )


def _profit_factor(pnl: pd.Series) -> float:
    gains = pnl[pnl > 0].sum()
    losses = pnl[pnl < 0].sum()
    if losses == 0:
        return float("inf") if gains > 0 else 0.0
    return float(gains / abs(losses))


def summarize(df: pd.DataFrame, label: str) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()

    def agg(group: pd.DataFrame) -> pd.Series:
        pnl = group["pnl_pct"].astype(float)
        expiry = group["expiry_proxy_pnl_pct"].astype(float)
        return pd.Series({
            "scope": label,
            "trades": len(group),
            "win_rate_pct": (pnl > 0).mean() * 100.0,
            "mean_pnl_pct": pnl.mean(),
            "median_pnl_pct": pnl.median(),
            "profit_factor": _profit_factor(pnl),
            "avg_win_pct": pnl[pnl > 0].mean() if (pnl > 0).any() else 0.0,
            "avg_loss_pct": pnl[pnl < 0].mean() if (pnl < 0).any() else 0.0,
            "best_pnl_pct": pnl.max(),
            "worst_pnl_pct": pnl.min(),
            "mean_expiry_proxy_pnl_pct": expiry.mean(),
            "median_expiry_proxy_pnl_pct": expiry.median(),
            "mean_underlying_ret_to_exit_pct": group["underlying_ret_to_exit_pct"].mean(),
            "mean_dte_underlying_ret_pct": group["dte_underlying_ret_pct"].mean(),
        })

    overall = agg(df).to_frame().T
    by_kind = df.groupby("kind", sort=True).apply(agg, include_groups=False).reset_index()
    by_symbol_kind = df.groupby(["symbol", "kind"], sort=True).apply(agg, include_groups=False).reset_index()
    return pd.concat([overall, by_kind, by_symbol_kind], ignore_index=True)


def run(args: argparse.Namespace) -> tuple[pd.DataFrame, pd.DataFrame]:
    signals = pd.read_csv(args.signals, parse_dates=["date"])
    strong = signals[signals["strength"] == "强机会"].copy()
    if strong.empty:
        return pd.DataFrame(), pd.DataFrame()

    strong = _mark_episodes(strong)
    histories = _prepare_history(sorted(strong["symbol"].unique()), args.period)
    snapshots = _load_snapshots(args.snapshots) if args.use_snapshots else pd.DataFrame()
    if args.use_snapshots:
        print(f"snapshots: {len(snapshots)} rows from {args.snapshots}")

    rows: list[dict] = []
    for _, row in strong.iterrows():
        hist = histories.get(str(row["symbol"]))
        if hist is None or hist.empty:
            continue
        result = _simulate_trade(
            row,
            hist,
            args.risk_free,
            snapshots=snapshots,
            use_snapshots=args.use_snapshots,
            execution_filter=args.execution_filter,
            require_snapshot_entry=args.require_snapshot_entry,
            max_single_debit_per_contract=args.max_single_debit_per_contract,
            max_straddle_debit_per_contract=args.max_straddle_debit_per_contract,
            max_entry_spread_pct_mid=args.max_entry_spread_pct_mid,
        )
        if result is not None:
            rows.append(asdict(result))

    trades = pd.DataFrame(rows)
    if trades.empty:
        return trades, pd.DataFrame()

    episode_trades = trades[trades["is_episode_entry"]].copy()
    summary = pd.concat(
        [
            summarize(trades, "signal_day"),
            summarize(episode_trades, "episode_entry"),
        ],
        ignore_index=True,
    )
    return trades, summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Proxy PnL review for historical strong option signals")
    parser.add_argument("--signals", type=Path, default=ROOT / "historical_option_signal_review.csv")
    parser.add_argument("--period", default="1y")
    parser.add_argument("--risk-free", type=float, default=0.04)
    parser.add_argument("--snapshots", type=Path, default=ROOT / "option_chain_snapshots.csv")
    parser.add_argument("--use-snapshots", action="store_true", help="Use local option-chain snapshots for entry price when available")
    parser.add_argument("--execution-filter", action="store_true", help="Apply v4 execution gates before accepting a trade")
    parser.add_argument("--require-snapshot-entry", action="store_true", help="Skip trades without same-day real option-chain entry quotes")
    parser.add_argument("--max-single-debit-per-contract", type=float, default=MAX_SINGLE_DEBIT_PER_CONTRACT)
    parser.add_argument("--max-straddle-debit-per-contract", type=float, default=MAX_STRADDLE_DEBIT_PER_CONTRACT)
    parser.add_argument("--max-entry-spread-pct-mid", type=float, default=MAX_ENTRY_SPREAD_PCT_MID)
    parser.add_argument("--output", type=Path, default=ROOT / "historical_option_pnl_review.csv")
    parser.add_argument("--summary-output", type=Path, default=ROOT / "historical_option_pnl_summary.csv")
    args = parser.parse_args()

    print("══ Historical Option PnL Proxy Review ══")
    print(f"signals: {args.signals}")
    print(f"risk_free: {args.risk_free:.2%}")
    trades, summary = run(args)
    if trades.empty:
        print("No completed strong-signal trades.")
        return

    trades.to_csv(args.output, index=False)
    summary.to_csv(args.summary_output, index=False)

    display_cols = [
        "scope",
        "kind",
        "trades",
        "win_rate_pct",
        "mean_pnl_pct",
        "median_pnl_pct",
        "profit_factor",
        "worst_pnl_pct",
        "mean_expiry_proxy_pnl_pct",
    ]
    overall = summary[summary["symbol"].isna() if "symbol" in summary.columns else [True] * len(summary)].copy()
    print()
    print("[1] overall + by-kind summary")
    print(overall[[c for c in display_cols if c in overall.columns]].to_string(index=False, float_format=lambda x: f"{x:+.2f}"))

    print()
    print("[2] exit reasons")
    print(
        trades.groupby(["is_episode_entry", "kind", "exit_reason"])
        .size()
        .rename("trades")
        .reset_index()
        .to_string(index=False)
    )

    print()
    print(f"trades: {args.output}")
    print(f"summary: {args.summary_output}")


if __name__ == "__main__":
    main()
