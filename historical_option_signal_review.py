#!/usr/bin/env python3
"""Replay current option signal rules over historical daily bars."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

from fetch_us import fetch_us_history
from fractal_survey import mfdfa_spectrum
from option_fractal_advisor import WATCHLISTS as SINGLE_WATCHLISTS
from option_fractal_advisor import classify_regime


ROOT = Path(__file__).resolve().parent


@dataclass
class ReviewRow:
    date: str
    symbol: str
    kind: str
    signal: str
    strength: str
    confidence: str
    regime: str
    asym: float
    delta_alpha: float
    rsi6: float
    ma20_diff_pct: float
    realized_vol: float
    iv_proxy_rank: float | None


def _ticker(code: str) -> str:
    return code.split(".")[-1].upper()


def _rsi6(closes: pd.Series) -> float:
    delta = closes.diff()
    gain = delta.where(delta > 0, 0).rolling(6).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(6).mean()
    rs = gain / loss.replace(0, np.nan)
    val = (100 - 100 / (1 + rs)).iloc[-1]
    return float(val) if pd.notna(val) else 50.0


def _feature_at(df: pd.DataFrame, pos: int) -> dict | None:
    sub = df.iloc[: pos + 1].copy()
    if len(sub) < 150:
        return None
    closes = sub["close"].astype(float)
    log_ret = np.log(closes / closes.shift(1)).dropna()
    if len(log_ret) < 120:
        return None
    spec = mfdfa_spectrum(log_ret.iloc[-120:])
    ma20 = closes.rolling(20).mean().iloc[-1]
    ma20_diff = (closes.iloc[-1] / ma20 - 1) * 100 if pd.notna(ma20) and ma20 else np.nan
    realized_vol = float(log_ret.iloc[-20:].std() * np.sqrt(252) * 100)

    return {
        "asym": spec.get("asym"),
        "hq2": spec.get("hq2"),
        "delta_alpha": spec.get("delta_alpha"),
        "alpha0": spec.get("alpha0"),
        "rsi6": _rsi6(closes),
        "ma20_diff_pct": float(ma20_diff) if pd.notna(ma20_diff) else 0.0,
        "vol_20d_ann": realized_vol,
        "realized_vol": realized_vol,
    }


def _iv_proxy_rank(df: pd.DataFrame, pos: int) -> float | None:
    """Historical IV-rank proxy using realized volatility percentile.

    Current live straddle code uses option-chain IV rank when available and falls
    back to realized-vol based estimates. Historical option chains are not
    available here, so this proxy keeps the rule testable.
    """
    sub = df.iloc[: pos + 1].copy()
    closes = sub["close"].astype(float)
    log_ret = np.log(closes / closes.shift(1))
    rv20 = log_ret.rolling(20).std() * np.sqrt(252) * 100
    hist = rv20.dropna().iloc[-252:]
    if len(hist) < 60:
        return None
    cur = hist.iloc[-1]
    return float((hist < cur).mean() * 100)


def _straddle_signal(feat: dict, iv_rank: float | None) -> tuple[str, str, str]:
    delta_a = feat.get("delta_alpha", 0) or 0
    realized_vol = feat.get("realized_vol", 0) or 0
    if iv_rank is not None:
        iv_low = iv_rank < 30
        iv_high = iv_rank > 70
    else:
        iv_low = False
        iv_high = False

    if delta_a > 0.6 and iv_high:
        return "WAIT_IV_HIGH", "无机会", ""
    if delta_a > 0.6 and iv_low and (realized_vol >= 40 or delta_a > 1.0):
        return "BUY_STRADDLE_WEAK", "弱机会", "LOW"
    if delta_a > 0.6 and iv_low:
        return "BUY_STRADDLE_STRONG", "强机会", "MEDIUM"
    if delta_a > 0.6:
        return "BUY_STRADDLE_WEAK", "弱机会", "LOW"
    if iv_low:
        return "BUY_STRADDLE_VOL_ONLY", "弱机会", "LOW"
    if iv_high:
        return "WAIT_IV_HIGH_SELL_CANDIDATE", "无机会", ""
    return "WAIT", "无机会", ""


def _review_symbol(symbol: str, months: int, period: str) -> list[ReviewRow]:
    df = fetch_us_history(symbol, period=period)
    if df is None or df.empty:
        return []
    df = df.sort_index()
    cutoff = pd.Timestamp(datetime.now() - timedelta(days=months * 31))
    rows: list[ReviewRow] = []

    for pos, (idx, _) in enumerate(df.iterrows()):
        if pd.Timestamp(idx) < cutoff:
            continue
        feat = _feature_at(df, pos)
        if not feat:
            continue

        regime = classify_regime(feat)
        signal = regime["strategy"]
        confidence = regime.get("confidence") or ""
        if signal == "BUY_CALL" and confidence == "HIGH":
            strength = "强机会"
        elif signal.startswith("BUY_") and confidence in {"LOW", "MEDIUM"}:
            strength = "弱机会"
        else:
            strength = "无机会"

        rows.append(ReviewRow(
            date=pd.Timestamp(idx).strftime("%Y-%m-%d"),
            symbol=symbol,
            kind="single",
            signal=signal,
            strength=strength,
            confidence=confidence,
            regime=regime["regime"],
            asym=float(regime.get("asym") or 0),
            delta_alpha=float(regime.get("delta_alpha") or 0),
            rsi6=float(regime.get("rsi6") or 0),
            ma20_diff_pct=float(regime.get("ma20_diff_pct") or 0),
            realized_vol=float(feat.get("realized_vol") or 0),
            iv_proxy_rank=None,
        ))

        iv_rank = _iv_proxy_rank(df, pos)
        st_signal, st_strength, st_conf = _straddle_signal(feat, iv_rank)
        rows.append(ReviewRow(
            date=pd.Timestamp(idx).strftime("%Y-%m-%d"),
            symbol=symbol,
            kind="straddle",
            signal=st_signal,
            strength=st_strength,
            confidence=st_conf,
            regime="delta_alpha_iv_proxy",
            asym=float(feat.get("asym") or 0),
            delta_alpha=float(feat.get("delta_alpha") or 0),
            rsi6=float(feat.get("rsi6") or 0),
            ma20_diff_pct=float(feat.get("ma20_diff_pct") or 0),
            realized_vol=float(feat.get("realized_vol") or 0),
            iv_proxy_rank=iv_rank,
        ))
    return rows


def _episode_count(df: pd.DataFrame, strength: str) -> pd.DataFrame:
    sub = df[df["strength"] == strength].copy()
    if sub.empty:
        return pd.DataFrame(columns=["symbol", "kind", "signal_days", "episodes"])
    rows = []
    for (symbol, kind), group in sub.groupby(["symbol", "kind"]):
        dates = pd.to_datetime(group["date"]).sort_values()
        episodes = 0
        prev = None
        for date in dates:
            if prev is None or (date - prev).days > 4:
                episodes += 1
            prev = date
        rows.append({
            "symbol": symbol,
            "kind": kind,
            "signal_days": len(group),
            "episodes": episodes,
        })
    return pd.DataFrame(rows).sort_values(["episodes", "signal_days"], ascending=False)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Replay current option strong/weak signal rules over history")
    parser.add_argument("--watchlist", default="tech", choices=sorted(SINGLE_WATCHLISTS))
    parser.add_argument("--months", type=int, default=6)
    parser.add_argument("--period", default="1y", help="history fetch period, e.g. 1y/2y/3y/5y")
    parser.add_argument("--output", type=Path, default=ROOT / "historical_option_signal_review.csv")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    symbols = [_ticker(code) for code in SINGLE_WATCHLISTS[args.watchlist]]
    all_rows: list[ReviewRow] = []
    print("\n══ Historical Option Signal Review ══")
    print(f"watchlist: {args.watchlist} ({len(symbols)} symbols)")
    print(f"window: last {args.months} months")
    print(f"history period: {args.period}")

    for symbol in symbols:
        try:
            rows = _review_symbol(symbol, args.months, args.period)
            all_rows.extend(rows)
            print(f"[+] {symbol}: {len(rows)} rows")
        except Exception as exc:
            print(f"[!] {symbol}: {exc}")

    if not all_rows:
        raise SystemExit("no rows")

    df = pd.DataFrame([row.__dict__ for row in all_rows])
    df.to_csv(args.output, index=False)

    summary = (
        df.groupby(["kind", "strength"], as_index=False)
        .size()
        .sort_values(["kind", "strength"])
    )
    strong = df[df["strength"] == "强机会"]
    weak = df[df["strength"] == "弱机会"]
    strong_episodes = _episode_count(df, "强机会")
    weak_episodes = _episode_count(df, "弱机会")

    print("\n[1] signal-day counts")
    print(summary.to_string(index=False))
    print("\n[2] strong signal episodes by symbol")
    print(strong_episodes.to_string(index=False) if not strong_episodes.empty else "无")
    print("\n[3] weak signal episodes by symbol")
    print(weak_episodes.head(20).to_string(index=False) if not weak_episodes.empty else "无")
    print("\n[4] strong signal details")
    cols = ["date", "symbol", "kind", "signal", "confidence", "regime", "asym", "delta_alpha", "rsi6", "ma20_diff_pct", "iv_proxy_rank"]
    print(strong[cols].to_string(index=False) if not strong.empty else "无")
    print(f"\noutput: {args.output}")


if __name__ == "__main__":
    main()
