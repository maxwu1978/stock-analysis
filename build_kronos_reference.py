#!/usr/bin/env python3
"""Build a current Kronos sidecar snapshot for A-share and U.S. research pages.

This snapshot is read-only for reports/pages and does not feed strategy actions.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd
from pandas.tseries.offsets import BDay

from backtest_v2 import fetch_sina_history
from fetch_data import STOCKS
from fetch_us import US_STOCKS, fetch_us_history
from kronos_reference import SNAPSHOT_PATH
from kronos_us_experiment import DEFAULT_KRONOS_REPO, _build_predictor, _ensure_kronos_repo, _kronos_direction


def _build_future_index(last_index: pd.Timestamp, pred_len: int) -> pd.Series:
    start = pd.Timestamp(last_index) + BDay(1)
    return pd.Series(pd.bdate_range(start=start, periods=pred_len))


def _prepare_a_share(code: str, lookback: int) -> tuple[pd.DataFrame, pd.Series, str]:
    df = fetch_sina_history(code, count=max(lookback + 50, 500)).copy()
    df = df.sort_index()
    df.index = pd.to_datetime(df.index).tz_localize(None)
    x_df = df[["open", "high", "low", "close", "volume"]].dropna().iloc[-lookback:].copy()
    y_timestamp = _build_future_index(x_df.index[-1], 5)
    return x_df, y_timestamp, STOCKS[code]


def _prepare_us(ticker: str, period: str, lookback: int) -> tuple[pd.DataFrame, pd.Series, str]:
    df = fetch_us_history(ticker, period=period).copy()
    df = df.sort_index()
    df.index = pd.to_datetime(df.index).tz_localize(None)
    x_df = df[["open", "high", "low", "close", "volume"]].dropna().iloc[-lookback:].copy()
    y_timestamp = _build_future_index(x_df.index[-1], 5)
    return x_df, y_timestamp, US_STOCKS[ticker]


def build_snapshot(
    *,
    repo_path: Path,
    bootstrap: bool,
    model_size: str,
    device: str,
    lookback: int,
    pred_len: int,
    us_period: str,
    output: Path,
) -> pd.DataFrame:
    repo_path = _ensure_kronos_repo(repo_path, bootstrap)
    predictor = _build_predictor(repo_path, model_size, device)

    rows = []

    a_codes = list(STOCKS.keys())
    a_x = []
    a_xt = []
    a_yt = []
    a_meta = []
    for code in a_codes:
        x_df, y_timestamp, name = _prepare_a_share(code, lookback)
        a_x.append(x_df)
        a_xt.append(pd.Series(pd.to_datetime(x_df.index)))
        a_yt.append(y_timestamp)
        a_meta.append((code, name, x_df))
    a_pred = predictor.predict_batch(a_x, a_xt, a_yt, pred_len=pred_len, T=1.0, top_p=0.9, sample_count=1, verbose=False)
    for pred_df, (code, name, x_df) in zip(a_pred, a_meta):
        last_close = float(x_df["close"].iloc[-1])
        pred_close = float(pred_df["close"].iloc[-1])
        pred_ret = (pred_close / last_close - 1) * 100
        rows.append({
            "market": "CN",
            "symbol": code,
            "name": name,
            "as_of": x_df.index[-1].strftime("%Y-%m-%d"),
            "model": model_size,
            "lookback": lookback,
            "pred_len": pred_len,
            "last_close": round(last_close, 4),
            "pred_close_5d": round(pred_close, 4),
            "pred_ret_5d_pct": round(pred_ret, 2),
            "direction": _kronos_direction(pred_ret),
        })

    us_tickers = list(US_STOCKS.keys())
    us_x = []
    us_xt = []
    us_yt = []
    us_meta = []
    for ticker in us_tickers:
        x_df, y_timestamp, name = _prepare_us(ticker, us_period, lookback)
        us_x.append(x_df)
        us_xt.append(pd.Series(pd.to_datetime(x_df.index)))
        us_yt.append(y_timestamp)
        us_meta.append((ticker, name, x_df))
    us_pred = predictor.predict_batch(us_x, us_xt, us_yt, pred_len=pred_len, T=1.0, top_p=0.9, sample_count=1, verbose=False)
    for pred_df, (ticker, name, x_df) in zip(us_pred, us_meta):
        last_close = float(x_df["close"].iloc[-1])
        pred_close = float(pred_df["close"].iloc[-1])
        pred_ret = (pred_close / last_close - 1) * 100
        rows.append({
            "market": "US",
            "symbol": ticker,
            "name": name,
            "as_of": x_df.index[-1].strftime("%Y-%m-%d"),
            "model": model_size,
            "lookback": lookback,
            "pred_len": pred_len,
            "last_close": round(last_close, 4),
            "pred_close_5d": round(pred_close, 4),
            "pred_ret_5d_pct": round(pred_ret, 2),
            "direction": _kronos_direction(pred_ret),
        })

    df = pd.DataFrame(rows)
    output.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output, index=False, encoding="utf-8-sig")
    return df


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build Kronos research-only reference snapshot")
    parser.add_argument("--repo-path", type=Path, default=DEFAULT_KRONOS_REPO)
    parser.add_argument("--bootstrap", action="store_true")
    parser.add_argument("--model", choices=["mini", "small", "base"], default="mini")
    parser.add_argument("--device", default="auto")
    parser.add_argument("--lookback", type=int, default=180)
    parser.add_argument("--pred-len", type=int, default=5)
    parser.add_argument("--us-period", default="2y")
    parser.add_argument("--output", type=Path, default=SNAPSHOT_PATH)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    df = build_snapshot(
        repo_path=args.repo_path,
        bootstrap=args.bootstrap,
        model_size=args.model,
        device=args.device,
        lookback=args.lookback,
        pred_len=args.pred_len,
        us_period=args.us_period,
        output=args.output,
    )
    print(df[["market", "symbol", "direction", "pred_ret_5d_pct", "model"]].to_string(index=False))
    print(f"\n已写出: {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
