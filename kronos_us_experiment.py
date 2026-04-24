#!/usr/bin/env python3
"""Minimal Kronos sidecar experiment for a small U.S. equity watchlist.

This script does not replace the existing factor model. It runs Kronos as a
parallel signal on a small set of U.S. tickers and writes a compact CSV report.

Modes:
  - holdout: use the latest real window as out-of-sample validation
  - latest:  use the latest lookback window to forecast future business days
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

import pandas as pd
from pandas.tseries.offsets import BDay

from fetch_us import US_STOCKS, fetch_us_history
from indicators import compute_all
from position_sizing import recommend_model_action
from probability_us import score_trend_us
from reliability import get_reliability_label, load_reliability_labels


REPO_ROOT = Path(__file__).resolve().parent
DEFAULT_KRONOS_REPO = REPO_ROOT / ".cache" / "Kronos"
DEFAULT_OUTPUT = REPO_ROOT / "kronos_us_experiment_report.csv"
KRONOS_GIT_URL = "https://github.com/shiyu-coder/Kronos.git"

MODEL_ZOO = {
    "mini": {
        "tokenizer": "NeoQuasar/Kronos-Tokenizer-2k",
        "model": "NeoQuasar/Kronos-mini",
        "max_context": 2048,
    },
    "small": {
        "tokenizer": "NeoQuasar/Kronos-Tokenizer-base",
        "model": "NeoQuasar/Kronos-small",
        "max_context": 512,
    },
    "base": {
        "tokenizer": "NeoQuasar/Kronos-Tokenizer-base",
        "model": "NeoQuasar/Kronos-base",
        "max_context": 512,
    },
}

DEFAULT_TICKERS = ["NVDA", "AMD", "WDC", "TSM", "AAPL"]


@dataclass
class SeriesBundle:
    ticker: str
    name: str
    factor_df: pd.DataFrame
    x_df: pd.DataFrame
    x_timestamp: pd.Series
    y_timestamp: pd.Series
    actual_df: pd.DataFrame | None


def _direction_from_prob(hp: dict) -> str:
    p30 = hp.get("30日")
    if not p30:
        return "--"
    pct = int(p30["上涨概率"].replace("%", ""))
    if pct >= 60:
        return "看涨"
    if pct >= 55:
        return "偏涨"
    if pct <= 35:
        return "看跌"
    if pct <= 45:
        return "偏跌"
    return "震荡"


def _kronos_direction(ret_pct: float, neutral_band: float = 1.0) -> str:
    if ret_pct >= neutral_band:
        return "看涨"
    if ret_pct <= -neutral_band:
        return "看跌"
    return "震荡"


def _ensure_kronos_repo(path: Path, bootstrap: bool) -> Path:
    if path.exists():
        return path
    if not bootstrap:
        raise FileNotFoundError(
            f"Kronos 仓库不存在: {path}\n"
            f"可重新运行并加上 --bootstrap 自动拉取官方仓库。"
        )
    path.parent.mkdir(parents=True, exist_ok=True)
    subprocess.run(
        ["git", "clone", "--depth", "1", KRONOS_GIT_URL, str(path)],
        check=True,
    )
    return path


def _load_kronos_classes(repo_path: Path):
    repo_str = str(repo_path)
    if repo_str not in sys.path:
        sys.path.insert(0, repo_str)
    try:
        from model import Kronos, KronosPredictor, KronosTokenizer
    except Exception as exc:
        raise RuntimeError(
            "无法导入 Kronos 官方模型代码。\n"
            "请确认本机已安装 torch / huggingface_hub / einops / safetensors / tqdm，"
            "且 repo_path 指向官方仓库根目录。"
        ) from exc
    return Kronos, KronosTokenizer, KronosPredictor


def _build_predictor(repo_path: Path, model_size: str, device: str):
    Kronos, KronosTokenizer, KronosPredictor = _load_kronos_classes(repo_path)
    cfg = MODEL_ZOO[model_size]
    tokenizer = KronosTokenizer.from_pretrained(cfg["tokenizer"])
    model = Kronos.from_pretrained(cfg["model"])
    actual_device = None if device == "auto" else device
    return KronosPredictor(
        model,
        tokenizer,
        device=actual_device,
        max_context=cfg["max_context"],
    )


def _prepare_series(
    ticker: str,
    *,
    period: str,
    lookback: int,
    pred_len: int,
    mode: str,
) -> SeriesBundle:
    hist = fetch_us_history(ticker, period=period).copy()
    hist = hist.sort_index()
    hist.index = pd.to_datetime(hist.index).tz_localize(None)
    required_cols = ["open", "high", "low", "close", "volume"]
    hist = hist[required_cols].dropna()

    if mode == "holdout":
        needed = lookback + pred_len
        if len(hist) < needed:
            raise ValueError(f"{ticker} 历史数据不足，至少需要 {needed} 行，当前 {len(hist)} 行")
        window = hist.iloc[-needed:].copy()
        x_df = window.iloc[:-pred_len].copy()
        actual_df = window.iloc[-pred_len:].copy()
        factor_df = hist.iloc[:-pred_len].copy()
        y_index = actual_df.index
    else:
        if len(hist) < lookback:
            raise ValueError(f"{ticker} 历史数据不足，至少需要 {lookback} 行，当前 {len(hist)} 行")
        x_df = hist.iloc[-lookback:].copy()
        actual_df = None
        factor_df = hist.copy()
        start = x_df.index[-1] + BDay(1)
        y_index = pd.bdate_range(start=start, periods=pred_len)

    return SeriesBundle(
        ticker=ticker,
        name=US_STOCKS.get(ticker, ticker),
        factor_df=factor_df,
        x_df=x_df,
        x_timestamp=pd.Series(pd.to_datetime(x_df.index)),
        y_timestamp=pd.Series(pd.to_datetime(y_index)),
        actual_df=actual_df,
    )


def _factor_sidecar(bundle: SeriesBundle, reliability_labels: dict, apply_macro_overlay: bool) -> dict:
    factor_df = compute_all(bundle.factor_df.copy())
    prob = score_trend_us(factor_df, symbol=bundle.ticker, apply_macro_overlay=apply_macro_overlay)
    if "error" in prob:
        return {
            "factor_score": None,
            "factor_direction": "?",
            "factor_action": "N/A",
            "factor_plan": "-",
            "reliability": get_reliability_label(reliability_labels, "us", bundle.ticker),
            "macro_penalty": 0,
        }

    direction = _direction_from_prob(prob.get("historical_prob", {}))
    reliability = get_reliability_label(reliability_labels, "us", bundle.ticker)
    penalty = int(prob.get("macro_overlay", {}).get("penalty", 0) or 0)
    decision = recommend_model_action(
        direction=direction,
        entry_price=float(bundle.factor_df["close"].iloc[-1]),
        score=prob.get("score"),
        reliability=reliability,
        macro_penalty=penalty,
    )
    plan_text = f"{decision.plan.position_tier} / {decision.plan.qty}股 / ${decision.plan.risk_budget:,.0f}"
    return {
        "factor_score": round(float(prob.get("score", 0) or 0), 2),
        "factor_direction": direction,
        "factor_action": decision.action,
        "factor_plan": plan_text,
        "reliability": reliability,
        "macro_penalty": penalty,
    }


def run_experiment(
    *,
    tickers: list[str],
    period: str,
    lookback: int,
    pred_len: int,
    mode: str,
    model_size: str,
    repo_path: Path,
    bootstrap: bool,
    device: str,
    temperature: float,
    top_p: float,
    sample_count: int,
    output: Path,
) -> pd.DataFrame:
    repo_path = _ensure_kronos_repo(repo_path, bootstrap)
    predictor = _build_predictor(repo_path, model_size, device)
    reliability_labels = load_reliability_labels()

    bundles: list[SeriesBundle] = []
    for ticker in tickers:
        bundles.append(
            _prepare_series(
                ticker,
                period=period,
                lookback=lookback,
                pred_len=pred_len,
                mode=mode,
            )
        )

    pred_df_list = predictor.predict_batch(
        df_list=[b.x_df for b in bundles],
        x_timestamp_list=[b.x_timestamp for b in bundles],
        y_timestamp_list=[b.y_timestamp for b in bundles],
        pred_len=pred_len,
        T=temperature,
        top_p=top_p,
        sample_count=sample_count,
        verbose=False,
    )

    rows = []
    for bundle, pred_df in zip(bundles, pred_df_list):
        last_close = float(bundle.x_df["close"].iloc[-1])
        pred_close_1 = float(pred_df["close"].iloc[0])
        pred_close_n = float(pred_df["close"].iloc[-1])
        pred_ret_1 = (pred_close_1 / last_close - 1) * 100
        pred_ret_n = (pred_close_n / last_close - 1) * 100
        sidecar = _factor_sidecar(bundle, reliability_labels, apply_macro_overlay=(mode == "latest"))

        row = {
            "ticker": bundle.ticker,
            "name": bundle.name,
            "mode": mode,
            "model_size": model_size,
            "lookback": lookback,
            "pred_len": pred_len,
            "last_close": round(last_close, 4),
            "pred_close_1": round(pred_close_1, 4),
            "pred_close_n": round(pred_close_n, 4),
            "pred_ret_1d_pct": round(pred_ret_1, 2),
            "pred_ret_nd_pct": round(pred_ret_n, 2),
            "kronos_direction": _kronos_direction(pred_ret_n),
            **sidecar,
        }

        if bundle.actual_df is not None:
            actual_close_n = float(bundle.actual_df["close"].iloc[-1])
            actual_ret_n = (actual_close_n / last_close - 1) * 100
            row.update(
                {
                    "actual_close_n": round(actual_close_n, 4),
                    "actual_ret_nd_pct": round(actual_ret_n, 2),
                    "direction_hit": int(pred_ret_n * actual_ret_n > 0),
                    "close_error_pct": round((pred_close_n / actual_close_n - 1) * 100, 2),
                }
            )
        rows.append(row)

    report = pd.DataFrame(rows)
    output.parent.mkdir(parents=True, exist_ok=True)
    report.to_csv(output, index=False, encoding="utf-8-sig")
    return report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Kronos 美股最小实验脚本")
    parser.add_argument("--tickers", nargs="+", default=DEFAULT_TICKERS, help="默认: NVDA AMD WDC TSM AAPL")
    parser.add_argument("--period", default="2y", help="历史数据区间, 例如 1y/2y/5y")
    parser.add_argument("--lookback", type=int, default=400, help="Kronos 输入窗口长度")
    parser.add_argument("--pred-len", type=int, default=5, help="预测步数")
    parser.add_argument("--mode", choices=["holdout", "latest"], default="holdout")
    parser.add_argument("--model", choices=sorted(MODEL_ZOO), default="small")
    parser.add_argument("--repo-path", type=Path, default=DEFAULT_KRONOS_REPO)
    parser.add_argument("--bootstrap", action="store_true", help="若本地没有官方仓库，则自动 git clone")
    parser.add_argument("--device", default="auto", help="auto/cpu/mps/cuda:0")
    parser.add_argument("--temperature", type=float, default=1.0)
    parser.add_argument("--top-p", type=float, default=0.9)
    parser.add_argument("--sample-count", type=int, default=1)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        report = run_experiment(
            tickers=args.tickers,
            period=args.period,
            lookback=args.lookback,
            pred_len=args.pred_len,
            mode=args.mode,
            model_size=args.model,
            repo_path=args.repo_path,
            bootstrap=args.bootstrap,
            device=args.device,
            temperature=args.temperature,
            top_p=args.top_p,
            sample_count=args.sample_count,
            output=args.output,
        )
    except Exception as exc:
        print(f"[!] Kronos 实验失败: {exc}", file=sys.stderr)
        return 1

    display_cols = [
        "ticker",
        "kronos_direction",
        "pred_ret_nd_pct",
        "factor_direction",
        "factor_score",
        "factor_action",
        "factor_plan",
    ]
    if "actual_ret_nd_pct" in report.columns:
        display_cols += ["actual_ret_nd_pct", "direction_hit", "close_error_pct"]

    print(report[display_cols].to_string(index=False))
    print(f"\n已写出: {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
