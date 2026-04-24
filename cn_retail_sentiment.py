#!/usr/bin/env python3
"""A-share retail sentiment proxy from public attention datasets.

This module uses public Eastmoney/AkShare attention data as a proxy for retail
crowding. It is designed as a contrarian risk overlay, not a standalone trading
signal: extreme attention is treated as chase risk, especially when price and
turnover are already stretched.
"""

from __future__ import annotations

import argparse
import contextlib
import io
import math
from dataclasses import asdict, dataclass
from pathlib import Path
from time import time

import pandas as pd


CACHE_DIR = Path(".cache/cn_retail_sentiment")
DEFAULT_CACHE_MAX_AGE_SEC = 15 * 60
COMMENT_CACHE_NAME = "stock_comment_em.csv"
HOT_RANK_CACHE_NAME = "stock_hot_rank_em.csv"
REQUIRED_COMMENT_COLUMNS = {"代码", "关注指数", "目前排名"}
REQUIRED_HOT_RANK_COLUMNS = {"代码", "当前排名"}


@dataclass
class RetailSentimentView:
    code: str
    name: str
    date: str
    attention_index: float
    comment_rank: int | None
    rank_change: float
    hot_rank: int | None
    history_rank: int | None
    price_change_pct: float
    turnover_rate: float
    retail_score: float
    contra_risk: str
    signal: str
    confidence: str
    explanation: str


def market_symbol(code: str) -> str:
    return ("SH" if str(code).startswith(("5", "6", "9")) else "SZ") + str(code)


def _cache_path(name: str) -> Path:
    return CACHE_DIR / name


def _read_cache(name: str, max_age_sec: int) -> pd.DataFrame | None:
    path = _cache_path(name)
    if not path.exists():
        return None
    if max_age_sec >= 0 and time() - path.stat().st_mtime > max_age_sec:
        return None
    try:
        return pd.read_csv(path)
    except Exception:
        return None


def _read_stale_cache(name: str) -> pd.DataFrame | None:
    return _read_cache(name, -1)


def _write_cache(name: str, df: pd.DataFrame) -> None:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    df.to_csv(_cache_path(name), index=False)


def _quiet_akshare_call(fn):
    """Suppress tqdm/progress noise from AkShare while keeping exceptions."""
    with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
        return fn()


def _normalize_code_series(series: pd.Series) -> pd.Series:
    """Normalize A-share code formats such as 600519, SH600519, 600519.SH."""
    raw = series.astype(str).str.strip()
    extracted = raw.str.extract(r"(\d{6})", expand=False)
    numeric = raw.str.extract(r"(\d+)", expand=False).fillna("").str.zfill(6).str[-6:]
    return extracted.fillna(numeric)


def _valid_comment_table(df: pd.DataFrame | None) -> bool:
    if df is None or df.empty:
        return False
    if not REQUIRED_COMMENT_COLUMNS.issubset(set(df.columns)):
        return False
    return len(df) >= 100


def _valid_hot_rank_table(df: pd.DataFrame | None) -> bool:
    if df is None or df.empty:
        return False
    if not REQUIRED_HOT_RANK_COLUMNS.issubset(set(df.columns)):
        return False
    return len(df) >= 10


def fetch_comment_table(
    *,
    use_cache: bool = True,
    cache_max_age_sec: int = DEFAULT_CACHE_MAX_AGE_SEC,
) -> pd.DataFrame:
    """Fetch Eastmoney 千股千评 table through AkShare."""
    stale = _read_stale_cache(COMMENT_CACHE_NAME) if use_cache else None
    if use_cache:
        cached = _read_cache(COMMENT_CACHE_NAME, cache_max_age_sec)
        if _valid_comment_table(cached):
            return cached

    try:
        import akshare as ak

        df = _quiet_akshare_call(ak.stock_comment_em)
    except Exception:
        return stale if _valid_comment_table(stale) else pd.DataFrame()

    if not _valid_comment_table(df):
        return stale if _valid_comment_table(stale) else pd.DataFrame()
    if use_cache:
        _write_cache(COMMENT_CACHE_NAME, df)
    return df


def fetch_hot_rank_table(
    *,
    use_cache: bool = True,
    cache_max_age_sec: int = DEFAULT_CACHE_MAX_AGE_SEC,
) -> pd.DataFrame:
    """Fetch current Eastmoney stock popularity top list."""
    stale = _read_stale_cache(HOT_RANK_CACHE_NAME) if use_cache else None
    if use_cache:
        cached = _read_cache(HOT_RANK_CACHE_NAME, cache_max_age_sec)
        if _valid_hot_rank_table(cached):
            return cached

    try:
        import akshare as ak

        df = _quiet_akshare_call(ak.stock_hot_rank_em)
    except Exception:
        return stale if _valid_hot_rank_table(stale) else pd.DataFrame()

    if not _valid_hot_rank_table(df):
        return stale if _valid_hot_rank_table(stale) else pd.DataFrame()
    if use_cache:
        _write_cache(HOT_RANK_CACHE_NAME, df)
    return df


def fetch_hot_rank_history(
    code: str,
    *,
    use_cache: bool = True,
    cache_max_age_sec: int = DEFAULT_CACHE_MAX_AGE_SEC,
) -> pd.DataFrame:
    """Fetch per-stock popularity-rank history."""
    cache_name = f"stock_hot_rank_detail_{code}.csv"
    stale = _read_stale_cache(cache_name) if use_cache else None
    if use_cache:
        cached = _read_cache(cache_name, cache_max_age_sec)
        if cached is not None and not cached.empty:
            return cached

    try:
        import akshare as ak

        df = _quiet_akshare_call(lambda: ak.stock_hot_rank_detail_em(symbol=market_symbol(code)))
    except Exception:
        return stale if stale is not None and not stale.empty else pd.DataFrame()

    if df is None or df.empty:
        return stale if stale is not None and not stale.empty else pd.DataFrame()
    if use_cache:
        _write_cache(cache_name, df)
    return df


def _safe_float(value, default: float = 0.0) -> float:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return default
    if math.isnan(out) or math.isinf(out):
        return default
    return out


def _safe_int(value) -> int | None:
    try:
        out = int(float(value))
    except (TypeError, ValueError):
        return None
    return out if out > 0 else None


def _clip(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def _heat_from_rank(rank: int | None, max_rank: int) -> float:
    if not rank:
        return 0.0
    return _clip((1.0 - min(rank, max_rank) / max_rank) * 100.0, 0.0, 100.0)


def _hot_rank_score(rank: int | None) -> float:
    if not rank:
        return 0.0
    return _clip(102.0 - rank, 0.0, 100.0)


def _classify(
    *,
    score: float,
    price_change_pct: float,
    turnover_rate: float,
    rank_change: float,
) -> tuple[str, str, str]:
    chase = price_change_pct >= 2.5 or turnover_rate >= 4.0 or rank_change >= 300
    if score >= 82 and chase:
        return "高", "追高风险", "散户关注度极高且价格/换手/排名已有追涨特征，作为反向风险处理。"
    if score >= 74:
        return "中", "拥挤", "散户关注度处于偏热区，若同时出现主力流出，应降低追涨权重。"
    if score <= 35:
        return "低", "关注低位", "散户关注度偏低，只有在主力流入和趋势改善时才视为潜在低拥挤优势。"
    return "低", "中性", "散户关注度未达到反向风险阈值。"


def _confidence(comment_found: bool, history_found: bool, hot_found: bool) -> str:
    if comment_found and history_found and hot_found:
        return "高"
    if comment_found and history_found:
        return "中"
    if comment_found or history_found or hot_found:
        return "低"
    return "低"


def analyze_stock_retail_sentiment(
    code: str,
    name: str,
    *,
    comment_table: pd.DataFrame | None = None,
    hot_rank_table: pd.DataFrame | None = None,
    use_cache: bool = True,
    cache_max_age_sec: int = DEFAULT_CACHE_MAX_AGE_SEC,
) -> RetailSentimentView:
    comment_table = comment_table if comment_table is not None else fetch_comment_table(use_cache=use_cache, cache_max_age_sec=cache_max_age_sec)
    hot_rank_table = hot_rank_table if hot_rank_table is not None else fetch_hot_rank_table(use_cache=use_cache, cache_max_age_sec=cache_max_age_sec)

    comment_row = pd.Series(dtype=object)
    if comment_table is not None and not comment_table.empty and "代码" in comment_table.columns:
        codes = _normalize_code_series(comment_table["代码"])
        matched = comment_table[codes.eq(str(code).zfill(6))]
        if not matched.empty:
            comment_row = matched.iloc[0]

    hot_row = pd.Series(dtype=object)
    if hot_rank_table is not None and not hot_rank_table.empty and "代码" in hot_rank_table.columns:
        hot_codes = _normalize_code_series(hot_rank_table["代码"])
        matched_hot = hot_rank_table[hot_codes.eq(str(code).zfill(6))]
        if not matched_hot.empty:
            hot_row = matched_hot.iloc[0]

    history_rank = None
    history_date = ""
    try:
        history = fetch_hot_rank_history(code, use_cache=use_cache, cache_max_age_sec=cache_max_age_sec)
        if history is not None and not history.empty:
            latest_history = history.dropna(subset=["排名"]).tail(1)
            if not latest_history.empty:
                history_rank = _safe_int(latest_history.iloc[0].get("排名"))
                history_date = str(latest_history.iloc[0].get("时间", ""))
    except Exception:
        history = pd.DataFrame()

    attention = _safe_float(comment_row.get("关注指数"), 0.0)
    comment_rank = _safe_int(comment_row.get("目前排名"))
    rank_change = _safe_float(comment_row.get("上升"), 0.0)
    price_change = _safe_float(comment_row.get("涨跌幅"), 0.0)
    turnover = _safe_float(comment_row.get("换手率"), 0.0)
    hot_rank = _safe_int(hot_row.get("当前排名"))

    comment_heat = _heat_from_rank(comment_rank, 6000)
    history_heat = _heat_from_rank(history_rank, 600)
    rank_change_component = _clip(rank_change / 600.0 * 18.0, -14.0, 18.0)
    price_chase_component = _clip(max(price_change, 0.0) * 2.2, 0.0, 16.0)
    turnover_component = _clip(turnover * 1.8, 0.0, 12.0)
    score = (
        attention * 0.28
        + comment_heat * 0.18
        + history_heat * 0.24
        + _hot_rank_score(hot_rank) * 0.18
        + rank_change_component
        + price_chase_component
        + turnover_component
    )
    score = round(_clip(score, 0.0, 100.0), 1)
    contra_risk, signal, explanation = _classify(
        score=score,
        price_change_pct=price_change,
        turnover_rate=turnover,
        rank_change=rank_change,
    )

    return RetailSentimentView(
        code=code,
        name=name,
        date=str(comment_row.get("交易日") or history_date or "-"),
        attention_index=round(attention, 1),
        comment_rank=comment_rank,
        rank_change=round(rank_change, 1),
        hot_rank=hot_rank,
        history_rank=history_rank,
        price_change_pct=round(price_change, 2),
        turnover_rate=round(turnover, 2),
        retail_score=score,
        contra_risk=contra_risk,
        signal=signal,
        confidence=_confidence(not comment_row.empty, history_rank is not None, hot_rank is not None),
        explanation=explanation,
    )


def analyze_retail_sentiment(
    stocks: dict[str, str],
    *,
    use_cache: bool = True,
    cache_max_age_sec: int = DEFAULT_CACHE_MAX_AGE_SEC,
) -> list[RetailSentimentView]:
    try:
        comment_table = fetch_comment_table(use_cache=use_cache, cache_max_age_sec=cache_max_age_sec)
    except Exception:
        comment_table = pd.DataFrame()
    try:
        hot_rank_table = fetch_hot_rank_table(use_cache=use_cache, cache_max_age_sec=cache_max_age_sec)
    except Exception:
        hot_rank_table = pd.DataFrame()

    rows: list[RetailSentimentView] = []
    for code, name in stocks.items():
        try:
            rows.append(
                analyze_stock_retail_sentiment(
                    code,
                    name,
                    comment_table=comment_table,
                    hot_rank_table=hot_rank_table,
                    use_cache=use_cache,
                    cache_max_age_sec=cache_max_age_sec,
                )
            )
        except Exception as exc:
            rows.append(
                RetailSentimentView(
                    code=code,
                    name=name,
                    date="-",
                    attention_index=0.0,
                    comment_rank=None,
                    rank_change=0.0,
                    hot_rank=None,
                    history_rank=None,
                    price_change_pct=0.0,
                    turnover_rate=0.0,
                    retail_score=0.0,
                    contra_risk="低",
                    signal="数据不足",
                    confidence="低",
                    explanation=str(exc),
                )
            )
    risk_rank = {"高": 0, "中": 1, "低": 2}
    return sorted(rows, key=lambda r: (risk_rank.get(r.contra_risk, 9), -r.retail_score, r.code))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Analyze A-share retail sentiment proxies")
    parser.add_argument("--no-cache", action="store_true", help="ignore cached sentiment files")
    parser.add_argument("--csv", help="optional CSV output path")
    return parser.parse_args()


def main() -> None:
    from fetch_data import STOCKS

    args = parse_args()
    rows = analyze_retail_sentiment(STOCKS, use_cache=not args.no_cache)
    df = pd.DataFrame([asdict(r) for r in rows])
    print(
        df[
            [
                "code",
                "name",
                "date",
                "attention_index",
                "comment_rank",
                "hot_rank",
                "history_rank",
                "retail_score",
                "contra_risk",
                "signal",
                "confidence",
                "explanation",
            ]
        ].to_string(index=False)
    )
    if args.csv:
        df.to_csv(args.csv, index=False)
        print(f"\n输出: {args.csv}")


if __name__ == "__main__":
    main()
