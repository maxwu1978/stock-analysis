#!/usr/bin/env python3
"""A-share capital-flow analysis for the public dashboard.

The module uses per-stock Eastmoney fund-flow history through AkShare. It is
kept separate from the trading decision layer: the output is an explanatory
signal about capital behavior, not an automatic trade trigger.
"""

from __future__ import annotations

import argparse
import math
from dataclasses import asdict, dataclass
from pathlib import Path
from time import time

import pandas as pd


CACHE_DIR = Path(".cache/cn_capital_flow")
DEFAULT_CACHE_MAX_AGE_SEC = 15 * 60


@dataclass
class CapitalFlowView:
    code: str
    name: str
    date: str
    close: float
    change_pct: float
    main_net: float
    main_ratio: float
    main_net_3d: float
    main_net_5d: float
    main_net_10d: float
    super_large_net_5d: float
    large_net_5d: float
    small_net_5d: float
    price_ret_5d: float
    capital_score: float
    intent: str
    confidence: str
    explanation: str


def market_for_code(code: str) -> str:
    """Return AkShare market suffix for a 6-digit A-share code."""
    return "sh" if str(code).startswith(("5", "6", "9")) else "sz"


def _cache_path(code: str) -> Path:
    return CACHE_DIR / f"{code}.csv"


def _read_cached_flow(code: str, max_age_sec: int) -> pd.DataFrame | None:
    path = _cache_path(code)
    if not path.exists():
        return None
    if max_age_sec >= 0 and time() - path.stat().st_mtime > max_age_sec:
        return None
    try:
        df = pd.read_csv(path)
        if "日期" in df.columns:
            df["日期"] = pd.to_datetime(df["日期"], errors="coerce").dt.date
        return df
    except Exception:
        return None


def fetch_stock_capital_flow(
    code: str,
    *,
    use_cache: bool = True,
    cache_max_age_sec: int = DEFAULT_CACHE_MAX_AGE_SEC,
) -> pd.DataFrame:
    """Fetch recent per-stock capital-flow history.

    AkShare's stock_individual_fund_flow is much faster for a specific stock
    than full-market rankings and contains the fields needed for this section.
    """
    if use_cache:
        cached = _read_cached_flow(code, cache_max_age_sec)
        if cached is not None and not cached.empty:
            return cached

    import akshare as ak

    df = ak.stock_individual_fund_flow(stock=code, market=market_for_code(code))
    if df is None or df.empty:
        return pd.DataFrame()

    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    if use_cache:
        df.to_csv(_cache_path(code), index=False)
    return df


def _num(df: pd.DataFrame, col: str) -> pd.Series:
    if col not in df.columns:
        return pd.Series([math.nan] * len(df), index=df.index, dtype="float64")
    return pd.to_numeric(df[col], errors="coerce")


def _sum_tail(series: pd.Series, n: int) -> float:
    return float(series.tail(n).sum(skipna=True))


def _ret_tail(close: pd.Series, n: int) -> float:
    close = close.dropna()
    if len(close) <= n:
        return 0.0
    base = float(close.iloc[-n - 1])
    last = float(close.iloc[-1])
    if base == 0:
        return 0.0
    return (last / base - 1.0) * 100.0


def _score_flow(df: pd.DataFrame) -> float:
    main_net = _num(df, "主力净流入-净额")
    main_ratio = _num(df, "主力净流入-净占比")
    super_large = _num(df, "超大单净流入-净额")
    large = _num(df, "大单净流入-净额")
    small = _num(df, "小单净流入-净额")

    latest_ratio = float(main_ratio.iloc[-1]) if len(main_ratio) else 0.0
    recent_abs = main_net.tail(30).abs().median()
    if pd.isna(recent_abs) or recent_abs <= 0:
        recent_abs = max(float(main_net.tail(30).abs().mean(skipna=True) or 1.0), 1.0)

    d5_component = max(-35.0, min(35.0, _sum_tail(main_net, 5) / recent_abs * 10.0))
    d10_component = max(-20.0, min(20.0, _sum_tail(main_net, 10) / recent_abs * 4.0))
    latest_component = max(-30.0, min(30.0, latest_ratio * 1.8))
    large_component = max(-15.0, min(15.0, _sum_tail(super_large + large, 5) / recent_abs * 4.0))
    small_component = max(-10.0, min(10.0, -_sum_tail(small, 5) / recent_abs * 2.0))
    return round(max(-100.0, min(100.0, latest_component + d5_component + d10_component + large_component + small_component)), 1)


def _confidence(score: float, main_ratio: float, main_net_5d: float) -> str:
    if abs(score) >= 55 and abs(main_ratio) >= 5 and abs(main_net_5d) > 0:
        return "高"
    if abs(score) >= 28 or abs(main_ratio) >= 4:
        return "中"
    return "低"


def _classify_intent(
    *,
    main_net: float,
    main_ratio: float,
    main_net_3d: float,
    main_net_5d: float,
    main_net_10d: float,
    super_large_net_5d: float,
    large_net_5d: float,
    small_net_5d: float,
    price_ret_5d: float,
    capital_score: float,
) -> tuple[str, str]:
    big_net_5d = super_large_net_5d + large_net_5d

    if main_net_5d > 0 and big_net_5d > 0 and price_ret_5d <= 2.5 and small_net_5d < 0:
        return "吸筹", "主力和大单连续净流入，但价格未明显拉升，小单偏流出，偏低位承接。"
    if main_net_3d > 0 and main_ratio >= 4 and price_ret_5d > 2.0 and big_net_5d > 0:
        return "拉升", "短线主力净流入与价格同步上行，大单结构支持趋势推进。"
    if price_ret_5d > 3.0 and (main_net_3d < 0 or main_ratio < -3) and small_net_5d > 0:
        return "派发", "价格仍强但主力转净流出，小单承接增加，警惕高位换手。"
    if main_net_5d < 0 and main_net_10d < 0 and (price_ret_5d < 0 or capital_score < -35):
        return "撤退", "5日和10日主力资金同时净流出，价格或资金结构偏弱。"
    if main_net > 0 and main_net_5d > 0 and capital_score > 20:
        return "流入确认", "今日和5日主力净流入一致，但价格结构未满足吸筹或拉升特征。"
    if main_net < 0 and main_net_5d < 0 and capital_score < -20:
        return "流出确认", "今日和5日主力净流出一致，但尚未形成明确撤退/派发形态。"
    return "分歧", "资金、价格和大小单结构不一致，暂不解释为明确主力意图。"


def analyze_stock_capital_flow(
    code: str,
    name: str,
    *,
    use_cache: bool = True,
    cache_max_age_sec: int = DEFAULT_CACHE_MAX_AGE_SEC,
) -> CapitalFlowView:
    df = fetch_stock_capital_flow(code, use_cache=use_cache, cache_max_age_sec=cache_max_age_sec)
    if df.empty:
        raise RuntimeError(f"{code} capital-flow history is empty")

    df = df.dropna(subset=["日期"]).copy()
    for col in [
        "收盘价",
        "涨跌幅",
        "主力净流入-净额",
        "主力净流入-净占比",
        "超大单净流入-净额",
        "大单净流入-净额",
        "小单净流入-净额",
    ]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    latest = df.iloc[-1]
    main_net = _num(df, "主力净流入-净额")
    super_large = _num(df, "超大单净流入-净额")
    large = _num(df, "大单净流入-净额")
    small = _num(df, "小单净流入-净额")
    close = _num(df, "收盘价")

    view_args = {
        "main_net": float(latest.get("主力净流入-净额", 0) or 0),
        "main_ratio": float(latest.get("主力净流入-净占比", 0) or 0),
        "main_net_3d": _sum_tail(main_net, 3),
        "main_net_5d": _sum_tail(main_net, 5),
        "main_net_10d": _sum_tail(main_net, 10),
        "super_large_net_5d": _sum_tail(super_large, 5),
        "large_net_5d": _sum_tail(large, 5),
        "small_net_5d": _sum_tail(small, 5),
        "price_ret_5d": _ret_tail(close, 5),
    }
    score = _score_flow(df)
    intent, explanation = _classify_intent(**view_args, capital_score=score)

    return CapitalFlowView(
        code=code,
        name=name,
        date=str(latest.get("日期", "")),
        close=float(latest.get("收盘价", 0) or 0),
        change_pct=float(latest.get("涨跌幅", 0) or 0),
        capital_score=score,
        intent=intent,
        confidence=_confidence(score, view_args["main_ratio"], view_args["main_net_5d"]),
        explanation=explanation,
        **view_args,
    )


def analyze_capital_flows(
    stocks: dict[str, str],
    *,
    use_cache: bool = True,
    cache_max_age_sec: int = DEFAULT_CACHE_MAX_AGE_SEC,
) -> list[CapitalFlowView]:
    rows: list[CapitalFlowView] = []
    for code, name in stocks.items():
        try:
            rows.append(
                analyze_stock_capital_flow(
                    code,
                    name,
                    use_cache=use_cache,
                    cache_max_age_sec=cache_max_age_sec,
                )
            )
        except Exception as exc:
            rows.append(
                CapitalFlowView(
                    code=code,
                    name=name,
                    date="-",
                    close=0.0,
                    change_pct=0.0,
                    main_net=0.0,
                    main_ratio=0.0,
                    main_net_3d=0.0,
                    main_net_5d=0.0,
                    main_net_10d=0.0,
                    super_large_net_5d=0.0,
                    large_net_5d=0.0,
                    small_net_5d=0.0,
                    price_ret_5d=0.0,
                    capital_score=0.0,
                    intent="数据不足",
                    confidence="低",
                    explanation=str(exc),
                )
            )
    rank = {"吸筹": 0, "拉升": 1, "流入确认": 2, "分歧": 3, "流出确认": 4, "派发": 5, "撤退": 6, "数据不足": 7}
    return sorted(rows, key=lambda r: (rank.get(r.intent, 9), -r.capital_score, r.code))


def format_money_cn(value: float) -> str:
    if value is None or pd.isna(value):
        return "-"
    sign = "+" if value > 0 else ""
    abs_value = abs(float(value))
    if abs_value >= 1e8:
        return f"{sign}{value / 1e8:.2f}亿"
    if abs_value >= 1e4:
        return f"{sign}{value / 1e4:.0f}万"
    return f"{sign}{value:.0f}"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Analyze A-share capital flows")
    parser.add_argument("--no-cache", action="store_true", help="ignore cached flow files")
    parser.add_argument("--csv", help="optional CSV output path")
    return parser.parse_args()


def main() -> None:
    from fetch_data import STOCKS

    args = parse_args()
    rows = analyze_capital_flows(STOCKS, use_cache=not args.no_cache)
    df = pd.DataFrame([asdict(r) for r in rows])
    print(df[["code", "name", "date", "main_net", "main_ratio", "main_net_5d", "capital_score", "intent", "confidence", "explanation"]].to_string(index=False))
    if args.csv:
        df.to_csv(args.csv, index=False)
        print(f"\n输出: {args.csv}")


if __name__ == "__main__":
    main()
