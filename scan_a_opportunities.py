#!/usr/bin/env python3
"""Scan the current A-share watchlist for actionable opportunities."""

from __future__ import annotations

import argparse
from datetime import datetime

import pandas as pd

from analyze import direction_from_prob
from cn_retail_sentiment import analyze_retail_sentiment
from fetch_data import STOCKS, fetch_all_history, fetch_realtime_quotes
from fundamental import fetch_all_financials
from indicators import compute_all
from kronos_reference import format_kronos_reference_text, get_kronos_reference, load_kronos_reference
from macro_events import get_cn_risk_warnings
from position_sizing import recommend_model_action
from probability import score_trend
from reliability import get_reliability_label, load_reliability_labels


def _prob_pct(item: dict | None) -> int | None:
    if not item:
        return None
    try:
        return int(str(item.get("上涨概率", "")).replace("%", ""))
    except Exception:
        return None


def _avg_ret(item: dict | None) -> str:
    return str(item.get("平均收益", "-")) if item else "-"


def _opportunity_level(direction: str, reliability: str, action: str, p30: int | None, fat_tail: int) -> tuple[str, str]:
    if action in {"BUILD_LONG", "PROBE_LONG"}:
        return "明显机会", "动作层已允许开仓"
    if direction in {"看涨", "偏涨"} and p30 is not None and p30 >= 60:
        if reliability != "弱":
            return "明显机会", "概率和可靠度同时通过"
        return "重点观察", "概率偏强但可靠度弱，暂不主动开仓"
    if fat_tail >= 3 and direction in {"震荡", "偏涨", "看涨"}:
        return "重点观察", "肥尾信号较高，可能有波动放大"
    if direction in {"偏跌", "看跌"}:
        return "回避", "主模型方向偏弱"
    return "普通观察", "未达到明确触发条件"


def _apply_retail_overlay(level: str, reason: str, direction: str, retail) -> tuple[str, str]:
    """Use retail crowding as a risk overlay, not a standalone trigger."""
    if not retail or retail.signal == "数据不足":
        return level, reason

    note = f"散户{retail.signal}/反向{retail.contra_risk}/评分{retail.retail_score:.1f}"
    if retail.contra_risk == "高":
        if level == "明显机会":
            return "重点观察", reason + "；" + note + "，先降级等待降温"
        if level in {"重点观察", "普通观察"}:
            return "回避", reason + "；" + note + "，追高风险优先"
        return level, reason + "；" + note

    if retail.contra_risk == "中":
        if level in {"明显机会", "重点观察"}:
            return level, reason + "；" + note + "，只允许低仓观察"
        return level, reason + "；" + note

    if retail.signal == "关注低位" and level == "普通观察" and direction in {"震荡", "偏涨", "看涨"}:
        return "重点观察", reason + "；" + note + "，低拥挤可继续跟踪"

    return level, reason + "；" + note


def _fat_tail_score(df: pd.DataFrame) -> int:
    if "fat_tail_score" not in df.columns or df.empty:
        return 0
    val = df["fat_tail_score"].iloc[-1]
    if pd.isna(val):
        return 0
    return int(val)


def scan(days: int) -> list[dict]:
    labels = load_reliability_labels()
    kronos_refs = load_kronos_reference()

    try:
        quotes = fetch_realtime_quotes()
    except Exception:
        quotes = pd.DataFrame()
    quote_map = {}
    if not quotes.empty and "代码" in quotes.columns:
        quote_map = {str(row["代码"]): row for _, row in quotes.iterrows()}

    try:
        all_fund = fetch_all_financials()
    except Exception:
        all_fund = {}
    all_hist = fetch_all_history(days=days)
    try:
        retail_rows = analyze_retail_sentiment(STOCKS)
    except Exception:
        retail_rows = []
    retail_map = {row.code: row for row in retail_rows}

    rows: list[dict] = []
    for code, name in STOCKS.items():
        df = all_hist.get(code)
        if df is None or df.empty:
            rows.append({
                "code": code,
                "name": name,
                "level": "数据不足",
                "reason": "历史数据为空",
            })
            continue

        enriched = compute_all(df, all_fund.get(code))
        prob = score_trend(enriched)
        if "error" in prob:
            rows.append({
                "code": code,
                "name": name,
                "level": "数据不足",
                "reason": prob["error"],
            })
            continue

        hp = prob.get("historical_prob", {})
        direction = direction_from_prob(hp)
        reliability = get_reliability_label(labels, "a_share", code)
        last_close = float(enriched["close"].iloc[-1])
        decision = recommend_model_action(
            direction=direction,
            entry_price=last_close,
            score=prob.get("score"),
            reliability=reliability,
            macro_penalty=0,
        )
        fat_tail = _fat_tail_score(enriched)
        p30 = _prob_pct(hp.get("30日"))
        p10 = _prob_pct(hp.get("10日"))
        level, reason = _opportunity_level(direction, reliability, decision.action, p30, fat_tail)
        retail = retail_map.get(code)
        level, reason = _apply_retail_overlay(level, reason, direction, retail)

        quote = quote_map.get(code)
        change_pct = quote.get("涨跌幅") if quote is not None else None
        kronos_text = format_kronos_reference_text(get_kronos_reference(kronos_refs, "CN", code))
        rows.append({
            "code": code,
            "name": name,
            "level": level,
            "direction": direction,
            "reliability": reliability,
            "action": decision.action,
            "tier": decision.plan.position_tier,
            "qty": decision.plan.qty,
            "risk_budget": decision.plan.risk_budget,
            "p10": p10,
            "p30": p30,
            "avg30": _avg_ret(hp.get("30日")),
            "fat_tail": fat_tail,
            "score": round(float(prob.get("score", 0)), 1),
            "last_close": last_close,
            "change_pct": change_pct,
            "kronos": kronos_text,
            "retail_signal": retail.signal if retail else "-",
            "retail_risk": retail.contra_risk if retail else "-",
            "retail_score": retail.retail_score if retail else None,
            "reason": reason,
        })

    rank = {"明显机会": 0, "重点观察": 1, "普通观察": 2, "回避": 3, "数据不足": 4}
    return sorted(rows, key=lambda row: (rank.get(row.get("level", ""), 9), -(row.get("p30") or -1), row.get("code", "")))


def print_report(rows: list[dict]) -> None:
    headers = ["等级", "股票", "方向", "可靠度", "动作", "仓位", "10日", "30日", "散户", "肥尾", "Kronos", "原因"]
    widths = [8, 10, 6, 6, 10, 18, 6, 12, 16, 4, 18, 56]
    table_width = sum(widths) + len(widths) - 1

    print("\n" + "═" * table_width)
    print(f"  A股机会扫描  {datetime.now():%Y-%m-%d %H:%M:%S}")
    print("═" * table_width)

    macro = get_cn_risk_warnings(days_ahead=14)
    if macro:
        print("宏观窗口: " + " | ".join(macro[:4]))
    print()

    print(" ".join(h.ljust(w) for h, w in zip(headers, widths)))
    print("-" * table_width)
    for row in rows:
        stock = f"{row.get('name', '-')}"
        position = f"{row.get('tier', '-')} / {row.get('qty', 0)}股 / ${row.get('risk_budget', 0):,.0f}" if row.get("tier") else "-"
        p10 = f"{row.get('p10', '-') or '-'}%"
        p30 = f"{row.get('p30', '-') or '-'}% {row.get('avg30', '-')}"
        retail = f"{row.get('retail_signal', '-')}/{row.get('retail_risk', '-')}"
        values = [
            row.get("level", "-"),
            stock,
            row.get("direction", "-"),
            row.get("reliability", "-"),
            row.get("action", "-"),
            position,
            p10,
            p30,
            retail,
            "⚡" * int(row.get("fat_tail") or 0) if row.get("fat_tail") else "-",
            row.get("kronos", "-"),
            row.get("reason", "-"),
        ]
        print(" ".join(str(v)[:w].ljust(w) for v, w in zip(values, widths)))

    actionable = [r for r in rows if r.get("level") == "明显机会"]
    watch = [r for r in rows if r.get("level") == "重点观察"]
    print("\n结论:")
    if actionable:
        print("  明显机会: " + ", ".join(f"{r['name']}({r['code']})" for r in actionable))
    else:
        print("  明显机会: 无")
    if watch:
        print("  重点观察: " + ", ".join(f"{r['name']}({r['code']})" for r in watch))
    else:
        print("  重点观察: 无")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scan A-share watchlist opportunities")
    parser.add_argument("--days", type=int, default=800, help="history days to fetch")
    parser.add_argument("--csv", help="optional CSV output path")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    rows = scan(days=args.days)
    print_report(rows)
    if args.csv:
        pd.DataFrame(rows).to_csv(args.csv, index=False)
        print(f"\n输出: {args.csv}")


if __name__ == "__main__":
    main()
