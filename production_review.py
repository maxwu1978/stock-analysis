"""Production review scorecards.

面向操盘复盘，不替代研究回测。
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
import re

import pandas as pd

from signal_hit_rate import LOG_PATH as SIGNAL_LOG_PATH, parse_advisor_log


ROOT = Path(__file__).parent
TRADE_LOG_PATH = ROOT / "trade_sim_log.csv"
OPTION_LOG_PATH = ROOT / "option_status.log"


def load_signal_history() -> pd.DataFrame:
    df = parse_advisor_log(SIGNAL_LOG_PATH)
    if df.empty:
        return df
    df = df.copy()
    df["signal_date"] = pd.to_datetime(df["timestamp"]).dt.date
    return df


def load_trade_log() -> pd.DataFrame:
    if not TRADE_LOG_PATH.exists():
        return pd.DataFrame()
    df = pd.read_csv(TRADE_LOG_PATH)
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    if "note" in df.columns:
        meta = df["note"].fillna("").apply(parse_note_metadata)
        meta_df = pd.DataFrame(list(meta))
        df = pd.concat([df, meta_df], axis=1)
    return df


def parse_note_metadata(note: str) -> dict:
    note = note or ""
    out = {
        "signal_id": "",
        "plan_tier": "",
        "plan_risk": "",
        "plan_exit": "",
        "plan_note": "",
        "has_plan": False,
    }
    for key in ["signal_id", "plan_tier", "plan_risk", "plan_exit", "plan_note"]:
        m = re.search(rf"{key}=([^|]+)", note)
        if m:
            out[key] = m.group(1).strip()
    out["has_plan"] = any(out[k] for k in ["signal_id", "plan_tier", "plan_risk", "plan_exit", "plan_note"])
    return out


def summarize_signals(signals: pd.DataFrame) -> pd.DataFrame:
    if signals.empty:
        return pd.DataFrame()
    out = (
        signals.groupby(["strategy", "regime"])
        .agg(
            signals=("symbol", "size"),
            names=("symbol", lambda x: ",".join(sorted(set(x)))),
            avg_hold_days=("hold_days", "mean"),
        )
        .reset_index()
        .sort_values(["signals", "strategy"], ascending=[False, True])
    )
    return out


def summarize_execution(trades: pd.DataFrame) -> pd.DataFrame:
    if trades.empty:
        return pd.DataFrame()
    actions = (
        trades.groupby("action")
        .agg(
            count=("code", "size"),
            symbols=("code", lambda x: ",".join(sorted(set(map(str, x)))[:8])),
        )
        .reset_index()
        .sort_values("count", ascending=False)
    )
    return actions


def summarize_execution_quality(trades: pd.DataFrame) -> pd.DataFrame:
    if trades.empty:
        return pd.DataFrame()
    out = pd.DataFrame(
        [
            {"metric": "total_actions", "value": len(trades)},
            {"metric": "actions_with_plan", "value": int(trades.get("has_plan", pd.Series(dtype=bool)).fillna(False).sum())},
            {"metric": "actions_without_plan", "value": int((~trades.get("has_plan", pd.Series(dtype=bool)).fillna(False)).sum()) if "has_plan" in trades.columns else len(trades)},
            {"metric": "buy_actions", "value": int(trades["action"].astype(str).str.contains("BUY").sum()) if "action" in trades.columns else 0},
            {"metric": "sell_actions", "value": int(trades["action"].astype(str).str.contains("SELL").sum()) if "action" in trades.columns else 0},
        ]
    )
    return out


def summarize_plan_coverage(trades: pd.DataFrame) -> pd.DataFrame:
    if trades.empty or "has_plan" not in trades.columns:
        return pd.DataFrame()
    cols = [c for c in ["plan_tier", "plan_exit"] if c in trades.columns]
    if not cols:
        return pd.DataFrame()
    covered = trades[trades["has_plan"]].copy()
    if covered.empty:
        return pd.DataFrame()
    out = (
        covered.groupby(cols)
        .agg(
            actions=("action", "size"),
            codes=("code", lambda x: ",".join(sorted(set(map(str, x)))[:8])),
        )
        .reset_index()
        .sort_values("actions", ascending=False)
    )
    return out


def summarize_signal_execution_gap(signals: pd.DataFrame, trades: pd.DataFrame) -> pd.DataFrame:
    if signals.empty:
        return pd.DataFrame()
    signal_symbols = sorted(set(signals["symbol"].astype(str)))
    traded_symbols = set()
    if not trades.empty and "code" in trades.columns:
        for code in trades["code"].astype(str):
            if code.startswith("US.") and len(code.split(".")) == 2:
                traded_symbols.add(code.split(".")[1][:10])
    rows = []
    for sym in signal_symbols:
        rows.append({
            "symbol": sym,
            "signal_count": int((signals["symbol"] == sym).sum()),
            "has_trade_log": sym in traded_symbols,
        })
    return pd.DataFrame(rows).sort_values(["has_trade_log", "signal_count"], ascending=[True, False])


def summarize_option_monitor_log() -> pd.DataFrame:
    if not OPTION_LOG_PATH.exists():
        return pd.DataFrame()
    text = OPTION_LOG_PATH.read_text(encoding="utf-8", errors="ignore")
    rows = []
    for keyword in ["TAKE_PROFIT", "STOP_LOSS", "CLOSE_URGENT", "WATCH_CLOSE", "HOLD_CAUTION"]:
        rows.append({"action": keyword, "mentions": text.count(keyword)})
    return pd.DataFrame(rows)


def run() -> None:
    signals = load_signal_history()
    trades = load_trade_log()
    signal_scorecard = summarize_signals(signals)
    execution_scorecard = summarize_execution(trades)
    execution_quality = summarize_execution_quality(trades)
    plan_coverage = summarize_plan_coverage(trades)
    gap_scorecard = summarize_signal_execution_gap(signals, trades)
    monitor_scorecard = summarize_option_monitor_log()

    ts = datetime.now().strftime("%Y%m%d_%H%M")
    signal_out = ROOT / "production_signal_scorecard.csv"
    execution_out = ROOT / "production_execution_scorecard.csv"
    execution_quality_out = ROOT / "production_execution_quality.csv"
    plan_coverage_out = ROOT / "production_plan_coverage.csv"
    gap_out = ROOT / f"production_signal_gap_{ts}.csv"

    if not signal_scorecard.empty:
        signal_scorecard.to_csv(signal_out, index=False)
    if not execution_scorecard.empty:
        execution_scorecard.to_csv(execution_out, index=False)
    if not execution_quality.empty:
        execution_quality.to_csv(execution_quality_out, index=False)
    if not plan_coverage.empty:
        plan_coverage.to_csv(plan_coverage_out, index=False)
    if not gap_scorecard.empty:
        gap_scorecard.to_csv(gap_out, index=False)

    print("\n══ 生产复盘摘要 ══")
    print(f"信号日志: {len(signals)} 条" if not signals.empty else "信号日志: 无可解析数据")
    print(f"交易日志: {len(trades)} 条" if not trades.empty else "交易日志: 无可解析数据")

    if not signal_scorecard.empty:
        print("\n[信号评分卡]")
        print(signal_scorecard.to_string(index=False))
        print(f"\n已写出: {signal_out.name}")

    if not execution_scorecard.empty:
        print("\n[执行评分卡]")
        print(execution_scorecard.to_string(index=False))
        print(f"\n已写出: {execution_out.name}")

    if not execution_quality.empty:
        print("\n[执行质量]")
        print(execution_quality.to_string(index=False))
        print(f"\n已写出: {execution_quality_out.name}")

    if not plan_coverage.empty:
        print("\n[计划覆盖]")
        print(plan_coverage.to_string(index=False))
        print(f"\n已写出: {plan_coverage_out.name}")

    if not gap_scorecard.empty:
        print("\n[信号-执行缺口]")
        print(gap_scorecard.head(20).to_string(index=False))
        print(f"\n已写出: {gap_out.name}")

    if not monitor_scorecard.empty:
        print("\n[监控动作提及]")
        print(monitor_scorecard.to_string(index=False))


if __name__ == "__main__":
    run()
