"""Production review scorecards.

面向操盘复盘，不替代研究回测。
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pandas as pd

from signal_hit_rate import (
    LOG_PATH as SIGNAL_LOG_PATH,
    evaluate_signal_history,
    parse_advisor_log,
    summarize_hit_rate,
)
from trade_plan import parse_plan_meta


ROOT = Path(__file__).parent
TRADE_LOG_PATH = ROOT / "trade_sim_log.csv"
OPTION_LOG_PATH = ROOT / "option_status.log"


def load_signal_history(evaluated: bool = False) -> pd.DataFrame:
    df = parse_advisor_log(SIGNAL_LOG_PATH)
    if df.empty:
        return df
    df = df.copy()
    df["signal_date"] = pd.to_datetime(df["timestamp"]).dt.date
    if evaluated:
        df = evaluate_signal_history(df)
    return df


def load_trade_log() -> pd.DataFrame:
    if not TRADE_LOG_PATH.exists():
        return pd.DataFrame()
    df = pd.read_csv(TRADE_LOG_PATH)
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    if "note" in df.columns:
        meta = df["note"].fillna("").apply(parse_plan_meta)
        meta_df = pd.DataFrame(list(meta))
        df = pd.concat([df, meta_df], axis=1)
    return df


def summarize_signals(signals: pd.DataFrame) -> pd.DataFrame:
    return summarize_hit_rate(signals, ["strategy", "regime"])


def summarize_execution(trades: pd.DataFrame) -> pd.DataFrame:
    if trades.empty:
        return pd.DataFrame()
    return (
        trades.groupby("action")
        .agg(
            count=("code", "size"),
            symbols=("code", lambda x: ",".join(sorted(set(map(str, x)))[:8])),
        )
        .reset_index()
        .sort_values("count", ascending=False)
    )


def summarize_execution_quality(trades: pd.DataFrame) -> pd.DataFrame:
    if trades.empty:
        return pd.DataFrame()
    has_plan = trades.get("has_plan", pd.Series(dtype=bool)).fillna(False)
    return pd.DataFrame(
        [
            {"metric": "total_actions", "value": len(trades)},
            {"metric": "actions_with_plan", "value": int(has_plan.sum())},
            {"metric": "actions_without_plan", "value": int((~has_plan).sum())},
            {"metric": "buy_actions", "value": int(trades["action"].astype(str).str.contains("BUY").sum()) if "action" in trades.columns else 0},
            {"metric": "sell_actions", "value": int(trades["action"].astype(str).str.contains("SELL").sum()) if "action" in trades.columns else 0},
        ]
    )


def summarize_plan_coverage(trades: pd.DataFrame) -> pd.DataFrame:
    if trades.empty or "has_plan" not in trades.columns:
        return pd.DataFrame()
    covered = trades[trades["has_plan"]].copy()
    if covered.empty:
        return pd.DataFrame()
    cols = [c for c in ["plan_tier", "plan_exit"] if c in covered.columns]
    return (
        covered.groupby(cols)
        .agg(
            actions=("action", "size"),
            codes=("code", lambda x: ",".join(sorted(set(map(str, x)))[:8])),
        )
        .reset_index()
        .sort_values("actions", ascending=False)
    )


def summarize_exit_distribution(trades: pd.DataFrame) -> pd.DataFrame:
    if trades.empty or "plan_exit" not in trades.columns:
        return pd.DataFrame()
    covered = trades[trades["plan_exit"].astype(str).str.len() > 0].copy()
    if covered.empty:
        return pd.DataFrame()
    return (
        covered.groupby("plan_exit")
        .agg(actions=("action", "size"))
        .reset_index()
        .sort_values("actions", ascending=False)
    )


def summarize_tier_distribution(signals: pd.DataFrame) -> pd.DataFrame:
    if signals.empty or "position_tier" not in signals.columns:
        return pd.DataFrame()
    actionable = signals[signals.get("is_actionable", False).fillna(False)].copy()
    if actionable.empty:
        return pd.DataFrame()
    return (
        actionable.groupby("position_tier")
        .agg(
            signals=("symbol", "size"),
            avg_risk=("risk_budget", "mean"),
        )
        .reset_index()
        .sort_values("signals", ascending=False)
    )


def summarize_signal_execution_gap(signals: pd.DataFrame, trades: pd.DataFrame) -> pd.DataFrame:
    if signals.empty:
        return pd.DataFrame()
    traded_ids = set(trades.get("signal_id", pd.Series(dtype=str)).dropna().astype(str))
    rows = []
    for _, row in signals.iterrows():
        if not bool(row.get("is_actionable", False)):
            continue
        rows.append(
            {
                "symbol": row["symbol"],
                "strategy": row["strategy"],
                "signal_id": row.get("signal_id", ""),
                "position_tier": row.get("position_tier", ""),
                "planned_qty": row.get("planned_qty", 0),
                "executed": str(row.get("signal_id", "")) in traded_ids if row.get("signal_id", "") else False,
            }
        )
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows).sort_values(["executed", "position_tier", "symbol"], ascending=[True, True, True])


def summarize_recent_win_rate(trades: pd.DataFrame, days: int = 30) -> float | None:
    if trades.empty or "timestamp" not in trades.columns:
        return None
    pnl_candidates = ["realized_pnl", "pnl", "profit", "net_pnl"]
    pnl_col = next((c for c in pnl_candidates if c in trades.columns), None)
    if not pnl_col:
        return None
    recent = trades[trades["timestamp"] >= pd.Timestamp.now() - pd.Timedelta(days=days)].copy()
    recent = recent[recent[pnl_col].notna()]
    if recent.empty:
        return None
    return float((recent[pnl_col].astype(float) > 0).mean() * 100)


def summarize_option_monitor_log() -> pd.DataFrame:
    if not OPTION_LOG_PATH.exists():
        return pd.DataFrame()
    text = OPTION_LOG_PATH.read_text(encoding="utf-8", errors="ignore")
    rows = []
    for keyword in ["TAKE_PROFIT", "STOP_LOSS", "CLOSE_URGENT", "WATCH_CLOSE", "HOLD_CAUTION"]:
        rows.append({"action": keyword, "mentions": text.count(keyword)})
    return pd.DataFrame(rows)


def run() -> None:
    signals = load_signal_history(evaluated=True)
    trades = load_trade_log()
    signal_scorecard = summarize_signals(signals)
    execution_scorecard = summarize_execution(trades)
    execution_quality = summarize_execution_quality(trades)
    tier_distribution = summarize_tier_distribution(signals)
    plan_coverage = summarize_plan_coverage(trades)
    exit_distribution = summarize_exit_distribution(trades)
    gap_scorecard = summarize_signal_execution_gap(signals, trades)
    monitor_scorecard = summarize_option_monitor_log()

    ts = datetime.now().strftime("%Y%m%d_%H%M")
    outputs = {
        "production_signal_scorecard.csv": signal_scorecard,
        "production_execution_scorecard.csv": execution_scorecard,
        "production_execution_quality.csv": execution_quality,
        "production_tier_distribution.csv": tier_distribution,
        "production_plan_coverage.csv": plan_coverage,
        "production_exit_distribution.csv": exit_distribution,
        f"production_signal_gap_{ts}.csv": gap_scorecard,
    }
    for name, df in outputs.items():
        if not df.empty:
            df.to_csv(ROOT / name, index=False)

    print("\n══ 生产复盘摘要 ══")
    print(f"信号日志: {len(signals)} 条" if not signals.empty else "信号日志: 无可解析数据")
    print(f"交易日志: {len(trades)} 条" if not trades.empty else "交易日志: 无可解析数据")

    for title, df in [
        ("信号评分卡", signal_scorecard),
        ("执行评分卡", execution_scorecard),
        ("执行质量", execution_quality),
        ("仓位层级", tier_distribution),
        ("计划覆盖", plan_coverage),
        ("退出模板分布", exit_distribution),
        ("信号-执行缺口", gap_scorecard.head(20) if not gap_scorecard.empty else gap_scorecard),
        ("监控动作提及", monitor_scorecard),
    ]:
        if df.empty:
            continue
        print(f"\n[{title}]")
        print(df.to_string(index=False))

    recent_7 = summarize_recent_win_rate(trades, days=7)
    recent_30 = summarize_recent_win_rate(trades, days=30)
    if recent_7 is not None or recent_30 is not None:
        print("\n[近期执行胜率]")
        print(f"7日: {'N/A' if recent_7 is None else f'{recent_7:.1f}%'}")
        print(f"30日: {'N/A' if recent_30 is None else f'{recent_30:.1f}%'}")


if __name__ == "__main__":
    run()
