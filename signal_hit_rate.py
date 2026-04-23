"""生产信号命中率统计与复盘底座.

统一负责:
  1. 解析 advisor_history.log 的生产信号
  2. 评估理论信号后续表现
  3. 输出可复用的命中率摘要
"""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
import re
import sys

import pandas as pd


ROOT = Path(__file__).parent
LOG_PATH = ROOT / "advisor_history.log"

ACTIONABLE_PREFIXES = ("BUY_",)

RUN_SPLIT_RE = re.compile(
    r"═{70,}\s*\n\s*运行时间:\s*([^\n]+)\s*\n═{70,}",
    re.MULTILINE,
)

SIGNAL_RE = re.compile(
    r"(?ms)^  (?P<code>US\.(?P<symbol>\w+))\s+现价=\$(?P<spot>\d+\.\d+)\s+今日\s+(?P<chg>[+-]?\d+\.\d+)%\s*\n"
    r"\s+分形:\s+asym=(?P<asym>[+-]?\d+\.\d+)\s+h\(q=2\)=(?P<hq2>[+-]?\d+\.\d+)\s+Δα=(?P<delta_alpha>[+-]?\d+\.\d+)\s*\n"
    r"\s+技术:\s+RSI6=(?P<rsi6>\d+\.\d+)\s+MA20偏离=(?P<ma20_diff>[+-]?\d+\.\d+)%\s+年化σ=(?P<vol_20d_ann>\d+\.\d+)%\s*\n"
    r"\s+情境:\s+(?P<regime>\S+)\s*\n"
    r"\s+建议:\s+(?P<strategy>\S+)\s+到期天数目标=(?P<hold_days>\d+)(?:\s+置信度:\s+(?P<confidence>\S+))?"
    r"(?P<body>.*?)(?=^─{20,}|^  US\.|\Z)"
)

POSITION_RE = re.compile(
    r"仓位:\s*(?P<position_tier>\S+)\s*·\s*建议\s*(?P<planned_qty>\d+)\s*(?P<qty_unit>[张套股])\s*·\s*"
    r"风险预算\s*\$(?P<risk_budget>[\d,]+)\s*·\s*名义资金\s*\$(?P<notional_value>[\d,]+)"
)
EXIT_RE = re.compile(r"退出:\s*(?P<exit_plan>[^\n]+)")
NOTE_RE = re.compile(r"说明:\s*(?P<sizing_note>[^\n]+)")
CMD_RE = re.compile(r"trade_futu_sim\.py\s+(?P<cmd>\w+)\s+(?P<contract>\S+)\s+(?P<qty>\d+)(?P<flags>[^\n#]*)")
FLAG_RE = re.compile(r"--(?P<key>signal-id|plan-tier|plan-risk|plan-exit|plan-note)\s+(?P<value>\S+)")


def _empty_signal_frame() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "timestamp",
            "signal_date",
            "code",
            "symbol",
            "spot_at_signal",
            "chg_pct",
            "asym",
            "hq2",
            "delta_alpha",
            "rsi6",
            "ma20_diff",
            "vol_20d_ann",
            "regime",
            "strategy",
            "hold_days",
            "confidence",
            "position_tier",
            "planned_qty",
            "qty_unit",
            "risk_budget",
            "notional_value",
            "exit_plan",
            "sizing_note",
            "signal_id",
            "command_count",
            "command_qty_total",
            "command_codes",
            "legs_per_signal",
            "theoretical_order_qty_total",
            "is_actionable",
        ]
    )


def _parse_run_timestamp(raw: str) -> pd.Timestamp | None:
    cleaned = (
        raw.replace(" CEST", "")
        .replace(" CET", "")
        .replace(" EDT", "")
        .replace(" EST", "")
        .strip()
    )
    ts = pd.to_datetime(cleaned, errors="coerce")
    return None if pd.isna(ts) else ts


def _parse_float(text: str | None) -> float | None:
    if text is None or text == "":
        return None
    return float(text.replace(",", ""))


def _parse_int(text: str | None) -> int | None:
    if text is None or text == "":
        return None
    return int(float(text.replace(",", "")))


def _extract_command_meta(flags_text: str) -> dict[str, str]:
    meta = {
        "signal_id": "",
        "plan_tier": "",
        "plan_risk": "",
        "plan_exit": "",
        "plan_note": "",
    }
    for match in FLAG_RE.finditer(flags_text or ""):
        key = match.group("key").replace("-", "_")
        meta[key] = match.group("value").strip()
    return meta


def _strategy_is_actionable(strategy: str | None) -> bool:
    text = str(strategy or "")
    return any(text.startswith(prefix) for prefix in ACTIONABLE_PREFIXES)


def parse_advisor_log(path: Path) -> pd.DataFrame:
    """解析 advisor_history.log, 抽取生产信号与仓位/退出计划字段."""
    if not path.exists():
        return _empty_signal_frame()

    text = path.read_text(encoding="utf-8", errors="ignore")
    blocks = RUN_SPLIT_RE.split(text)
    rows: list[dict] = []

    for i in range(1, len(blocks), 2):
        ts = _parse_run_timestamp(blocks[i])
        if ts is None:
            continue
        content = blocks[i + 1] if i + 1 < len(blocks) else ""

        for match in SIGNAL_RE.finditer(content):
            body = match.group("body") or ""
            pos_match = POSITION_RE.search(body)
            exit_match = EXIT_RE.search(body)
            note_match = NOTE_RE.search(body)

            commands = list(CMD_RE.finditer(body))
            command_codes = []
            command_qty_total = 0
            signal_id = ""
            plan_tier = ""
            plan_risk = ""
            plan_exit = ""
            plan_note = ""
            for cmd in commands:
                command_codes.append(cmd.group("contract"))
                command_qty_total += int(cmd.group("qty"))
                cmd_meta = _extract_command_meta(cmd.group("flags"))
                signal_id = signal_id or cmd_meta.get("signal_id", "")
                plan_tier = plan_tier or cmd_meta.get("plan_tier", "")
                plan_risk = plan_risk or cmd_meta.get("plan_risk", "")
                plan_exit = plan_exit or cmd_meta.get("plan_exit", "")
                plan_note = plan_note or cmd_meta.get("plan_note", "")

            planned_qty = _parse_int(pos_match.group("planned_qty")) if pos_match else None
            qty_unit = pos_match.group("qty_unit") if pos_match else ""
            command_count = len(commands)
            legs_per_signal = command_count if command_count > 0 else 1
            theoretical_order_qty_total = (planned_qty or 0) * legs_per_signal if planned_qty else 0

            row = {
                "timestamp": ts,
                "signal_date": ts.date(),
                "code": match.group("code"),
                "symbol": match.group("symbol"),
                "spot_at_signal": float(match.group("spot")),
                "chg_pct": float(match.group("chg")),
                "asym": float(match.group("asym")),
                "hq2": float(match.group("hq2")),
                "delta_alpha": float(match.group("delta_alpha")),
                "rsi6": float(match.group("rsi6")),
                "ma20_diff": float(match.group("ma20_diff")),
                "vol_20d_ann": float(match.group("vol_20d_ann")),
                "regime": match.group("regime"),
                "strategy": match.group("strategy"),
                "hold_days": int(match.group("hold_days")),
                "confidence": match.group("confidence") or "",
                "position_tier": (pos_match.group("position_tier") if pos_match else "") or plan_tier,
                "planned_qty": planned_qty,
                "qty_unit": qty_unit,
                "risk_budget": _parse_float(pos_match.group("risk_budget")) if pos_match else _parse_float(plan_risk),
                "notional_value": _parse_float(pos_match.group("notional_value")) if pos_match else None,
                "exit_plan": (exit_match.group("exit_plan") if exit_match else "") or plan_exit,
                "sizing_note": (note_match.group("sizing_note") if note_match else "") or plan_note,
                "signal_id": signal_id,
                "command_count": command_count,
                "command_qty_total": command_qty_total,
                "command_codes": ",".join(command_codes),
                "legs_per_signal": legs_per_signal,
                "theoretical_order_qty_total": theoretical_order_qty_total,
            }
            row["is_actionable"] = _strategy_is_actionable(row["strategy"])
            rows.append(row)

    if not rows:
        return _empty_signal_frame()

    df = pd.DataFrame(rows)
    for col in ["planned_qty", "command_count", "command_qty_total", "legs_per_signal", "theoretical_order_qty_total", "hold_days"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    for col in ["risk_budget", "notional_value", "spot_at_signal"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df.sort_values(["timestamp", "symbol", "strategy"]).reset_index(drop=True)


def check_outcome(row: pd.Series) -> dict:
    """评估理论信号在持有期后的表现."""
    import yfinance as yf

    if not bool(row.get("is_actionable", False)):
        return {"completed": False, "skip_reason": "not_actionable"}

    symbol = row["symbol"]
    signal_date = pd.Timestamp(row["timestamp"])
    hold = int(row["hold_days"])
    end_date = signal_date + timedelta(days=hold + 3)
    start_date = signal_date - timedelta(days=2)

    try:
        ticker = yf.Ticker(symbol)
        hist = ticker.history(
            start=start_date.strftime("%Y-%m-%d"),
            end=end_date.strftime("%Y-%m-%d"),
            interval="1d",
        )
        if hist.index.tz is not None:
            hist.index = hist.index.tz_localize(None)
        if hist.empty:
            return {"completed": False, "skip_reason": "no_price_history"}

        closes = hist.sort_index()["Close"].astype(float)
        signal_closes = closes[closes.index >= signal_date.normalize()]
        if signal_closes.empty:
            return {"completed": False, "skip_reason": "no_signal_close"}
        close_at_signal = float(signal_closes.iloc[0])

        target_date = signal_date + timedelta(days=hold)
        fwd_closes = closes[closes.index >= target_date.normalize()]
        if fwd_closes.empty:
            return {"completed": False, "skip_reason": "hold_not_finished"}
        close_at_end = float(fwd_closes.iloc[0])

        fwd_ret_pct = (close_at_end / close_at_signal - 1) * 100
        strategy = str(row["strategy"])
        if strategy == "BUY_PUT":
            correct = fwd_ret_pct < 0
        elif strategy == "BUY_CALL":
            correct = fwd_ret_pct > 0
        elif strategy == "BUY_STRADDLE":
            correct = abs(fwd_ret_pct) > 7
        else:
            correct = pd.NA

        return {
            "completed": True,
            "skip_reason": "",
            "close_at_signal": close_at_signal,
            "close_at_end": close_at_end,
            "fwd_ret_pct": fwd_ret_pct,
            "direction_correct": correct,
            "abs_move_pct": abs(fwd_ret_pct),
        }
    except Exception as exc:
        return {"completed": False, "skip_reason": str(exc)[:80]}


def evaluate_signal_history(signals: pd.DataFrame) -> pd.DataFrame:
    """为信号表补齐后续表现字段."""
    if signals.empty:
        return signals.copy()
    outcomes = [check_outcome(row) for _, row in signals.iterrows()]
    out = pd.concat([signals.reset_index(drop=True), pd.DataFrame(outcomes)], axis=1)
    return out


def summarize_hit_rate(df: pd.DataFrame, group_cols: list[str]) -> pd.DataFrame:
    """统一口径的命中率汇总."""
    if df.empty:
        return pd.DataFrame()

    actionable = df[df.get("is_actionable", False).fillna(False)].copy()
    if actionable.empty:
        return pd.DataFrame()
    completed = actionable[actionable.get("completed", False).fillna(False)].copy()
    if completed.empty:
        return pd.DataFrame()

    out = (
        completed.groupby(group_cols, dropna=False)
        .agg(
            signals=("symbol", "size"),
            hit_rate=("direction_correct", lambda x: float(pd.Series(x).astype("boolean").mean() * 100)),
            mean_ret=("fwd_ret_pct", "mean"),
            median_ret=("fwd_ret_pct", "median"),
            avg_hold_days=("hold_days", "mean"),
        )
        .reset_index()
        .sort_values(["signals"] + group_cols, ascending=[False] + [True] * len(group_cols))
    )
    return out


def report(df: pd.DataFrame) -> None:
    if df.empty:
        print("无信号历史.")
        return

    actionable = df[df.get("is_actionable", False).fillna(False)].copy()
    completed = actionable[actionable.get("completed", False).fillna(False)].copy()

    print(f"\n{'═' * 88}")
    print(f"  生产信号命中率报告 (解析信号 n={len(df)})")
    print("═" * 88)
    print(f"\n  可执行信号: {len(actionable)} / {len(df)}")

    if completed.empty:
        print("  已完成持有期信号: 0")
        print("  说明: 当前没有到期后的可评估样本")
        return

    print(f"  已完成持有期信号: {len(completed)} / {len(actionable)}")
    wins = int(completed["direction_correct"].astype("boolean").sum())
    total = len(completed)
    print(f"  整体命中率: {wins}/{total} = {wins / total * 100:.1f}%")
    print(f"  平均后续收益: {completed['fwd_ret_pct'].mean():+.2f}%")

    sections = [
        ("按策略", ["strategy"]),
        ("按情境", ["strategy", "regime"]),
        ("按股票", ["symbol", "strategy"]),
    ]
    for title, cols in sections:
        summary = summarize_hit_rate(completed, cols)
        if summary.empty:
            continue
        print(f"\n  [{title}]")
        print(summary.to_string(index=False, float_format=lambda x: f"{x:+.2f}"))


def run() -> None:
    print(f"[1/3] 解析 {LOG_PATH.name} ...")
    signals = parse_advisor_log(LOG_PATH)
    if signals.empty:
        print("  日志为空或解析失败. 运行 ./run_advisor_daily.sh 累积数据")
        return
    print(f"  抽出 {len(signals)} 条信号, 可执行 {int(signals['is_actionable'].sum())} 条")

    print("\n[2/3] 评估后续走势 (yfinance) ...")
    reviewed = evaluate_signal_history(signals)

    print("[3/3] 汇总报告")
    report(reviewed)

    out = ROOT / f"signal_hit_rate_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
    reviewed.to_csv(out, index=False)
    print(f"\n  详细数据: {out.name}\n")


if __name__ == "__main__":
    run()
