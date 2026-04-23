"""Shared trading-plan schema and helpers."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import re


PLAN_META_KEYS = ["signal_id", "plan_tier", "plan_risk", "plan_exit", "plan_note"]


@dataclass
class TradePlanMeta:
    signal_id: str = ""
    plan_tier: str = ""
    plan_risk: str = ""
    plan_exit: str = ""
    plan_note: str = ""

    def as_dict(self) -> dict[str, str]:
        return {
            "signal_id": self.signal_id,
            "plan_tier": self.plan_tier,
            "plan_risk": self.plan_risk,
            "plan_exit": self.plan_exit,
            "plan_note": self.plan_note,
        }

    def to_flags(self) -> str:
        parts: list[str] = []
        for key in PLAN_META_KEYS:
            value = (getattr(self, key) or "").strip()
            if value:
                parts.append(f"--{key.replace('_', '-')} {value}")
        return " ".join(parts)


def safe_plan_value(value: str) -> str:
    return re.sub(r"[|\n\r]+", "/", str(value or "").strip())


def serialize_plan_meta(plan_meta: dict[str, str] | TradePlanMeta | None) -> str:
    if not plan_meta:
        return ""
    meta = plan_meta.as_dict() if isinstance(plan_meta, TradePlanMeta) else plan_meta
    parts = []
    for key in PLAN_META_KEYS:
        value = safe_plan_value(meta.get(key, ""))
        if value:
            parts.append(f"{key}={value}")
    return " | ".join(parts)


def parse_plan_meta(note: str) -> dict[str, str]:
    note = note or ""
    out = {key: "" for key in PLAN_META_KEYS}
    for key in PLAN_META_KEYS:
        m = re.search(rf"{key}=([^|]+)", note)
        if m:
            out[key] = m.group(1).strip()
    out["has_plan"] = any(out.values())
    return out


def extract_plan_metadata_from_args(args: list[str]) -> dict[str, str]:
    def get_opt_arg(flag: str, default: str = "") -> str:
        if flag in args:
            idx = args.index(flag)
            if idx + 1 < len(args):
                return args[idx + 1]
        return default

    return {
        "signal_id": get_opt_arg("--signal-id"),
        "plan_tier": get_opt_arg("--plan-tier"),
        "plan_risk": get_opt_arg("--plan-risk"),
        "plan_exit": get_opt_arg("--plan-exit"),
        "plan_note": get_opt_arg("--plan-note"),
    }


def compose_note(base_note: str = "", plan_meta: dict[str, str] | TradePlanMeta | None = None) -> str:
    parts = [base_note] if base_note else []
    serialized = serialize_plan_meta(plan_meta)
    if serialized:
        parts.append(serialized)
    return " | ".join(parts)


def build_signal_id(symbol: str, signal: str, now: datetime | None = None) -> str:
    dt = now or datetime.now()
    clean_symbol = symbol.replace("US.", "").replace("SZ.", "").replace("SH.", "").replace(".", "_")
    clean_signal = re.sub(r"[^A-Z0-9_]+", "_", signal.upper())
    return f"{clean_symbol}_{clean_signal}_{dt.strftime('%Y%m%d')}"


def build_trade_plan_meta(
    symbol: str,
    signal: str,
    plan_tier: str = "",
    plan_risk: float | int | str = "",
    plan_exit: str = "",
    plan_note: str = "",
    now: datetime | None = None,
) -> TradePlanMeta:
    risk_str = ""
    if isinstance(plan_risk, (int, float)):
        risk_str = f"{plan_risk:.0f}"
    else:
        risk_str = str(plan_risk or "")
    return TradePlanMeta(
        signal_id=build_signal_id(symbol, signal, now=now),
        plan_tier=str(plan_tier or ""),
        plan_risk=risk_str,
        plan_exit=str(plan_exit or ""),
        plan_note=str(plan_note or ""),
    )
