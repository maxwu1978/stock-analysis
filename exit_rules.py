"""Unified exit templates for options and overlays.

第一阶段只输出计划，不直接触发下单。
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Literal


ExitType = Literal["option_long", "option_straddle"]


@dataclass
class ExitPlan:
    exit_type: ExitType
    take_profit_partial: float
    take_profit_full: float
    soft_stop: float
    hard_stop: float
    time_stop_days: int
    review_days: int
    notes: str

    def to_dict(self) -> dict:
        return asdict(self)


def build_long_option_exit(
    *,
    premium: float,
    days_to_expiry: int,
    confidence: str | None = None,
) -> ExitPlan:
    confidence = confidence or "MEDIUM"
    if confidence == "HIGH":
        tp_partial_mult = 1.50
        tp_full_mult = 2.10
        soft_stop_mult = 0.65
        hard_stop_mult = 0.45
    elif confidence == "LOW":
        tp_partial_mult = 1.35
        tp_full_mult = 1.80
        soft_stop_mult = 0.72
        hard_stop_mult = 0.55
    else:
        tp_partial_mult = 1.40
        tp_full_mult = 1.90
        soft_stop_mult = 0.68
        hard_stop_mult = 0.50

    return ExitPlan(
        exit_type="option_long",
        take_profit_partial=round(premium * tp_partial_mult, 2),
        take_profit_full=round(premium * tp_full_mult, 2),
        soft_stop=round(premium * soft_stop_mult, 2),
        hard_stop=round(premium * hard_stop_mult, 2),
        time_stop_days=min(5, max(2, days_to_expiry // 2)),
        review_days=min(7, max(3, days_to_expiry)),
        notes="单腿期权优先执行 50% 附近锁利，亏损扩大到硬止损则离场；临近到期加快复核。",
    )


def build_straddle_exit(
    *,
    total_premium: float,
    days_to_expiry: int,
    confidence: str | None = None,
) -> ExitPlan:
    confidence = confidence or "LOW"
    if confidence == "MEDIUM":
        tp_partial_mult = 1.30
        tp_full_mult = 1.55
    else:
        tp_partial_mult = 1.25
        tp_full_mult = 1.45

    return ExitPlan(
        exit_type="option_straddle",
        take_profit_partial=round(total_premium * tp_partial_mult, 2),
        take_profit_full=round(total_premium * tp_full_mult, 2),
        soft_stop=round(total_premium * 0.70, 2),
        hard_stop=round(total_premium * 0.50, 2),
        time_stop_days=min(7, max(3, days_to_expiry // 3)),
        review_days=min(10, max(5, days_to_expiry)),
        notes="跨式优先看事件前后波动兑现；若剩余天数快速下降且未放量突破，按时间止损处理。",
    )


def format_exit_plan(plan: ExitPlan) -> str:
    return (
        f"止盈一 ${plan.take_profit_partial:.2f} / 止盈二 ${plan.take_profit_full:.2f} / "
        f"软止损 ${plan.soft_stop:.2f} / 硬止损 ${plan.hard_stop:.2f} / "
        f"{plan.time_stop_days}天时间止损"
    )
