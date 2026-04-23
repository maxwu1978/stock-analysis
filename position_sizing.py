"""Position sizing rules for discretionary signal outputs.

第一阶段目标：
1. 让可靠度/置信度/宏观惩罚进入仓位决策
2. 返回统一结构，供顾问脚本、复盘脚本、后续执行层复用
3. 不直接下单，只给出风险预算和建议张数/股数
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Literal


InstrumentType = Literal["equity", "option_long", "option_straddle"]


@dataclass
class PositionPlan:
    instrument_type: InstrumentType
    allowed: bool
    position_tier: str
    qty: int
    account_equity: float
    risk_budget: float
    capital_at_risk: float
    entry_price: float
    notional_value: float
    sizing_note: str

    def to_dict(self) -> dict:
        return asdict(self)


def _reliability_mult(label: str | None) -> float:
    return {
        "强": 1.00,
        "中": 0.60,
        "弱": 0.00,
    }.get(label or "", 0.35)


def _confidence_mult(label: str | None) -> float:
    return {
        "HIGH": 1.00,
        "MEDIUM": 0.75,
        "LOW": 0.45,
    }.get(label or "", 0.60)


def _macro_mult(penalty: int | float | None) -> float:
    penalty = float(penalty or 0)
    if penalty >= 12:
        return 0.35
    if penalty >= 8:
        return 0.50
    if penalty >= 4:
        return 0.75
    return 1.00


def _score_mult(score: int | float | None) -> float:
    score = abs(float(score or 0))
    if score >= 60:
        return 1.00
    if score >= 35:
        return 0.80
    if score >= 15:
        return 0.60
    return 0.40


def _tier_from_risk_ratio(risk_ratio: float, allowed: bool) -> str:
    if not allowed or risk_ratio <= 0:
        return "NO_TRADE"
    if risk_ratio >= 0.007:
        return "STANDARD"
    if risk_ratio >= 0.003:
        return "PROBE"
    return "MICRO"


def recommend_equity_position(
    *,
    entry_price: float,
    account_equity: float = 1_000_000,
    score: int | float | None = None,
    reliability: str | None = None,
    macro_penalty: int | float | None = None,
    hard_stop_pct: float = 0.08,
) -> PositionPlan:
    """股票建议仓位。

    风险预算按账户净值比例给出，再换算为股数。
    """
    base_risk = account_equity * 0.0075
    risk_ratio = 0.0075 * _reliability_mult(reliability) * _score_mult(score) * _macro_mult(macro_penalty)
    risk_budget = account_equity * risk_ratio
    allowed = risk_budget > 0 and entry_price > 0 and hard_stop_pct > 0
    if not allowed:
        return PositionPlan(
            instrument_type="equity",
            allowed=False,
            position_tier="NO_TRADE",
            qty=0,
            account_equity=account_equity,
            risk_budget=0.0,
            capital_at_risk=0.0,
            entry_price=entry_price,
            notional_value=0.0,
            sizing_note="可靠度或风险折扣不足，默认不建议主动开仓。",
        )

    risk_per_share = entry_price * hard_stop_pct
    qty = max(1, int(risk_budget / risk_per_share))
    notional = qty * entry_price
    return PositionPlan(
        instrument_type="equity",
        allowed=True,
        position_tier=_tier_from_risk_ratio(risk_ratio, True),
        qty=qty,
        account_equity=account_equity,
        risk_budget=round(risk_budget, 2),
        capital_at_risk=round(qty * risk_per_share, 2),
        entry_price=entry_price,
        notional_value=round(notional, 2),
        sizing_note=f"基于约 {hard_stop_pct:.0%} 硬止损估算，预算约占净值 {risk_ratio:.2%}。",
    )


def recommend_long_option_position(
    *,
    premium: float,
    account_equity: float = 1_000_000,
    reliability: str | None = None,
    confidence: str | None = None,
    macro_penalty: int | float | None = None,
    contract_size: int = 100,
) -> PositionPlan:
    """单腿期权建议张数。

    默认把“最多接受整张权利金显著亏损”视为风险预算口径。
    """
    risk_ratio = 0.0030 * _reliability_mult(reliability) * _confidence_mult(confidence) * _macro_mult(macro_penalty)
    risk_budget = account_equity * risk_ratio
    contract_risk = premium * contract_size * 0.60
    allowed = risk_budget > 0 and premium > 0 and contract_risk > 0
    if not allowed:
        return PositionPlan(
            instrument_type="option_long",
            allowed=False,
            position_tier="NO_TRADE",
            qty=0,
            account_equity=account_equity,
            risk_budget=0.0,
            capital_at_risk=0.0,
            entry_price=premium,
            notional_value=0.0,
            sizing_note="弱信号或宏观收缩后预算过低，默认不建议开单腿期权。",
        )

    qty = max(1, int(risk_budget / contract_risk))
    qty = min(qty, 3)
    notional = premium * contract_size * qty
    return PositionPlan(
        instrument_type="option_long",
        allowed=True,
        position_tier=_tier_from_risk_ratio(risk_ratio, True),
        qty=qty,
        account_equity=account_equity,
        risk_budget=round(risk_budget, 2),
        capital_at_risk=round(contract_risk * qty, 2),
        entry_price=premium,
        notional_value=round(notional, 2),
        sizing_note=f"按约 60% 权利金风险估算；预算约占净值 {risk_ratio:.2%}。",
    )


def recommend_straddle_position(
    *,
    total_premium: float,
    account_equity: float = 1_000_000,
    confidence: str | None = None,
    macro_penalty: int | float | None = None,
    contract_size: int = 100,
) -> PositionPlan:
    """跨式建议张数。

    跨式默认风险预算更低，因为 theta 与事件风险更高。
    """
    risk_ratio = 0.0020 * _confidence_mult(confidence) * _macro_mult(macro_penalty)
    risk_budget = account_equity * risk_ratio
    contract_risk = total_premium * contract_size * 0.65
    allowed = risk_budget > 0 and total_premium > 0 and contract_risk > 0
    if not allowed:
        return PositionPlan(
            instrument_type="option_straddle",
            allowed=False,
            position_tier="NO_TRADE",
            qty=0,
            account_equity=account_equity,
            risk_budget=0.0,
            capital_at_risk=0.0,
            entry_price=total_premium,
            notional_value=0.0,
            sizing_note="跨式风险预算不足，默认只观察不下单。",
        )

    qty = max(1, int(risk_budget / contract_risk))
    qty = min(qty, 2)
    notional = total_premium * contract_size * qty
    return PositionPlan(
        instrument_type="option_straddle",
        allowed=True,
        position_tier=_tier_from_risk_ratio(risk_ratio, True),
        qty=qty,
        account_equity=account_equity,
        risk_budget=round(risk_budget, 2),
        capital_at_risk=round(contract_risk * qty, 2),
        entry_price=total_premium,
        notional_value=round(notional, 2),
        sizing_note=f"跨式预算更保守；按约 65% 权利金风险估算，预算约占净值 {risk_ratio:.2%}。",
    )
