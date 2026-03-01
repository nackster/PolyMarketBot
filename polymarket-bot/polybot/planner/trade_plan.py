# polybot/planner/trade_plan.py
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import List, Optional
from uuid import uuid4

from polybot.signals.opportunity import Leg, Opportunity, OpportunityType


class PlanStatus(str, Enum):
    PROPOSED = "proposed"
    VALIDATED = "validated"
    REJECTED = "rejected"
    SUBMITTING = "submitting"
    FILLED = "filled"
    PARTIAL = "partial"
    ABORTED = "aborted"
    CLOSED = "closed"


@dataclass(slots=True)
class TradePlan:
    plan_id: str
    market_id: str
    opp_type: OpportunityType
    legs: List[Leg]

    qty: float
    edge_per_unit: float
    est_fees_per_unit: float
    est_slippage_per_unit: float

    created_ts_ms: int
    status: PlanStatus = PlanStatus.PROPOSED
    reject_reason: Optional[str] = None
    note: Optional[str] = None

    # Execution results (paper/live)
    filled_notional: float = 0.0
    filled_payout_equiv: float = 0.0  # bundle payout baseline (qty * 1.0)
    realized_profit: float = 0.0


def plan_from_opportunity(opp: Opportunity) -> TradePlan:
    qty = float(opp.suggested_qty)
    return TradePlan(
        plan_id=str(uuid4()),
        market_id=opp.market_id,
        opp_type=opp.opp_type,
        legs=list(opp.legs),
        qty=qty,
        edge_per_unit=float(opp.edge_per_unit),
        est_fees_per_unit=float(opp.est_fees_per_unit),
        est_slippage_per_unit=float(opp.est_slippage_per_unit),
        created_ts_ms=int(opp.ts_ms),
        note=opp.note,
    )
