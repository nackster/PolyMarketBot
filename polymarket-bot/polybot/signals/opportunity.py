# polybot/signals/opportunity.py
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import List, Optional

from polybot.market_data.events import Side


class OpportunityType(str, Enum):
    TIER1_BUY_BUNDLE = "tier1_buy_bundle"
    TIER1_SELL_BUNDLE = "tier1_sell_bundle"

    # NEW (Tier 2 candidate set)
    TIER2_BUY_YES_BASKET = "tier2_buy_yes_basket"
    TIER2_SELL_YES_BASKET = "tier2_sell_yes_basket"

    TIER2_BUY_FULL_SET = "tier2_buy_full_set"



@dataclass(frozen=True, slots=True)
class Leg:
    token_id: str
    side: Side            # BUY consumes asks, SELL consumes bids
    qty: float
    limit_price: float    # safety limit, not “best” price


@dataclass(frozen=True, slots=True)
class Opportunity:
    """
    A trade idea produced by signal modules.

    edge_per_unit:
      - For BUY_BUNDLE: guaranteed payout(=1) - worst_case_cost
      - For SELL_BUNDLE: worst_case_revenue - payout(=1)
    """
    opp_type: OpportunityType
    market_id: str
    legs: List[Leg]

    edge_per_unit: float
    est_fees_per_unit: float
    est_slippage_per_unit: float

    # size guidance
    suggested_qty: float
    max_qty: float

    # helpful metadata
    ts_ms: int
    note: Optional[str] = None
