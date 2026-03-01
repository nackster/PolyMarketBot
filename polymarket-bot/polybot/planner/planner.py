# polybot/planner/planner.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from polybot.market_data.events import Side
from polybot.orderbook.store import BookStore
from polybot.planner.trade_plan import PlanStatus, TradePlan, plan_from_opportunity
from polybot.signals.opportunity import Opportunity, OpportunityType


@dataclass(frozen=True, slots=True)
class PlannerConfig:
    min_edge: float = 0.05
    max_book_age_ms: int = 1500  # if books older than this, reject
    max_qty: float = 1_000.0     # hard cap for MVP
    require_all_books_present: bool = True


class Planner:
    def __init__(self, store: BookStore, cfg: PlannerConfig):
        self.store = store
        self.cfg = cfg

    def build_plan(self, opp: Opportunity, now_ts_ms: int) -> TradePlan:
        plan = plan_from_opportunity(opp)

        # Basic edge gate
        if plan.edge_per_unit < self.cfg.min_edge:
            plan.status = PlanStatus.REJECTED
            plan.reject_reason = f"edge {plan.edge_per_unit:.4f} < min_edge {self.cfg.min_edge:.4f}"
            return plan

        # Qty gate
        if plan.qty <= 0 or plan.qty > self.cfg.max_qty:
            plan.status = PlanStatus.REJECTED
            plan.reject_reason = f"qty {plan.qty} out of range"
            return plan

        # Staleness + liquidity gate (VWAP must be computable)
        for leg in plan.legs:
            book = self.store.get_book(leg.token_id)
            if book is None:
                if self.cfg.require_all_books_present:
                    plan.status = PlanStatus.REJECTED
                    plan.reject_reason = f"missing book for token {leg.token_id}"
                    return plan
                continue

            if book.is_stale(now_ts_ms, self.cfg.max_book_age_ms):
                age_ms = int(now_ts_ms) - int(book.last_ts_ms)
                plan.status = PlanStatus.REJECTED
                plan.reject_reason = (
                    f"stale book token={leg.token_id} age_ms={age_ms} "
                    f"last_ts_ms={int(book.last_ts_ms)} now_ts_ms={int(now_ts_ms)}"
                )
                return plan

            # Ensure VWAP can be computed for intended qty
            try:
                _ = book.vwap(leg.side, leg.qty)
            except Exception as e:
                plan.status = PlanStatus.REJECTED
                plan.reject_reason = f"no liquidity token={leg.token_id}: {e}"
                return plan

        plan.status = PlanStatus.VALIDATED
        return plan
