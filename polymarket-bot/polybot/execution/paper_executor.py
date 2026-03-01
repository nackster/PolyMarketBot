# polybot/execution/paper_executor.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from polybot.accounting.positions import Portfolio
from polybot.market_data.events import Side
from polybot.orderbook.store import BookStore
from polybot.planner.trade_plan import PlanStatus, TradePlan


@dataclass(frozen=True, slots=True)
class PaperExecConfig:
    """
    fill_mode:
      - "vwap": fill at VWAP for requested qty (most realistic MVP)
      - "best": fill at best bid/ask (optimistic)
    apply_fees_rate: proportional fee on notional (simple model for now)
    """
    fill_mode: str = "vwap"
    apply_fees_rate: float = 0.0


class PaperExecutor:
    def __init__(self, store: BookStore, portfolio: Portfolio, cfg: PaperExecConfig):
        self.store = store
        self.portfolio = portfolio
        self.cfg = cfg

    def execute(self, plan: TradePlan) -> TradePlan:
        if plan.status != PlanStatus.VALIDATED:
            plan.status = PlanStatus.REJECTED
            plan.reject_reason = plan.reject_reason or "plan not validated"
            return plan

        plan.status = PlanStatus.SUBMITTING

        total_notional = 0.0

        # Fill all legs; if any fail, abort (MVP)
        for leg in plan.legs:
            book = self.store.get_book(leg.token_id)
            if book is None:
                plan.status = PlanStatus.ABORTED
                plan.reject_reason = f"missing book token={leg.token_id}"
                return plan

            try:
                if self.cfg.fill_mode == "best":
                    if leg.side == Side.BUY:
                        best = book.best_ask()
                        if not best:
                            raise ValueError("no ask")
                        fill_price = best.price
                    else:
                        best = book.best_bid()
                        if not best:
                            raise ValueError("no bid")
                        fill_price = best.price
                else:
                    fill_price = book.vwap(leg.side, leg.qty)
            except Exception as e:
                plan.status = PlanStatus.ABORTED
                plan.reject_reason = f"fill failed token={leg.token_id}: {e}"
                return plan

            # Apply a simple limit check
            if leg.side == Side.BUY and fill_price > leg.limit_price + 1e-12:
                plan.status = PlanStatus.ABORTED
                plan.reject_reason = f"buy slipped beyond limit token={leg.token_id}"
                return plan
            if leg.side == Side.SELL and fill_price < leg.limit_price - 1e-12:
                plan.status = PlanStatus.ABORTED
                plan.reject_reason = f"sell slipped beyond limit token={leg.token_id}"
                return plan

            notional = float(fill_price * leg.qty)
            total_notional += notional

            # Update portfolio inventory + cash
            if leg.side == Side.BUY:
                self.portfolio.buy(leg.token_id, leg.qty, fill_price)
            else:
                self.portfolio.sell(leg.token_id, leg.qty, fill_price)

        # Apply fees to cash (simple proportional fee)
        fees = total_notional * float(self.cfg.apply_fees_rate)
        if fees > 0:
            self.portfolio.cash -= fees

        # For Tier-1 bundle: payout baseline is qty * 1.0
        plan.filled_notional = total_notional
        plan.filled_payout_equiv = float(plan.qty)  # 1.0 payout per unit bundle
        # "Realized profit" here is the theoretical bundle edge minus fees model
        plan.realized_profit = float(plan.edge_per_unit * plan.qty) - fees

        plan.status = PlanStatus.FILLED
        return plan
