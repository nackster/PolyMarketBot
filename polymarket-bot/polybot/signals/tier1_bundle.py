# polybot/signals/tier1_bundle.py
from __future__ import annotations

from dataclasses import dataclass
from typing import List, Sequence

from polybot.market_data.events import Side
from polybot.orderbook.store import BookStore
from polybot.signals.opportunity import Leg, Opportunity, OpportunityType


@dataclass(frozen=True, slots=True)
class Tier1Config:
    """
    Tier-1: within a single binary market (YES + NO full-set bundle).

    fees_rate: proportional fees applied to notional (e.g., 0.02 for 2%).
    min_edge: minimum edge per $1 payout required to act (e.g. 0.001).
    max_slippage: maximum allowed “worse than best” per leg in price terms.
                  Used to compute a conservative limit_price.
    depth_fraction_cap / vwap_levels: kept for future sizing (not used heavily here yet).

    use_tob_for_signal:
      If True, compute signal edge from Top-Of-Book (best ask/bid) sums.
      This produces more opportunities (VWAP tends to bake in depth/slippage and often kills edge).
      Execution still uses conservative limit prices around TOB.
    """
    fees_rate: float = 0.0
    min_edge: float = 0.05
    max_slippage: float = 0.02
    depth_fraction_cap: float = 0.5
    vwap_levels: int = 20

    # NEW PATCH
    use_tob_for_signal: bool = True


def _safe_limit_price(best_price: float, side: Side, max_slip: float) -> float:
    """
    Conservative limit price:
    - BUY: allow paying up to best_ask + max_slip
    - SELL: allow selling down to best_bid - max_slip
    Clamp to [0, 1].
    """
    if side == Side.BUY:
        p = best_price + max_slip
    else:
        p = best_price - max_slip
    return float(min(1.0, max(0.0, p)))


def _estimate_fee(notional: float, fees_rate: float) -> float:
    return float(notional * fees_rate) if fees_rate > 0 else 0.0


def find_tier1_bundle_opportunities(
    store: BookStore,
    market_id: str,
    token_ids: Sequence[str],
    qty: float,
    cfg: Tier1Config,
    ts_ms: int,
) -> List[Opportunity]:
    """
    Returns BUY_BUNDLE if (1 - (YES_ask + NO_ask)) >= min_edge (after slip buffer),
    Returns SELL_BUNDLE if ((YES_bid + NO_bid) - 1) >= min_edge (after slip buffer).

    Notes:
    - For binary markets, token_ids should be [YES_token, NO_token] (2 tokens),
      but we allow N legs to be safe.
    - If cfg.use_tob_for_signal=True, signal cost/revenue uses TOB sums.
      Otherwise uses VWAP(qty) sums.
    """
    if len(token_ids) < 2:
        return []

    qty = float(qty)
    if qty <= 0:
        raise ValueError("qty must be > 0")

    # Gather books
    books = []
    for tid in token_ids:
        book = store.get_book(tid)
        if book is None:
            return []
        books.append(book)

    # Need top-of-book for signal and limit calculation
    best_asks: List[float] = []
    best_bids: List[float] = []
    for b in books:
        ba = b.best_ask()
        bb = b.best_bid()
        if ba is None or bb is None:
            return []
        best_asks.append(float(ba.price))
        best_bids.append(float(bb.price))

    nlegs = len(token_ids)
    out: List[Opportunity] = []

    # Slippage cushion (very rough) expressed per $1 payout unit
    slip_buffer_per_unit = cfg.max_slippage * nlegs

    # -------------------------
    # BUY bundle (buy all legs)
    # -------------------------
    try:
        if cfg.use_tob_for_signal:
            total_cost = float(sum(best_asks)) * qty
            signal_cost_str = f"tob_ask_sum={sum(best_asks):.4f}"
        else:
            costs = [b.vwap(Side.BUY, qty) for b in books]
            total_cost = float(sum(costs))
            signal_cost_str = f"buy_vwap_sum={total_cost:.4f}"

        fees_total = _estimate_fee(total_cost, cfg.fees_rate)
        net_profit_total = qty - (total_cost + fees_total)
        edge_per_unit = net_profit_total / qty
        edge_after_slip = edge_per_unit - slip_buffer_per_unit

        if edge_after_slip >= cfg.min_edge:
            legs = [
                Leg(
                    token_id=tid,
                    side=Side.BUY,
                    qty=qty,
                    limit_price=_safe_limit_price(best_ask, Side.BUY, cfg.max_slippage),
                )
                for tid, best_ask in zip(token_ids, best_asks)
            ]
            out.append(
                Opportunity(
                    opp_type=OpportunityType.TIER1_BUY_BUNDLE,
                    market_id=market_id,
                    legs=legs,
                    edge_per_unit=edge_per_unit,
                    est_fees_per_unit=(fees_total / qty),
                    est_slippage_per_unit=slip_buffer_per_unit,
                    suggested_qty=qty,
                    max_qty=qty,
                    ts_ms=ts_ms,
                    note=(
                        f"{signal_cost_str} "
                        f"fees_total={fees_total:.4f} "
                        f"net_profit_total={net_profit_total:.4f} "
                        f"edge_per_unit={edge_per_unit:.5f}"
                    ),
                )
            )
    except Exception:
        # Keep scanner resilient
        pass

    # --------------------------
    # SELL bundle (sell all legs)
    # --------------------------
    try:
        if cfg.use_tob_for_signal:
            total_rev = float(sum(best_bids)) * qty
            signal_rev_str = f"tob_bid_sum={sum(best_bids):.4f}"
        else:
            revs = [b.vwap(Side.SELL, qty) for b in books]
            total_rev = float(sum(revs))
            signal_rev_str = f"sell_vwap_sum={total_rev:.4f}"

        fees_total = _estimate_fee(total_rev, cfg.fees_rate)
        net_profit_total = (total_rev - fees_total) - qty
        edge_per_unit = net_profit_total / qty
        edge_after_slip = edge_per_unit - slip_buffer_per_unit

        if edge_after_slip >= cfg.min_edge:
            legs = [
                Leg(
                    token_id=tid,
                    side=Side.SELL,
                    qty=qty,
                    limit_price=_safe_limit_price(best_bid, Side.SELL, cfg.max_slippage),
                )
                for tid, best_bid in zip(token_ids, best_bids)
            ]
            out.append(
                Opportunity(
                    opp_type=OpportunityType.TIER1_SELL_BUNDLE,
                    market_id=market_id,
                    legs=legs,
                    edge_per_unit=edge_per_unit,
                    est_fees_per_unit=(fees_total / qty),
                    est_slippage_per_unit=slip_buffer_per_unit,
                    suggested_qty=qty,
                    max_qty=qty,
                    ts_ms=ts_ms,
                    note=(
                        f"{signal_rev_str} "
                        f"fees_total={fees_total:.4f} "
                        f"net_profit_total={net_profit_total:.4f} "
                        f"edge_per_unit={edge_per_unit:.5f}"
                    ),
                )
            )
    except Exception:
        pass

    return out
