# polybot/signals/tier2_candidate_set.py
from __future__ import annotations

from dataclasses import dataclass
from typing import List, Sequence

from polybot.market_data.events import Side
from polybot.orderbook.store import BookStore
from polybot.signals.opportunity import Leg, Opportunity, OpportunityType


@dataclass(frozen=True, slots=True)
class Tier2Config:
    fees_rate: float = 0.0
    min_edge: float = 0.02
    max_slippage: float = 0.03

    # New: liquidity-aware sizing controls
    depth_fraction: float = 0.50      # use only 50% of the calculated available depth (safety)
    min_total_profit: float = 1.00    # dollars (edge * qty) must exceed this
    min_total_notional: float = 20.0  # dollars spent/received must exceed this


def _clamp01(x: float) -> float:
    return float(min(1.0, max(0.0, x)))


def _safe_limit_price(best_price: float, side: Side, max_slip: float) -> float:
    """
    Construct a limit price that allows up to max_slip movement beyond top-of-book.
    (In real execution you may use tighter limits + retry logic.)
    """
    best_price = float(best_price)
    max_slip = float(max_slip)

    if side == Side.BUY:
        return _clamp01(best_price + max_slip)
    else:
        return _clamp01(best_price - max_slip)


def _estimate_fee(notional: float, fees_rate: float) -> float:
    return float(notional * fees_rate) if fees_rate > 0 else 0.0


def _basket_max_qty(
    store: BookStore,
    token_ids: Sequence[str],
    side: Side,
    limit_prices: Sequence[float],
    depth_fraction: float,
) -> float:
    """
    Basket qty is limited by the thinnest leg (min fillable qty across legs),
    optionally scaled down by depth_fraction for safety.
    """
    if len(token_ids) != len(limit_prices):
        return 0.0

    max_q = None
    for tid, lp in zip(token_ids, limit_prices):
        b = store.get_book(tid)
        if b is None:
            return 0.0
        leg_q = b.max_fill_qty(side, lp)
        if max_q is None or leg_q < max_q:
            max_q = leg_q

    if max_q is None:
        return 0.0

    frac = float(depth_fraction)
    if frac <= 0:
        frac = 0.0
    if frac > 1:
        frac = 1.0

    return float(max_q * frac)


def find_candidate_yes_basket_opportunities(
    store: BookStore,
    basket_id: str,
    yes_token_ids: Sequence[str],
    qty: float,              # if <= 0, auto-size based on depth
    cfg: Tier2Config,
    ts_ms: int,
) -> List[Opportunity]:
    """
    Tier 2: Candidate-set arbitrage across multiple binary markets.

    Assumption: Exactly one candidate wins => exactly one YES resolves to 1, others 0.
    Buying all YES across candidates is a "basket" that pays out 1.

    BUY basket:
      edge_per_unit = 1 - sum(VWAP_asks(YES_i, qty)) - fees_per_unit

    SELL basket:
      edge_per_unit = sum(VWAP_bids(YES_i, qty)) - 1 - fees_per_unit
    """
    if len(yes_token_ids) < 2:
        return []

    # Need books and top-of-book for each token
    books = []
    best_asks = []
    best_bids = []

    for tid in yes_token_ids:
        b = store.get_book(tid)
        if b is None:
            return []
        ba = b.best_ask()
        bb = b.best_bid()
        if ba is None or bb is None:
            return []
        books.append(b)
        best_asks.append(float(ba.price))
        best_bids.append(float(bb.price))

    out: List[Opportunity] = []

    # ---- BUY all YES (basket) ----
    buy_limits = [_safe_limit_price(p, Side.BUY, cfg.max_slippage) for p in best_asks]
    buy_max_qty = _basket_max_qty(store, yes_token_ids, Side.BUY, buy_limits, cfg.depth_fraction)

    if buy_max_qty > 0:
        qty_exec = float(buy_max_qty if qty <= 0 else min(float(qty), buy_max_qty))

        # Compute VWAP cost for qty_exec
        try:
            costs = [b.vwap(Side.BUY, qty_exec) for b in books]
            total_cost = float(sum(costs))
            fees = _estimate_fee(total_cost, cfg.fees_rate)
            edge_per_unit = 1.0 - (total_cost + fees)

            total_profit = edge_per_unit * qty_exec
            total_notional = total_cost

            if (
                edge_per_unit >= cfg.min_edge
                and total_profit >= cfg.min_total_profit
                and total_notional >= cfg.min_total_notional
            ):
                legs = [
                    Leg(
                        token_id=tid,
                        side=Side.BUY,
                        qty=qty_exec,
                        limit_price=lp,
                    )
                    for tid, lp in zip(yes_token_ids, buy_limits)
                ]
                out.append(
                    Opportunity(
                        opp_type=OpportunityType.TIER2_BUY_YES_BASKET,
                        market_id=basket_id,
                        legs=legs,
                        edge_per_unit=edge_per_unit,
                        est_fees_per_unit=fees,
                        est_slippage_per_unit=0.0,
                        suggested_qty=qty_exec,
                        max_qty=buy_max_qty,
                        ts_ms=ts_ms,
                        note=(
                            f"tier2_buy_yes_basket cost={total_cost:.4f} fees={fees:.4f} "
                            f"edge={edge_per_unit:.4f} qty={qty_exec:.2f} max_qty={buy_max_qty:.2f} "
                            f"profit={total_profit:.2f} n={len(yes_token_ids)}"
                        ),
                    )
                )
        except Exception:
            pass

    # ---- SELL all YES (basket) ----
    sell_limits = [_safe_limit_price(p, Side.SELL, cfg.max_slippage) for p in best_bids]
    sell_max_qty = _basket_max_qty(store, yes_token_ids, Side.SELL, sell_limits, cfg.depth_fraction)

    if sell_max_qty > 0:
        qty_exec = float(sell_max_qty if qty <= 0 else min(float(qty), sell_max_qty))

        try:
            revs = [b.vwap(Side.SELL, qty_exec) for b in books]
            total_rev = float(sum(revs))
            fees = _estimate_fee(total_rev, cfg.fees_rate)
            edge_per_unit = (total_rev - fees) - 1.0

            total_profit = edge_per_unit * qty_exec
            total_notional = total_rev

            if (
                edge_per_unit >= cfg.min_edge
                and total_profit >= cfg.min_total_profit
                and total_notional >= cfg.min_total_notional
            ):
                legs = [
                    Leg(
                        token_id=tid,
                        side=Side.SELL,
                        qty=qty_exec,
                        limit_price=lp,
                    )
                    for tid, lp in zip(yes_token_ids, sell_limits)
                ]
                out.append(
                    Opportunity(
                        opp_type=OpportunityType.TIER2_SELL_YES_BASKET,
                        market_id=basket_id,
                        legs=legs,
                        edge_per_unit=edge_per_unit,
                        est_fees_per_unit=fees,
                        est_slippage_per_unit=0.0,
                        suggested_qty=qty_exec,
                        max_qty=sell_max_qty,
                        ts_ms=ts_ms,
                        note=(
                            f"tier2_sell_yes_basket rev={total_rev:.4f} fees={fees:.4f} "
                            f"edge={edge_per_unit:.4f} qty={qty_exec:.2f} max_qty={sell_max_qty:.2f} "
                            f"profit={total_profit:.2f} n={len(yes_token_ids)}"
                        ),
                    )
                )
        except Exception:
            pass

    return out
