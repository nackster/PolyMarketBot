# polybot/signals/tier2_full_set.py
from __future__ import annotations

from dataclasses import dataclass
from typing import List, Sequence

from polybot.market_data.events import Side
from polybot.orderbook.store import BookStore
from polybot.optimization import FrankWolfeProjector, SimplexOracle, kl_divergence
from polybot.signals.opportunity import Leg, Opportunity, OpportunityType


@dataclass(frozen=True, slots=True)
class Tier2FullSetConfig:
    """
    Full-set arb for multi-outcome markets (N > 2).
    Buy 1 share of every outcome => guaranteed $1 payout per set.

    fees_rate: proportional fees applied to notional (if applicable)
    min_edge: minimum edge per $1 payout (per-unit) required to act
    max_slippage: conservative cushion when building limit prices
    min_legs: require markets with at least this many outcomes
    """
    fees_rate: float = 0.0
    min_edge: float = 0.001
    max_slippage: float = 0.002
    min_legs: int = 3

    # Optional: report Bregman/FW diagnostic gap (does not change trading logic)
    report_fw_gap: bool = False
    fw_max_iter: int = 80
    fw_tol: float = 1e-6


def _clamp01(x: float) -> float:
    return float(min(1.0, max(0.0, x)))


def _safe_limit_price(best_price: float, side: Side, max_slip: float) -> float:
    """
    Conservative limit:
      BUY: best_ask + max_slip
      SELL: best_bid - max_slip
    """
    if side == Side.BUY:
        return _clamp01(best_price + max_slip)
    return _clamp01(best_price - max_slip)


def _estimate_fee(notional: float, fees_rate: float) -> float:
    return float(notional * fees_rate) if fees_rate and fees_rate > 0 else 0.0


def _normalize(x: Sequence[float]) -> List[float]:
    s = float(sum(x))
    if s <= 0:
        return [1.0 / len(x) for _ in x]
    return [float(v) / s for v in x]


def find_tier2_full_set_opportunities(
    store: BookStore,
    market_id: str,
    token_ids: Sequence[str],
    qty: float,
    cfg: Tier2FullSetConfig,
    ts_ms: int,
) -> List[Opportunity]:
    """
    Full-set BUY arb:
      cost = sum(VWAP_buy(token_i, qty))
      payout = 1.0 * qty
      net_profit = payout - cost - fees
      edge_per_unit = net_profit / qty
    """
    qty = float(qty)
    if qty <= 0:
        raise ValueError("qty must be > 0")

    token_ids = [str(t) for t in token_ids if t]
    if len(token_ids) < cfg.min_legs:
        return []

    # gather books + top-of-book asks for limit pricing
    books = []
    best_asks = []
    for tid in token_ids:
        b = store.get_book(tid)
        if not b:
            return []
        ba = b.best_ask()
        bb = b.best_bid()
        if ba is None or bb is None:
            return []
        books.append(b)
        best_asks.append(float(ba.price))

    try:
        costs = [float(b.vwap(Side.BUY, qty)) for b in books]
    except Exception:
        return []

    total_cost = float(sum(costs))
    fees_total = _estimate_fee(total_cost, cfg.fees_rate)

    payout_total = 1.0 * qty
    net_profit_total = payout_total - (total_cost + fees_total)
    edge_per_unit = net_profit_total / qty  # normalized per $1 payout

    if edge_per_unit < cfg.min_edge:
        return []

    legs = [
        Leg(
            token_id=tid,
            side=Side.BUY,
            qty=qty,
            limit_price=_safe_limit_price(best_ask, Side.BUY, cfg.max_slippage),
        )
        for tid, best_ask in zip(token_ids, best_asks)
    ]

    note_extra = ""
    if cfg.report_fw_gap:
        try:
            theta_raw = []
            for b, best_ask in zip(books, best_asks):
                mid = b.mid_price()
                theta_raw.append(float(mid if mid is not None else best_ask))

            oracle = SimplexOracle(len(theta_raw))
            fw = FrankWolfeProjector(max_iter=cfg.fw_max_iter, tol=cfg.fw_tol, use_line_search=False)
            res = fw.project(theta_raw, oracle, init=_normalize(theta_raw))
            gap = res.gap
            kl = kl_divergence(res.mu, _normalize(theta_raw))
            note_extra = f" fw_gap={gap:.6e} fw_kl={kl:.6e}"
        except Exception:
            note_extra = ""

    return [
        Opportunity(
            opp_type=OpportunityType.TIER2_BUY_FULL_SET,
            market_id=market_id,
            legs=legs,
            edge_per_unit=edge_per_unit,
            est_fees_per_unit=(fees_total / qty) if qty else 0.0,
            est_slippage_per_unit=(cfg.max_slippage * len(token_ids)),
            suggested_qty=qty,
            max_qty=qty,
            ts_ms=ts_ms,
            note=(
                f"full_set_buy_vwap_sum={total_cost:.4f} "
                f"fees_total={fees_total:.4f} "
                f"net_profit_total={net_profit_total:.4f} "
                f"edge_per_unit={edge_per_unit:.6f} "
                f"nlegs={len(token_ids)}"
                f"{note_extra}"
            ),
        )
    ]
