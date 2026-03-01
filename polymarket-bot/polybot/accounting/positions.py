# polybot/accounting/positions.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict


@dataclass(slots=True)
class Position:
    token_id: str
    qty: float = 0.0
    avg_price: float = 0.0  # weighted average entry price (for inventory valuation)


class Portfolio:
    """
    Minimal portfolio tracking for MVP paper trading:
    - cash balance
    - token inventory with avg entry
    - realized PnL from sells vs avg entry

    NOTE: This is not mark-to-market; we track realized PnL and inventory cost basis.
    """
    def __init__(self, starting_cash: float = 10_000.0):
        self.cash: float = float(starting_cash)
        self.realized_pnl: float = 0.0
        self.positions: Dict[str, Position] = {}

    def _pos(self, token_id: str) -> Position:
        p = self.positions.get(token_id)
        if p is None:
            p = Position(token_id=token_id)
            self.positions[token_id] = p
        return p

    def buy(self, token_id: str, qty: float, price: float) -> None:
        qty = float(qty)
        price = float(price)
        if qty <= 0:
            return

        cost = qty * price
        self.cash -= cost

        p = self._pos(token_id)
        new_qty = p.qty + qty
        if new_qty <= 0:
            p.qty = 0.0
            p.avg_price = 0.0
            return

        # weighted average price
        p.avg_price = (p.avg_price * p.qty + price * qty) / new_qty
        p.qty = new_qty

    def sell(self, token_id: str, qty: float, price: float) -> None:
        qty = float(qty)
        price = float(price)
        if qty <= 0:
            return

        p = self._pos(token_id)
        sell_qty = min(qty, p.qty)  # clamp (MVP)
        if sell_qty <= 0:
            return

        proceeds = sell_qty * price
        self.cash += proceeds

        # realized pnl = (sell price - avg entry) * qty
        self.realized_pnl += (price - p.avg_price) * sell_qty

        p.qty -= sell_qty
        if p.qty <= 1e-12:
            p.qty = 0.0
            p.avg_price = 0.0

    def snapshot(self) -> Dict[str, dict]:
        return {
            tid: {"qty": p.qty, "avg_price": p.avg_price}
            for tid, p in self.positions.items()
            if abs(p.qty) > 1e-12
        }
