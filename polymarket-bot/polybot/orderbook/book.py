# polybot/orderbook/book.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Tuple

from polybot.market_data.events import Side


@dataclass(slots=True)
class Level:
    price: float
    size: float


class OrderBookSide:
    """
    Stores levels as a dict {price: size}.
    Keeps a cached sorted price list for fast top-of-book / VWAP queries.
    """
    __slots__ = ("_levels", "_sorted_prices", "_dirty", "_descending")

    def __init__(self, descending: bool):
        self._levels: Dict[float, float] = {}
        self._sorted_prices: List[float] = []
        self._dirty: bool = True
        self._descending: bool = descending  # bids desc, asks asc

    def clear(self) -> None:
        self._levels.clear()
        self._sorted_prices.clear()
        self._dirty = True

    def set_level(self, price: float, size: float) -> None:
        price = float(price)
        size = float(size)
        if size <= 0:
            self._levels.pop(price, None)
        else:
            self._levels[price] = size
        self._dirty = True

    def bulk_replace(self, levels: Dict[float, float]) -> None:
        self._levels = {float(p): float(s) for p, s in (levels or {}).items() if float(s) > 0}
        self._dirty = True

    def _ensure_sorted(self) -> None:
        if not self._dirty:
            return
        self._sorted_prices = sorted(self._levels.keys(), reverse=self._descending)
        self._dirty = False

    def best(self) -> Optional[Level]:
        self._ensure_sorted()
        if not self._sorted_prices:
            return None
        p = self._sorted_prices[0]
        return Level(price=p, size=self._levels[p])

    def top_n(self, n: int) -> List[Level]:
        self._ensure_sorted()
        out: List[Level] = []
        for p in self._sorted_prices[: max(0, int(n))]:
            out.append(Level(price=p, size=self._levels[p]))
        return out

    def iter_levels(self) -> Iterable[Tuple[float, float]]:
        """
        Iterates levels in price priority order.
        """
        self._ensure_sorted()
        for p in self._sorted_prices:
            yield p, self._levels[p]

    def vwap_for_qty(self, qty: float) -> float:
        """
        VWAP to TAKE qty from this side:
        - If this is asks: you're buying, you consume asks from low->high.
        - If this is bids: you're selling, you consume bids from high->low.

        Raises ValueError if insufficient liquidity.
        """
        qty = float(qty)
        if qty <= 0:
            raise ValueError("qty must be > 0")

        remaining = qty
        notional = 0.0

        for price, size in self.iter_levels():
            if remaining <= 0:
                break
            take = size if size < remaining else remaining
            notional += take * price
            remaining -= take

        if remaining > 1e-12:
            raise ValueError("insufficient liquidity for VWAP")

        return notional / qty

    def qty_available_through_price(self, price_limit: float) -> float:
        """
        Returns cumulative size available up to price_limit (inclusive).

        - For asks (ascending): available where price <= limit.
        - For bids (descending): available where price >= limit.

        This is the primitive we use for liquidity-aware sizing.
        """
        price_limit = float(price_limit)
        total = 0.0
        for price, size in self.iter_levels():
            if self._descending:  # bids
                if price < price_limit:
                    break
            else:  # asks
                if price > price_limit:
                    break
            total += float(size)
        return float(total)

    def as_dict(self) -> Dict[float, float]:
        return dict(self._levels)


class OrderBook:
    """
    One outcome token's order book (bids + asks).

    You’ll have one OrderBook per token_id (YES token, NO token, etc.).
    """
    __slots__ = ("token_id", "bids", "asks", "last_ts_ms", "last_seq")

    def __init__(self, token_id: str):
        self.token_id = token_id
        self.bids = OrderBookSide(descending=True)
        self.asks = OrderBookSide(descending=False)
        self.last_ts_ms: int = 0
        self.last_seq: Optional[int] = None

    def apply_snapshot(
        self,
        bids: Dict[float, float],
        asks: Dict[float, float],
        ts_ms: int,
        seq: Optional[int] = None,
    ) -> None:
        self.bids.bulk_replace(bids or {})
        self.asks.bulk_replace(asks or {})
        self._update_meta(ts_ms, seq)

    def apply_delta(
        self,
        side: Side,
        price: float,
        size: float,
        ts_ms: int,
        seq: Optional[int] = None,
    ) -> None:
        # Optional out-of-order protection if seq is present
        if seq is not None and self.last_seq is not None and seq < self.last_seq:
            return  # ignore stale update

        price = float(price)
        size = float(size)

        if side == Side.BUY:
            self.bids.set_level(price, size)
        else:
            self.asks.set_level(price, size)

        self._update_meta(ts_ms, seq)

    def _update_meta(self, ts_ms: int, seq: Optional[int]) -> None:
        ts_ms = int(ts_ms)
        if ts_ms > self.last_ts_ms:
            self.last_ts_ms = ts_ms
        if seq is not None:
            self.last_seq = seq if (self.last_seq is None or seq > self.last_seq) else self.last_seq

    def best_bid(self) -> Optional[Level]:
        return self.bids.best()

    def best_ask(self) -> Optional[Level]:
        return self.asks.best()

    def mid_price(self) -> Optional[float]:
        b = self.best_bid()
        a = self.best_ask()
        if not b or not a:
            return None
        return (b.price + a.price) / 2.0

    def vwap(self, side: Side, qty: float) -> float:
        """
        VWAP for taking liquidity:
        - side=BUY  => you buy, consume asks
        - side=SELL => you sell, consume bids
        """
        if side == Side.BUY:
            return self.asks.vwap_for_qty(qty)
        return self.bids.vwap_for_qty(qty)

    def max_fill_qty(self, side: Side, limit_price: float) -> float:
        """
        Liquidity-aware sizing primitive.

        Returns the maximum quantity that can be executed on this book
        without trading beyond limit_price.

        BUY  -> consumes asks with price <= limit_price
        SELL -> consumes bids with price >= limit_price
        """
        limit_price = float(limit_price)
        if limit_price <= 0:
            return 0.0

        if side == Side.BUY:
            return self.asks.qty_available_through_price(limit_price)
        else:
            return self.bids.qty_available_through_price(limit_price)

    def is_stale(self, now_ts_ms: int, max_age_ms: int) -> bool:
        return (int(now_ts_ms) - self.last_ts_ms) > int(max_age_ms)

    def top_n(self, n: int = 10) -> Dict[str, List[Level]]:
        return {"bids": self.bids.top_n(n), "asks": self.asks.top_n(n)}
