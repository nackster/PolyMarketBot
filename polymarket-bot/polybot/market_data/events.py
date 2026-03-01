# polybot/market_data/events.py
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, Optional


class Side(str, Enum):
    BUY = "buy"    # bids
    SELL = "sell"  # asks

    @property
    def book_side(self) -> str:
        return "bids" if self is Side.BUY else "asks"


class EventType(str, Enum):
    BOOK_SNAPSHOT = "book_snapshot"
    BOOK_DELTA = "book_delta"
    TRADE_PRINT = "trade_print"
    ORDER_UPDATE = "order_update"
    MARKET_STATUS = "market_status"
    HEARTBEAT = "heartbeat"


@dataclass(frozen=True, slots=True)
class BaseEvent:
    """
    Normalized event envelope.

    - market_id: Polymarket market identifier (your internal string)
    - token_id: outcome token identifier (YES token id, NO token id, etc.)
    - ts_ms: event timestamp (ms)
    - seq: monotonic sequence if provided (used to detect out-of-order)
    """
    type: EventType
    market_id: str
    token_id: Optional[str]
    ts_ms: int
    seq: Optional[int] = None
    raw: Optional[Dict[str, Any]] = None


@dataclass(frozen=True, slots=True)
class BookSnapshotEvent(BaseEvent):
    """
    Full order book snapshot for a token (one outcome).
    bids/asks map price -> size. Prices are floats in [0,1] typically.
    """
    bids: Dict[float, float] = None  # type: ignore[assignment]
    asks: Dict[float, float] = None  # type: ignore[assignment]


@dataclass(frozen=True, slots=True)
class BookDeltaEvent(BaseEvent):
    """
    Single level update.
    size=0 means remove level.
    """
    side: Side = Side.BUY
    price: float = 0.0
    size: float = 0.0


@dataclass(frozen=True, slots=True)
class TradePrintEvent(BaseEvent):
    """
    A trade that occurred (useful for analytics, not required for MVP arb).
    """
    side: Side = Side.BUY
    price: float = 0.0
    size: float = 0.0
    trade_id: Optional[str] = None


class MarketStatus(str, Enum):
    ACTIVE = "active"
    PAUSED = "paused"
    RESOLVED = "resolved"
    CLOSED = "closed"
    UNKNOWN = "unknown"


@dataclass(frozen=True, slots=True)
class MarketStatusEvent(BaseEvent):
    status: MarketStatus = MarketStatus.UNKNOWN


@dataclass(frozen=True, slots=True)
class HeartbeatEvent(BaseEvent):
    """
    Used if the WS feed provides heartbeats / pings.
    """
    pass
