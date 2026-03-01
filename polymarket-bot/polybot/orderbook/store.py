# polybot/orderbook/store.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Set, Tuple

from polybot.market_data.events import BaseEvent, BookDeltaEvent, BookSnapshotEvent, EventType
from polybot.orderbook.book import OrderBook


@dataclass(slots=True)
class MarketIndex:
    """
    Tracks which token_ids belong to a market_id.
    Useful for scanning all outcomes in a market.
    """
    market_id: str
    token_ids: Set[str]


class BookStore:
    """
    In-memory state store for order books.

    - One OrderBook per token_id
    - Index of market_id -> token_ids
    - Applies normalized events from market_data layer
    """

    def __init__(self) -> None:
        self._books_by_token: Dict[str, OrderBook] = {}
        self._market_to_tokens: Dict[str, Set[str]] = {}

    # -------- accessors --------

    def get_or_create_book(self, token_id: str) -> OrderBook:
        book = self._books_by_token.get(token_id)
        if book is None:
            book = OrderBook(token_id=token_id)
            self._books_by_token[token_id] = book
        return book

    def get_book(self, token_id: str) -> Optional[OrderBook]:
        return self._books_by_token.get(token_id)

    def token_ids_for_market(self, market_id: str) -> List[str]:
        return sorted(self._market_to_tokens.get(market_id, set()))

    def markets(self) -> List[str]:
        return sorted(self._market_to_tokens.keys())

    def all_books(self) -> Iterable[OrderBook]:
        return self._books_by_token.values()

    # -------- indexing --------

    def _index(self, market_id: str, token_id: str) -> None:
        s = self._market_to_tokens.get(market_id)
        if s is None:
            s = set()
            self._market_to_tokens[market_id] = s
        s.add(token_id)

    # -------- event application --------

    def apply(self, event: BaseEvent) -> None:
        """
        Apply a normalized event to the store.

        Accepts:
          - BookSnapshotEvent
          - BookDeltaEvent

        Ignores:
          - Events with token_id=None
          - Unknown event types
        """
        if event.token_id is None:
            return

        if event.type == EventType.BOOK_SNAPSHOT:
            assert isinstance(event, BookSnapshotEvent)
            book = self.get_or_create_book(event.token_id)
            self._index(event.market_id, event.token_id)
            book.apply_snapshot(
                bids=event.bids or {},
                asks=event.asks or {},
                ts_ms=event.ts_ms,
                seq=event.seq,
            )
            return

        if event.type == EventType.BOOK_DELTA:
            assert isinstance(event, BookDeltaEvent)
            book = self.get_or_create_book(event.token_id)
            self._index(event.market_id, event.token_id)
            book.apply_delta(
                side=event.side,
                price=event.price,
                size=event.size,
                ts_ms=event.ts_ms,
                seq=event.seq,
            )
            return

        # Ignore other event types for now.
        return
