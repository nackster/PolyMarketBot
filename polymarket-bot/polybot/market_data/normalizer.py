# polybot/market_data/normalizer.py
from __future__ import annotations

from typing import Any, Dict, List, Optional

from polybot.market_data.events import (
    BaseEvent,
    BookDeltaEvent,
    BookSnapshotEvent,
    EventType,
    Side,
)


def _to_float(x: Any) -> float:
    return float(str(x))


def _to_int_ms(x: Any) -> int:
    # WS docs: timestamp is unix timestamp in milliseconds (string). :contentReference[oaicite:5]{index=5}
    return int(float(str(x)))


def normalize_market_channel_message(msg: Dict[str, Any]) -> List[BaseEvent]:
    """
    Converts a Polymarket Market Channel WS message into our normalized events.

    Market Channel message types include:
      - book: full L2 snapshot (bids/asks arrays)
      - price_change: per-level updates
      - last_trade_price / best_bid_ask etc. (ignored for MVP)

    Schemas documented here. :contentReference[oaicite:6]{index=6}
    """
    et = msg.get("event_type") or msg.get("type")
    if not et:
        return []

    events: List[BaseEvent] = []

    if et == "book":
        # Docs fields: asset_id, market, bids[], asks[], timestamp. :contentReference[oaicite:7]{index=7}
        market_id = str(msg.get("market") or "")
        token_id = str(msg.get("asset_id") or "")
        ts_ms = _to_int_ms(msg.get("timestamp") or 0)

        bids_raw = msg.get("bids") or msg.get("buys") or []
        asks_raw = msg.get("asks") or msg.get("sells") or []

        bids = { _to_float(l["price"]): _to_float(l["size"]) for l in bids_raw if "price" in l and "size" in l }
        asks = { _to_float(l["price"]): _to_float(l["size"]) for l in asks_raw if "price" in l and "size" in l }

        if market_id and token_id:
            events.append(
                BookSnapshotEvent(
                    type=EventType.BOOK_SNAPSHOT,
                    market_id=market_id,
                    token_id=token_id,
                    ts_ms=ts_ms,
                    seq=None,
                    raw=msg,
                    bids=bids,
                    asks=asks,
                )
            )
        return events

    if et == "price_change":
        # Docs: { market, price_changes: [ {asset_id, price, size, side, ...} ], timestamp } :contentReference[oaicite:8]{index=8}
        market_id = str(msg.get("market") or "")
        ts_ms = _to_int_ms(msg.get("timestamp") or 0)

        pcs = msg.get("price_changes") or []
        for pc in pcs:
            token_id = str(pc.get("asset_id") or "")
            price = _to_float(pc.get("price"))
            size = _to_float(pc.get("size"))
            side_raw = str(pc.get("side") or "").upper()
            side = Side.BUY if side_raw == "BUY" else Side.SELL

            if market_id and token_id:
                events.append(
                    BookDeltaEvent(
                        type=EventType.BOOK_DELTA,
                        market_id=market_id,
                        token_id=token_id,
                        ts_ms=ts_ms,
                        seq=None,
                        raw=msg,
                        side=side,
                        price=price,
                        size=size,  # 0 means remove level
                    )
                )
        return events

    # MVP: ignore other message types (last_trade_price, best_bid_ask, etc.)
    return []
