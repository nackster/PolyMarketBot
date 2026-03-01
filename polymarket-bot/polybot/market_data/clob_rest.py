# polybot/market_data/clob_rest.py
from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Dict, List, Sequence

import httpx

CLOB_BASE = "https://clob.polymarket.com"


@dataclass(frozen=True, slots=True)
class BookSnapshot:
    token_id: str
    market_id: str
    bids: Dict[float, float]
    asks: Dict[float, float]
    ts_ms: int


def _levels_to_dict(levels: List[dict]) -> Dict[float, float]:
    out: Dict[float, float] = {}
    for lv in levels or []:
        try:
            p = float(lv.get("price"))
            s = float(lv.get("size"))
        except Exception:
            continue
        if s > 0:
            out[p] = s
    return out


def _ingest_item(
    item: dict,
    token_to_market: Dict[str, str],
) -> BookSnapshot | None:
    """
    Normalize either /books (batch) item or /book (single) item into BookSnapshot.
    The API may use different keys across endpoints: bids/asks or buys/sells.
    """
    token_id = str(item.get("asset_id") or item.get("token_id") or "")
    if not token_id:
        return None

    market_id = str(item.get("market") or token_to_market.get(token_id) or "unknown")

    bids = _levels_to_dict(item.get("bids") or item.get("buys") or item.get("buy") or [])
    asks = _levels_to_dict(item.get("asks") or item.get("sells") or item.get("sell") or [])

    ts_ms = 0
    try:
        if "timestamp" in item:
            ts_ms = int(float(item["timestamp"]))
    except Exception:
        ts_ms = 0

    return BookSnapshot(
        token_id=token_id,
        market_id=market_id,
        bids=bids,
        asks=asks,
        ts_ms=ts_ms,
    )


async def _fetch_book_single(client: httpx.AsyncClient, token_id: str) -> dict | None:
    """
    GET /book?token_id=...
    """
    r = await client.get(f"{CLOB_BASE}/book", params={"token_id": token_id})
    if r.status_code == 404:
        return None
    r.raise_for_status()
    data = r.json()
    return data if isinstance(data, dict) else None


async def fetch_books_snapshots(
    client: httpx.AsyncClient,
    token_ids: Sequence[str],
    token_to_market: Dict[str, str],
) -> List[BookSnapshot]:
    """
    Fast path: POST /books (batch)
    Fallback: GET /book?token_id=... for any missing token_id
    """
    wanted = [str(t) for t in token_ids if t]
    if not wanted:
        return []

    out_by_token: Dict[str, BookSnapshot] = {}

    # ---- batch: POST /books ----
    try:
        payload = [{"token_id": t} for t in wanted]
        r = await client.post(f"{CLOB_BASE}/books", json=payload)
        r.raise_for_status()
        batch = r.json()
        if isinstance(batch, list):
            for item in batch:
                if not isinstance(item, dict):
                    continue
                snap = _ingest_item(item, token_to_market)
                if snap:
                    out_by_token[snap.token_id] = snap
    except Exception:
        # It's okay if batch fails; we'll fall back per-token
        pass

    # ---- fallback: GET /book for missing tokens ----
    missing = [t for t in wanted if t not in out_by_token]
    if missing:
        sem = asyncio.Semaphore(10)

        async def _one(t: str) -> None:
            async with sem:
                data = await _fetch_book_single(client, t)
            if not data:
                return
            snap = _ingest_item(data, token_to_market)
            if snap:
                out_by_token[snap.token_id] = snap

        await asyncio.gather(*[_one(t) for t in missing], return_exceptions=True)

    return list(out_by_token.values())
