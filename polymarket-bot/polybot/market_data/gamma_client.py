# polybot/market_data/gamma_client.py
from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse

import httpx

GAMMA_BASE = "https://gamma-api.polymarket.com"


@dataclass(frozen=True, slots=True)
class MarketInfo:
    condition_id: str
    clob_token_ids: List[str]
    yes_token_id: Optional[str] = None
    no_token_id: Optional[str] = None
    question: Optional[str] = None
    slug: Optional[str] = None
    market_id: Optional[str] = None


def _extract_event_slug(event_url: str) -> str:
    p = urlparse(event_url)
    path = (p.path or "").strip("/")
    parts = path.split("/")
    if len(parts) >= 2 and parts[0] == "event" and parts[1]:
        return parts[1]
    raise ValueError(f"Expected an /event/<slug> url, got: {event_url}")


def _normalize_clob_token_ids(token_ids: object) -> List[str]:
    if token_ids is None:
        return []
    if isinstance(token_ids, (list, tuple)):
        return [str(x) for x in token_ids if str(x)]
    if isinstance(token_ids, str):
        s = token_ids.strip()
        if s.startswith("[") and s.endswith("]"):
            try:
                parsed = json.loads(s)
                if isinstance(parsed, list):
                    return [str(x) for x in parsed if str(x)]
            except Exception:
                pass
        return [s] if s else []
    return [str(token_ids)]


def _extract_yes_no_tokens(market_raw: dict, clob_ids: List[str]) -> Tuple[Optional[str], Optional[str]]:
    """
    Gamma often has an `outcomes` array aligned with clobTokenIds.
    We'll map YES/NO token ids if we can; otherwise fallback to [0]=YES, [1]=NO for binary.
    """
    outcomes = market_raw.get("outcomes")
    if isinstance(outcomes, str):
        try:
            outcomes = json.loads(outcomes)
        except Exception:
            outcomes = None

    if isinstance(outcomes, list) and len(outcomes) == len(clob_ids):
        outcomes_l = [str(x).strip().lower() for x in outcomes]
        yes_idx = None
        no_idx = None
        for i, o in enumerate(outcomes_l):
            if o == "yes":
                yes_idx = i
            elif o == "no":
                no_idx = i
        yes_id = clob_ids[yes_idx] if yes_idx is not None else None
        no_id = clob_ids[no_idx] if no_idx is not None else None
        return yes_id, no_id

    if len(clob_ids) == 2:
        return clob_ids[0], clob_ids[1]

    return None, None


def _market_raw_to_info(m: dict) -> Optional[MarketInfo]:
    condition_id = m.get("conditionId") or m.get("condition_id") or m.get("conditionID")
    token_ids_raw = m.get("clobTokenIds") or m.get("clob_token_ids") or m.get("clobTokenIDs")
    if not condition_id or token_ids_raw is None:
        return None

    clob_ids = _normalize_clob_token_ids(token_ids_raw)
    if not clob_ids:
        return None

    yes_id, no_id = _extract_yes_no_tokens(m, clob_ids)

    return MarketInfo(
        condition_id=str(condition_id),
        clob_token_ids=clob_ids,
        yes_token_id=yes_id,
        no_token_id=no_id,
        question=m.get("question") or m.get("title"),
        slug=m.get("slug"),
        market_id=str(m.get("id")) if m.get("id") is not None else None,
    )


# ----------------------------
# Existing: event-based fetch
# ----------------------------

async def get_event_markets_by_event_url(event_url: str, limit_markets: int = 10) -> List[MarketInfo]:
    slug = _extract_event_slug(event_url)
    return await get_event_markets_by_slug(slug, limit_markets=limit_markets)


async def get_event_markets_by_slug(slug: str, limit_markets: int = 10) -> List[MarketInfo]:
    url = f"{GAMMA_BASE}/events/slug/{slug}"
    async with httpx.AsyncClient(timeout=20.0) as client:
        r = await client.get(url)
        r.raise_for_status()
        data = r.json()

    markets_raw = data.get("markets") or []
    out: List[MarketInfo] = []

    for m in markets_raw[: max(0, limit_markets)]:
        info = _market_raw_to_info(m)
        if info:
            out.append(info)

    return out


# ----------------------------
# New: global markets universe
# ----------------------------

async def get_markets_page(
    *,
    client: httpx.AsyncClient,
    limit: int = 100,
    offset: int = 0,
    order: str = "volume24hr",
    ascending: bool = False,
    active: Optional[bool] = True,
    closed: Optional[bool] = False,
    archived: Optional[bool] = False,
    liquidity_num_min: Optional[float] = 2500.0,
    volume_num_min: Optional[float] = 5000.0,
) -> List[MarketInfo]:
    """
    Fetch one page of markets from Gamma /markets with filters.
    Gamma supports limit/offset and many filters. :contentReference[oaicite:1]{index=1}
    """
    params: Dict[str, object] = {
        "limit": int(limit),
        "offset": int(offset),
        "order": order,
        "ascending": bool(ascending),
    }

    # These filters are widely supported by Gamma /markets
    if active is not None:
        params["active"] = bool(active)
    if closed is not None:
        params["closed"] = bool(closed)
    if archived is not None:
        params["archived"] = bool(archived)

    if liquidity_num_min is not None:
        params["liquidity_num_min"] = float(liquidity_num_min)
    if volume_num_min is not None:
        params["volume_num_min"] = float(volume_num_min)

    url = f"{GAMMA_BASE}/markets"
    r = await client.get(url, params=params)
    r.raise_for_status()
    data = r.json()

    # Gamma returns list directly for /markets in many cases; sometimes wrapped.
    if isinstance(data, list):
        raw_list = data
    else:
        raw_list = data.get("markets") or data.get("data") or []

    out: List[MarketInfo] = []
    for m in raw_list:
        if not isinstance(m, dict):
            continue
        info = _market_raw_to_info(m)
        if info:
            out.append(info)
    return out


async def get_markets(
    *,
    limit_total: int = 500,
    page_size: int = 100,
    order: str = "volume24hr",
    ascending: bool = False,
    active: Optional[bool] = True,
    closed: Optional[bool] = False,
    archived: Optional[bool] = False,
    liquidity_num_min: Optional[float] = 2500.0,
    volume_num_min: Optional[float] = 5000.0,
) -> List[MarketInfo]:
    """
    Pull multiple pages until limit_total markets collected.
    """
    out: List[MarketInfo] = []
    seen_conditions = set()

    async with httpx.AsyncClient(timeout=30.0) as client:
        offset = 0
        while len(out) < limit_total:
            page = await get_markets_page(
                client=client,
                limit=min(page_size, limit_total - len(out)),
                offset=offset,
                order=order,
                ascending=ascending,
                active=active,
                closed=closed,
                archived=archived,
                liquidity_num_min=liquidity_num_min,
                volume_num_min=volume_num_min,
            )
            if not page:
                break

            for m in page:
                if m.condition_id in seen_conditions:
                    continue
                seen_conditions.add(m.condition_id)
                out.append(m)
                if len(out) >= limit_total:
                    break

            offset += page_size

    return out


async def get_active_markets_universe(
    *,
    limit_total: int = 500,
    liquidity_min: float = 2500.0,
    volume_min: float = 5000.0,
) -> List[MarketInfo]:
    """
    Opinionated "scan the site" universe:
    - active, not closed, not archived
    - ordered by 24h volume (descending)
    - filter out illiquid/noisy markets
    """
    return await get_markets(
        limit_total=limit_total,
        page_size=100,
        order="volume24hr",
        ascending=False,
        active=True,
        closed=False,
        archived=False,
        liquidity_num_min=liquidity_min,
        volume_num_min=volume_min,
    )
