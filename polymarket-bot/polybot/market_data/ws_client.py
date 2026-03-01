# polybot/market_data/ws_client.py
from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass
from typing import List, Sequence

import websockets

from polybot.market_data.normalizer import normalize_market_channel_message

logger = logging.getLogger(__name__)

# Polymarket CLOB WS market channel
WS_URL_MARKET = "wss://ws-subscriptions-clob.polymarket.com/ws/market"


@dataclass(frozen=True, slots=True)
class WsConfig:
    custom_feature_enabled: bool = False
    ping_interval_s: int = 10          # Polymarket sample uses "PING" periodically
    reconnect_backoff_s: float = 1.0
    max_backoff_s: float = 30.0
    resubscribe_after_s: float = 10.0  # one-time resub to prompt snapshots


def _chunked(xs: Sequence[str], n: int) -> List[List[str]]:
    n = max(1, int(n))
    return [list(xs[i : i + n]) for i in range(0, len(xs), n)]


async def stream_market_channel_events(
    asset_ids: Sequence[str],
    out_queue: asyncio.Queue,
    cfg: WsConfig = WsConfig(),
    subscribe_chunk_size: int = 200,
) -> None:
    """
    Connect to Polymarket WS 'market' channel and stream normalized events into out_queue.

    - Subscribes using: {"assets_ids":[...],"type":"market"} (no "operation")
    - Sends "PING" periodically to keep connection healthy
    - Handles both dict and list JSON frames
    - Reconnects with backoff on errors
    """
    ids = [str(x) for x in asset_ids if x]
    if not ids:
        raise ValueError("No asset_ids provided")

    chunks = _chunked(ids, subscribe_chunk_size)
    backoff = float(cfg.reconnect_backoff_s)

    while True:
        ping_task: asyncio.Task | None = None
        try:
            logger.info("WS connecting: %s", WS_URL_MARKET)

            # NOTE: we disable built-in pings and use Polymarket's "PING" message.
            async with websockets.connect(
                WS_URL_MARKET,
                ping_interval=None,
                close_timeout=5,
                max_size=4 * 1024 * 1024,
            ) as ws:
                # ---- subscribe (required) ----
                for chunk in chunks:
                    payload = {"assets_ids": chunk, "type": "market"}
                    if cfg.custom_feature_enabled:
                        payload["custom_feature_enabled"] = True
                    await ws.send(json.dumps(payload))
                    await asyncio.sleep(0.05)

                # ---- keepalive "PING" ----
                async def _pinger() -> None:
                    while True:
                        try:
                            await ws.send("PING")
                        except Exception:
                            return
                        await asyncio.sleep(float(cfg.ping_interval_s))

                ping_task = asyncio.create_task(_pinger())

                # ---- one-time resubscribe to prompt snapshots ----
                if cfg.resubscribe_after_s and cfg.resubscribe_after_s > 0:
                    await asyncio.sleep(float(cfg.resubscribe_after_s))
                    for chunk in chunks:
                        payload = {"assets_ids": chunk, "type": "market"}
                        if cfg.custom_feature_enabled:
                            payload["custom_feature_enabled"] = True
                        await ws.send(json.dumps(payload))
                        await asyncio.sleep(0.05)

                # ---- receive loop ----
                msg_count = 0
                backoff = float(cfg.reconnect_backoff_s)

                async for raw in ws:
                    msg_count += 1
                    if msg_count == 1:
                        # Helpful if you're debugging "connected but no data"
                        print("[ws] first message received:", str(raw)[:200])

                    # Polymarket may send "PONG" or other non-JSON frames
                    if not raw or (isinstance(raw, str) and raw.upper() in ("PONG", "PING")):
                        continue

                    try:
                        payload = json.loads(raw)
                    except Exception:
                        continue

                    # Sometimes server sends batches (list of messages)
                    if isinstance(payload, list):
                        for msg in payload:
                            if isinstance(msg, dict):
                                for ev in normalize_market_channel_message(msg):
                                    await out_queue.put(ev)
                        continue

                    if isinstance(payload, dict):
                        for ev in normalize_market_channel_message(payload):
                            await out_queue.put(ev)

        except asyncio.CancelledError:
            if ping_task:
                ping_task.cancel()
            raise
        except Exception as e:
            logger.warning("WS error: %s (reconnect in %.1fs)", e, backoff)
            if ping_task:
                ping_task.cancel()
            await asyncio.sleep(backoff)
            backoff = min(float(cfg.max_backoff_s), backoff * 1.7)
