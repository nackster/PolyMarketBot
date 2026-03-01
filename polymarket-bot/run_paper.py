# run_paper.py
# LIVE market-data scanner for Polymarket CLOB
# - pulls first N markets from Gamma
# - subscribes to their CLOB token IDs (WS market channel)
# - maintains in-memory orderbooks (BookStore)
# - scans:
#     Tier-1: within a single binary market (YES+NO bundle)
#     Tier-2: across many candidate markets (buy/sell all YES basket)
#
# Safety default: DOES NOT paper-execute unless EXECUTE_PAPER=1

from __future__ import annotations

import asyncio
import csv
import logging
import os
import time
from pathlib import Path
from typing import List, Optional

import httpx

from polybot.accounting.positions import Portfolio
from polybot.execution.paper_executor import PaperExecConfig, PaperExecutor
from polybot.market_data.clob_rest import fetch_books_snapshots
from polybot.market_data.events import BookSnapshotEvent, EventType, Side
from polybot.market_data.gamma_client import MarketInfo, get_active_markets_universe
from polybot.market_data.ws_client import WsConfig, stream_market_channel_events
from polybot.orderbook.store import BookStore
from polybot.planner.planner import Planner, PlannerConfig
from polybot.signals.tier1_bundle import Tier1Config, find_tier1_bundle_opportunities
from polybot.signals.tier2_candidate_set import Tier2Config, find_candidate_yes_basket_opportunities
from polybot.signals.tier2_full_set import Tier2FullSetConfig, find_tier2_full_set_opportunities


# --------- logging ---------

LOG_PATH = os.getenv("RUN_PAPER_LOG", "run_paper.log")
LOG_LEVEL = os.getenv("RUN_PAPER_LOG_LEVEL", "INFO").upper()

_logger = logging.getLogger("polybot.run_paper")
if not _logger.handlers:
    _logger.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))
    _logger.propagate = False

    _fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")

    _stream = logging.StreamHandler()
    _stream.setFormatter(_fmt)

    _file = logging.FileHandler(LOG_PATH, encoding="utf-8")
    _file.setFormatter(_fmt)

    _logger.addHandler(_stream)
    _logger.addHandler(_file)


def _log_print(*args, **kwargs):
    msg = " ".join(str(a) for a in args)
    _logger.info(msg)


# Shadow print to also write to log file
print = _log_print  # type: ignore[assignment]


# --------- CONFIG ---------

EVENT_URL = "https://polymarket.com/event/texas-democratic-senate-primary-winner"
MAX_MARKETS = 10  # not used in your current universe fetch, kept for reference

# Safety default: do not execute paper trades unless explicitly enabled
EXECUTE_PAPER = os.getenv("EXECUTE_PAPER", "0").strip() == "1"
REPORT_FW_GAP = os.getenv("REPORT_FW_GAP", "0").strip() == "1"

# Scan cadence / thresholds
SCAN_INTERVAL_S = 0.25
PRINT_INTERVAL_MS = 3000

# Planner staleness gate (ms)
MAX_BOOK_AGE_MS = int(os.getenv("MAX_BOOK_AGE_MS", "300000"))

# Start small for live testing
TIER1_SCAN_QTY = 0.1   # or 0.2
TIER2_SCAN_QTY = 2.0  # <= 0 means "auto-size based on depth" (depends on your tier2 code)

# Lower edges initially to see signals; tighten later
TIER1_MIN_EDGE = 0.00005  # just to see signals; tighten later
TIER2_MIN_EDGE = 0.02

# Slippage cushion used to compute leg limit prices (still paper mode)
MAX_SLIPPAGE = 0.001

TIER1_QTYS = [1.0, 2.0, 5.0, 10.0]   # qty ladder for Tier-1 VWAP scan
TIER1_SCREEN_USING_TOB = True        # enable top-of-book screening prints

# --------- helpers ---------

def now_ms() -> int:
    return int(time.time() * 1000)

def dedupe_keep_order(xs: List[str]) -> List[str]:
    return list(dict.fromkeys(xs))

def fmt(x: Optional[float], ndp: int = 4) -> str:
    if x is None:
        return "NA"
    try:
        return f"{float(x):.{ndp}f}"
    except Exception:
        return "NA"

# --------- scanner loop ---------

async def event_consumer(store: BookStore, yes_tokens: List[str]) -> None:
    # Tier-1 config
    tier1_cfg = Tier1Config(
        fees_rate=0.0,
        min_edge=TIER1_MIN_EDGE,
        max_slippage=MAX_SLIPPAGE,
        use_tob_for_signal=True,
    )

    # Tier-2 config
    tier2_cfg = Tier2Config(
        min_edge=TIER2_MIN_EDGE,
        max_slippage=0.03,
        fees_rate=0.0,
        depth_fraction=0.50,
        min_total_profit=1.00,
        min_total_notional=20.0,
    )

    tier2_fs_cfg = Tier2FullSetConfig(
        fees_rate=0.0,
        min_edge=0.0005,      # start low to see hits, tighten later
        max_slippage=0.002,
        min_legs=3,
        report_fw_gap=REPORT_FW_GAP,
    )

    TIER2_FULLSET_QTY = 1.0

    # Planner gates (shared)
    planner = Planner(
        store=store,
        cfg=PlannerConfig(
            min_edge=min(TIER1_MIN_EDGE, TIER2_MIN_EDGE),
            max_book_age_ms=MAX_BOOK_AGE_MS,
            max_qty=250.0,
        ),
    )

    # Paper execution (optional)
    portfolio = Portfolio(starting_cash=10_000.0)
    executor = PaperExecutor(
        store=store,
        portfolio=portfolio,
        cfg=PaperExecConfig(fill_mode="vwap", apply_fees_rate=0.0),
    )

    TRADES_CSV = Path("paper_trades.csv")
    if EXECUTE_PAPER and not TRADES_CSV.exists():
        with TRADES_CSV.open("w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["ts_ms", "market_id", "opp_type", "filled_notional", "realized_profit", "cash", "realized_pnl"])

    yes_tokens = [str(x) for x in yes_tokens if x]

    last_print = 0
    last_portfolio_print_ms = 0

    # trade cooldown
    last_trade_ms_by_market: dict[str, int] = {}
    TRADE_COOLDOWN_MS = 60_000  # 60 seconds between trades per market

    # to prevent spam: only print TOB best when it changes
    last_best_tob_key = None
    last_best_vwap_key = None

    while True:
        await asyncio.sleep(SCAN_INTERVAL_S)
        now = now_ms()

        do_print = (now - last_print >= PRINT_INTERVAL_MS)
        if do_print:
            last_print = now

        market_ids = store.markets()
        total_markets = len(market_ids)
        total_tokens = sum(len(store.token_ids_for_market(m)) for m in market_ids)

        if do_print:
            print(f"\n[live] scanning... markets={total_markets} tokens_indexed={total_tokens} tier2_yes_tokens={len(yes_tokens)}")

        # ---- portfolio/equity heartbeat (paper only) ----
        if EXECUTE_PAPER and (now - last_portfolio_print_ms >= 30_000):
            last_portfolio_print_ms = now

            cash = executor.portfolio.cash
            realized = executor.portfolio.realized_pnl

            mtm = 0.0
            for tid, pos in executor.portfolio.positions.items():
                qty = float(pos.get("qty", 0.0))
                if qty == 0:
                    continue
                b = store.get_book(tid)
                if not b:
                    continue
                px = b.mid_price()
                if px is None:
                    bb = b.best_bid()
                    px = float(bb.price) if bb else None
                if px is None:
                    continue
                mtm += qty * float(px)

            equity = cash + mtm
            start_cash = getattr(executor.portfolio, "starting_cash", 10_000.0)
            total_pnl = equity - float(start_cash)

            # Print only non-zero positions (cleaner)
            nonzero = {}
            for k, v in executor.portfolio.positions.items():
                q = float(v.get("qty", 0.0))
                if abs(q) > 1e-12:
                    nonzero[k] = {"qty": q, "avg_price": v.get("avg_price")}

            print(
                f"\n=== PORTFOLIO ===\n"
                f"cash={cash:.2f} mtm={mtm:.2f} equity={equity:.2f}\n"
                f"realized_pnl={realized:.2f} total_pnl={total_pnl:.2f}\n"
                f"open_positions={len(nonzero)}\n"
            )
            if nonzero:
                # print up to 20
                i = 0
                for tid, info in nonzero.items():
                    print(f"  {tid} qty={info['qty']} avg_price={info['avg_price']}")
                    i += 1
                    if i >= 20:
                        break

        # ---- sample market print (only on print window) ----
        if do_print:
            printed_sample = False
            for mid in market_ids:
                toks = store.token_ids_for_market(mid)
                if len(toks) < 2:
                    continue
                b0 = store.get_book(toks[0])
                b1 = store.get_book(toks[1])
                if not b0 or not b1:
                    continue
                bb0, ba0 = b0.best_bid(), b0.best_ask()
                bb1, ba1 = b1.best_bid(), b1.best_ask()
                if not (bb0 and ba0 and bb1 and ba1):
                    continue
                print(f"  sample market={mid}")
                print(f"    token0={toks[0]} bid/ask={float(bb0.price):.3f}/{float(ba0.price):.3f}")
                print(f"    token1={toks[1]} bid/ask={float(bb1.price):.3f}/{float(ba1.price):.3f}")
                printed_sample = True
                break
            if not printed_sample:
                print("  (no complete books yet — waiting for book snapshots)")

        # ---- TIER 1 SCAN ----

        # Global bests across the whole universe (per loop)
        best_tob_buy_edge = float("-inf")
        best_tob_sell_edge = float("-inf")
        best_tob_buy_mid = None
        best_tob_sell_mid = None
        best_tob_buy_cost: Optional[float] = None
        best_tob_sell_rev: Optional[float] = None

        best_vwap_buy_edge = float("-inf")
        best_vwap_sell_edge = float("-inf")
        best_vwap_buy_mid = None
        best_vwap_sell_mid = None
        best_vwap_buy_qty = None
        best_vwap_sell_qty = None
        best_vwap_buy_cost: Optional[float] = None
        best_vwap_sell_rev: Optional[float] = None

        for mid in market_ids:
            toks = store.token_ids_for_market(mid)
            if len(toks) < 2:
                continue

            b0 = store.get_book(toks[0])
            b1 = store.get_book(toks[1])
            if not b0 or not b1:
                continue

            # ---- B) TOB screening (compute, but DO NOT print here) ----
            bb0, ba0 = b0.best_bid(), b0.best_ask()
            bb1, ba1 = b1.best_bid(), b1.best_ask()
            if bb0 and ba0 and bb1 and ba1:
                tob_buy_cost = float(ba0.price) + float(ba1.price)
                tob_sell_rev = float(bb0.price) + float(bb1.price)
                tob_buy_edge = 1.0 - tob_buy_cost
                tob_sell_edge = tob_sell_rev - 1.0

                if tob_buy_edge > best_tob_buy_edge:
                    best_tob_buy_edge = tob_buy_edge
                    best_tob_buy_mid = mid
                    best_tob_buy_cost = tob_buy_cost

                if tob_sell_edge > best_tob_sell_edge:
                    best_tob_sell_edge = tob_sell_edge
                    best_tob_sell_mid = mid
                    best_tob_sell_rev = tob_sell_rev

            # ---- C) VWAP qty ladder (pick best qty per market) ----
            best_buy_edge_for_market = float("-inf")
            best_sell_edge_for_market = float("-inf")
            best_buy_qty_for_market = None
            best_sell_qty_for_market = None
            best_buy_cost_for_market: Optional[float] = None
            best_sell_rev_for_market: Optional[float] = None

            for q in TIER1_QTYS:
                try:
                    buy_cost = float(b0.vwap(Side.BUY, q) + b1.vwap(Side.BUY, q))
                    sell_rev = float(b0.vwap(Side.SELL, q) + b1.vwap(Side.SELL, q))
                except Exception:
                    continue

                buy_edge = 1.0 - buy_cost
                sell_edge = sell_rev - 1.0

                if buy_edge > best_buy_edge_for_market:
                    best_buy_edge_for_market = buy_edge
                    best_buy_qty_for_market = q
                    best_buy_cost_for_market = buy_cost

                if sell_edge > best_sell_edge_for_market:
                    best_sell_edge_for_market = sell_edge
                    best_sell_qty_for_market = q
                    best_sell_rev_for_market = sell_rev

            # Update global best VWAP trackers (for printing)
            if best_buy_qty_for_market is not None and best_buy_edge_for_market > best_vwap_buy_edge:
                best_vwap_buy_edge = best_buy_edge_for_market
                best_vwap_buy_mid = mid
                best_vwap_buy_qty = best_buy_qty_for_market
                best_vwap_buy_cost = best_buy_cost_for_market

            if best_sell_qty_for_market is not None and best_sell_edge_for_market > best_vwap_sell_edge:
                best_vwap_sell_edge = best_sell_edge_for_market
                best_vwap_sell_mid = mid
                best_vwap_sell_qty = best_sell_qty_for_market
                best_vwap_sell_rev = best_sell_rev_for_market

            # Decide qty to run signal for this market (only 1 attempt)
            chosen_qty = None
            if best_buy_qty_for_market is not None and best_buy_edge_for_market >= best_sell_edge_for_market:
                chosen_qty = best_buy_qty_for_market
            elif best_sell_qty_for_market is not None:
                chosen_qty = best_sell_qty_for_market

            if chosen_qty is None:
                continue

            opps = find_tier1_bundle_opportunities(
                store=store,
                market_id=mid,
                token_ids=toks,
                qty=float(chosen_qty),
                cfg=tier1_cfg,
                ts_ms=now,
            )

            for opp in opps:
                print("\n*** TIER1 OPPORTUNITY DETECTED ***")
                print(
                    f"market={mid} type={opp.opp_type} edge_per_unit={opp.edge_per_unit:.6f} "
                    f"qty={opp.suggested_qty}"
                )

                last_t = last_trade_ms_by_market.get(mid, 0)
                if now - last_t < TRADE_COOLDOWN_MS:
                    print("  [cooldown] skipping trade (recent fill)")
                    continue

                if opp.note:
                    print(f"note={opp.note}")

                plan = planner.build_plan(opp, now_ts_ms=now)
                print(f"plan_status={plan.status} reason={plan.reject_reason}")

                if EXECUTE_PAPER and plan.status.value == "validated":
                    plan = executor.execute(plan)

                    if plan.status.value == "filled":
                        with TRADES_CSV.open("a", newline="") as f:
                            w = csv.writer(f)
                            w.writerow([
                                now,
                                getattr(plan, "market_id", ""),
                                getattr(plan, "opp_type", ""),
                                f"{plan.filled_notional:.2f}",
                                f"{plan.realized_profit:.6f}",
                                f"{executor.portfolio.cash:.2f}",
                                f"{executor.portfolio.realized_pnl:.2f}",
                            ])
                        last_trade_ms_by_market[mid] = now

                    print(
                        f"EXECUTED(PAPER) status={plan.status} profit_est={plan.realized_profit:.6f} "
                        f"notional={plan.filled_notional:.2f}"
                    )
                    print(
                        f"portfolio cash={portfolio.cash:.2f} realized_pnl={portfolio.realized_pnl:.2f} "
                        f"positions={portfolio.snapshot()}"
                    )

        # ---- Print best-of-interval summary (ONLY when do_print) ----
        if do_print:
            # TOB summary (print only if changed)
            tob_key = (best_tob_buy_mid, best_tob_buy_edge, best_tob_sell_mid, best_tob_sell_edge)
            if tob_key != last_best_tob_key:
                last_best_tob_key = tob_key
                if TIER1_SCREEN_USING_TOB and best_tob_buy_mid is not None:
                    print(
                        f"  best_tier1_TOB_buy_edge={best_tob_buy_edge:.6f} "
                        f"market={best_tob_buy_mid} tob_buy_cost={fmt(best_tob_buy_cost, 4)}"
                    )
                if TIER1_SCREEN_USING_TOB and best_tob_sell_mid is not None:
                    print(
                        f"  best_tier1_TOB_sell_edge={best_tob_sell_edge:.6f} "
                        f"market={best_tob_sell_mid} tob_sell_rev={fmt(best_tob_sell_rev, 4)}"
                    )

            # VWAP summary (print only if changed)
            vwap_key = (best_vwap_buy_mid, best_vwap_buy_edge, best_vwap_buy_qty,
                        best_vwap_sell_mid, best_vwap_sell_edge, best_vwap_sell_qty)
            if vwap_key != last_best_vwap_key:
                last_best_vwap_key = vwap_key
                if best_vwap_buy_mid is not None:
                    print(
                        f"  best_tier1_VWAP_buy_edge={best_vwap_buy_edge:.6f} "
                        f"market={best_vwap_buy_mid} qty={best_vwap_buy_qty} "
                        f"buy_cost={fmt(best_vwap_buy_cost, 4)}"
                    )
                if best_vwap_sell_mid is not None:
                    print(
                        f"  best_tier1_VWAP_sell_edge={best_vwap_sell_edge:.6f} "
                        f"market={best_vwap_sell_mid} qty={best_vwap_sell_qty} "
                        f"sell_rev={fmt(best_vwap_sell_rev, 4)}"
                    )

        # ---- TIER 2 SCAN (candidate YES basket across many markets) ----
        best_t2_edge = float("-inf")
        best_t2_type = None
        best_t2_qty = None

        try:
            opps = find_candidate_yes_basket_opportunities(
                store=store,
                basket_id="YES_BASKET",
                yes_token_ids=yes_tokens,
                qty=TIER2_SCAN_QTY,
                cfg=tier2_cfg,
                ts_ms=now,
            )
        except Exception:
            opps = []

        if opps:
            for opp in opps:
                if opp.edge_per_unit > best_t2_edge:
                    best_t2_edge = opp.edge_per_unit
                    best_t2_type = opp.opp_type
                    best_t2_qty = opp.suggested_qty

                print("\n*** TIER2 YES-BASKET OPPORTUNITY DETECTED ***")
                print(
                    f"basket=YES_BASKET type={opp.opp_type} edge_per_unit={opp.edge_per_unit:.6f} "
                    f"qty={opp.suggested_qty} nlegs={len(opp.legs)}"
                )
                if opp.note:
                    print(f"note={opp.note}")

                last_t = last_trade_ms_by_market.get("YES_BASKET", 0)
                if now - last_t < TRADE_COOLDOWN_MS:
                    print("  [cooldown] skipping trade (recent fill)")
                    continue

                plan = planner.build_plan(opp, now_ts_ms=now)
                print(f"plan_status={plan.status} reason={plan.reject_reason}")

                if EXECUTE_PAPER and plan.status.value == "validated":
                    plan = executor.execute(plan)

                    if plan.status.value == "filled":
                        with TRADES_CSV.open("a", newline="") as f:
                            w = csv.writer(f)
                            w.writerow([
                                now,
                                getattr(plan, "market_id", ""),
                                getattr(plan, "opp_type", ""),
                                f"{plan.filled_notional:.2f}",
                                f"{plan.realized_profit:.6f}",
                                f"{executor.portfolio.cash:.2f}",
                                f"{executor.portfolio.realized_pnl:.2f}",
                            ])
                        last_trade_ms_by_market["YES_BASKET"] = now

                    print(
                        f"EXECUTED(PAPER) status={plan.status} profit_est={plan.realized_profit:.6f} "
                        f"notional={plan.filled_notional:.2f}"
                    )
                    print(
                        f"portfolio cash={portfolio.cash:.2f} realized_pnl={portfolio.realized_pnl:.2f} "
                        f"positions={portfolio.snapshot()}"
                    )

        if do_print:
            if best_t2_type is not None:
                print(
                    f"  [tier2 yes-basket best] edge_per_unit={best_t2_edge:.6f} "
                    f"type={best_t2_type} qty={best_t2_qty}"
                )
            else:
                print("  [tier2 yes-basket best] none_seen (no signals)")

        # ---- TIER 2 SCAN (FULL-SET arb in multi-outcome markets N>2) ----
        best_fs_edge = float("-inf")
        best_fs_mid = None
        best_fs_n = 0
        best_fs_cost = None
        
        for mid in market_ids:
            toks = store.token_ids_for_market(mid)
            if len(toks) < tier2_fs_cfg.min_legs:
                continue
        
            # quick book completeness gate
            ok = True
            for tid in toks:
                b = store.get_book(tid)
                if not b or b.best_ask() is None:
                    ok = False
                    break
            if not ok:
                continue
        
            opps = find_tier2_full_set_opportunities(
                store=store,
                market_id=mid,
                token_ids=toks,
                qty=TIER2_FULLSET_QTY,
                cfg=tier2_fs_cfg,
                ts_ms=now,
            )
        
            # Track best seen edge for printing even if below planner/execute gating
            # (If opps returned, edge is >= cfg.min_edge by definition)
            if opps:
                opp = opps[0]
                if opp.edge_per_unit > best_fs_edge:
                    best_fs_edge = opp.edge_per_unit
                    best_fs_mid = mid
                    best_fs_n = len(toks)
                    # parse cost from note (or just ignore); we’ll keep a rough best print
                    best_fs_cost = None
        
                print("\n*** TIER2 FULL-SET OPPORTUNITY DETECTED ***")
                print(f"market={mid} type={opp.opp_type} edge_per_unit={opp.edge_per_unit:.6f} qty={opp.suggested_qty} nlegs={len(opp.legs)}")
                if opp.note:
                    print(f"note={opp.note}")
        
                plan = planner.build_plan(opp, now_ts_ms=now)
                print(f"plan_status={plan.status} reason={plan.reject_reason}")
        
                if EXECUTE_PAPER and plan.status.value == "validated":
                    plan = executor.execute(plan)
                    print(f"EXECUTED(PAPER) status={plan.status} profit_est={plan.realized_profit:.6f} notional={plan.filled_notional:.2f}")
                    print(f"portfolio cash={portfolio.cash:.2f} realized_pnl={portfolio.realized_pnl:.2f} positions={portfolio.snapshot()}")

        # Always show paper snapshot (matches your existing behavior)
        if EXECUTE_PAPER:
            if do_print:
                if best_fs_mid is not None:
                    print(f"  [tier2 full-set best] edge_per_unit={best_fs_edge:.6f} market={best_fs_mid} nlegs={best_fs_n}")
                else:
                    print("  [tier2 full-set best] none_seen (no N>2 markets w/ complete asks yet)")
        else:
            if do_print:
                print("\n[live] scanning... (set EXECUTE_PAPER=1 to paper-execute validated plans)")

# --------- main wiring ---------

async def main() -> None:
    # 1) Fetch markets universe (Gamma)
    markets: List[MarketInfo] = await get_active_markets_universe(
        limit_total=1000,
        liquidity_min=100.0,
        volume_min=100.0,
    )

    if not markets:
        raise RuntimeError("No markets returned from Gamma.")

    asset_ids: List[str] = []
    yes_tokens: List[str] = []
    token_to_market: dict[str, str] = {}

    for m in markets:
        for tid in m.clob_token_ids:
            tid = str(tid)
            if tid:
                asset_ids.append(tid)
                token_to_market[tid] = str(m.condition_id)  # WS market_id is conditionId

        if m.yes_token_id:
            yes_tokens.append(str(m.yes_token_id))

    asset_ids = dedupe_keep_order([str(x) for x in asset_ids if x])
    yes_tokens = dedupe_keep_order([str(x) for x in yes_tokens if x])

    print(f"Loaded {len(markets)} markets, subscribing to {len(asset_ids)} asset_ids")
    for i, m in enumerate(markets, 1):
        print(f"  {i:02d}. conditionId={m.condition_id} tokens={len(m.clob_token_ids)} question={m.question}")

    print(f"\nTier2 YES basket tokens: {len(yes_tokens)}")

    # 2) REST bootstrap snapshots into BookStore
    q: asyncio.Queue = asyncio.Queue(maxsize=100_000)
    store = BookStore()

    async with httpx.AsyncClient(timeout=20.0) as client:
        snaps = await fetch_books_snapshots(
            client=client,
            token_ids=asset_ids,
            token_to_market=token_to_market,
        )

    applied = 0
    for s in snaps:
        ev = BookSnapshotEvent(
            type=EventType.BOOK_SNAPSHOT,
            market_id=s.market_id,
            token_id=s.token_id,
            bids=s.bids,
            asks=s.asks,
            ts_ms=s.ts_ms,
            seq=None,
        )
        store.apply(ev)
        applied += 1

    print(f"REST bootstrap applied snapshots: {applied}/{len(asset_ids)}")

    print("REST bootstrap sample (first 10 asset_ids):")
    for tid in asset_ids[:10]:
        b = store.get_book(tid)
        if not b:
            print("  missing", tid)
            continue
        bb = b.best_bid()
        ba = b.best_ask()
        print(" ", tid, "bid/ask=", (bb.price if bb else None), "/", (ba.price if ba else None))

    print("YES tokens missing after bootstrap:")
    for tid in yes_tokens:
        if store.get_book(tid) is None:
            print("  missing YES", tid)

    # 3) WS task: stream updates into queue
    ws_task = asyncio.create_task(
        stream_market_channel_events(
            asset_ids=asset_ids,
            out_queue=q,
            cfg=WsConfig(custom_feature_enabled=False),
            subscribe_chunk_size=50,  # chunk smaller to get faster initial fills in live testing (adjust as needed)
        )
    )

    # 4) Apply loop: queue -> store
    async def apply_loop() -> None:
        while True:
            ev = await q.get()
            store.apply(ev)
            q.task_done()

    apply_task = asyncio.create_task(apply_loop())

    # 5) Scanner loop
    scan_task = asyncio.create_task(event_consumer(store, yes_tokens))

    await asyncio.gather(ws_task, apply_task, scan_task)

if __name__ == "__main__":
    asyncio.run(main())
