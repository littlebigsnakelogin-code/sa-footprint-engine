import encodings.idna

import json
import os
import queue
import threading
import time
import uuid
from collections import defaultdict, deque
from contextlib import contextmanager
from itertools import islice
import requests
import websocket
from flask import Flask, jsonify, request
from libsql_client import create_client_sync


# ============================================================
# TURSO DATABASE
# ============================================================

TURSO_DATABASE_URL = os.environ.get("TURSO_DATABASE_URL")
TURSO_AUTH_TOKEN = os.environ.get("TURSO_AUTH_TOKEN")

if not TURSO_DATABASE_URL or not TURSO_AUTH_TOKEN:
    print("[TURSO] Environment variables missing")

# ============================================================
# TURSO BACKGROUND WORKER
# ============================================================
turso_write_queue = queue.Queue()
turso_worker_started = False
turso_worker_lock = threading.Lock()
turso_client = None

last_turso_cleanup = 0

app = Flask(__name__)

# ============================================================
# CONFIG
# ============================================================

SYMBOLS = [
    "BTCUSDT",
    "ETHUSDT",
    "SOLUSDT",
    "XRPUSDT",
    "AVAXUSDT",
    "LINKUSDT",
    "LTCUSDT",
]


TIMEFRAMES = {
    "1m": 60,
    "3m": 180,
    "5m": 300,
    "15m": 900,
    "30m": 1800,
    "1h": 3600,
    "4h": 14400,
    "1d": 86400,
}


# Keep 24 hours of RAM + Turso data
ROLLING_SECONDS = 24 * 60 * 60


# ============================================================
# FOOTPRINT PRICE STEPS
# ============================================================

PRICE_STEP = {
    "BTCUSDT": 1.0,
    "ETHUSDT": 0.1,
    "SOLUSDT": 0.01,
    "XRPUSDT": 0.0001,
    "AVAXUSDT": 0.01,
    "LINKUSDT": 0.001,
    "LTCUSDT": 0.01,
}


# ============================================================
# MEMORY
# ============================================================

lock = threading.RLock()
# Serialize mutable collector state per symbol; busy books must not block peers.
symbol_locks = {symbol: threading.RLock() for symbol in SYMBOLS}
collector_start_lock = threading.Lock()

# Instrument only hot-path lock sections so logs separate lock contention
# from actual processing time. The measured context preserves RLock semantics.
_lock_perf_guard = threading.Lock()
_lock_perf = {}


@contextmanager
def measured_lock(label, symbol=None):
    wait_started = time.perf_counter()
    active_lock = symbol_locks.get(symbol, lock) if symbol else lock
    active_lock.acquire()
    acquired_at = time.perf_counter()
    try:
        yield
    finally:
        released_at = time.perf_counter()
        active_lock.release()

        key = (label, symbol or "ALL")
        wait_ms = (acquired_at - wait_started) * 1000.0
        hold_ms = (released_at - acquired_at) * 1000.0

        with _lock_perf_guard:
            stats = _lock_perf.setdefault(key, {
                "count": 0,
                "wait_total_ms": 0.0,
                "wait_max_ms": 0.0,
                "hold_total_ms": 0.0,
                "hold_max_ms": 0.0,
            })
            stats["count"] += 1
            stats["wait_total_ms"] += wait_ms
            stats["wait_max_ms"] = max(stats["wait_max_ms"], wait_ms)
            stats["hold_total_ms"] += hold_ms
            stats["hold_max_ms"] = max(stats["hold_max_ms"], hold_ms)

            if stats["count"] >= 100:
                count = stats["count"]
                print(
                    f"[LOCK PERF] section={label} symbol={symbol or 'ALL'} "
                    f"n={count} "
                    f"wait_avg_ms={stats['wait_total_ms'] / count:.2f} "
                    f"wait_max_ms={stats['wait_max_ms']:.2f} "
                    f"hold_avg_ms={stats['hold_total_ms'] / count:.2f} "
                    f"hold_max_ms={stats['hold_max_ms']:.2f}"
                )
                stats.update({
                    "count": 0,
                    "wait_total_ms": 0.0,
                    "wait_max_ms": 0.0,
                    "hold_total_ms": 0.0,
                    "hold_max_ms": 0.0,
                })

# Binance Futures snapshot requests ko ek-ek karke execute karenge.
# Isse startup par 7 simultaneous WS-FAPI connections ke
# stuck/zombie hone ka risk control hota hai.
snapshot_ws_lock = threading.Lock()

# Finished candles stored in RAM
candles = {
    symbol: {
        timeframe: deque()
        for timeframe in TIMEFRAMES
    }
    for symbol in SYMBOLS
}


# Current unfinished candles
current_candles = {
    symbol: {
        timeframe: None
        for timeframe in TIMEFRAMES
    }
    for symbol in SYMBOLS
}


# ============================================================
# COLLECTOR STATE
# ============================================================

collector_state = {
    "status": "starting",
    "connected": False,
    "error": None,
    "started_at": None,
    "last_message_at": None,
    "raw_message_count": 0,
    "trade_message_count": 0,
    "invalid_message_count": 0,
}


# ============================================================
# LAST TRADE & ORDERBOOK
# ============================================================

last_trade = {
    symbol: {
        "time": None,
        "price": None,
        "quantity": None,
    }
    for symbol in SYMBOLS
}

orderbook = {
    symbol: {
        "bids": {},
        "asks": {},

        # Binance Futures depth synchronization state
        "last_update_id": None,
        "initialized": False,
        "resyncing": False,
        "buffer": deque(),

        # ====================================================
        # LIQUIDITY HISTORY
        # ====================================================

        "liquidity_history": deque(maxlen=3000),

        # ====================================================
        # FIFO LIQUIDITY LEDGER
        #
        # Har price level par liquidity ko FIFO lots ke form
        # mein track karenge.
        #
        # Example:
        #
        # bid 80000:
        #   Lot 1 -> old liquidity
        #   Lot 2 -> newly added liquidity
        #
        # Execution / deletion mein pehle Lot 1 consume hoga.
        # ====================================================

        "liquidity_lots": {
            "bid": defaultdict(deque),
            "ask": defaultdict(deque),
        },

        # Recent aggressive trades
        "trade_history": deque(maxlen=2000),

        # Diagnostics
        "last_depth_event_time": None,
        "last_depth_update_id": None,
        "sequence_errors": 0,
        "resync_count": 0,
    }
    for symbol in SYMBOLS
}
collector_thread = None
# ============================================================
# HELPERS
# ============================================================

def now_ms():
    return int(time.time() * 1000)


def floor_timestamp(ts, seconds):
    """
    Binance timestamp milliseconds mein hota hai.
    Candle timestamp bhi milliseconds mein rakha jayega.
    """

    return int(
        ts // (seconds * 1000)
    ) * (seconds * 1000)


def round_price_to_step(price, step):
    """
    Convert a price into its footprint price bucket using
    decimal arithmetic to avoid binary floating-point errors.
    """

    from decimal import Decimal, ROUND_FLOOR

    try:
        price_decimal = Decimal(str(price))
        step_decimal = Decimal(str(step))

        if not price_decimal.is_finite():
            return price

        if not step_decimal.is_finite() or step_decimal <= 0:
            return price

        bucket_count = (
            price_decimal / step_decimal
        ).to_integral_value(rounding=ROUND_FLOOR)

        result = bucket_count * step_decimal

        return float(result)

    except (ArithmeticError, TypeError, ValueError):
        return price


def create_empty_candle(
    symbol,
    timeframe,
    start,
    open_price,
):

    return {
        "symbol": symbol,
        "timeframe": timeframe,

        "start": start,

        "end": (
            start
            + TIMEFRAMES[timeframe] * 1000
        ),

        "open": open_price,
        "high": open_price,
        "low": open_price,
        "close": open_price,

        "volume": 0.0,
        "buy_volume": 0.0,
        "sell_volume": 0.0,
        "delta": 0.0,

        "trades": 0,

        "footprint": {},
    }


# ============================================================
# TRADE -> CANDLE
# ============================================================

def add_trade_to_candle(
    candle,
    price,
    quantity,
    is_buyer_maker,
):

    candle["high"] = max(
        candle["high"],
        price
    )

    candle["low"] = min(
        candle["low"],
        price
    )

    candle["close"] = price

    candle["volume"] += quantity

    candle["trades"] += 1

    if is_buyer_maker:

        # Aggressive SELL
        candle["sell_volume"] += quantity

    else:

        # Aggressive BUY
        candle["buy_volume"] += quantity


    candle["delta"] = (
        candle["buy_volume"]
        - candle["sell_volume"]
    )


    # ========================================================
    # FOOTPRINT PRICE LEVEL
    # ========================================================

    symbol = candle["symbol"]

    step = PRICE_STEP[symbol]

    price_level = round_price_to_step(
        price,
        step
    )

    key = str(price_level)

    level = candle["footprint"].get(key)


    if level is None:

        level = {
            "price": price_level,
            "buy": 0.0,
            "sell": 0.0,
            "delta": 0.0,
            "volume": 0.0,
            "trades": 0,
        }

        candle["footprint"][key] = level


    if is_buyer_maker:

        level["sell"] += quantity

    else:

        level["buy"] += quantity


    level["volume"] += quantity

    level["trades"] += 1

    level["delta"] = (
        level["buy"]
        - level["sell"]
    )

# ============================================================
# BINANCE FUTURES ORDERBOOK SYNCHRONIZATION
# ============================================================

BINANCE_FUTURES_DEPTH_URL = (
    "https://fapi.binance.com/fapi/v1/depth"
)

ORDERBOOK_SNAPSHOT_LIMIT = 1000


def fetch_orderbook_snapshot_ws(symbol):
    SNAPSHOT_DEADLINE = 20
    SOCKET_TIMEOUT = 5

    ws = None
    started_at = time.time()

    try:
        ws_url = "wss://ws-fapi.binance.com/ws-fapi/v1"

        print(
            f"[SNAPSHOT] CONNECTING {symbol}"
        )

        ws = websocket.create_connection(
            ws_url,
            timeout=SOCKET_TIMEOUT
        )

        print(
            f"[SNAPSHOT] CONNECTED {symbol} "
            f"({time.time() - started_at:.2f}s)"
        )

        try:
            ws.settimeout(
                SOCKET_TIMEOUT
            )
        except Exception:
            pass

        request_id = (
            int(time.time() * 1000)
            % 1000000000
        )

        request = {
            "id": request_id,
            "method": "depth",
            "params": {
                "symbol": symbol,
                "limit": ORDERBOOK_SNAPSHOT_LIMIT
            }
        }

        ws.send(
            json.dumps(request)
        )

        print(
            f"[SNAPSHOT] REQUEST SENT {symbol}"
        )

        deadline = (
            time.time()
            + SNAPSHOT_DEADLINE
        )

        while time.time() < deadline:

            remaining = (
                deadline
                - time.time()
            )

            if remaining <= 0:
                break

            try:
                ws.settimeout(
                    min(
                        SOCKET_TIMEOUT,
                        max(
                            0.5,
                            remaining
                        )
                    )
                )
            except Exception:
                pass

            try:
                raw = ws.recv()

            except websocket.WebSocketTimeoutException:
                print(
                    f"[SNAPSHOT] "
                    f"WAITING RESPONSE {symbol}"
                )
                continue

            except Exception as e:
                print(
                    f"[SNAPSHOT] RECV ERROR {symbol}: "
                    f"{type(e).__name__}: {e}"
                )
                return None

            if not raw:
                continue

            try:
                response = json.loads(
                    raw
                )

            except Exception as e:
                print(
                    f"[SNAPSHOT] JSON ERROR {symbol}: "
                    f"{type(e).__name__}: {e}"
                )
                continue

            if response.get("id") != request_id:
                continue

            status = response.get(
                "status"
            )

            if status != 200:
                print(
                    f"[SNAPSHOT] API ERROR {symbol}: "
                    f"status={status} "
                    f"response={response}"
                )
                return None

            result = response.get(
                "result"
            )

            if not isinstance(
                result,
                dict
            ):
                print(
                    f"[SNAPSHOT] INVALID RESULT {symbol}: "
                    f"{response}"
                )
                return None

            last_update_id = result.get(
                "lastUpdateId"
            )

            bids = result.get(
                "bids"
            )

            asks = result.get(
                "asks"
            )

            if (
                last_update_id is None
                or not isinstance(
                    bids,
                    list
                )
                or not isinstance(
                    asks,
                    list
                )
            ):
                print(
                    f"[SNAPSHOT] "
                    f"INVALID SNAPSHOT {symbol}: "
                    f"lastUpdateId="
                    f"{last_update_id} "
                    f"bids="
                    f"{type(bids).__name__} "
                    f"asks="
                    f"{type(asks).__name__}"
                )
                return None

            elapsed = (
                time.time()
                - started_at
            )

            print(
                f"[SNAPSHOT] RESPONSE OK {symbol} "
                f"lastUpdateId={last_update_id} "
                f"bids={len(bids)} "
                f"asks={len(asks)} "
                f"({elapsed:.2f}s)"
            )

            return result

        print(
            f"[SNAPSHOT] TIMEOUT {symbol} "
            f"after "
            f"{time.time() - started_at:.2f}s"
        )

        return None

    except websocket.WebSocketTimeoutException:
        print(
            f"[SNAPSHOT] "
            f"CONNECTION/RECV TIMEOUT {symbol} "
            f"after "
            f"{time.time() - started_at:.2f}s"
        )
        return None

    except Exception as e:
        print(
            f"[SNAPSHOT] ERROR {symbol}: "
            f"{type(e).__name__}: {e}"
        )
        return None

    finally:

        if ws is not None:
            try:
                ws.close()
                print(
                    f"[SNAPSHOT] CLOSED {symbol}"
                )
            except Exception:
                pass


def fetch_orderbook_snapshot(symbol):
    """
    Binance Futures REST snapshot fetch karta hai.

    Snapshot ke saath bids, asks aur lastUpdateId milta hai.
    """
    try:
        response = requests.get(
            BINANCE_FUTURES_DEPTH_URL,
            params={
                "symbol": symbol,
                "limit": ORDERBOOK_SNAPSHOT_LIMIT,
            },
            timeout=10,
        )

        response.raise_for_status()

        data = response.json()

        if "lastUpdateId" not in data:
            raise RuntimeError(
                f"Invalid Binance snapshot for {symbol}: "
                f"missing lastUpdateId"
            )

        return data

    except Exception as exc:
        print(
            f"[ORDERBOOK] Snapshot failed "
            f"{symbol}: {exc}"
        )
        return None


def finalize_liquidity_records(symbol, current_time_ms=None):
    """Finalize expired reductions and amortize index cleanup work.

    Accounting and the 1500 ms matching window remain unchanged. With live
    depth events, the expensive history/index sweeps are rate-limited per
    symbol: finalization is checked at most every 250 ms, while full index
    cleanup runs at most every 1000 ms. Calls without an event timestamp
    perform a full sweep immediately (debug/manual callers).
    """
    if symbol not in orderbook:
        return 0

    has_event_time = current_time_ms is not None
    if current_time_ms is None:
        current_time_ms = now_ms()

    state = orderbook[symbol]
    liquidity_history = state.get("liquidity_history", ())

    # Throttle expensive sweeps using local monotonic processing time,
    # not exchange event time. Backlogged/replayed events can have event
    # timestamps older than the previous event; using those timestamps here
    # would disable the throttle and repeatedly scan the full history.
    sweep_clock_ms = time.monotonic_ns() // 1_000_000
    last_finalize_sweep = state.get("_last_finalize_sweep_monotonic_ms")
    if (
        has_event_time
        and last_finalize_sweep is not None
        and 0 <= sweep_clock_ms - last_finalize_sweep < 250
    ):
        return 0

    state["_last_finalize_sweep_monotonic_ms"] = sweep_clock_ms
    finalized_count = 0

    # Only reductions younger than the 1500 ms matching window need to be
    # examined on the hot depth-event path. Scanning all 3000 history records
    # every 250 ms held the shared collector lock for long periods.
    pending_finalize = state.setdefault(
        "_pending_finalize_records",
        deque(),
    )

    if has_event_time:
        # New reductions are appended in apply_orderbook_event() order.
        # Keep not-yet-expired records queued; finalize eligible ones in place.
        retained_pending = deque()
        while pending_finalize:
            record = pending_finalize.popleft()
            if not isinstance(record, dict) or record.get("finalized"):
                continue

            try:
                reduced_qty = float(record.get("reduced_qty", 0.0))
                if reduced_qty <= 0.0:
                    continue
                record_time = int(record.get("time", current_time_ms))
                if current_time_ms - record_time < 1500:
                    retained_pending.append(record)
                    continue
                executed_qty = float(record.get("executed_qty", 0.0))
            except (TypeError, ValueError, OverflowError):
                continue

            executed_qty = max(0.0, min(reduced_qty, executed_qty))
            unmatched_qty = reduced_qty - executed_qty
            epsilon = max(reduced_qty, executed_qty) * 1e-12

            if abs(unmatched_qty) <= epsilon:
                executed_qty = reduced_qty
                unmatched_qty = 0.0
            else:
                unmatched_qty = max(0.0, unmatched_qty)

            pulled_qty = unmatched_qty
            record["executed_qty"] = executed_qty
            record["unmatched_qty"] = unmatched_qty
            record["pulled_qty"] = pulled_qty
            record["pull_pct"] = (pulled_qty / reduced_qty) * 100.0
            record["finalized"] = True

            if executed_qty > 0.0 and pulled_qty > 0.0:
                record["status"] = "PARTIAL_EXECUTION_PULL"
            elif executed_qty > 0.0:
                record["status"] = "EXECUTED"
            elif pulled_qty > 0.0:
                record["status"] = "LIQUIDITY_PULLED"
            else:
                record["status"] = "REDUCTION_ZERO"
            finalized_count += 1

        pending_finalize.extend(retained_pending)
    else:
        # Explicit debug/manual calls retain full-history recovery semantics.
        for record in liquidity_history:
            if not isinstance(record, dict) or record.get("finalized"):
                continue

            try:
                reduced_qty = float(record.get("reduced_qty", 0.0))
                if reduced_qty <= 0.0:
                    continue
                record_time = int(record.get("time", current_time_ms))
                if current_time_ms - record_time < 1500:
                    continue
                executed_qty = float(record.get("executed_qty", 0.0))
            except (TypeError, ValueError, OverflowError):
                continue

            executed_qty = max(0.0, min(reduced_qty, executed_qty))
            unmatched_qty = reduced_qty - executed_qty
            epsilon = max(reduced_qty, executed_qty) * 1e-12

            if abs(unmatched_qty) <= epsilon:
                executed_qty = reduced_qty
                unmatched_qty = 0.0
            else:
                unmatched_qty = max(0.0, unmatched_qty)

            pulled_qty = unmatched_qty
            record["executed_qty"] = executed_qty
            record["unmatched_qty"] = unmatched_qty
            record["pulled_qty"] = pulled_qty
            record["pull_pct"] = (pulled_qty / reduced_qty) * 100.0
            record["finalized"] = True

            if executed_qty > 0.0 and pulled_qty > 0.0:
                record["status"] = "PARTIAL_EXECUTION_PULL"
            elif executed_qty > 0.0:
                record["status"] = "EXECUTED"
            elif pulled_qty > 0.0:
                record["status"] = "LIQUIDITY_PULLED"
            else:
                record["status"] = "REDUCTION_ZERO"
            finalized_count += 1

    # Live cleanup is incremental: never copy every index key or rebuild
    # every indexed deque while holding the collector's shared lock.
    last_index_cleanup = state.get(
        "_last_match_index_cleanup_monotonic_ms"
    )
    do_index_cleanup = (
        not has_event_time
        or last_index_cleanup is None
        or sweep_clock_ms - last_index_cleanup >= 1000
    )

    if not do_index_cleanup:
        return finalized_count

    state["_last_match_index_cleanup_monotonic_ms"] = sweep_clock_ms
    liquidity_index = state.get("liquidity_match_index")
    trade_index = state.get("trade_match_index")

    if not has_event_time:
        # Explicit debug/manual recovery keeps the previous full-sweep
        # semantics. This path is not used by live depth events.
        if isinstance(liquidity_index, dict):
            history_ids = {
                id(record)
                for record in liquidity_history
                if isinstance(record, dict)
            }
            for key, records in list(liquidity_index.items()):
                retained_records = deque(
                    record
                    for record in records
                    if (
                        isinstance(record, dict)
                        and id(record) in history_ids
                        and not record.get("finalized")
                    )
                )
                if retained_records:
                    liquidity_index[key] = retained_records
                else:
                    liquidity_index.pop(key, None)

        if isinstance(trade_index, dict):
            for key, trades in list(trade_index.items()):
                retained_trades = deque()
                for trade in trades:
                    if not isinstance(trade, dict):
                        continue
                    try:
                        trade_time = int(trade.get("time"))
                        remaining_qty = float(
                            trade.get("remaining_qty", trade.get("quantity", 0.0))
                        )
                    except (TypeError, ValueError, OverflowError):
                        continue
                    if remaining_qty <= 1e-12:
                        continue
                    retained_trades.append(trade)
                if retained_trades:
                    trade_index[key] = retained_trades
                else:
                    trade_index.pop(key, None)

        # Keep the incremental cursors consistent after a manual recovery.
        if isinstance(liquidity_index, dict):
            keys = deque(liquidity_index.keys())
            state["_liquidity_match_cleanup_keys"] = keys
            state["_liquidity_match_cleanup_queued"] = set(keys)
        if isinstance(trade_index, dict):
            keys = deque(trade_index.keys())
            state["_trade_match_cleanup_keys"] = keys
            state["_trade_match_cleanup_queued"] = set(keys)
        return finalized_count

    trade_cutoff = current_time_ms - 300
    max_keys_per_sweep = 128
    max_records_per_key = 64

    def cleanup_index_incrementally(
        index,
        queue_name,
        queued_name,
        is_liquidity_index,
    ):
        if not isinstance(index, dict):
            return

        cleanup_keys = state.setdefault(queue_name, deque())
        queued_keys = state.setdefault(queued_name, set())
        processed_keys = 0

        while cleanup_keys and processed_keys < max_keys_per_sweep:
            key = cleanup_keys.popleft()
            queued_keys.discard(key)
            processed_keys += 1

            records = index.get(key)
            if not records:
                index.pop(key, None)
                continue

            inspected = 0
            # Index entries are appended in stream order. Removing only stale
            # entries at the deque head preserves the order and all live
            # candidates needed by exact-price execution matching.
            while records and inspected < max_records_per_key:
                record = records[0]
                if not isinstance(record, dict):
                    records.popleft()
                    inspected += 1
                    continue

                if is_liquidity_index:
                    stale = bool(record.get("finalized"))
                else:
                    try:
                        record_time = int(record.get("time"))
                        remaining_qty = float(
                            record.get(
                                "remaining_qty",
                                record.get("quantity", 0.0),
                            )
                        )
                    except (TypeError, ValueError, OverflowError):
                        stale = True
                    else:
                        stale = (
                            remaining_qty <= 1e-12
                            or record_time < trade_cutoff
                        )

                if not stale:
                    break

                records.popleft()
                inspected += 1

            if records:
                if key not in queued_keys:
                    cleanup_keys.append(key)
                    queued_keys.add(key)
            else:
                index.pop(key, None)

    cleanup_index_incrementally(
        liquidity_index,
        "_liquidity_match_cleanup_keys",
        "_liquidity_match_cleanup_queued",
        True,
    )
    cleanup_index_incrementally(
        trade_index,
        "_trade_match_cleanup_keys",
        "_trade_match_cleanup_queued",
        False,
    )

    return finalized_count


def record_trade_for_execution_matching(
    symbol,
    price,
    quantity,
    trade_time,
    is_buyer_maker,
):
    """
    Record an aggressive market trade and match it against
    liquidity reductions at the same price/side.

    bid reduction -> aggressive SELL (buyer_maker=True)
    ask reduction -> aggressive BUY  (buyer_maker=False)

    Supports trade-before-depth and depth-before-trade.

    FIFO lots are consumed by depth reductions only.
    Trades update execution attribution on existing FIFO
    consumption entries; they never consume lots again.
    """

    if symbol not in orderbook:
        return 0.0

    try:
        price = float(price)
        quantity = float(quantity)
        trade_time = int(trade_time)
        is_buyer_maker = bool(is_buyer_maker)
    except (TypeError, ValueError):
        return 0.0

    if price <= 0.0 or quantity <= 0.0:
        return 0.0

    state = orderbook[symbol]

    diagnostic_history = state.setdefault(
        "trade_flow_diagnostics",
        deque(maxlen=300),
    )

    expected_side = "bid" if is_buyer_maker else "ask"
    price_key = round(price, 12)

    trade_match_index = state.setdefault(
        "trade_match_index",
        defaultdict(deque),
    )

    liquidity_match_index = state.setdefault(
        "liquidity_match_index",
        defaultdict(deque),
    )

    same_price_trade_key = (expected_side, price_key)

    existing_same_price_trade_deque = trade_match_index.get(
        same_price_trade_key, ()
    )
    existing_same_price_trade_count = len(
        existing_same_price_trade_deque
    )

    # Diagnostics need only the latest ten examples. Do not copy the
    # complete same-price trade deque on every incoming market trade.
    existing_same_price_trades = list(
        reversed(list(islice(
            reversed(existing_same_price_trade_deque), 10
        )))
    )

    existing_same_price_trade_snapshot = []

    for existing_trade in existing_same_price_trades:
        try:
            existing_trade_time = int(existing_trade.get("time"))
        except (TypeError, ValueError):
            continue

        existing_same_price_trade_snapshot.append({
            "time": existing_trade_time,
            "price": float(existing_trade.get("price", price)),
            "quantity": float(existing_trade.get("quantity", 0.0)),
            "remaining_qty": float(
                existing_trade.get("remaining_qty", 0.0)
            ),
            "time_diff_ms": trade_time - existing_trade_time,
        })

    same_price_liquidity = liquidity_match_index.get(
        same_price_trade_key, ()
    )
    same_price_liquidity_count = len(same_price_liquidity)

    pending_liquidity_snapshot = []

    # As above, snapshot only the latest ten entries for diagnostics;
    # the execution matcher below continues using the full indexed deque.
    for liquidity in reversed(list(islice(
        reversed(same_price_liquidity), 10
    ))):
        if liquidity.get("finalized"):
            continue

        try:
            liquidity_time = int(liquidity.get("time", 0))
            reduced_qty = float(liquidity.get("reduced_qty", 0.0))
            executed_qty = float(liquidity.get("executed_qty", 0.0))
            liquidity_price = float(liquidity.get("price", price))
        except (TypeError, ValueError):
            continue

        pending_liquidity_snapshot.append({
            "time": liquidity_time,
            "price": liquidity_price,
            "reduced_qty": reduced_qty,
            "executed_qty": executed_qty,
            "unmatched_qty": max(
                0.0,
                reduced_qty - executed_qty,
            ),
            "status": liquidity.get("status"),
            "time_diff_ms": trade_time - liquidity_time,
        })

    diagnostic_history.append({
        "event": "TRADE_ARRIVAL",
        "time": trade_time,
        "symbol": symbol,
        "price": price,
        "quantity": quantity,
        "is_buyer_maker": is_buyer_maker,
        "expected_side": expected_side,
        "trade_index_key": [expected_side, price_key],
        "trade_index_key_count_before": existing_same_price_trade_count,
        "pending_liquidity_key_count_before": same_price_liquidity_count,
        "existing_same_price_trades": (
            existing_same_price_trade_snapshot
        ),
        "pending_same_price_liquidity": (
            pending_liquidity_snapshot
        ),
        "trade_history_count_before": len(
            state.get("trade_history", ())
        ),
        "liquidity_history_count": len(
            state.get("liquidity_history", ())
        ),
        "last_depth_event_time": state.get("last_depth_event_time"),
        "last_depth_update_id": state.get("last_depth_update_id"),
    })

    trade_record = {
        "time": trade_time,
        "price": price,
        "quantity": quantity,
        "remaining_qty": quantity,
        "is_buyer_maker": is_buyer_maker,
    }

    # Register a price key once so stale trade-index entries can be
    # cleaned incrementally without copying every index key.
    trade_index_key = (expected_side, price_key)
    if trade_index_key not in trade_match_index:
        cleanup_keys = state.setdefault(
            "_trade_match_cleanup_keys", deque()
        )
        queued_keys = state.setdefault(
            "_trade_match_cleanup_queued", set()
        )
        if trade_index_key not in queued_keys:
            cleanup_keys.append(trade_index_key)
            queued_keys.add(trade_index_key)

    # Index this trade so later depth reductions can match it.
    trade_match_index[trade_index_key].append(trade_record)

    # All live reductions are indexed atomically by apply_orderbook_event()
    # before the shared lock is released. Scanning the entire 3000-record
    # history and rebuilding every index key for every trade is redundant
    # and caused O(history + index) work on the hottest trade path.

    matching_records = liquidity_match_index.get(
        (expected_side, price_key),
        (),
    )

    matched_total = 0.0
    epsilon = 1e-12

    for liquidity in matching_records:
        if trade_record["remaining_qty"] <= epsilon:
            break

        if liquidity.get("finalized"):
            continue

        try:
            reduced_qty = float(
                liquidity.get("reduced_qty", 0.0)
            )
            executed_qty = float(
                liquidity.get("executed_qty", 0.0)
            )
            liquidity_time = int(
                liquidity.get("time", trade_time)
            )
            liquidity_price = float(
                liquidity.get("price", price)
            )
        except (TypeError, ValueError):
            continue

        if reduced_qty <= 0.0:
            continue

        remaining_reduction = max(
            0.0,
            reduced_qty - executed_qty,
        )

        if remaining_reduction <= epsilon:
            continue

        time_difference = trade_time - liquidity_time

        # Permit a trade up to 300 ms before reduction
        # and up to 1500 ms after reduction.
        if time_difference < -300 or time_difference > 1500:
            continue

        if abs(liquidity_price - price) > 1e-6:
            continue

        available_trade = max(
            0.0,
            float(trade_record.get("remaining_qty", 0.0)),
        )

        matched_qty = min(
            available_trade,
            remaining_reduction,
        )

        if matched_qty <= epsilon:
            continue

        new_executed_qty = min(
            reduced_qty,
            executed_qty + matched_qty,
        )

        actual_matched_qty = max(
            0.0,
            new_executed_qty - executed_qty,
        )

        if actual_matched_qty <= epsilon:
            continue

        liquidity["executed_qty"] = new_executed_qty
        liquidity["unmatched_qty"] = max(
            0.0,
            reduced_qty - new_executed_qty,
        )

        # Pull classification remains provisional until the
        # matching window expires.
        liquidity["pulled_qty"] = 0.0
        liquidity["pull_pct"] = 0.0
        liquidity["status"] = "reduction_pending"

        # FIFO attribution: update existing consumption entries.
        # Never consume liquidity lots from this trade handler.
        fifo_consumption = liquidity.setdefault(
            "fifo_consumption",
            [],
        )

        fifo_remaining = actual_matched_qty

        for item in fifo_consumption:
            if fifo_remaining <= epsilon:
                break

            try:
                consumed_qty = max(
                    0.0,
                    float(item.get("consumed_qty", 0.0)),
                )
                prior_execution_qty = max(
                    0.0,
                    float(item.get("execution_qty", 0.0)),
                )
            except (TypeError, ValueError):
                continue

            attribution_capacity = max(
                0.0,
                consumed_qty - prior_execution_qty,
            )

            if attribution_capacity <= epsilon:
                continue

            attributed_qty = min(
                attribution_capacity,
                fifo_remaining,
            )

            item["execution_qty"] = (
                prior_execution_qty + attributed_qty
            )

            item["unmatched_qty"] = max(
                0.0,
                consumed_qty - item["execution_qty"],
            )

            if attributed_qty > 0.0:
                item["execution_time"] = trade_time
                item["trade_price"] = price

            fifo_remaining -= attributed_qty

        # Track execution that could not be attributed to
        # an existing FIFO consumption entry.
        previous_unattributed = max(
            0.0,
            float(
                liquidity.get(
                    "fifo_unattributed_execution_qty",
                    0.0,
                )
            ),
        )

        if fifo_remaining > epsilon:
            liquidity["fifo_unattributed_execution_qty"] = (
                previous_unattributed + fifo_remaining
            )
        else:
            liquidity["fifo_unattributed_execution_qty"] = (
                previous_unattributed
            )

        # Recalculate FIFO counters from their source entries.
        liquidity["fifo_executed_qty"] = sum(
            max(
                0.0,
                float(item.get("execution_qty", 0.0)),
            )
            for item in fifo_consumption
        )

        # Remaining FIFO-consumed quantity not yet attributed
        # to execution. This is not the same as pulled_qty.
        liquidity["fifo_unmatched_qty"] = sum(
            max(
                0.0,
                float(item.get("consumed_qty", 0.0))
                - float(item.get("execution_qty", 0.0)),
            )
            for item in fifo_consumption
        )

        # Consume the trade quantity exactly once.
        trade_record["remaining_qty"] = max(
            0.0,
            float(trade_record["remaining_qty"])
            - actual_matched_qty,
        )

        matched_total += actual_matched_qty

    state["trade_history"].append(trade_record)

    return matched_total


def match_trade_to_liquidity_reduction(
    symbol,
    liquidity_side,
    liquidity_price,
    reduced_qty,
    reduction_time,
):
    """
    Match aggressive trades to a liquidity reduction.

    Exact-price matches affect executed quantity.
    Nearby trades are diagnostic-only.
    """

    if symbol not in orderbook:
        return 0.0

    try:
        liquidity_price_float = float(liquidity_price)
        reduced_qty_float = float(reduced_qty)
        reduction_time_int = int(reduction_time)
    except (TypeError, ValueError):
        return 0.0

    if liquidity_price_float <= 0 or reduced_qty_float <= 0:
        return 0.0

    if liquidity_side not in ("bid", "ask"):
        return 0.0

    expected_is_buyer_maker = liquidity_side == "bid"
    expected_side = liquidity_side
    price_key = round(liquidity_price_float, 12)

    state = orderbook[symbol]

    # Always normalize the index to defaultdict(deque).
    # Preserve existing indexed trades during conversion.
    trade_index = state.get("trade_match_index")

    if not isinstance(trade_index, defaultdict):
        trade_index = defaultdict(
            deque,
            trade_index if isinstance(trade_index, dict) else {},
        )
        state["trade_match_index"] = trade_index
    elif trade_index.default_factory is not deque:
        trade_index = defaultdict(deque, trade_index)
        state["trade_match_index"] = trade_index

    index_key = (expected_side, price_key)
    trades_at_price = trade_index.get(index_key, ())
    matched_qty = 0.0

    # Exact-price match unavailable: collect nearby trades
    # for diagnostics only. Never use them as executions.
    if not trades_at_price:
        nearby_trades = []

        try:
            max_price_distance = float(PRICE_STEP[symbol]) * 10
        except (KeyError, TypeError, ValueError):
            max_price_distance = 0.0

        # Nearby-price evidence never contributes to executed quantity.
        # Sample this diagnostic-only scan at most once per 100 ms per symbol;
        # unmatched reductions remain recorded, and exact-price matching above
        # is unchanged. This avoids rescanning/sorting recent trades for every
        # changed depth level during high-volume bursts.
        diagnostic_clock_ms = time.monotonic_ns() // 1_000_000
        last_nearby_scan_ms = state.get(
            "_last_nearby_trade_diagnostic_monotonic_ms"
        )
        collect_nearby_diagnostics = (
            last_nearby_scan_ms is None
            or diagnostic_clock_ms - last_nearby_scan_ms >= 100
        )

        if collect_nearby_diagnostics:
            state["_last_nearby_trade_diagnostic_monotonic_ms"] = (
                diagnostic_clock_ms
            )
            recent_trades = state.get("trade_history", ())
            inspected_trades = 0

            for trade in reversed(recent_trades):
                if inspected_trades >= 20:
                    break
                inspected_trades += 1

                if not isinstance(trade, dict):
                    continue

                try:
                    trade_time = int(trade["time"])
                    trade_price = float(trade["price"])
                    trade_qty = float(trade.get("quantity", 0.0))
                    remaining_qty = float(
                        trade.get("remaining_qty", trade_qty)
                    )
                except (KeyError, TypeError, ValueError):
                    continue

                time_diff_ms = trade_time - reduction_time_int
                if time_diff_ms < -300 or time_diff_ms > 1500:
                    continue

                price_diff = trade_price - liquidity_price_float
                if abs(price_diff) > max_price_distance or remaining_qty <= 0:
                    continue

                nearby_trades.append({
                    "price": trade_price,
                    "price_key": round(trade_price, 12),
                    "price_diff": price_diff,
                    "abs_price_diff": abs(price_diff),
                    "quantity": trade_qty,
                    "remaining_qty": remaining_qty,
                    "time": trade_time,
                    "time_diff_ms": time_diff_ms,
                    "is_buyer_maker": trade.get("is_buyer_maker"),
                })

            nearby_trades.sort(
                key=lambda item: (
                    item["abs_price_diff"],
                    abs(item["time_diff_ms"]),
                )
            )

        state.setdefault(
            "match_diagnostics",
            deque(maxlen=500),
        ).append({
            "symbol": symbol,
            "side": liquidity_side,
            "price": str(liquidity_price),
            "price_key": price_key,
            "reduced_qty": reduced_qty_float,
            "reduction_time": reduction_time_int,
            "expected_is_buyer_maker": expected_is_buyer_maker,
            "reason": "NO_TRADES_AT_PRICE",
            "trade_index_keys": len(trade_index),
            "recent_same_side_trades": [
                {
                    "price": item["price"],
                    "price_key": item["price_key"],
                    "remaining_qty": item["remaining_qty"],
                    "time": item["time"],
                    "time_diff_ms": item["time_diff_ms"],
                }
                for item in nearby_trades[:20]
            ],
            "nearby_trades": nearby_trades[:20],
        })

        return 0.0

    # Exact-price matching only.
    for trade in trades_at_price:
        if matched_qty >= reduced_qty_float:
            break

        if not isinstance(trade, dict):
            continue

        try:
            trade_time = int(trade["time"])
            trade_price = float(trade["price"])
            trade_remaining = float(
                trade.get(
                    "remaining_qty",
                    trade.get("quantity", 0.0),
                )
            )
        except (KeyError, TypeError, ValueError):
            continue

        if trade_remaining <= 0:
            continue

        if trade.get("is_buyer_maker") is not expected_is_buyer_maker:
            continue

        time_diff_ms = trade_time - reduction_time_int

        if time_diff_ms < -300 or time_diff_ms > 1500:
            continue

        if round(trade_price, 12) != price_key:
            continue

        available_reduction = reduced_qty_float - matched_qty
        qty_to_match = min(trade_remaining, available_reduction)

        if qty_to_match <= 0:
            continue

        trade["remaining_qty"] = trade_remaining - qty_to_match
        matched_qty += qty_to_match

    if matched_qty > 0:
        state.setdefault(
            "match_diagnostics",
            deque(maxlen=500),
        ).append({
            "symbol": symbol,
            "side": liquidity_side,
            "price": str(liquidity_price),
            "price_key": price_key,
            "reduced_qty": reduced_qty_float,
            "matched_qty": matched_qty,
            "reduction_time": reduction_time_int,
            "expected_is_buyer_maker": expected_is_buyer_maker,
            "reason": "MATCH_SUCCESS",
        })

    return matched_qty


def apply_orderbook_event(symbol, event):
    """
    Validated Binance depth event ko local orderbook par apply karta hai
    aur har price-level liquidity movement ko record karta hai.

    FIFO evidence model:

        - Snapshot liquidity = oldest observed baseline lot
        - New liquidity addition = new FIFO lot
        - Liquidity reduction = oldest available lots se consume
        - reduced_qty = LIVE orderbook se immediately removed quantity

    Lifecycle evidence:

        ADD
          -> PERSIST
          -> MARKET APPROACH
          -> FLOW PRESSURE
          -> REDUCE
          -> EXECUTION MATCH
          -> PULL / ABSORPTION

    Adaptive relevance:

        - 1s / 3s / 10s relevant aggressive flow
        - liquidity vs relevant flow
        - distance vs observed market movement
        - approach pressure
        - bounded significance score

    IMPORTANT:

        - LIVE reduction ko kabhi delay nahi kiya jata.
        - Trade matching sirf attribution/evidence ke liye hai.
        - FIFO modeled evidence hai, exchange queue ka exact proof nahi.
        - Adaptive relevance classification nahi hai.
        - LIQUIDITY_PULLED ka existing finalization behavior unchanged hai.
        - Koi fixed BTC / dollar / distance threshold use nahi hota.
    """

    apply_perf_start = time.perf_counter()
    state = orderbook[symbol]

    event_update_id = int(event["u"])
    event_time = int(
        event.get("E", now_ms())
    )

    # ============================================================
    # FINALIZE EXPIRED LIQUIDITY REDUCTIONS
    # ============================================================

    finalize_perf_start = time.perf_counter()
    finalize_liquidity_records(
        symbol,
        event_time
    )
    finalize_perf_ms = (time.perf_counter() - finalize_perf_start) * 1000.0

    # ============================================================
    # LIQUIDITY MATCH INDEX
    # ============================================================

    liquidity_index = state.setdefault(
        "liquidity_match_index",
        defaultdict(deque)
    )

    # ============================================================
    # MARKET REFERENCE
    # ============================================================

    # Reuse the best prices from the prior applied event. Scanning every
    # live book level on every depth message made the hot path O(book depth),
    # even when the best bid/ask had not changed.
    market_cache = {
        "valid": bool(state.get("_best_price_cache_valid", False)),
        "best_bid": state.get("_cached_best_bid"),
        "best_ask": state.get("_cached_best_ask"),
        "market_reference": state.get("_cached_market_reference"),
    }

    def get_current_market_reference():

        if market_cache["valid"]:

            return (
                market_cache["best_bid"],
                market_cache["best_ask"],
                market_cache["market_reference"],
            )

        best_bid = None
        best_ask = None

        for p, q in state.get(
            "bids",
            {}
        ).items():

            try:

                if float(q) <= 0.0:
                    continue

                price_float = float(p)

                if (
                    best_bid is None
                    or price_float > best_bid
                ):

                    best_bid = price_float

            except Exception:

                continue

        for p, q in state.get(
            "asks",
            {}
        ).items():

            try:

                if float(q) <= 0.0:
                    continue

                price_float = float(p)

                if (
                    best_ask is None
                    or price_float < best_ask
                ):

                    best_ask = price_float

            except Exception:

                continue

        if (
            best_bid is not None
            and best_ask is not None
        ):

            market_reference = (
                best_bid + best_ask
            ) / 2.0

        elif best_bid is not None:

            market_reference = best_bid

        elif best_ask is not None:

            market_reference = best_ask

        else:

            market_reference = None

        market_cache["best_bid"] = best_bid
        market_cache["best_ask"] = best_ask
        market_cache["market_reference"] = market_reference
        market_cache["valid"] = True

        return (
            best_bid,
            best_ask,
            market_reference,
        )

    # ============================================================
    # CONTINUOUS TRADE-FLOW ACCOUNTING
    # ============================================================

    lifecycle_flow_totals = state.setdefault(
        "lifecycle_flow_totals",
        {
            "aggressive_buy_qty": 0.0,
            "aggressive_sell_qty": 0.0,
            "total_aggressive_qty": 0.0,
            "net_aggressive_delta": 0.0,
            "trade_count": 0,
        }
    )

    lifecycle_seen_trade_ids = state.setdefault(
        "lifecycle_seen_trade_ids",
        set()
    )

    lifecycle_seen_trade_queue = state.setdefault(
        "lifecycle_seen_trade_queue",
        deque(maxlen=5000)
    )

    trades_for_lifecycle = state.get(
        "trade_history",
        []
    )

    lifecycle_flow_changed = False

    for trade in reversed(
        trades_for_lifecycle
    ):

        trade_identity = id(trade)

        if trade_identity in lifecycle_seen_trade_ids:
            break

        if len(
            lifecycle_seen_trade_queue
        ) >= lifecycle_seen_trade_queue.maxlen:

            old_trade_identity = (
                lifecycle_seen_trade_queue.popleft()
            )

            lifecycle_seen_trade_ids.discard(
                old_trade_identity
            )

        lifecycle_seen_trade_ids.add(
            trade_identity
        )

        lifecycle_seen_trade_queue.append(
            trade_identity
        )

        try:

            qty = float(
                trade.get(
                    "qty",
                    trade.get(
                        "quantity",
                        0.0
                    )
                )
            )

        except Exception:

            continue

        if qty <= 0.0:
            continue

        is_buyer_maker = trade.get(
            "is_buyer_maker"
        )

        if is_buyer_maker is False:

            lifecycle_flow_totals[
                "aggressive_buy_qty"
            ] += qty

            lifecycle_flow_totals[
                "net_aggressive_delta"
            ] += qty

        elif is_buyer_maker is True:

            lifecycle_flow_totals[
                "aggressive_sell_qty"
            ] += qty

            lifecycle_flow_totals[
                "net_aggressive_delta"
            ] -= qty

        else:

            continue

        lifecycle_flow_totals[
            "total_aggressive_qty"
        ] += qty

        lifecycle_flow_totals[
            "trade_count"
        ] += 1

        lifecycle_flow_changed = True

    # ============================================================
    # CURRENT MARKET REFERENCE BEFORE EVENT
    # ============================================================

    (
        current_best_bid,
        current_best_ask,
        current_market_reference
    ) = get_current_market_reference()

    previous_lifecycle_market_reference = state.get(
        "lifecycle_last_market_reference"
    )

    market_reference_changed = (
        current_market_reference is not None
        and (
            previous_lifecycle_market_reference is None
            or current_market_reference
            != previous_lifecycle_market_reference
        )
    )

    # Lifecycle metadata scans every live FIFO lot under the collector
    # lock. Sample this secondary evidence scan at 250 ms to reduce repeated
    # full-lot walks during rapid best-price movement. Actual depth, FIFO,
    # trade matching, and pull finalization remain per-event.
    lifecycle_scan_clock_ms = time.monotonic_ns() // 1_000_000
    last_lifecycle_scan_ms = state.get(
        "_last_lifecycle_scan_monotonic_ms"
    )
    lifecycle_scan_due = (
        last_lifecycle_scan_ms is None
        or lifecycle_scan_clock_ms - last_lifecycle_scan_ms >= 250
    )

    # ============================================================
    # UPDATE EXISTING LOT LIFECYCLE
    # ============================================================

    lifecycle_perf_start = time.perf_counter()
    if (
        current_market_reference is not None
        and market_reference_changed
        and lifecycle_scan_due
    ):
        state["_last_lifecycle_scan_monotonic_ms"] = (
            lifecycle_scan_clock_ms
        )

        for lifecycle_side in (
            "bid",
            "ask",
        ):

            side_lots = state[
                "liquidity_lots"
            ].get(
                lifecycle_side,
                {}
            )

            for price_key, lots in side_lots.items():

                try:

                    level_price = float(
                        price_key
                    )

                except Exception:

                    continue

                current_distance = abs(
                    level_price
                    - current_market_reference
                )

                for lot in lots:

                    try:

                        first_seen_time = int(
                            lot.get(
                                "first_seen_time",
                                lot.get(
                                    "time",
                                    event_time
                                )
                            )
                        )

                    except Exception:

                        first_seen_time = event_time

                    lot[
                        "time_alive_ms"
                    ] = max(
                        event_time
                        - first_seen_time,
                        0
                    )

                    previous_closest = lot.get(
                        "closest_market_distance"
                    )

                    if (
                        previous_closest is None
                        or current_distance
                        < float(previous_closest)
                    ):

                        lot[
                            "closest_market_distance"
                        ] = current_distance

                        lot[
                            "closest_market_price"
                        ] = current_market_reference

                        lot[
                            "closest_market_time"
                        ] = event_time

                    first_seen_market_price = lot.get(
                        "first_seen_market_price"
                    )

                    initial_distance = lot.get(
                        "first_seen_distance"
                    )

                    if (
                        initial_distance is None
                        and first_seen_market_price is not None
                    ):

                        try:

                            initial_distance = abs(
                                level_price
                                - float(
                                    first_seen_market_price
                                )
                            )

                            lot[
                                "first_seen_distance"
                            ] = initial_distance

                        except Exception:

                            initial_distance = None

                    approach_started_now = False

                    if (
                        initial_distance is not None
                        and current_distance
                        < float(initial_distance)
                        and not lot.get(
                            "market_approached",
                            False
                        )
                    ):

                        lot[
                            "market_approached"
                        ] = True

                        approach_started_now = True

                    if approach_started_now:

                        lot[
                            "approach_flow_baseline"
                        ] = {

                            "aggressive_buy_qty": (
                                float(
                                    lifecycle_flow_totals.get(
                                        "aggressive_buy_qty",
                                        0.0
                                    )
                                )
                            ),

                            "aggressive_sell_qty": (
                                float(
                                    lifecycle_flow_totals.get(
                                        "aggressive_sell_qty",
                                        0.0
                                    )
                                )
                            ),

                            "total_aggressive_qty": (
                                float(
                                    lifecycle_flow_totals.get(
                                        "total_aggressive_qty",
                                        0.0
                                    )
                                )
                            ),

                            "net_aggressive_delta": (
                                float(
                                    lifecycle_flow_totals.get(
                                        "net_aggressive_delta",
                                        0.0
                                    )
                                )
                            ),

                            "trade_count": (
                                int(
                                    lifecycle_flow_totals.get(
                                        "trade_count",
                                        0
                                    )
                                )
                            ),

                            "time": event_time,
                        }

                        lot[
                            "approach_flow_start_time"
                        ] = event_time

                    if (
                        lot.get("market_approached", False)
                        and (
                            approach_started_now
                            or lifecycle_flow_changed
                        )
                    ):

                        baseline = lot.get(
                            "approach_flow_baseline"
                        )

                        if baseline is not None:

                            buy_qty = max(
                                float(
                                    lifecycle_flow_totals.get(
                                        "aggressive_buy_qty",
                                        0.0
                                    )
                                )
                                - float(
                                    baseline.get(
                                        "aggressive_buy_qty",
                                        0.0
                                    )
                                ),
                                0.0
                            )

                            sell_qty = max(
                                float(
                                    lifecycle_flow_totals.get(
                                        "aggressive_sell_qty",
                                        0.0
                                    )
                                )
                                - float(
                                    baseline.get(
                                        "aggressive_sell_qty",
                                        0.0
                                    )
                                ),
                                0.0
                            )

                            total_qty = max(
                                float(
                                    lifecycle_flow_totals.get(
                                        "total_aggressive_qty",
                                        0.0
                                    )
                                )
                                - float(
                                    baseline.get(
                                        "total_aggressive_qty",
                                        0.0
                                    )
                                ),
                                0.0
                            )

                            net_delta = (
                                float(
                                    lifecycle_flow_totals.get(
                                        "net_aggressive_delta",
                                        0.0
                                    )
                                )
                                - float(
                                    baseline.get(
                                        "net_aggressive_delta",
                                        0.0
                                    )
                                )
                            )

                            trade_count = max(
                                int(
                                    lifecycle_flow_totals.get(
                                        "trade_count",
                                        0
                                    )
                                )
                                - int(
                                    baseline.get(
                                        "trade_count",
                                        0
                                    )
                                ),
                                0
                            )

                            if lifecycle_side == "ask":

                                relevant_pressure = buy_qty
                                opposite_pressure = sell_qty
                                relevant_side = "aggressive_buy"

                            else:

                                relevant_pressure = sell_qty
                                opposite_pressure = buy_qty
                                relevant_side = "aggressive_sell"

                            lot[
                                "approach_flow"
                            ] = {

                                "aggressive_buy_qty": buy_qty,
                                "aggressive_sell_qty": sell_qty,
                                "total_aggressive_qty": total_qty,
                                "net_aggressive_delta": net_delta,
                                "trade_count": trade_count,
                                "relevant_pressure_qty": relevant_pressure,
                                "opposite_pressure_qty": opposite_pressure,
                                "relevant_side": relevant_side,
                                "approach_started_time": (
                                    lot.get(
                                        "approach_flow_start_time"
                                    )
                                ),
                            }

                    if not lot.get(
                        "market_approached",
                        False
                    ):

                        if not lot.get(
                            "approach_flow"
                        ):

                            lot[
                                "approach_flow"
                            ] = {

                                "aggressive_buy_qty": 0.0,
                                "aggressive_sell_qty": 0.0,
                                "total_aggressive_qty": 0.0,
                                "net_aggressive_delta": 0.0,
                                "trade_count": 0,
                                "relevant_pressure_qty": 0.0,
                                "opposite_pressure_qty": 0.0,
                                "relevant_side": (
                                    "aggressive_buy"
                                    if lifecycle_side == "ask"
                                    else "aggressive_sell"
                                ),
                                "approach_started_time": None,
                            }

        state[
            "lifecycle_last_market_reference"
        ] = current_market_reference

    lifecycle_perf_ms = (time.perf_counter() - lifecycle_perf_start) * 1000.0

    # ============================================================
    # MARKET / FLOW / ADAPTIVE CONTEXT
    # ============================================================

    market_flow_window_cache = None

    def get_market_flow_context(
        reduction_price,
        reduction_qty,
        reduction_side,
    ):
        nonlocal market_flow_window_cache

        try:
            reduction_price_float = float(reduction_price)
            (
                best_bid,
                best_ask,
                market_reference
            ) = get_current_market_reference()

            if market_reference is not None:
                distance_from_market = (
                    reduction_price_float - market_reference
                )
                abs_distance_from_market = abs(distance_from_market)
            else:
                distance_from_market = None
                abs_distance_from_market = None

            # The trade window aggregates are identical for every changed
            # price level in this depth event. Build them once, rather than
            # rescanning trade_history for every reduction.
            if market_flow_window_cache is None:
                trades = state.get("trade_history", [])
                window_sizes = (1000, 3000, 10000)
                cached_windows = {
                    window_ms: {
                        "trade_count": 0,
                        "buy_qty": 0.0,
                        "sell_qty": 0.0,
                        "total_qty": 0.0,
                        "observed_low": None,
                        "observed_high": None,
                        "first_trade_price": None,
                        "last_trade_price": None,
                    }
                    for window_ms in window_sizes
                }

                # trade_history is appended in stream-arrival order.
                # Walk newest-to-oldest so old trades outside the widest
                # 10-second window do not consume CPU on every depth event.
                # Preserve earliest/latest price semantics for each window:
                # in reverse order, latest is assigned once and earliest is
                # overwritten as older eligible trades are encountered.
                for trade in reversed(trades):
                    try:
                        trade_time = int(
                            trade.get("time", trade.get("event_time", 0))
                        )
                    except Exception:
                        continue

                    if trade_time > event_time:
                        continue

                    age_ms = event_time - trade_time
                    if age_ms > 10000:
                        # Trade history is chronological; all remaining
                        # entries are older than every lifecycle flow window.
                        break

                    try:
                        qty = float(
                            trade.get("qty", trade.get("quantity", 0.0))
                        )
                    except Exception:
                        continue

                    if qty <= 0.0:
                        continue

                    is_buyer_maker = trade.get("is_buyer_maker")
                    if is_buyer_maker is not False and is_buyer_maker is not True:
                        continue

                    try:
                        price = float(trade.get("price"))
                    except (TypeError, ValueError, OverflowError):
                        price = None

                    for window_ms in window_sizes:
                        if age_ms > window_ms:
                            continue

                        window = cached_windows[window_ms]
                        if is_buyer_maker is False:
                            window["buy_qty"] += qty
                        else:
                            window["sell_qty"] += qty

                        window["total_qty"] += qty
                        window["trade_count"] += 1

                        if price is not None:
                            if window["last_trade_price"] is None:
                                window["last_trade_price"] = price
                            window["first_trade_price"] = price

                            current_low = window["observed_low"]
                            current_high = window["observed_high"]
                            window["observed_low"] = (
                                price if current_low is None
                                else min(current_low, price)
                            )
                            window["observed_high"] = (
                                price if current_high is None
                                else max(current_high, price)

                market_flow_window_cache = cached_windows

            context = {
                "reduction_price": reduction_price_float,
                "reduction_qty": float(reduction_qty),
                "reduction_side": reduction_side,
                "event_time": event_time,
                "depth_update_id": event_update_id,
                "best_bid": best_bid,
                "best_ask": best_ask,
                "market_reference": market_reference,
                "distance_from_market": distance_from_market,
                "abs_distance_from_market": abs_distance_from_market,
                "windows": {},
            }

            for window_ms in (1000, 3000, 10000):
                base = market_flow_window_cache[window_ms]
                buy_qty = float(base["buy_qty"])
                sell_qty = float(base["sell_qty"])
                total_qty = float(base["total_qty"])
                trade_count = int(base["trade_count"])
                observed_low = base["observed_low"]
                observed_high = base["observed_high"]
                first_trade_price = base["first_trade_price"]
                last_trade_price = base["last_trade_price"]

                net_delta = buy_qty - sell_qty
                flow_imbalance = (
                    net_delta / total_qty if total_qty > 0.0 else 0.0
                )

                if observed_low is not None and observed_high is not None:
                    observed_range = observed_high - observed_low
                    observed_displacement = (
                        last_trade_price - first_trade_price
                        if first_trade_price is not None
                        and last_trade_price is not None
                        else 0.0
                    )
                    observed_abs_displacement = abs(observed_displacement)

                    if (
                        abs_distance_from_market is not None
                        and observed_range > 0.0
                    ):
                        distance_to_range_ratio = (
                            abs_distance_from_market / observed_range
                        )
                    else:
                        distance_to_range_ratio = None
                else:
                    observed_range = 0.0
                    observed_displacement = 0.0
                    observed_abs_displacement = 0.0
                    distance_to_range_ratio = None

                if reduction_side == "ask":
                    relevant_flow_qty = buy_qty
                    opposite_flow_qty = sell_qty
                else:
                    relevant_flow_qty = sell_qty
                    opposite_flow_qty = buy_qty

                if relevant_flow_qty > 0.0:
                    liquidity_flow_ratio = (
                        float(reduction_qty) / relevant_flow_qty
                    )
                    flow_factor = (
                        liquidity_flow_ratio / (liquidity_flow_ratio + 1.0)
                    )
                else:
                    liquidity_flow_ratio = None
                    flow_factor = 0.0

                if (
                    abs_distance_from_market is not None
                    and observed_range > 0.0
                ):
                    distance_factor = 1.0 / (
                        1.0 + (abs_distance_from_market / observed_range)
                    )
                else:
                    distance_factor = 0.0

                context["windows"][str(window_ms)] = {
                    "window_ms": window_ms,
                    "trade_count": trade_count,
                    "aggressive_buy_qty": buy_qty,
                    "aggressive_sell_qty": sell_qty,
                    "total_aggressive_qty": total_qty,
                    "relevant_aggressive_flow_qty": relevant_flow_qty,
                    "opposite_aggressive_flow_qty": opposite_flow_qty,
                    "net_aggressive_delta": net_delta,
                    "flow_imbalance": flow_imbalance,
                    "observed_low": observed_low,
                    "observed_high": observed_high,
                    "observed_range": observed_range,
                    "first_trade_price": first_trade_price,
                    "last_trade_price": last_trade_price,
                    "observed_displacement": observed_displacement,
                    "observed_abs_displacement": observed_abs_displacement,
                    "distance_to_observed_range_ratio": distance_to_range_ratio,
                    "liquidity_flow_ratio": liquidity_flow_ratio,
                    "flow_factor": flow_factor,
                    "distance_factor": distance_factor,
                }

            windows = context["windows"]
            weighted_flow_factor = (
                0.50 * float(windows["1000"].get("flow_factor", 0.0))
                + 0.30 * float(windows["3000"].get("flow_factor", 0.0))
                + 0.20 * float(windows["10000"].get("flow_factor", 0.0))
            )
            weighted_distance_factor = (
                0.50 * float(windows["1000"].get("distance_factor", 0.0))
                + 0.30 * float(windows["3000"].get("distance_factor", 0.0))
                + 0.20 * float(windows["10000"].get("distance_factor", 0.0))
            )

            context["adaptive"] = {
                "flow_factor": weighted_flow_factor,
                "distance_factor": weighted_distance_factor,
                "approach_factor": 0.0,
                "significance_score": (
                    0.50 * weighted_flow_factor
                    + 0.25 * weighted_distance_factor
                ),
                "classification": "normal",
            }
            return context

        except Exception as exc:
            return {
                "error": "market_flow_context_failed",
                "message": str(exc),
                "event_time": event_time,
                "depth_update_id": event_update_id,
                "reduction_price": str(reduction_price),
                "reduction_qty": float(reduction_qty),
                "reduction_side": reduction_side,
            }

    # ============================================================
    # PROCESS ONE SIDE
    # ============================================================

    def process_side(
        side,
        event_levels,
        book,
    ):

        for price, quantity in event_levels:

            price = str(price)

            new_quantity = float(
                quantity
            )

            old_quantity = float(
                book.get(
                    price,
                    0.0
                )
            )

            added_qty = max(
                new_quantity
                - old_quantity,
                0.0
            )

            reduced_qty = max(
                old_quantity
                - new_quantity,
                0.0
            )

            # Binance depth events can repeat unchanged nonzero levels.
            # They have no book, FIFO, lifecycle, or liquidity-record
            # transition to account for, so skip their downstream work.
            # Keep zero-quantity levels on the normal path to preserve
            # empty FIFO price-level cleanup.
            if old_quantity == new_quantity and new_quantity > 0.0:
                continue

            # ====================================================
            # REDUCTION CONTEXT
            # ====================================================

            market_flow_context = None

            if reduced_qty > 0.0:

                market_flow_context = (
                    get_market_flow_context(
                        price,
                        reduced_qty,
                        side,
                    )
                )

            # ====================================================
            # UPDATE LIVE ORDERBOOK
            # ====================================================

            # Keep the event-local top-of-book cache valid when a
            # non-best level changes. Previously every changed price level
            # invalidated it, so each added level rescanned the entire book.
            cache_was_valid = market_cache["valid"]
            price_float = float(price)

            if new_quantity == 0.0:
                book.pop(price, None)
            else:
                book[price] = new_quantity

            if cache_was_valid:
                top_of_book_changed = False

                if side == "bid":
                    cached_best = market_cache["best_bid"]

                    if (
                        new_quantity == 0.0
                        and cached_best is not None
                        and price_float == cached_best
                    ):
                        # The best bid was removed; rescan once when needed.
                        market_cache["valid"] = False
                    elif (
                        new_quantity > 0.0
                        and (
                            cached_best is None
                            or price_float > cached_best
                        )
                    ):
                        market_cache["best_bid"] = price_float
                        top_of_book_changed = True

                else:
                    cached_best = market_cache["best_ask"]

                    if (
                        new_quantity == 0.0
                        and cached_best is not None
                        and price_float == cached_best
                    ):
                        # The best ask was removed; rescan once when needed.
                        market_cache["valid"] = False
                    elif (
                        new_quantity > 0.0
                        and (
                            cached_best is None
                            or price_float < cached_best
                        )
                    ):
                        market_cache["best_ask"] = price_float
                        top_of_book_changed = True

                # Recalculate the midpoint only when a best price actually
                # changes. Most depth updates affect non-best levels, so
                # repeatedly rebuilding the same market reference is wasted
                # work. If a best level is removed, the existing invalidation
                # path rescans and rebuilds the cache when next requested.
                if market_cache["valid"] and top_of_book_changed:
                    cached_bid = market_cache["best_bid"]
                    cached_ask = market_cache["best_ask"]

                    if cached_bid is not None and cached_ask is not None:
                        market_cache["market_reference"] = (
                            cached_bid + cached_ask
                        ) / 2.0
                    elif cached_bid is not None:
                        market_cache["market_reference"] = cached_bid
                    elif cached_ask is not None:
                        market_cache["market_reference"] = cached_ask
                    else:
                        market_cache["market_reference"] = None

            # ====================================================
            # FIFO LIQUIDITY LEDGER
            # ====================================================

            lots = state[
                "liquidity_lots"
            ][side][price]

            # ====================================================
            # NEW LIQUIDITY
            # ====================================================

            if added_qty > 0.0:

                (
                    add_best_bid,
                    add_best_ask,
                    add_market_reference
                ) = get_current_market_reference()

                if add_market_reference is not None:

                    add_initial_distance = abs(
                        float(price)
                        - add_market_reference
                    )

                else:

                    add_initial_distance = None

                new_lot = {

                    "lot_id": str(
                        uuid.uuid4()
                    ),

                    "original_qty": float(
                        added_qty
                    ),

                    "remaining_qty": float(
                        added_qty
                    ),

                    "time": event_time,

                    "origin": "depth_add",

                    "update_id": event_update_id,

                    "first_seen_time": (
                        event_time
                    ),

                    "first_seen_market_price": (
                        add_market_reference
                    ),

                    "first_seen_distance": (
                        add_initial_distance
                    ),

                    "closest_market_distance": (
                        add_initial_distance
                    ),

                    "closest_market_price": (
                        add_market_reference
                    ),

                    "closest_market_time": (
                        event_time
                    ),

                    "time_alive_ms": 0,

                    "market_approached": False,

                    "approach_flow_baseline": None,

                    "approach_flow_start_time": None,

                    "approach_flow": {

                        "aggressive_buy_qty": 0.0,

                        "aggressive_sell_qty": 0.0,

                        "total_aggressive_qty": 0.0,

                        "net_aggressive_delta": 0.0,

                        "trade_count": 0,

                        "relevant_pressure_qty": 0.0,

                        "opposite_pressure_qty": 0.0,

                        "relevant_side": (
                            "aggressive_buy"
                            if side == "ask"
                            else "aggressive_sell"
                        ),

                        "approach_started_time": None,
                    },
                }

                lots.append(
                    new_lot
                )

            # ====================================================
            # FIFO REDUCTION
            # ====================================================

            fifo_consumption = []

            fifo_unattributed_qty = 0.0

            if reduced_qty > 0.0:

                remaining_reduction = float(
                    reduced_qty
                )

                while (
                    remaining_reduction > 0.0
                    and lots
                ):

                    oldest_lot = lots[0]

                    lot_remaining = float(
                        oldest_lot.get(
                            "remaining_qty",
                            0.0
                        )
                    )

                    if lot_remaining <= 0.0:

                        lots.popleft()

                        continue

                    consumed_qty = min(
                        lot_remaining,
                        remaining_reduction
                    )

                    remaining_after = (
                        lot_remaining
                        - consumed_qty
                    )

                    oldest_lot[
                        "remaining_qty"
                    ] = remaining_after

                    try:

                        first_seen_time = int(
                            oldest_lot.get(
                                "first_seen_time",
                                oldest_lot.get(
                                    "time",
                                    event_time
                                )
                            )
                        )

                    except Exception:

                        first_seen_time = event_time

                    lifecycle_time_alive = max(
                        event_time
                        - first_seen_time,
                        0
                    )

                    # --------------------------------------------
                    # REFRESH APPROACH FLOW
                    # --------------------------------------------

                    if (
                        oldest_lot.get(
                            "market_approached",
                            False
                        )
                        and oldest_lot.get(
                            "approach_flow_baseline"
                        ) is not None
                    ):

                        baseline = oldest_lot.get(
                            "approach_flow_baseline"
                        )

                        buy_qty = max(
                            float(
                                lifecycle_flow_totals.get(
                                    "aggressive_buy_qty",
                                    0.0
                                )
                            )
                            - float(
                                baseline.get(
                                    "aggressive_buy_qty",
                                    0.0
                                )
                            ),
                            0.0
                        )

                        sell_qty = max(
                            float(
                                lifecycle_flow_totals.get(
                                    "aggressive_sell_qty",
                                    0.0
                                )
                            )
                            - float(
                                baseline.get(
                                    "aggressive_sell_qty",
                                    0.0
                                )
                            ),
                            0.0
                        )

                        total_flow_qty = max(
                            float(
                                lifecycle_flow_totals.get(
                                    "total_aggressive_qty",
                                    0.0
                                )
                            )
                            - float(
                                baseline.get(
                                    "total_aggressive_qty",
                                    0.0
                                )
                            ),
                            0.0
                        )

                        net_flow_delta = (
                            float(
                                lifecycle_flow_totals.get(
                                    "net_aggressive_delta",
                                    0.0
                                )
                            )
                            - float(
                                baseline.get(
                                    "net_aggressive_delta",
                                    0.0
                                )
                            )
                        )

                        flow_trade_count = max(
                            int(
                                lifecycle_flow_totals.get(
                                    "trade_count",
                                    0
                                )
                            )
                            - int(
                                baseline.get(
                                    "trade_count",
                                    0
                                )
                            ),
                            0
                        )

                        if side == "ask":

                            relevant_pressure = buy_qty
                            opposite_pressure = sell_qty
                            relevant_side = "aggressive_buy"

                        else:

                            relevant_pressure = sell_qty
                            opposite_pressure = buy_qty
                            relevant_side = "aggressive_sell"

                        oldest_lot[
                            "approach_flow"
                        ] = {

                            "aggressive_buy_qty": buy_qty,

                            "aggressive_sell_qty": sell_qty,

                            "total_aggressive_qty": (
                                total_flow_qty
                            ),

                            "net_aggressive_delta": (
                                net_flow_delta
                            ),

                            "trade_count": (
                                flow_trade_count
                            ),

                            "relevant_pressure_qty": (
                                relevant_pressure
                            ),

                            "opposite_pressure_qty": (
                                opposite_pressure
                            ),

                            "relevant_side": (
                                relevant_side
                            ),

                            "approach_started_time": (
                                oldest_lot.get(
                                    "approach_flow_start_time"
                                )
                            ),
                        }

                    # --------------------------------------------
                    # FIFO CONSUMPTION RECORD
                    # --------------------------------------------

                    fifo_consumption.append({

                        "lot_id": oldest_lot.get(
                            "lot_id"
                        ),

                        "origin": oldest_lot.get(
                            "origin"
                        ),

                        "origin_time": int(
                            oldest_lot.get(
                                "time",
                                event_time
                            )
                        ),

                        "origin_update_id": (
                            oldest_lot.get(
                                "update_id"
                            )
                        ),

                        "original_qty": float(
                            oldest_lot.get(
                                "original_qty",
                                0.0
                            )
                        ),

                        "consumed_qty": float(
                            consumed_qty
                        ),

                        "remaining_qty_after": float(
                            remaining_after
                        ),

                        "execution_qty": 0.0,

                        "unmatched_qty": float(consumed_qty),

                        "first_seen_time": (
                            oldest_lot.get(
                                "first_seen_time"
                            )
                        ),

                        "first_seen_market_price": (
                            oldest_lot.get(
                                "first_seen_market_price"
                            )
                        ),

                        "first_seen_distance": (
                            oldest_lot.get(
                                "first_seen_distance"
                            )
                        ),

                        "closest_market_distance": (
                            oldest_lot.get(
                                "closest_market_distance"
                            )
                        ),

                        "closest_market_price": (
                            oldest_lot.get(
                                "closest_market_price"
                            )
                        ),

                        "closest_market_time": (
                            oldest_lot.get(
                                "closest_market_time"
                            )
                        ),

                        "time_alive_ms": (
                            lifecycle_time_alive
                        ),

                        "market_approached": bool(
                            oldest_lot.get(
                                "market_approached",
                                False
                            )
                        ),

                        "approach_flow": dict(
                            oldest_lot.get(
                                "approach_flow",
                                {}
                            )
                        ),
                    })

                    remaining_reduction -= (
                        consumed_qty
                    )

                    if (
                        oldest_lot[
                            "remaining_qty"
                        ] <= 0.0
                    ):

                        lots.popleft()

                fifo_unattributed_qty = max(
                    remaining_reduction,
                    0.0
                )

            # ====================================================
            # EMPTY FIFO PRICE LEVEL CLEANUP
            # ====================================================

            if not lots:

                state[
                    "liquidity_lots"
                ][side].pop(
                    price,
                    None
                )

            # ====================================================
            # RECORD LIQUIDITY MOVEMENT
            # ====================================================

            if old_quantity != new_quantity:

                executed_qty = 0.0

                if reduced_qty > 0.0:

                    executed_qty = (
                        match_trade_to_liquidity_reduction(
                            symbol,
                            side,
                            price,
                            reduced_qty,
                            event_time,
                        )
                    )

                executed_qty = min(
                    max(
                        float(executed_qty),
                        0.0
                    ),
                    float(reduced_qty)
                )

                unmatched_qty = max(
                    reduced_qty
                    - executed_qty,
                    0.0
                )

                # ------------------------------------------------
                # FIFO EXECUTION ATTRIBUTION
                # ------------------------------------------------

                remaining_execution = (
                    executed_qty
                )

                for consumption in fifo_consumption:

                    if remaining_execution <= 0.0:
                        break

                    consumed_qty = float(
                        consumption.get(
                            "consumed_qty",
                            0.0
                        )
                    )

                    allocated_execution = min(
                        consumed_qty,
                        remaining_execution
                    )

                    consumption[
                        "execution_qty"
                    ] = allocated_execution

                    consumption[
                        "unmatched_qty"
                    ] = max(
                        consumed_qty
                        - allocated_execution,
                        0.0
                    )

                    remaining_execution -= (
                        allocated_execution
                    )

                # ------------------------------------------------
                # ADAPTIVE APPROACH FACTOR
                # ------------------------------------------------

                adaptive_context = None

                if (
                    reduced_qty > 0.0
                    and market_flow_context is not None
                ):

                    if fifo_consumption:

                        weighted_approach_qty = 0.0
                        weighted_consumed_qty = 0.0

                        for consumption in fifo_consumption:

                            consumed_qty = float(
                                consumption.get(
                                    "consumed_qty",
                                    0.0
                                )
                            )

                            approach_flow = (
                                consumption.get(
                                    "approach_flow",
                                    {}
                                )
                            )

                            relevant_pressure = float(
                                approach_flow.get(
                                    "relevant_pressure_qty",
                                    0.0
                                )
                            )

                            weighted_approach_qty += (
                                relevant_pressure
                                * consumed_qty
                            )

                            weighted_consumed_qty += (
                                consumed_qty
                            )

                        if (
                            weighted_consumed_qty > 0.0
                        ):

                            approach_flow_qty = (
                                weighted_approach_qty
                                / weighted_consumed_qty
                            )

                        else:

                            approach_flow_qty = 0.0

                    else:

                        approach_flow_qty = 0.0

                    if (
                        approach_flow_qty > 0.0
                        and reduced_qty > 0.0
                    ):

                        approach_factor = (
                            approach_flow_qty
                            / (
                                approach_flow_qty
                                + reduced_qty
                            )
                        )

                    else:

                        approach_factor = 0.0

                    adaptive_context = (
                        market_flow_context.get(
                            "adaptive",
                            {}
                        )
                    )

                    flow_factor = float(
                        adaptive_context.get(
                            "flow_factor",
                            0.0
                        )
                    )

                    distance_factor = float(
                        adaptive_context.get(
                            "distance_factor",
                            0.0
                        )
                    )

                    significance_score = (
                        0.50
                        * flow_factor
                        +
                        0.25
                        * distance_factor
                        +
                        0.25
                        * approach_factor
                    )

                    significance_score = min(
                        max(
                            significance_score,
                            0.0
                        ),
                        1.0
                    )

                    if significance_score >= 0.75:

                        significance_classification = (
                            "strong_interest"
                        )

                    elif significance_score >= 0.55:

                        significance_classification = (
                            "high_interest"
                        )

                    elif significance_score >= 0.30:

                        significance_classification = (
                            "meaningful"
                        )

                    else:

                        significance_classification = (
                            "normal"
                        )

                    market_flow_context[
                        "adaptive"
                    ] = {

                        "flow_factor": (
                            flow_factor
                        ),

                        "distance_factor": (
                            distance_factor
                        ),

                        "approach_factor": (
                            approach_factor
                        ),

                        "approach_relevant_flow_qty": (
                            approach_flow_qty
                        ),

                        "significance_score": (
                            significance_score
                        ),

                        "classification": (
                            significance_classification
                        ),
                    }

                # ------------------------------------------------
                # LIQUIDITY RECORD
                # ------------------------------------------------

                liquidity_record = {

                    "time": event_time,

                    "update_id": event_update_id,

                    "side": side,

                    "price": price,

                    "old_qty": old_quantity,

                    "new_qty": new_quantity,

                    "added_qty": added_qty,

                    "reduced_qty": reduced_qty,

                    "executed_qty": executed_qty,

                    "unmatched_qty": unmatched_qty,

                    "pulled_qty": 0.0,

                    "pull_pct": 0.0,

                    "finalized": False,

                    "fifo_reduction": (
                        reduced_qty > 0.0
                    ),

                    "fifo_consumption": (
                        fifo_consumption
                    ),

                    "fifo_consumed_qty": sum(
                        float(
                            item.get(
                                "consumed_qty",
                                0.0
                            )
                        )
                        for item in fifo_consumption
                    ),

                    "fifo_unattributed_qty": (
                        fifo_unattributed_qty
                    ),

                    "fifo_executed_qty": sum(
                        float(
                            item.get(
                                "execution_qty",
                                0.0
                            )
                        )
                        for item in fifo_consumption
                    ),

                    "fifo_unmatched_qty": sum(
                        float(
                            item.get(
                                "unmatched_qty",
                                0.0
                            )
                        )
                        for item in fifo_consumption
                    ),

                    "market_context": (
                        market_flow_context
                    ),

                    "status": (
                        "reduction_pending"
                        if reduced_qty > 0.0
                        else "liquidity_added"
                    ),
                }

                state[
                    "liquidity_history"
                ].append(
                    liquidity_record
                )

                # Track only live reductions for the 1500 ms finalization
                # window; additions do not need finalization work.
                if reduced_qty > 0.0:
                    state.setdefault(
                        "_pending_finalize_records",
                        deque(),
                    ).append(liquidity_record)

                # =================================================
                # INDEX NEW LIQUIDITY MOVEMENT
                # =================================================

                if reduced_qty > 0.0:

                    price_key = round(
                        float(price),
                        12
                    )

                    liquidity_index_key = (
                        side,
                        price_key
                    )
                    if liquidity_index_key not in liquidity_index:
                        cleanup_keys = state.setdefault(
                            "_liquidity_match_cleanup_keys", deque()
                        )
                        queued_keys = state.setdefault(
                            "_liquidity_match_cleanup_queued", set()
                        )
                        if liquidity_index_key not in queued_keys:
                            cleanup_keys.append(liquidity_index_key)
                            queued_keys.add(liquidity_index_key)

                    liquidity_index[
                        liquidity_index_key
                    ].append(
                        liquidity_record
                    )

    # ============================================================
    # BIDS
    # ============================================================

    bid_perf_start = time.perf_counter()
    process_side(
        "bid",
        event.get("b", []),
        state["bids"],
    )
    bid_perf_ms = (time.perf_counter() - bid_perf_start) * 1000.0

    # ============================================================
    # ASKS
    # ============================================================

    ask_perf_start = time.perf_counter()
    process_side(
        "ask",
        event.get("a", []),
        state["asks"],
    )
    ask_perf_ms = (time.perf_counter() - ask_perf_start) * 1000.0

    # If an event removed the cached best level, rescan only for that
    # exceptional case; otherwise preserve O(1) best-price lookup next event.
    if not market_cache["valid"]:
        get_current_market_reference()

    state["_cached_best_bid"] = market_cache["best_bid"]
    state["_cached_best_ask"] = market_cache["best_ask"]
    state["_cached_market_reference"] = market_cache["market_reference"]
    state["_best_price_cache_valid"] = True

    # ============================================================
    # UPDATE SYNC STATE
    # ============================================================

    state["last_update_id"] = (
        event_update_id
    )

    state["last_depth_update_id"] = (
        event_update_id
    )

    state["last_depth_event_time"] = (
        now_ms()
    )

    # Aggregate timing and log only once per 100 applied depth events.
    perf = state.setdefault("_apply_perf", {
        "count": 0, "total_ms": 0.0, "max_ms": 0.0,
        "finalize_ms": 0.0, "finalize_max_ms": 0.0,
        "lifecycle_ms": 0.0, "lifecycle_max_ms": 0.0,
        "bid_ms": 0.0, "bid_max_ms": 0.0,
        "ask_ms": 0.0, "ask_max_ms": 0.0,
    })
    apply_perf_ms = (time.perf_counter() - apply_perf_start) * 1000.0
    perf["count"] += 1
    perf["total_ms"] += apply_perf_ms
    perf["max_ms"] = max(perf["max_ms"], apply_perf_ms)
    for key, value in (
        ("finalize_ms", finalize_perf_ms),
        ("lifecycle_ms", lifecycle_perf_ms),
        ("bid_ms", bid_perf_ms),
        ("ask_ms", ask_perf_ms),
    ):
        perf[key] += value
        max_key = key.replace("_ms", "_max_ms")
        perf[max_key] = max(perf[max_key], value)

    if perf["count"] >= 100:
        count = perf["count"]
        print(
            f"[PERF] {symbol} n={count} "
            f"apply_avg_ms={perf['total_ms'] / count:.2f} "
            f"apply_max_ms={perf['max_ms']:.2f} "
            f"finalize_avg_ms={perf['finalize_ms'] / count:.2f} "
            f"finalize_max_ms={perf['finalize_max_ms']:.2f} "
            f"lifecycle_avg_ms={perf['lifecycle_ms'] / count:.2f} "
            f"lifecycle_max_ms={perf['lifecycle_max_ms']:.2f} "
            f"bid_avg_ms={perf['bid_ms'] / count:.2f} "
            f"bid_max_ms={perf['bid_max_ms']:.2f} "
            f"ask_avg_ms={perf['ask_ms'] / count:.2f} "
            f"ask_max_ms={perf['ask_max_ms']:.2f}"
        )
        for key in (
            "count", "total_ms", "max_ms", "finalize_ms",
            "finalize_max_ms", "lifecycle_ms", "lifecycle_max_ms",
            "bid_ms", "bid_max_ms", "ask_ms", "ask_max_ms",
        ):
            perf[key] = 0 if key == "count" else 0.0


def initialize_orderbook(symbol):
    """
    Recover and synchronize a Binance USD-M Futures orderbook.

    Keep a valid snapshot while waiting for the depth stream to
    catch up. Fetch a fresh snapshot when a real sequence gap
    or invalid snapshot makes the current snapshot unusable.
    Never mark the book synchronized without sequence validation.
    """

    RETRY_BASE_SECONDS = 0.5
    RETRY_MAX_SECONDS = 5.0
    BRIDGE_TIMEOUT_SECONDS = 20.0
    BRIDGE_CHECK_INTERVAL = 0.05
    MAX_BUFFER_EVENTS = 2000

    if symbol not in orderbook:
        print(f"[ORDERBOOK INIT] INVALID SYMBOL {symbol}")
        return False

    with symbol_locks[symbol]:
        state = orderbook[symbol]

        if state.get("_init_worker_active", False):
            return False

        state["_init_worker_active"] = True
        state["initialized"] = False
        state["resyncing"] = True

    attempt = 0
    snapshot = None
    snapshot_id = None
    snapshot_bids = None
    snapshot_asks = None
    bridge_started = None
    last_wait_log = 0.0

    try:
        while True:
            with symbol_locks[symbol]:
                state = orderbook[symbol]

                if not collector_state.get("connected", False):
                    print(
                        f"[ORDERBOOK INIT] WAITING FOR COLLECTOR "
                        f"{symbol}"
                    )
                    return False

                while len(state["buffer"]) > MAX_BUFFER_EVENTS:
                    state["buffer"].popleft()

            # Fetch a snapshot only when the current one is absent
            # or has been invalidated by a real sequence gap.
            if snapshot is None:
                attempt += 1

                print(
                    f"[ORDERBOOK INIT] ATTEMPT {symbol} "
                    f"attempt={attempt}"
                )

                try:
                    with snapshot_ws_lock:
                        with symbol_locks[symbol]:
                            if not collector_state.get(
                                "connected", False
                            ):
                                return False

                        new_snapshot = fetch_orderbook_snapshot_ws(
                            symbol
                        )

                except Exception as exc:
                    new_snapshot = None
                    print(
                        f"[ORDERBOOK INIT] SNAPSHOT ERROR {symbol}: "
                        f"{type(exc).__name__}: {exc}"
                    )

                if not isinstance(new_snapshot, dict):
                    retry_reason = "SNAPSHOT_FAILED"
                else:
                    try:
                        new_snapshot_id = int(
                            new_snapshot["lastUpdateId"]
                        )

                        new_snapshot_bids = {
                            str(price): float(qty)
                            for price, qty
                            in new_snapshot.get("bids", [])
                            if float(qty) > 0
                        }

                        new_snapshot_asks = {
                            str(price): float(qty)
                            for price, qty
                            in new_snapshot.get("asks", [])
                            if float(qty) > 0
                        }

                        if (
                            new_snapshot_id <= 0
                            or not new_snapshot_bids
                            or not new_snapshot_asks
                        ):
                            raise ValueError(
                                "Invalid or empty snapshot"
                            )

                        snapshot = new_snapshot
                        snapshot_id = new_snapshot_id
                        snapshot_bids = new_snapshot_bids
                        snapshot_asks = new_snapshot_asks
                        bridge_started = time.monotonic()
                        last_wait_log = 0.0
                        retry_reason = None

                        print(
                            f"[ORDERBOOK INIT] SNAPSHOT READY "
                            f"{symbol} snapshot_id={snapshot_id} "
                            f"buffer_count="
                            f"{len(orderbook[symbol]['buffer'])}"
                        )

                    except (
                        KeyError,
                        TypeError,
                        ValueError,
                    ) as exc:
                        print(
                            f"[ORDERBOOK INIT] INVALID SNAPSHOT "
                            f"{symbol}: {exc}"
                        )
                        retry_reason = "INVALID_SNAPSHOT"

                if retry_reason is not None:
                    delay = min(
                        RETRY_BASE_SECONDS
                        * (2 ** min(attempt - 1, 4)),
                        RETRY_MAX_SECONDS,
                    )

                    print(
                        f"[ORDERBOOK INIT] RETRY {symbol} "
                        f"reason={retry_reason} delay={delay:.2f}s"
                    )
                    time.sleep(delay)
                    continue

            target_id = snapshot_id + 1
            restart_with_fresh_snapshot = False

            while True:
                with symbol_locks[symbol]:
                    state = orderbook[symbol]

                    if not collector_state.get("connected", False):
                        print(
                            f"[ORDERBOOK INIT] COLLECTOR LOST "
                            f"{symbol} DURING BRIDGE"
                        )
                        return False

                    # Remove only events already covered by snapshot.
                    while state["buffer"]:
                        try:
                            first_u = int(state["buffer"][0]["u"])
                        except (
                            KeyError,
                            TypeError,
                            ValueError,
                        ):
                            state["buffer"].popleft()
                            continue

                        if first_u <= snapshot_id:
                            state["buffer"].popleft()
                        else:
                            break

                    events = list(state["buffer"])

                    first_U = None
                    first_u = None
                    first_pu = None
                    bridge_mode = None
                    sequence_error = None

                    if events:
                        try:
                            first = events[0]
                            first_U = int(first["U"])
                            first_u = int(first["u"])
                            first_pu = first.get("pu")

                            if first_pu is not None:
                                first_pu = int(first_pu)

                            if first_U <= target_id <= first_u:
                                bridge_mode = "OVERLAP"

                            elif first_pu == snapshot_id:
                                bridge_mode = "PU_CONTINUITY"

                            else:
                                # The first available event cannot
                                # bridge this snapshot. A newer
                                # snapshot is required; waiting for
                                # this same event cannot fix a gap.
                                sequence_error = "FIRST_EVENT_NOT_BRIDGED"

                        except (
                            KeyError,
                            TypeError,
                            ValueError,
                        ):
                            sequence_error = "INVALID_EVENT_IDS"

                    if bridge_mode is not None:
                        valid_events = []
                        previous_u = snapshot_id

                        for index, event in enumerate(events):
                            try:
                                event_U = int(event["U"])
                                event_u = int(event["u"])
                                event_pu = event.get("pu")

                                if event_pu is not None:
                                    event_pu = int(event_pu)

                            except (
                                KeyError,
                                TypeError,
                                ValueError,
                            ):
                                sequence_error = "INVALID_EVENT_IDS"
                                break

                            # Ignore events already covered by the
                            # snapshot or by an earlier event.
                            if event_u <= previous_u:
                                continue

                            if index == 0:
                                overlaps = (
                                    event_U <= target_id <= event_u
                                )
                                continues = event_pu == snapshot_id

                                if not (overlaps or continues):
                                    sequence_error = (
                                        "FIRST_EVENT_NOT_BRIDGED"
                                    )
                                    break

                            elif event_pu != previous_u:
                                sequence_error = "SEQUENCE_GAP"
                                break

                            valid_events.append(event)
                            previous_u = event_u

                        if sequence_error is None:
                            # Install snapshot and rebuild FIFO baseline.
                            state["bids"] = snapshot_bids.copy()
                            state["asks"] = snapshot_asks.copy()
                            # Snapshot replaces the whole book; force one
                            # fresh best-price calculation on the next event.
                            state["_best_price_cache_valid"] = False
                            state["_cached_best_bid"] = None
                            state["_cached_best_ask"] = None
                            state["_cached_market_reference"] = None

                            state["liquidity_lots"]["bid"].clear()
                            state["liquidity_lots"]["ask"].clear()

                            snapshot_time = now_ms()

                            for price, quantity in snapshot_bids.items():
                                state["liquidity_lots"]["bid"][price].append({
                                    "lot_id": str(uuid.uuid4()),
                                    "original_qty": quantity,
                                    "remaining_qty": quantity,
                                    "time": snapshot_time,
                                    "origin": "snapshot",
                                    "update_id": snapshot_id,
                                })

                            for price, quantity in snapshot_asks.items():
                                state["liquidity_lots"]["ask"][price].append({
                                    "lot_id": str(uuid.uuid4()),
                                    "original_qty": quantity,
                                    "remaining_qty": quantity,
                                    "time": snapshot_time,
                                    "origin": "snapshot",
                                    "update_id": snapshot_id,
                                })

                            state["last_update_id"] = snapshot_id

                            for event in valid_events:
                                apply_orderbook_event(symbol, event)

                            state["last_update_id"] = previous_u
                            state["last_depth_update_id"] = previous_u
                            state["last_depth_event_time"] = now_ms()
                            state["buffer"].clear()
                            state["initialized"] = True
                            state["resyncing"] = False
                            state["sequence_errors"] = 0

                            print(
                                f"[ORDERBOOK] SYNCED {symbol} "
                                f"mode={bridge_mode} "
                                f"updateId={previous_u} "
                                f"snapshotId={snapshot_id} "
                                f"replayed={len(valid_events)} "
                                f"bids={len(state['bids'])} "
                                f"asks={len(state['asks'])}"
                            )

                            return True

                    if sequence_error is not None:
                        print(
                            f"[ORDERBOOK INIT] SEQUENCE INVALID "
                            f"{symbol} reason={sequence_error} "
                            f"snapshot_id={snapshot_id} "
                            f"buffer_count={len(events)} "
                            f"first_U={first_U} "
                            f"first_u={first_u} "
                            f"first_pu={first_pu}"
                        )

                        restart_with_fresh_snapshot = True

                    elapsed = (
                        time.monotonic() - bridge_started
                    )

                    if (
                        not events
                        and elapsed >= BRIDGE_TIMEOUT_SECONDS
                    ):
                        # Crucial recovery behavior:
                        # the stream may still be catching up to
                        # this snapshot. Do not continually fetch
                        # newer snapshots while the buffer is empty.
                        if (
                            last_wait_log == 0.0
                            or time.monotonic() - last_wait_log
                            >= BRIDGE_TIMEOUT_SECONDS
                        ):
                            print(
                                f"[ORDERBOOK INIT] WAITING FOR "
                                f"STREAM CATCH-UP {symbol} "
                                f"snapshot_id={snapshot_id} "
                                f"buffer_count=0 "
                                f"wait={elapsed:.2f}s"
                            )
                            last_wait_log = time.monotonic()

                    elif elapsed >= BRIDGE_TIMEOUT_SECONDS:
                        # A non-empty but unbridgeable buffer needs
                        # a fresh snapshot instead of an endless wait.
                        restart_with_fresh_snapshot = True

                if restart_with_fresh_snapshot:
                    snapshot = None
                    snapshot_id = None
                    snapshot_bids = None
                    snapshot_asks = None
                    bridge_started = None

                    delay = min(
                        RETRY_BASE_SECONDS
                        * (2 ** min(attempt - 1, 4)),
                        RETRY_MAX_SECONDS,
                    )

                    print(
                        f"[ORDERBOOK INIT] RETRY {symbol} "
                        f"reason=BRIDGE_INVALID delay={delay:.2f}s"
                    )
                    time.sleep(delay)
                    break

                time.sleep(BRIDGE_CHECK_INTERVAL)

    except Exception as exc:
        print(
            f"[ORDERBOOK INIT] UNHANDLED ERROR {symbol}: "
            f"{type(exc).__name__}: {exc}"
        )

        with symbol_locks[symbol]:
            state = orderbook[symbol]
            state["initialized"] = False
            state["last_update_id"] = None
            state["last_depth_update_id"] = None
            state["bids"].clear()
            state["asks"].clear()
            state["_best_price_cache_valid"] = False
            state["_cached_best_bid"] = None
            state["_cached_best_ask"] = None
            state["_cached_market_reference"] = None
            state["liquidity_lots"]["bid"].clear()
            state["liquidity_lots"]["ask"].clear()

        return False

    finally:
        restart_initializer = False

        with symbol_locks[symbol]:
            state = orderbook[symbol]
            state["_init_worker_active"] = False

            if state.get("initialized", False):
                state["resyncing"] = False

            elif (
                collector_state.get("connected", False)
                and state.get("buffer")
            ):
                state["resyncing"] = True
                restart_initializer = True

            else:
                state["resyncing"] = False

        if restart_initializer:
            threading.Thread(
                target=initialize_orderbook,
                args=(symbol,),
                name=f"orderbook-init-{symbol}-recovery",
                daemon=True,
            ).start()


def handle_depth_update(symbol, event):
    """
    Buffer, validate and apply Binance Futures depth events.
    Adds low-volume diagnostics to isolate orderbook bridge timeouts.
    Preserves existing sequence validation and recovery behavior.
    """
    if symbol not in orderbook or not isinstance(event, dict):
        return

    try:
        event_first_id = int(event["U"])
        event_final_id = int(event["u"])
        event_previous_id = event.get("pu")

        if event_previous_id is not None:
            event_previous_id = int(event_previous_id)

        if event_first_id <= 0 or event_final_id < event_first_id:
            raise ValueError("Invalid depth update IDs")

    except (KeyError, TypeError, ValueError) as exc:
        print(
            f"[ORDERBOOK] INVALID DEPTH EVENT "
            f"{symbol}: {type(exc).__name__}: {exc}"
        )
        with symbol_locks[symbol]:
            orderbook[symbol]["sequence_errors"] += 1
        return

    start_initializer = False
    reason = None

    with measured_lock("depth_apply", symbol):
        state = orderbook[symbol]

        # Count events that actually reach this handler.
        handler_calls = state.get("_depth_handler_calls", 0) + 1
        state["_depth_handler_calls"] = handler_calls
        state["last_depth_event_time"] = now_ms()

        # Buffer events until a snapshot has been synchronized.
        if not state["initialized"]:
            state["buffer"].append(event)

            while len(state["buffer"]) > 2000:
                state["buffer"].popleft()

            if not state.get("_init_worker_active", False):
                state["resyncing"] = True
                start_initializer = True
                reason = "UNINITIALIZED"

        else:
            previous_u = state.get("last_update_id")

            # A synchronized book must have a valid update ID.
            if previous_u is None:
                state["initialized"] = False
                state["resyncing"] = True
                state["sequence_errors"] += 1
                state["buffer"].clear()
                state["buffer"].append(event)

                start_initializer = not state.get(
                    "_init_worker_active", False
                )
                reason = "MISSING_LAST_UPDATE_ID"

            # Ignore already-applied or older updates.
            elif event_final_id <= int(previous_u):
                return

            # Futures continuity: pu must match the prior final ID.
            elif (
                event_previous_id is not None
                and event_previous_id != int(previous_u)
            ):
                state["sequence_errors"] += 1
                state["initialized"] = False
                state["resyncing"] = True

                # Preserve the event that triggered recovery.
                state["buffer"].clear()
                state["buffer"].append(event)

                start_initializer = not state.get(
                    "_init_worker_active", False
                )
                reason = "SEQUENCE_GAP"

                print(
                    f"[ORDERBOOK] SEQUENCE GAP {symbol}: "
                    f"expected_pu={previous_u} "
                    f"received_pu={event_previous_id} "
                    f"event_U={event_first_id} "
                    f"event_u={event_final_id}"
                )

            # Apply only a continuous event.
            else:
                try:
                    apply_orderbook_event(symbol, event)

                except Exception as exc:
                    state["initialized"] = False
                    state["resyncing"] = True
                    state["sequence_errors"] += 1
                    state["buffer"].clear()
                    state["buffer"].append(event)

                    start_initializer = not state.get(
                        "_init_worker_active", False
                    )
                    reason = "APPLY_ERROR"

                    print(
                        f"[ORDERBOOK] APPLY ERROR {symbol}: "
                        f"{type(exc).__name__}: {exc}"
                    )

        # Log every 100 handler calls per symbol, not every event.
        if handler_calls % 100 == 0:
            print(
                f"[DEPTH HANDLER TRACE] {symbol} "
                f"calls={handler_calls} "
                f"initialized={state.get('initialized')} "
                f"resyncing={state.get('resyncing')} "
                f"init_active={state.get('_init_worker_active', False)} "
                f"buffer_count={len(state['buffer'])} "
                f"last_update_id={state.get('last_update_id')} "
                f"event_U={event_first_id} "
                f"event_u={event_final_id} "
                f"event_pu={event_previous_id} "
                f"sequence_errors={state.get('sequence_errors', 0)}"
            )

    # Start recovery outside the shared lock.
    if start_initializer:
        print(
            f"[ORDERBOOK RECOVERY] START {symbol} "
            f"reason={reason}"
        )

        thread = threading.Thread(
            target=initialize_orderbook,
            args=(symbol,),
            name=f"orderbook-init-{symbol}-recovery",
            daemon=True,
        )
        thread.start()


# ============================================================
# VALUE AREA
# ============================================================

def calculate_value_area(
    footprint,
    value_area_percent=0.70
):

    if not footprint:
        return None, None, 0.0


    levels = sorted(
        footprint,
        key=lambda x: x["price"]
    )


    total_volume = sum(
        level["volume"]
        for level in levels
    )


    if total_volume <= 0:
        return None, None, 0.0


    target_volume = (
        total_volume
        * value_area_percent
    )


    poc_index = max(
        range(len(levels)),
        key=lambda i:
            levels[i]["volume"]
    )


    included = {
        poc_index
    }


    value_area_volume = (
        levels[poc_index]["volume"]
    )


    lower = poc_index - 1
    upper = poc_index + 1


    while value_area_volume < target_volume:

        lower_volume = (
            levels[lower]["volume"]
            if lower >= 0
            else -1
        )

        upper_volume = (
            levels[upper]["volume"]
            if upper < len(levels)
            else -1
        )


        if (
            lower_volume < 0
            and upper_volume < 0
        ):
            break


        if upper_volume >= lower_volume:

            included.add(upper)

            value_area_volume += (
                upper_volume
            )

            upper += 1

        else:

            included.add(lower)

            value_area_volume += (
                lower_volume
            )

            lower -= 1


    vah = max(
        levels[i]["price"]
        for i in included
    )


    val = min(
        levels[i]["price"]
        for i in included
    )


    return (
        vah,
        val,
        value_area_volume
    )
    # ============================================================
# FINALIZE CANDLE
# ============================================================

def finalize_candle(candle):
    """
    Candle ko API-friendly format mein finalize karta hai.
    Footprint levels price ascending order mein bheje jayenge.
    """

    if candle is None:
        return None

    result = dict(candle)

    footprint = list(
        candle["footprint"].values()
    )

    footprint.sort(
        key=lambda x: x["price"]
    )

    result["footprint"] = footprint

    # --------------------------------------------------------
    # POC
    # --------------------------------------------------------

    poc = None

    if footprint:
        poc = max(
            footprint,
            key=lambda x: x["volume"]
        )

    if poc:
        result["poc"] = poc["price"]
        result["poc_volume"] = poc["volume"]
    else:
        result["poc"] = None
        result["poc_volume"] = 0.0

    # --------------------------------------------------------
    # VALUE AREA
    # --------------------------------------------------------

    vah, val, value_area_volume = calculate_value_area(
        result["footprint"],
        0.70
    )

    result["vah"] = vah
    result["val"] = val
    result["value_area_volume"] = value_area_volume
    result["value_area_percent"] = 0.70

    return result


# ============================================================
# TURSO SAVE
# ============================================================

def save_candle_to_turso(candle):
    if not TURSO_DATABASE_URL or not TURSO_AUTH_TOKEN:
        return

    try:
        footprint_json = json.dumps(
            candle.get("footprint", []),
            separators=(",", ":")
        )

        sql = """
        INSERT OR REPLACE INTO candles (
            symbol, tf, time, open, high, low, close, delta, totalVol,
            buyVol, sellVol, trades, poc, pocVol, vah, val, valueAreaVol, footprint
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        args = (
            candle["symbol"],
            candle["timeframe"],
            candle["start"],
            candle["open"],
            candle["high"],
            candle["low"],
            candle["close"],
            candle["delta"],
            candle["volume"],
            candle["buy_volume"],
            candle["sell_volume"],
            candle["trades"],
            candle.get("poc"),
            candle.get("poc_volume"),
            candle.get("vah"),
            candle.get("val"),
            candle.get("value_area_volume"),
            footprint_json
        )

        turso_write_queue.put({
            "sql": sql,
            "args": args
        })

    except Exception as e:
        print(f"[TURSO] Candle queue failed: {e}")


def _write_candle_to_turso(sql, args):
    global turso_client

    try:
        if turso_client is None:
            turso_client = create_client_sync(
                TURSO_DATABASE_URL,
                auth_token=TURSO_AUTH_TOKEN
            )

        turso_client.execute(sql, args)
        return True

    except Exception as e:
        print(f"[TURSO] Background candle write failed: {e}")

        try:
            if turso_client is not None:
                turso_client.close()
        except Exception:
            pass

        turso_client = None
        return False


def _run_turso_cleanup():
    global turso_client

    try:
        if turso_client is None:
            turso_client = create_client_sync(
                TURSO_DATABASE_URL,
                auth_token=TURSO_AUTH_TOKEN
            )

        cutoff = int(time.time() * 1000) - (3 * 24 * 60 * 60 * 1000)

        turso_client.execute(
            "DELETE FROM candles WHERE time < ?",
            (cutoff,)
        )

        print("[TURSO] Cleanup completed")

    except Exception as e:
        print(f"[TURSO] Cleanup failed: {e}")

        try:
            if turso_client is not None:
                turso_client.close()
        except Exception:
            pass

        turso_client = None


def _turso_worker_loop():
    print("[TURSO] Background worker started")

    while True:
        job = None

        try:
            job = turso_write_queue.get()

            if job is None:
                turso_write_queue.task_done()
                break

            if job.get("cleanup"):
                _run_turso_cleanup()
            else:
                _write_candle_to_turso(
                    job["sql"],
                    job["args"]
                )

        except Exception as e:
            print(f"[TURSO] Worker error: {e}")

        finally:
            if job is not None:
                try:
                    turso_write_queue.task_done()
                except Exception:
                    pass

def ensure_turso_worker_started():
    global turso_worker_started

    if turso_worker_started:
        return

    with turso_worker_lock:
        if turso_worker_started:
            return

        worker = threading.Thread(
            target=_turso_worker_loop,
            name="turso-worker",
            daemon=True
        )

        worker.start()
        turso_worker_started = True

        print("[TURSO] Background worker initialized")


# ============================================================
# TURSO CLEANUP
# ============================================================

def cleanup_old_turso_candles():
    global last_turso_cleanup

    if not TURSO_DATABASE_URL or not TURSO_AUTH_TOKEN:
        return

    current_time = time.time()

    # Cleanup maximum once every 5 minutes
    if current_time - last_turso_cleanup < 300:
        return

    last_turso_cleanup = current_time

    try:
        turso_write_queue.put({
            "cleanup": True
        })

    except Exception as e:
        print(f"[TURSO] Cleanup queue failed: {e}")

# ============================================================
# STORE FINISHED CANDLE
# ============================================================

def store_finished_candle(candle):

    if candle is None:
        return

    symbol = candle["symbol"]
    timeframe = candle["timeframe"]

    finished = finalize_candle(
        candle
    )

    if finished is None:
        return

    # --------------------------------------------------------
    # RAM STORAGE
    # --------------------------------------------------------

    with symbol_locks[symbol]:

        candles[symbol][timeframe].append(
            finished
        )

        cutoff = (
            time.time()
            - ROLLING_SECONDS
        ) * 1000

        while candles[symbol][timeframe]:

            oldest = candles[
                symbol
            ][timeframe][0]

            if oldest["start"] >= cutoff:
                break

            candles[
                symbol
            ][timeframe].popleft()

    # --------------------------------------------------------
    # TURSO BACKGROUND SAVE
    # --------------------------------------------------------

    ensure_turso_worker_started()

    save_candle_to_turso(
        finished
    )

    # --------------------------------------------------------
    # TURSO CLEANUP
    # Maximum once every 5 minutes
    # --------------------------------------------------------

    cleanup_old_turso_candles()

# ============================================================
# PROCESS TRADE
# ============================================================


def process_trade(
    symbol,
    price,
    quantity,
    trade_time,
    is_buyer_maker,
):
    """
    Ek Binance trade ko saare timeframes mein process karta hai.
    Execution matching aur depth updates ko shared lock se
    serialize karta hai.
    """

    # ----------------------------------------------------
    # EXECUTION MATCHING
    # ----------------------------------------------------
    # handle_depth_update() bhi isi RLock ka use karta hai.
    # Matching state ko concurrent modification se bachao.
    with measured_lock("trade_match", symbol):
        record_trade_for_execution_matching(
            symbol,
            price,
            quantity,
            trade_time,
            is_buyer_maker,
        )

    # ----------------------------------------------------
    # CANDLE PROCESSING
    # ----------------------------------------------------
    with measured_lock("trade_candles", symbol):

        for timeframe, seconds in TIMEFRAMES.items():

            start = floor_timestamp(
                trade_time,
                seconds
            )

            current = current_candles[
                symbol
            ][timeframe]

            # ------------------------------------------------
            # FIRST CANDLE
            # ------------------------------------------------

            if current is None:

                current = create_empty_candle(
                    symbol,
                    timeframe,
                    start,
                    price,
                )

                current_candles[
                    symbol
                ][timeframe] = current

            # ------------------------------------------------
            # NEW CANDLE
            # ------------------------------------------------

            elif start != current["start"]:

                store_finished_candle(
                    current
                )

                current = create_empty_candle(
                    symbol,
                    timeframe,
                    start,
                    price,
                )

                current_candles[
                    symbol
                ][timeframe] = current

            # ------------------------------------------------
            # ADD TRADE
            # ------------------------------------------------

            add_trade_to_candle(
                current,
                price,
                quantity,
                is_buyer_maker,
            )

        # ----------------------------------------------------
        # LAST TRADE INFO
        # ----------------------------------------------------

        last_trade[symbol] = {
            "time": trade_time,
            "price": price,
            "quantity": quantity,
        }


# ============================================================
# BINANCE MESSAGE HANDLER
# ============================================================

def handle_message(ws, message):
    """
    Binance combined-stream message router.

    Depth events ko event type aur payload structure se identify
    karta hai, aur symbol ko data ya stream name se resolve karta hai.
    Existing trade processing aur execution matcher unchanged hain.
    """

    with lock:
        collector_state["raw_message_count"] += 1
        collector_state["last_message_at"] = now_ms()

    try:
        payload = json.loads(message)

        if not isinstance(payload, dict):
            return

        data = payload.get("data", payload)

        if not isinstance(data, dict):
            return

        # Resolve symbol from the event first.
        symbol = str(data.get("s", "")).upper()

        # Fallback for combined-stream wrappers.
        if symbol not in SYMBOLS:
            stream_name = str(payload.get("stream", ""))
            if stream_name:
                stream_symbol = stream_name.split("@", 1)[0].upper()
                if stream_symbol in SYMBOLS:
                    symbol = stream_symbol

        if symbol not in SYMBOLS:
            return

        event_type = data.get("e")

        # Depth payload fallback: U/u are update IDs; b/a are
        # bid/ask updates. Keep validation strict before dispatch.
        is_depth_event = (
            event_type == "depthUpdate"
            or (
                "U" in data
                and "u" in data
                and isinstance(data.get("b"), list)
                and isinstance(data.get("a"), list)
            )
        )

        if is_depth_event:
            try:
                first_id = int(data["U"])
                final_id = int(data["u"])

                if first_id <= 0 or final_id < first_id:
                    raise ValueError(
                        f"Invalid depth update range U={first_id}, u={final_id}"
                    )

            except (KeyError, TypeError, ValueError) as exc:
                with lock:
                    collector_state["invalid_message_count"] += 1

                print(
                    f"[DEPTH ROUTER] INVALID EVENT {symbol}: "
                    f"{type(exc).__name__}: {exc}"
                )
                return

            # Count actual depth events received for each symbol.
            with symbol_locks[symbol]:
                state = orderbook[symbol]
                state["_depth_messages_received"] = (
                    state.get("_depth_messages_received", 0) + 1
                )
                state["_depth_last_message_at"] = now_ms()

                # Log the first event for each symbol after process startup.
                if not state.get("_depth_first_message_logged", False):
                    state["_depth_first_message_logged"] = True
                    print(
                        f"[DEPTH ROUTER] FIRST EVENT {symbol} "
                        f"U={first_id} u={final_id} "
                        f"pu={data.get('pu')}"
                    )

            handle_depth_update(symbol, data)
            return

        # Preserve the existing aggregate-trade processing path.
        if event_type not in ("trade", "aggTrade"):
            return

        try:
            price = float(data.get("p", 0))
            quantity = float(data.get("q", 0))
            trade_time = int(data.get("T", 0))
            is_buyer_maker = bool(data.get("m", False))

        except (TypeError, ValueError):
            with lock:
                collector_state["invalid_message_count"] += 1
            return

        if price <= 0 or quantity <= 0 or trade_time <= 0:
            with lock:
                collector_state["invalid_message_count"] += 1
            return

        with lock:
            collector_state["trade_message_count"] += 1

        process_trade(
            symbol,
            price,
            quantity,
            trade_time,
            is_buyer_maker,
        )

    except Exception as exc:
        with lock:
            collector_state["invalid_message_count"] += 1
            collector_state["error"] = str(exc)

        print(
            f"[COLLECTOR] MESSAGE ERROR: "
            f"{type(exc).__name__}: {exc}"
        )


# ============================================================
# BINANCE CONNECTION
# ============================================================


def collector_loop():
    trade_streams = [
        f"{symbol.lower()}@aggTrade"
        for symbol in SYMBOLS
    ]

    trade_url = (
        "wss://fstream.binance.com/market/stream"
        f"?streams={'/'.join(trade_streams)}"
    )

    connection_state = {
        "trade_connected": False,
        "depth_connected_symbols": set(),
    }

    with lock:
        collector_state["status"] = "connecting"
        collector_state["connected"] = False
        collector_state["started_at"] = now_ms()
        collector_state["trade_stream_connected"] = False
        collector_state["depth_stream_connected"] = False
        collector_state["trade_stream_error"] = None
        collector_state["depth_stream_error"] = None
        collector_state["depth_symbol_errors"] = {
            symbol: None for symbol in SYMBOLS
        }

    def update_connection_status():
        depth_connected = bool(
            connection_state["depth_connected_symbols"]
        )
        trade_connected = connection_state["trade_connected"]

        collector_state["connected"] = depth_connected
        collector_state["trade_stream_connected"] = trade_connected
        collector_state["depth_stream_connected"] = depth_connected

        if trade_connected and depth_connected:
            collector_state["status"] = "connected"
        elif trade_connected or depth_connected:
            collector_state["status"] = "partial"
        else:
            collector_state["status"] = "connecting"

    def invalidate_symbol_orderbook(symbol):
        state = orderbook[symbol]

        was_active = (
            state.get("initialized", False)
            or bool(state.get("bids"))
            or bool(state.get("asks"))
        )

        state["initialized"] = False
        state["resyncing"] = bool(
            state.get("_init_worker_active", False)
        )
        state["last_update_id"] = None
        state["last_depth_update_id"] = None

        state["bids"].clear()
        state["asks"].clear()
        state["_best_price_cache_valid"] = False
        state["_cached_best_bid"] = None
        state["_cached_best_ask"] = None
        state["_cached_market_reference"] = None
        state["liquidity_lots"]["bid"].clear()
        state["liquidity_lots"]["ask"].clear()
        state["buffer"].clear()

        if was_active:
            state["resync_count"] = (
                state.get("resync_count", 0) + 1
            )

        print(
            f"[ORDERBOOK RECOVERY] INVALIDATED {symbol} "
            f"reason=SYMBOL_DEPTH_DISCONNECT "
            f"init_active={state.get('_init_worker_active', False)}"
        )

    def run_stream_worker(role, url, symbol=None):
        is_depth = symbol is not None
        error_key = (
            "depth_stream_error"
            if is_depth
            else "trade_stream_error"
        )

        reconnect_delay = 5
        max_reconnect_delay = 60
        max_queue_size = 5000

        while True:
            connection_started_at = time.time()
            opened = False
            overflowed = False
            accepting_messages = threading.Event()
            accepting_messages.set()

            close_details = {
                "code": None,
                "reason": None,
            }

            processing_queue = queue.Queue(
                maxsize=max_queue_size
            )

            def process_queued_messages():
                while True:
                    message = processing_queue.get()

                    try:
                        if message is None:
                            return

                        started = time.monotonic()

                        try:
                            handle_message(None, message)
                        except Exception as exc:
                            error_text = (
                                f"{type(exc).__name__}: {exc}"
                            )

                            with lock:
                                collector_state["error"] = error_text
                                collector_state[
                                    "invalid_message_count"
                                ] += 1

                            print(
                                f"[COLLECTOR] {role.upper()} "
                                f"PROCESSING ERROR "
                                f"{'symbol=' + symbol + ' ' if is_depth else ''}"
                                f"{error_text}"
                            )

                        elapsed = time.monotonic() - started

                        if elapsed >= 0.1:
                            print(
                                f"[COLLECTOR] {role.upper()} "
                                f"PROCESSING SLOW "
                                f"{'symbol=' + symbol + ' ' if is_depth else ''}"
                                f"duration={elapsed:.3f}s "
                                f"queue={processing_queue.qsize()}"
                            )

                    finally:
                        processing_queue.task_done()

            processing_thread = threading.Thread(
                target=process_queued_messages,
                name=(
                    f"binance-message-processor-{symbol}"
                    if is_depth
                    else "binance-trade-message-processor"
                ),
                daemon=True,
            )
            processing_thread.start()

            def on_open(ws):
                nonlocal opened
                opened = True

                with lock:
                    if is_depth:
                        connection_state[
                            "depth_connected_symbols"
                        ].add(symbol)

                        collector_state[
                            "depth_symbol_errors"
                        ][symbol] = None
                        collector_state["depth_stream_error"] = None
                        collector_state["error"] = None
                    else:
                        connection_state["trade_connected"] = True
                        collector_state["trade_stream_error"] = None

                    update_connection_status()

                print(
                    f"[COLLECTOR] {role.upper()} STREAM CONNECTED"
                    f"{' symbol=' + symbol if is_depth else ''}"
                )

            def on_message(ws, message):
                nonlocal overflowed

                if not accepting_messages.is_set():
                    return

                try:
                    processing_queue.put_nowait(message)
                except queue.Full:
                    overflowed = True
                    accepting_messages.clear()

                    error_text = (
                        f"Processing queue overflow "
                        f"capacity={max_queue_size}"
                    )

                    with lock:
                        collector_state[error_key] = error_text

                        if is_depth:
                            collector_state["error"] = error_text
                            collector_state[
                                "depth_symbol_errors"
                            ][symbol] = error_text

                    print(
                        f"[COLLECTOR] {role.upper()} QUEUE OVERFLOW "
                        f"{'symbol=' + symbol + ' ' if is_depth else ''}"
                        f"capacity={max_queue_size}; closing socket"
                    )

                    try:
                        ws.close()
                    except Exception as exc:
                        print(
                            f"[COLLECTOR] {role.upper()} "
                            f"CLOSE ERROR: {exc}"
                        )

            def on_error(ws, error):
                error_text = (
                    f"{type(error).__name__}: {error}"
                )

                with lock:
                    collector_state[error_key] = error_text

                    if is_depth:
                        collector_state["error"] = error_text
                        collector_state[
                            "depth_symbol_errors"
                        ][symbol] = error_text

                print(
                    f"[COLLECTOR] {role.upper()} STREAM ERROR "
                    f"{'symbol=' + symbol + ' ' if is_depth else ''}"
                    f"{error_text}"
                )

            def on_close(ws, close_status_code, close_msg):
                close_details["code"] = close_status_code
                close_details["reason"] = close_msg
                accepting_messages.clear()

            try:
                print(
                    f"[COLLECTOR] Connecting {role.upper()} "
                    f"{'symbol=' + symbol + ' ' if is_depth else ''}"
                    "stream to Binance Futures..."
                )

                ws = websocket.WebSocketApp(
                    url,
                    on_open=on_open,
                    on_message=on_message,
                    on_error=on_error,
                    on_close=on_close,
                )

                ws.run_forever(
                    ping_interval=30,
                    ping_timeout=25,
                )

            except Exception as exc:
                error_text = (
                    f"{type(exc).__name__}: {exc}"
                )

                with lock:
                    collector_state[error_key] = error_text

                    if is_depth:
                        collector_state["error"] = error_text
                        collector_state[
                            "depth_symbol_errors"
                        ][symbol] = error_text

                print(
                    f"[COLLECTOR] {role.upper()} STREAM EXCEPTION "
                    f"{'symbol=' + symbol + ' ' if is_depth else ''}"
                    f"{error_text}"
                )

            finally:
                accepting_messages.clear()

                # Connection band hone par queued events stale ho sakte hain.
                # Unhe replay karne se reconnect/resync hazaaron messages tak
                # delay ho sakta hai; depth book neeche invalidate/resync hoga.
                dropped_messages = 0
                while True:
                    try:
                        processing_queue.get_nowait()
                    except queue.Empty:
                        break
                    else:
                        processing_queue.task_done()
                        dropped_messages += 1

                if dropped_messages:
                    print(
                        f"[COLLECTOR] {role.upper()} DISCARDING STALE QUEUE "
                        f"{'symbol=' + symbol + ' ' if is_depth else ''}"
                        f"count={dropped_messages}"
                    )

                # Processor mein chal raha max ek message finish karega,
                # phir sentinel se worker cleanly exit karega.
                processing_queue.put(None)
                processing_thread.join()

                close_code = close_details["code"]
                close_reason = close_details["reason"]

                if not close_reason:
                    close_reason = (
                        "Processing queue overflow"
                        if overflowed
                        else "connection ended"
                    )

                with lock:
                    if is_depth:
                        connection_state[
                            "depth_connected_symbols"
                        ].discard(symbol)

                        collector_state[
                            "depth_symbol_errors"
                        ][symbol] = close_reason
                        collector_state["depth_stream_error"] = (
                            close_reason
                        )
                        collector_state["error"] = close_reason

                        # Queue drain hone ke baad hi stale book invalidate.
                        if opened:
                            invalidate_symbol_orderbook(symbol)

                    else:
                        connection_state["trade_connected"] = False
                        collector_state[
                            "trade_stream_connected"
                        ] = False

                        if close_reason:
                            collector_state[
                                "trade_stream_error"
                            ] = close_reason

                    update_connection_status()

                print(
                    f"[COLLECTOR] {role.upper()} STREAM CLOSED "
                    f"{'symbol=' + symbol + ' ' if is_depth else ''}"
                    f"code={close_code}, reason={close_reason}"
                )

            connection_uptime = (
                time.time() - connection_started_at
            )

            if connection_uptime >= 60:
                reconnect_delay = 5

            print(
                f"[COLLECTOR] {role.upper()} reconnecting "
                f"{'symbol=' + symbol + ' ' if is_depth else ''}"
                f"in {reconnect_delay} seconds..."
            )

            time.sleep(reconnect_delay)

            reconnect_delay = min(
                reconnect_delay * 2,
                max_reconnect_delay,
            )

    workers = [
        threading.Thread(
            target=run_stream_worker,
            args=("trade", trade_url, None),
            name="binance-trade-stream",
            daemon=True,
        )
    ]

    for symbol in SYMBOLS:
        depth_url = (
            "wss://fstream.binance.com/public/stream"
            f"?streams={symbol.lower()}@depth@100ms"
        )

        workers.append(
            threading.Thread(
                target=run_stream_worker,
                args=(
                    f"depth-{symbol}",
                    depth_url,
                    symbol,
                ),
                name=f"binance-depth-stream-{symbol}",
                daemon=True,
            )
        )

    for worker in workers:
        worker.start()

    for worker in workers:
        worker.join()


# ============================================================
# COLLECTOR START
# ============================================================

def ensure_collector_started():

    global collector_thread

    if (
        collector_thread is not None
        and collector_thread.is_alive()
    ):
        return

    with collector_start_lock:

        if (
            collector_thread is not None
            and collector_thread.is_alive()
        ):
            return

        collector_thread = threading.Thread(
            target=collector_loop,
            name="binance-trade-collector",
            daemon=True,
        )

        collector_thread.start()

        print(
            "[COLLECTOR] Background collector started"
        )


@app.before_request
def before_request():

    ensure_collector_started()
    # ============================================================
# ROUTES
# ============================================================

@app.route("/")
def home():

    return jsonify({
        "status": "ok",
        "service": "SA Footprint Engine",
        "version": "V3",
        "message": "Trade + Footprint + Turso engine running",
    })


@app.route("/hello")
def hello():

    return "SA Footprint Engine OK"


# ============================================================
# API TEST
# ============================================================

@app.route("/api/test")
def api_test():

    return jsonify({
        "status": "ok",
        "message": "API working",
        "version": "V3",
    })


# ============================================================
# TURSO DATABASE TEST
# ============================================================

@app.route("/api/db-test")
def db_test():
    if not TURSO_DATABASE_URL:
        return jsonify({"ok": False, "error": "Turso env variables missing"}), 500

    try:
        with create_client_sync(TURSO_DATABASE_URL, auth_token=TURSO_AUTH_TOKEN) as client:
            result = client.execute("SELECT COUNT(*) AS count FROM candles")
            count = result.rows[0][0]
            return jsonify({"ok": True, "turso": "connected", "candles": count})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

# ============================================================
# STATUS
# ============================================================

@app.route("/api/status")
def api_status():

    with lock:

        symbol_status = {}

        for symbol in SYMBOLS:

            symbol_status[symbol] = {
                "last_trade_time":
                    last_trade[symbol]["time"],

                "price":
                    last_trade[symbol]["price"],

                "quantity":
                    last_trade[symbol]["quantity"],
            }

        return jsonify({

            "status": "ok",

            "version": "V3",

            "rolling_hours": 24,

            "symbols": symbol_status,

            "timeframes": list(
                TIMEFRAMES.keys()
            ),

            "price_steps": PRICE_STEP,

            "collector": dict(
                collector_state
            ),

                        "process": {
                "thread_alive": (
                    collector_thread is not None
                    and collector_thread.is_alive()
                ),

                "thread_name": (
                    collector_thread.name
                    if collector_thread
                    else None
                ),
            },

            "orderbook": {
                symbol: {
                    "synchronized":
                        orderbook[symbol]["initialized"],

                    "resyncing":
                        orderbook[symbol]["resyncing"],

                    "last_update_id":
                        orderbook[symbol]["last_update_id"],

                    "sequence_errors":
                        orderbook[symbol]["sequence_errors"],

                    "resync_count":
                        orderbook[symbol]["resync_count"],

                    "bid_count":
                        len(orderbook[symbol]["bids"]),

                    "ask_count":
                        len(orderbook[symbol]["asks"]),
                }
                for symbol in SYMBOLS
            },

        })


# ============================================================
# CANDLES API
# ============================================================

@app.route("/api/candles")
def api_candles():

    symbol = (
        request.args
        .get(
            "symbol",
            "BTCUSDT"
        )
        .upper()
    )

    timeframe = (
        request.args
        .get(
            "timeframe",
            "1m"
        )
        .lower()
    )

    try:

        limit = int(
            request.args.get(
                "limit",
                100
            )
        )

    except ValueError:

        limit = 100

    if symbol not in SYMBOLS:

        return jsonify({
            "status": "error",
            "message": "Invalid symbol",
            "allowed": SYMBOLS,
        }), 400

    if timeframe not in TIMEFRAMES:

        return jsonify({
            "status": "error",
            "message": "Invalid timeframe",
            "allowed": list(
                TIMEFRAMES.keys()
            ),
        }), 400

    limit = max(
        1,
        min(
            limit,
            500
        )
    )

    with symbol_locks[symbol]:

        finished = list(
            candles[
                symbol
            ][timeframe]
        )

        current = current_candles[
            symbol
        ][timeframe]

        result = finished[-limit:]

        # ----------------------------------------------------
        # CURRENT LIVE CANDLE
        # ----------------------------------------------------

        if current is not None:

            current_final = finalize_candle(
                current
            )

            if (
                not result
                or current_final["start"]
                != result[-1]["start"]
            ):

                result = (
                    result
                    + [current_final]
                )

        return jsonify({

            "status": "ok",

            "symbol": symbol,

            "timeframe": timeframe,

            "count": len(result),

            "candles": result,

        })


# ============================================================
# SINGLE FOOTPRINT CANDLE
# ============================================================

@app.route("/api/footprint")
def api_footprint():

    symbol = (
        request.args
        .get(
            "symbol",
            "BTCUSDT"
        )
        .upper()
    )

    timeframe = (
        request.args
        .get(
            "timeframe",
            "1m"
        )
        .lower()
    )

    if symbol not in SYMBOLS:

        return jsonify({
            "status": "error",
            "message": "Invalid symbol",
        }), 400

    if timeframe not in TIMEFRAMES:

        return jsonify({
            "status": "error",
            "message": "Invalid timeframe",
        }), 400

    try:

        start = int(
            request.args.get(
                "start"
            )
        )

    except (
        TypeError,
        ValueError
    ):

        return jsonify({
            "status": "error",
            "message":
                "Provide candle start timestamp",
        }), 400

    with symbol_locks[symbol]:

        # ----------------------------------------------------
        # FINISHED CANDLES
        # ----------------------------------------------------

        for candle in candles[
            symbol
        ][timeframe]:

            if candle["start"] == start:

                return jsonify({

                    "status": "ok",

                    "symbol": symbol,

                    "timeframe": timeframe,

                    "candle": candle,

                })

        # ----------------------------------------------------
        # CURRENT CANDLE
        # ----------------------------------------------------

        current = current_candles[
            symbol
        ][timeframe]

        if (
            current is not None
            and current["start"] == start
        ):

            return jsonify({

                "status": "ok",

                "symbol": symbol,

                "timeframe": timeframe,

                "candle":
                    finalize_candle(
                        current
                    ),

            })

    return jsonify({
        "status": "error",
        "message": "Candle not found",
    }), 404


# ============================================================
# SNAPSHOT
# ============================================================

@app.route("/api/snapshot")
def api_snapshot():

    symbol = (
        request.args
        .get(
            "symbol",
            "BTCUSDT"
        )
        .upper()
    )

    timeframe = (
        request.args
        .get(
            "timeframe",
            "1m"
        )
        .lower()
    )

    if symbol not in SYMBOLS:

        return jsonify({
            "status": "error",
            "message": "Invalid symbol",
        }), 400

    if timeframe not in TIMEFRAMES:

        return jsonify({
            "status": "error",
            "message": "Invalid timeframe",
        }), 400

    with symbol_locks[symbol]:

        current = current_candles[
            symbol
        ][timeframe]

        if current is None:

            return jsonify({

                "status": "ok",

                "symbol": symbol,

                "timeframe": timeframe,

                "current_candle": None,

            })

        return jsonify({

            "status": "ok",

            "symbol": symbol,

            "timeframe": timeframe,

            "current_candle":
                finalize_candle(
                    current
                ),

        })


# ============================================================
# LIVE ORDERBOOK
# ============================================================

@app.route("/api/orderbook")
def api_orderbook():

    symbol = (
        request.args
        .get(
            "symbol",
            "BTCUSDT"
        )
        .upper()
    )

    if symbol not in SYMBOLS:

        return jsonify({
            "status": "error",
            "message": "Invalid symbol",
        }), 400

    try:

        limit = int(
            request.args.get(
                "limit",
                20
            )
        )

    except ValueError:

        limit = 20

    limit = max(
        1,
        min(
            limit,
            100
        )
    )

    with symbol_locks[symbol]:

        state = orderbook[symbol]

        bids = sorted(
            state["bids"].items(),
            key=lambda x: float(x[0]),
            reverse=True
        )[:limit]

        asks = sorted(
            state["asks"].items(),
            key=lambda x: float(x[0])
        )[:limit]

        return jsonify({

            "status": "ok",

            "symbol": symbol,

            "synchronized":
                state["initialized"],

            "resyncing":
                state["resyncing"],

            "last_update_id":
                state["last_update_id"],

            "last_depth_update_id":
                state["last_depth_update_id"],

            "last_depth_event_time":
                state["last_depth_event_time"],

            "sequence_errors":
                state["sequence_errors"],

            "resync_count":
                state["resync_count"],

            "bid_count":
                len(state["bids"]),

            "ask_count":
                len(state["asks"]),

            "bids": [
                {
                    "price": float(price),
                    "quantity": quantity,
                }
                for price, quantity in bids
            ],

            "asks": [
                {
                    "price": float(price),
                    "quantity": quantity,
                }
                for price, quantity in asks
            ],
        })

# ============================================================
# LIQUIDITY DEBUG / FIFO EVIDENCE
# ============================================================


@app.route("/api/liquidity-debug")
def liquidity_debug():
    symbol = request.args.get("symbol", "BTCUSDT").upper()

    if symbol not in orderbook:
        return jsonify({
            "error": "invalid_symbol",
            "symbol": symbol
        }), 400

    try:
        finalize_liquidity_records(symbol)
        state = orderbook[symbol]

        history = [
            r for r in list(state.get("liquidity_history", []))
            if isinstance(r, dict)
        ]
        trades = [
            d for d in list(state.get("trade_flow_diagnostics", []))
            if isinstance(d, dict)
        ]
        diagnostics = [
            d for d in list(state.get("match_diagnostics", []))
            if isinstance(d, dict)
        ]

        finalized = [
            r for r in history
            if r.get("finalized") is True
        ]
        executed = [
            r for r in finalized
            if float(r.get("executed_qty") or 0) > 0
        ]
        pulled = [
            r for r in finalized
            if float(r.get("pulled_qty") or 0) > 0
        ]
        reductions = [
            r for r in history
            if float(r.get("reduced_qty") or 0) > 0
        ]

        def short_record(r):
            fifo_consumption = r.get("fifo_consumption") or []

            return {
                "time": r.get("time"),
                "price": r.get("price"),
                "side": r.get("side"),
                "reduced_qty": r.get("reduced_qty"),
                "executed_qty": r.get("executed_qty"),
                "pulled_qty": r.get("pulled_qty"),
                "status": r.get("status"),
                "finalized": r.get("finalized"),
                "fifo_consumed_qty": r.get("fifo_consumed_qty"),
                "fifo_executed_qty": r.get("fifo_executed_qty"),
                "fifo_unattributed_qty": r.get("fifo_unattributed_qty"),
                "fifo_unmatched_qty": r.get("fifo_unmatched_qty"),
                "fifo_consumption_count": len(fifo_consumption),
                "fifo_consumption": [
                    {
                        "lot_id": item.get("lot_id"),
                        "origin": item.get("origin"),
                        "consumed_qty": item.get("consumed_qty"),
                        "execution_qty": item.get("execution_qty"),
                        "remaining_qty_after": item.get("remaining_qty_after"),
                        "unmatched_qty": item.get("unmatched_qty")
                    }
                    for item in fifo_consumption[:5]
                    if isinstance(item, dict)
                ]
            }

        trade_examples = []

        for d in reversed(trades):
            if d.get("event") != "TRADE_ARRIVAL":
                continue

            pending = d.get("pending_same_price_liquidity") or []
            existing = d.get("existing_same_price_trades") or []

            trade_examples.append({
                "event": d.get("event"),
                "price": d.get("price"),
                "trade_time": d.get("time", d.get("trade_time")),
                "expected_side": d.get("expected_side"),
                "is_buyer_maker": d.get("is_buyer_maker"),
                "pending_same_price_count": len(pending),
                "pending_same_price_liquidity": [
                    {
                        "price": r.get("price"),
                        "time": r.get("time"),
                        "reduced_qty": r.get("reduced_qty"),
                        "executed_qty": r.get("executed_qty"),
                        "finalized": r.get("finalized"),
                        "status": r.get("status")
                    }
                    for r in pending[:2]
                    if isinstance(r, dict)
                ],
                "existing_same_price_trade_count": len(existing),
                "pending_liquidity_key_count_before":
                    d.get("pending_liquidity_key_count_before")
            })

            if len(trade_examples) >= 3:
                break

        response = {
            "symbol": symbol,
            "orderbook": {
                "initialized": state.get("initialized"),
                "synchronized": state.get("synchronized"),
                "resyncing": state.get("resyncing"),
                "sequence_errors": state.get("sequence_errors", 0),
                "bid_levels": len(state.get("bids", {})),
                "ask_levels": len(state.get("asks", {}))
            },
            "summary": {
                "history_records": len(history),
                "finalized_records": len(finalized),
                "reduction_records": len(reductions),
                "finalized_with_execution": len(executed),
                "finalized_with_pull": len(pulled),
                "trade_diagnostic_entries": len(trades),
                "match_diagnostic_entries": len(diagnostics)
            },
            "execution_examples": [
                short_record(r) for r in executed[-3:]
            ],
            "recent_reductions": [
                short_record(r) for r in reductions[-3:]
            ],
            "trade_examples": trade_examples,
            "match_diagnostics": diagnostics[-3:]
        }

        return jsonify(response)

    except Exception as e:
        app.logger.exception(
            "liquidity_debug failed for %s", symbol
        )
        return jsonify({
            "error": "liquidity_debug_failed",
            "symbol": symbol,
            "message": str(e)
        }), 500

previous_cluster_state = {
    sym: {"bids": {}, "asks": {}} for sym in SYMBOLS
}
@app.route("/api/scan")
def api_scan():
    symbol = request.args.get("symbol", "BTCUSDT").upper()
    if symbol not in SYMBOLS:
        return jsonify({"status": "error", "message": "Invalid symbol"}), 400

    step = PRICE_STEP.get(symbol, 1.0)
    
    bid_clusters = defaultdict(float)
    ask_clusters = defaultdict(float)

    with symbol_locks[symbol]:
        for p, q in orderbook[symbol]["bids"].items():
            bucket = round_price_to_step(float(p), step)
            bid_clusters[bucket] += q
            
        for p, q in orderbook[symbol]["asks"].items():
            bucket = round_price_to_step(float(p), step)
            ask_clusters[bucket] += q

        current_candle = current_candles[symbol]["1m"]
        footprint_data = current_candle["footprint"] if current_candle else {}

    # Thresholds for detection
    HEAVY_ORDER = 5.0
    MIN_EXECUTION = 1.0

    bids_out = []
    for p, q in sorted(bid_clusters.items(), reverse=True)[:20]:
        executed = footprint_data.get(str(p), {}).get("volume", 0.0)
        delta = footprint_data.get(str(p), {}).get("delta", 0.0)
        
        prev_q = previous_cluster_state[symbol]["bids"].get(p, 0.0)
        
        status = "NORMAL"
        # Spoofing Logic: If previous heavy order vanished without sufficient execution
        if prev_q >= HEAVY_ORDER and q < (prev_q * 0.2) and executed < MIN_EXECUTION:
            status = "LIQUIDITY PULLED (TRAP)"
        # Absorption Logic: Heavy execution but order is still resting
        elif executed >= HEAVY_ORDER:
            status = "ABSORPTION"

        bids_out.append({
            "price_zone": p, 
            "resting_liquidity": round(q, 3), 
            "executed_volume": round(executed, 3),
            "delta": round(delta, 3),
            "status": status
        })
        previous_cluster_state[symbol]["bids"][p] = q

    asks_out = []
    for p, q in sorted(ask_clusters.items())[:20]:
        executed = footprint_data.get(str(p), {}).get("volume", 0.0)
        delta = footprint_data.get(str(p), {}).get("delta", 0.0)
        
        prev_q = previous_cluster_state[symbol]["asks"].get(p, 0.0)
        
        status = "NORMAL"
        if prev_q >= HEAVY_ORDER and q < (prev_q * 0.2) and executed < MIN_EXECUTION:
            status = "LIQUIDITY PULLED (TRAP)"
        elif executed >= HEAVY_ORDER:
            status = "ABSORPTION"

        asks_out.append({
            "price_zone": p, 
            "resting_liquidity": round(q, 3), 
            "executed_volume": round(executed, 3),
            "delta": round(delta, 3),
            "status": status
        })
        previous_cluster_state[symbol]["asks"][p] = q

    # Clean memory of old levels
    for p in list(previous_cluster_state[symbol]["bids"].keys()):
        if p not in bid_clusters:
            # If a heavy order completely disappears, flag it next time or just clear it
            del previous_cluster_state[symbol]["bids"][p]
            
    for p in list(previous_cluster_state[symbol]["asks"].keys()):
        if p not in ask_clusters:
            del previous_cluster_state[symbol]["asks"][p]

    return jsonify({
        "status": "ok",
        "symbol": symbol,
        "cluster_size": step,
        "bids_zone": bids_out,
        "asks_zone": asks_out
    })
@app.route("/api/binance-test")
def binance_test():

    url = "https://fapi4.binance.com/fapi/v1/depth"

    try:
        response = requests.get(
            url,
            params={
                "symbol": "BTCUSDT",
                "limit": 5
            },
            timeout=10,
        )

        return {
            "status_code": response.status_code,
            "server": response.headers.get("server"),
            "content_type": response.headers.get("content-type"),
            "body": response.text[:1000],
        }

    except Exception as exc:

        return {
            "error": str(exc)
        }, 500
# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":

    ensure_collector_started()
    app.run(
        host="0.0.0.0",
        port=10000,
        debug=False,
    )
