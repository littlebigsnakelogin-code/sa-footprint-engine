import encodings.idna

import json
import os
import queue
import threading
import time
import uuid
from collections import defaultdict, deque
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
collector_start_lock = threading.Lock()

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
    Price ko footprint price bucket mein convert karta hai.
    """

    if step <= 0:
        return price

    bucket = int(
        price / step
    )

    result = bucket * step

    decimals = max(
        0,
        len(
            str(step).split(".")[-1]
        )
    )

    return round(
        result,
        decimals
    )


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
    """
    Binance Futures WebSocket API se orderbook snapshot fetch karta hai.

    Important:
    - Snapshot WS connections serialized hain.
    - Ek time par sirf ek symbol snapshot request karega.
    - Socket-level timeout + absolute response deadline.
    - Har failure par socket aur lock guaranteed cleanup.
    - Ek symbol ka stuck snapshot baaki symbols ko indefinitely block nahi karega.
    """

    ws_url = "wss://ws-fapi.binance.com/ws-fapi/v1"

    ws = None
    lock_acquired = False
    started_at = time.time()

    # Maximum time allowed for the complete snapshot operation.
    SNAPSHOT_DEADLINE = 20

    # Maximum time allowed for one websocket receive.
    SOCKET_TIMEOUT = 5

    try:

        print(
            f"[ORDERBOOK WS API] "
            f"SNAPSHOT START {symbol}"
        )

        print(
            f"[ORDERBOOK WS API] "
            f"WAITING SNAPSHOT LOCK {symbol}"
        )

        lock_acquired = snapshot_ws_lock.acquire(
            timeout=15
        )

        if not lock_acquired:

            print(
                f"[ORDERBOOK WS API] "
                f"SNAPSHOT LOCK TIMEOUT {symbol} "
                f"after {time.time() - started_at:.2f}s"
            )

            return None

        print(
            f"[ORDERBOOK WS API] "
            f"SNAPSHOT LOCK ACQUIRED {symbol}"
        )

        request_id = str(uuid.uuid4())

        print(
            f"[ORDERBOOK WS API] "
            f"CONNECTING {symbol}"
        )

        connect_started = time.time()

        ws = websocket.create_connection(
            ws_url,
            timeout=SOCKET_TIMEOUT,
        )

        # Explicitly enforce the receive timeout as well.
        ws.settimeout(SOCKET_TIMEOUT)

        print(
            f"[ORDERBOOK WS API] "
            f"CONNECTED {symbol} "
            f"in {time.time() - connect_started:.2f}s"
        )

        request = {
            "id": request_id,
            "method": "depth",
            "params": {
                "symbol": symbol,
                "limit": ORDERBOOK_SNAPSHOT_LIMIT,
            },
        }

        print(
            f"[ORDERBOOK WS API] "
            f"SENDING DEPTH REQUEST {symbol}"
        )

        send_started = time.time()

        ws.send(
            json.dumps(request)
        )

        print(
            f"[ORDERBOOK WS API] "
            f"REQUEST SENT {symbol} "
            f"in {time.time() - send_started:.2f}s "
            f"id={request_id}"
        )

        recv_started = time.time()
        deadline = time.time() + SNAPSHOT_DEADLINE

        while True:

            remaining = deadline - time.time()

            if remaining <= 0:

                raise TimeoutError(
                    f"snapshot response deadline exceeded "
                    f"after {SNAPSHOT_DEADLINE}s"
                )

            # Never allow a single recv() to consume the
            # entire remaining snapshot deadline.
            ws.settimeout(
                min(
                    SOCKET_TIMEOUT,
                    max(0.1, remaining),
                )
            )

            print(
                f"[ORDERBOOK WS API] "
                f"WAITING RESPONSE {symbol} "
                f"elapsed={time.time() - recv_started:.2f}s "
                f"remaining={remaining:.2f}s"
            )

            try:

                raw_message = ws.recv()

            except websocket.WebSocketTimeoutException:

                if time.time() >= deadline:

                    raise TimeoutError(
                        f"snapshot response deadline exceeded "
                        f"after {SNAPSHOT_DEADLINE}s"
                    )

                print(
                    f"[ORDERBOOK WS API] "
                    f"RECV TIMEOUT {symbol} "
                    f"elapsed={time.time() - recv_started:.2f}s"
                )

                continue

            if not raw_message:

                continue

            response = json.loads(
                raw_message
            )

            print(
                f"[ORDERBOOK WS API] "
                f"RESPONSE RECEIVED {symbol} "
                f"elapsed={time.time() - recv_started:.2f}s"
            )

            if response.get("id") != request_id:

                print(
                    f"[ORDERBOOK WS API] "
                    f"IGNORING OTHER RESPONSE {symbol} "
                    f"response_id={response.get('id')}"
                )

                continue

            if response.get("status") != 200:

                raise RuntimeError(
                    f"WS API error: {response}"
                )

            result = response.get("result")

            if not result:

                raise RuntimeError(
                    f"WS API missing result: {response}"
                )

            if (
                "lastUpdateId" not in result
                or "bids" not in result
                or "asks" not in result
            ):

                raise RuntimeError(
                    f"Invalid WS orderbook snapshot: "
                    f"{response}"
                )

            print(
                f"[ORDERBOOK WS API] "
                f"SNAPSHOT SUCCESS {symbol} "
                f"lastUpdateId={result['lastUpdateId']} "
                f"bids={len(result.get('bids', []))} "
                f"asks={len(result.get('asks', []))} "
                f"total={time.time() - started_at:.2f}s"
            )

            return result

    except Exception as exc:

        print(
            f"[ORDERBOOK WS API] "
            f"SNAPSHOT FAILED {symbol}: "
            f"{type(exc).__name__}: {exc} "
            f"after {time.time() - started_at:.2f}s"
        )

        return None

    finally:

        if ws is not None:

            try:
                ws.close()

                print(
                    f"[ORDERBOOK WS API] "
                    f"SOCKET CLOSED {symbol}"
                )

            except Exception as close_exc:

                print(
                    f"[ORDERBOOK WS API] "
                    f"SOCKET CLOSE ERROR {symbol}: "
                    f"{close_exc}"
                )

        if lock_acquired:

            try:
                snapshot_ws_lock.release()

                print(
                    f"[ORDERBOOK WS API] "
                    f"SNAPSHOT LOCK RELEASED {symbol}"
                )

            except Exception as release_exc:

                print(
                    f"[ORDERBOOK WS API] "
                    f"SNAPSHOT LOCK RELEASE ERROR {symbol}: "
                    f"{release_exc}"
                )


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
    """
    Finalize liquidity-reduction records whose execution-matching window
    has expired.

    Accounting:
        pulled_qty = reduced_qty - executed_qty

    Execution can be matched from both:
        - trades already present before the depth reduction
        - late trades arriving after the reduction

    This function does not classify spoofing or absorption.
    It only finalizes objective liquidity accounting.
    """

    if symbol not in orderbook:
        return 0

    if current_time_ms is None:
        current_time_ms = now_ms()

    finalized_count = 0

    state = orderbook[symbol]

    for record in state["liquidity_history"]:

        if record.get("finalized"):
            continue

        reduced_qty = float(record.get("reduced_qty", 0.0))

        # Only reductions need execution matching/finalization.
        if reduced_qty <= 0.0:
            continue

        record_time = int(record.get("time", current_time_ms))

        # Keep the 1500 ms matching window open.
        if current_time_ms - record_time < 1500:
            continue

        executed_qty = max(
            0.0,
            min(
                reduced_qty,
                float(record.get("executed_qty", 0.0))
            )
        )

        unmatched_qty = max(
            0.0,
            reduced_qty - executed_qty
        )

        pulled_qty = unmatched_qty

        if reduced_qty > 0.0:
            pull_pct = (pulled_qty / reduced_qty) * 100.0
        else:
            pull_pct = 0.0

        record["executed_qty"] = executed_qty
        record["unmatched_qty"] = unmatched_qty
        record["pulled_qty"] = pulled_qty
        record["pull_pct"] = pull_pct
        record["finalized"] = True

        # Pure accounting state.
        if executed_qty > 0.0 and pulled_qty > 0.0:
            record["status"] = "PARTIAL_EXECUTION_PULL"
        elif executed_qty > 0.0:
            record["status"] = "EXECUTED"
        elif pulled_qty > 0.0:
            record["status"] = "LIQUIDITY_PULLED"
        else:
            record["status"] = "REDUCTION_ZERO"

        finalized_count += 1

    return finalized_count


def record_trade_for_execution_matching(
    symbol,
    price,
    quantity,
    trade_time,
    is_buyer_maker,
):
    """
    Recent aggressive trade ko execution matching ke liye store karta hai.

    Matching performance optimization:
        trade_index[(side, price)] -> recent trade records

    Accounting semantics unchanged:
        True  -> aggressive SELL -> bid consume
        False -> aggressive BUY  -> ask consume
    """

    state = orderbook[symbol]

    trade_time = int(trade_time)
    price = float(price)
    quantity = float(quantity)

    # ========================================================
    # FINALIZE OLD REDUCTIONS FIRST
    # ========================================================

    finalize_liquidity_records(
        symbol,
        trade_time
    )

    # ========================================================
    # TRADE RECORD
    # ========================================================

    trade_record = {
        "time": trade_time,
        "price": price,
        "quantity": quantity,
        "remaining_qty": quantity,
        "is_buyer_maker": bool(is_buyer_maker),
    }

    state["trade_history"].append(
        trade_record
    )

    # ========================================================
    # TRADE INDEX
    #
    # Key:
    #     (aggressive side, normalized price)
    #
    # True  -> bid
    # False -> ask
    # ========================================================

    trade_index = state.setdefault(
        "trade_match_index",
        defaultdict(deque)
    )

    expected_side = (
        "bid" if is_buyer_maker else "ask"
    )

    price_key = round(
        price,
        12
    )

    trade_index[
        (
            expected_side,
            price_key
        )
    ].append(
        trade_record
    )

    # ========================================================
    # MATCH TRADE AGAINST ALREADY RECORDED REDUCTIONS
    #
    # IMPORTANT:
    # Ab poori liquidity_history scan nahi hogi.
    # Sirf same side + same price ke reduction records dekhenge.
    # ========================================================

    liquidity_index = state.get(
        "liquidity_match_index"
    )

    if liquidity_index is None:
        liquidity_index = defaultdict(deque)

        # Existing history ko sirf index initialize karne ke
        # liye ek baar process karo.
        for liquidity in state["liquidity_history"]:

            if liquidity.get(
                "finalized",
                False
            ):
                continue

            if float(
                liquidity.get(
                    "reduced_qty",
                    0.0
                )
            ) <= 0.0:
                continue

            liquidity_side = liquidity.get(
                "side"
            )

            if liquidity_side not in (
                "bid",
                "ask"
            ):
                continue

            liquidity_price = round(
                float(
                    liquidity.get(
                        "price",
                        0.0
                    )
                ),
                12
            )

            liquidity_index[
                (
                    liquidity_side,
                    liquidity_price
                )
            ].append(
                liquidity
            )

        state[
            "liquidity_match_index"
        ] = liquidity_index

    matching_records = liquidity_index.get(
        (
            expected_side,
            price_key
        ),
        ()
    )

    # ========================================================
    # MATCH
    # ========================================================

    for liquidity in matching_records:

        if trade_record["remaining_qty"] <= 0.0:
            break

        if liquidity.get(
            "finalized",
            False
        ):
            continue

        reduced_qty = float(
            liquidity.get(
                "reduced_qty",
                0.0
            )
        )

        executed_qty = float(
            liquidity.get(
                "executed_qty",
                0.0
            )
        )

        remaining_reduction = max(
            reduced_qty
            - executed_qty,
            0.0
        )

        if remaining_reduction <= 0.0:
            continue

        time_difference = abs(
            trade_time
            - int(
                liquidity.get(
                    "time",
                    trade_time
                )
            )
        )

        if time_difference > 1500:
            continue

        matched_qty = min(
            trade_record["remaining_qty"],
            remaining_reduction,
        )

        if matched_qty <= 0.0:
            continue

        # ====================================================
        # UPDATE TOP-LEVEL EXECUTION
        # ====================================================

        new_executed_qty = (
            executed_qty
            + matched_qty
        )

        liquidity["executed_qty"] = (
            new_executed_qty
        )

        liquidity["unmatched_qty"] = max(
            reduced_qty
            - new_executed_qty,
            0.0
        )

        # ====================================================
        # UPDATE FIFO EXECUTION ATTRIBUTION
        # ====================================================

        remaining_fifo_execution = (
            matched_qty
        )

        fifo_consumption = liquidity.get(
            "fifo_consumption",
            []
        )

        for consumption in fifo_consumption:

            if remaining_fifo_execution <= 0.0:
                break

            consumed_qty = float(
                consumption.get(
                    "consumed_qty",
                    0.0
                )
            )

            existing_execution_qty = float(
                consumption.get(
                    "execution_qty",
                    0.0
                )
            )

            fifo_available_qty = max(
                consumed_qty
                - existing_execution_qty,
                0.0
            )

            if fifo_available_qty <= 0.0:

                consumption[
                    "unmatched_qty"
                ] = 0.0

                continue

            allocated_execution = min(
                fifo_available_qty,
                remaining_fifo_execution
            )

            new_fifo_execution_qty = (
                existing_execution_qty
                + allocated_execution
            )

            consumption[
                "execution_qty"
            ] = new_fifo_execution_qty

            consumption[
                "unmatched_qty"
            ] = max(
                consumed_qty
                - new_fifo_execution_qty,
                0.0
            )

            remaining_fifo_execution -= (
                allocated_execution
            )

        # ====================================================
        # EXECUTION NOT ATTRIBUTED TO FIFO LOTS
        # ====================================================

        previous_unattributed_execution = float(
            liquidity.get(
                "fifo_unattributed_execution_qty",
                0.0
            )
        )

        liquidity[
            "fifo_unattributed_execution_qty"
        ] = (
            previous_unattributed_execution
            + remaining_fifo_execution
        )

        # ====================================================
        # RECALCULATE FIFO AGGREGATES
        # ====================================================

        fifo_executed_qty = sum(
            float(
                item.get(
                    "execution_qty",
                    0.0
                )
            )
            for item in fifo_consumption
        )

        fifo_unmatched_qty = sum(
            float(
                item.get(
                    "unmatched_qty",
                    0.0
                )
            )
            for item in fifo_consumption
        )

        liquidity[
            "fifo_executed_qty"
        ] = fifo_executed_qty

        liquidity[
            "fifo_unmatched_qty"
        ] = fifo_unmatched_qty

        # ====================================================
        # CONSUME TRADE REMAINING QUANTITY
        # ====================================================

        trade_record["remaining_qty"] = max(
            trade_record["remaining_qty"]
            - matched_qty,
            0.0
        )


def match_trade_to_liquidity_reduction(
    symbol,
    side,
    price,
    reduced_qty,
    reduction_time,
):
    """
    Existing aggressive trades ko newly observed liquidity reduction
    ke saath match karta hai.

    Performance optimization:
        trade_match_index[(side, price)] se sirf relevant
        recent trades lookup kiye jaate hain.

    Matching rules:
        BID reduction -> aggressive SELL -> is_buyer_maker=True
        ASK reduction -> aggressive BUY  -> is_buyer_maker=False

    Timing:
        Trade maximum 300 ms reduction se pehle ho sakta hai.
        Trade maximum 1500 ms reduction ke baad ho sakta hai.
    """

    if symbol not in orderbook:
        return 0.0

    try:
        price = float(price)
        reduced_qty = float(reduced_qty)
        reduction_time = int(reduction_time)
    except (TypeError, ValueError):
        return 0.0

    if side not in ("bid", "ask"):
        return 0.0

    if price <= 0.0 or reduced_qty <= 0.0:
        return 0.0

    # ============================================================
    # AGGRESSIVE SIDE
    # ============================================================

    expected_is_buyer_maker = (
        True if side == "bid" else False
    )

    # ============================================================
    # ASYMMETRIC TIMING WINDOW
    # ============================================================

    MAX_TRADE_BEFORE_REDUCTION_MS = 300
    MAX_TRADE_AFTER_REDUCTION_MS = 1500

    earliest_allowed = (
        reduction_time
        - MAX_TRADE_BEFORE_REDUCTION_MS
    )

    latest_allowed = (
        reduction_time
        + MAX_TRADE_AFTER_REDUCTION_MS
    )

    remaining_reduction = reduced_qty
    matched_qty = 0.0

    state = orderbook[symbol]

    # ============================================================
    # TRADE INDEX
    #
    # Same side + same price ke trades hi dekho.
    # ============================================================

    trade_index = state.get(
        "trade_match_index"
    )

    if not trade_index:
        return 0.0

    price_key = round(
        price,
        12
    )

    matching_trades = trade_index.get(
        (
            side,
            price_key
        ),
        ()
    )

    # ============================================================
    # MATCH RECENT TRADES
    # ============================================================

    for trade in matching_trades:

        if remaining_reduction <= 0.0:
            break

        trade_time = trade.get(
            "time"
        )

        if trade_time is None:
            continue

        trade_time = int(
            trade_time
        )

        # --------------------------------------------------------
        # TIME FILTER
        # --------------------------------------------------------

        if trade_time < earliest_allowed:
            continue

        if trade_time > latest_allowed:
            continue

        # --------------------------------------------------------
        # SIDE FILTER
        # --------------------------------------------------------

        if bool(
            trade.get(
                "is_buyer_maker",
                False
            )
        ) != expected_is_buyer_maker:
            continue

        # --------------------------------------------------------
        # PRICE FILTER
        #
        # Index already price-based hai, lekin defensive check
        # preserve kar rahe hain.
        # --------------------------------------------------------

        try:
            trade_price = float(
                trade.get(
                    "price",
                    0.0
                )
            )
        except (TypeError, ValueError):
            continue

        if abs(
            trade_price - price
        ) > 1e-6:
            continue

        # --------------------------------------------------------
        # REMAINING TRADE QUANTITY
        # --------------------------------------------------------

        try:
            available_trade_qty = float(
                trade.get(
                    "remaining_qty",
                    0.0
                )
            )
        except (TypeError, ValueError):
            continue

        if available_trade_qty <= 0.0:
            continue

        # --------------------------------------------------------
        # MATCH QUANTITY
        # --------------------------------------------------------

        execution_qty = min(
            available_trade_qty,
            remaining_reduction,
        )

        if execution_qty <= 0.0:
            continue

        # --------------------------------------------------------
        # CONSUME TRADE
        # --------------------------------------------------------

        trade["remaining_qty"] = max(
            available_trade_qty
            - execution_qty,
            0.0
        )

        matched_qty += execution_qty

        remaining_reduction -= execution_qty

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

    Evidence model:

        reduced_qty
            = orderbook se LIVE quantity jo gayi

        executed_qty
            = aggressive trade matching se attributed quantity

        unmatched_qty
            = reduction ka abhi unattributed portion

        pulled_qty
            = finalization ke baad non-executed quantity

    FIFO evidence:

        fifo_consumption
            = exactly kaunse observed old lots consume hue

        fifo_unattributed_qty
            = reduction ka woh portion jiske liye
              modeled FIFO ledger mein enough old liquidity nahi thi

    IMPORTANT:

        - LIVE reduction ko kabhi delay nahi kiya jata.
        - Trade matching sirf attribution/evidence ke liye hai.
        - Price approach hone se pehle hui deletion bhi record hoti hai.
        - FIFO yahan observed liquidity additions par based hai.
        - Binance aggregated orderbook individual order IDs nahi deta,
          isliye ye modeled FIFO evidence hai, exchange queue ka exact proof nahi.
    """

    state = orderbook[symbol]

    event_update_id = int(event["u"])
    event_time = int(
        event.get("E", now_ms())
    )

    # ============================================================
    # FINALIZE EXPIRED LIQUIDITY REDUCTIONS
    #
    # Har depth event ke arrival par purane reduction records
    # check karo. Isse finalization sirf naye trade par dependent
    # nahi rahega.
    #
    # Important:
    # Current event ke naye reduction ko ye finalize nahi karega,
    # kyunki uski age abhi 1500 ms se kam hogi.
    # ============================================================

    finalize_liquidity_records(
        symbol,
        event_time
    )

    # ============================================================
    # LIQUIDITY MATCH INDEX
    #
    # Key:
    #     (side, rounded_price)
    #
    # New reduction records isi index mein immediately add honge.
    # Isse next aggressive trade ko poori liquidity_history scan
    # karne ki zarurat nahi padegi.
    # ============================================================

    liquidity_index = state.setdefault(
        "liquidity_match_index",
        defaultdict(deque)
    )

    def process_side(
        side,
        event_levels,
        book,
    ):
        for price, quantity in event_levels:

            price = str(price)
            new_quantity = float(quantity)

            old_quantity = float(
                book.get(price, 0.0)
            )

            added_qty = max(
                new_quantity - old_quantity,
                0.0
            )

            reduced_qty = max(
                old_quantity - new_quantity,
                0.0
            )

            # ====================================================
            # UPDATE LIVE ORDERBOOK FIRST
            # ====================================================

            if new_quantity == 0.0:

                book.pop(
                    price,
                    None
                )

            else:

                book[price] = new_quantity

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

                lots.append({
                    "lot_id": str(uuid.uuid4()),

                    "original_qty": float(
                        added_qty
                    ),

                    "remaining_qty": float(
                        added_qty
                    ),

                    "time": event_time,
                    "origin": "depth_add",
                    "update_id": event_update_id,
                })

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

                        "origin_update_id": oldest_lot.get(
                            "update_id"
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

                        "unmatched_qty": 0.0,
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

                # ------------------------------------------------
                # Agar modeled FIFO ledger mein enough quantity
                # nahi thi, to remainder explicitly record karo.
                # ------------------------------------------------

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

                # ------------------------------------------------
                # EXISTING TRADE KO REDUCTION SE MATCH KARO
                # ------------------------------------------------

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
                # FIFO CONSUMPTION PAR EXECUTION ATTRIBUTE KARO
                #
                # Trade attribution bhi FIFO order mein assign
                # hoga, taaki old liquidity ke execution ko
                # new liquidity ke saath mix na kiya jaye.
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
                # FIFO evidence ko direct event mein preserve karo.
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

                    # Final pull abhi declare nahi kar rahe.
                    "pulled_qty": 0.0,

                    "pull_pct": 0.0,

                    "finalized": False,

                    # --------------------------------------------
                    # FIFO EVIDENCE
                    # --------------------------------------------

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

                    # --------------------------------------------
                    # LIVE STATE
                    # --------------------------------------------

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

                # =================================================
                # INDEX NEW LIQUIDITY MOVEMENT
                #
                # Future aggressive trades ab poori
                # liquidity_history scan nahi karenge.
                #
                # Sirf same side + same price ke pending
                # reduction records dekhe jayenge.
                # =================================================

                if reduced_qty > 0.0:

                    price_key = round(
                        float(price),
                        12
                    )

                    liquidity_index[
                        (
                            side,
                            price_key
                        )
                    ].append(
                        liquidity_record
                    )

    # ============================================================
    # BIDS
    # ============================================================

    process_side(
        "bid",
        event.get("b", []),
        state["bids"],
    )

    # ============================================================
    # ASKS
    # ============================================================

    process_side(
        "ask",
        event.get("a", []),
        state["asks"],
    )

    # ============================================================
    # UPDATE SYNC STATE
    # ============================================================

    state["last_update_id"] = event_update_id

    state["last_depth_update_id"] = event_update_id

    state["last_depth_event_time"] = now_ms()


def initialize_orderbook(symbol):
    """
    Binance Futures local orderbook synchronization.

    Startup/resync flow:

    1. Depth events pehle buffer hote hain.
    2. Binance Futures WS API snapshot liya jata hai.
    3. Snapshot ke lastUpdateId ke saath buffered event bridge
       locate kiya jata hai.
    4. Bridge valid hone par snapshot + buffered events apply hote hain.
    5. FIFO liquidity lots snapshot se seed hote hain.
    6. Sequence invalid hone par attempt safely stop hota hai.
    7. Snapshot failure / timeout par resyncing flag permanently
       stuck nahi hota.
    """

    BRIDGE_WAIT_SECONDS = 10
    BRIDGE_CHECK_INTERVAL = 0.05

    print(
        f"[ORDERBOOK INIT] START {symbol}"
    )

    try:

        with lock:

            state = orderbook[symbol]

            if not collector_state.get(
                "connected",
                False
            ):

                state["initialized"] = False
                state["resyncing"] = False

                print(
                    f"[ORDERBOOK INIT] "
                    f"ABORT {symbol}; "
                    f"collector is not connected."
                )

                return False

        print(
            f"[ORDERBOOK INIT] "
            f"REQUESTING SNAPSHOT {symbol}"
        )

        snapshot = fetch_orderbook_snapshot_ws(
            symbol
        )

        if snapshot is None:

            with lock:

                state = orderbook[symbol]

                state["initialized"] = False
                state["resyncing"] = False

            print(
                f"[ORDERBOOK INIT] "
                f"SNAPSHOT FAILED {symbol}; "
                f"retry will occur from next depth event."
            )

            return False

        snapshot_last_update_id = int(
            snapshot["lastUpdateId"]
        )

        snapshot_time = now_ms()

        snapshot_bids = {
            str(price): float(quantity)
            for price, quantity in snapshot.get(
                "bids",
                []
            )
            if float(quantity) > 0
        }

        snapshot_asks = {
            str(price): float(quantity)
            for price, quantity in snapshot.get(
                "asks",
                []
            )
            if float(quantity) > 0
        }

        print(
            f"[ORDERBOOK INIT] "
            f"SNAPSHOT READY {symbol} "
            f"lastUpdateId={snapshot_last_update_id} "
            f"bids={len(snapshot_bids)} "
            f"asks={len(snapshot_asks)}"
        )

        bridge_wait_started = time.time()

        while True:

            retry_snapshot = False
            sequence_failed = False

            with lock:

                state = orderbook[symbol]

                if not collector_state.get(
                    "connected",
                    False
                ):

                    state["initialized"] = False
                    state["resyncing"] = False

                    print(
                        f"[ORDERBOOK INIT] "
                        f"STOPPED {symbol}; "
                        f"collector disconnected."
                    )

                    return False

                # -------------------------------------------------
                # Remove events that are already covered by snapshot
                # -------------------------------------------------

                while state["buffer"]:

                    oldest_event = state["buffer"][0]

                    if int(
                        oldest_event["u"]
                    ) <= snapshot_last_update_id:

                        state["buffer"].popleft()

                    else:

                        break

                # -------------------------------------------------
                # Find first event that bridges snapshot
                # -------------------------------------------------

                bridge_index = None

                for index, event in enumerate(
                    state["buffer"]
                ):

                    event_first_id = int(
                        event["U"]
                    )

                    event_final_id = int(
                        event["u"]
                    )

                    if (
                        event_first_id
                        <= snapshot_last_update_id + 1
                        <= event_final_id
                    ):

                        bridge_index = index
                        break

                # -------------------------------------------------
                # No bridge yet
                # -------------------------------------------------

                if bridge_index is None:

                    while len(
                        state["buffer"]
                    ) > 2000:

                        state["buffer"].popleft()

                    elapsed = (
                        time.time()
                        - bridge_wait_started
                    )

                    if elapsed >= BRIDGE_WAIT_SECONDS:

                        retry_snapshot = True

                    else:

                        # Lock release ke baad short sleep.
                        retry_snapshot = False

                # -------------------------------------------------
                # Bridge found
                # -------------------------------------------------

                else:

                    state["bids"] = (
                        snapshot_bids.copy()
                    )

                    state["asks"] = (
                        snapshot_asks.copy()
                    )

                    state["last_update_id"] = (
                        snapshot_last_update_id
                    )

                    # ---------------------------------------------
                    # Reset FIFO baseline
                    # ---------------------------------------------

                    state[
                        "liquidity_lots"
                    ][
                        "bid"
                    ].clear()

                    state[
                        "liquidity_lots"
                    ][
                        "ask"
                    ].clear()

                    for price, quantity in (
                        snapshot_bids.items()
                    ):

                        state[
                            "liquidity_lots"
                        ][
                            "bid"
                        ][
                            price
                        ].append({

                            "lot_id": str(
                                uuid.uuid4()
                            ),

                            "original_qty": float(
                                quantity
                            ),

                            "remaining_qty": float(
                                quantity
                            ),

                            "time": snapshot_time,

                            "origin": "snapshot",

                            "update_id":
                                snapshot_last_update_id,
                        })

                    for price, quantity in (
                        snapshot_asks.items()
                    ):

                        state[
                            "liquidity_lots"
                        ][
                            "ask"
                        ][
                            price
                        ].append({

                            "lot_id": str(
                                uuid.uuid4()
                            ),

                            "original_qty": float(
                                quantity
                            ),

                            "remaining_qty": float(
                                quantity
                            ),

                            "time": snapshot_time,

                            "origin": "snapshot",

                            "update_id":
                                snapshot_last_update_id,
                        })

                    # ---------------------------------------------
                    # Apply bridge + all following buffered events
                    # ---------------------------------------------

                    buffered_events = list(
                        state["buffer"]
                    )[bridge_index:]

                    previous_u = (
                        snapshot_last_update_id
                    )

                    valid = True

                    for event in buffered_events:

                        event_first_id = int(
                            event["U"]
                        )

                        event_final_id = int(
                            event["u"]
                        )

                        if (
                            event_final_id
                            <= snapshot_last_update_id
                        ):

                            continue

                        # First event must bridge snapshot.
                        if (
                            previous_u
                            == snapshot_last_update_id
                        ):

                            if not (
                                event_first_id
                                <= snapshot_last_update_id + 1
                                <= event_final_id
                            ):

                                valid = False
                                break

                        # Every following event must chain correctly.
                        else:

                            event_previous_id = (
                                event.get("pu")
                            )

                            if event_previous_id is None:

                                valid = False
                                break

                            if int(
                                event_previous_id
                            ) != previous_u:

                                valid = False
                                break

                        apply_orderbook_event(
                            symbol,
                            event
                        )

                        previous_u = (
                            event_final_id
                        )

                    if not valid:

                        state[
                            "sequence_errors"
                        ] += 1

                        sequence_failed = True

                    else:

                        state[
                            "buffer"
                        ].clear()

                        state[
                            "initialized"
                        ] = True

                        state[
                            "resyncing"
                        ] = False

                        state[
                            "last_update_id"
                        ] = previous_u

                        state[
                            "last_depth_update_id"
                        ] = previous_u

                        state[
                            "last_depth_event_time"
                        ] = now_ms()

                        print(
                            f"[ORDERBOOK] SYNCED "
                            f"{symbol} "
                            f"updateId={previous_u} "
                            f"bids={len(state['bids'])} "
                            f"asks={len(state['asks'])} "
                            f"fifo_bids="
                            f"{len(state['liquidity_lots']['bid'])} "
                            f"fifo_asks="
                            f"{len(state['liquidity_lots']['ask'])}"
                        )

                        return True

            # -----------------------------------------------------
            # Sequence validation failed
            # -----------------------------------------------------

            if sequence_failed:

                print(
                    f"[ORDERBOOK INIT] "
                    f"SEQUENCE FAILED {symbol}; "
                    f"current attempt stopped."
                )

                with lock:

                    state = orderbook[symbol]

                    state["initialized"] = False
                    state["resyncing"] = False

                    state["last_update_id"] = None
                    state["last_depth_update_id"] = None

                    state["bids"].clear()
                    state["asks"].clear()

                    state[
                        "liquidity_lots"
                    ][
                        "bid"
                    ].clear()

                    state[
                        "liquidity_lots"
                    ][
                        "ask"
                    ].clear()

                    state["buffer"].clear()

                return False

            # -----------------------------------------------------
            # Bridge timeout
            # -----------------------------------------------------

            if retry_snapshot:

                print(
                    f"[ORDERBOOK INIT] "
                    f"BRIDGE TIMEOUT {symbol}; "
                    f"stopping current attempt."
                )

                with lock:

                    state = orderbook[symbol]

                    state["initialized"] = False
                    state["resyncing"] = False

                    state["last_update_id"] = None
                    state["last_depth_update_id"] = None

                    state["bids"].clear()
                    state["asks"].clear()

                    state[
                        "liquidity_lots"
                    ][
                        "bid"
                    ].clear()

                    state[
                        "liquidity_lots"
                    ][
                        "ask"
                    ].clear()

                    state["buffer"].clear()

                return False

            time.sleep(
                BRIDGE_CHECK_INTERVAL
            )

    except Exception as exc:

        print(
            f"[ORDERBOOK INIT] "
            f"UNHANDLED ERROR {symbol}: "
            f"{type(exc).__name__}: {exc}"
        )

        return False

    finally:

        # ---------------------------------------------------------
        # Safety net:
        # initializer kisi unexpected exception ki wajah se
        # resyncing=True me permanently stuck na rahe.
        # ---------------------------------------------------------

        with lock:

            state = orderbook[symbol]

            if (
                not state.get("initialized", False)
                and state.get("resyncing", False)
            ):

                state["resyncing"] = False

                print(
                    f"[ORDERBOOK INIT] "
                    f"RESET STALE RESYNC FLAG {symbol}"
                )


def request_orderbook_resync(symbol):

    with lock:

        state = orderbook[symbol]

        # Do not start multiple resync threads for the same symbol.
        if state["resyncing"]:
            return

        state["resyncing"] = True
        state["initialized"] = False

        state["resync_count"] += 1

        # Clear the current local book.
        state["bids"].clear()
        state["asks"].clear()

        state["last_update_id"] = None
        state["last_depth_update_id"] = None

        # Keep only a bounded amount of recent events.
        while len(state["buffer"]) > 2000:
            state["buffer"].popleft()

    print(
        f"[ORDERBOOK] Starting resync for {symbol} "
        f"(attempt #{orderbook[symbol]['resync_count']})"
    )

    thread = threading.Thread(
        target=initialize_orderbook,
        args=(symbol,),
        daemon=True
    )

    thread.start()

def handle_depth_update(symbol, event):
    """
    Binance Futures depth event ko safely process karta hai.
    """

    event_first_id = int(event["U"])
    event_final_id = int(event["u"])

    with lock:

        state = orderbook[symbol]

        state["last_depth_event_time"] = now_ms()

        # ----------------------------------------------------
        # Not initialized yet
        # ----------------------------------------------------

        if not state["initialized"]:

            state["buffer"].append(event)

            # Prevent unlimited buffer growth while waiting
            # for REST snapshot synchronization.
            while len(state["buffer"]) > 2000:
                state["buffer"].popleft()

            # Snapshot synchronization start karo
            if not state["resyncing"]:

                state["resyncing"] = True

                thread = threading.Thread(
                    target=initialize_orderbook,
                    args=(symbol,),
                    name=f"orderbook-init-{symbol}",
                    daemon=True,
                )

                thread.start()

            return
        # ----------------------------------------------------
        # Already initialized
        # ----------------------------------------------------

        previous_u = state["last_update_id"]

        if previous_u is None:

            state["buffer"].append(event)
            state["initialized"] = False

            state["sequence_errors"] += 1

            request_orderbook_resync(symbol)

            return

        # ----------------------------------------------------
        # Ignore old event
        # ----------------------------------------------------

        if event_final_id <= previous_u:
            return

        # ----------------------------------------------------
        # Futures sequence continuity
        # ----------------------------------------------------

        event_previous_id = event.get("pu")

        if (
            event_previous_id is not None
            and int(event_previous_id) != previous_u
        ):

            state["sequence_errors"] += 1

            print(
                f"[ORDERBOOK] SEQUENCE GAP "
                f"{symbol}: "
                f"expected pu={previous_u}, "
                f"received pu={event_previous_id}"
            )

            # Keep this event so resync can potentially use it
            state["buffer"].clear()
            state["buffer"].append(event)

            state["initialized"] = False

            request_orderbook_resync(symbol)

            return

        # ----------------------------------------------------
        # Apply valid event
        # ----------------------------------------------------

        apply_orderbook_event(
            symbol,
            event
        )
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

    with lock:

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
    """
    
    # ----------------------------------------------------
    # EXECUTION MATCHING
    # ----------------------------------------------------

    record_trade_for_execution_matching(
        symbol,
        price,
        quantity,
        trade_time,
        is_buyer_maker,
    )
    
    with lock:

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

def handle_message(
    ws,
    message
):

    with lock:

        collector_state[
            "raw_message_count"
        ] += 1

        collector_state[
            "last_message_at"
        ] = now_ms()

    try:

        payload = json.loads(
            message
        )

        data = payload.get("data", payload)
        event_type = data.get("e")
        symbol = str(data.get("s", "")).upper()

        if symbol not in SYMBOLS:
            return

                # --- ORDERBOOK (DEPTH) UPDATE ---
        if event_type == "depthUpdate":

            handle_depth_update(
                symbol,
                data
            )

            return

        # --- TRADE UPDATE ---
        if event_type != "trade":
            return

        price = float(data.get("p", 0))
        quantity = float(
            data.get("q", 0)
        )

        trade_time = int(
            data.get("T", 0)
        )

        is_buyer_maker = bool(
            data.get("m", False)
        )

        # ----------------------------------------------------
        # INVALID TRADE PROTECTION
        # ----------------------------------------------------

        if (
            price <= 0
            or quantity <= 0
            or trade_time <= 0
        ):

            with lock:

                collector_state[
                    "invalid_message_count"
                ] += 1

            return

        with lock:

            collector_state[
                "trade_message_count"
            ] += 1

        process_trade(
            symbol,
            price,
            quantity,
            trade_time,
            is_buyer_maker,
        )

    except Exception as exc:

        with lock:

            collector_state[
                "invalid_message_count"
            ] += 1

            collector_state[
                "error"
            ] = str(exc)


# ============================================================
# BINANCE CONNECTION
# ============================================================

def collector_loop():

    trade_streams = [
        f"{symbol.lower()}@trade"
        for symbol in SYMBOLS
    ]

    depth_streams = [
        f"{symbol.lower()}@depth@100ms"
        for symbol in SYMBOLS
    ]

    streams = "/".join(
        trade_streams + depth_streams
    )

    url = (
        "wss://fstream.binance.com/stream"
        f"?streams={streams}"
    )

    reconnect_delay = 5
    max_reconnect_delay = 60

    with lock:

        collector_state["status"] = "connecting"
        collector_state["started_at"] = now_ms()

    while True:

        connection_started_at = time.time()

        try:

            print(
                "[COLLECTOR] Connecting to Binance Futures..."
            )

            def on_open(ws):

                nonlocal reconnect_delay

                with lock:

                    collector_state["connected"] = True
                    collector_state["status"] = "connected"
                    collector_state["error"] = None

                reconnect_delay = 5

                print(
                    "[COLLECTOR] CONNECTED"
                )

            def on_message(ws, message):

                handle_message(
                    ws,
                    message
                )

            def on_error(ws, error):

                with lock:

                    collector_state["error"] = str(error)
                    collector_state["status"] = "error"

                print(
                    "[COLLECTOR] ERROR:",
                    error
                )

            def on_close(
                ws,
                close_status_code,
                close_msg
            ):

                with lock:

                    collector_state["connected"] = False
                    collector_state["status"] = "closed"

                    for symbol in SYMBOLS:

                        state = orderbook[symbol]

                        # Current orderbook ko invalid mark karo.
                        # Disconnect ke baad purana book evidence
                        # ke liye use nahi hona chahiye.
                        state["initialized"] = False
                        state["resyncing"] = False

                        state["last_update_id"] = None
                        state["last_depth_update_id"] = None

                        state["bids"].clear()
                        state["asks"].clear()

                        # Purani FIFO liquidity bhi invalid hai.
                        state["liquidity_lots"]["bid"].clear()
                        state["liquidity_lots"]["ask"].clear()

                        # Purane connection ke buffered events
                        # naye connection ke saath mix nahi hone chahiye.
                        state["buffer"].clear()

                print(
                    "[COLLECTOR] CLOSED:",
                    close_status_code,
                    close_msg
                )

            ws = websocket.WebSocketApp(
                url,
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close,
            )

            # Binance server-side ping/pong handle karega.
            # Client-side ping ko disable rakha hai taaki
            # websocket-client ka artificial ping timeout
            # reconnect trigger na kare.
            ws.run_forever(
                ping_interval=0,
                ping_timeout=None,
            )

        except Exception as exc:

            with lock:

                collector_state["connected"] = False
                collector_state["status"] = "error"
                collector_state["error"] = str(exc)

            print(
                "[COLLECTOR] EXCEPTION:",
                exc
            )

        connection_uptime = (
            time.time() - connection_started_at
        )

        # Agar connection reasonably long chala,
        # reconnect delay ko reset rakho.
        if connection_uptime >= 60:
            reconnect_delay = 5

        print(
            f"[COLLECTOR] Reconnecting in "
            f"{reconnect_delay} seconds..."
        )

        time.sleep(reconnect_delay)

        reconnect_delay = min(
            reconnect_delay * 2,
            max_reconnect_delay
        )        

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

    with lock:

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

    with lock:

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

    with lock:

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

    with lock:

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
def api_liquidity_debug():

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
                50
            )
        )

    except ValueError:

        limit = 50

    limit = max(
        1,
        min(
            limit,
            100
        )
    )

    with lock:

        state = orderbook[symbol]

        # Finalize reduction records whose
        # 1500 ms execution-matching window expired.
        finalized_count = finalize_liquidity_records(
            symbol
        )

        raw_history = list(
            state["liquidity_history"]
        )[-limit:]

        raw_diagnostics = list(
            state.get(
                "match_diagnostics",
                []
            )
        )[-10:]

        def compact_record(record):

            if not isinstance(
                record,
                dict
            ):
                return record

            allowed_keys = (
                "event",
                "event_type",
                "type",
                "status",
                "symbol",
                "side",
                "price",
                "time",
                "timestamp",
                "created_at",
                "updated_at",
                "original_qty",
                "initial_qty",
                "reduced_qty",
                "executed_qty",
                "remaining_qty",
                "pulled_qty",
                "unmatched_qty",
                "pull_pct",
                "distance_pct",
                "distance",
                "reason",
                "lot_id",
                "update_id",
            )

            compact = {}

            for key in allowed_keys:

                if key not in record:
                    continue

                value = record[key]

                if isinstance(
                    value,
                    (int, float)
                ):
                    compact[key] = value

                elif isinstance(
                    value,
                    (str, bool)
                ) or value is None:
                    compact[key] = value

            return compact

        history = [
            compact_record(record)
            for record in raw_history
        ]

        match_diagnostics = [
            compact_record(record)
            for record in raw_diagnostics
        ]

        bid_lot_count = sum(
            len(lots)
            for lots in state[
                "liquidity_lots"
            ]["bid"].values()
        )

        ask_lot_count = sum(
            len(lots)
            for lots in state[
                "liquidity_lots"
            ]["ask"].values()
        )

        return jsonify({

            "status":
                "ok",

            "symbol":
                symbol,

            "finalized_count":
                finalized_count,

            "orderbook": {

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

            },

            "fifo": {

                "bid_lot_count":
                    bid_lot_count,

                "ask_lot_count":
                    ask_lot_count,

            },

            "liquidity_history":
                history,

            "match_diagnostics":
                match_diagnostics,

        })


@app.route("/api/scan")
def api_scan():
    symbol = request.args.get("symbol", "BTCUSDT").upper()
    if symbol not in SYMBOLS:
        return jsonify({"status": "error", "message": "Invalid symbol"}), 400

    step = PRICE_STEP.get(symbol, 1.0)
    
    bid_clusters = defaultdict(float)
    ask_clusters = defaultdict(float)

    with lock:
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
