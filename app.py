import encodings.idna

import json
import os
import threading
import time
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


def apply_orderbook_event(symbol, event):
    """
    Ek validated Binance depth event ko local orderbook
    par apply karta hai.
    """

    state = orderbook[symbol]

    for price, quantity in event.get("b", []):

        quantity = float(quantity)

        if quantity == 0:
            state["bids"].pop(price, None)
        else:
            state["bids"][price] = quantity

    for price, quantity in event.get("a", []):

        quantity = float(quantity)

        if quantity == 0:
            state["asks"].pop(price, None)
        else:
            state["asks"][price] = quantity

    state["last_update_id"] = int(event["u"])
    state["last_depth_update_id"] = int(event["u"])
    state["last_depth_event_time"] = now_ms()


def initialize_orderbook(symbol):
    """
    Binance Futures local orderbook synchronization.

    WebSocket events pehle buffer hote hain.
    REST snapshot liya jata hai.
    Snapshot ke baad correct bridging event se
    buffered updates apply kiye jate hain.
    """

    snapshot = fetch_orderbook_snapshot(symbol)

    if snapshot is None:

        with lock:
            state = orderbook[symbol]

            state["resyncing"] = False

            # Keep only the most recent buffered events.
            # This prevents unlimited buffer growth while snapshot access
            # is temporarily unavailable.
            while len(state["buffer"]) > 2000:
                state["buffer"].popleft()

        print(
            f"[ORDERBOOK] Snapshot fetch failed for {symbol}; "
            f"will retry on next depth event."
        )

        return False

    snapshot_last_update_id = int(
        snapshot["lastUpdateId"]
    )

    snapshot_bids = {
        str(price): float(quantity)
        for price, quantity in snapshot.get("bids", [])
        if float(quantity) > 0
    }

    snapshot_asks = {
        str(price): float(quantity)
        for price, quantity in snapshot.get("asks", [])
        if float(quantity) > 0
    }

    with lock:

        state = orderbook[symbol]

        # ----------------------------------------------------
        # Find first buffered event that bridges the snapshot
        # ----------------------------------------------------

        bridge_index = None

        for index, event in enumerate(state["buffer"]):

            event_first_id = int(event["U"])
            event_final_id = int(event["u"])

            if (
                event_first_id
                <= snapshot_last_update_id + 1
                <= event_final_id
            ):
                bridge_index = index
                break

        # ----------------------------------------------------
        # No valid bridge yet
        # ----------------------------------------------------

        if bridge_index is None:

            # Snapshot and buffered events did not overlap.
            # Keep only recent events so the buffer cannot grow
            # indefinitely while waiting for a valid bridge.
            while len(state["buffer"]) > 2000:
                state["buffer"].popleft()

            state["resyncing"] = False

            print(
                f"[ORDERBOOK] No bridge event for "
                f"{symbol}. Retrying sync. "
                f"buffer={len(state['buffer'])}"
            )

            return False

        # ----------------------------------------------------
        # Load REST snapshot
        # ----------------------------------------------------

        state["bids"] = snapshot_bids
        state["asks"] = snapshot_asks

        state["last_update_id"] = (
            snapshot_last_update_id
        )

        # ----------------------------------------------------
        # Apply buffered events from bridge onward
        # ----------------------------------------------------

        buffered_events = list(
            state["buffer"]
        )[bridge_index:]

        previous_u = snapshot_last_update_id

        for event in buffered_events:

            event_first_id = int(event["U"])
            event_final_id = int(event["u"])

            # Old event
            if event_final_id <= snapshot_last_update_id:
                continue

            # First bridging event
            if previous_u == snapshot_last_update_id:

                if not (
                    event_first_id
                    <= snapshot_last_update_id + 1
                    <= event_final_id
                ):
                    state["sequence_errors"] += 1
                    state["resyncing"] = False

                    print(
                        f"[ORDERBOOK] Initial sequence "
                        f"validation failed: {symbol}"
                    )

                    return False

            # Every subsequent event must connect
            else:

                event_previous_id = event.get("pu")

                if event_previous_id is None:

                    state["sequence_errors"] += 1
                    state["resyncing"] = False

                    print(
                        f"[ORDERBOOK] Missing pu during "
                        f"initialization: {symbol}"
                    )

                    return False

                if int(event_previous_id) != previous_u:

                    state["sequence_errors"] += 1
                    state["resyncing"] = False

                    print(
                        f"[ORDERBOOK] Sequence gap detected "
                        f"during initialization: {symbol}"
                    )

                    return False

            apply_orderbook_event(
                symbol,
                event
            )

            previous_u = event_final_id

        # ----------------------------------------------------
        # Clear consumed buffer
        # ----------------------------------------------------

        state["buffer"].clear()

        state["initialized"] = True
        state["resyncing"] = False

        state["last_update_id"] = previous_u
        state["last_depth_update_id"] = previous_u
        state["last_depth_event_time"] = now_ms()

        print(
            f"[ORDERBOOK] SYNCED {symbol} "
            f"updateId={previous_u} "
            f"bids={len(state['bids'])} "
            f"asks={len(state['asks'])}"
        )

        return True

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
    if not TURSO_DATABASE_URL:
        return

    try:
        footprint_json = json.dumps(candle.get("footprint", []), separators=(",", ":"))
        sql = """
        INSERT OR REPLACE INTO candles (
            symbol, tf, time, open, high, low, close, delta, totalVol,
            buyVol, sellVol, trades, poc, pocVol, vah, val, valueAreaVol, footprint
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        args = (
            candle["symbol"], candle["timeframe"], candle["start"], candle["open"],
            candle["high"], candle["low"], candle["close"], candle["delta"],
            candle["volume"], candle["buy_volume"], candle["sell_volume"],
            candle["trades"], candle.get("poc"), candle.get("poc_volume"),
            candle.get("vah"), candle.get("val"), candle.get("value_area_volume"),
            footprint_json
        )

        with create_client_sync(TURSO_DATABASE_URL, auth_token=TURSO_AUTH_TOKEN) as client:
            client.execute(sql, args)

    except Exception as e:
        print(f"[TURSO] Candle save failed: {e}")

# ============================================================
# TURSO CLEANUP
# ============================================================

def cleanup_old_turso_candles():
    if not TURSO_DATABASE_URL:
        return

    try:
        cutoff = int((time.time() - ROLLING_SECONDS) * 1000)
        with create_client_sync(TURSO_DATABASE_URL, auth_token=TURSO_AUTH_TOKEN) as client:
            client.execute("DELETE FROM candles WHERE time < ?", (cutoff,))
        print("[TURSO] Old candles cleaned")
    except Exception as e:
        print(f"[TURSO] Cleanup failed: {e}")

# ============================================================
# STORE FINISHED CANDLE
# ============================================================

def store_finished_candle(candle):

    global last_turso_cleanup

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
    # TURSO SAVE
    # --------------------------------------------------------

    save_candle_to_turso(
        finished
    )

    # --------------------------------------------------------
    # TURSO CLEANUP
    # Maximum once every 5 minutes
    # --------------------------------------------------------

    now = time.time()

    if (
        now - last_turso_cleanup
        >= 300
    ):

        cleanup_old_turso_candles()

        last_turso_cleanup = now
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

    trade_streams = [f"{symbol.lower()}@trade" for symbol in SYMBOLS]
    depth_streams = [f"{symbol.lower()}@depth@100ms" for symbol in SYMBOLS]
    
    streams = "/".join(trade_streams + depth_streams)

    url = (
        "wss://fstream.binance.com/stream"
        f"?streams={streams}"
    )
    with lock:

        collector_state[
            "status"
        ] = "connecting"

        collector_state[
            "started_at"
        ] = now_ms()

    while True:

        try:

            print(
                "[COLLECTOR] Connecting to Binance Futures..."
            )

            def on_open(ws):

                with lock:

                    collector_state[
                        "connected"
                    ] = True

                    collector_state[
                        "status"
                    ] = "connected"

                    collector_state[
                        "error"
                    ] = None

                print(
                    "[COLLECTOR] CONNECTED"
                )

            def on_message(
                ws,
                message
            ):

                handle_message(
                    ws,
                    message
                )

            def on_error(
                ws,
                error
            ):

                with lock:

                    collector_state[
                        "error"
                    ] = str(error)

                    collector_state[
                        "status"
                    ] = "error"

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

                    collector_state[
                        "connected"
                    ] = False

                    collector_state[
                        "status"
                    ] = "closed"

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

            ws.run_forever(
                ping_interval=20,
                ping_timeout=10,
            )

        except Exception as exc:

            with lock:

                collector_state[
                    "connected"
                ] = False

                collector_state[
                    "status"
                ] = "error"

                collector_state[
                    "error"
                ] = str(exc)

            print(
                "[COLLECTOR] EXCEPTION:",
                exc
            )

        print(
            "[COLLECTOR] Reconnecting in 5 seconds..."
        )

        time.sleep(5)


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
# CLOUD-BASED SPOOF & ABSORPTION DETECTOR
# ============================================================

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

    url = "https://fapi3.binance.com/fapi/v1/depth"

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
