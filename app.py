import os
import encodings.idna
import json
import time
import threading
from collections import defaultdict

import websocket
from flask import Flask, jsonify, request


# ============================================================
# APP
# ============================================================

app = Flask(__name__)


# ============================================================
# CONFIG
# ============================================================

SYMBOLS = [
    "btcusdt",
    "ethusdt",
    "solusdt",
    "xrpusdt",
    "avaxusdt",
    "linkusdt",
    "ltcusdt",
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

ROLLING_SECONDS = 24 * 60 * 60

WS_URL = (
    "wss://fstream.binance.com/stream?streams="
    + "/".join(f"{symbol}@trade" for symbol in SYMBOLS)
)


# ============================================================
# GLOBAL STATE
# ============================================================

lock = threading.Lock()

candles = defaultdict(lambda: defaultdict(list))
current_candles = defaultdict(dict)

last_trade_time = {}
last_price = {}
trade_count = defaultdict(int)

collector_connected = False
collector_status = "starting"
collector_error = None

collector_started_at = None
collector_last_message_at = None


# ============================================================
# HELPERS
# ============================================================

def now_ms():
    return int(time.time() * 1000)


def timeframe_start(timestamp_ms, seconds):
    timestamp_sec = timestamp_ms // 1000
    return (timestamp_sec // seconds) * seconds


def empty_candle(symbol, timeframe, start):
    return {
        "symbol": symbol.upper(),
        "timeframe": timeframe,
        "start": start,
        "end": start + TIMEFRAMES[timeframe],
        "open": None,
        "high": None,
        "low": None,
        "close": None,
        "volume": 0.0,
        "buy_volume": 0.0,
        "sell_volume": 0.0,
        "delta": 0.0,
        "trades": 0,
    }


def update_candle(
    symbol,
    timeframe,
    start,
    price,
    quantity,
    is_buyer_maker,
):
    """
    Binance Futures trade:

    m=True:
        buyer is maker
        aggressive SELL / taker sell

    m=False:
        seller is maker
        aggressive BUY / taker buy
    """

    candle = current_candles[symbol].get(timeframe)

    # --------------------------------------------------------
    # New candle
    # --------------------------------------------------------

    if candle is None or candle["start"] != start:

        if candle is not None:
            candles[symbol][timeframe].append(candle)

        candle = empty_candle(
            symbol,
            timeframe,
            start,
        )

        current_candles[symbol][timeframe] = candle

    # --------------------------------------------------------
    # OHLC
    # --------------------------------------------------------

    if candle["open"] is None:
        candle["open"] = price

    if candle["high"] is None:
        candle["high"] = price
    else:
        candle["high"] = max(
            candle["high"],
            price,
        )

    if candle["low"] is None:
        candle["low"] = price
    else:
        candle["low"] = min(
            candle["low"],
            price,
        )

    candle["close"] = price

    # --------------------------------------------------------
    # Volume
    # --------------------------------------------------------

    candle["volume"] += quantity
    candle["trades"] += 1

    # --------------------------------------------------------
    # Buy / Sell volume
    # --------------------------------------------------------

    if is_buyer_maker:
        candle["sell_volume"] += quantity
    else:
        candle["buy_volume"] += quantity

    # --------------------------------------------------------
    # Delta
    # --------------------------------------------------------

    candle["delta"] = (
        candle["buy_volume"]
        - candle["sell_volume"]
    )


def prune_old_data(symbol):

    cutoff = (
        now_ms() // 1000
        - ROLLING_SECONDS
    )

    for timeframe in TIMEFRAMES:

        arr = candles[symbol][timeframe]

        if arr:

            candles[symbol][timeframe] = [
                candle
                for candle in arr
                if candle["start"] >= cutoff
            ]


# ============================================================
# TRADE PROCESSING
# ============================================================

def process_trade(symbol, data):

    global collector_last_message_at

    try:

        price = float(
            data.get("p", 0)
        )

        quantity = float(
            data.get("q", 0)
        )

        trade_time = int(
            data.get("T", 0)
        )

        # ----------------------------------------------------
        # Reject invalid messages
        # ----------------------------------------------------

        if price <= 0:
            return

        if quantity <= 0:
            return

        if trade_time <= 0:
            return

        is_buyer_maker = bool(
            data.get("m", False)
        )

        # ----------------------------------------------------
        # Update state
        # ----------------------------------------------------

        with lock:

            last_price[symbol] = price

            last_trade_time[symbol] = trade_time

            trade_count[symbol] += 1

            collector_last_message_at = now_ms()

            # ------------------------------------------------
            # Build every timeframe
            # ------------------------------------------------

            for timeframe, seconds in TIMEFRAMES.items():

                start = timeframe_start(
                    trade_time,
                    seconds,
                )

                update_candle(
                    symbol=symbol,
                    timeframe=timeframe,
                    start=start,
                    price=price,
                    quantity=quantity,
                    is_buyer_maker=is_buyer_maker,
                )

            # ------------------------------------------------
            # Keep only 24h
            # ------------------------------------------------

            prune_old_data(symbol)

    except Exception as exc:

        print(
            "[TRADE] processing error: "
            f"{type(exc).__name__}: {exc}",
            flush=True,
        )


# ============================================================
# WEBSOCKET CALLBACKS
# ============================================================

def handle_open(ws):

    global collector_connected
    global collector_status
    global collector_error
    global collector_started_at

    print(
        "[COLLECTOR] CONNECTED TO BINANCE FUTURES",
        flush=True,
    )

    with lock:

        collector_connected = True

        collector_status = "connected"

        collector_error = None

        collector_started_at = now_ms()


def handle_message(ws, message):

    global collector_error

    try:

        data = json.loads(message)

        # Combined stream:
        #
        # {
        #   "stream": "btcusdt@trade",
        #   "data": {...}
        # }

        stream_data = data.get(
            "data",
            data,
        )

        symbol = stream_data.get("s")

        if not symbol:
            return

        symbol = symbol.lower()

        if symbol not in SYMBOLS:
            return

        process_trade(
            symbol,
            stream_data,
        )

    except Exception as exc:

        print(
            "[COLLECTOR] MESSAGE ERROR: "
            f"{type(exc).__name__}: {exc}",
            flush=True,
        )

        with lock:
            collector_error = str(exc)


def handle_error(ws, error):

    global collector_connected
    global collector_status
    global collector_error

    print(
        "[COLLECTOR] WEBSOCKET ERROR: "
        f"{type(error).__name__}: {error}",
        flush=True,
    )

    with lock:

        collector_connected = False

        collector_status = "error"

        collector_error = str(error)


def handle_close(
    ws,
    close_status_code,
    close_msg,
):

    global collector_connected
    global collector_status

    print(
        "[COLLECTOR] WEBSOCKET CLOSED: "
        f"code={close_status_code}, "
        f"message={close_msg}",
        flush=True,
    )

    with lock:

        collector_connected = False

        collector_status = "disconnected"


# ============================================================
# BINANCE COLLECTOR LOOP
# ============================================================

def collector_loop():

    global collector_connected
    global collector_status
    global collector_error

    print(
        "[COLLECTOR] Background collector started",
        flush=True,
    )

    print(
        f"[COLLECTOR] Symbols: "
        f"{','.join(SYMBOLS)}",
        flush=True,
    )

    while True:

        try:

            print(
                "[COLLECTOR] Connecting to Binance Futures...",
                flush=True,
            )

            with lock:

                collector_connected = False

                collector_status = "connecting"

                collector_error = None

            ws = websocket.WebSocketApp(

                WS_URL,

                on_open=handle_open,

                on_message=handle_message,

                on_error=handle_error,

                on_close=handle_close,
            )

            print(
                "[COLLECTOR] Starting run_forever()",
                flush=True,
            )

            ws.run_forever(
                ping_interval=20,
                ping_timeout=10,
                ping_payload="SA_ENGINE",
            )

            print(
                "[COLLECTOR] run_forever() ended",
                flush=True,
            )

        except Exception as exc:

            print(
                "[COLLECTOR] EXCEPTION: "
                f"{type(exc).__name__}: {exc}",
                flush=True,
            )

            with lock:

                collector_connected = False

                collector_status = "exception"

                collector_error = str(exc)

        print(
            "[COLLECTOR] Reconnecting in 5 seconds...",
            flush=True,
        )

        time.sleep(5)


# ============================================================
# HOME
# ============================================================

@app.route("/")
def home():

    return jsonify({
        "name": "SA Footprint Engine",
        "status": "running",
        "version": "V1",
        "collector": collector_status,
        "symbols": [
            symbol.upper()
            for symbol in SYMBOLS
        ],
        "timeframes": list(
            TIMEFRAMES.keys()
        ),
        "rolling_hours": 24,
    })


# ============================================================
# HELLO
# ============================================================

@app.route("/hello")
def hello():

    return "SA Footprint Engine is alive"


# ============================================================
# TEST
# ============================================================

@app.route("/api/test")
def api_test():

    return jsonify({
        "status": "ok",
        "message": "SA Engine API is working",
    })


# ============================================================
# STATUS
# ============================================================

@app.route("/api/status")
def api_status():

    with lock:

        symbols_status = {}

        for symbol in SYMBOLS:

            symbols_status[
                symbol.upper()
            ] = {

                "price":
                    last_price.get(symbol),

                "last_trade_time":
                    last_trade_time.get(symbol),

                "trade_count":
                    trade_count.get(
                        symbol,
                        0,
                    ),
            }

        return jsonify({

            "status": "ok",

            "collector": {

                "connected":
                    collector_connected,

                "status":
                    collector_status,

                "error":
                    collector_error,

                "last_message_at":
                    collector_last_message_at,
            },

            "rolling_hours": 24,

            "symbols":
                symbols_status,

            "timeframes":
                list(TIMEFRAMES.keys()),
        })


# ============================================================
# CANDLES
# ============================================================

@app.route("/api/candles")
def api_candles():

    symbol = request.args.get(
        "symbol",
        "BTCUSDT",
    ).lower()

    timeframe = request.args.get(
        "timeframe",
        "1m",
    )

    try:

        limit = int(
            request.args.get(
                "limit",
                "100",
            )
        )

    except Exception:

        limit = 100

    # --------------------------------------------------------
    # Validate
    # --------------------------------------------------------

    if symbol not in SYMBOLS:

        return jsonify({
            "status": "error",
            "error": "Invalid symbol",
        }), 400

    if timeframe not in TIMEFRAMES:

        return jsonify({
            "status": "error",
            "error": "Invalid timeframe",
        }), 400

    limit = max(
        1,
        min(limit, 1000),
    )

    # --------------------------------------------------------
    # Get candles
    # --------------------------------------------------------

    with lock:

        result = list(
            candles[symbol][timeframe]
        )

        current = (
            current_candles[
                symbol
            ].get(timeframe)
        )

        if current is not None:

            result.append(current)

        result = result[-limit:]

        return jsonify({

            "status": "ok",

            "symbol":
                symbol.upper(),

            "timeframe":
                timeframe,

            "count":
                len(result),

            "candles":
                result,
        })


# ============================================================
# SNAPSHOT
# ============================================================

@app.route("/api/snapshot")
def api_snapshot():

    with lock:

        snapshot = {}

        for symbol in SYMBOLS:

            current = {}

            for timeframe in TIMEFRAMES:

                candle = (
                    current_candles[
                        symbol
                    ].get(timeframe)
                )

                if candle is not None:

                    current[
                        timeframe
                    ] = candle

            snapshot[
                symbol.upper()
            ] = {

                "price":
                    last_price.get(symbol),

                "last_trade_time":
                    last_trade_time.get(symbol),

                "trade_count":
                    trade_count.get(
                        symbol,
                        0,
                    ),

                "current_candles":
                    current,
            }

        return jsonify({

            "status": "ok",

            "collector": {

                "connected":
                    collector_connected,

                "status":
                    collector_status,

                "error":
                    collector_error,
            },

            "symbols":
                snapshot,
        })


# ============================================================
# START COLLECTOR
# ============================================================

collector_thread = threading.Thread(

    target=collector_loop,

    daemon=True,

    name="binance-trade-collector",
)

collector_thread.start()


# ============================================================
# LOCAL DEVELOPMENT
# ============================================================

if __name__ == "__main__":

    app.run(

        host="0.0.0.0",

        port=int(
            os.environ.get(
                "PORT",
                10000,
            )
        ),

        threaded=True,
    )
