import os
import json
import time
import math
import threading
from collections import defaultdict, deque

import websocket
from flask import Flask, jsonify, request


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
    + "/".join(symbol + "@trade" for symbol in SYMBOLS)
)


# ============================================================
# IN-MEMORY DATA
# ============================================================

lock = threading.RLock()

candles = defaultdict(lambda: defaultdict(deque))
current_candles = defaultdict(dict)

last_trade_time = {}
last_price = {}

trade_count = defaultdict(int)

seen_trade_ids = defaultdict(lambda: deque(maxlen=5000))
seen_trade_id_sets = defaultdict(set)

collector_connected = False
collector_status = "starting"
collector_error = None


# ============================================================
# HELPERS
# ============================================================

def now_ms():
    return int(time.time() * 1000)


def clean_number(value):
    try:
        number = float(value)

        if not math.isfinite(number):
            return None

        return number

    except (TypeError, ValueError):
        return None


def bucket_start(timestamp_ms, timeframe_seconds):
    timestamp_sec = timestamp_ms // 1000
    return (timestamp_sec // timeframe_seconds) * timeframe_seconds


def make_empty_candle(start, timeframe_seconds):
    return {
        "start": start,
        "end": start + timeframe_seconds,
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


def candle_to_json(candle):

    if candle is None:
        return None

    return {
        "start": candle["start"],
        "end": candle["end"],
        "open": candle["open"],
        "high": candle["high"],
        "low": candle["low"],
        "close": candle["close"],
        "volume": candle["volume"],
        "buy_volume": candle["buy_volume"],
        "sell_volume": candle["sell_volume"],
        "delta": candle["delta"],
        "trades": candle["trades"],
    }


def prune_old_candles(symbol, timeframe, current_start):
    cutoff = current_start - ROLLING_SECONDS

    history = candles[symbol][timeframe]

    while history and history[0]["start"] < cutoff:
        history.popleft()


# ============================================================
# TRADE PROCESSOR
# ============================================================

def process_trade(symbol, trade):

    price = clean_number(trade.get("p"))
    quantity = clean_number(trade.get("q"))

    trade_id = trade.get("t")
    trade_time = trade.get("T")

    # --------------------------------------------------------
    # Reject invalid Binance messages
    # --------------------------------------------------------

    if price is None or quantity is None:
        return

    if price <= 0 or quantity <= 0:
        return

    if trade_time is None:
        trade_time = now_ms()

    # --------------------------------------------------------
    # Duplicate protection
    # --------------------------------------------------------

    if trade_id is not None:

        if trade_id in seen_trade_id_sets[symbol]:
            return

        old_ids = seen_trade_ids[symbol]

        if len(old_ids) >= old_ids.maxlen:
            old_id = old_ids[0]
            seen_trade_id_sets[symbol].discard(old_id)

        old_ids.append(trade_id)
        seen_trade_id_sets[symbol].add(trade_id)

    # --------------------------------------------------------
    # Aggressor classification
    #
    # m = true
    # Buyer is maker
    # Therefore seller is aggressive/taker
    #
    # m = false
    # Buyer is taker
    # Therefore buyer is aggressive
    # --------------------------------------------------------

    buyer_is_maker = bool(trade.get("m", False))

    if buyer_is_maker:
        buy_volume = 0.0
        sell_volume = quantity
    else:
        buy_volume = quantity
        sell_volume = 0.0

    # --------------------------------------------------------
    # Update market state
    # --------------------------------------------------------

    with lock:

        last_trade_time[symbol] = trade_time
        last_price[symbol] = price

        trade_count[symbol] += 1

        # ----------------------------------------------------
        # Build all timeframes directly from live trades
        # ----------------------------------------------------

        for timeframe, seconds in TIMEFRAMES.items():

            start = bucket_start(
                trade_time,
                seconds
            )

            current = current_candles[symbol].get(timeframe)

            # ------------------------------------------------
            # New candle
            # ------------------------------------------------

            if current is None:

                current = make_empty_candle(
                    start,
                    seconds
                )

                current_candles[symbol][timeframe] = current

            # ------------------------------------------------
            # Timeframe changed
            # ------------------------------------------------

            elif current["start"] != start:

                # Save completed candle
                candles[symbol][timeframe].append(current)

                # Create new candle
                current = make_empty_candle(
                    start,
                    seconds
                )

                current_candles[symbol][timeframe] = current

                prune_old_candles(
                    symbol,
                    timeframe,
                    start
                )

            # ------------------------------------------------
            # Update OHLC
            # ------------------------------------------------

            if current["open"] is None:
                current["open"] = price

            current["high"] = (
                price
                if current["high"] is None
                else max(current["high"], price)
            )

            current["low"] = (
                price
                if current["low"] is None
                else min(current["low"], price)
            )

            current["close"] = price

            # ------------------------------------------------
            # Update volume
            # ------------------------------------------------

            current["volume"] += quantity
            current["buy_volume"] += buy_volume
            current["sell_volume"] += sell_volume

            current["delta"] = (
                current["buy_volume"]
                - current["sell_volume"]
            )

            current["trades"] += 1


# ============================================================
# BINANCE WEBSOCKET
# ============================================================

def handle_message(ws, message):

    global collector_connected
    global collector_status
    global collector_error

    try:

        data = json.loads(message)

        stream_data = data.get("data", data)

        symbol = stream_data.get("s")

        if not symbol:
            return

        symbol = symbol.lower()

        if symbol not in SYMBOLS:
            return

        process_trade(
            symbol,
            stream_data
        )

    except Exception as exc:

        with lock:
            collector_error = str(exc)


def handle_open(ws):

    global collector_connected
    global collector_status
    global collector_error

    with lock:
        collector_connected = True
        collector_status = "connected"
        collector_error = None


def handle_close(ws, close_status_code, close_msg):

    global collector_connected
    global collector_status

    with lock:
        collector_connected = False
        collector_status = "disconnected"


def handle_error(ws, error):

    global collector_connected
    global collector_status
    global collector_error

    with lock:
        collector_connected = False
        collector_status = "error"
        collector_error = str(error)


# ============================================================
# BACKGROUND COLLECTOR
# ============================================================

def collector_loop():

    global collector_connected
    global collector_status
    global collector_error

    while True:

        try:

            with lock:
                collector_status = "connecting"
                collector_connected = False

            ws = websocket.WebSocketApp(
                WS_URL,
                on_open=handle_open,
                on_message=handle_message,
                on_error=handle_error,
                on_close=handle_close,
            )

            ws.run_forever(
                ping_interval=20,
                ping_timeout=10,
            )

        except Exception as exc:

            with lock:
                collector_connected = False
                collector_status = "exception"
                collector_error = str(exc)

        time.sleep(5)


# ============================================================
# API ROUTES
# ============================================================

@app.route("/")
def home():

    return jsonify({
        "service": "SA Footprint Engine",
        "status": "running"
    })


@app.route("/hello")
def hello():

    return "SA ENGINE IS RUNNING"


@app.route("/api/test")
def api_test():

    return jsonify({
        "status": "ok",
        "message": "SA Engine API is working"
    })


@app.route("/api/status")
def api_status():

    with lock:

        symbols_status = {}

        for symbol in SYMBOLS:

            symbols_status[symbol.upper()] = {
                "price": last_price.get(symbol),
                "last_trade_time": last_trade_time.get(symbol),
                "trade_count": trade_count.get(symbol, 0),
            }

        return jsonify({
            "status": "ok",
            "collector": {
                "connected": collector_connected,
                "status": collector_status,
                "error": collector_error,
            },
            "symbols": symbols_status,
            "timeframes": list(TIMEFRAMES.keys()),
            "rolling_hours": 24,
        })


@app.route("/api/candles")
def api_candles():

    symbol = request.args.get(
        "symbol",
        "btcusdt"
    ).lower()

    timeframe = request.args.get(
        "timeframe",
        "1m"
    ).lower()

    limit_raw = request.args.get(
        "limit",
        "100"
    )

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
            "allowed": list(TIMEFRAMES.keys()),
        }), 400

    try:

        limit = int(limit_raw)

    except ValueError:

        limit = 100

    limit = max(1, min(limit, 2000))

    with lock:

        history = list(
            candles[symbol][timeframe]
        )

        current = current_candles[symbol].get(
            timeframe
        )

        result = history[-limit:]

        if current is not None:

            result = result + [current]

        result = result[-limit:]

        return jsonify({
            "status": "ok",
            "symbol": symbol.upper(),
            "timeframe": timeframe,
            "count": len(result),
            "candles": [
                candle_to_json(candle)
                for candle in result
            ],
        })


@app.route("/api/snapshot")
def api_snapshot():

    with lock:

        snapshot = {}

        for symbol in SYMBOLS:

            snapshot[symbol.upper()] = {
                "price": last_price.get(symbol),
                "last_trade_time": last_trade_time.get(symbol),
                "trade_count": trade_count.get(symbol),
                "candles": {},
            }

            for timeframe in TIMEFRAMES:

                current = current_candles[symbol].get(
                    timeframe
                )

                snapshot[symbol.upper()]["candles"][timeframe] = (
                    candle_to_json(current)
                )

        return jsonify({
            "status": "ok",
            "data": snapshot,
        })


# ============================================================
# START BACKGROUND COLLECTOR
# ============================================================

collector_thread = threading.Thread(
    target=collector_loop,
    daemon=True
)

collector_thread.start()


# ============================================================
# LOCAL RUN
# ============================================================

if __name__ == "__main__":

    port = int(
        os.environ.get(
            "PORT",
            "10000"
        )
    )

    app.run(
        host="0.0.0.0",
        port=port
    )
