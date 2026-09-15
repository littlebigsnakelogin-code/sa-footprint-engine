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
@app.route("/")
def home():
    return "SA ENGINE IS RUNNING"


@app.route("/hello")
def hello():
    return "SA ENGINE IS RUNNING"


@app.route("/api/test")
def api_test():
    return jsonify({
        "status": "ok",
        "message": "SA Engine API is working"
    })
