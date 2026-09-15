import os
import encodings.idna
import json
import time
import threading
from collections import defaultdict

import websocket
from flask import Flask, jsonify, request


# ============================================================
# CONFIG
# ============================================================

app = Flask(__name__)

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


def update_candle(symbol, timeframe, start, price, quantity, is_buyer_maker):
    """
    Binance trade:
        m=True  -> buyer is maker
                   aggressive seller / taker sell

        m=False -> seller is maker
                   aggressive buyer / taker buy
    """

    key = (symbol, timeframe)

    candle = current_candles[symbol].get(timeframe)

    # New candle
    if candle is None or candle["start"] != start:

        if candle is not None:
            candles[symbol][timeframe].append(candle)

        candle = empty_candle(symbol, timeframe, start)
        current_candles[symbol][timeframe] = candle

    # OHLC
    if candle["open"] is None:
        candle["open"] = price

    candle["high"] = (
        price
        if candle["high"] is None
        else max(candle["high"], price)
    )

    candle["low"] = (
        price
        if candle["low"] is None
        else min(candle["low"], price)
    )

    candle["close"] = price

    # Volume
    candle["volume"] += quantity
    candle["trades"] += 1

    # Delta
    if is_buyer_maker:
        # Aggressive SELL
        candle["sell_volume"] += quantity
    else:
        # Aggressive BUY
        candle["buy_volume"] += quantity

    candle["delta"] = (
        candle["buy_volume"] -
        candle["sell_volume"]
    )


def prune_old_data(symbol):
    cutoff = now_ms() // 1000 - ROLLING_SECONDS

    for timeframe in TIMEFRAMES:
        arr = candles[symbol][timeframe]

        if arr:
            candles[symbol][timeframe] = [
                c for c in arr
                if c["start"] >= cutoff
            ]


# ============================================================
# TRADE PROCESSING
# ============================================================


def process_trade(symbol, data):

    global collector_last_message_at

    try:
        price = float(data.get("p", 0))
        quantity = float(data.get("q", 0))
        trade_time = int(data.get("T", 0))

        # Reject invalid Binance messages
        if price <= 0:
            return

        if quantity <= 0:
            return

        if trade_time <= 0:
            return

        is_buyer_maker = bool(data.get("m", False))

        with lock:

            last_price[symbol] = price
            last_trade_time[symbol] = trade_time
            trade_count[symbol] += 1

            collector_last_message_at = now_ms()

            for timeframe, seconds in TIMEFRAMES.items():

                start = timeframe_start(
                    trade_time,
                    seconds
                )

                update_candle(
                    symbol=symbol,
                    timeframe=timeframe,
                    start=start,
                    price=price,
                    quantity=quantity,
                    is_buyer_maker=is_buyer_maker,
                )

            prune_old_data(symbol)

    except Exception as exc:

        print(
            f"[TRADE] processing error: "
            f"{type(exc).__name__}: {exc}",
            flush=True
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
        flush=True
    )

    with lock:

        collector_connected = True
        collector_status = "connected"
        collector_error = None
        collector_started_at = now_ms()


def handle_message(ws, message):

    global collector_error
    global collector_last_message_at

    try:

        data = json.loads(message)

        # Combined stream format:
        #
        # {
        #   "stream": "btcusdt@trade",
        #   "data": {...}
        # }

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

        print(
            f"[COLLECTOR] MESSAGE ERROR: "
            f"{type(exc).__name__}: {exc}",
            flush=True
        )

        with lock:
            collector_error = str(exc)


def handle_error(ws, error):

    global collector_connected
    global collector_status
    global collector_error

    print(
        f"[COLLECTOR] WEBSOCKET ERROR: "
        f"{type(error).__name__}: {error}",
        flush=True
    )

    with lock:

        collector_connected = False
        collector_status = "error"
        collector
