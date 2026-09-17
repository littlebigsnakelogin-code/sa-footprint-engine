import encodings.idna

import json
import threading
import time
from collections import defaultdict, deque
from datetime import datetime, timezone

import websocket
from flask import Flask, jsonify, request


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

ROLLING_SECONDS = 24 * 60 * 60


# ------------------------------------------------------------
# Initial footprint price steps
#
# Ye configurable hain.
# Baad mein Binance Futures metadata se automatically
# exact tick size lene ka system add karenge.
# ------------------------------------------------------------

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

# Raw trade-derived candle storage
# candles[symbol][timeframe] = deque(...)
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


# ------------------------------------------------------------
# Statistics
# ------------------------------------------------------------

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


last_trade = {
    symbol: {
        "time": None,
        "price": None,
        "quantity": None,
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
    return int(ts // seconds) * seconds


def round_price_to_step(price, step):
    """
    Price ko footprint bucket mein convert karta hai.

    Example:
    BTC step=1
    75787.63 -> 75787.0

    ETH step=0.1
    2395.82 -> 2395.8
    """

    if step <= 0:
        return price

    bucket = int(price / step)
    result = bucket * step

    # floating-point garbage avoid karne ke liye
    decimals = max(0, len(str(step).split(".")[-1]))
    return round(result, decimals)


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
        "end": start + TIMEFRAMES[timeframe],

        "open": open_price,
        "high": open_price,
        "low": open_price,
        "close": open_price,

        "volume": 0.0,
        "buy_volume": 0.0,
        "sell_volume": 0.0,
        "delta": 0.0,

        "trades": 0,

        # ----------------------------------------------------
        # Footprint
        #
        # {
        #   "price": {
        #       "buy": x,
        #       "sell": y,
        #       "delta": z,
        #       "trades": n
        #   }
        # }
        # ----------------------------------------------------
        "footprint": {},
    }


def add_trade_to_candle(
    candle,
    price,
    quantity,
    is_buyer_maker,
):
    """
    Binance trade:
        m=false -> buyer was NOT maker
                   => aggressive BUY / taker buy

        m=true  -> buyer WAS maker
                   => aggressive SELL / taker sell
    """

    candle["high"] = max(candle["high"], price)
    candle["low"] = min(candle["low"], price)
    candle["close"] = price

    candle["volume"] += quantity
    candle["trades"] += 1

    if is_buyer_maker:
        # aggressive seller
        candle["sell_volume"] += quantity
    else:
        # aggressive buyer
        candle["buy_volume"] += quantity


    candle["delta"] = (
        candle["buy_volume"] -
        candle["sell_volume"]
    )


    # --------------------------------------------------------
    # FOOTPRINT PRICE LEVEL
    # --------------------------------------------------------

    symbol = candle["symbol"]
    step = PRICE_STEP[symbol]

    price_level = round_price_to_step(price, step)

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
        level["buy"] -
        level["sell"]
    )


def finalize_candle(candle):
    """
    Candle ko API-friendly format mein finalize karta hai.

    Footprint levels ko price ascending order mein bhejenge.
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
    # Highest total volume price
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

    return result


def store_finished_candle(candle):
    if candle is None:
        return

    symbol = candle["symbol"]
    timeframe = candle["timeframe"]

    finished = finalize_candle(candle)

    if finished is None:
        return

    with lock:
        candles[symbol][timeframe].append(
            finished
        )

        cutoff = time.time() - ROLLING_SECONDS

        dq = candles[symbol][timeframe]

        while dq and dq[0]["end"] < cutoff:
            dq.popleft()


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
                
