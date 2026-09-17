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
    """
    Binance trade timestamp milliseconds mein hota hai.
    Candle timestamps internally milliseconds mein rakhe ja rahe hain.
    """
    return int(ts // (seconds * 1000)) * (seconds * 1000)


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

        cutoff = (time.time() - ROLLING_SECONDS) * 1000

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
                seconds
            )

            current = current_candles[
                symbol
            ][timeframe]

            # ------------------------------------------------
            # First candle
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
            # New candle
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
            # Add trade
            # ------------------------------------------------

            add_trade_to_candle(
                current,
                price,
                quantity,
                is_buyer_maker,
            )


        # ----------------------------------------------------
        # Last trade info
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

    with lock:
        collector_state[
            "raw_message_count"
        ] += 1

        collector_state[
            "last_message_at"
        ] = now_ms()


    try:

        payload = json.loads(message)

        data = payload.get("data", payload)

        if data.get("e") != "trade":
            return


        symbol = str(
            data.get("s", "")
        ).upper()

        if symbol not in SYMBOLS:
            return


        price = float(
            data.get("p", 0)
        )

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
        # Invalid trade protection
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

    streams = "/".join(
        f"{symbol.lower()}@trade"
        for symbol in SYMBOLS
    )

    url = (
        "wss://fstream.binance.com/stream"
        f"?streams={streams}"
    )


    with lock:
        collector_state["status"] = "connecting"
        collector_state["started_at"] = now_ms()


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


            def on_message(ws, message):
                handle_message(
                    ws,
                    message
                )


            def on_error(ws, error):

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
                close_msg,
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
        "version": "V2",
        "message": "Trade + Footprint engine running",
    })


@app.route("/hello")
def hello():
    return "SA Footprint Engine OK"


@app.route("/api/test")
def api_test():

    return jsonify({
        "status": "ok",
        "message": "API working",
        "version": "V2",
    })


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

            "version": "V2",

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

        })


# ============================================================
# CANDLES API
# ============================================================

@app.route("/api/candles")
def api_candles():

    symbol = (
        request.args
        .get("symbol", "BTCUSDT")
        .upper()
    )

    timeframe = (
        request.args
        .get("timeframe", "1m")
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
        min(limit, 500)
    )


    with lock:

        finished = list(
            candles[symbol][timeframe]
        )

        current = current_candles[
            symbol
        ][timeframe]

        # ----------------------------------------------------
        # Current live candle bhi include karenge
        # ----------------------------------------------------

        result = finished[-limit:]


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
                    result + [current_final]
                )


        return jsonify({

            "status": "ok",

            "symbol": symbol,

            "timeframe": timeframe,

            "count": len(result),

            "candles": result,

        })


# ============================================================
# FOOTPRINT SINGLE CANDLE
# ============================================================

@app.route("/api/footprint")
def api_footprint():

    symbol = (
        request.args
        .get("symbol", "BTCUSDT")
        .upper()
    )

    timeframe = (
        request.args
        .get("timeframe", "1m")
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
    except (TypeError, ValueError):

        return jsonify({
            "status": "error",
            "message":
                "Provide candle start timestamp",
        }), 400


    with lock:

        # Finished candles
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


        # Current candle
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
                    finalize_candle(current),
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
        .get("symbol", "BTCUSDT")
        .upper()
    )

    timeframe = (
        request.args
        .get("timeframe", "1m")
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
                finalize_candle(current),
        })


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
