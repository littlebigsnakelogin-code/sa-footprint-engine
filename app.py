import json
import websocket

from flask import Flask, jsonify, request
import requests


app = Flask(__name__)


# ============================================================
# CONFIG
# ============================================================

SPOT_KLINES_URL = (
    "https://data-api.binance.vision/api/v3/klines"
)

FUTURES_WS_BASE = (
    "wss://fstream.binance.com/stream"
)


# ============================================================
# HOME
# ============================================================

@app.route("/")
def home():

    return """
    <!DOCTYPE html>
    <html>
    <head>
        <title>SA Footprint Engine</title>

        <style>

            body {
                background: #111;
                color: #eee;
                font-family: Arial, sans-serif;
                padding: 30px;
            }

            h1 {
                margin-bottom: 10px;
            }

            a {
                color: #4da6ff;
            }

            .box {
                background: #1b1b1b;
                padding: 15px;
                margin-top: 15px;
                border-radius: 8px;
            }

            code {
                color: #7cff9b;
            }

        </style>
    </head>

    <body>

        <h1>SA Footprint Engine</h1>

        <div class="box">
            <b>Server:</b> ONLINE
        </div>

        <div class="box">
            <p>
                Binance Spot diagnostic:
            </p>

            <a href="/api/test" target="_blank">
                /api/test
            </a>
        </div>

        <div class="box">
            <p>
                Spot candles:
            </p>

            <a href="/api/candles?symbol=BTCUSDT&interval=1m&limit=5"
               target="_blank">
                /api/candles
            </a>
        </div>

        <div class="box">
            <p>
                Binance Futures WebSocket:
            </p>

            <a href="/api/futures-ws-test?symbol=BTCUSDT"
               target="_blank">
                /api/futures-ws-test
            </a>
        </div>

    </body>
    </html>
    """


# ============================================================
# BASIC BINANCE SPOT TEST
# ============================================================

@app.route("/api/test")
def api_test():

    params = {
        "symbol": "BTCUSDT",
        "interval": "1m",
        "limit": 2
    }

    try:

        response = requests.get(
            SPOT_KLINES_URL,
            params=params,
            timeout=10
        )

        return jsonify({
            "ok": True,
            "status_code": response.status_code,
            "url": response.url,
            "headers": {
                "content_type":
                    response.headers.get("content-type"),
                "server":
                    response.headers.get("server"),
                "retry_after":
                    response.headers.get("retry-after")
            },
            "body": response.text
        })

    except Exception as e:

        return jsonify({
            "ok": False,
            "error_type": type(e).__name__,
            "error": str(e)
        }), 500


# ============================================================
# SPOT CANDLES
# ============================================================

@app.route("/api/candles")
def candles():

    symbol = request.args.get(
        "symbol",
        "BTCUSDT"
    ).upper()

    interval = request.args.get(
        "interval",
        "1m"
    )

    try:

        limit = int(
            request.args.get(
                "limit",
                "100"
            )
        )

    except ValueError:

        limit = 100


    # Binance limit safety

    if limit < 1:
        limit = 1

    if limit > 1000:
        limit = 1000


    params = {
        "symbol": symbol,
        "interval": interval,
        "limit": limit
    }


    try:

        response = requests.get(
            SPOT_KLINES_URL,
            params=params,
            timeout=10
        )

        response.raise_for_status()

        data = response.json()


        candles_data = []


        for row in data:

            candles_data.append({

                "time": int(row[0] / 1000),

                "open": float(row[1]),

                "high": float(row[2]),

                "low": float(row[3]),

                "close": float(row[4]),

                "volume": float(row[5])

            })


        return jsonify({

            "ok": True,

            "symbol": symbol,

            "interval": interval,

            "count": len(candles_data),

            "candles": candles_data

        })


    except Exception as e:

        return jsonify({

            "ok": False,

            "symbol": symbol,

            "interval": interval,

            "error_type": type(e).__name__,

            "error": str(e)

        }), 500


# ============================================================
# FUTURES WEBSOCKET TEST
#
# Receives:
#
#   1. aggTrade
#   2. depth
#
# This endpoint is ONLY a diagnostic test.
#
# ============================================================

@app.route("/api/futures-ws-test")
def futures_ws_test():

    symbol = request.args.get(
        "symbol",
        "BTCUSDT"
    ).lower()


    ws_url = (
        f"{FUTURES_WS_BASE}"
        f"?streams="
        f"{symbol}@aggTrade/"
        f"{symbol}@depth"
    )


    depth_count = 0

    trade_count = 0


    samples = {

        "depth": [],

        "aggTrade": []

    }


    ws = None


    try:

        print("=" * 60)

        print("FUTURES DATA TEST")

        print(ws_url)

        print("=" * 60)


        ws = websocket.create_connection(

            ws_url,

            timeout=10

        )


        # ----------------------------------------------------
        # Receive up to 30 messages
        # ----------------------------------------------------

        for _ in range(30):

            raw = ws.recv()


            if not raw:

                continue


            message = json.loads(raw)


            stream = message.get(
                "stream",
                ""
            )


            data = message.get(
                "data",
                {}
            )


            # ------------------------------------------------
            # DEPTH
            # ------------------------------------------------

            if "@depth" in stream:

                depth_count += 1


                if len(samples["depth"]) < 2:

                    samples["depth"].append(
                        data
                    )


            # ------------------------------------------------
            # AGG TRADE
            # ------------------------------------------------

            elif "@aggTrade" in stream:

                trade_count += 1


                if len(samples["aggTrade"]) < 3:

                    samples["aggTrade"].append(
                        data
                    )


        # ----------------------------------------------------
        # RESULT
        # ----------------------------------------------------

        result = {

            "ok": True,

            "symbol":
                symbol.upper(),

            "websocket":
                ws_url,

            "depth_messages":
                depth_count,

            "aggTrade_messages":
                trade_count,

            "depth_samples":
                samples["depth"],

            "aggTrade_samples":
                samples["aggTrade"]

        }


        print(
            json.dumps(
                result,
                indent=2
            )
        )


        return jsonify(result)


    except Exception as e:

        result = {

            "ok": False,

            "symbol":
                symbol.upper(),

            "websocket":
                ws_url,

            "depth_messages":
                depth_count,

            "aggTrade_messages":
                trade_count,

            "error_type":
                type(e).__name__,

            "error":
                str(e)

        }


        print(
            json.dumps(
                result,
                indent=2
            )
        )


        return jsonify(result), 500


    finally:

        if ws is not None:

            try:

                ws.close()

            except Exception:

                pass


# ============================================================
# START SERVER
# ============================================================

if __name__ == "__main__":

    app.run(

        host="0.0.0.0",

        port=10000

    )
