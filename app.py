import os
import json
import requests
import websocket

from flask import Flask, jsonify, request

app = Flask(__name__)

# ============================================================
# BINANCE URLS
# ============================================================

# Public Binance Spot market-data endpoint
SPOT_KLINES_URL = "https://data-api.binance.vision/api/v3/klines"

# Binance USD-M Futures WebSocket
FUTURES_WS_BASE = "wss://fstream.binance.com/stream"


# ============================================================
# HEALTH CHECK
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

            button {
                padding: 12px 20px;
                margin: 5px;
                cursor: pointer;
            }

            pre {
                background: #222;
                padding: 15px;
                overflow-x: auto;
                white-space: pre-wrap;
            }
        </style>
    </head>

    <body>

        <h1>SA Footprint Engine</h1>

        <p>Backend is running.</p>

        <button onclick="testSpot()">Test Binance Spot</button>

        <button onclick="testFuturesWS()">Test Binance Futures WebSocket</button>

        <h3>Result</h3>

        <pre id="result">Waiting...</pre>

        <script>

        async function testSpot() {
            document.getElementById("result").textContent =
                "Testing Binance Spot...";

            try {
                const response =
                    await fetch("/api/test?symbol=BTCUSDT");

                const data = await response.json();

                document.getElementById("result").textContent =
                    JSON.stringify(data, null, 2);

            } catch (error) {

                document.getElementById("result").textContent =
                    "Browser error: " + error;

            }
        }


        async function testFuturesWS() {
            document.getElementById("result").textContent =
                "Testing Binance Futures WebSocket...";

            try {
                const response =
                    await fetch("/api/futures-ws-test?symbol=BTCUSDT");

                const data = await response.json();

                document.getElementById("result").textContent =
                    JSON.stringify(data, null, 2);

            } catch (error) {

                document.getElementById("result").textContent =
                    "Browser error: " + error;

            }
        }

        </script>

    </body>
    </html>
    """


# ============================================================
# BINANCE SPOT TEST
# ============================================================

@app.route("/api/test")
def test_binance_spot():

    symbol = request.args.get(
        "symbol",
        "BTCUSDT"
    ).upper()

    params = {
        "symbol": symbol,
        "interval": "1m",
        "limit": 2
    }

    headers = {
        "User-Agent": "SA-Footprint-Engine/1.0"
    }

    url = SPOT_KLINES_URL

    try:

        response = requests.get(
            url,
            params=params,
            headers=headers,
            timeout=10
        )

        result = {
            "ok": response.status_code == 200,
            "status_code": response.status_code,
            "url": response.url,
            "headers": {
                "content_type": response.headers.get(
                    "content-type"
                ),
                "server": response.headers.get(
                    "server"
                ),
                "retry_after": response.headers.get(
                    "retry-after"
                )
            },
            "body": response.text[:5000]
        }

        return jsonify(result)

    except Exception as e:

        result = {
            "ok": False,
            "error_type": type(e).__name__,
            "error": str(e),
            "url": url
        }

        return jsonify(result), 500


# ============================================================
# SPOT CANDLES
# ============================================================

@app.route("/api/candles")
def get_candles():

    symbol = request.args.get(
        "symbol",
        "BTCUSDT"
    ).upper()

    params = {
        "symbol": symbol,
        "interval": "1m",
        "limit": 300
    }

    headers = {
        "User-Agent": "SA-Footprint-Engine/1.0"
    }

    try:

        response = requests.get(
            SPOT_KLINES_URL,
            params=params,
            headers=headers,
            timeout=10
        )

        if response.status_code != 200:

            return jsonify({
                "ok": False,
                "status_code": response.status_code,
                "body": response.text[:5000]
            }), response.status_code

        raw_data = response.json()

        candles = []

        for c in raw_data:

            candles.append({
                "time": int(c[0]) // 1000,
                "open": float(c[1]),
                "high": float(c[2]),
                "low": float(c[3]),
                "close": float(c[4]),
                "volume": float(c[5])
            })

        return jsonify(candles)

    except Exception as e:

        return jsonify({
            "ok": False,
            "error_type": type(e).__name__,
            "error": str(e)
        }), 500


# ============================================================
# BINANCE FUTURES WEBSOCKET TEST
# ============================================================

@app.route("/api/futures-ws-test")
def futures_ws_test():

    symbol = request.args.get(
        "symbol",
        "BTCUSDT"
    ).lower()

    # --------------------------------------------------------
    # Combined stream:
    #
    # aggTrade = actual futures trades
    # depth    = orderbook updates
    # --------------------------------------------------------

    ws_url = (
        f"{FUTURES_WS_BASE}"
        f"?streams="
        f"{symbol}@aggTrade/"
        f"{symbol}@depth"
    )

    messages = []

    ws = None

    try:

        print("=" * 60)
        print("FUTURES WEBSOCKET TEST")
        print("Connecting to:")
        print(ws_url)
        print("=" * 60)

        ws = websocket.create_connection(
            ws_url,
            timeout=10
        )

        # Receive a few messages
        for i in range(5):

            message = ws.recv()

            if not message:
                continue

            # Keep response reasonably small
            messages.append(message[:5000])

        result = {
            "ok": True,
            "websocket": ws_url,
            "messages_received": len(messages),
            "messages": messages
        }

        print("FUTURES WS SUCCESS")
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
            "websocket": ws_url,
            "error_type": type(e).__name__,
            "error": str(e),
            "messages_received": len(messages),
            "messages": messages
        }

        print("FUTURES WS ERROR")
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
# SERVER START
# ============================================================

if __name__ == "__main__":

    port = int(
        os.environ.get(
            "PORT",
            5000
        )
    )

    app.run(
        host="0.0.0.0",
        port=port
    )
