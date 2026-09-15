import json
import websocket

from flask import Flask, jsonify, request
import requests


app = Flask(__name__)


SPOT_KLINES_URL = "https://data-api.binance.vision/api/v3/klines"

FUTURES_WS_BASE = "wss://fstream.binance.com/stream"


# ============================================================
# HOME
# ============================================================

@app.route("/")
def home():
    return """
    <html>
    <head>
        <title>SA Footprint Engine</title>
    </head>

    <body style="background:#111;color:#eee;font-family:Arial;padding:30px">

        <h1>SA Footprint Engine</h1>

        <p>Server: ONLINE</p>

        <p>
            <a style="color:#4da6ff"
               href="/api/aggtrade-test?symbol=BTCUSDT">
               Test aggTrade
            </a>
        </p>

        <p>
            <a style="color:#4da6ff"
               href="/api/depth-test?symbol=BTCUSDT">
               Test Depth
            </a>
        </p>

        <p>
            <a style="color:#4da6ff"
               href="/api/futures-ws-test?symbol=BTCUSDT">
               Test Both
            </a>
        </p>

    </body>
    </html>
    """


# ============================================================
# SPOT TEST
# ============================================================

@app.route("/api/test")
def api_test():

    try:

        response = requests.get(
            SPOT_KLINES_URL,
            params={
                "symbol": "BTCUSDT",
                "interval": "1m",
                "limit": 2
            },
            timeout=10
        )

        return jsonify({
            "ok": True,
            "status_code": response.status_code,
            "body": response.text
        })

    except Exception as e:

        return jsonify({
            "ok": False,
            "error_type": type(e).__name__,
            "error": str(e)
        }), 500


# ============================================================
# AGGTRADE ONLY TEST
# ============================================================

@app.route("/api/aggtrade-test")
def aggtrade_test():

    symbol = request.args.get(
        "symbol",
        "BTCUSDT"
    ).lower()

    stream_url = (
        f"{FUTURES_WS_BASE}"
        f"?streams={symbol}@aggTrade"
    )

    ws = None

    trades = []

    count = 0

    try:

        print("=" * 60)
        print("AGGTRADE ONLY TEST")
        print(stream_url)
        print("=" * 60)

        ws = websocket.create_connection(
            stream_url,
            timeout=15
        )

        # Wait for up to 15 seconds
        ws.settimeout(15)

        for _ in range(100):

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

            if "@aggTrade" in stream:

                count += 1

                if len(trades) < 10:

                    trades.append({
                        "event_time": data.get("E"),
                        "trade_time": data.get("T"),
                        "price": data.get("p"),
                        "quantity": data.get("q"),
                        "first_trade_id": data.get("f"),
                        "last_trade_id": data.get("l"),
                        "buyer_is_maker": data.get("m")
                    })

        result = {
            "ok": True,
            "symbol": symbol.upper(),
            "stream": stream_url,
            "aggTrade_messages": count,
            "samples": trades
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
            "symbol": symbol.upper(),
            "stream": stream_url,
            "aggTrade_messages": count,
            "error_type": type(e).__name__,
            "error": str(e)
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
# DEPTH ONLY TEST
# ============================================================

@app.route("/api/depth-test")
def depth_test():

    symbol = request.args.get(
        "symbol",
        "BTCUSDT"
    ).lower()

    stream_url = (
        f"{FUTURES_WS_BASE}"
        f"?streams={symbol}@depth"
    )

    ws = None

    depth_count = 0

    samples = []

    try:

        print("=" * 60)
        print("DEPTH ONLY TEST")
        print(stream_url)
        print("=" * 60)

        ws = websocket.create_connection(
            stream_url,
            timeout=15
        )

        ws.settimeout(15)

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

            if "@depth" in stream:

                depth_count += 1

                if len(samples) < 3:
                    samples.append(data)

        result = {
            "ok": True,
            "symbol": symbol.upper(),
            "stream": stream_url,
            "depth_messages": depth_count,
            "samples": samples
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
            "symbol": symbol.upper(),
            "stream": stream_url,
            "depth_messages": depth_count,
            "error_type": type(e).__name__,
            "error": str(e)
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
# BOTH STREAMS TEST
# ============================================================

@app.route("/api/futures-ws-test")
def futures_ws_test():

    symbol = request.args.get(
        "symbol",
        "BTCUSDT"
    ).lower()

    stream_url = (
        f"{FUTURES_WS_BASE}"
        f"?streams="
        f"{symbol}@aggTrade/"
        f"{symbol}@depth"
    )

    ws = None

    depth_count = 0
    trade_count = 0

    depth_samples = []
    trade_samples = []

    try:

        print("=" * 60)
        print("FUTURES BOTH STREAM TEST")
        print(stream_url)
        print("=" * 60)

        ws = websocket.create_connection(
            stream_url,
            timeout=15
        )

        ws.settimeout(15
