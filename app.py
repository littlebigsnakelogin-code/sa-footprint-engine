import json
import websocket
import requests

from flask import Flask, jsonify, request


app = Flask(__name__)


# ============================================================
# CONFIG
# ============================================================

SPOT_KLINES_URL = "https://data-api.binance.vision/api/v3/klines"

FUTURES_WS_BASE = "wss://fstream.binance.com/stream"


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

            a {
                color: #4da6ff;
            }

            .box {
                background: #1b1b1b;
                padding: 15px;
                margin: 15px 0;
                border-radius: 8px;
            }

        </style>
    </head>

    <body>

        <h1>SA Footprint Engine</h1>

        <div class="box">
            Server: ONLINE
        </div>

        <div class="box">
            <a href="/api/test" target="_blank">
                Binance Spot Test
            </a>
        </div>

        <div class="box">
            <a href="/api/aggtrade-test?symbol=BTCUSDT"
               target="_blank">
                Futures aggTrade Test
            </a>
        </div>

        <div class="box">
            <a href="/api/depth-test?symbol=BTCUSDT"
               target="_blank">
                Futures Depth Test
            </a>
        </div>

        <div class="box">
            <a href="/api/futures-ws-test?symbol=BTCUSDT"
               target="_blank">
                Futures Both Test
            </a>
        </div>

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
            "url": response.url,
            "body": response.text
        })

    except Exception as e:

        return jsonify({
            "ok": False,
            "error_type": type(e).__name__,
            "error": str(e)
        }), 500


# ============================================================
# FUTURES AGGTRADE ONLY
# ============================================================

@app.route("/api/aggtrade-test")
def aggtrade_test():

    symbol = request.args.get(
        "symbol",
        "BTCUSDT"
    ).lower()

    stream_url = (
        FUTURES_WS_BASE
        + "?streams="
        + symbol
        + "@aggTrade"
    )

    ws = None

    trade_count = 0

    samples = []

    try:

        print("=" * 60)
        print("FUTURES AGGTRADE TEST")
        print("URL:", stream_url)
        print("=" * 60)

        ws = websocket.create_connection(
            stream_url,
            timeout=15
        )

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

            if "@aggTrade" not in stream:
                continue

            trade_count += 1

            if len(samples) < 10:

                samples.append({
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
            "aggTrade_messages": trade_count,
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
            "aggTrade_messages": trade_count,
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
# FUTURES DEPTH ONLY
# ============================================================

@app.route("/api/depth-test")
def depth_test():

    symbol = request.args.get(
        "symbol",
        "BTCUSDT"
    ).lower()

    stream_url = (
        FUTURES_WS_BASE
        + "?streams="
        + symbol
        + "@depth"
    )

    ws = None

    depth_count = 0

    samples = []

    try:

        print("=" * 60)
        print("FUTURES DEPTH TEST")
        print("URL:", stream_url)
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

            if "@depth" not in stream:
                continue

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
# FUTURES AGGTRADE + DEPTH
# ============================================================

@app.route("/api/futures-ws-test")
def futures_ws_test():

    symbol = request.args.get(
        "symbol",
        "BTCUSDT"
    ).lower()

    stream_url = (
        FUTURES_WS_BASE
        + "?streams="
        + symbol
        + "@aggTrade/"
        + symbol
        + "@depth"
    )

    ws = None

    depth_count = 0

    trade_count = 0

    depth_samples = []

    trade_samples = []

    try:

        print("=" * 60)
        print("FUTURES BOTH STREAM TEST")
        print("URL:", stream_url)
        print("=" * 60)

        ws = websocket.create_connection(
            stream_url,
            timeout=15
        )

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

            if "@depth" in stream:

                depth_count += 1

                if len(depth_samples) < 2:

                    depth_samples.append(data)

            elif "@aggTrade" in stream:

                trade_count += 1

                if len(trade_samples) < 5:

                    trade_samples.append({
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
            "depth_messages": depth_count,
            "aggTrade_messages": trade_count,
            "depth_samples": depth_samples,
            "aggTrade_samples": trade_samples
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
            "aggTrade_messages": trade_count,
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
# SERVER
# ============================================================

if __name__ == "__main__":

    app.run(
        host="0.0.0.0",
        port=10000
    )
