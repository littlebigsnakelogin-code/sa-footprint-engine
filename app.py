from flask import Flask, jsonify, request
import websocket
import json
import time

app = Flask(__name__)

BINANCE_WS = "wss://fstream.binance.com/stream"


def get_symbol():
    return request.args.get("symbol", "BTCUSDT").upper()


def test_trade_stream(symbol):
    stream = f"{symbol.lower()}@trade"
    url = f"{BINANCE_WS}?streams={stream}"

    messages = []
    error = None
    error_type = None

    ws = None

    try:
        ws = websocket.create_connection(
            url,
            timeout=15,
            enable_multithread=True
        )

        start = time.time()

        while time.time() - start < 12 and len(messages) < 50:
            try:
                raw = ws.recv()

                if not raw:
                    continue

                data = json.loads(raw)

                payload = data.get("data", data)

                messages.append({
                    "event": payload.get("e"),
                    "event_time": payload.get("E"),
                    "symbol": payload.get("s"),
                    "trade_id": payload.get("t"),
                    "price": payload.get("p"),
                    "quantity": payload.get("q"),
                    "trade_time": payload.get("T"),
                    "buyer_is_maker": payload.get("m")
                })

            except websocket.WebSocketTimeoutException:
                break

    except Exception as e:
        error = str(e)
        error_type = type(e).__name__

    finally:
        if ws is not None:
            try:
                ws.close()
            except Exception:
                pass

    return {
        "ok": len(messages) > 0,
        "symbol": symbol,
        "stream": url,
        "trade_messages": len(messages),
        "samples": messages[:10],
        "error": error,
        "error_type": error_type
    }


@app.route("/")
def home():
    return jsonify({
        "service": "SA Footprint Engine",
        "status": "running",
        "endpoints": [
            "/api/test",
            "/api/trade-test?symbol=BTCUSDT",
            "/api/aggtrade-test?symbol=BTCUSDT",
            "/api/depth-test?symbol=BTCUSDT",
            "/api/futures-ws-test?symbol=BTCUSDT"
        ]
    })


@app.route("/api/test")
def api_test():
    return jsonify({
        "ok": True,
        "message": "SA Footprint Engine is running"
    })


@app.route("/api/trade-test")
def trade_test():
    symbol = get_symbol()
    return jsonify(test_trade_stream(symbol))


@app.route("/api/aggtrade-test")
def aggtrade_test():
    symbol = get_symbol()

    stream = f"{symbol.lower()}@aggTrade"
    url = f"{BINANCE_WS}?streams={stream}"

    messages = []
    error = None
    error_type = None

    ws = None

    try:
        ws = websocket.create_connection(
            url,
            timeout=15,
            enable_multithread=True
        )

        start = time.time()

        while time.time() - start < 12 and len(messages) < 30:
            try:
                raw = ws.recv()

                if not raw:
                    continue

                data = json.loads(raw)
                payload = data.get("data", data)

                messages.append({
                    "event": payload.get("e"),
                    "event_time": payload.get("E"),
                    "symbol": payload.get("s"),
                    "agg_trade_id": payload.get("a"),
                    "price": payload.get("p"),
                    "quantity": payload.get("q"),
                    "first_trade_id": payload.get("f"),
                    "last_trade_id": payload.get("l"),
                    "trade_time": payload.get("T"),
                    "buyer_is_maker": payload.get("m")
                })

            except websocket.WebSocketTimeoutException:
                break

    except Exception as e:
        error = str(e)
        error_type = type(e).__name__

    finally:
        if ws is not None:
            try:
                ws.close()
            except Exception:
                pass

    return jsonify({
        "ok": len(messages) > 0,
        "symbol": symbol,
        "stream": url,
        "aggTrade_messages": len(messages),
        "samples": messages[:10],
        "error": error,
        "error_type": error_type
    })


@app.route("/api/depth-test")
def depth_test():
    symbol = get_symbol()

    stream = f"{symbol.lower()}@depth"
    url = f"{BINANCE_WS}?streams={stream}"

    messages = []
    error = None
    error_type = None

    ws = None

    try:
        ws = websocket.create_connection(
            url,
            timeout=15,
            enable_multithread=True
        )

        start = time.time()

        while time.time() - start < 10 and len(messages) < 10:
            try:
                raw = ws.recv()

                if not raw:
                    continue

                data = json.loads(raw)
                payload = data.get("data", data)

                messages.append({
                    "event": payload.get("e"),
                    "event_time": payload.get("E"),
                    "symbol": payload.get("s"),
                    "first_update_id": payload.get("U"),
                    "final_update_id": payload.get("u"),
                    "previous_update_id": payload.get("pu"),
                    "bids": len(payload.get("b", [])),
                    "asks": len(payload.get("a", []))
                })

            except websocket.WebSocketTimeoutException:
                break

    except Exception as e:
        error = str(e)
        error_type = type(e).__name__

    finally:
        if ws is not None:
            try:
                ws.close()
            except Exception:
                pass

    return jsonify({
        "ok": len(messages) > 0,
        "symbol": symbol,
        "stream": url,
        "depth_messages": len(messages),
        "samples": messages[:10],
        "error": error,
        "error_type": error_type
    })


@app.route("/api/futures-ws-test")
def futures_ws_test():
    symbol = get_symbol()

    streams = [
        f"{symbol.lower()}@trade",
        f"{symbol.lower()}@depth"
    ]

    stream_text = "/".join(streams)
    url = f"{BINANCE_WS}?streams={stream_text}"

    messages = []
    counts = {
        "trade": 0,
        "depth": 0,
        "other": 0
    }

    error
