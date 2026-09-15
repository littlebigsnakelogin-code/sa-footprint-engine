from flask import Flask, jsonify, request
import websocket
import json
import time

app = Flask(__name__)

BINANCE_WS = "wss://fstream.binance.com/stream"

DEFAULT_SYMBOLS = [
    "BTCUSDT",
    "ETHUSDT",
    "SOLUSDT",
    "XRPUSDT",
    "AVAXUSDT",
    "LINKUSDT",
    "LTCUSDT"
]


def get_symbols():
    raw = request.args.get("symbols")

    if not raw:
        return DEFAULT_SYMBOLS

    symbols = []

    for item in raw.split(","):
        symbol = item.strip().upper()

        if symbol and symbol not in symbols:
            symbols.append(symbol)

    return symbols[:20]


@app.route("/")
def home():
    return jsonify({
        "service": "SA Footprint Engine",
        "status": "running",
        "endpoints": [
            "/api/test",
            "/api/multi-trade-test"
        ]
    })


@app.route("/api/test")
def api_test():
    return jsonify({
        "ok": True,
        "message": "SA Footprint Engine is running"
    })


@app.route("/api/multi-trade-test")
def multi_trade_test():

    symbols = get_symbols()

    streams = [
        f"{symbol.lower()}@trade"
        for symbol in symbols
    ]

    stream_text = "/".join(streams)
    url = f"{BINANCE_WS}?streams={stream_text}"

    counts = {
        symbol: 0
        for symbol in symbols
    }

    samples = []

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

        while time.time() - start < 15:

            if len(samples) >= 50:
                break

            try:

                raw = ws.recv()

                if not raw:
                    continue

                data = json.loads(raw)

                payload = data.get("data", data)

                event_type = payload.get("e")

                if event_type != "trade":
                    continue

                symbol = payload.get("s")

                if symbol in counts:
                    counts[symbol] += 1

                if len(samples) < 50:

                    samples.append({
                        "symbol": symbol,
                        "price": payload.get("p"),
                        "quantity": payload.get("q"),
                        "trade_id": payload.get("t"),
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

    total_messages = sum(counts.values())

    return jsonify({
        "ok": total_messages > 0,
        "symbols_requested": symbols,
        "symbols_received": [
            symbol
            for symbol in symbols
            if counts[symbol] > 0
        ],
        "symbols_missing": [
            symbol
            for symbol in symbols
            if counts[symbol] == 0
        ],
        "trade_counts": counts,
        "total_trade_messages": total_messages,
        "samples": samples,
        "stream_count": len(streams),
        "stream": url,
        "error": error,
        "error_type": error_type
    })


if __name__ == "__main__":

    app.run(
        host="0.0.0.0",
        port=10000
    )
