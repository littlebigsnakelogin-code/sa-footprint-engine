import os
import requests
from flask import Flask, jsonify, render_template_string, request

app = Flask(__name__)

BINANCE_URL = "https://fapi.binance.com/fapi/v1/klines"

HTML = """
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>SA Binance Diagnostic</title>

    <style>
        body {
            background: #121212;
            color: white;
            font-family: Arial;
            padding: 20px;
        }

        button, select {
            padding: 10px;
            background: #222;
            color: white;
            border: 1px solid #555;
            margin: 5px;
        }

        pre {
            background: #1e1e1e;
            padding: 15px;
            white-space: pre-wrap;
            word-break: break-word;
        }

        .ok {
            color: #00ff88;
        }

        .bad {
            color: #ff4444;
        }
    </style>
</head>

<body>

<h2>SA Footprint Engine - Binance Diagnostic</h2>

<select id="symbol">
    <option>BTCUSDT</option>
    <option>ETHUSDT</option>
    <option>SOLUSDT</option>
    <option>XRPUSDT</option>
    <option>AVAXUSDT</option>
    <option>LINKUSDT</option>
    <option>LTCUSDT</option>
</select>

<button onclick="testBinance()">TEST BINANCE</button>

<h3>Status</h3>

<div id="status">Waiting...</div>

<h3>Response</h3>

<pre id="output"></pre>

<script>

async function testBinance() {

    const symbol = document.getElementById("symbol").value;

    const status = document.getElementById("status");
    const output = document.getElementById("output");

    status.innerText = "Testing...";
    status.className = "";

    output.innerText = "";

    try {

        const response = await fetch(
            `/api/test?symbol=${symbol}`
        );

        const data = await response.json();

        output.innerText =
            JSON.stringify(data, null, 2);

        if (data.ok) {

            status.innerText =
                "BINANCE CONNECTION OK";

            status.className = "ok";

        } else {

            status.innerText =
                "BINANCE CONNECTION FAILED";

            status.className = "bad";
        }

    } catch (error) {

        status.innerText =
            "FLASK REQUEST FAILED";

        status.className = "bad";

        output.innerText =
            error.toString();
    }
}

</script>

</body>
</html>
"""


@app.route("/")
def index():
    return render_template_string(HTML)


@app.route("/api/test")
def test_binance():

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
        "User-Agent": "SA-Footprint-Engine/1.0",
        "Accept": "application/json"
    }

    try:

        response = requests.get(
            BINANCE_URL,
            params=params,
            headers=headers,
            timeout=10
        )

        result = {
            "ok": response.status_code == 200,
            "status_code": response.status_code,
            "url": response.url,
            "headers": {
                "content_type":
                    response.headers.get("Content-Type"),
                "retry_after":
                    response.headers.get("Retry-After"),
                "server":
                    response.headers.get("Server")
            },
            "body": response.text[:2000]
        }

        print("BINANCE TEST:")
        print(result)

        return jsonify(result)

    except Exception as e:

        error = {
            "ok": False,
            "error_type": type(e).__name__,
            "error": str(e)
        }

        print("BINANCE EXCEPTION:")
        print(error)

        return jsonify(error), 500


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
