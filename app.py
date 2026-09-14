import os
import json
import time
import threading
import websocket
from flask import Flask, render_template_string, jsonify, request

app = Flask(__name__)

# Binance Live Memory Store
BINANCE_CACHE = {}

SYMBOLS = ['btcusdt', 'ethusdt', 'solusdt', 'xrpusdt', 'avaxusdt', 'linkusdt', 'ltcusdt']

def on_message(ws, message):
    try:
        data = json.loads(message)
        if 'k' in data:
            k = data['k']
            symbol = data['s']
            
            candle = {
                "time": int(k['t']) // 1000,
                "open": float(k['o']),
                "high": float(k['h']),
                "low": float(k['l']),
                "close": float(k['c'])
            }
            
            if symbol not in BINANCE_CACHE:
                BINANCE_CACHE[symbol] = []
                
            # Keep last 300 candles in memory
            cache = BINANCE_CACHE[symbol]
            if len(cache) > 0 and cache[-1]['time'] == candle['time']:
                cache[-1] = candle
            else:
                cache.append(candle)
                if len(cache) > 300:
                    cache.pop(0)
    except Exception as e:
        print("WS Processing Error:", e)

def start_ws():
    streams = "/".join([f"{s}@kline_1m" for s in SYMBOLS])
    ws_url = f"wss://stream.binance.com:9443/ws/{streams}"
    
    while True:
        try:
            ws = websocket.WebSocketApp(
                ws_url,
                on_message=on_message,
                on_error=lambda ws, e: print("WS Error:", e),
                on_close=lambda ws, c, m: print("WS Closed")
            )
            ws.run_forever()
        except Exception as e:
            print("WS Thread Exception:", e)
        time.sleep(3)

# Start Binance Direct WS Ingestion Thread
threading.Thread(target=start_ws, daemon=True).start()

HTML_UI = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>SA Institutional Footprint Engine</title>
    <script src="https://unpkg.com/lightweight-charts/dist/lightweight-charts.standalone.production.js"></script>
    <style>
        body { background-color: #121212; color: #fff; font-family: Arial, sans-serif; margin: 0; padding: 10px; }
        #header { display: flex; gap: 15px; align-items: center; margin-bottom: 10px; background: #1e1e1e; padding: 10px; border-radius: 5px; }
        select, button { background: #2a2a2a; color: #fff; border: 1px solid #444; padding: 6px 12px; border-radius: 4px; cursor: pointer; }
        #chart-container { width: 100%; height: 650px; background: #181818; border-radius: 5px; }
        #status-bar { color: #ffeb3b; font-size: 13px; font-weight: bold; }
    </style>
</head>
<body>
    <div id="header">
        <h2>SA Footprint Dashboard (Binance Stream)</h2>
        <select id="symbolSelect" onchange="loadChart()">
            <option value="BTCUSDT">BTCUSDT</option>
            <option value="ETHUSDT">ETHUSDT</option>
            <option value="SOLUSDT">SOLUSDT</option>
            <option value="XRPUSDT">XRPUSDT</option>
            <option value="AVAXUSDT">AVAXUSDT</option>
            <option value="LINKUSDT">LINKUSDT</option>
            <option value="LTCUSDT">LTCUSDT</option>
        </select>
        <button onclick="loadChart()">Refresh</button>
        <span id="status-bar">Connecting Binance Stream...</span>
    </div>
    <div id="chart-container"></div>

    <script>
        const chartContainer = document.getElementById('chart-container');
        const chart = LightweightCharts.createChart(chartContainer, {
            layout: { background: { color: '#181818' }, textColor: '#d1d4dc' },
            grid: { vertLines: { color: '#2B2B43' }, horzLines: { color: '#2B2B43' } },
            timeScale: { timeVisible: true, secondsVisible: true }
        });
        const candleSeries = chart.addCandlestickSeries({
            upColor: '#26a69a', downColor: '#ef5350',
            borderVisible: false, wickUpColor: '#26a69a', wickDownColor: '#ef5350'
        });

        async function loadChart() {
            const symbol = document.getElementById('symbolSelect').value;
            const statusEl = document.getElementById('status-bar');
            
            try {
                const response = await fetch(`/api/candles?symbol=${symbol}`);
                const data = await response.json();
                
                if (Array.isArray(data) && data.length > 0) {
                    candleSeries.setData(data);
                    statusEl.innerText = `Binance Live 🟢 (${data.length} Bars Ingested)`;
                    statusEl.style.color = "#00ff00";
                } else {
                    statusEl.innerText = "Building Live Stream Buffer... Wait 5-10 Sec ⏳";
                    statusEl.style.color = "#ff9800";
                }
            } catch(e) {
                console.error("UI Fetch Error:", e);
                statusEl.innerText = "Stream Disconnected 🔴";
                statusEl.style.color = "#f44336";
            }
        }
        setInterval(loadChart, 3000);
        loadChart();
    </script>
</body>
</html>
"""

@app.route('/')
def index():
    return render_template_string(HTML_UI)

@app.route('/api/candles')
def get_candles():
    symbol = request.args.get('symbol', 'BTCUSDT').upper()
    data = BINANCE_CACHE.get(symbol, [])
    return jsonify(data)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
