import os
import json
import time
import threading
import requests
import websocket
from flask import Flask, render_template_string, jsonify, request

app = Flask(__name__)

# Binance Live Memory Store
BINANCE_CACHE = {}
SYMBOLS = ['BTCUSDT', 'ETHUSDT', 'SOLUSDT', 'XRPUSDT', 'AVAXUSDT', 'LINKUSDT', 'LTCUSDT']

def fetch_initial_history(symbol):
    """Initial Boot-up par historical candles load karne ke liye"""
    try:
        url = f"https://api.binance.com/api/v3/klines?symbol={symbol}&interval=1m&limit=300"
        res = requests.get(url, timeout=5)
        if res.status_code == 200:
            raw_candles = res.json()
            formatted = []
            for c in raw_candles:
                formatted.append({
                    "time": int(c[0]) // 1000,
                    "open": float(c[1]),
                    "high": float(c[2]),
                    "low": float(c[3]),
                    "close": float(c[4])
                })
            BINANCE_CACHE[symbol] = formatted
            print(f"[HIST SUCCESS] Loaded {len(formatted)} candles for {symbol}")
    except Exception as e:
        print(f"[HIST ERROR] Failed to fetch for {symbol}: {e}")

# App startup per-symbol load
for s in SYMBOLS:
    fetch_initial_history(s)

def on_message(ws, message):
    try:
        raw = json.loads(message)
        data = raw.get('data', raw)
        
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
    streams = "/".join([f"{s.lower()}@kline_1m" for s in SYMBOLS])
    ws_url = f"wss://stream.binance.com:9443/stream?streams={streams}"
    
    while True:
        try:
            print("[WS] Connecting to Binance Combined Stream...")
            ws = websocket.WebSocketApp(
                ws_url,
                on_message=on_message,
                on_error=lambda ws, e: print("WS Error:", e),
                on_close=lambda ws, c, m: print("WS Closed. Reconnecting...")
            )
            ws.run_forever(ping_interval=30, ping_timeout=10)
        except Exception as e:
            print("WS Thread Exception:", e)
        time.sleep(3)

threading.Thread(target=start_ws, daemon=True).start()

HTML_UI = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>SA Institutional Footprint Engine</title>
    <script src="https://unpkg.com/lightweight-charts@3.8.0/dist/lightweight-charts.standalone.production.js"></script>
    <style>
        body { background-color: #121212; color: #fff; font-family: Arial, sans-serif; margin: 0; padding: 10px; }
        #header { display: flex; gap: 15px; align-items: center; margin-bottom: 10px; background: #1e1e1e; padding: 10px; border-radius: 5px; }
        select, button { background: #2a2a2a; color: #fff; border: 1px solid #444; padding: 6px 12px; border-radius: 4px; cursor: pointer; }
        #chart-container { width: 100%; height: 600px; background: #181818; border-radius: 5px; position: relative; }
        #status-bar { color: #ffeb3b; font-size: 13px; font-weight: bold; }
    </style>
</head>
<body>
    <div id="header">
        <h2>SA Footprint Dashboard</h2>
        <select id="symbolSelect" onchange="loadChart()">
            <option value="BTCUSDT">BTCUSDT</option>
            <option value="ETHUSDT">ETHUSDT</option>
            <option value="SOLUSDT">SOLUSDT</option>
            <option value="XRPUSDT">XRPUSDT</option>
            <option value="AVAXUSDT">AVAXUSDT</option>
            <option value="LINKUSDT">LINKUSDT</option>
            <option value="LTCUSDT">LTCUSDT</option>
        </select>
        <button onclick="loadChart()">Force Refresh</button>
        <span id="status-bar">Initializing Engine...</span>
    </div>
    <div id="chart-container"></div>

    <script>
        let chart = null;
        let candleSeries = null;

        function initChart() {
            const chartContainer = document.getElementById('chart-container');
            chartContainer.innerHTML = '';
            
            chart = LightweightCharts.createChart(chartContainer, {
                width: chartContainer.clientWidth,
                height: 600,
                layout: { backgroundColor: '#181818', textColor: '#d1d4dc' },
                grid: { vertLines: { color: '#2B2B43' }, horzLines: { color: '#2B2B43' } },
                timeScale: { timeVisible: true, secondsVisible: true }
            });

            candleSeries = chart.addCandlestickSeries({
                upColor: '#26a69a', downColor: '#ef5350',
                borderVisible: false, wickUpColor: '#26a69a', wickDownColor: '#ef5350'
            });

            window.addEventListener('resize', () => {
                if (chart) chart.applyOptions({ width: chartContainer.clientWidth });
            });
        }

        async function loadChart() {
            if (!chart) initChart();
            
            const symbol = document.getElementById('symbolSelect').value;
            const statusEl = document.getElementById('status-bar');
            
            try {
                // Window origin se absolute URL match karega
                const response = await fetch(window.location.origin + '/api/candles?symbol=' + symbol);
                if (!response.ok) throw new Error("Network response was not OK");
                
                const data = await response.json();
                
                if (Array.isArray(data) && data.length > 0) {
                    candleSeries.setData(data);
                    statusEl.innerText = `Binance Live 🟢 (${data.length} Bars Ingested)`;
                    statusEl.style.color = "#00ff00";
                } else {
                    statusEl.innerText = "Waiting for Backend Buffer... ⏳";
                    statusEl.style.color = "#ff9800";
                }
            } catch(e) {
                console.error("UI Fetch Error:", e);
                statusEl.innerText = "API Connection Error 🔴";
                statusEl.style.color = "#f44336";
            }
        }

        document.addEventListener("DOMContentLoaded", () => {
            initChart();
            loadChart();
            setInterval(loadChart, 2000);
        });
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
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)
