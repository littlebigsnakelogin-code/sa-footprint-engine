import os
import json
import requests
from flask import Flask, render_template_string, jsonify, request

app = Flask(__name__)

# Binance Futures Public Endpoint (Backend Only Proxy)
BINANCE_FUTURES_URL = "https://fapi.binance.com/fapi/v1/klines"

def fetch_binance_candles(symbol):
    """
    Backend Only Data Fetcher:
    Browser Binance se bilkul connect nahi hoga.
    Python Server khud Binance se data layega.
    """
    params = {
        "symbol": symbol.upper(),
        "interval": "1m",
        "limit": 300
    }
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
    }
    
    try:
        response = requests.get(BINANCE_FUTURES_URL, params=params, headers=headers, timeout=5)
        if response.status_code == 200:
            raw_data = response.json()
            candles = []
            for c in raw_data:
                candles.append({
                    "time": int(c[0]) // 1000,  # UNIX Timestamp in seconds
                    "open": float(c[1]),
                    "high": float(c[2]),
                    "low": float(c[3]),
                    "close": float(c[4])
                })
            return candles
    except Exception as e:
        print(f"[Backend Error] Data fetch failed for {symbol}: {e}")
    
    return []

# Light-weight UI (Pure HTML/JS - Binance se ZERO connection)
HTML_UI = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>SA Footprint Engine</title>
    <script src="https://unpkg.com/lightweight-charts@3.8.0/dist/lightweight-charts.standalone.production.js"></script>
    <style>
        body { background-color: #121212; color: #fff; font-family: Arial, sans-serif; margin: 0; padding: 12px; }
        #header { display: flex; gap: 15px; align-items: center; margin-bottom: 12px; background: #1e1e1e; padding: 12px; border-radius: 6px; }
        select, button { background: #2a2a2a; color: #fff; border: 1px solid #444; padding: 6px 14px; border-radius: 4px; cursor: pointer; font-size: 14px; }
        #chart-container { width: 100%; height: 600px; background: #181818; border-radius: 6px; }
        #status-bar { color: #ffeb3b; font-size: 14px; font-weight: bold; }
    </style>
</head>
<body>
    <div id="header">
        <h3 style="margin:0;">SA Footprint Dashboard</h3>
        <select id="symbolSelect" onchange="loadChartData()">
            <option value="BTCUSDT">BTCUSDT</option>
            <option value="ETHUSDT">ETHUSDT</option>
            <option value="SOLUSDT">SOLUSDT</option>
            <option value="XRPUSDT">XRPUSDT</option>
            <option value="AVAXUSDT">AVAXUSDT</option>
            <option value="LINKUSDT">LINKUSDT</option>
        </select>
        <button onclick="loadChartData()">Force Reload</button>
        <span id="status-bar">Connecting to Python Backend...</span>
    </div>
    
    <div id="chart-container"></div>

    <script>
        let chart = null;
        let candleSeries = null;

        function initChart() {
            const container = document.getElementById('chart-container');
            container.innerHTML = '';
            
            chart = LightweightCharts.createChart(container, {
                width: container.clientWidth,
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
                if (chart) chart.applyOptions({ width: container.clientWidth });
            });
        }

        async function loadChartData() {
            if (!chart) initChart();
            
            const symbol = document.getElementById('symbolSelect').value;
            const statusEl = document.getElementById('status-bar');
            
            statusEl.innerText = `Fetching via Python Backend... ⏳`;
            statusEl.style.color = "#ff9800";

            try {
                // IMPORTANT: Browser calls ONLY local Flask Server API
                const res = await fetch(`/api/candles?symbol=${symbol}`);
                const data = await res.json();
                
                if (Array.isArray(data) && data.length > 0) {
                    candleSeries.setData(data);
                    statusEl.innerText = `Backend Live 🟢 (${data.length} Candles Loaded)`;
                    statusEl.style.color = "#00ff00";
                } else {
                    statusEl.innerText = "Data Empty / Python Backend Connection Failed 🔴";
                    statusEl.style.color = "#f44336";
                }
            } catch (err) {
                console.error("Fetch Error:", err);
                statusEl.innerText = "Server Unreachable 🔴";
                statusEl.style.color = "#f44336";
            }
        }

        document.addEventListener("DOMContentLoaded", () => {
            initChart();
            loadChartData();
            // Polling interval: Auto update every 3 seconds from Python Server
            setInterval(loadChartData, 3000);
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
    data = fetch_binance_candles(symbol)
    return jsonify(data)

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)
