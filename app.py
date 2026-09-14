import os
import requests
from flask import Flask, render_template_string, jsonify, request

app = Flask(__name__)

TURSO_DB_URL = os.environ.get("TURSO_DB_URL", "").strip().replace("libsql://", "https://")
TURSO_AUTH_TOKEN = os.environ.get("TURSO_AUTH_TOKEN", "").strip()

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
        <select id="tfSelect" onchange="loadChart()">
            <option value="1m">1m</option>
            <option value="3m">3m</option>
            <option value="5m">5m</option>
            <option value="15m">15m</option>
            <option value="1h">1h</option>
            <option value="4h">4h</option>
            <option value="1d">1d</option>
        </select>
        <button onclick="loadChart()">Refresh Data</button>
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
            const tf = document.getElementById('tfSelect').value;
            try {
                const response = await fetch(`/api/candles?symbol=${symbol}&tf=${tf}`);
                const data = await response.json();
                if (Array.isArray(data) && data.length > 0) {
                    candleSeries.setData(data);
                }
            } catch(e) {
                console.error("UI Fetch Error:", e);
            }
        }
        setInterval(loadChart, 5000);
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
    tf = request.args.get('tf', '1m')
    headers = {"User-Agent": "Mozilla/5.0"}
    
    try:
        url = f"https://api.binance.com/api/3/klines?symbol={symbol}&interval={tf}&limit=300"
        res = requests.get(url, headers=headers, timeout=3).json()
        
        if isinstance(res, list):
            data = []
            for c in res:
                data.append({
                    "time": int(c[0]) // 1000,
                    "open": float(c[1]),
                    "high": float(c[2]),
                    "low": float(c[3]),
                    "close": float(c[4])
                })
            return jsonify(data)
        return jsonify([])
    except Exception as e:
        print("Backend Fetch Error:", str(e))
        return jsonify([])

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
