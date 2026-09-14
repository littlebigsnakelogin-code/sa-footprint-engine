import os
import json
import time
import threading
import requests
from flask import Flask, render_template_string, jsonify, request

app = Flask(__name__)

TURSO_DB_URL = os.environ.get("TURSO_DB_URL", "").strip().replace("libsql://", "https://")
TURSO_AUTH_TOKEN = os.environ.get("TURSO_AUTH_TOKEN", "").strip()

SYMBOLS = ['btcusdt', 'ethusdt', 'solusdt', 'xrpusdt', 'avaxusdt', 'linkusdt', 'ltcusdt']

def query_turso(sql, params=[]):
    if not TURSO_DB_URL or not TURSO_AUTH_TOKEN:
        return None
    url = f"{TURSO_DB_URL}/v2/pipeline"
    headers = {
        "Authorization": f"Bearer {TURSO_AUTH_TOKEN}",
        "Content-Type": "application/json"
    }
    
    args = []
    for p in params:
        if isinstance(p, float):
            args.append({"type": "float", "value": p})
        elif isinstance(p, int):
            args.append({"type": "integer", "value": str(p)})
        else:
            args.append({"type": "text", "value": str(p)})

    payload = {
        "requests": [
            {"type": "execute", "stmt": {"sql": sql, "args": args}},
            {"type": "close"}
        ]
    }
    try:
        res = requests.post(url, json=payload, headers=headers, timeout=5)
        return res.json()
    except Exception as e:
        print("Turso HTTP Request Error:", e)
        return None

def init_db():
    sql = '''CREATE TABLE IF NOT EXISTS candles (
                symbol TEXT, tf TEXT, time INTEGER,
                open REAL, high REAL, low REAL, close REAL,
                delta REAL, totalVol REAL,
                PRIMARY KEY (symbol, tf, time)
            )'''
    query_turso(sql)

init_db()

HTML_UI = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>SA Institutional Footprint & Spoofing Engine</title>
    <script src="https://unpkg.com/lightweight-charts/dist/lightweight-charts.standalone.production.js"></script>
    <style>
        body { background-color: #121212; color: #fff; font-family: Arial, sans-serif; margin: 0; padding: 10px; }
        #header { display: flex; gap: 15px; align-items: center; margin-bottom: 10px; background: #1e1e1e; padding: 10px; border-radius: 5px; }
        select, button { background: #2a2a2a; color: #fff; border: 1px solid #444; padding: 6px 12px; border-radius: 4px; cursor: pointer; }
        #chart-container { width: 100%; height: 650px; background: #181818; border-radius: 5px; }
        #spoof-alert { background: #2c1515; border: 1px solid #ff4444; color: #ff6666; padding: 8px; border-radius: 4px; margin-top: 10px; display: none; }
    </style>
</head>
<body>
    <div id="header">
        <h2>SA Footprint Dashboard</h2>
        <select id="symbolSelect" onchange="loadChart()">
            <option value="btcusdt">BTCUSDT</option>
            <option value="ethusdt">ETHUSDT</option>
            <option value="solusdt">SOLUSDT</option>
            <option value="xrpusdt">XRPUSDT</option>
            <option value="avaxusdt">AVAXUSDT</option>
            <option value="linkusdt">LINKUSDT</option>
            <option value="ltcusdt">LTCUSDT</option>
        </select>
        <select id="tfSelect" onchange="loadChart()">
            <option value="1m">1m</option>
            <option value="3m">3m</option>
            <option value="5m">5m</option>
            <option value="15m">15m</option>
            <option value="1h">1h</option>
            <option value="4h">4h</option>
            <option value="1d">1D</option>
        </select>
        <button onclick="loadChart()">Refresh</button>
    </div>
    <div id="chart-container"></div>
    <div id="spoof-alert"></div>

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
            const response = await fetch(`/api/candles?symbol=${symbol}&tf=${tf}`);
            const data = await response.json();
            if(data && data.length > 0) {
                candleSeries.setData(data);
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
    symbol = request.args.get('symbol', 'btcusdt').upper()
    tf = request.args.get('tf', '1m')
    
    # Direct Binance REST fallback for instant graph loading
    try:
        url = f"https://api.binance.com/api/3/klines?symbol={symbol}&interval={tf}&limit=300"
        res = requests.get(url, timeout=3).json()
        data = []
        for c in res:
            data.append({
                "time": int(c[0] // 1000),
                "open": float(c[1]),
                "high": float(c[2]),
                "low": float(c[3]),
                "close": float(c[4])
            })
        return jsonify(data)
    except Exception as e:
        print("Binance Direct Fetch Error:", e)
        return jsonify([])

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
