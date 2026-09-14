import os
import json
import time
import threading
from flask import Flask, render_template_string, jsonify, request
import libsql_client
import websocket

app = Flask(__name__)

TURSO_DB_URL = os.environ.get("TURSO_DB_URL", "").strip()
TURSO_AUTH_TOKEN = os.environ.get("TURSO_AUTH_TOKEN", "").strip()

SYMBOLS = ['btcusdt', 'ethusdt', 'solusdt', 'xrpusdt', 'avaxusdt', 'linkusdt', 'ltcusdt']

def get_db():
    if not TURSO_DB_URL or not TURSO_AUTH_TOKEN:
        return None
    try:
        # Clean up any accidental quotes or spaces in environment variables
        url = TURSO_DB_URL.strip(" '\"")
        token = TURSO_AUTH_TOKEN.strip(" '\"")
        return libsql_client.create_client_sync(url=url, auth_token=token)
    except Exception as e:
        print("Connection Helper Error:", e)
        return None

def init_db():
    client = get_db()
    if client:
        try:
            client.execute('''CREATE TABLE IF NOT EXISTS candles (
                                symbol TEXT, tf TEXT, time INTEGER,
                                open REAL, high REAL, low REAL, close REAL,
                                delta REAL, totalVol REAL,
                                PRIMARY KEY (symbol, tf, time)
                            )''')
            client.close()
            print("Database Table Initialized Successfully.")
        except Exception as e:
            print("DB Init Error:", e)
    else:
        print("Failed to connect to Turso DB during init.")

init_db()

# TradingView + Footprint + Spoofing UI HTML Template
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
        select:hover, button:hover { background: #3a3a3a; }
        #chart-container { width: 100%; height: 650px; background: #181818; border-radius: 5px; }
        #spoof-alert { background: #2c1515; border: 1px solid #ff4444; color: #ff6666; padding: 8px; border-radius: 4px; margin-top: 10px; font-size: 13px; display: none; }
    </style>
</head>
<body>
    <div id="header">
        <h2>SA Footprint Dashboard</h2>
        <label>Symbol:</label>
        <select id="symbolSelect" onchange="loadChart()">
            <option value="btcusdt">BTCUSDT</option>
            <option value="ethusdt">ETHUSDT</option>
            <option value="solusdt">SOLUSDT</option>
            <option value="xrpusdt">XRPUSDT</option>
            <option value="avaxusdt">AVAXUSDT</option>
            <option value="linkusdt">LINKUSDT</option>
            <option value="ltcusdt">LTCUSDT</option>
        </select>
        <label>Timeframe:</label>
        <select id="tfSelect" onchange="loadChart()">
            <option value="1m">1m</option>
            <option value="3m">3m</option>
            <option value="5m">5m</option>
            <option value="15m">15m</option>
            <option value="1h">1h</option>
            <option value="4h">4h</option>
            <option value="1d">1D</option>
        </select>
        <button onclick="loadChart()">Refresh Data</button>
    </div>

    <div id="chart-container"></div>
    <div id="spoof-alert">⚠️ Spoofing / Large Liquidity Pull Detected at Orderbook Wall!</div>

    <script>
        const chartContainer = document.getElementById('chart-container');
        const chart = LightweightCharts.createChart(chartContainer, {
            layout: { background: { color: '#181818' }, textColor: '#d1d4dc' },
            grid: { vertLines: { color: '#2B2B43' }, horzLines: { color: '#2B2B43' } },
            timeScale: { timeVisible: true, secondsVisible: false }
        });
        const candleSeries = chart.addCandlestickSeries({
            upColor: '#26a69a', downColor: '#ef5350', borderVisible: false,
            wickUpColor: '#26a69a', wickDownColor: '#ef5350'
        });

        async function loadChart() {
            const symbol = document.getElementById('symbolSelect').value;
            const tf = document.getElementById('tfSelect').value;
            const response = await fetch(`/api/candles?symbol=${symbol}&tf=${tf}`);
            const data = await response.json();
            
            if(data && data.length > 0) {
                candleSeries.setData(data);
            } else {
                candleSeries.setData([]);
            }
        }

        // Simulating Live Spoofing & Orderbook feed alert check
        setInterval(async () => {
            const symbol = document.getElementById('symbolSelect').value;
            const res = await fetch(`/api/spoofing?symbol=${symbol}`);
            const alertData = await res.json();
            const alertBox = document.getElementById('spoof-alert');
            if(alertData.spoof_detected) {
                alertBox.style.display = 'block';
                alertBox.innerText = `⚠️ Spoofing Alert: Large wall pulled (${alertData.size} Token) near ${alertData.price}`;
            } else {
                alertBox.style.display = 'none';
            }
        }, 5000);

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
    symbol = request.args.get('symbol', 'btcusdt')
    tf = request.args.get('tf', '1m')
    
    client = get_db()
    if not client:
        return jsonify([])
    
    try:
        res = client.execute("SELECT time, open, high, low, close FROM candles WHERE symbol=? AND tf='1m' ORDER BY time ASC LIMIT 500", [symbol])
        rows = res.rows
        client.close()
        
        raw_data = []
        for r in rows:
            raw_data.append({"time": r[0], "open": r[1], "high": r[2], "low": r[3], "close": r[4]})
        
        if tf != '1m':
            multiplier = 1
            if tf == '3m': multiplier = 3
            elif tf == '5m': multiplier = 5
            elif tf == '15m': multiplier = 15
            elif tf == '1h': multiplier = 60
            elif tf == '4h': multiplier = 240
            elif tf == '1d': multiplier = 1440
            
            aggregated = []
            chunk_size = multiplier * 60
            grouped = {}
            for c in raw_data:
                bucket = (c['time'] // chunk_size) * chunk_size
                if bucket not in grouped:
                    grouped[bucket] = {'time': bucket, 'open': c['open'], 'high': c['high'], 'low': c['low'], 'close': c['close']}
                else:
                    grouped[bucket]['high'] = max(grouped[bucket]['high'], c['high'])
                    grouped[bucket]['low'] = min(grouped[bucket]['low'], c['low'])
                    grouped[bucket]['close'] = c['close']
            aggregated = sorted(list(grouped.values()), key=lambda x: x['time'])
            return jsonify(aggregated)
            
        return jsonify(raw_data)
    except Exception as e:
        print("API Error:", e)
        return jsonify([])

@app.route('/api/spoofing')
def get_spoofing():
    symbol = request.args.get('symbol', 'btcusdt')
    return jsonify({"spoof_detected": False, "price": 0, "size": 0})

def run_binance_stream():
    def on_message(ws, message):
        try:
            data = json.loads(message)
            if 'data' in data:
                d = data['data']
                symbol = d['s'].lower()
                price = float(d['c'])
                vol = float(d['v'])
                timestamp = int(time.time())
                
                client = get_db()
                if client:
                    client.execute("""
                        INSERT INTO candles (symbol, tf, time, open, high, low, close, delta, totalVol)
                        VALUES (?, '1m', ?, ?, ?, ?, ?, 0.0, ?)
                        ON CONFLICT(symbol, tf, time) DO UPDATE SET
                        high = MAX(high, ?), low = MIN(low, ?), close = ?, totalVol = totalVol + ?
                    """, [symbol, timestamp, price, price, price, price, vol, price, price, price, vol])
                    client.close()
        except Exception as e:
            print("Stream Error:", e)

    streams = "/".join([f"{s}@ticker" for s in SYMBOLS])
    socket_url = f"wss://stream.binance.com:9443/stream?streams={streams}"
    
    ws = websocket.WebSocketApp(socket_url, on_message=on_message)
    ws.run_forever()

threading.Thread(target=run_binance_stream, daemon=True).start()

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
