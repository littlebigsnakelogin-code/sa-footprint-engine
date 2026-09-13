import os
import json
import time
import threading
from flask import Flask, render_template_string, jsonify, request
import libsql_client

app = Flask(__name__)

TURSO_DB_URL = os.environ.get("TURSO_DB_URL", "")
TURSO_AUTH_TOKEN = os.environ.get("TURSO_AUTH_TOKEN", "")

def get_db():
    if not TURSO_DB_URL or not TURSO_AUTH_TOKEN:
        return None
    return libsql_client.create_client_sync(url=TURSO_DB_URL, auth_token=TURSO_AUTH_TOKEN)

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
        except Exception as e:
            print("DB Init Error:", e)

init_db()

@app.route('/')
def index():
    return "<h1>SA Footprint Engine - Base Online</h1>"

@app.route('/api/history')
def get_history():
    symbol = request.args.get('symbol', 'btcusdt')
    tf = request.args.get('tf', '1m')
    return jsonify([])

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
