import json
import os
import queue
import threading
import time
import gzip
from collections import defaultdict, deque
from decimal import Decimal, ROUND_FLOOR
import requests
import websocket
from flask import Flask, jsonify, request
from libsql_client import create_client_sync

# ============================================================
# TURSO DATABASE & AUTO-TABLE INITIALIZATION
# ============================================================

TURSO_DATABASE_URL = os.environ.get("TURSO_DATABASE_URL")
TURSO_AUTH_TOKEN = os.environ.get("TURSO_AUTH_TOKEN")

if not TURSO_DATABASE_URL or not TURSO_AUTH_TOKEN:
    print("[TURSO] Environment variables missing")

turso_write_queue = queue.Queue()
turso_worker_started = False
turso_worker_lock = threading.Lock()
turso_client = None
last_turso_cleanup = 0

app = Flask(__name__)

def init_turso_table():
    if not TURSO_DATABASE_URL or not TURSO_AUTH_TOKEN:
        return
    try:
        with create_client_sync(TURSO_DATABASE_URL, auth_token=TURSO_AUTH_TOKEN) as client:
            client.execute("""
                CREATE TABLE IF NOT EXISTS candles (
                    symbol TEXT,
                    tf TEXT,
                    time INTEGER,
                    open REAL,
                    high REAL,
                    low REAL,
                    close REAL,
                    delta REAL,
                    totalVol REAL,
                    buyVol REAL,
                    sellVol REAL,
                    trades INTEGER,
                    poc REAL,
                    pocVol REAL,
                    vah REAL,
                    val REAL,
                    valueAreaVol REAL,
                    footprint TEXT,
                    PRIMARY KEY (symbol, tf, time)
                )
            """)
            print("[TURSO] Candles table verified/created successfully.")
    except Exception as e:
        print(f"[TURSO] Table initialization failed: {e}")

# ============================================================
# CONFIG (4 OPTIMIZED SYMBOLS FOR RENDER FREE TIER)
# ============================================================

SYMBOLS = [
    "BTCUSDT",
    "ETHUSDT",
    "SOLUSDT",
    "XRPUSDT",
]

TIMEFRAMES = {
    "1m": 60,
    "3m": 180,
    "5m": 300,
    "15m": 900,
    "30m": 1800,
    "1h": 3600,
    "4h": 14400,
    "1d": 86400,
}

ROLLING_SECONDS = 12 * 60 * 60  # 12 Hours in RAM (48H stays in Turso DB)

PRICE_STEP = {
    "BTCUSDT": 1.0,
    "ETHUSDT": 0.1,
    "SOLUSDT": 0.01,
    "XRPUSDT": 0.0001,
}

# ============================================================
# MEMORY & LOCKS
# ============================================================

lock = threading.RLock()
collector_start_lock = threading.Lock()
snapshot_ws_lock = threading.Lock()

candles = {
    symbol: {timeframe: deque() for timeframe in TIMEFRAMES}
    for symbol in SYMBOLS
}

current_candles = {
    symbol: {timeframe: None for timeframe in TIMEFRAMES}
    for symbol in SYMBOLS
}

collector_state = {
    "status": "starting",
    "connected": False,
    "trade_stream_connected": False,
    "depth_stream_connected": False,
    "trade_stream_error": None,
    "depth_stream_error": None,
    "error": None,
    "started_at": None,
    "last_message_at": None,
    "raw_message_count": 0,
    "trade_message_count": 0,
    "invalid_message_count": 0,
}

last_trade = {
    symbol: {"time": None, "price": None, "quantity": None}
    for symbol in SYMBOLS
}

orderbook = {
    symbol: {
        "bids": {},
        "asks": {},
        "last_update_id": None,
        "initialized": False,
        "resyncing": False,
        "_init_worker_active": False,
        "buffer": deque(maxlen=1500),
        "liquidity_history": deque(maxlen=3500),
        "liquidity_lots": {
            "bid": defaultdict(deque),
            "ask": defaultdict(deque),
        },
        "trade_history": deque(maxlen=500),
        "trade_match_index": defaultdict(deque),
        "liquidity_match_index": defaultdict(deque),
        "trade_flow_diagnostics": deque(maxlen=50),
        "match_diagnostics": deque(maxlen=50),
        "last_depth_event_time": None,
        "last_depth_update_id": None,
        "sequence_errors": 0,
        "resync_count": 0,
    }
    for symbol in SYMBOLS
}

previous_cluster_state = {
    sym: {"bids": {}, "asks": {}} for sym in SYMBOLS
}

collector_thread = None

# ============================================================
# STARTUP HYDRATION (LOAD PAST 48H FROM TURSO TO RAM)
# ============================================================

def heal_recent_candles_via_ws():
    """Runs ONLY ONCE on server startup to heal any deploy OHLC gaps from Binance Futures WS!"""
    ws = None
    try:
        ws = websocket.create_connection("wss://ws-fapi.binance.com/ws-fapi/v1", timeout=6)
        ws.settimeout(4)
        req_id = 100
        for sym in SYMBOLS:
            for tf, sec in TIMEFRAMES.items():
                req_id += 1
                ws.send(json.dumps({
                    "id": req_id,
                    "method": "klines",
                    "params": {"symbol": sym, "interval": tf, "limit": 8}
                }))
                raw = ws.recv()
                if not raw:
                    continue
                resp = json.loads(raw)
                klines = resp.get("result")
                if not isinstance(klines, list):
                    continue

                now_bucket = floor_timestamp(now_ms(), sec)
                with lock:
                    existing_map = {c["start"]: c for c in candles[sym][tf]}
                    if current_candles[sym][tf] is not None:
                        existing_map[current_candles[sym][tf]["start"]] = current_candles[sym][tf]

                    for k in klines:
                        k_start = int(k[0])
                        k_o, k_h, k_l, k_c = float(k[1]), float(k[2]), float(k[3]), float(k[4])
                        k_vol = float(k[5])
                        k_trades = int(k[8])
                        k_buy = float(k[9])
                        k_sell = max(0.0, k_vol - k_buy)
                        k_delta = k_buy - k_sell

                        if k_start in existing_map:
                            c_obj = existing_map[k_start]
                            c_obj["open"] = k_o
                            c_obj["high"] = max(float(c_obj.get("high", k_h)), k_h)
                            c_obj["low"] = min(float(c_obj.get("low", k_l)), k_l)
                            c_obj["close"] = k_c
                            if k_vol > float(c_obj.get("volume", 0.0)):
                                c_obj["volume"] = k_vol
                                c_obj["buy_volume"] = k_buy
                                c_obj["sell_volume"] = k_sell
                                c_obj["delta"] = k_delta
                                c_obj["trades"] = max(int(c_obj.get("trades", 0)), k_trades)
                        elif k_start == now_bucket and current_candles[sym][tf] is None:
                            current_candles[sym][tf] = {
                                "symbol": sym,
                                "timeframe": tf,
                                "start": k_start,
                                "end": k_start + sec * 1000,
                                "open": k_o,
                                "high": k_h,
                                "low": k_l,
                                "close": k_c,
                                "volume": k_vol,
                                "buy_volume": k_buy,
                                "sell_volume": k_sell,
                                "delta": k_delta,
                                "whale_buy_vol": 0.0,
                                "whale_sell_vol": 0.0,
                                "retail_buy_vol": 0.0,
                                "retail_sell_vol": 0.0,
                                "trades": k_trades,
                                "footprint": {},
                            }
        print("[HEALER] Boot OHLC sync complete (Zero extra Turso writes).")
    except Exception as e:
        print(f"[HEALER] WS Kline sync skipped: {e}")
    finally:
        if ws is not None:
            try:
                ws.close()
            except Exception:
                pass


def load_historical_candles_from_turso():
    if not TURSO_DATABASE_URL or not TURSO_AUTH_TOKEN:
        return
    try:
        cutoff = int(time.time() * 1000) - (ROLLING_SECONDS * 1000)
        with create_client_sync(TURSO_DATABASE_URL, auth_token=TURSO_AUTH_TOKEN) as client:
            res = client.execute(
                "SELECT symbol, tf, time, open, high, low, close, delta, totalVol, buyVol, sellVol, trades, poc, pocVol, vah, val, valueAreaVol, footprint FROM candles WHERE time >= ? ORDER BY time ASC",
                (cutoff,)
            )
            loaded_count = 0
            with lock:
                for row in res.rows:
                    sym, tf, t, o, h, l, c, delta, t_vol, b_vol, s_vol, trades, poc, poc_vol, vah, val, va_vol, fp_json = row
                    if sym not in candles or tf not in candles[sym]:
                        continue
                    try:
                        fp_list = json.loads(fp_json) if fp_json else []
                        step = PRICE_STEP.get(sym, 1.0)
                        margin = step * 30
                        fp_dict = {
                            str(item["price"]): item
                            for item in fp_list
                            if (l - margin) <= float(item.get("price", 0)) <= (h + margin)
                        }
                    except Exception:
                        fp_dict = {}

                    # Restore exact Whale vs Retail volumes from saved footprint cells on server restart
                    w_buy_sum = sum(float(v.get("whale_buy", 0.0)) for v in fp_dict.values())
                    w_sell_sum = sum(float(v.get("whale_sell", 0.0)) for v in fp_dict.values())
                    r_buy_sum = sum(float(v.get("retail_buy", 0.0)) for v in fp_dict.values())
                    r_sell_sum = sum(float(v.get("retail_sell", 0.0)) for v in fp_dict.values())

                    candle_obj = {
                        "symbol": sym,
                        "timeframe": tf,
                        "start": t,
                        "end": t + TIMEFRAMES[tf] * 1000,
                        "open": o,
                        "high": h,
                        "low": l,
                        "close": c,
                        "volume": t_vol,
                        "buy_volume": b_vol,
                        "sell_volume": s_vol,
                        "whale_buy_vol": round(w_buy_sum, 4),
                        "whale_sell_vol": round(w_sell_sum, 4),
                        "retail_buy_vol": round(r_buy_sum, 4),
                        "retail_sell_vol": round(r_sell_sum, 4),
                        "delta": delta,
                        "trades": trades,
                        "poc": poc,
                        "poc_volume": poc_vol,
                        "vah": vah,
                        "val": val,
                        "value_area_volume": va_vol,
                        "footprint": fp_dict,
                    }
                    candles[sym][tf].append(candle_obj)
                    loaded_count += 1
            print(f"[TURSO] Successfully hydrated {loaded_count} historical candles into RAM.")
        threading.Thread(target=heal_recent_candles_via_ws, name="ws-kline-healer", daemon=True).start()
    except Exception as e:
        print(f"[TURSO] Hydration failed: {e}")

# ============================================================
# HELPERS, DYNAMIC WHALE DETECTOR & FOOTPRINT STAMPING
# ============================================================

def now_ms():
    return int(time.time() * 1000)


def floor_timestamp(ts, seconds):
    return int(ts // (seconds * 1000)) * (seconds * 1000)


def round_price_to_step(price, step):
    try:
        price_decimal = Decimal(str(price))
        step_decimal = Decimal(str(step))
        if not price_decimal.is_finite() or not step_decimal.is_finite() or step_decimal <= 0:
            return float(price)
        bucket_count = (price_decimal / step_decimal).to_integral_value(rounding=ROUND_FLOOR)
        return float(bucket_count * step_decimal)
    except Exception:
        return float(price)


# 100% Dynamic Orderflow Relative Trackers (Adapts to Current Market Liquidity & Book Depth!)
rolling_lot_tracker = {"BTCUSDT": 0.04, "ETHUSDT": 0.40, "SOLUSDT": 4.0, "XRPUSDT": 800.0}
rolling_book_depth_tracker = {"BTCUSDT": 1.2, "ETHUSDT": 10.0, "SOLUSDT": 60.0, "XRPUSDT": 12000.0}
burst_flow_tracker = {
    sym: {"ts": 0, "side": None, "price_bucket": 0.0, "cum_qty": 0.0, "count": 0}
    for sym in SYMBOLS
}
paper_post_entry_bounds = {
    "scalp": {"high": None, "low": None},
    "intraday": {"high": None, "low": None},
}

dom_raw_telemetry = {sym: defaultdict(lambda: {
    "w_buy": 0.0, "w_sell": 0.0, "r_buy": 0.0, "r_sell": 0.0,
    "ms_bid_add": 0.0, "ms_ask_add": 0.0,
    "flash_bid_pull": 0.0, "flash_ask_pull": 0.0,
    "aged_bid_pull": 0.0, "aged_ask_pull": 0.0,
    "iceberg_bid": 0.0, "iceberg_ask": 0.0,
    "peak_bid": 0.0, "peak_ask": 0.0, "ts": 0
}) for sym in SYMBOLS}


def create_empty_candle(symbol, timeframe, start, open_price):
    return {
        "symbol": symbol,
        "timeframe": timeframe,
        "start": start,
        "end": start + TIMEFRAMES[timeframe] * 1000,
        "open": open_price,
        "high": open_price,
        "low": open_price,
        "close": open_price,
        "volume": 0.0,
        "buy_volume": 0.0,
        "sell_volume": 0.0,
        "whale_buy_vol": 0.0,
        "whale_sell_vol": 0.0,
        "retail_buy_vol": 0.0,
        "retail_sell_vol": 0.0,
        "delta": 0.0,
        "trades": 0,
        "footprint": {},
    }


# Behavioral Institutional Tracker (Tracks Cluster Sticking/Absorption vs Aggressive Price Displacement)
cluster_behavior_tracker = {
    sym: {
        "anchor_px": None,
        "stuck_start_ms": 0,
        "stuck_buy_vol": 0.0,
        "stuck_sell_vol": 0.0,
        "stuck_trades": 0,
        "last_px": None,
        "last_ts": 0,
        "disp_start_px": None,
        "disp_ts": 0,
        "disp_vol": 0.0,
        "disp_trades": 0,
        "disp_dir": 0,
    }
    for sym in SYMBOLS
}


def add_trade_to_candle(
    candle, price, quantity, is_buyer_maker, price_level=None,
    inst_mode="RETAIL", absorbed_vol=0.0
):
    """
    inst_mode:
      - 'DISP_WHALE': Big chunks + Fast Price Displacement (Aggressive Whale)
      - 'ABS_WHALE': Price stuck in cluster while eating opposing retail orders (Passive Whale)
      - 'RETAIL': Standard retail/noise flow
    """
    candle["high"] = max(candle["high"], price)
    candle["low"] = min(candle["low"], price)
    candle["close"] = price
    candle["volume"] += quantity
    candle["trades"] += 1

    symbol = candle["symbol"]
    step = PRICE_STEP[symbol]
    if price_level is None:
        price_level = round_price_to_step(price, step)
    key = str(price_level)

    level = candle["footprint"].get(key)
    if level is None:
        level = {
            "price": price_level,
            "buy": 0.0,
            "sell": 0.0,
            "whale_buy": 0.0,
            "whale_sell": 0.0,
            "retail_buy": 0.0,
            "retail_sell": 0.0,
            "whale_trades": 0,
            "delta": 0.0,
            "volume": 0.0,
            "trades": 0,
            "pulled_qty": 0.0,
            "bid_pulled": 0.0,
            "ask_pulled": 0.0,
            "whale_pull_chunks": 0,
            "status": "NORMAL",
        }
        candle["footprint"][key] = level

    if is_buyer_maker:
        # Market Sell Order hit the Bid
        candle["sell_volume"] += quantity
        level["sell"] += quantity
        if inst_mode == "DISP_WHALE":
            # Aggressive Whale dumping & displacing price down!
            candle["whale_sell_vol"] = float(candle.get("whale_sell_vol", 0.0)) + quantity
            level["whale_sell"] = float(level.get("whale_sell", 0.0)) + quantity
            level["whale_trades"] = int(level.get("whale_trades", 0)) + 1
        elif inst_mode == "ABS_WHALE":
            # Price is STUCK in cluster while retail sells -> Passive Bid Whale is BUYING/ABSORBING them!
            candle["retail_sell_vol"] = float(candle.get("retail_sell_vol", 0.0)) + quantity
            level["retail_sell"] = float(level.get("retail_sell", 0.0)) + quantity
            w_credit = max(quantity, absorbed_vol)
            candle["whale_buy_vol"] = float(candle.get("whale_buy_vol", 0.0)) + w_credit
            level["whale_buy"] = float(level.get("whale_buy", 0.0)) + w_credit
            level["whale_trades"] = int(level.get("whale_trades", 0)) + 1
            level["status"] = "HEAVY ABSORPTION"
        else:
            candle["retail_sell_vol"] = float(candle.get("retail_sell_vol", 0.0)) + quantity
            level["retail_sell"] = float(level.get("retail_sell", 0.0)) + quantity
    else:
        # Market Buy Order lifted the Ask
        candle["buy_volume"] += quantity
        level["buy"] += quantity
        if inst_mode == "DISP_WHALE":
            # Aggressive Whale sweeping & displacing price up!
            candle["whale_buy_vol"] = float(candle.get("whale_buy_vol", 0.0)) + quantity
            level["whale_buy"] = float(level.get("whale_buy", 0.0)) + quantity
            level["whale_trades"] = int(level.get("whale_trades", 0)) + 1
        elif inst_mode == "ABS_WHALE":
            # Price is STUCK in cluster while retail buys -> Passive Ask Whale is SELLING/CAPPING them!
            candle["retail_buy_vol"] = float(candle.get("retail_buy_vol", 0.0)) + quantity
            level["retail_buy"] = float(level.get("retail_buy", 0.0)) + quantity
            w_credit = max(quantity, absorbed_vol)
            candle["whale_sell_vol"] = float(candle.get("whale_sell_vol", 0.0)) + w_credit
            level["whale_sell"] = float(level.get("whale_sell", 0.0)) + w_credit
            level["whale_trades"] = int(level.get("whale_trades", 0)) + 1
            level["status"] = "HEAVY ABSORPTION"
        else:
            candle["retail_buy_vol"] = float(candle.get("retail_buy_vol", 0.0)) + quantity
            level["retail_buy"] = float(level.get("retail_buy", 0.0)) + quantity

    candle["delta"] = candle["buy_volume"] - candle["sell_volume"]
    level["volume"] += quantity
    level["trades"] += 1
    level["delta"] = level["buy"] - level["sell"]

    # Feed Raw Millisecond DOM Telemetry + True Behavioral Iceberg & Whale Tracking
    if candle["timeframe"] == "1m":
        d_raw = dom_raw_telemetry[symbol]
        d_cell = d_raw[price_level]
        d_cell["ts"] = now_ms()
        if is_buyer_maker:
            if inst_mode == "DISP_WHALE":
                d_cell["w_sell"] += quantity
            elif inst_mode == "ABS_WHALE":
                d_cell["r_sell"] += quantity
                d_cell["w_buy"] += quantity
                d_cell["iceberg_bid"] = round(d_cell["iceberg_bid"] + quantity, 4)
            else:
                d_cell["r_sell"] += quantity
            c_sell_exec = sum((d_raw.get(round_price_to_step(price_level + k * step, step)) or {}).get("w_sell", 0.0) + (d_raw.get(round_price_to_step(price_level + k * step, step)) or {}).get("r_sell", 0.0) for k in (-1, 0, 1))
            c_peak_bid = sum((d_raw.get(round_price_to_step(price_level + k * step, step)) or {}).get("peak_bid", 0.0) for k in (-1, 0, 1))
            if c_peak_bid > 0 and c_sell_exec > c_peak_bid * 1.15:
                d_cell["iceberg_bid"] = max(d_cell["iceberg_bid"], round(c_sell_exec - c_peak_bid, 4))
        else:
            if inst_mode == "DISP_WHALE":
                d_cell["w_buy"] += quantity
            elif inst_mode == "ABS_WHALE":
                d_cell["r_buy"] += quantity
                d_cell["w_sell"] += quantity
                d_cell["iceberg_ask"] = round(d_cell["iceberg_ask"] + quantity, 4)
            else:
                d_cell["r_buy"] += quantity
            c_buy_exec = sum((d_raw.get(round_price_to_step(price_level + k * step, step)) or {}).get("w_buy", 0.0) + (d_raw.get(round_price_to_step(price_level + k * step, step)) or {}).get("r_buy", 0.0) for k in (-1, 0, 1))
            c_peak_ask = sum((d_raw.get(round_price_to_step(price_level + k * step, step)) or {}).get("peak_ask", 0.0) for k in (-1, 0, 1))
            if c_peak_ask > 0 and c_buy_exec > c_peak_ask * 1.15:
                d_cell["iceberg_ask"] = max(d_cell["iceberg_ask"], round(c_buy_exec - c_peak_ask, 4))


def stamp_liquidity_event_to_active_candles(symbol, price_str, side, pulled_qty, old_book_qty, status_str):
    """FOOTPRINT RULE: Stamps >=10s Aged Resting Wall pulls across the active 18-step cluster zone!"""
    step = PRICE_STEP.get(symbol, 1.0)
    try:
        p_float = float(price_str)
    except Exception:
        return
    bucket_price = round_price_to_step(p_float, step)
    key = str(bucket_price)
    max_distance = step * 18.0

    dyn_book_ref = rolling_book_depth_tracker.get(symbol, 1.0)
    whale_chunk_cut = max(dyn_book_ref * 1.35, rolling_lot_tracker.get(symbol, 0.05) * 6.0)

    with lock:
        live_px = last_trade.get(symbol, {}).get("price")
        if live_px is None or abs(bucket_price - float(live_px)) > max_distance:
            return

        for tf in TIMEFRAMES.keys():
            curr = current_candles[symbol][tf]
            if curr is not None and "footprint" in curr:
                if bucket_price > curr["high"] + step * 10.0 or bucket_price < curr["low"] - step * 10.0:
                    continue

                level = curr["footprint"].get(key)
                if level is None:
                    level = {
                        "price": bucket_price,
                        "buy": 0.0,
                        "sell": 0.0,
                        "whale_buy": 0.0,
                        "whale_sell": 0.0,
                        "retail_buy": 0.0,
                        "retail_sell": 0.0,
                        "whale_trades": 0,
                        "delta": 0.0,
                        "volume": 0.0,
                        "trades": 0,
                        "pulled_qty": 0.0,
                        "bid_pulled": 0.0,
                        "ask_pulled": 0.0,
                        "whale_pull_chunks": 0,
                        "max_book_seen": 0.0,
                        "status": "NORMAL",
                    }
                    curr["footprint"][key] = level

                if pulled_qty > 0.0:
                    p_val = float(pulled_qty)
                    peak_seen = max(float(level.get("max_book_seen", 0.0)), float(old_book_qty), p_val)
                    level["max_book_seen"] = peak_seen
                    max_allowed_pull = peak_seen * 1.30

                    if p_val >= whale_chunk_cut:
                        level["whale_pull_chunks"] = int(level.get("whale_pull_chunks", 0)) + 1

                    if str(side).lower() == "bid":
                        new_bp = min(float(level.get("bid_pulled", 0.0)) + p_val, max_allowed_pull)
                        level["bid_pulled"] = round(new_bp, 4)
                    else:
                        new_ap = min(float(level.get("ask_pulled", 0.0)) + p_val, max_allowed_pull)
                        level["ask_pulled"] = round(new_ap, 4)

                    level["pulled_qty"] = round(level["bid_pulled"] + level["ask_pulled"], 4)

                if status_str in ("LIQUIDITY_PULLED", "HEAVY ABSORPTION", "PARTIAL_EXECUTION_PULL"):
                    level["status"] = status_str


# ============================================================
# BINANCE FUTURES SNAPSHOT (WS API)
# ============================================================

ORDERBOOK_SNAPSHOT_LIMIT = 1000

def fetch_orderbook_snapshot_ws(symbol):
    SNAPSHOT_DEADLINE = 20
    SOCKET_TIMEOUT = 6
    ws = None

    try:
        ws_url = "wss://ws-fapi.binance.com/ws-fapi/v1"
        ws = websocket.create_connection(ws_url, timeout=SOCKET_TIMEOUT)
        ws.settimeout(SOCKET_TIMEOUT)

        request_id = int(time.time() * 1000) % 1000000000
        req = {
            "id": request_id,
            "method": "depth",
            "params": {
                "symbol": symbol,
                "limit": ORDERBOOK_SNAPSHOT_LIMIT,
            },
        }
        ws.send(json.dumps(req))

        deadline = time.time() + SNAPSHOT_DEADLINE
        while time.time() < deadline:
            remaining = deadline - time.time()
            if remaining <= 0:
                break
            ws.settimeout(min(SOCKET_TIMEOUT, max(0.5, remaining)))
            try:
                raw = ws.recv()
            except websocket.WebSocketTimeoutException:
                continue
            except Exception:
                return None

            if not raw:
                continue

            response = json.loads(raw)
            if response.get("id") != request_id:
                continue

            if response.get("status") != 200:
                return None

            result = response.get("result")
            if (
                isinstance(result, dict)
                and result.get("lastUpdateId") is not None
                and isinstance(result.get("bids"), list)
                and isinstance(result.get("asks"), list)
            ):
                return result
            return None
        return None
    except Exception:
        return None
    finally:
        if ws is not None:
            try:
                ws.close()
            except Exception:
                pass

# ============================================================
# EXECUTION MATCHING & FIFO/LIFO ACCOUNTING
# ============================================================

def finalize_liquidity_records(symbol, current_time_ms=None):
    if symbol not in orderbook:
        return 0

    has_event_time = current_time_ms is not None
    if current_time_ms is None:
        current_time_ms = now_ms()

    state = orderbook[symbol]
    history = state["liquidity_history"]
    finalized_count = 0

    for record in history:
        if not isinstance(record, dict) or record.get("finalized"):
            continue

        record_time = int(record.get("time", current_time_ms))
        if current_time_ms - record_time < 450:
            continue

        reduced_qty = float(record.get("reduced_qty", 0.0))
        executed_qty = min(float(record.get("executed_qty", 0.0)), reduced_qty)
        total_unmatched_qty = max(0.0, reduced_qty - executed_qty)

        aged_unmatched_raw = float(record.get("aged_unmatched_qty", 0.0))
        aged_consumed_qty = float(record.get("aged_consumed_qty", 0.0))

        pulled_qty = min(max(0.0, aged_unmatched_raw), total_unmatched_qty)
        pull_pct = (pulled_qty / aged_consumed_qty) if aged_consumed_qty > 0 else 0.0

        record["executed_qty"] = round(executed_qty, 8)
        record["unmatched_qty"] = round(total_unmatched_qty, 8)
        record["pulled_qty"] = round(pulled_qty, 8)
        record["pull_pct"] = round(pull_pct, 4)
        record["finalized"] = True

        if reduced_qty <= 0:
            record["status"] = "NO_REDUCTION"
        elif executed_qty >= reduced_qty * 0.7:
            record["status"] = "HEAVY ABSORPTION"
        elif pulled_qty > 0 and pull_pct >= 0.65:
            record["status"] = "LIQUIDITY_PULLED"
        elif executed_qty > 0 and pulled_qty > 0:
            record["status"] = "PARTIAL_EXECUTION_PULL"
        else:
            record["status"] = "UNCONFIRMED_REDUCTION"

        if pulled_qty > 0.0 and record["status"] != "HEAVY ABSORPTION":
            stamp_liquidity_event_to_active_candles(
                symbol,
                record.get("price"),
                record.get("side"),
                pulled_qty,
                max(aged_consumed_qty, pulled_qty),
                record["status"],
            )
        finalized_count += 1

    liquidity_index = state.get("liquidity_match_index")
    if isinstance(liquidity_index, dict):
        history_ids = {id(r) for r in history if isinstance(r, dict)}
        for key, records in list(liquidity_index.items()):
            retained = deque(
                r for r in records
                if isinstance(r, dict) and id(r) in history_ids and not r.get("finalized")
            )
            if retained:
                liquidity_index[key] = retained
            else:
                liquidity_index.pop(key, None)

    trade_index = state.get("trade_match_index")
    if isinstance(trade_index, dict):
        trade_cutoff = current_time_ms - 1600 if has_event_time else None
        for key, trades in list(trade_index.items()):
            retained_trades = deque(
                t for t in trades
                if isinstance(t, dict)
                and float(t.get("remaining_qty", 0.0)) > 1e-12
                and (trade_cutoff is None or int(t.get("time", 0)) >= trade_cutoff)
            )
            if retained_trades:
                trade_index[key] = retained_trades
            else:
                trade_index.pop(key, None)

    return finalized_count


def record_trade_for_execution_matching(symbol, price, quantity, trade_time, is_buyer_maker):
    if symbol not in orderbook:
        return 0.0

    price = float(price)
    quantity = float(quantity)
    trade_time = int(trade_time)
    is_buyer_maker = bool(is_buyer_maker)

    if price <= 0.0 or quantity <= 0.0:
        return 0.0

    state = orderbook[symbol]
    expected_side = "bid" if is_buyer_maker else "ask"
    price_key = round(price, 8)

    trade_match_index = state["trade_match_index"]
    liquidity_match_index = state["liquidity_match_index"]
    same_price_key = (expected_side, price_key)

    trade_record = {
        "time": trade_time,
        "price": price,
        "quantity": quantity,
        "remaining_qty": quantity,
        "is_buyer_maker": is_buyer_maker,
    }

    trade_match_index[same_price_key].append(trade_record)
    matching_records = liquidity_match_index.get(same_price_key, ())
    matched_total = 0.0
    epsilon = 1e-12

    for liquidity in matching_records:
        if trade_record["remaining_qty"] <= epsilon:
            break
        if liquidity.get("finalized"):
            continue

        reduced_qty = float(liquidity.get("reduced_qty", 0.0))
        executed_qty = float(liquidity.get("executed_qty", 0.0))
        liquidity_time = int(liquidity.get("time", trade_time))
        liquidity_price = float(liquidity.get("price", price))

        remaining_reduction = max(0.0, reduced_qty - executed_qty)
        if remaining_reduction <= epsilon:
            continue

        time_difference = trade_time - liquidity_time
        if time_difference < -300 or time_difference > 1500:
            continue
        if abs(liquidity_price - price) > 1e-6:
            continue

        available_trade = max(0.0, float(trade_record["remaining_qty"]))
        matched_qty = min(available_trade, remaining_reduction)
        if matched_qty <= epsilon:
            continue

        new_executed_qty = min(reduced_qty, executed_qty + matched_qty)
        actual_matched_qty = max(0.0, new_executed_qty - executed_qty)
        if actual_matched_qty <= epsilon:
            continue

        liquidity["executed_qty"] = new_executed_qty
        liquidity["unmatched_qty"] = max(0.0, reduced_qty - new_executed_qty)
        # Deduct late trade directly from aged_unmatched_qty so eaten walls NEVER register as pulls!
        liquidity["aged_unmatched_qty"] = max(0.0, float(liquidity.get("aged_unmatched_qty", 0.0)) - actual_matched_qty)
        liquidity["aged_executed_qty"] = float(liquidity.get("aged_executed_qty", 0.0)) + actual_matched_qty
        liquidity["pulled_qty"] = 0.0
        liquidity["pull_pct"] = 0.0
        liquidity["status"] = "reduction_pending"

        trade_record["remaining_qty"] = max(0.0, available_trade - actual_matched_qty)
        matched_total += actual_matched_qty

    return matched_total


def match_trade_to_liquidity_reduction(symbol, liquidity_side, liquidity_price, reduced_qty, reduction_time):
    if symbol not in orderbook:
        return 0.0

    liquidity_price_float = float(liquidity_price)
    reduced_qty_float = float(reduced_qty)
    reduction_time_int = int(reduction_time)

    if liquidity_price_float <= 0 or reduced_qty_float <= 0:
        return 0.0

    expected_is_buyer_maker = (liquidity_side == "bid")
    price_key = round(liquidity_price_float, 8)
    state = orderbook[symbol]
    trade_index = state["trade_match_index"]

    trades_at_price = trade_index.get((liquidity_side, price_key), ())
    matched_qty = 0.0

    if not trades_at_price:
        return 0.0

    for trade in trades_at_price:
        if matched_qty >= reduced_qty_float:
            break
        trade_time = int(trade.get("time", 0))
        trade_remaining = float(trade.get("remaining_qty", 0.0))
        if trade_remaining <= 1e-12:
            continue
        if bool(trade.get("is_buyer_maker")) != expected_is_buyer_maker:
            continue

        time_diff_ms = trade_time - reduction_time_int
        if time_diff_ms < -300 or time_diff_ms > 1500:
            continue

        available_reduction = reduced_qty_float - matched_qty
        qty_to_match = min(trade_remaining, available_reduction)
        if qty_to_match <= 1e-12:
            continue

        trade["remaining_qty"] = max(0.0, trade_remaining - qty_to_match)
        matched_qty += qty_to_match

    return matched_qty

# ============================================================
# ORDERBOOK EVENT APPLICATION (DYNAMIC DEPTH & SPOOF TRACKING)
# ============================================================

def apply_orderbook_event(symbol, event):
    state = orderbook[symbol]
    event_update_id = int(event["u"])
    event_time = int(event.get("E", now_ms()))

    finalize_liquidity_records(symbol, event_time)
    liquidity_index = state["liquidity_match_index"]

    step = PRICE_STEP.get(symbol, 1.0)
    live_px = last_trade.get(symbol, {}).get("price")
    dom_raw = dom_raw_telemetry[symbol]

    # Dynamically adapt resting depth baseline from current active book
    dyn_book_avg = rolling_book_depth_tracker.get(symbol, 1.0)
    min_track_qty = max(dyn_book_avg * 0.55, rolling_lot_tracker.get(symbol, 0.04) * 3.0)
    flash_chunk_qty = min_track_qty * 0.65
    MIN_WALL_AGE_MS = 10000

    if event_update_id % 60 == 0 and dom_raw:
        cutoff_ts = event_time - 300000
        for k_px, v_obj in list(dom_raw.items()):
            if v_obj["ts"] > 0 and v_obj["ts"] < cutoff_ts:
                dom_raw.pop(k_px, None)

    def process_side(side, event_levels, book):
        nonlocal dyn_book_avg
        is_bid = (side == "bid")
        for price, quantity in event_levels:
            price = str(price)
            p_flt = float(price)
            new_quantity = float(quantity)
            old_quantity = float(book.get(price, 0.0))

            added_qty = max(new_quantity - old_quantity, 0.0)
            reduced_qty = max(old_quantity - new_quantity, 0.0)

            if new_quantity == 0.0:
                book.pop(price, None)
            else:
                book[price] = new_quantity

            b_px = round_price_to_step(p_flt, step)
            near_live = (live_px is None) or (abs(b_px - float(live_px)) <= step * 35.0)
            if near_live and new_quantity > 0:
                dyn_book_avg = (dyn_book_avg * 0.995) + (new_quantity * 0.005)
                rolling_book_depth_tracker[symbol] = dyn_book_avg

            d_cell = dom_raw[b_px] if near_live else None
            if d_cell is not None:
                d_cell["ts"] = event_time
                if is_bid:
                    d_cell["peak_bid"] = max(d_cell["peak_bid"], new_quantity, old_quantity)
                else:
                    d_cell["peak_ask"] = max(d_cell["peak_ask"], new_quantity, old_quantity)

            lots = state["liquidity_lots"][side][price]

            if added_qty > 1e-8:
                lots.append({
                    "original_qty": added_qty,
                    "remaining_qty": added_qty,
                    "time": event_time,
                    "origin": "depth_add",
                    "update_id": event_update_id,
                })
                if d_cell is not None:
                    if is_bid:
                        d_cell["ms_bid_add"] += added_qty
                    else:
                        d_cell["ms_ask_add"] += added_qty

            aged_consumed_qty = 0.0
            aged_executed_qty = 0.0
            aged_unmatched_qty = 0.0
            flash_unmatched_qty = 0.0

            if reduced_qty > 1e-8:
                executed_qty = match_trade_to_liquidity_reduction(
                    symbol, side, price, reduced_qty, event_time
                )
                executed_qty = min(max(float(executed_qty), 0.0), reduced_qty)
                canceled_qty = max(reduced_qty - executed_qty, 0.0)

                rem_exec = executed_qty
                while rem_exec > 1e-12 and lots:
                    oldest_lot = lots[0]
                    lot_rem = float(oldest_lot.get("remaining_qty", 0.0))
                    if lot_rem <= 1e-12:
                        lots.popleft()
                        continue

                    take = min(lot_rem, rem_exec)
                    oldest_lot["remaining_qty"] = max(0.0, lot_rem - take)
                    lot_age = event_time - int(oldest_lot.get("time", event_time))
                    if lot_age >= MIN_WALL_AGE_MS:
                        aged_consumed_qty += take
                        aged_executed_qty += take

                    rem_exec -= take
                    if oldest_lot["remaining_qty"] <= 1e-12:
                        lots.popleft()

                rem_cancel = canceled_qty
                while rem_cancel > 1e-12 and lots:
                    newest_lot = lots[-1]
                    lot_rem = float(newest_lot.get("remaining_qty", 0.0))
                    if lot_rem <= 1e-12:
                        lots.pop()
                        continue

                    take = min(lot_rem, rem_cancel)
                    newest_lot["remaining_qty"] = max(0.0, lot_rem - take)
                    lot_age = event_time - int(newest_lot.get("time", event_time))

                    if lot_age >= MIN_WALL_AGE_MS:
                        if canceled_qty >= min_track_qty:
                            aged_consumed_qty += take
                            aged_unmatched_qty += take
                    else:
                        if canceled_qty >= flash_chunk_qty and lot_age >= 50:
                            flash_unmatched_qty += take

                    rem_cancel -= take
                    if newest_lot["remaining_qty"] <= 1e-12:
                        lots.pop()

                if not lots:
                    state["liquidity_lots"][side].pop(price, None)

                if d_cell is not None:
                    if is_bid:
                        d_cell["flash_bid_pull"] += flash_unmatched_qty
                        d_cell["aged_bid_pull"] += aged_unmatched_qty
                    else:
                        d_cell["flash_ask_pull"] += flash_unmatched_qty
                        d_cell["aged_ask_pull"] += aged_unmatched_qty

                if aged_unmatched_qty >= min_track_qty:
                    liquidity_record = {
                        "time": event_time,
                        "update_id": event_update_id,
                        "side": side,
                        "price": price,
                        "old_qty": old_quantity,
                        "new_qty": new_quantity,
                        "added_qty": added_qty,
                        "reduced_qty": reduced_qty,
                        "executed_qty": executed_qty,
                        "unmatched_qty": canceled_qty,
                        "aged_consumed_qty": aged_consumed_qty,
                        "aged_executed_qty": aged_executed_qty,
                        "aged_unmatched_qty": aged_unmatched_qty,
                        "pulled_qty": 0.0,
                        "pull_pct": 0.0,
                        "finalized": False,
                        "fifo_reduction": True,
                        "status": "reduction_pending",
                    }
                    state["liquidity_history"].append(liquidity_record)
                    price_key = round(p_flt, 8)
                    liquidity_index[(side, price_key)].append(liquidity_record)
            else:
                if not lots:
                    state["liquidity_lots"][side].pop(price, None)

    process_side("bid", event.get("b", []), state["bids"])
    process_side("ask", event.get("a", []), state["asks"])

    state["last_update_id"] = event_update_id
    state["last_depth_update_id"] = event_update_id
    state["last_depth_event_time"] = now_ms()

# ============================================================
# ORDERBOOK INITIALIZATION & BUFFER AUTO-CATCHUP
# ============================================================

def initialize_orderbook(symbol):
    RETRY_BASE_SECONDS = 0.5
    RETRY_MAX_SECONDS = 5.0
    BRIDGE_TIMEOUT_SECONDS = 20.0
    BRIDGE_CHECK_INTERVAL = 0.05

    with lock:
        state = orderbook[symbol]
        if state.get("_init_worker_active", False):
            return False
        state["_init_worker_active"] = True
        state["initialized"] = False
        state["resyncing"] = True

    attempt = 0
    snapshot = None
    snapshot_id = None
    snapshot_bids = None
    snapshot_asks = None
    bridge_started = None

    try:
        while True:
            with lock:
                if not collector_state.get("depth_stream_connected", False):
                    return False

            if snapshot is None:
                attempt += 1
                with snapshot_ws_lock:
                    with lock:
                        if not collector_state.get("depth_stream_connected", False):
                            return False
                    new_snapshot = fetch_orderbook_snapshot_ws(symbol)

                if not isinstance(new_snapshot, dict):
                    delay = min(RETRY_BASE_SECONDS * (2 ** min(attempt - 1, 4)), RETRY_MAX_SECONDS)
                    time.sleep(delay)
                    continue

                try:
                    snapshot_id = int(new_snapshot["lastUpdateId"])
                    snapshot_bids = {
                        str(p): float(q) for p, q in new_snapshot.get("bids", []) if float(q) > 0
                    }
                    snapshot_asks = {
                        str(p): float(q) for p, q in new_snapshot.get("asks", []) if float(q) > 0
                    }
                    snapshot = new_snapshot
                    bridge_started = time.monotonic()
                except Exception:
                    snapshot = None
                    time.sleep(1.0)
                    continue

            target_id = snapshot_id + 1
            restart_fresh = False

            while True:
                with lock:
                    state = orderbook[symbol]
                    if not collector_state.get("depth_stream_connected", False):
                        return False

                    while state["buffer"]:
                        first_u = int(state["buffer"][0]["u"])
                        if first_u <= snapshot_id:
                            state["buffer"].popleft()
                        else:
                            break

                    events = list(state["buffer"])
                    bridge_mode = None
                    sequence_error = None

                    if events:
                        first = events[0]
                        first_U = int(first["U"])
                        first_u = int(first["u"])
                        first_pu = int(first["pu"]) if first.get("pu") is not None else None

                        if first_U <= target_id <= first_u:
                            bridge_mode = "OVERLAP"
                        elif first_pu == snapshot_id:
                            bridge_mode = "PU_CONTINUITY"
                        elif first_U > target_id:
                            sequence_error = "FIRST_EVENT_PAST_SNAPSHOT"

                    if bridge_mode is not None:
                        valid_events = []
                        previous_u = snapshot_id

                        for idx, ev in enumerate(events):
                            ev_U = int(ev["U"])
                            ev_u = int(ev["u"])
                            ev_pu = int(ev["pu"]) if ev.get("pu") is not None else None

                            if ev_u <= previous_u:
                                continue
                            if idx > 0 and ev_pu is not None and ev_pu != previous_u:
                                sequence_error = "SEQUENCE_GAP"
                                break

                            valid_events.append(ev)
                            previous_u = ev_u

                        if sequence_error is None:
                            state["bids"] = snapshot_bids.copy()
                            state["asks"] = snapshot_asks.copy()
                            state["liquidity_lots"]["bid"].clear()
                            state["liquidity_lots"]["ask"].clear()

                            snap_time = now_ms()
                            for p, q in snapshot_bids.items():
                                state["liquidity_lots"]["bid"][p].append({
                                    "original_qty": q,
                                    "remaining_qty": q,
                                    "time": snap_time,
                                    "origin": "snapshot",
                                    "update_id": snapshot_id,
                                })
                            for p, q in snapshot_asks.items():
                                state["liquidity_lots"]["ask"][p].append({
                                    "original_qty": q,
                                    "remaining_qty": q,
                                    "time": snap_time,
                                    "origin": "snapshot",
                                    "update_id": snapshot_id,
                                })

                            state["last_update_id"] = snapshot_id
                            for ev in valid_events:
                                apply_orderbook_event(symbol, ev)

                            state["last_update_id"] = previous_u
                            state["last_depth_update_id"] = previous_u
                            state["last_depth_event_time"] = now_ms()
                            state["buffer"].clear()
                            state["initialized"] = True
                            state["resyncing"] = False
                            state["sequence_errors"] = 0

                            return True

                    if sequence_error is not None:
                        restart_fresh = True
                    elif (time.monotonic() - bridge_started) >= BRIDGE_TIMEOUT_SECONDS and events:
                        restart_fresh = True

                if restart_fresh:
                    snapshot = None
                    time.sleep(0.5)
                    break

                time.sleep(BRIDGE_CHECK_INTERVAL)

    finally:
        restart_init = False
        with lock:
            state = orderbook[symbol]
            state["_init_worker_active"] = False

            if state.get("initialized", False):
                state["resyncing"] = False
            elif collector_state.get("depth_stream_connected", False):
                state["resyncing"] = True
                restart_init = True
            else:
                state["resyncing"] = False

        if restart_init:
            threading.Thread(
                target=initialize_orderbook,
                args=(symbol,),
                name=f"orderbook-init-{symbol}-retry",
                daemon=True,
            ).start()


def handle_depth_update(symbol, event):
    try:
        event_first_id = int(event["U"])
        event_final_id = int(event["u"])
        event_previous_id = int(event["pu"]) if event.get("pu") is not None else None
    except Exception:
        return

    start_initializer = False

    with lock:
        state = orderbook[symbol]
        state["last_depth_event_time"] = now_ms()

        if not state["initialized"]:
            state["buffer"].append(event)
            if not state.get("_init_worker_active", False):
                state["resyncing"] = True
                start_initializer = True
        else:
            previous_u = state.get("last_update_id")
            if previous_u is None:
                state["initialized"] = False
                state["resyncing"] = True
                state["buffer"].clear()
                state["buffer"].append(event)
                start_initializer = not state.get("_init_worker_active", False)
            elif event_final_id <= int(previous_u):
                return
            elif event_previous_id is not None and event_previous_id != int(previous_u):
                state["sequence_errors"] += 1
                state["resync_count"] += 1
                state["initialized"] = False
                state["resyncing"] = True
                state["buffer"].clear()
                state["buffer"].append(event)
                start_initializer = not state.get("_init_worker_active", False)
            else:
                apply_orderbook_event(symbol, event)

    if start_initializer:
        threading.Thread(
            target=initialize_orderbook,
            args=(symbol,),
            name=f"orderbook-init-{symbol}",
            daemon=True,
        ).start()

# ============================================================
# VALUE AREA & CANDLE FINALIZATION
# ============================================================

def calculate_value_area(footprint, value_area_percent=0.70):
    if not footprint:
        return None, None, 0.0

    levels = sorted(footprint.values() if isinstance(footprint, dict) else footprint, key=lambda x: x["price"])
    total_volume = sum(level["volume"] for level in levels)
    if total_volume <= 0:
        return None, None, 0.0

    target_volume = total_volume * value_area_percent
    poc_index = max(range(len(levels)), key=lambda i: levels[i]["volume"])
    included = {poc_index}
    value_area_volume = levels[poc_index]["volume"]

    lower = poc_index - 1
    upper = poc_index + 1

    while value_area_volume < target_volume:
        lower_vol = levels[lower]["volume"] if lower >= 0 else -1
        upper_vol = levels[upper]["volume"] if upper < len(levels) else -1
        if lower_vol < 0 and upper_vol < 0:
            break
        if upper_vol >= lower_vol:
            included.add(upper)
            value_area_volume += upper_vol
            upper += 1
        else:
            included.add(lower)
            value_area_volume += lower_vol
            lower -= 1

    vah = max(levels[i]["price"] for i in included)
    val = min(levels[i]["price"] for i in included)
    return vah, val, value_area_volume


def finalize_candle(candle):
    if candle is None:
        return None
    result = dict(candle)
    fp_raw = candle["footprint"]
    footprint = sorted(fp_raw.values() if isinstance(fp_raw, dict) else fp_raw, key=lambda x: x["price"])
    result["footprint"] = footprint

    if footprint:
        poc = max(footprint, key=lambda x: x["volume"])
        result["poc"] = poc["price"]
        result["poc_volume"] = poc["volume"]
    else:
        result["poc"] = None
        result["poc_volume"] = 0.0

    vah, val, va_vol = calculate_value_area(footprint, 0.70)
    result["vah"] = vah
    result["val"] = val
    result["value_area_volume"] = va_vol
    result["value_area_percent"] = 0.70
    return result

# ============================================================
# TURSO PERSISTENCE (STRICT 48-HOUR RETENTION)
# ============================================================

def save_candle_to_turso(candle):
    if not TURSO_DATABASE_URL or not TURSO_AUTH_TOKEN:
        return
    try:
        fp_raw = candle.get("footprint", {})
        fp_list = list(fp_raw.values()) if isinstance(fp_raw, dict) else fp_raw
        footprint_json = json.dumps(fp_list, separators=(",", ":"))
        
        sql = """
        INSERT OR REPLACE INTO candles (
            symbol, tf, time, open, high, low, close, delta, totalVol,
            buyVol, sellVol, trades, poc, pocVol, vah, val, valueAreaVol, footprint
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        args = (
            candle["symbol"], candle["timeframe"], candle["start"], candle["open"],
            candle["high"], candle["low"], candle["close"], candle["delta"],
            candle["volume"], candle["buy_volume"], candle["sell_volume"],
            candle["trades"], candle.get("poc"), candle.get("poc_volume"),
            candle.get("vah"), candle.get("val"), candle.get("value_area_volume"),
            footprint_json,
        )
        turso_write_queue.put({"sql": sql, "args": args})
    except Exception as e:
        print(f"[TURSO] Queue error: {e}")


def _turso_worker_loop():
    global turso_client
    while True:
        job = turso_write_queue.get()
        try:
            if job is None:
                break
            if turso_client is None:
                turso_client = create_client_sync(TURSO_DATABASE_URL, auth_token=TURSO_AUTH_TOKEN)
            if job.get("cleanup"):
                cutoff = int(time.time() * 1000) - (48 * 60 * 60 * 1000)
                turso_client.execute("DELETE FROM candles WHERE time < ?", (cutoff,))
            else:
                turso_client.execute(job["sql"], job["args"])
        except Exception as e:
            print(f"[TURSO] Worker error: {e}")
            try:
                if turso_client:
                    turso_client.close()
            except Exception:
                pass
            turso_client = None
        finally:
            turso_write_queue.task_done()


def ensure_turso_worker_started():
    global turso_worker_started
    if turso_worker_started:
        return
    with turso_worker_lock:
        if turso_worker_started:
            return
        init_turso_table()
        load_historical_candles_from_turso()
        threading.Thread(target=_turso_worker_loop, name="turso-worker", daemon=True).start()
        turso_worker_started = True


def store_finished_candle(candle):
    global last_turso_cleanup
    if candle is None:
        return
    symbol = candle["symbol"]
    timeframe = candle["timeframe"]
    finished = finalize_candle(candle)
    if finished is None:
        return

    with lock:
        candles[symbol][timeframe].append(finished)
        cutoff = (time.time() - ROLLING_SECONDS) * 1000
        while candles[symbol][timeframe] and candles[symbol][timeframe][0]["start"] < cutoff:
            candles[symbol][timeframe].popleft()

    ensure_turso_worker_started()
    save_candle_to_turso(finished)

    now = time.time()
    if now - last_turso_cleanup >= 300 and TURSO_DATABASE_URL and TURSO_AUTH_TOKEN:
        last_turso_cleanup = now
        turso_write_queue.put({"cleanup": True})

# ============================================================
# TRADE & MESSAGE ROUTER (100% DYNAMIC WHALE ORDERFLOW ENGINE)
# ============================================================

# Absolute Minimum Institutional Cluster Volume (Below this = 100% Retail / Bot Noise!)
INST_MIN_CLUSTER_VOL = {
    "BTCUSDT": 8.0,      # At least 8+ BTC in a fast sweep or stuck cluster to be called Institutional!
    "ETHUSDT": 80.0,     # At least 80+ ETH
    "SOLUSDT": 600.0,    # At least 600+ SOL
    "XRPUSDT": 120000.0, # At least 120k+ XRP
}

def process_trade(symbol, price, quantity, trade_time, is_buyer_maker):
    step = PRICE_STEP[symbol]
    price_level = round_price_to_step(price, step)

    with lock:
        record_trade_for_execution_matching(symbol, price, quantity, trade_time, is_buyer_maker)

        cb = cluster_behavior_tracker[symbol]
        inst_floor = INST_MIN_CLUSTER_VOL.get(symbol, 10.0)
        dyn_book_avg = max(rolling_book_depth_tracker.get(symbol, inst_floor), inst_floor)

        # 1. Track Price Sticking in a 3-Tick Cluster (Absorption Detection)
        if cb["anchor_px"] is None or abs(price_level - cb["anchor_px"]) > step * 2.5:
            cb["anchor_px"] = price_level
            cb["stuck_start_ms"] = trade_time
            cb["stuck_buy_vol"] = 0.0 if is_buyer_maker else quantity
            cb["stuck_sell_vol"] = quantity if is_buyer_maker else 0.0
            cb["stuck_trades"] = 1
        else:
            if is_buyer_maker:
                cb["stuck_sell_vol"] += quantity
            else:
                cb["stuck_buy_vol"] += quantity
            cb["stuck_trades"] += 1

        # 2. Track Fast Price Displacement / Momentum Sweep (Within 1200ms window)
        cur_dir = -1 if is_buyer_maker else 1
        if cb["disp_start_px"] is None or (trade_time - cb["disp_ts"] > 1200) or (cb["disp_dir"] != cur_dir):
            cb["disp_start_px"] = price
            cb["disp_ts"] = trade_time
            cb["disp_vol"] = quantity
            cb["disp_trades"] = 1
            cb["disp_dir"] = cur_dir
        else:
            cb["disp_vol"] += quantity
            cb["disp_trades"] += 1

        price_disp_steps = abs(price - cb["disp_start_px"]) / step

        d_raw = dom_raw_telemetry[symbol]
        zone_bid_pull = sum((d_raw.get(round_price_to_step(price_level + k * step, step)) or {}).get("aged_bid_pull", 0.0) for k in (-1, 0, 1))
        zone_ask_pull = sum((d_raw.get(round_price_to_step(price_level + k * step, step)) or {}).get("aged_ask_pull", 0.0) for k in (-1, 0, 1))

        inst_mode = "RETAIL"
        absorbed_credit = 0.0

        # RULE A: AGGRESSIVE WHALE DISPLACEMENT
        # Must move price >= 3 steps within 1.2s WITH at least inst_floor (e.g. 8+ BTC) or single block >= 3.5+ BTC!
        if (
            (price_disp_steps >= 3.0 and cb["disp_vol"] >= inst_floor)
            or (quantity >= inst_floor * 0.45 and price_disp_steps >= 1.5)
        ):
            inst_mode = "DISP_WHALE"

        # RULE B: PASSIVE WHALE ABSORPTION
        # Price STUCK in 3-tick cluster for >= 1.5s while eating >= inst_floor (8+ BTC) one-sided retail orders!
        elif cb["stuck_trades"] >= 8 and (trade_time - cb["stuck_start_ms"] >= 1500):
            if is_buyer_maker and cb["stuck_sell_vol"] >= inst_floor and cb["stuck_sell_vol"] > cb["stuck_buy_vol"] * 1.8 and zone_bid_pull < cb["stuck_sell_vol"] * 0.30:
                inst_mode = "ABS_WHALE"
                absorbed_credit = quantity
            elif (not is_buyer_maker) and cb["stuck_buy_vol"] >= inst_floor and cb["stuck_buy_vol"] > cb["stuck_sell_vol"] * 1.8 and zone_ask_pull < cb["stuck_buy_vol"] * 0.30:
                inst_mode = "ABS_WHALE"
                absorbed_credit = quantity

        cb["last_px"] = price
        cb["last_ts"] = trade_time

        for timeframe, seconds in TIMEFRAMES.items():
            start = floor_timestamp(trade_time, seconds)
            current = current_candles[symbol][timeframe]

            if current is None:
                prev_list = candles[symbol][timeframe]
                open_px = prev_list[-1]["close"] if prev_list and (start - prev_list[-1]["start"] == seconds * 1000) else price
                current = create_empty_candle(symbol, timeframe, start, open_px)
                current_candles[symbol][timeframe] = current
            elif start != current["start"]:
                prev_close = current["close"]
                store_finished_candle(current)
                current = create_empty_candle(symbol, timeframe, start, prev_close)
                current_candles[symbol][timeframe] = current

            add_trade_to_candle(current, price, quantity, is_buyer_maker, price_level, inst_mode, absorbed_credit)

        last_trade[symbol] = {
            "time": trade_time,
            "price": price,
            "quantity": quantity,
        }

        if paper_bot_state.get("enabled") and paper_bot_state.get("symbol") == symbol:
            for mode in ("scalp", "intraday"):
                act = paper_bot_state.get(mode, {}).get("active")
                if act and act.get("symbol") == symbol:
                    b = paper_post_entry_bounds[mode]
                    b["high"] = price if b["high"] is None else max(b["high"], price)
                    b["low"] = price if b["low"] is None else min(b["low"], price)


def handle_message(ws, message):
    with lock:
        collector_state["raw_message_count"] += 1
        collector_state["last_message_at"] = now_ms()

    try:
        payload = json.loads(message)
        data = payload.get("data", payload)
        if not isinstance(data, dict):
            return

        symbol = str(data.get("s", "")).upper()
        if symbol not in SYMBOLS:
            stream_name = str(payload.get("stream", ""))
            if stream_name:
                sym_candidate = stream_name.split("@", 1)[0].upper()
                if sym_candidate in SYMBOLS:
                    symbol = sym_candidate
        if symbol not in SYMBOLS:
            return

        event_type = data.get("e")
        if event_type == "depthUpdate" or ("U" in data and "u" in data):
            handle_depth_update(symbol, data)
            return

        if event_type not in ("trade", "aggTrade"):
            return

        price = float(data.get("p", 0))
        quantity = float(data.get("q", 0))
        trade_time = int(data.get("T", 0))
        is_buyer_maker = bool(data.get("m", False))

        if price <= 0 or quantity <= 0 or trade_time <= 0:
            with lock:
                collector_state["invalid_message_count"] += 1
            return

        with lock:
            collector_state["trade_message_count"] += 1

        process_trade(symbol, price, quantity, trade_time, is_buyer_maker)

    except Exception as exc:
        with lock:
            collector_state["invalid_message_count"] += 1
            collector_state["error"] = str(exc)

# ============================================================
# LIGHTWEIGHT MULTIPLEXED COLLECTOR
# ============================================================

def collector_loop():
    trade_streams = [f"{s.lower()}@aggTrade" for s in SYMBOLS]
    depth_streams = [f"{s.lower()}@depth@100ms" for s in SYMBOLS]

    trade_url = f"wss://fstream.binance.com/market/stream?streams={'/'.join(trade_streams)}"
    depth_url = f"wss://fstream.binance.com/public/stream?streams={'/'.join(depth_streams)}"

    with lock:
        collector_state["status"] = "connecting"
        collector_state["started_at"] = now_ms()

    def update_status():
        t_ok = collector_state["trade_stream_connected"]
        d_ok = collector_state["depth_stream_connected"]
        collector_state["connected"] = d_ok
        if t_ok and d_ok:
            collector_state["status"] = "connected"
        elif t_ok or d_ok:
            collector_state["status"] = "partial"
        else:
            collector_state["status"] = "connecting"

    def run_multiplexed_stream(role, url):
        is_depth = (role == "depth")
        err_key = "depth_stream_error" if is_depth else "trade_stream_error"
        conn_key = "depth_stream_connected" if is_depth else "trade_stream_connected"
        reconnect_delay = 5

        while True:
            started_at = time.time()
            opened = False

            def on_open(ws):
                nonlocal opened, reconnect_delay
                opened = True
                reconnect_delay = 5
                with lock:
                    collector_state[conn_key] = True
                    collector_state[err_key] = None
                    collector_state["error"] = None
                    update_status()

            def on_message(ws, message):
                handle_message(ws, message)

            def on_error(ws, error):
                with lock:
                    collector_state[err_key] = str(error)
                    collector_state["error"] = str(error)

            def on_close(ws, code, msg):
                pass

            try:
                ws = websocket.WebSocketApp(
                    url,
                    on_open=on_open,
                    on_message=on_message,
                    on_error=on_error,
                    on_close=on_close,
                )
                ws.run_forever(ping_interval=0, ping_timeout=None)
            except Exception as exc:
                with lock:
                    collector_state[err_key] = str(exc)
            finally:
                with lock:
                    collector_state[conn_key] = False
                    if is_depth and opened:
                        for sym in SYMBOLS:
                            st = orderbook[sym]
                            st["initialized"] = False
                            st["resyncing"] = False
                            st["last_update_id"] = None
                            st["bids"].clear()
                            st["asks"].clear()
                            st["liquidity_lots"]["bid"].clear()
                            st["liquidity_lots"]["ask"].clear()
                            st["buffer"].clear()
                    update_status()

            if time.time() - started_at >= 60:
                reconnect_delay = 5
            time.sleep(reconnect_delay)
            reconnect_delay = min(reconnect_delay * 2, 60)

    t1 = threading.Thread(target=run_multiplexed_stream, args=("trade", trade_url), name="binance-trades", daemon=True)
    t2 = threading.Thread(target=run_multiplexed_stream, args=("depth", depth_url), name="binance-depths", daemon=True)
    t1.start()
    t2.start()
    t1.join()
    t2.join()


def ensure_collector_started():
    global collector_thread
    if collector_thread is not None and collector_thread.is_alive():
        return
    with collector_start_lock:
        if collector_thread is not None and collector_thread.is_alive():
            return
        ensure_turso_worker_started()
        collector_thread = threading.Thread(
            target=collector_loop,
            name="binance-trade-collector",
            daemon=True,
        )
        collector_thread.start()


@app.before_request
def before_request():
    ensure_collector_started()

# ============================================================
# ROUTES & UI
# ============================================================

@app.route("/")
def home():
    return """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no">
<title>SA Institutional Footprint & Pulling-Delta Engine V5.0</title>
<style>
    * { box-sizing: border-box; margin: 0; padding: 0; user-select: none; -webkit-user-select: none; }
    html, body {
        background: #090b10;
        color: #d1d4dc;
        font-family: monospace;
        overflow: hidden;
        height: 100vh;
        height: 100dvh;
        width: 100vw;
        display: flex;
        flex-direction: column;
        touch-action: none;
    }
    .topbar {
        min-height: 38px;
        background: #11151f;
        border-bottom: 1px solid #232838;
        display: flex;
        align-items: center;
        padding: 2px 8px;
        gap: 6px;
        font-size: 11px;
        overflow-x: auto;
        white-space: nowrap;
        scrollbar-width: thin;
        scrollbar-color: #2962ff #11151f;
        flex-shrink: 0;
    }
    .topbar::-webkit-scrollbar {
        height: 4px;
    }
    .topbar::-webkit-scrollbar-thumb {
        background: #2962ff;
        border-radius: 2px;
    }

    .decision-bar {
        min-height: 34px;
        background: #0d111a;
        border-bottom: 1px solid #232838;
        display: flex;
        align-items: center;
        padding: 2px 8px;
        gap: 6px;
        font-size: 11px;
        overflow-x: auto;
        white-space: nowrap;
        scrollbar-width: thin;
        scrollbar-color: #2962ff #0d111a;
        flex-shrink: 0;
    }
    .decision-bar::-webkit-scrollbar {
        height: 4px;
    }
    .decision-bar::-webkit-scrollbar-thumb {
        background: #2962ff;
        border-radius: 2px;
    }
    .bench-box {
        background: #151a28;
        border: 1px solid #2a324b;
        padding: 3px 7px;
        border-radius: 3px;
        display: flex;
        gap: 6px;
        align-items: center;
        flex-shrink: 0;
    }
    .smt-box {
        background: #181d2c;
        border: 1px solid #37474f;
        padding: 3px 8px;
        border-radius: 3px;
        font-weight: bold;
        font-size: 10px;
        flex-shrink: 0;
        color: #90a4ae;
    }
    .verdict-box {
        flex: 1;
        min-width: 260px;
        padding: 3px 8px;
        border-radius: 4px;
        font-size: 10.5px;
        display: flex;
        flex-direction: column;
        justify-content: center;
        gap: 2px;
        border: 1px solid #2c3347;
        background: #141926;
    }
    .verdict-top {
        display: flex;
        justify-content: space-between;
        align-items: center;
        font-weight: bold;
        font-size: 11px;
    }
    .verdict-sub {
        font-size: 9.5px;
        color: #cfd8dc;
        white-space: nowrap;
        overflow: hidden;
        text-overflow: ellipsis;
    }
    .brand { color: #00e676; font-weight: bold; margin-right: 4px; font-size: 12px; flex-shrink: 0; }
    select, .btn {
        background: #1a2030;
        color: #d1d4dc;
        border: 1px solid #2c3347;
        padding: 3px 6px;
        border-radius: 3px;
        font-family: monospace;
        font-size: 11px;
        cursor: pointer;
        flex-shrink: 0;
    }
    .btn.active { background: #2962ff; color: #fff; border-color: #2962ff; font-weight: bold; }
    .sep { width: 1px; height: 20px; background: #232838; margin: 0 2px; flex-shrink: 0; }
    .legend { display: flex; gap: 4px; font-size: 10px; align-items: center; margin-left: 2px; flex-shrink: 0; }
    .lg { padding: 1px 4px; border-radius: 2px; font-weight: bold; }
    .status-right { margin-left: auto; display: flex; gap: 8px; align-items: center; flex-shrink: 0; }
    .live-px { font-size: 13px; font-weight: bold; padding: 2px 6px; background: #1a2030; border-radius: 3px; }
    #wrap { flex: 1; position: relative; overflow: hidden; background: #090b10; touch-action: none; }
    canvas { display: block; touch-action: none; }

    .dom-slide-tab {
        position: absolute;
        top: 50%;
        transform: translateY(-50%);
        z-index: 30;
        background: rgba(41, 98, 255, 0.95);
        color: #ffffff;
        border: 1px solid #00e5ff;
        border-right: none;
        padding: 14px 6px;
        border-radius: 6px 0 0 6px;
        cursor: pointer;
        font-weight: bold;
        font-size: 11px;
        writing-mode: vertical-rl;
        text-orientation: mixed;
        letter-spacing: 1px;
        box-shadow: -3px 0 12px rgba(0,0,0,0.7);
        transition: right 0.15s ease, background 0.15s ease;
    }
    .dom-slide-tab:hover { background: #00e5ff; color: #000; }

    #ruleModal, #aiModal {
        display: none;
        position: fixed;
        inset: 36px 4%;
        background: rgba(11, 14, 22, 0.98);
        border: 1px solid #00e5ff;
        border-radius: 6px;
        z-index: 999;
        padding: 14px 18px;
        overflow-y: auto;
        color: #d1d4dc;
        font-size: 12px;
        line-height: 1.55;
        box-shadow: 0 12px 40px #000;
        touch-action: pan-y;
    }
    .rule-sec {
        background: #131824;
        border: 1px solid #232a3d;
        border-radius: 4px;
        padding: 10px 12px;
        margin-bottom: 10px;
    }
    .rule-sec h4 { color: #ffeb3b; margin-bottom: 6px; font-size: 13px; border-bottom: 1px solid #232a3d; padding-bottom: 4px; }
    .ai-input {
        width: 100%;
        background: #131824;
        color: #fff;
        border: 1px solid #2962ff;
        border-radius: 4px;
        padding: 8px 10px;
        font-family: monospace;
        font-size: 12px;
        margin-top: 6px;
    }
    .ai-reply-box {
        background: #0f1420;
        border: 1px solid #263147;
        border-radius: 4px;
        padding: 12px;
        margin-top: 10px;
        white-space: pre-wrap;
        font-size: 12px;
        line-height: 1.6;
        color: #e0f7fa;
        max-height: 52vh;
        overflow-y: auto;
    }
</style>
</head>
<body>

<div class="topbar">
    <span class="brand">⚡ SA V5.0</span>
    <select id="symSel" onchange="setSym(this.value)">
        <option value="BTCUSDT" selected>BTCUSDT</option>
        <option value="ETHUSDT">ETHUSDT</option>
        <option value="SOLUSDT">SOLUSDT</option>
        <option value="XRPUSDT">XRPUSDT</option>
    </select>
    <div class="sep"></div>
    <div id="tfBtns" style="display:flex;gap:3px;">
        <button class="btn active" onclick="setTF('1m',this)">1m</button>
        <button class="btn" onclick="setTF('3m',this)">3m</button>
        <button class="btn" onclick="setTF('5m',this)">5m</button>
        <button class="btn" onclick="setTF('15m',this)">15m</button>
        <button class="btn" onclick="setTF('30m',this)">30m</button>
        <button class="btn" onclick="setTF('1h',this)">1h</button>
        <button class="btn" onclick="setTF('4h',this)">4h</button>
        <button class="btn" onclick="setTF('1d',this)">1d</button>
    </div>
    <div class="sep"></div>
    <select id="tickMultSel" onchange="setTickMult(this.value)">
        <option value="auto" selected>Scale: Auto</option>
        <option value="1">1x (Raw)</option>
        <option value="2">2x</option>
        <option value="5">5x</option>
        <option value="10">10x</option>
        <option value="25">25x</option>
        <option value="50">50x</option>
    </select>
    <button class="btn" onclick="scaleY(0.82)">Y+</button>
    <button class="btn" onclick="scaleY(1.22)">Y-</button>
    <button class="btn" onclick="scaleX(1.18)">X+</button>
    <button class="btn" onclick="scaleX(0.84)">X-</button>
    <button class="btn" onclick="resetCamera()">Fit</button>
    <button class="btn active" id="viewModeBtn" onclick="toggleViewMode()">👁️ PRO</button>
    <button class="btn" id="domToggleBtn" onclick="toggleDOM()">DOM</button>
    <button class="btn" onclick="toggleMeter()" style="background:#7c4dff;color:#fff;border-color:#b388ff;font-weight:bold;">📊 METER</button>
    <button class="btn" onclick="toggleAI()" style="background:#00bfa5;color:#000;border-color:#00e5ff;font-weight:bold;">🤖 AI MENTOR</button>
    <button class="btn" onclick="toggleRules()" style="border-color:#ffeb3b;color:#ffeb3b;font-weight:bold;">📖 RULES</button>
    <button class="btn" id="paperTopBtn" onclick="togglePaperModal()" style="background:#ff6d00;color:#000;border-color:#ffd740;font-weight:bold;">🎯 AUTO $10</button>

    <div class="legend">
        <span class="lg" style="background:#00e5ff;color:#000;">H-B.ABS(&ge;65%)</span>
        <span class="lg" style="background:#ff9100;color:#000;">H-A.ABS(&ge;65%)</span>
        <span class="lg" style="background:#ff4081;color:#fff;">10s PULL &amp; P&Delta;</span>
        <span class="lg" style="background:#ffeb3b;color:#000;">🟡 nPOC</span>
        <span class="lg" style="background:#e040fb;color:#fff;">🎯 UB-HI/LO</span>
        <span class="lg" style="background:#ffd700;color:#000;">◆ WHALE</span>
    </div>

    <div class="status-right">
        <span id="connTxt" style="color:#00e676;">● SYNC</span>
        <span id="pxBox" class="live-px">--</span>
    </div>
</div>

<div class="decision-bar">
    <div class="bench-box">
        <span style="color:#8b93a7;">FLOW:</span>
        <span id="bmAvg" style="color:#d1d4dc;">Avg:--</span>
        <span style="color:#444;">|</span>
        <span id="bmHeavy" style="color:#ffeb3b;font-weight:bold;">HeavyL:>--</span>
        <span style="color:#444;">|</span>
        <span id="bmLvl" style="color:#00e5ff;font-weight:bold;">RestBook:--</span>
        <span style="color:#444;">|</span>
        <span id="bmCur" style="color:#fff;">Cur:--</span>
    </div>
    <div class="smt-box" id="smtRadarBox">🔗 SMT: SCANNING...</div>
    <div class="verdict-box" id="masterVerdictBox" style="flex-direction:row;justify-content:space-between;align-items:center;">
        <span id="masterVerdictTitle" style="font-weight:bold;font-size:11px;">SCANNING MACRO MARKET ORDERFLOW...</span>
        <span id="masterVerdictReasons" style="font-size:10px;font-weight:normal;color:#cfd8dc;">Calculating structure...</span>
    </div>
</div>

<div id="wrap">
    <canvas id="cv"></canvas>
    <div id="domSlideTab" class="dom-slide-tab" onclick="toggleDOM()">❮ BATTLE DOM</div>
</div>

<div id="aiModal">
    <div style="display:flex; justify-content:space-between; align-items:center; border-bottom:1px solid #00e5ff; padding-bottom:8px; margin-bottom:10px; position:sticky; top:0; background:#0b0e16; z-index:2;">
        <b style="color:#00e5ff; font-size:14px;">🤖 SA ENGINE + TRADINGVIEW AI MENTOR (Powered by GEMINI_API_KEY)</b>
        <button class="btn" onclick="toggleAI()" style="background:#ff1744;color:#fff;border:none;padding:4px 12px;font-weight:bold;">✕ CLOSE</button>
    </div>
    <div style="font-size:11px; color:#b0bec5; margin-bottom:8px;">
        Apna TradingView structure/observation niche likh (jaise: <i>"5m low sweep hua hai aur FVG par hai, Long lu ya wait karu?"</i>) ya seedha Quick Button daba. AI tere SA Engine ke live DOM, Absorption, 10s Pulls, UB/nPOC aur Whale data ko tere view se mila kar sachcha jawab dega:
    </div>
    <div style="display:flex; gap:6px; flex-wrap:wrap; margin-bottom:6px;">
        <button class="btn" style="background:#1e88e5;color:#fff;border:none;padding:5px 10px;" onclick="askAI('Abhi live SA Engine data ke hisab se Scalp aur Intraday dono ka exact trade decision bata: Buying Ready hai, Selling Ready hai, ya Wait karna hai aur kyu?')">🔍 Verify Live Setup (Scalp + Intraday)</button>
        <button class="btn" style="background:#00897b;color:#fff;border:none;padding:5px 10px;" onclick="askAI('Main LONG trade dekh raha hu. Check karke bata ki humara DOM, Bid Absorption, 10s Pulling Delta aur UB/nPOC magnets Long ko support kar rahe hain ya koi trap hai?')">🟢 Check LONG Alignment</button>
        <button class="btn" style="background:#e53935;color:#fff;border:none;padding:5px 10px;" onclick="askAI('Main SHORT trade dekh raha hu. Check karke bata ki humara DOM, Ask Absorption, 10s Pulling Delta aur UB/nPOC magnets Short ko support kar rahe hain ya koi trap hai?')">🔴 Check SHORT Alignment</button>
    </div>
    <div style="display:flex; gap:6px;">
        <input type="text" id="aiUserPrompt" class="ai-input" placeholder="Yahan likh: TradingView par kya dikh raha hai ya kya poochna hai..." onkeydown="if(event.key==='Enter') askAI()">
        <button class="btn" onclick="askAI()" style="background:#00e676;color:#000;font-weight:bold;padding:6px 16px;margin-top:6px;">🚀 ASK AI</button>
    </div>
    <div class="ai-reply-box" id="aiReplyBox">🤖 Ready! Upar koi button daba ya apna TradingView observation likh kar ASK AI par click kar.</div>
</div>

<div id="ruleModal">
    <div style="display:flex; justify-content:space-between; align-items:center; border-bottom:1px solid #00e5ff; padding-bottom:8px; margin-bottom:12px; position:sticky; top:0; background:#0b0e16; z-index:2;">
        <b style="color:#00e5ff; font-size:14px;">⚡ SA INSTITUTIONAL FOOTPRINT & ORDERFLOW ENGINE V5 — MASTER RULEBOOK</b>
        <button class="btn" onclick="toggleRules()" style="background:#ff1744;color:#fff;border:none;padding:4px 12px;font-weight:bold;">✕ CLOSE</button>
    </div>

    <div class="rule-sec" style="border-color:#00e5ff;">
        <h4 style="color:#00e5ff;">1. 10-SECOND AGED RESTING WALL PULLS (H-BP / bp / H-AP / ap) &amp; LIFO NOISE FILTER</h4>
        • <b>10-Second Heatmap Wall Gate:</b> Orderbook me har millisecond aane-jane wale HFT flickers ko LIFO queue se filter kiya jata hai. Sirf wahi limit orders jo <b>&ge; 10 Seconds</b> se orderbook me resting the aur price ke <b>18 steps</b> ke andar aane par pull huye, unhe genuine <b>Institutional Wall Pull</b> maana jata hai.<br>
        • <b>Price Level Tags:</b> Left (Bid) side par <code>H-BP:Qty(%)</code> (Heavy Bid Pull) ya <code>bp:Qty(%)</code> aur Right (Ask) side par <code>H-AP:Qty(%)</code> (Heavy Ask Pull) ya <code>ap:Qty(%)</code> dikhta hai.
    </div>

    <div class="rule-sec">
        <h4>2. CANDLE PILL PULLING DELTA (P&Delta; = Total 10s+ Bid Pulled &minus; Total 10s+ Ask Pulled)</h4>
        • <b>Formula (Candle Pill Line 3):</b> <code>P&Delta; = Total Bid Pull (BP) &minus; Total Ask Pull (AP)</code>.<br>
        • <b>Negative P&Delta; (AP &gt; BP) + Bullish Close:</b> Upar Ask side par bada fake sell wall dikha kar retail ko short karwaya gaya, aur price aate hi wall pull kar li gayi! (<b>⚠️ FAKE-ASK WALL PULLED &rarr; BULL BREAK!</b>).<br>
        • <b>Positive P&Delta; (BP &gt; AP) + Bearish Close:</b> Niche Bid side par fake support wall dikha kar retail ko long karwaya gaya, aur price aate hi wall kheench li gayi! (<b>⚠️ FAKE-BID WALL PULLED &rarr; BEAR DUMP!</b>).
    </div>

    <div class="rule-sec" style="border-color:#ff9100;">
        <h4 style="color:#ff9100;">3. TRUE BEHAVIORAL WHALE DETECTION (ABSORPTION STICKING vs DISPLACEMENT SWEEP)</h4>
        • <b>Passive Whale (Absorption Sticking):</b> Jab ek 3-tick cluster me retail lagatar market orders maare, volume badhe, par price wahi stuck rahe aur wall pull na ho = <b>Passive Whale Absorbing Retail!</b><br>
        • <b>Aggressive Whale (Displacement Sweep):</b> Jab bade chunks ke sath price tezi se steps sweep kare = <b>Aggressive Whale Displacement!</b>
    </div>
</div>

<div id="meterModal" style="display:none; position:fixed; inset:36px 4%; background:rgba(11,14,22,0.98); border:1px solid #b388ff; border-radius:6px; z-index:999; padding:14px 18px; overflow-y:auto; color:#d1d4dc; font-size:12px; line-height:1.55; box-shadow:0 12px 40px #000; touch-action:pan-y;">
    <div style="display:flex; justify-content:space-between; align-items:center; border-bottom:1px solid #b388ff; padding-bottom:8px; margin-bottom:12px; position:sticky; top:0; background:#0b0e16; z-index:2;">
        <b style="color:#b388ff; font-size:14px;">📊 LIVE PARAMETER CONFLUENCE METER (🟢 BULLISH vs 🔴 BEARISH vs ⚪ NEUTRAL)</b>
        <button class="btn" onclick="toggleMeter()" style="background:#ff1744;color:#fff;border:none;padding:4px 12px;font-weight:bold;">✕ CLOSE</button>
    </div>
    <div id="meterModalBody" style="display:grid; grid-template-columns:repeat(auto-fit, minmax(300px, 1fr)); gap:12px;">
        Loading live parameters...
    </div>
</div>

<!-- ==================== START DEMO BOT UI ==================== -->
<div id="paperModal" style="display:none; position:fixed; inset:36px 3%; background:rgba(11,14,22,0.98); border:1px solid #ffd740; border-radius:6px; z-index:999; padding:14px 18px; overflow-y:auto; color:#d1d4dc; font-size:12px; line-height:1.5; box-shadow:0 12px 40px #000; touch-action:pan-y;">
    <div style="display:flex; justify-content:space-between; align-items:center; flex-wrap:wrap; gap:8px; border-bottom:1px solid #ffd740; padding-bottom:8px; margin-bottom:12px; position:sticky; top:0; background:#0b0e16; z-index:2;">
        <b style="color:#ffd740; font-size:14px;">🎯 24/7 SERVER-SIDE $10 AUTO-TESTER (POST-ENTRY TICK VERIFIED)</b>
        <div style="display:flex; gap:6px; align-items:center; flex-wrap:wrap;">
            <select id="pBotSym" onchange="updatePaperConfig()" style="background:#1a2030;color:#00e5ff;border:1px solid #00e5ff;">
                <option value="BTCUSDT">Test: BTCUSDT</option>
                <option value="ETHUSDT">Test: ETHUSDT</option>
                <option value="SOLUSDT">Test: SOLUSDT</option>
                <option value="XRPUSDT">Test: XRPUSDT</option>
            </select>
            <select id="pBotTf" onchange="updatePaperConfig()" style="background:#1a2030;color:#00e5ff;border:1px solid #00e5ff;">
                <option value="1m">Scalp TF: 1m</option>
                <option value="3m">Scalp TF: 3m</option>
                <option value="5m" selected>Scalp TF: 5m</option>
                <option value="15m">Scalp TF: 15m</option>
            </select>
            <select id="pBotLev" onchange="updatePaperConfig()" style="background:#1a2030;color:#ffd740;border:1px solid #ffd740;">
                <option value="5">Lev: 5x ($50)</option>
                <option value="10" selected>Lev: 10x ($100)</option>
                <option value="20">Lev: 20x ($200)</option>
            </select>
            <button class="btn" id="pBotToggleBtn" onclick="togglePaperBotPower()" style="background:#00e676;color:#000;font-weight:bold;">🟢 BOT: ON</button>
            <button class="btn" onclick="resetPaperBot()" style="background:#37474f;color:#fff;">🔄 RESET $10</button>
            <button class="btn" onclick="togglePaperModal()" style="background:#ff1744;color:#fff;border:none;padding:4px 12px;font-weight:bold;">✕ CLOSE</button>
        </div>
    </div>
    <div id="paperModalBody" style="display:grid; grid-template-columns:repeat(auto-fit, minmax(320px, 1fr)); gap:12px;">
        Loading 24/7 Server Bot Status...
    </div>
</div>

<script>
function togglePaperModal() {
    const m = document.getElementById('paperModal');
    m.style.display = (m.style.display === 'block') ? 'none' : 'block';
    if (m.style.display === 'block') fetchPaperStatus();
}

async function fetchPaperStatus() {
    try {
        const res = await fetch('/api/paper-status');
        const d = await res.json();
        if (d.status !== 'ok') return;
        const st = d.data;

        const topBtn = document.getElementById('paperTopBtn');
        if (topBtn) {
            const sAct = st.scalp.active ? '1' : '0';
            const iAct = st.intraday.active ? '1' : '0';
            const pwr = st.enabled ? '🟢' : '⏸️';
            topBtn.innerText = `${pwr} $10 BOT: $${st.scalp.balance.toFixed(1)}/$${st.intraday.balance.toFixed(1)} [${sAct}|${iAct}]`;
        }

        const pwrBtn = document.getElementById('pBotToggleBtn');
        if (pwrBtn) {
            pwrBtn.innerText = st.enabled ? '🟢 BOT: ON (24/7)' : '🔴 BOT: OFF';
            pwrBtn.style.background = st.enabled ? '#00e676' : '#ff5252';
            pwrBtn.style.color = st.enabled ? '#000' : '#fff';
        }
        document.getElementById('pBotSym').value = st.symbol || 'BTCUSDT';
        document.getElementById('pBotTf').value = st.tf || '5m';
        document.getElementById('pBotLev').value = String(st.leverage || 10);

        const m = document.getElementById('paperModal');
        if (m && m.style.display === 'block') {
            renderServerPaperUI(st);
        }
    } catch (e) {}
}

async function togglePaperBotPower() {
    await fetch('/api/paper-action', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ action: 'toggle' })
    });
    fetchPaperStatus();
}

async function updatePaperConfig() {
    const sym = document.getElementById('pBotSym').value;
    const tfVal = document.getElementById('pBotTf').value;
    const lev = parseInt(document.getElementById('pBotLev').value, 10) || 10;
    await fetch('/api/paper-action', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ action: 'config', symbol: sym, tf: tfVal, leverage: lev })
    });
    fetchPaperStatus();
}

async function resetPaperBot() {
    await fetch('/api/paper-action', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ action: 'reset' })
    });
    fetchPaperStatus();
}

async function closeServerPaperTrade(mode) {
    await fetch('/api/paper-action', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ action: 'close', mode })
    });
    fetchPaperStatus();
}

function renderServerPaperUI(st) {
    const livePx = st.live_price || 0;
    const buildBook = (title, mode, book, col) => {
        const tot = book.wins + book.losses;
        const wr = tot > 0 ? Math.round((book.wins / tot) * 100) : 0;
        const diff = book.balance - 10.0;
        const pct = ((diff / 10.0) * 100).toFixed(1);

        let actBox = `<div style="background:#0f1420;border:1px dashed #37474f;padding:8px;border-radius:4px;color:#90a4ae;margin:8px 0;">⏳ Scanning Live Orderflow on Server... (No Open Trade)</div>`;
        if (book.active) {
            const p = book.active;
            const cur = livePx || p.entry;
            const notional = p.margin * p.leverage;
            const move = p.side === 'LONG' ? ((cur - p.entry) / p.entry) : ((p.entry - cur) / p.entry);
            const uPnl = (notional * move) - (notional * 0.0012);
            const uCol = uPnl >= 0 ? '#00e676' : '#ff5252';
            const sCol = p.side === 'LONG' ? '#00e676' : '#ff5252';

            actBox = `
                <div style="background:#0e1726;border:1px solid ${sCol};padding:8px;border-radius:4px;margin:8px 0;">
                    <div style="display:flex;justify-content:space-between;align-items:center;">
                        <b style="color:${sCol};">🔥 ACTIVE ${p.side} (${p.symbol} ${p.tf}) @ ${p.leverage}x</b>
                        <button class="btn" onclick="closeServerPaperTrade('${mode}')" style="background:#ff1744;color:#fff;font-size:10px;padding:2px 6px;">Close Now</button>
                    </div>
                    <div style="margin-top:4px;font-size:11px;">
                        Entry: <b>${p.entry}</b> | 🎯 TP: <b style="color:#00e676;">${p.tp}</b> | 🛑 SL: <b style="color:#ff5252;">${p.sl}</b>
                    </div>
                    <div style="margin-top:4px;display:flex;justify-content:space-between;font-size:11px;">
                        <span>Signal: <code style="color:#ffeb3b;">${p.reason}</code></span>
                        <b style="color:${uCol};font-size:12px;">Live PnL: ${uPnl >= 0 ? '+' : ''}$${uPnl.toFixed(3)}</b>
                    </div>
                </div>`;
        }

        const logs = (book.history || []).length === 0
            ? `<div style="color:#607d8b;font-size:11px;">No closed trades yet.</div>`
            : book.history.map(h => {
                const c = h.pnl >= 0 ? '#00e676' : '#ff5252';
                return `<div style="display:flex;justify-content:space-between;border-bottom:1px solid #1c2436;padding:4px 0;font-size:10.5px;">
                    <span>[${h.time}] <b>${h.side}</b> ${h.symbol}(${h.tf}) ${h.entry}➔${h.exit}</span>
                    <span>${h.outcome} <b style="color:${c};">${h.pnl >= 0 ? '+' : ''}$${Number(h.pnl).toFixed(3)}</b></span>
                </div>`;
            }).join('');

        return `
            <div class="rule-sec" style="border-color:${col};">
                <h4 style="color:${col};display:flex;justify-content:space-between;">
                    <span>${title}</span>
                    <span>Wallet: <b style="color:#fff;">$${book.balance.toFixed(2)}</b> (${diff >= 0 ? '+' : ''}${pct}%)</span>
                </h4>
                <div style="display:flex;gap:12px;background:#0d111a;padding:6px 10px;border-radius:4px;font-size:11px;">
                    <span>Trades: <b>${tot}</b></span>
                    <span style="color:#00e676;">TP Hits: <b>${book.wins}</b></span>
                    <span style="color:#ff5252;">SL Hits: <b>${book.losses}</b></span>
                    <span style="color:#ffd740;">Win Rate: <b>${wr}%</b></span>
                </div>
                ${actBox}
                <div style="margin-top:6px;font-weight:bold;color:#90a4ae;font-size:11px;">📜 24H Trade Log (0.12% Fee+Slippage Deducted):</div>
                <div style="max-height:210px;overflow-y:auto;margin-top:4px;">${logs}</div>
            </div>`;
    };

    document.getElementById('paperModalBody').innerHTML =
        buildBook('⚡ SCALP 24/7 BOT ($10 CAPITAL)', 'scalp', st.scalp, '#00e5ff') +
        buildBook('🌊 INTRADAY 24/7 BOT ($10 CAPITAL)', 'intraday', st.intraday, '#e040fb');
}

setInterval(() => {
    if (document.hidden) return;
    const m = document.getElementById('paperModal');
    if (m && m.style.display === 'block') fetchPaperStatus();
}, 5000);
setTimeout(fetchPaperStatus, 1500);

</script>
<!-- ==================== END DEMO BOT UI ==================== -->

<script>
const wrap = document.getElementById('wrap');
const cv = document.getElementById('cv');
const ctx = cv.getContext('2d', { alpha: false });
const slideTab = document.getElementById('domSlideTab');
const ruleModal = document.getElementById('ruleModal');
const aiModal = document.getElementById('aiModal');
let latestEngineSummary = {};

function toggleRules() {
    ruleModal.style.display = (ruleModal.style.display === 'block') ? 'none' : 'block';
}
function toggleAI() {
    aiModal.style.display = (aiModal.style.display === 'block') ? 'none' : 'block';
}
const meterModal = document.getElementById('meterModal');
function toggleMeter() {
    meterModal.style.display = (meterModal.style.display === 'block') ? 'none' : 'block';
    needsRender = true;
}

async function askAI(customPrompt) {
    const inp = document.getElementById('aiUserPrompt');
    const q = customPrompt || inp.value.trim();
    if (!q) return;
    const box = document.getElementById('aiReplyBox');
    box.innerText = '🧠 AI Mentor analyzing live SA Engine DOM, Absorptions, 10s Pulls & Magnets...';
    try {
        const res = await fetch('/api/ai-mentor', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                symbol,
                timeframe: tf,
                question: q,
                engine_data: latestEngineSummary
            })
        });
        const data = await res.json();
        if (data.status === 'ok') {
            box.innerText = data.reply;
        } else {
            box.innerText = '⚠️ AI Error: ' + (data.message || 'Check GEMINI_API_KEY in Render Environment.');
        }
    } catch (e) {
        box.innerText = '⚠️ Network error connecting to AI Mentor: ' + e.message;
    }
}

const STEP_MAP = { BTCUSDT: 1.0, ETHUSDT: 0.1, SOLUSDT: 0.01, XRPUSDT: 0.0001 };

let symbol = document.getElementById('symSel').value || 'BTCUSDT';
let tf = '1m';
let tickMultMode = document.getElementById('tickMultSel').value || 'auto';
let proViewMode = true;
let candles = [];
let peerCandles = [];
let smtState = { bias: 0, text: 'SYNCED' };
let obData = { bids: [], asks: [] };
let rawStep = STEP_MAP[symbol] || 1.0;

let isMobile = window.innerWidth < 768;
let showDOM = false;
let candleW = isMobile ? 185 : 265;
let panX = 0;
let minP = null;
let maxP = null;
let domScrollRows = 0;

let dragMode = null;
let lastX = 0, lastY = 0;
let curX = -1, curY = -1;
let pinchDist = null;
let needsRender = true;

const TIME_H = 26;
const PILL_SAFE_H = 78;

function toggleViewMode() {
    proViewMode = !proViewMode;
    const btn = document.getElementById('viewModeBtn');
    btn.innerText = proViewMode ? '👁️ PRO' : '👁️ CLEAN';
    btn.classList.toggle('active', proViewMode);
    needsRender = true;
}

function getDomWidth() {
    if (!showDOM) return 0;
    return cv.width < 768 ? Math.min(310, Math.floor(cv.width * 0.82)) : 460;
}

function updateSlideTabUI() {
    const domW = getDomWidth();
    const btn = document.getElementById('domToggleBtn');
    if (showDOM) {
        slideTab.style.right = domW + 'px';
        slideTab.innerText = '❯ HIDE DOM';
        btn.innerText = 'DOM: ON';
        btn.classList.add('active');
    } else {
        slideTab.style.right = '0px';
        slideTab.innerText = '❮ BATTLE DOM';
        btn.innerText = 'DOM: OFF';
        btn.classList.remove('active');
    }
}

function getLayout() {
    isMobile = cv.width < 768;
    const yAxisW = isMobile ? 60 : 74;
    const domW = getDomWidth();
    const chartW = Math.max(100, cv.width - domW - yAxisW);
    const chartH = cv.height - TIME_H - PILL_SAFE_H;
    return { yAxisW, domW, chartW, chartH };
}

function resize() {
    cv.width = wrap.clientWidth;
    cv.height = wrap.clientHeight;
    updateSlideTabUI();
    needsRender = true;
}
window.addEventListener('resize', resize);
resize();

function toggleDOM() {
    showDOM = !showDOM;
    updateSlideTabUI();
    needsRender = true;
}

function setSym(s) {
    symbol = s;
    rawStep = STEP_MAP[s] || 1.0;
    resetCamera();
    fetchData();
    fetchSmtPeer();
}
function setTF(t, b) {
    tf = t;
    document.querySelectorAll('#tfBtns .btn').forEach(el => el.classList.remove('active'));
    b.classList.add('active');
    resetCamera();
    fetchData();
    fetchSmtPeer();
}
function setTickMult(v) {
    tickMultMode = v;
    needsRender = true;
}
function scaleY(factor) {
    if (minP !== null && maxP !== null) {
        const mid = (maxP + minP) * 0.5;
        const half = Math.max((maxP - minP) * 0.5 * factor, rawStep * 2);
        minP = mid - half;
        maxP = mid + half;
        needsRender = true;
    }
}
function scaleX(factor) {
    candleW = Math.max(40, Math.min(540, candleW * factor));
    needsRender = true;
}
function resetCamera() {
    panX = 0;
    minP = null;
    maxP = null;
    domScrollRows = 0;
    candleW = window.innerWidth < 768 ? 185 : 265;
    needsRender = true;
}

function detectZone(x, y) {
    const { chartW, yAxisW } = getLayout();
    if (x < chartW) {
        if (y >= cv.height - TIME_H - 6) return 'xaxis';
        return 'chart';
    }
    if (x <= chartW + yAxisW) return 'yaxis';
    return 'dom';
}

function applyDrag(dx, dy) {
    const { chartH } = getLayout();
    if (dragMode === 'chart') {
        panX += dx;
        if (minP !== null && maxP !== null) {
            const shift = (dy / chartH) * (maxP - minP);
            minP += shift;
            maxP += shift;
        }
    } else if (dragMode === 'xaxis') {
        candleW = Math.max(40, Math.min(540, candleW * (1 - dx * 0.006)));
    } else if (dragMode === 'yaxis') {
        if (minP !== null && maxP !== null) {
            const mid = (maxP + minP) * 0.5;
            let half = (maxP - minP) * 0.5 * (1 + dy * 0.006);
            if (half >= rawStep * 2) { minP = mid - half; maxP = mid + half; }
        }
    } else if (dragMode === 'dom') {
        domScrollRows += (dy / 16);
    }
    needsRender = true;
}

cv.addEventListener('mousedown', e => {
    const r = cv.getBoundingClientRect();
    lastX = e.clientX - r.left;
    lastY = e.clientY - r.top;
    dragMode = detectZone(lastX, lastY);
});

window.addEventListener('mousemove', e => {
    const r = cv.getBoundingClientRect();
    curX = e.clientX - r.left;
    curY = e.clientY - r.top;
    const z = detectZone(curX, curY);
    cv.style.cursor = (z === 'xaxis') ? 'ew-resize' : ((z === 'yaxis') ? 'ns-resize' : ((z === 'dom') ? 'ns-resize' : 'crosshair'));

    if (dragMode) {
        applyDrag(curX - lastX, curY - lastY);
        lastX = curX;
        lastY = curY;
    } else {
        needsRender = true;
    }
});

window.addEventListener('mouseup', () => { dragMode = null; });

cv.addEventListener('dblclick', e => {
    const r = cv.getBoundingClientRect();
    if (detectZone(e.clientX - r.left, e.clientY - r.top) === 'dom') {
        domScrollRows = 0;
        needsRender = true;
    } else {
        resetCamera();
    }
});

cv.addEventListener('wheel', e => {
    e.preventDefault();
    const { chartH } = getLayout();
    const zone = detectZone(curX, curY);
    const zoomIn = e.deltaY < 0;

    if (zone === 'dom') {
        domScrollRows += (e.deltaY > 0 ? -2 : 2);
    } else if (zone === 'yaxis') {
        scaleY(zoomIn ? 0.84 : 1.19);
    } else if (zone === 'xaxis') {
        scaleX(zoomIn ? 1.15 : 0.87);
    } else {
        const zx = zoomIn ? 1.15 : 0.87;
        candleW = Math.max(40, Math.min(540, candleW * zx));
        if (!e.shiftKey && minP !== null && maxP !== null) {
            const zy = zoomIn ? 0.85 : 1.18;
            const span = maxP - minP;
            const newSpan = Math.max(rawStep * 6, span * zy);
            const mouseRatio = Math.max(0.1, Math.min(0.9, (chartH - curY) / chartH));
            const mousePrice = minP + span * mouseRatio;
            minP = mousePrice - newSpan * mouseRatio;
            maxP = minP + newSpan;
        }
    }
    needsRender = true;
}, { passive: false });

cv.addEventListener('touchstart', e => {
    e.preventDefault();
    const r = cv.getBoundingClientRect();
    if (e.touches.length === 1) {
        lastX = e.touches[0].clientX - r.left;
        lastY = e.touches[0].clientY - r.top;
        curX = lastX; curY = lastY;
        dragMode = detectZone(lastX, lastY);
        pinchDist = null;
    } else if (e.touches.length === 2) {
        dragMode = null;
        pinchDist = Math.hypot(e.touches[0].clientX - e.touches[1].clientX, e.touches[0].clientY - e.touches[1].clientY);
    }
}, { passive: false });

cv.addEventListener('touchmove', e => {
    e.preventDefault();
    const r = cv.getBoundingClientRect();
    if (e.touches.length === 1 && dragMode) {
        const tx = e.touches[0].clientX - r.left;
        const ty = e.touches[0].clientY - r.top;
        applyDrag(tx - lastX, ty - lastY);
        lastX = tx; lastY = ty; curX = tx; curY = ty;
    } else if (e.touches.length === 2 && pinchDist !== null) {
        const newDist = Math.hypot(e.touches[0].clientX - e.touches[1].clientX, e.touches[0].clientY - e.touches[1].clientY);
        if (newDist > 10) {
            const ratio = newDist / pinchDist;
            candleW = Math.max(40, Math.min(540, candleW * ratio));
            scaleY(1 / ratio);
            pinchDist = newDist;
            needsRender = true;
        }
    }
}, { passive: false });

cv.addEventListener('touchend', () => { dragMode = null; pinchDist = null; });

let fetching = false;
async function fetchData() {
    if (fetching) return;
    fetching = true;
    try {
        const [cRes, obRes] = await Promise.all([
            fetch(`/api/candles?symbol=${symbol}&timeframe=${tf}&limit=18`),
            fetch(`/api/orderbook?symbol=${symbol}&limit=80`)
        ]);
        const cData = await cRes.json();
        const oData = await obRes.json();

        if (cData.status === 'ok' && cData.candles) {
            candles = cData.candles;
            if (candles.length > 0) {
                const last = candles[candles.length - 1];
                const pxEl = document.getElementById('pxBox');
                pxEl.innerText = last.close;
                pxEl.style.color = last.close >= last.open ? '#00e676' : '#ff5252';
            }
        }
        if (oData.status === 'ok') {
            obData = oData;
        }
        document.getElementById('connTxt').innerText = '● SYNC';
        document.getElementById('connTxt').style.color = '#00e676';
        needsRender = true;
    } catch (e) {
        document.getElementById('connTxt').innerText = '● RETRY';
        document.getElementById('connTxt').style.color = '#ff9800';
    } finally {
        fetching = false;
    }
}

async function fetchSmtPeer() {
    const peerSym = (symbol === 'BTCUSDT') ? 'ETHUSDT' : 'BTCUSDT';
    try {
        const res = await fetch(`/api/candles?symbol=${peerSym}&timeframe=${tf}&limit=8`);
        const d = await res.json();
        if (d.status === 'ok' && d.candles) {
            peerCandles = d.candles;
            updateSmtRadar(peerSym);
            needsRender = true;
        }
    } catch (e) {}
}

function updateSmtRadar(peerSym) {
    const el = document.getElementById('smtRadarBox');
    if (!candles || candles.length < 5 || !peerCandles || peerCandles.length < 5) return;

    const curSet = candles.slice(-6);
    const peerSet = peerCandles.slice(-6);
    const cLast = curSet[curSet.length - 1];
    const pLast = peerSet[peerSet.length - 1];

    const cPrevLow = Math.min(...curSet.slice(0, -1).map(x => x.low));
    const cPrevHigh = Math.max(...curSet.slice(0, -1).map(x => x.high));
    const pPrevLow = Math.min(...peerSet.slice(0, -1).map(x => x.low));
    const pPrevHigh = Math.max(...peerSet.slice(0, -1).map(x => x.high));

    const cSweptLow = cLast.low <= cPrevLow;
    const pSweptLow = pLast.low <= pPrevLow;
    const cSweptHigh = cLast.high >= cPrevHigh;
    const pSweptHigh = pLast.high >= pPrevHigh;

    const shortSym = symbol.replace('USDT', '');
    const shortPeer = peerSym.replace('USDT', '');

    if (cSweptLow !== pSweptLow) {
        const stronger = !cSweptLow ? shortSym : shortPeer;
        smtState = { bias: 1, text: `Bull SMT (${stronger} Held Low)` };
        el.style.background = 'rgba(0, 229, 255, 0.20)';
        el.style.borderColor = '#00e5ff';
        el.style.color = '#00e5ff';
        el.innerText = `🚨 BULL SMT: ${stronger} Held Low!`;
    } else if (cSweptHigh !== pSweptHigh) {
        const weaker = !cSweptHigh ? shortSym : shortPeer;
        smtState = { bias: -1, text: `Bear SMT (${weaker} Failed High)` };
        el.style.background = 'rgba(255, 145, 0, 0.22)';
        el.style.borderColor = '#ff9100';
        el.style.color = '#ffab00';
        el.innerText = `🚨 BEAR SMT: ${weaker} Failed High!`;
    } else {
        smtState = { bias: 0, text: 'SYNCED' };
        el.style.background = '#181d2c';
        el.style.borderColor = '#37474f';
        el.style.color = '#90a4ae';
        el.innerText = `🔗 SMT: ${shortSym}/${shortPeer} SYNCED`;
    }
}

function fmt(v) {
    if (!v) return '0';
    const a = Math.abs(v);
    if (a >= 1000) return (v / 1000).toFixed(1) + 'k';
    if (a >= 10) return v.toFixed(0);
    if (a >= 1) return v.toFixed(1);
    return v.toFixed(2);
}

// Zero-CPU Cached groupFootprint for smooth P4 2GB RAM rendering
function groupFootprint(fpRaw, low, high, effStep, closePx, liveRestBench) {
    if (fpRaw && typeof fpRaw === 'object') {
        const cacheKey = `${effStep.toFixed(6)}_${low}_${high}_${closePx}_${Math.round(liveRestBench || 0)}`;
        if (fpRaw._cacheKey === cacheKey && fpRaw._cachedOut) {
            return fpRaw._cachedOut;
        }
        const res = _computeGroupFootprint(fpRaw, low, high, effStep, closePx, liveRestBench);
        fpRaw._cacheKey = cacheKey;
        fpRaw._cachedOut = res;
        return res;
    }
    return _computeGroupFootprint(fpRaw, low, high, effStep, closePx, liveRestBench);
}

function _computeGroupFootprint(fpRaw, low, high, effStep, closePx, liveRestBench) {
    const map = new Map();
    const arr = Array.isArray(fpRaw) ? fpRaw : Object.values(fpRaw || {});
    arr.forEach(l => {
        if (l.price < low - effStep * 2.5 || l.price > high + effStep * 2.5) return;
        const bucket = Math.floor(l.price / effStep + 1e-9) * effStep;
        const key = bucket.toFixed(6);
        let g = map.get(key);
        if (!g) {
            g = {
                price: bucket, buy: 0, sell: 0,
                whale_buy: 0, whale_sell: 0, retail_buy: 0, retail_sell: 0,
                whale_trades: 0, whale_pull_chunks: 0,
                volume: 0, trades: 0, bid_pulled: 0, ask_pulled: 0, pulled_qty: 0
            };
            map.set(key, g);
        }
        g.buy += (l.buy || 0);
        g.sell += (l.sell || 0);
        g.whale_buy += (l.whale_buy || 0);
        g.whale_sell += (l.whale_sell || 0);
        g.retail_buy += (l.retail_buy || 0);
        g.retail_sell += (l.retail_sell || 0);
        g.whale_trades += (l.whale_trades || 0);
        g.whale_pull_chunks += (l.whale_pull_chunks || 0);
        g.volume += (l.volume || 0);
        g.trades += (l.trades || 0);

        let bp = l.bid_pulled || 0;
        let ap = l.ask_pulled || 0;
        if (bp === 0 && ap === 0 && (l.pulled_qty || 0) > 0) {
            if (l.price <= closePx) bp = l.pulled_qty;
            else ap = l.pulled_qty;
        }
        g.bid_pulled += bp;
        g.ask_pulled += ap;
    });

    const restCap = Math.max((liveRestBench || 15) * 4.5, 45);
    const out = [];
    map.forEach(g => {
        if (g.volume <= 0 && g.bid_pulled <= 0 && g.ask_pulled <= 0) return;
        const maxReasonablePull = Math.max(g.volume * 3.5, restCap);
        if (g.bid_pulled > maxReasonablePull) g.bid_pulled = maxReasonablePull;
        if (g.ask_pulled > maxReasonablePull) g.ask_pulled = maxReasonablePull;
        g.pulled_qty = g.bid_pulled + g.ask_pulled;
        out.push(g);
    });
    return out.sort((a, b) => a.price - b.price);
}

function getLiveRestingBenchmark(effStep, lastClose) {
    const bidsArr = obData.bids || [];
    const asksArr = obData.asks || [];
    if (bidsArr.length === 0 && asksArr.length === 0) return 0;

    const bucketMap = new Map();
    const maxDist = effStep * 25;
    bidsArr.forEach(b => {
        if (Math.abs(b.price - lastClose) <= maxDist) {
            const k = Math.floor(b.price / effStep + 1e-9);
            bucketMap.set('B' + k, (bucketMap.get('B' + k) || 0) + b.quantity);
        }
    });
    asksArr.forEach(a => {
        if (Math.abs(a.price - lastClose) <= maxDist) {
            const k = Math.floor(a.price / effStep + 1e-9);
            bucketMap.set('A' + k, (bucketMap.get('A' + k) || 0) + a.quantity);
        }
    });
    const vals = Array.from(bucketMap.values()).filter(v => v > 0);
    if (vals.length === 0) return 0;
    return vals.reduce((a, b) => a + b, 0) / vals.length;
}

function updateMacroMarketDecision(avgFlowVol, effStep, baselineLotSize, nakedPocs, heavyLvlBenchmark, liveRestBench) {
    if (!candles || candles.length < 3) return;

    const sample = candles.slice(-20);
    const last = sample[sample.length - 1];
    const first = sample[0];
    const curRvol = (last.volume || 0) / avgFlowVol;

    document.getElementById('bmAvg').innerText = `Avg:${fmt(avgFlowVol)}`;
    document.getElementById('bmHeavy').innerText = `HeavyL:>${fmt(heavyLvlBenchmark)}`;
    document.getElementById('bmLvl').innerText = `RestBook:${fmt(liveRestBench)}`;
    document.getElementById('bmCur').innerText = `Cur:${fmt(last.volume)}(${curRvol.toFixed(1)}x)`;
    document.getElementById('bmCur').style.color = curRvol >= 1.3 ? '#00e676' : '#d1d4dc';

    let macroHigh = -Infinity, macroLow = Infinity, sumCandleRanges = 0;
    let totalCvd = 0, pocUpSteps = 0, pocDownSteps = 0;
    let macroBidAbsorbs = 0, macroAskAbsorbs = 0;
    let macroBidPulled = 0, macroAskPulled = 0;
    let whaleBuyLots = 0, whaleSellLots = 0;
    let sessWhaleBuyVol = 0, sessWhaleSellVol = 0;
    let sessRetailBuyVol = 0, sessRetailSellVol = 0;

    sample.forEach((c, idx) => {
        if (c.high > macroHigh) macroHigh = c.high;
        if (c.low < macroLow) macroLow = c.low;
        sumCandleRanges += Math.max(c.high - c.low, rawStep);
        totalCvd += (c.delta || 0);
        sessWhaleBuyVol += (c.whale_buy_vol || 0);
        sessWhaleSellVol += (c.whale_sell_vol || 0);
        sessRetailBuyVol += (c.retail_buy_vol || 0);
        sessRetailSellVol += (c.retail_sell_vol || 0);

        if (idx > 0 && c.poc && sample[idx - 1].poc) {
            if (c.poc > sample[idx - 1].poc) pocUpSteps++;
            else if (c.poc < sample[idx - 1].poc) pocDownSteps++;
        }

        const fpGrouped = groupFootprint(c.footprint, c.low, c.high, effStep, c.close, liveRestBench);
        const activeCells = fpGrouped.filter(x => x.volume > 0);
        const cAvgV = activeCells.length > 0 ? (c.volume || 1) / activeCells.length : 1;
        const cHeavyCut = Math.max(cAvgV * 1.25, heavyLvlBenchmark * 0.45);
        const cSpan = Math.max(c.high - c.low, effStep);
        const lowerSwingTop = c.low + cSpan * 0.42;
        const upperSwingBot = c.high - cSpan * 0.42;

        for (let i = 0; i < fpGrouped.length; i++) {
            const l = fpGrouped[i];
            const sv = l.sell || 0, bv = l.buy || 0, tv = l.volume || 0;
            const bp = l.bid_pulled || 0, ap = l.ask_pulled || 0;

            let clusSell = sv, clusBuy = bv;
            if (i > 0 && Math.abs(fpGrouped[i - 1].price - l.price) <= effStep * 1.5) {
                clusSell += (fpGrouped[i - 1].sell || 0);
                clusBuy += (fpGrouped[i - 1].buy || 0);
            }
            if (i + 1 < fpGrouped.length && Math.abs(fpGrouped[i + 1].price - l.price) <= effStep * 1.5) {
                clusSell += (fpGrouped[i + 1].sell || 0);
                clusBuy += (fpGrouped[i + 1].buy || 0);
            }
            const clusTot = clusSell + clusBuy;
            const clusBPct = clusTot > 0 ? (clusSell / clusTot) * 100 : 0;
            const clusAPct = clusTot > 0 ? (clusBuy / clusTot) * 100 : 0;

            macroBidPulled += bp;
            macroAskPulled += ap;

            if (l.price <= lowerSwingTop && clusSell >= cHeavyCut * 1.3 && clusBPct >= 58 && c.close > l.price + effStep * 0.4) {
                macroBidAbsorbs++;
            }
            if (l.price >= upperSwingBot && clusBuy >= cHeavyCut * 1.3 && clusAPct >= 58 && c.close < l.price - effStep * 0.4) {
                macroAskAbsorbs++;
            }

            // Behavioral Whale: Either Passive Absorption Sticking OR Aggressive Displacement Chunk
            const lt = Math.max(l.trades || 1, 1);
            const lotRatio = (tv / lt) / baselineLotSize;
            const hasBehavioralWhale = (l.whale_buy || 0) + (l.whale_sell || 0) > 0;
            if (hasBehavioralWhale || (tv >= Math.max(cAvgV * 1.35, heavyLvlBenchmark * 0.65) && lotRatio >= 3.2)) {
                if ((l.whale_buy || bv) > (l.whale_sell || sv)) whaleBuyLots++;
                else whaleSellLots++;
            }
        }
    });

    const netPriceMove = last.close - first.open;
    const efficiency = Math.abs(netPriceMove) / Math.max(sumCandleRanges, rawStep);
    const macroPullDelta = macroBidPulled - macroAskPulled;
    const netWhaleVolDelta = sessWhaleBuyVol - sessWhaleSellVol;

    let instBuildupPts = 0;
    let macroNotes = [];

    if (macroBidAbsorbs > macroAskAbsorbs) {
        instBuildupPts += Math.min(28, 12 + (macroBidAbsorbs - macroAskAbsorbs) * 4);
        macroNotes.push(`ClusterBidAbs(${macroBidAbsorbs}v${macroAskAbsorbs})`);
    } else if (macroAskAbsorbs > macroBidAbsorbs) {
        instBuildupPts -= Math.min(28, 12 + (macroAskAbsorbs - macroBidAbsorbs) * 4);
        macroNotes.push(`ClusterAskAbs(${macroAskAbsorbs}v${macroBidAbsorbs})`);
    }

    if (whaleBuyLots > whaleSellLots || netWhaleVolDelta > avgFlowVol * 0.3) {
        instBuildupPts += 24;
        macroNotes.push(`🐋WhaleNetBuy(+${fmt(netWhaleVolDelta)})`);
    } else if (whaleSellLots > whaleBuyLots || netWhaleVolDelta < -avgFlowVol * 0.3) {
        instBuildupPts -= 24;
        macroNotes.push(`🐋WhaleNetSell(${fmt(netWhaleVolDelta)})`);
    }

    if (macroPullDelta < -avgFlowVol * 0.15) {
        instBuildupPts += 18;
        macroNotes.push(`AskWallPulled(PΔ${fmt(macroPullDelta)})`);
    } else if (macroPullDelta > avgFlowVol * 0.15) {
        instBuildupPts -= 18;
        macroNotes.push(`BidWallPulled(PΔ+${fmt(macroPullDelta)})`);
    }

    if (pocUpSteps > pocDownSteps) {
        instBuildupPts += 16;
        macroNotes.push(`POC-DriftUp(${pocUpSteps}↑)`);
    } else if (pocDownSteps > pocUpSteps) {
        instBuildupPts -= 16;
        macroNotes.push(`POC-DriftDn(${pocDownSteps}↓)`);
    }

    if (totalCvd > 0 && (netPriceMove >= 0 || macroBidAbsorbs > macroAskAbsorbs)) {
        instBuildupPts += 14;
        macroNotes.push(`CVD+${fmt(totalCvd)}`);
    } else if (totalCvd < 0 && (netPriceMove <= 0 || macroAskAbsorbs > macroBidAbsorbs)) {
        instBuildupPts -= 14;
        macroNotes.push(`CVD${fmt(totalCvd)}`);
    }

    // Contradiction Veto: Never call LONG Buildup if Ask Absorption + Whale Net Sell dominate!
  if (instBuildupPts > 0 && macroAskAbsorbs > macroBidAbsorbs && netWhaleVolDelta < 0) {
      instBuildupPts = -18;
  } else if (instBuildupPts < 0 && macroBidAbsorbs > macroAskAbsorbs && netWhaleVolDelta > 0) {
      instBuildupPts = 18;
  }
    const vBox = document.getElementById('masterVerdictBox');
    const vTitle = document.getElementById('masterVerdictTitle');
    const vReasons = document.getElementById('masterVerdictReasons');
    const netMacro = instBuildupPts;
    const isCoilingCluster = efficiency < 0.22;

    if (isCoilingCluster && Math.abs(netMacro) >= 18) {
        if (netMacro > 0) {
            vBox.style.background = 'rgba(0, 229, 255, 0.18)';
            vBox.style.borderColor = '#00e5ff';
            vTitle.style.color = '#00e5ff';
            vTitle.innerText = `🏦 [${tf.toUpperCase()} CLUSTER BUILDUP]: INST ACCUMULATING LONG (Score:+${netMacro}) ➔ EXPECT LOW-SWEEP OR UPSIDE BREAK!`;
        } else {
            vBox.style.background = 'rgba(255, 145, 0, 0.20)';
            vBox.style.borderColor = '#ff9100';
            vTitle.style.color = '#ffab00';
            vTitle.innerText = `🏦 [${tf.toUpperCase()} CLUSTER BUILDUP]: INST DISTRIBUTING SHORT (Score:${netMacro}) ➔ EXPECT HIGH-SWEEP OR DOWNSIDE BREAK!`;
        }
        vReasons.innerText = macroNotes.slice(0, 4).join(' | ');
    } else if (netMacro >= 22) {
        vBox.style.background = 'rgba(0, 200, 83, 0.18)';
        vBox.style.borderColor = '#00e676';
        vTitle.style.color = '#00e676';
        vTitle.innerText = `🟢 [${tf.toUpperCase()} MACRO]: INSTITUTIONAL BULLISH CAMPAIGN (Score:+${netMacro}) — LONG BIAS`;
        vReasons.innerText = macroNotes.slice(0, 4).join(' | ');
    } else if (netMacro <= -22) {
        vBox.style.background = 'rgba(255, 23, 68, 0.18)';
        vBox.style.borderColor = '#ff5252';
        vTitle.style.color = '#ff5252';
        vTitle.innerText = `🔴 [${tf.toUpperCase()} MACRO]: INSTITUTIONAL BEARISH CAMPAIGN (Score:${netMacro}) — SHORT BIAS`;
        vReasons.innerText = macroNotes.slice(0, 4).join(' | ');
    } else {
        vBox.style.background = 'rgba(38, 50, 56, 0.85)';
        vBox.style.borderColor = '#90a4ae';
        vTitle.style.color = '#eceff1';
        vTitle.innerText = `⚖️ [${tf.toUpperCase()} CLUSTER]: NEUTRAL EQUILIBRIUM — MONITORING ORDERBOOK BUILDUP`;
        vReasons.innerText = macroNotes.slice(0, 4).join(' | ');
    }

    return {
        netMacro, isCoilingCluster, totalCvd, macroPullDelta, macroBidAbsorbs, macroAskAbsorbs,
        whaleBuyLots, whaleSellLots, sessWhaleBuyVol, sessWhaleSellVol,
        sessRetailBuyVol, sessRetailSellVol, macroNotes
    };
}

function drawLoop() {
    if (needsRender) {
        needsRender = false;
        try { drawAll(); } catch (err) { console.error("Render error:", err); }
    }
    requestAnimationFrame(drawLoop);
}

function drawAll() {
    const W = cv.width;
    const H = cv.height;
    const { yAxisW, domW, chartW, chartH } = getLayout();
    rawStep = STEP_MAP[symbol] || 1.0;
    const dec = rawStep < 0.01 ? 4 : (rawStep < 1 ? 2 : 1);

    ctx.fillStyle = '#090b10';
    ctx.fillRect(0, 0, W, H);

    if (!candles || candles.length === 0) {
        ctx.fillStyle = '#6c738a';
        ctx.font = '12px monospace';
        ctx.fillText('Loading Orderflow...', Math.max(10, chartW / 2 - 70), chartH / 2);
        return;
    }

    let totalHistVol = 0, totalHistTrades = 0, histCount = 0;
    candles.forEach((hc, idx) => {
        if (idx < candles.length - 1 || candles.length === 1) {
            totalHistVol += (hc.volume || 0);
            totalHistTrades += (hc.trades || 1);
            histCount++;
        }
    });
    const avgFlowVol = Math.max(totalHistVol / Math.max(1, histCount), 0.001);
    const baselineLotSize = Math.max(totalHistVol / Math.max(1, totalHistTrades), 0.0001);

    const nakedPocs = [];
    const unfinishedLevels = [];
    for (let i = 0; i < candles.length - 1; i++) {
        const c = candles[i];
        if (c.poc != null) {
            let touched = false;
            for (let j = i + 1; j < candles.length; j++) {
                if (candles[j].low <= c.poc && candles[j].high >= c.poc) { touched = true; break; }
            }
            if (!touched) nakedPocs.push({ price: c.poc, startIdx: i, vol: c.poc_volume || 0 });
        }

        const fpArr = Array.isArray(c.footprint) ? c.footprint : Object.values(c.footprint || {});
        if (fpArr.length > 0) {
            let hiLvl = null, loLvl = null;
            fpArr.forEach(l => {
                if ((l.volume || 0) <= 0) return;
                if (!hiLvl || l.price > hiLvl.price) hiLvl = l;
                if (!loLvl || l.price < loLvl.price) loLvl = l;
            });
            const minUbSideVol = Math.max(baselineLotSize * 0.8, 0.05);
            if (hiLvl && (hiLvl.buy || 0) >= minUbSideVol && (hiLvl.sell || 0) >= minUbSideVol) {
                let sweptHi = false;
                for (let j = i + 1; j < candles.length; j++) {
                    if (candles[j].high > hiLvl.price + rawStep * 0.2) { sweptHi = true; break; }
                }
                if (!sweptHi) {
                    unfinishedLevels.push({
                        price: hiLvl.price, startIdx: i, type: 'UB-HIGH',
                        buy: hiLvl.buy, sell: hiLvl.sell
                    });
                }
            }
            if (loLvl && (loLvl.buy || 0) >= minUbSideVol && (loLvl.sell || 0) >= minUbSideVol) {
                let sweptLo = false;
                for (let j = i + 1; j < candles.length; j++) {
                    if (candles[j].low < loLvl.price - rawStep * 0.2) { sweptLo = true; break; }
                }
                if (!sweptLo) {
                    unfinishedLevels.push({
                        price: loLvl.price, startIdx: i, type: 'UB-LOW',
                        buy: loLvl.buy, sell: loLvl.sell
                    });
                }
            }
        }
    }

    const rightStart = chartW - candleW * 0.62 + panX;
    const vis = [];
    for (let i = candles.length - 1; i >= 0; i--) {
        const cx = rightStart - (candles.length - 1 - i) * candleW;
        if (cx + candleW < 0) break;
        if (cx - candleW <= chartW) vis.push({ c: candles[i], cx, idx: i });
    }
    if (vis.length === 0) return;

    if (minP === null || maxP === null) {
        let lo = Infinity, hi = -Infinity;
        const focusSet = vis.slice(0, Math.min(vis.length, isMobile ? 2 : 4));
        focusSet.forEach(({ c }) => {
            if (c.low < lo) lo = c.low;
            if (c.high > hi) hi = c.high;
        });
        const span = Math.max(hi - lo, rawStep * 10);
        minP = lo - span * 0.12;
        maxP = hi + span * 0.14;
    }

    const pSpan = Math.max(maxP - minP, rawStep);

    let mult = 1;
    if (tickMultMode === 'auto') {
        const maxRowsOnScreen = Math.max(10, Math.floor(chartH / 16));
        const rawRows = pSpan / rawStep;
        if (rawRows > maxRowsOnScreen) {
            const needed = rawRows / maxRowsOnScreen;
            const niceSteps = [1, 2, 5, 10, 20, 25, 50, 100, 200, 500];
            mult = niceSteps.find(s => s >= needed) || Math.ceil(needed);
        }
    } else {
        mult = parseInt(tickMultMode, 10) || 1;
    }

    const effStep = rawStep * mult;
    const lastC = candles[candles.length - 1];
    const liveRestBench = getLiveRestingBenchmark(effStep, lastC.close);

    let totalVisBuckets = 0, totalVisVol = 0;
    vis.forEach(({ c }) => {
        const gFp = groupFootprint(c.footprint, c.low, c.high, effStep, c.close, liveRestBench).filter(x => x.volume > 0);
        totalVisBuckets += Math.max(gFp.length, 1);
        totalVisVol += (c.volume || 0);
    });
    const tfAvgCellVol = Math.max(totalVisVol / Math.max(totalVisBuckets, 1), 0.001);

    const heavyLvlBenchmark = liveRestBench > 0
        ? Math.max(tfAvgCellVol * 1.40, Math.min(tfAvgCellVol * 2.1, liveRestBench * 1.20))
        : tfAvgCellVol * 1.60;

    const macroInfo = updateMacroMarketDecision(avgFlowVol, effStep, baselineLotSize, nakedPocs, heavyLvlBenchmark, liveRestBench);

    const pToY = p => chartH - ((p - minP) / pSpan) * chartH;
    const yToP = y => minP + ((chartH - y) / chartH) * pSpan;
    const rowH = Math.max(3, Math.abs(pToY(minP + effStep) - pToY(minP)));

    ctx.strokeStyle = '#131722';
    ctx.lineWidth = 1;
    const gCount = isMobile ? 7 : 10;
    for (let i = 0; i <= gCount; i++) {
        const gy = (chartH / gCount) * i;
        ctx.beginPath(); ctx.moveTo(0, gy); ctx.lineTo(chartW, gy); ctx.stroke();
    }

    const candleIdxToX = idx => rightStart - (candles.length - 1 - idx) * candleW;

    nakedPocs.slice(-8).forEach(np => {
        if (np.price < minP || np.price > maxP) return;
        const py = pToY(np.price + effStep * 0.5);
        if (py < 14 || py > chartH - 14) return;
        const startX = Math.max(0, candleIdxToX(np.startIdx));

        ctx.save();
        ctx.strokeStyle = 'rgba(255, 235, 59, 0.65)';
        ctx.lineWidth = 1.2;
        ctx.setLineDash([5, 4]);
        ctx.beginPath();
        ctx.moveTo(startX, py);
        ctx.lineTo(chartW, py);
        ctx.stroke();
        ctx.restore();

        const lbl = `🟡 nPOC ${np.price.toFixed(dec)} (${fmt(np.vol)})`;
        ctx.font = 'bold 8.5px monospace';
        const tw = ctx.measureText(lbl).width + 8;
        ctx.fillStyle = 'rgba(255, 235, 59, 0.92)';
        ctx.fillRect(chartW - tw - 4, py - 7, tw, 14);
        ctx.fillStyle = '#000';
        ctx.textAlign = 'right';
        ctx.textBaseline = 'middle';
        ctx.fillText(lbl, chartW - 8, py);
    });

    unfinishedLevels.slice(-8).forEach(ub => {
        if (ub.price < minP || ub.price > maxP) return;
        const py = pToY(ub.price + effStep * 0.5);
        if (py < 14 || py > chartH - 14) return;
        const startX = Math.max(0, candleIdxToX(ub.startIdx));
        const isHi = ub.type === 'UB-HIGH';
        const col = isHi ? '#e040fb' : '#00e5ff';

        ctx.save();
        ctx.strokeStyle = col;
        ctx.lineWidth = 1.5;
        ctx.setLineDash([2, 3]);
        ctx.beginPath();
        ctx.moveTo(startX, py);
        ctx.lineTo(chartW, py);
        ctx.stroke();
        ctx.restore();

        const lbl = `🎯 ${ub.type} ${ub.price.toFixed(dec)} (${fmt(ub.sell)}x${fmt(ub.buy)})`;
        ctx.font = 'bold 8.5px monospace';
        const tw = ctx.measureText(lbl).width + 8;
        ctx.fillStyle = isHi ? 'rgba(224, 64, 251, 0.92)' : 'rgba(0, 229, 255, 0.92)';
        ctx.fillRect(chartW - tw - 4, py - 7, tw, 14);
        ctx.fillStyle = '#000';
        ctx.textAlign = 'right';
        ctx.textBaseline = 'middle';
        ctx.fillText(lbl, chartW - 8, py);
    });

    const timeBarTop = H - TIME_H;
    ctx.fillStyle = '#11151f';
    ctx.fillRect(0, timeBarTop, chartW, TIME_H);

    let cvd = 0;
    let hoveredCellInfo = null;

    vis.slice().reverse().forEach(({ c, cx, idx }) => {
        cvd += (c.delta || 0);
        const isUp = c.close >= c.open;
        const col = isUp ? '#00e676' : '#ff5252';

        const yH = pToY(c.high);
        const yL = pToY(c.low);
        const yO = pToY(c.open);
        const yC = pToY(c.close);

        const boxW = candleW * 0.94;
        const boxL = cx - boxW / 2;
        const boxR = cx + boxW / 2;
        const spineX = cx;

        const fp = groupFootprint(c.footprint, c.low, c.high, effStep, c.close, liveRestBench);
        const tradedCells = fp.filter(x => x.volume > 0);

        let maxV = 1, sumV = 0;
        let pocBucketPrice = c.poc;
        let maxBucketV = -1;

        tradedCells.forEach(l => {
            if (l.volume > maxV) maxV = l.volume;
            if (l.volume > maxBucketV) { maxBucketV = l.volume; pocBucketPrice = l.price; }
            sumV += l.volume;
        });

        const avgV = Math.max(tradedCells.length > 0 ? sumV / tradedCells.length : tfAvgCellVol, tfAvgCellVol * 0.85);
        const effectiveHeavyAbsCut = Math.max(avgV * 1.85, heavyLvlBenchmark * 1.15);
        const effectiveNormAbsCut = Math.max(avgV * 1.25, heavyLvlBenchmark * 0.75);

        const baseRestRef = liveRestBench > 0 ? Math.max(liveRestBench, heavyLvlBenchmark) : heavyLvlBenchmark;
        const effectiveHeavyPullQty = Math.max(baseRestRef * 0.95, avgV * 1.10);
        const effectiveNormPullQty = Math.max(baseRestRef * 0.50, avgV * 0.60);

        if (c.vah != null && c.val != null) {
            const yVah = pToY(c.vah + effStep * 0.5);
            const yVal = pToY(c.val - effStep * 0.5);
            ctx.fillStyle = 'rgba(41, 182, 246, 0.05)';
            ctx.fillRect(boxL, yVah, boxW, Math.max(2, yVal - yVah));
            ctx.strokeStyle = '#29b6f6';
            ctx.lineWidth = 1;
            ctx.beginPath();
            ctx.moveTo(boxL, yVah); ctx.lineTo(boxR, yVah);
            ctx.moveTo(boxL, yVal); ctx.lineTo(boxR, yVal);
            ctx.stroke();
        }

        const barX = boxL + 3;
        ctx.strokeStyle = col;
        ctx.lineWidth = 2;
        ctx.beginPath(); ctx.moveTo(barX, yH); ctx.lineTo(barX, yL); ctx.stroke();
        ctx.fillStyle = col;
        ctx.fillRect(barX - 3, Math.min(yO, yC), 6, Math.max(Math.abs(yC - yO), 2));

        ctx.strokeStyle = '#262c3d';
        ctx.lineWidth = 1;
        ctx.beginPath(); ctx.moveTo(spineX, yH); ctx.lineTo(spineX, yL); ctx.stroke();

        const halfW = (boxW - 10) * 0.5;
        const canPrint = candleW >= 65 && rowH >= 10;

        let topSummary = null;
        let botSummary = null;
        let cBidPullSum = 0, cAskPullSum = 0;
        let whaleBuyCount = 0, whaleSellCount = 0;
        let cWhaleBuyVol = c.whale_buy_vol || 0, cWhaleSellVol = c.whale_sell_vol || 0;
        const cRange = Math.max(c.high - c.low, rawStep * 0.5);
        const closePos = (c.close - c.low) / cRange;
        const lowerClusterTop = c.low + cRange * 0.45;
        const upperClusterBot = c.high - cRange * 0.45;

        fp.forEach((l, rowIdx) => {
            const py = pToY(l.price + effStep * 0.5);
            const sellV = l.sell || 0;
            const buyV = l.buy || 0;
            const totV = l.volume || 0;
            const bPull = l.bid_pulled || 0;
            const aPull = l.ask_pulled || 0;

            cBidPullSum += bPull;
            cAskPullSum += aPull;

            if (totV <= 0 && bPull <= 0 && aPull <= 0) return;

            let clusSell = sellV, clusBuy = buyV;
            if (rowIdx > 0 && Math.abs(fp[rowIdx - 1].price - l.price) <= effStep * 1.5) {
                clusSell += (fp[rowIdx - 1].sell || 0);
                clusBuy += (fp[rowIdx - 1].buy || 0);
            }
            if (rowIdx + 1 < fp.length && Math.abs(fp[rowIdx + 1].price - l.price) <= effStep * 1.5) {
                clusSell += (fp[rowIdx + 1].sell || 0);
                clusBuy += (fp[rowIdx + 1].buy || 0);
            }
            const clusTot = clusSell + clusBuy;
            const clusBPct = clusTot > 0 ? Math.round((clusSell / clusTot) * 100) : 0;
            const clusAPct = clusTot > 0 ? Math.round((clusBuy / clusTot) * 100) : 0;

            const trd = Math.max(l.trades || 1, 1);
            const lvlLotSize = totV / trd;
            const lotRatio = lvlLotSize / baselineLotSize;
            const wBuy = l.whale_buy || 0;
            const wSell = l.whale_sell || 0;

            const lvlPullDelta = bPull - aPull;
            const bPullPct = bPull > 0 ? Math.round((bPull / (bPull + sellV)) * 100) : 0;
            const aPullPct = aPull > 0 ? Math.round((aPull / (aPull + buyV)) * 100) : 0;
            const bAbsPct = totV > 0 ? Math.round((sellV / totV) * 100) : 0;
            const aAbsPct = totV > 0 ? Math.round((buyV / totV) * 100) : 0;

            const isStealthChurnLvl = clusTot >= Math.max(avgV * 2.2, heavyLvlBenchmark * 0.90) && clusBPct >= 42 && clusAPct >= 42;
            const inBidCluster = l.price <= lowerClusterTop;
            const bidClusterHeld = c.close > l.price + effStep * 0.4 && (c.close >= c.open || closePos >= 0.42);
            let bidAbsType = null;
            if (inBidCluster && bidClusterHeld && ((sellV >= effectiveHeavyAbsCut && bAbsPct >= 60) || (clusSell >= effectiveHeavyAbsCut * 1.65 && clusBPct >= 58 && sellV >= avgV * 0.8) || isStealthChurnLvl)) {
                bidAbsType = 'heavy';
                if (!botSummary || sellV > botSummary.val) botSummary = { type: 'ABS', val: sellV, pct: Math.max(bAbsPct, clusBPct) };
            } else if (inBidCluster && bidClusterHeld && sellV >= effectiveNormAbsCut && bAbsPct >= 52) {
                bidAbsType = 'norm';
            }

            const inAskCluster = l.price >= upperClusterBot;
            const askClusterHeld = c.close < l.price - effStep * 0.4 && (c.close <= c.open || closePos <= 0.58);
            let askAbsType = null;
            if (inAskCluster && askClusterHeld && ((buyV >= effectiveHeavyAbsCut && aAbsPct >= 60) || (clusBuy >= effectiveHeavyAbsCut * 1.65 && clusAPct >= 58 && buyV >= avgV * 0.8) || isStealthChurnLvl)) {
                askAbsType = 'heavy';
                if (!topSummary || buyV > topSummary.val) topSummary = { type: 'ABS', val: buyV, pct: Math.max(aAbsPct, clusAPct) };
            } else if (inAskCluster && askClusterHeld && buyV >= effectiveNormAbsCut && aAbsPct >= 52) {
                askAbsType = 'norm';
            }

            // Behavioral Whale Classification: Passive Absorption Whale OR Aggressive Displacement Whale
            const minRealWhaleCellVol = heavyLvlBenchmark * 1.25;
            const isPassiveAbsWhale = (bidAbsType === 'heavy' || askAbsType === 'heavy') && (totV >= minRealWhaleCellVol);
            const isDisplacementWhale = (totV >= minRealWhaleCellVol) && ((wBuy + wSell >= heavyLvlBenchmark * 0.60) || lotRatio >= 3.8);
            const isWhaleLot = isPassiveAbsWhale || isDisplacementWhale;

            if (isWhaleLot) {
                if (bidAbsType === 'heavy' || ((wBuy > 0 || wSell > 0) ? (wBuy > wSell) : (buyV > sellV))) whaleBuyCount++;
                else whaleSellCount++;
            }

            let bidPullType = null;
            if (bPull >= effectiveHeavyPullQty && bPullPct >= 55) bidPullType = 'heavy';
            else if (bPull >= effectiveNormPullQty && bPullPct >= 38) bidPullType = 'norm';

            let askPullType = null;
            if (aPull >= effectiveHeavyPullQty && aPullPct >= 55) askPullType = 'heavy';
            else if (aPull >= effectiveNormPullQty && aPullPct >= 38) askPullType = 'norm';

            if (py < -20 || py > chartH + 10) return;

            const cellH = Math.max(rowH - 1, 2);
            const topY = py - cellH * 0.5;

            if (proViewMode) {
                if (bidAbsType === 'heavy') {
                    ctx.fillStyle = 'rgba(0, 229, 255, 0.22)';
                    ctx.fillRect(spineX - halfW, topY, halfW, cellH);
                } else if (bidPullType === 'heavy' && lvlPullDelta > 0) {
                    ctx.fillStyle = 'rgba(255, 64, 129, 0.22)';
                    ctx.fillRect(spineX - halfW, topY, halfW, cellH);
                }

                if (askAbsType === 'heavy') {
                    ctx.fillStyle = 'rgba(255, 145, 0, 0.25)';
                    ctx.fillRect(spineX, topY, halfW, cellH);
                } else if (askPullType === 'heavy' && lvlPullDelta < 0) {
                    ctx.fillStyle = 'rgba(224, 64, 251, 0.22)';
                    ctx.fillRect(spineX, topY, halfW, cellH);
                }
            }

            const sellRatio = Math.min(1, sellV / maxV);
            const buyRatio = Math.min(1, buyV / maxV);
            const sellImb = sellV > buyV * 2.5 && sellV > 0;
            const buyImb = buyV > sellV * 2.5 && buyV > 0;

            ctx.fillStyle = sellImb ? 'rgba(255, 23, 68, 0.55)' : 'rgba(242, 54, 69, 0.25)';
            ctx.fillRect(spineX - halfW * sellRatio, topY, halfW * sellRatio, cellH);

            ctx.fillStyle = buyImb ? 'rgba(0, 230, 118, 0.55)' : 'rgba(8, 153, 129, 0.25)';
            ctx.fillRect(spineX, topY, halfW * buyRatio, cellH);

            const isPoc = pocBucketPrice != null && Math.abs(l.price - pocBucketPrice) < effStep * 0.4;
            if (isPoc) {
                ctx.strokeStyle = '#ffeb3b';
                ctx.lineWidth = 1.5;
                ctx.strokeRect(spineX - halfW, topY, halfW * 2, cellH);
            }

            if (curX >= boxL && curX <= boxR && curY >= topY && curY <= topY + cellH) {
                hoveredCellInfo = {
                    price: l.price, sellV, buyV,
                    wBuy, wSell, rBuy: l.retail_buy || 0, rSell: l.retail_sell || 0,
                    bPull, aPull, lvlPullDelta,
                    bPullPct, aPullPct, bAbsPct, aAbsPct,
                    trades: trd, lvlLotSize, lotRatio, isWhaleLot, isPassiveAbsWhale
                };
                ctx.strokeStyle = '#ffffff';
                ctx.lineWidth = 1;
                ctx.strokeRect(spineX - halfW, topY, halfW * 2, cellH);
            }

            if (canPrint) {
                ctx.textBaseline = 'middle';
                const whaleSellMark = (proViewMode && isWhaleLot && (askAbsType === 'heavy' || wSell > wBuy || (wSell === 0 && sellV >= buyV))) ? '◆' : '';
                const whaleBuyMark = (proViewMode && isWhaleLot && (bidAbsType === 'heavy' || wBuy > wSell || (wBuy === 0 && buyV > sellV))) ? '◆' : '';

                ctx.font = (sellImb || isPoc || whaleSellMark) ? 'bold 9.5px monospace' : '9.5px monospace';
                ctx.fillStyle = whaleSellMark ? '#ffd700' : (sellImb ? '#ff5252' : (isPoc ? '#ffeb3b' : '#e0e3eb'));
                ctx.textAlign = 'right';
                ctx.fillText(`${whaleSellMark}${fmt(sellV)}`, spineX - 4, py);

                ctx.font = (buyImb || isPoc || whaleBuyMark) ? 'bold 9.5px monospace' : '9.5px monospace';
                ctx.fillStyle = whaleBuyMark ? '#ffd700' : (buyImb ? '#00e676' : (isPoc ? '#ffeb3b' : '#e0e3eb'));
                ctx.textAlign = 'left';
                ctx.fillText(`${fmt(buyV)}${whaleBuyMark}`, spineX + 4, py);

                if (proViewMode && candleW >= 185) {
                    let leftTag = '';
                    let leftCol = '#00e5ff';
                    if (bidAbsType === 'heavy') { leftTag = `ABS:${bAbsPct}%`; leftCol = '#00e5ff'; }
                    else if (bidPullType === 'heavy') { leftTag = `BP:${fmt(bPull)}`; leftCol = '#ff4081'; }
                    if (leftTag) {
                        ctx.font = 'bold 7.5px monospace';
                        ctx.fillStyle = leftCol;
                        ctx.textAlign = 'left';
                        ctx.fillText(leftTag, boxL + 5, py);
                    }

                    let rightTag = '';
                    let rightCol = '#ff9100';
                    if (askAbsType === 'heavy') { rightTag = `ABS:${aAbsPct}%`; rightCol = '#ff9100'; }
                    else if (askPullType === 'heavy') { rightTag = `AP:${fmt(aPull)}`; rightCol = '#e040fb'; }
                    else if (isPoc) { rightTag = 'POC'; rightCol = '#ffeb3b'; }
                    if (rightTag) {
                        ctx.font = 'bold 7.5px monospace';
                        ctx.fillStyle = rightCol;
                        ctx.textAlign = 'right';
                        ctx.fillText(rightTag, boxR - 3, py);
                    }
                }
            }
        });

        const totalWhalesInCandle = whaleBuyCount + whaleSellCount;
        const cVol = c.volume || 0;
        const rvol = cVol / avgFlowVol;
        const mktBuyPct = cVol > 0 ? Math.round(((c.buy_volume || 0) / cVol) * 100) : 50;
        const mktSellPct = 100 - mktBuyPct;
        const bodySize = Math.abs(c.close - c.open);
        const upperWick = c.high - Math.max(c.open, c.close);
        const lowerWick = Math.min(c.open, c.close) - c.low;

        let prevSwingHigh = -Infinity, prevSwingLow = Infinity;
        for (let k = Math.max(0, idx - 5); k < idx; k++) {
            if (candles[k].high > prevSwingHigh) prevSwingHigh = candles[k].high;
            if (candles[k].low < prevSwingLow) prevSwingLow = candles[k].low;
        }
        const sweptSwingHigh = idx > 0 && c.high > prevSwingHigh;
        const sweptSwingLow = idx > 0 && c.low < prevSwingLow;
        const candleAvgLot = cVol / Math.max(c.trades || 1, 1);
        const isRetailLotBurst = (candleAvgLot < baselineLotSize * 1.15) && (rvol >= 0.85);

        const candlePullDelta = cBidPullSum - cAskPullSum;
        const cBpPct = cBidPullSum > 0 ? Math.round((cBidPullSum / (cBidPullSum + (c.sell_volume || 1))) * 100) : 0;
        const cApPct = cAskPullSum > 0 ? Math.round((cAskPullSum / (cAskPullSum + (c.buy_volume || 1))) * 100) : 0;

        let pullDeltaLine = `10sBP:${fmt(cBidPullSum)}(${cBpPct}%) AP:${fmt(cAskPullSum)}(${cApPct}%) PΔ:${candlePullDelta >= 0 ? '+' : ''}${fmt(candlePullDelta)}`;
        let pullDeltaCol = '#b0bec5';
        if (candlePullDelta < -avgV * 0.4 && c.close > c.open) {
            pullDeltaCol = '#00e5ff';
        } else if (candlePullDelta > avgV * 0.4 && c.close < c.open) {
            pullDeltaCol = '#ff4081';
        }

        let flowVerdict = (cWhaleBuyVol + cWhaleSellVol > 0)
            ? `🐋W:${fmt(cWhaleBuyVol)}B/${fmt(cWhaleSellVol)}S | 🐑R:${fmt(c.retail_buy_vol)}B/${fmt(c.retail_sell_vol)}S`
            : `MB:${mktBuyPct}%|MS:${mktSellPct}%`;
        let candleDecision = `⚪ NEUTRAL / CHOP`;
        let verdictCol = '#8b93a7';
        let decisionCol = '#9ba2b0';
        let isHighConviction = false;

        if (rvol < 0.65) {
            flowVerdict = `LOW FLOW (${rvol.toFixed(1)}x)`;
            candleDecision = `⚪ SKIP (RETAIL NOISE)`;
        } else {
            let cBull = 0, cBear = 0;
            if (mktSellPct >= 56 && (c.close >= c.open || lowerWick > bodySize * 0.75)) cBull += 34;
            if (mktBuyPct >= 56 && (c.close <= c.open || upperWick > bodySize * 0.75)) cBear += 34;
            if (mktBuyPct >= 58 && c.close > c.open && closePos >= 0.6) cBull += 30;
            if (mktSellPct >= 58 && c.close < c.open && closePos <= 0.4) cBear += 30;
            if (botSummary && closePos >= 0.42) cBull += 26;
            if (topSummary && closePos <= 0.58) cBear += 26;
            if (candlePullDelta < 0 && c.close > c.open) cBull += 20;
            if (candlePullDelta > 0 && c.close < c.open) cBear += 20;
            if (whaleBuyCount > whaleSellCount || cWhaleBuyVol > cWhaleSellVol * 1.3) cBull += 22;
            else if (whaleSellCount > whaleBuyCount || cWhaleSellVol > cWhaleBuyVol * 1.3) cBear += 22;

            if (sweptSwingLow && c.close > prevSwingLow && (botSummary || isRetailLotBurst)) {
                flowVerdict = `🎯SSL-SWEEP: Liquidity Grabbed ➔ Reversal!`; verdictCol = '#00e5ff'; isHighConviction = true; cBull += 28;
            } else if (sweptSwingHigh && c.close < prevSwingHigh && (topSummary || isRetailLotBurst)) {
                flowVerdict = `🎯BSL-SWEEP: Liquidity Grabbed ➔ Reject!`; verdictCol = '#ff9100'; isHighConviction = true; cBear += 28;
            } else if (candlePullDelta < -avgV * 0.6 && cApPct >= 55 && c.close > c.open) {
                flowVerdict = `⚠️FAKE-ASK WALL PULLED ➔ BULL BREAK!`; verdictCol = '#e040fb'; isHighConviction = true;
            } else if (candlePullDelta > avgV * 0.6 && cBpPct >= 55 && c.close < c.open) {
                flowVerdict = `⚠️FAKE-BID WALL PULLED ➔ BEAR DUMP!`; verdictCol = '#ff4081'; isHighConviction = true;
            } else if (topSummary && cAskPullSum < topSummary.val * 0.3 && c.close <= c.open) {
                flowVerdict = `🛡️CLUSTER-ASK CAP: Absorbed & Stopped!`; verdictCol = '#ff9100'; isHighConviction = true;
            } else if (botSummary && cBidPullSum < botSummary.val * 0.3 && c.close >= c.open) {
                flowVerdict = `🛡️CLUSTER-BID HOLD: Absorbed & Stopped!`; verdictCol = '#00e5ff'; isHighConviction = true;
            }

            const cNet = cBull - cBear;
            const whalePrefix = totalWhalesInCandle > 0 ? '◆' : '';

            if (cNet >= 20) {
                candleDecision = `${whalePrefix}🟢 BUYERS WIN (+${cNet})`;
                decisionCol = '#00e676';
            } else if (cNet <= -20) {
                candleDecision = `${whalePrefix}🔴 SELLERS WIN (${cNet})`;
                decisionCol = '#ff5252';
            } else {
                candleDecision = `${whalePrefix}⚖️ CLUSTER BUILDUP`;
                decisionCol = '#ffca28';
            }
        }

        let botYBanner = yL + 16;
        if (proViewMode && candleW >= 70 && rvol >= 0.75) {
            ctx.font = 'bold 9px monospace';
            ctx.textAlign = 'center';
            let topYBanner = Math.max(16, yH - 18);
            if (topSummary && c.close < c.high - rawStep) {
                ctx.fillStyle = 'rgba(255, 145, 0, 0.92)';
                ctx.fillRect(cx - 64, topYBanner, 128, 13);
                ctx.fillStyle = '#000';
                ctx.fillText(`▼ ASK ABS:${fmt(topSummary.val)}(${topSummary.pct}%)`, cx, topYBanner + 7);
            }
            if (botSummary && c.close > c.low + rawStep) {
                ctx.fillStyle = 'rgba(0, 229, 255, 0.92)';
                ctx.fillRect(cx - 64, botYBanner, 128, 13);
                ctx.fillStyle = '#000';
                ctx.fillText(`▲ BID ABS:${fmt(botSummary.val)}(${botSummary.pct}%)`, cx, botYBanner + 7);
                botYBanner += 18;
            }
        }

        if (candleW >= 65) {
            const pillW = Math.min(candleW * 0.96, 198);
            const pillH = 64;
            const maxAllowedY = H - TIME_H - pillH - 3;
            const pillY = Math.min(Math.max(botYBanner + 5, yL + 18), maxAllowedY);
            const pillX = cx - pillW * 0.5;

            ctx.fillStyle = isHighConviction ? 'rgba(16, 20, 34, 0.94)' : 'rgba(12, 16, 24, 0.88)';
            ctx.fillRect(pillX, pillY, pillW, pillH);
            ctx.strokeStyle = isHighConviction ? decisionCol : 'rgba(60, 72, 98, 0.65)';
            ctx.strokeRect(pillX, pillY, pillW, pillH);

            ctx.textAlign = 'center';
            ctx.textBaseline = 'middle';
            ctx.font = 'bold 9px monospace';
            ctx.fillStyle = c.delta >= 0 ? '#00e676' : '#ff5252';
            ctx.fillText(`Δ${c.delta >= 0 ? '+' : ''}${fmt(c.delta)} | V:${fmt(c.volume)} (${rvol.toFixed(1)}x)`, cx, pillY + 8);

            ctx.font = '8.5px monospace';
            ctx.fillStyle = '#b0b8c8';
            ctx.fillText(`CVD:${cvd >= 0 ? '+' : ''}${fmt(cvd)} | B:${fmt(c.buy_volume)} S:${fmt(c.sell_volume)}`, cx, pillY + 19);

            ctx.font = 'bold 8.5px monospace';
            ctx.fillStyle = pullDeltaCol;
            ctx.fillText(pullDeltaLine, cx, pillY + 31);

            ctx.font = '8.5px monospace';
            ctx.fillStyle = verdictCol;
            ctx.fillText(flowVerdict, cx, pillY + 43);

            ctx.fillStyle = decisionCol;
            ctx.font = 'bold 9px monospace';
            ctx.fillText(candleDecision, cx, pillY + 55);
        }

        if (candleW >= 40) {
            const tStr = new Date(c.start).toTimeString().slice(0, 5);
            ctx.fillStyle = '#9ba2b0';
            ctx.font = 'bold 10px monospace';
            ctx.textAlign = 'center';
            ctx.fillText(tStr, cx, timeBarTop + TIME_H * 0.55);
        }
    });

    const hudW = Math.min(chartW - 12, 510);
    ctx.fillStyle = 'rgba(17, 21, 31, 0.92)';
    ctx.fillRect(6, 6, hudW, hoveredCellInfo ? 66 : 22);
    ctx.strokeStyle = '#2962ff';
    ctx.strokeRect(6, 6, hudW, hoveredCellInfo ? 66 : 22);
    ctx.fillStyle = '#00e5ff';
    ctx.font = 'bold 10px monospace';
    ctx.textAlign = 'left';
    ctx.fillText(`Scale:${mult}x (${effStep.toFixed(dec)}) | HeavyL:>${fmt(heavyLvlBenchmark)} | RestBook:${fmt(liveRestBench)}`, 12, 17);

    if (hoveredCellInfo) {
        const h = hoveredCellInfo;
        ctx.fillStyle = '#ffeb3b';
        ctx.fillText(`Px:${h.price.toFixed(dec)} | 🐋Whale B:${fmt(h.wBuy)} S:${fmt(h.wSell)} | 🐑Retail B:${fmt(h.rBuy)} S:${fmt(h.rSell)}`, 12, 33);
        ctx.fillStyle = '#ff80ab';
        ctx.fillText(`10sBPull:${fmt(h.bPull)}(${h.bPullPct}%) | 10sAPull:${fmt(h.aPull)}(${h.aPullPct}%) | PΔ:${h.lvlPullDelta >= 0 ? '+' : ''}${fmt(h.lvlPullDelta)}`, 12, 47);
        ctx.fillStyle = h.isWhaleLot ? '#ffd700' : '#b0bec5';
        const playerTag = h.isPassiveAbsWhale ? '🛡️ PASSIVE ABSORB WHALE!' : (h.isWhaleLot ? '🐋 DISPLACEMENT WHALE!' : (h.lotRatio < 1.2 ? '🐑 RETAIL / BOT SPLIT' : '⚡ STANDARD FLOW'));
        ctx.fillText(`Trades:${h.trades} | AvgLot:${fmt(h.lvlLotSize)} (${h.lotRatio.toFixed(1)}x) [${playerTag}]`, 12, 61);
    }

    const yAxisX = chartW;
    ctx.fillStyle = '#11151f';
    ctx.fillRect(yAxisX, 0, yAxisW, H);
    ctx.fillStyle = '#8b93a7';
    ctx.font = '10px monospace';
    ctx.textAlign = 'left';
    for (let i = 0; i <= gCount; i++) {
        const gy = (chartH / gCount) * i;
        ctx.fillText(yToP(gy).toFixed(dec), yAxisX + 4, gy);
    }

    const curPy = pToY(lastC.close);
    if (curPy >= 0 && curPy <= chartH) {
        const pCol = lastC.close >= lastC.open ? '#00c853' : '#ff1744';
        ctx.fillStyle = pCol;
        ctx.fillRect(yAxisX, curPy - 10, yAxisW, 20);
        ctx.fillStyle = '#fff';
        ctx.font = 'bold 10px monospace';
        ctx.fillText(lastC.close.toFixed(dec), yAxisX + 4, curPy);
    }

    // =========================================================================
    // MULTI-ROW CLUSTER DOM + IRON-ANCHOR VERDICT ENGINE (CRASH-PROOF)
    // =========================================================================
    try {
        const domStep = Math.max(effStep || rawStep || 1.0, 1e-6);
        const bidsArr = Array.isArray(obData.bids) ? obData.bids : [];
        const asksArr = Array.isArray(obData.asks) ? obData.asks : [];
        const rawDomArr = Array.isArray(obData.dom_raw) ? obData.dom_raw : [];
        const liveCenterP = Math.floor(lastC.close / domStep + 1e-9) * domStep;

        const bidMap = new Map();
        const askMap = new Map();
        bidsArr.forEach(b => {
            if (!b || !Number.isFinite(b.price)) return;
            const bp = Math.floor(b.price / domStep + 1e-9) * domStep;
            const k = bp.toFixed(6);
            bidMap.set(k, (bidMap.get(k) || 0) + (Number(b.quantity) || 0));
        });
        asksArr.forEach(a => {
            if (!a || !Number.isFinite(a.price)) return;
            const ap = Math.floor(a.price / domStep + 1e-9) * domStep;
            const k = ap.toFixed(6);
            askMap.set(k, (askMap.get(k) || 0) + (Number(a.quantity) || 0));
        });

        const rawDomCluster = new Map();
        rawDomArr.forEach(d => {
            if (!d || !Number.isFinite(d.price)) return;
            const b = Math.floor(d.price / domStep + 1e-9) * domStep;
            const k = b.toFixed(6);
            let cur = rawDomCluster.get(k) || {
                wBuy: 0, wSell: 0, rBuy: 0, rSell: 0,
                flashBp: 0, flashAp: 0, agedBp: 0, agedAp: 0,
                iceBid: 0, iceAsk: 0
            };
            cur.wBuy += (Number(d.w_buy) || 0);
            cur.wSell += (Number(d.w_sell) || 0);
            cur.rBuy += (Number(d.r_buy) || 0);
            cur.rSell += (Number(d.r_sell) || 0);
            cur.flashBp += (Number(d.flash_bid_pull) || 0);
            cur.flashAp += (Number(d.flash_ask_pull) || 0);
            cur.agedBp += (Number(d.aged_bid_pull) || 0);
            cur.agedAp += (Number(d.aged_ask_pull) || 0);
            cur.iceBid += (Number(d.iceberg_bid) || 0);
            cur.iceAsk += (Number(d.iceberg_ask) || 0);
            rawDomCluster.set(k, cur);
        });

        const domMagnetMap = new Map();
        nakedPocs.forEach(np => {
            const b = Math.floor(np.price / domStep + 1e-9) * domStep;
            domMagnetMap.set(b.toFixed(6), { type: 'nPOC', lbl: '🟡nPOC' });
        });
        unfinishedLevels.forEach(ub => {
            const b = Math.floor(ub.price / domStep + 1e-9) * domStep;
            domMagnetMap.set(b.toFixed(6), { type: ub.type, lbl: ub.type === 'UB-HIGH' ? '🎯UB-HI' : '🎯UB-LO' });
        });

        const structSet = candles.slice(-6);
        const stickyMap = new Map();
        let minActivePrice = lastC.low;
        let maxActivePrice = lastC.high;
        let rBidAbs = 0, rAskAbs = 0;

        const minInstCellFloor = (avgFlowVol / 12.0) * 0.85;
        const effCellBench = Math.max(tfAvgCellVol, minInstCellFloor);

        structSet.forEach((rc, sIdx) => {
            const isRecent3 = sIdx >= Math.max(0, structSet.length - 3);
            if (isRecent3) {
                if (rc.low < minActivePrice) minActivePrice = rc.low;
                if (rc.high > maxActivePrice) maxActivePrice = rc.high;
            }
            const rcRange = Math.max(rc.high - rc.low, rawStep * 0.5);
            const rcClosePos = (rc.close - rc.low) / rcRange;
            const lowerSwingZone = rc.low + rcRange * 0.45;
            const upperSwingZone = rc.high - rcRange * 0.45;
            const gList = groupFootprint(rc.footprint, rc.low, rc.high, domStep, rc.close, liveRestBench);

            for (let i = 0; i < gList.length; i++) {
                const gl = gList[i];
                const k = gl.price.toFixed(6);
                if (isRecent3) {
                    let cur = stickyMap.get(k) || { buy: 0, sell: 0, wBuy: 0, wSell: 0, rBuy: 0, rSell: 0, bp: 0, ap: 0 };
                    cur.buy += gl.buy;
                    cur.sell += gl.sell;
                    cur.wBuy += (gl.whale_buy || 0);
                    cur.wSell += (gl.whale_sell || 0);
                    cur.rBuy += (gl.retail_buy || 0);
                    cur.rSell += (gl.retail_sell || 0);
                    cur.bp += gl.bid_pulled;
                    cur.ap += gl.ask_pulled;
                    stickyMap.set(k, cur);
                }

                let clusSell = gl.sell || 0, clusBuy = gl.buy || 0;
                if (i > 0 && Math.abs(gList[i - 1].price - gl.price) <= domStep * 1.5) {
                    clusSell += (gList[i - 1].sell || 0);
                    clusBuy += (gList[i - 1].buy || 0);
                }
                if (i + 1 < gList.length && Math.abs(gList[i + 1].price - gl.price) <= domStep * 1.5) {
                    clusSell += (gList[i + 1].sell || 0);
                    clusBuy += (gList[i + 1].buy || 0);
                }
                const clusTot = clusSell + clusBuy;
                const clusBPct = clusTot > 0 ? (clusSell / clusTot) * 100 : 0;
                const clusAPct = clusTot > 0 ? (clusBuy / clusTot) * 100 : 0;
                const isChurn = clusTot >= effCellBench * 2.1 && clusBPct >= 42 && clusAPct >= 42;
                const bRestHere = bidMap.get(k) || 0;
                const aRestHere = askMap.get(k) || 0;
                const rawD = rawDomCluster.get(k);

                const bidRotatedUp = (rc.close >= rc.open || rcClosePos >= 0.45) && (rc.close > gl.price + domStep * 0.4);
                const askRotatedDn = (rc.close <= rc.open || rcClosePos <= 0.55) && (rc.close < gl.price - domStep * 0.4);

                const isBidClusterAbs = gl.price <= lowerSwingZone && bidRotatedUp && (
                    (clusSell >= effCellBench * 1.6 && clusBPct >= 58 && gl.sell >= effCellBench * 0.7)
                    || isChurn
                    || (gl.sell >= effCellBench * 1.0 && bRestHere >= Math.max(liveRestBench * 1.3, effCellBench))
                    || (rawD && rawD.iceBid >= effCellBench * 0.5)
                );

                const isAskClusterAbs = gl.price >= upperSwingZone && askRotatedDn && (
                    (clusBuy >= effCellBench * 1.6 && clusAPct >= 58 && gl.buy >= effCellBench * 0.7)
                    || isChurn
                    || (gl.buy >= effCellBench * 1.0 && aRestHere >= Math.max(liveRestBench * 1.3, effCellBench))
                    || (rawD && rawD.iceAsk >= effCellBench * 0.5)
                );

                if (isBidClusterAbs) rBidAbs++;
                if (isAskClusterAbs) rAskAbs++;
            }
        });

        const rawMinB = Math.floor((minActivePrice - domStep * 5) / domStep) * domStep;
        const rawMaxB = Math.ceil((maxActivePrice + domStep * 5) / domStep) * domStep;
        const maxSpanSteps = domStep * 80;
        const minVisibleBucket = Math.max(rawMinB, liveCenterP - maxSpanSteps);
        const maxVisibleBucket = Math.min(rawMaxB, liveCenterP + maxSpanSteps);

        const activeDomRows = [];
        for (let p = maxVisibleBucket; p >= minVisibleBucket; p -= domStep) {
            const k = p.toFixed(6);
            const bRest = bidMap.get(k) || 0;
            const aRest = askMap.get(k) || 0;
            const st = stickyMap.get(k) || { buy: 0, sell: 0, wBuy: 0, wSell: 0, rBuy: 0, rSell: 0, bp: 0, ap: 0 };
            const rawD = rawDomCluster.get(k) || { wBuy: 0, wSell: 0, rBuy: 0, rSell: 0, flashBp: 0, flashAp: 0, agedBp: 0, agedAp: 0, iceBid: 0, iceAsk: 0 };
            const mag = domMagnetMap.get(k) || null;
            const distTicks = Math.round(Math.abs(p - liveCenterP) / domStep);

            const totRawBp = Math.max(st.bp, rawD.agedBp) + rawD.flashBp;
            const totRawAp = Math.max(st.ap, rawD.agedAp) + rawD.flashAp;

            if (st.buy > 0 || st.sell > 0 || totRawBp > 0 || totRawAp > 0 || mag !== null || distTicks <= 6 || ((bRest > 0 || aRest > 0) && distTicks <= 16)) {
                activeDomRows.push({ p, k, bRest, aRest, st, rawD, totRawBp, totRawAp, mag });
            }
        }

        let maxRowVal = 1;
        let totExecBuy = 0, totExecSell = 0, totPullB = 0, totPullA = 0;
        let totFlashPullB = 0, totFlashPullA = 0, totIceBid = 0, totIceAsk = 0;
        let totWhaleBuy = 0, totWhaleSell = 0, totRetailBuy = 0, totRetailSell = 0;
        let nearBidRest = 0, nearAskRest = 0;

        activeDomRows.forEach(r => {
            totExecBuy += r.st.buy;
            totExecSell += r.st.sell;
            totWhaleBuy += Math.max(r.st.wBuy, r.rawD.wBuy);
            totWhaleSell += Math.max(r.st.wSell, r.rawD.wSell);
            totRetailBuy += Math.max(r.st.rBuy, r.rawD.rBuy);
            totRetailSell += Math.max(r.st.rSell, r.rawD.rSell);
            totPullB += r.totRawBp;
            totPullA += r.totRawAp;
            totFlashPullB += r.rawD.flashBp;
            totFlashPullA += r.rawD.flashAp;
            totIceBid += r.rawD.iceBid;
            totIceAsk += r.rawD.iceAsk;

            if (r.p <= liveCenterP && r.p >= liveCenterP - domStep * 8) nearBidRest += r.bRest;
            if (r.p >= liveCenterP && r.p <= liveCenterP + domStep * 8) nearAskRest += r.aRest;
            maxRowVal = Math.max(maxRowVal, r.st.buy, r.st.sell, r.totRawBp, r.totRawAp, r.bRest, r.aRest);
        });

        const domPullDelta = totPullB - totPullA;
        const curRvol = (lastC.volume || 0) / avgFlowVol;
        const cRange = Math.max(lastC.high - lastC.low, rawStep * 0.5);
        const closePos = (lastC.close - lastC.low) / cRange;
        const prevC = candles.length >= 2 ? candles[candles.length - 2] : lastC;
        const prevPoc = prevC.poc != null ? prevC.poc : (prevC.high + prevC.low) * 0.5;
        const curPoc = lastC.poc != null ? lastC.poc : (lastC.high + lastC.low) * 0.5;

        let sPocUp = 0, sPocDn = 0, hhCount = 0, llCount = 0;
        const last4 = candles.slice(-4);
        for (let m = 1; m < last4.length; m++) {
            if (last4[m].poc != null && last4[m - 1].poc != null) {
                if (last4[m].poc > last4[m - 1].poc) sPocUp++;
                else if (last4[m].poc < last4[m - 1].poc) sPocDn++;
            }
            if (last4[m].high > last4[m - 1].high && last4[m].low >= last4[m - 1].low) hhCount++;
            if (last4[m].low < last4[m - 1].low && last4[m].high <= last4[m - 1].high) llCount++;
        }

        let prev5High = -Infinity, prev5Low = Infinity;
        for (let k = Math.max(0, candles.length - 6); k < candles.length - 1; k++) {
            if (candles[k].high > prev5High) prev5High = candles[k].high;
            if (candles[k].low < prev5Low) prev5Low = candles[k].low;
        }

        // =========================================================================
        // IRON-ANCHOR INSTITUTIONAL VERDICT ENGINE (FOOTPRINT + LIVE DOM SYNERGY)
        // =========================================================================
        const closedCandles = candles.length > 2 ? candles.slice(0, -1) : candles;
        const closed6 = closedCandles.slice(-6);
        const avgCandleRange = closed6.reduce((s, x) => s + Math.max(x.high - x.low, rawStep), 0) / Math.max(1, closed6.length);

        let anchorBullScore = 0, anchorBearScore = 0;
        let anchorSupportPx = null, anchorResistPx = null;

        closed6.forEach(cc => {
            const ccRange = Math.max(cc.high - cc.low, rawStep * 0.5);
            const ccClosePos = (cc.close - cc.low) / ccRange;
            const ccWBuy = cc.whale_buy_vol || 0;
            const ccWSell = cc.whale_sell_vol || 0;
            const ccDelta = cc.delta || 0;

            if (ccDelta < -effCellBench * 0.3 && ccClosePos >= 0.55) {
                anchorBullScore += 18;
                anchorSupportPx = anchorSupportPx === null ? cc.low : Math.min(anchorSupportPx, cc.low);
            } else if (ccDelta > effCellBench * 0.3 && ccClosePos <= 0.45) {
                anchorBearScore += 18;
                anchorResistPx = anchorResistPx === null ? cc.high : Math.max(anchorResistPx, cc.high);
            }

            if (ccWBuy > ccWSell * 1.35 && ccClosePos >= 0.50) {
                anchorBullScore += 14;
                if (cc.poc) anchorSupportPx = anchorSupportPx === null ? cc.poc : Math.min(anchorSupportPx, cc.poc);
            } else if (ccWSell > ccWBuy * 1.35 && ccClosePos <= 0.50) {
                anchorBearScore += 14;
                if (cc.poc) anchorResistPx = anchorResistPx === null ? cc.poc : Math.max(anchorResistPx, cc.poc);
            }
        });

        if (rBidAbs > rAskAbs) anchorBullScore += Math.min(26, 12 + (rBidAbs - rAskAbs) * 5);
        else if (rAskAbs > rBidAbs) anchorBearScore += Math.min(26, 12 + (rAskAbs - rBidAbs) * 5);

        if (totIceBid > totIceAsk * 1.3) anchorBullScore += 15;
        else if (totIceAsk > totIceBid * 1.3) anchorBearScore += 15;

        if (sPocUp > sPocDn) anchorBullScore += 14;
        else if (sPocDn > sPocUp) anchorBearScore += 14;

        const isBigExpansionCandle = cRange >= avgCandleRange * 1.55;
        const bullSweepReject = lastC.low < prev5Low && lastC.close > prev5Low && (closePos >= 0.48 || rBidAbs > 0 || totIceBid > 0);
        const bearSweepReject = lastC.high > prev5High && lastC.close < prev5High && (closePos <= 0.52 || rAskAbs > 0 || totIceAsk > 0);

        const recent20 = candles.slice(-20);
        const r20Hi = Math.max(...recent20.map(x => x.high));
        const r20Lo = Math.min(...recent20.map(x => x.low));
        const r20Span = Math.max(r20Hi - r20Lo, rawStep);
        const rangeLocation = (lastC.close - r20Lo) / r20Span;

        const isBullExhaustionTrap = (
            bearSweepReject
            || (lastC.close > lastC.open && isBigExpansionCandle && rangeLocation >= 0.78 && (rAskAbs > 0 || totIceAsk > effCellBench * 0.4))
            || (lastC.close > lastC.open && anchorBearScore > anchorBullScore + 20 && lastC.close < (anchorResistPx || r20Hi))
            || (lastC.delta > effCellBench * 0.45 && closePos <= 0.46)
        );

        const isBearExhaustionTrap = (
            bullSweepReject
            || (lastC.close < lastC.open && isBigExpansionCandle && rangeLocation <= 0.22 && (rBidAbs > 0 || totIceBid > effCellBench * 0.4))
            || (lastC.close < lastC.open && anchorBullScore > anchorBearScore + 20 && lastC.close > (anchorSupportPx || r20Lo))
            || (lastC.delta < -effCellBench * 0.45 && closePos >= 0.54)
        );

        const instBullDrive = (
            !isBullExhaustionTrap
            && anchorBullScore >= anchorBearScore
            && (totWhaleBuy > totWhaleSell * 1.20 || domPullDelta < -effCellBench * 0.30)
            && lastC.close > curPoc && prevC.close >= prevPoc
            && closePos >= 0.60 && rangeLocation <= 0.82
        );

        const instBearDrive = (
            !isBearExhaustionTrap
            && anchorBearScore >= anchorBullScore
            && (totWhaleSell > totWhaleBuy * 1.20 || domPullDelta > effCellBench * 0.30)
            && lastC.close < curPoc && prevC.close <= prevPoc
            && closePos <= 0.40 && rangeLocation >= 0.18
        );

        const clusterLongBuildup = (anchorBullScore >= anchorBearScore + 15) && !instBearDrive;
        const clusterShortBuildup = (anchorBearScore >= anchorBullScore + 15) && !instBullDrive;
        const retailBuysTrapped = isBullExhaustionTrap;
        const retailSellsTrapped = isBearExhaustionTrap;

        let battleWinner = `⚖️ [${tf.toUpperCase()}] CHOP / INDUCEMENT ZONE — DO NOT TRADE YET`;
        let bwCol = '#ffd700';

        if (bullSweepReject) {
            battleWinner = `🎯 [${tf.toUpperCase()}] SSL STOP-HUNT REJECTED + BID ABSORBED ➔ INSTITUTIONAL LONG TRAP-REVERSAL!`;
            bwCol = '#00e5ff';
        } else if (bearSweepReject) {
            battleWinner = `🎯 [${tf.toUpperCase()}] BSL STOP-HUNT REJECTED + ASK CAPPED ➔ INSTITUTIONAL SHORT TRAP-REVERSAL!`;
            bwCol = '#ff9100';
        } else if (lastC.close < lastC.open && isBearExhaustionTrap && clusterLongBuildup) {
            battleWinner = `🛡️ [${tf.toUpperCase()}] FAKE RED DUMP INTO BID ANCHOR (BidAbs:${rBidAbs}) ➔ DO NOT SHORT, LONG HOLD!`;
            bwCol = '#00e5ff';
        } else if (lastC.close > lastC.open && isBullExhaustionTrap && clusterShortBuildup) {
            battleWinner = `🛡️ [${tf.toUpperCase()}] FAKE GREEN PUMP INTO ASK ANCHOR (AskAbs:${rAskAbs}) ➔ DO NOT BUY, SHORT CAP!`;
            bwCol = '#ff9100';
        } else if (instBullDrive) {
            battleWinner = `🚀 [${tf.toUpperCase()}] CONFIRMED BULL EXPANSION (Anchor+🐋Whale+AskPulled)`;
            bwCol = '#00e676';
        } else if (instBearDrive) {
            battleWinner = `🚀 [${tf.toUpperCase()}] CONFIRMED BEAR EXPANSION (Anchor+🐋Whale+BidPulled)`;
            bwCol = '#ff1744';
        } else if (clusterLongBuildup) {
            battleWinner = `🏦 [${tf.toUpperCase()}] INSTITUTIONAL LONG BASE BUILDING (Wait for Low Sweep / POC Hold)`;
            bwCol = '#00e5ff';
        } else if (clusterShortBuildup) {
            battleWinner = `🏦 [${tf.toUpperCase()}] INSTITUTIONAL SHORT CEILING BUILDING (Wait for High Sweep / POC Reject)`;
            bwCol = '#ff9100';
        }

        let scalpPts = anchorBullScore - anchorBearScore;

        if (domPullDelta < -effCellBench * 0.30) scalpPts += 16;
        else if (domPullDelta > effCellBench * 0.30) scalpPts -= 16;

        const minBookFloor = effCellBench * 1.3;
        if (nearBidRest >= minBookFloor && nearBidRest > nearAskRest * 1.30) scalpPts += 10;
        else if (nearAskRest >= minBookFloor && nearAskRest > nearBidRest * 1.30) scalpPts -= 10;

        let deltaState = 'NEU';
        if (isBearExhaustionTrap) { scalpPts += 20; deltaState = 'BULL'; }
        else if (isBullExhaustionTrap) { scalpPts -= 20; deltaState = 'BEAR'; }
        else if (instBullDrive) { scalpPts += 14; deltaState = 'BULL'; }
        else if (instBearDrive) { scalpPts -= 14; deltaState = 'BEAR'; }

        let whaleVsRetailState = 'NEU';
        if (totWhaleBuy > totWhaleSell * 1.25 && !instBearDrive) { scalpPts += 16; whaleVsRetailState = 'BULL'; }
        else if (totWhaleSell > totWhaleBuy * 1.25 && !instBullDrive) { scalpPts -= 16; whaleVsRetailState = 'BEAR'; }

        let locState = 'NEU';
        if (lastC.close >= curPoc && prevC.close >= prevPoc && !isBullExhaustionTrap) { scalpPts += 12; locState = 'BULL'; }
        else if (lastC.close <= curPoc && prevC.close <= prevPoc && !isBearExhaustionTrap) { scalpPts -= 12; locState = 'BEAR'; }

        if (smtState.bias === 1) scalpPts += 10;
        else if (smtState.bias === -1) scalpPts -= 10;

        let chasingWarning = null;
        if (scalpPts > 0 && rangeLocation >= 0.78 && !bullSweepReject) {
            scalpPts = Math.min(scalpPts, 18);
            chasingWarning = '⚠️ AT RANGE HIGH (WAIT PULLBACK)';
        } else if (scalpPts < 0 && rangeLocation <= 0.22 && !bearSweepReject) {
            scalpPts = Math.max(scalpPts, -18);
            chasingWarning = '⚠️ AT RANGE LOW (WAIT BOUNCE)';
        }

        const scalpScoreVal = Math.abs(scalpPts);
        let scalpFinalTxt = `⏳ WAIT / NO CLEAR EDGE`;
        let scalpFinalCol = '#ffca28';

        if (chasingWarning) {
            scalpFinalTxt = chasingWarning;
            scalpFinalCol = '#ffab00';
        } else if (scalpPts >= 46 && (bullSweepReject || instBullDrive || (clusterLongBuildup && lastC.close >= curPoc))) {
            scalpFinalTxt = `🟢 CONFIRMED LONG (Score:+${scalpScoreVal})`;
            scalpFinalCol = '#00e676';
        } else if (scalpPts >= 22) {
            scalpFinalTxt = lastC.close < lastC.open
                ? `🛡️ BULL BASE: RED PULLBACK ABSORBING`
                : `🟡 LONG BUILDING (WAIT TRIGGER)`;
            scalpFinalCol = '#00e5ff';
        } else if (scalpPts <= -46 && (bearSweepReject || instBearDrive || (clusterShortBuildup && lastC.close <= curPoc))) {
            scalpFinalTxt = `🔴 CONFIRMED SHORT (Score:-${scalpScoreVal})`;
            scalpFinalCol = '#ff1744';
        } else if (scalpPts <= -22) {
            scalpFinalTxt = lastC.close > lastC.open
                ? `🛡️ BEAR CEILING: GREEN BOUNCE CAPPED`
                : `🟠 SHORT BUILDING (WAIT TRIGGER)`;
            scalpFinalCol = '#ff9100';
        }

        let intraPts = macroInfo ? macroInfo.netMacro : 0;
        if (sPocUp > sPocDn) intraPts += 14;
        else if (sPocDn > sPocUp) intraPts -= 14;
        if (smtState.bias === 1) intraPts += 12;
        else if (smtState.bias === -1) intraPts -= 12;

        const intraScoreVal = Math.abs(intraPts);
        let intraFinalTxt = `⏳ ${tf.toUpperCase()} ROTATIONAL RANGE`;
        let intraFinalCol = '#ffca28';
        if (intraPts >= 46) { intraFinalTxt = `🟢 MACRO LONG CAMPAIGN (Score:+${intraScoreVal})`; intraFinalCol = '#00e676'; }
        else if (intraPts >= 22) { intraFinalTxt = `🏦 ACCUMULATING LONG (Score:+${intraScoreVal})`; intraFinalCol = '#00e5ff'; }
        else if (intraPts <= -46) { intraFinalTxt = `🔴 MACRO SHORT CAMPAIGN (Score:-${intraScoreVal})`; intraFinalCol = '#ff1744'; }
        else if (intraPts <= -22) { intraFinalTxt = `🏦 DISTRIBUTING SHORT (Score:-${intraScoreVal})`; intraFinalCol = '#ff9100'; }

        const execVsAbsState = instBullDrive ? 'BULL' : (instBearDrive ? 'BEAR' : (clusterLongBuildup ? 'BULL' : (clusterShortBuildup ? 'BEAR' : 'NEU')));
        const execVsAbsLbl = isBearExhaustionTrap ? 'FakeRedDump(Absorbed🟢)' : (isBullExhaustionTrap ? 'FakeGreenPump(Capped🔴)' : (clusterLongBuildup ? `BullAnchor(+${anchorBullScore})` : (clusterShortBuildup ? `BearAnchor(-${anchorBearScore})` : 'NeutralFight')));

        const scalpMeterList = [
            { name: '1. 6-Candle Institutional Anchor vs Trap', state: execVsAbsState, val: execVsAbsLbl },
            { name: '2. Multi-Row Cluster + Iceberg Abs', state: (rBidAbs > rAskAbs || totIceBid > totIceAsk * 1.25) ? 'BULL' : ((rAskAbs > rBidAbs || totIceAsk > totIceBid * 1.25) ? 'BEAR' : 'NEU'), val: `BidClus:${rBidAbs}(🧊${fmt(totIceBid)}) vs AskClus:${rAskAbs}(🧊${fmt(totIceAsk)})` },
            { name: '3. Raw Flash + 10s Spoof Pull (PΔ)', state: domPullDelta < -effCellBench * 0.30 ? 'BULL' : (domPullDelta > effCellBench * 0.30 ? 'BEAR' : 'NEU'), val: `RawPΔ:${domPullDelta >= 0 ? '+' : ''}${fmt(domPullDelta)}` },
            { name: '4. 8-Step Cluster Resting Book Depth', state: (nearBidRest >= minBookFloor && nearBidRest > nearAskRest * 1.30) ? 'BULL' : ((nearAskRest >= minBookFloor && nearAskRest > nearBidRest * 1.30) ? 'BEAR' : 'NEU'), val: `Bid:${fmt(nearBidRest)} vs Ask:${fmt(nearAskRest)}` },
            { name: '5. Exhaustion Climax / Delta Trap', state: deltaState, val: `Δ:${lastC.delta >= 0 ? '+' : ''}${fmt(lastC.delta)}` },
            { name: '6. Two-Candle POC Structure Control', state: locState, val: `Px:${lastC.close.toFixed(dec)} vs POC:${curPoc.toFixed(dec)}` },
            { name: '7. Behavioral Whale (🐋) vs Retail (🐑)', state: whaleVsRetailState, val: `🐋W B:${fmt(totWhaleBuy)}/S:${fmt(totWhaleSell)}` },
            { name: '8. Range Location Safety (Anti-Chase)', state: rangeLocation <= 0.35 ? 'BULL' : (rangeLocation >= 0.65 ? 'BEAR' : 'NEU'), val: `Loc:${Math.round(rangeLocation * 100)}% (Safe:22-78%)` },
            { name: '9. 4-Candle POC Stepping Drift', state: sPocUp > sPocDn ? 'BULL' : (sPocDn > sPocUp ? 'BEAR' : 'NEU'), val: `Up:${sPocUp}↑ vs Dn:${sPocDn}↓` },
            { name: '10. Boundary Stop-Hunt Sweep (BSL/SSL)', state: bullSweepReject ? 'BULL' : (bearSweepReject ? 'BEAR' : 'NEU'), val: bullSweepReject ? 'SSL-Sweep🟢' : (bearSweepReject ? 'BSL-Sweep🔴' : 'None') },
            { name: '11. SMT Divergence Radar', state: smtState.bias === 1 ? 'BULL' : (smtState.bias === -1 ? 'BEAR' : 'NEU'), val: smtState.text },
            { name: '12. RVOL Institutional Participation', state: curRvol >= 0.85 ? execVsAbsState : 'NEU', val: `${curRvol.toFixed(1)}x Avg` }
        ];

        const sessionPocsList = candles.slice(-15).map(x => x.poc).filter(x => x != null);
        const avgSessPoc = sessionPocsList.length > 0 ? sessionPocsList.reduce((a, b) => a + b, 0) / sessionPocsList.length : lastC.close;
        const ubHiCnt = unfinishedLevels.filter(u => u.type === 'UB-HIGH' && u.price > lastC.close).length;
        const ubLoCnt = unfinishedLevels.filter(u => u.type === 'UB-LOW' && u.price < lastC.close).length;

        const intraMeterList = [
            { name: '1. Session Cumulative Delta (CVD)', state: (macroInfo && macroInfo.totalCvd > 0) ? 'BULL' : ((macroInfo && macroInfo.totalCvd < 0) ? 'BEAR' : 'NEU'), val: `CVD:${macroInfo ? fmt(macroInfo.totalCvd) : 0}` },
            { name: '2. Multi-Swing Cluster Absorption', state: (macroInfo && macroInfo.macroBidAbsorbs > macroInfo.macroAskAbsorbs) ? 'BULL' : ((macroInfo && macroInfo.macroAskAbsorbs > macroInfo.macroBidAbsorbs) ? 'BEAR' : 'NEU'), val: `BidClus:${macroInfo ? macroInfo.macroBidAbsorbs : 0} vs AskClus:${macroInfo ? macroInfo.macroAskAbsorbs : 0}` },
            { name: '3. 4-Candle Structure & POC Drift', state: sPocUp > sPocDn ? 'BULL' : (sPocDn > sPocUp ? 'BEAR' : 'NEU'), val: `POC Up:${sPocUp}↑ vs Dn:${sPocDn}↓` },
            { name: '4. Session Wall Pulling Campaign (PΔ)', state: (macroInfo && macroInfo.macroPullDelta < 0) ? 'BULL' : ((macroInfo && macroInfo.macroPullDelta > 0) ? 'BEAR' : 'NEU'), val: `SessPΔ:${macroInfo ? fmt(macroInfo.macroPullDelta) : 0}` },
            { name: '5. Session Whale Net Inventory (🐋)', state: (macroInfo && macroInfo.sessWhaleBuyVol > macroInfo.sessWhaleSellVol) ? 'BULL' : ((macroInfo && macroInfo.sessWhaleSellVol > macroInfo.sessWhaleBuyVol) ? 'BEAR' : 'NEU'), val: `🐋B:${macroInfo ? fmt(macroInfo.sessWhaleBuyVol) : 0} vs 🐋S:${macroInfo ? fmt(macroInfo.sessWhaleSellVol) : 0}` },
            { name: '6. Price vs Session Equilibrium POC', state: (lastC.close > avgSessPoc + domStep) ? 'BULL' : ((lastC.close < avgSessPoc - domStep) ? 'BEAR' : 'NEU'), val: `AvgPOC:${avgSessPoc.toFixed(dec)}` },
            { name: '7. Unfinished Business Magnet Draw', state: ubHiCnt > ubLoCnt ? 'BULL' : (ubLoCnt > ubHiCnt ? 'BEAR' : 'NEU'), val: `UB-HI:${ubHiCnt} vs UB-LO:${ubLoCnt}` },
            { name: '8. Untouched nPOC Gravity', state: nakedPocs.filter(n => n.price > lastC.close).length > nakedPocs.filter(n => n.price < lastC.close).length ? 'BULL' : (nakedPocs.filter(n => n.price < lastC.close).length > nakedPocs.filter(n => n.price > lastC.close).length ? 'BEAR' : 'NEU'), val: `nPOCs:${nakedPocs.length}` },
            { name: '9. Orderbook Macro Depth Support', state: (nearBidRest >= minBookFloor && nearBidRest > nearAskRest * 1.30) ? 'BULL' : ((nearAskRest >= minBookFloor && nearAskRest > nearBidRest * 1.30) ? 'BEAR' : 'NEU'), val: `B:${fmt(nearBidRest)} vs A:${fmt(nearAskRest)}` },
            { name: '10. SMT Divergence Alignment', state: smtState.bias === 1 ? 'BULL' : (smtState.bias === -1 ? 'BEAR' : 'NEU'), val: smtState.text }
        ];

        const sBullCnt = scalpMeterList.filter(x => x.state === 'BULL').length;
        const sBearCnt = scalpMeterList.filter(x => x.state === 'BEAR').length;
        const sNeuCnt = scalpMeterList.filter(x => x.state === 'NEU').length;

        const iBullCnt = intraMeterList.filter(x => x.state === 'BULL').length;
        const iBearCnt = intraMeterList.filter(x => x.state === 'BEAR').length;
        const iNeuCnt = intraMeterList.filter(x => x.state === 'NEU').length;

        if (meterModal && meterModal.style.display === 'block') {
            const renderRows = list => list.map(item => {
                const badge = item.state === 'BULL'
                    ? '<span style="color:#00e676;font-weight:bold;">🟢 BULL</span>'
                    : (item.state === 'BEAR' ? '<span style="color:#ff5252;font-weight:bold;">🔴 BEAR</span>' : '<span style="color:#ffca28;">⚪ NEU</span>');
                return `<div style="display:flex;justify-content:space-between;border-bottom:1px solid #1e2638;padding:4px 0;font-size:11px;">
                    <span>${item.name}</span>
                    <span><code style="color:#90a4ae;margin-right:6px;">${item.val}</code>${badge}</span>
                </div>`;
            }).join('');

            document.getElementById('meterModalBody').innerHTML = `
                <div class="rule-sec" style="border-color:${scalpFinalCol};">
                    <h4 style="color:${scalpFinalCol};">⚡ [${tf.toUpperCase()}] SCALP & CLUSTER METER: ${scalpFinalTxt} (🟢${sBullCnt} Bull | 🔴${sBearCnt} Bear | ⚪${sNeuCnt} Neu)</h4>
                    ${renderRows(scalpMeterList)}
                </div>
                <div class="rule-sec" style="border-color:${intraFinalCol};">
                    <h4 style="color:${intraFinalCol};">🌊 [${tf.toUpperCase()}] MACRO CAMPAIGN METER: ${intraFinalTxt} (🟢${iBullCnt} Bull | 🔴${iBearCnt} Bear | ⚪${iNeuCnt} Neu)</h4>
                    ${renderRows(intraMeterList)}
                </div>
            `;
        }

        latestEngineSummary = {
            symbol: symbol,
            timeframe: tf,
            live_price: lastC.close,
            current_candle: {
                open: lastC.open,
                high: lastC.high,
                low: lastC.low,
                close: lastC.close,
                delta: Math.round(lastC.delta || 0),
                volume: Math.round(lastC.volume || 0),
                rvol: Number(curRvol.toFixed(2)),
                poc: curPoc,
                vah: lastC.vah,
                val: lastC.val
            },
            dom_and_clusters: {
                battle_verdict: battleWinner,
                anchor_bull_score: anchorBullScore,
                anchor_bear_score: anchorBearScore,
                exec_buy_3c: Math.round(totExecBuy),
                exec_sell_3c: Math.round(totExecSell),
                whale_buy_vol: Math.round(totWhaleBuy),
                whale_sell_vol: Math.round(totWhaleSell),
                retail_buy_vol: Math.round(totRetailBuy),
                retail_sell_vol: Math.round(totRetailSell),
                bid_cluster_absorptions: rBidAbs,
                ask_cluster_absorptions: rAskAbs,
                iceberg_bid_reloads: Math.round(totIceBid),
                iceberg_ask_reloads: Math.round(totIceAsk),
                bid_pulled_10s_plus_flash: Math.round(totPullB),
                ask_pulled_10s_plus_flash: Math.round(totPullA),
                pull_delta: Math.round(domPullDelta),
                near_resting_bid_wall: Math.round(nearBidRest),
                near_resting_ask_wall: Math.round(nearAskRest)
            },
            magnets: {
                untouched_naked_pocs: nakedPocs.slice(-5).map(n => n.price),
                unfinished_business: unfinishedLevels.slice(-5).map(u => `${u.type}@${u.price}`)
            },
            scalp_10factor_verdict: {
                final_bias: scalpFinalTxt,
                quant_score: scalpPts,
                bull_factors: sBullCnt,
                bear_factors: sBearCnt,
                neutral_factors: sNeuCnt,
                parameters: scalpMeterList
            },
            intraday_10factor_verdict: {
                final_bias: intraFinalTxt,
                quant_score: intraPts,
                bull_factors: iBullCnt,
                bear_factors: iBearCnt,
                neutral_factors: iNeuCnt,
                session_cvd: macroInfo ? Math.round(macroInfo.totalCvd) : 0,
                parameters: intraMeterList
            }
        };

        if (!showDOM || domW <= 0) return;

        const domX = chartW + yAxisW;
        ctx.fillStyle = '#0a0d14';
        ctx.fillRect(domX, 0, domW, H);
        ctx.strokeStyle = '#00e5ff';
        ctx.lineWidth = 1;
        ctx.beginPath(); ctx.moveTo(domX, 0); ctx.lineTo(domX, H); ctx.stroke();

        const headerH = 92;
        ctx.fillStyle = '#141926';
        ctx.fillRect(domX + 1, 0, domW - 1, headerH);
        ctx.strokeStyle = '#263147';
        ctx.beginPath(); ctx.moveTo(domX, headerH); ctx.lineTo(domX + domW, headerH); ctx.stroke();

        ctx.font = 'bold 9px monospace';
        ctx.textAlign = 'left';
        ctx.textBaseline = 'middle';
        ctx.fillStyle = '#00e5ff';
        ctx.fillText(`🐋W:${fmt(totWhaleBuy)}B/${fmt(totWhaleSell)}S | 🐑R:${fmt(totRetailBuy)}B/${fmt(totRetailSell)}S | 🧊Ice:${fmt(totIceBid)}B/${fmt(totIceAsk)}A`, domX + 6, 11);

        ctx.fillStyle = bwCol;
        ctx.fillText(battleWinner, domX + 6, 25);

        const boxGap = 6;
        const halfBoxW = Math.floor((domW - 14 - boxGap) * 0.5);
        const b1X = domX + 6;
        const b2X = b1X + halfBoxW + boxGap;
        const bY = 35;
        const bH = 36;

        ctx.fillStyle = '#0e131d';
        ctx.fillRect(b1X, bY, halfBoxW, bH);
        ctx.strokeStyle = scalpFinalCol;
        ctx.lineWidth = 1.2;
        ctx.strokeRect(b1X, bY, halfBoxW, bH);

        ctx.textAlign = 'center';
        ctx.font = 'bold 8px monospace';
        ctx.fillStyle = '#cfd8dc';
        ctx.fillText(`⚡${tf.toUpperCase()} CLUSTER [🟢${sBullCnt} 🔴${sBearCnt} ⚪${sNeuCnt}]`, b1X + halfBoxW * 0.5, bY + 10);
        ctx.font = 'bold 9.5px monospace';
        ctx.fillStyle = scalpFinalCol;
        ctx.fillText(scalpFinalTxt, b1X + halfBoxW * 0.5, bY + 25);

        ctx.fillStyle = '#0e131d';
        ctx.fillRect(b2X, bY, halfBoxW, bH);
        ctx.strokeStyle = intraFinalCol;
        ctx.lineWidth = 1.2;
        ctx.strokeRect(b2X, bY, halfBoxW, bH);

        ctx.font = 'bold 8px monospace';
        ctx.fillStyle = '#cfd8dc';
        ctx.fillText(`🌊${tf.toUpperCase()} MACRO [🟢${iBullCnt} 🔴${iBearCnt} ⚪${iNeuCnt}]`, b2X + halfBoxW * 0.5, bY + 10);
        ctx.font = 'bold 9.5px monospace';
        ctx.fillStyle = intraFinalCol;
        ctx.fillText(intraFinalTxt, b2X + halfBoxW * 0.5, bY + 25);

        const pxScaleW = isMobile ? 64 : 78;
        const battleAreaW = domW - pxScaleW;
        const halfBattleW = battleAreaW * 0.5;
        const domSpineX = domX + halfBattleW;
        const domPxScaleX = domX + battleAreaW;

        ctx.fillStyle = '#90a4ae';
        ctx.font = 'bold 8px monospace';
        ctx.textAlign = 'center';
        ctx.fillText('◀ BID [🐋W/🐑R | 🧊ICE | ⚡PULL]', domX + halfBattleW * 0.5, 82);
        ctx.fillText('ASK [🐋W/🐑R | 🧊ICE | ⚡PULL] ▶', domSpineX + halfBattleW * 0.5, 82);
        ctx.fillStyle = '#ffeb3b';
        ctx.fillText('PRICE', domPxScaleX + pxScaleW * 0.5, 82);

        const rH = 24;
        const maxVisDomRows = Math.max(1, Math.floor((H - headerH - 6) / rH));
        const startIdx = Math.max(0, Math.min(Math.max(0, activeDomRows.length - maxVisDomRows), Math.floor((activeDomRows.length - maxVisDomRows) * 0.5 - domScrollRows)));
        const visibleSlice = activeDomRows.slice(startIdx, startIdx + maxVisDomRows);

        let ry = headerH + 2;

        visibleSlice.forEach((row, idx) => {
            const { p, bRest, aRest, st, rawD, totRawBp, totRawAp, mag } = row;
            const isCurrentRow = Math.abs(p - liveCenterP) < domStep * 0.4;

            ctx.fillStyle = isCurrentRow ? 'rgba(255, 235, 59, 0.16)' : (idx % 2 === 0 ? '#0c1018' : '#101520');
            ctx.fillRect(domX + 1, ry, domW - 1, rH);

            const bSpoofPct = (totRawBp + st.sell) > 0 ? Math.round((totRawBp / (totRawBp + st.sell)) * 100) : 0;
            const aSpoofPct = (totRawAp + st.buy) > 0 ? Math.round((totRawAp / (totRawAp + st.buy)) * 100) : 0;
            const rowPDelta = totRawBp - totRawAp;

            const execSellRatio = Math.min(1, st.sell / maxRowVal);
            if (st.sell > 0) {
                ctx.fillStyle = 'rgba(255, 23, 68, 0.55)';
                ctx.fillRect(domSpineX - halfBattleW * execSellRatio, ry + 2, halfBattleW * execSellRatio, 10);
            }
            const pullBidRatio = Math.min(1, totRawBp / maxRowVal);
            if (totRawBp > 0) {
                ctx.fillStyle = 'rgba(255, 64, 129, 0.40)';
                ctx.fillRect(domSpineX - halfBattleW * pullBidRatio, ry + 12, halfBattleW * pullBidRatio, 10);
            }

            ctx.textAlign = 'right';
            ctx.textBaseline = 'middle';
            ctx.font = 'bold 8px monospace';
            ctx.fillStyle = '#ff8a80';
            const wSellHere = Math.max(st.wSell, rawD.wSell);
            const winBidTag = (rawD.iceBid > effCellBench * 0.35 || (st.sell > st.buy * 1.4 && lastC.close >= p + domStep * 0.4)) ? '🧊ABS ' : '';
            const wSellTag = wSellHere > 0 ? `🐋${fmt(wSellHere)} ` : '';
            ctx.fillText(`${winBidTag}${wSellTag}S:${fmt(st.sell)} (Rest:${fmt(bRest)})`, domSpineX - 4, ry + 7);

            if (totRawBp > 0 || rawD.iceBid > 0) {
                ctx.font = bSpoofPct >= 55 ? 'bold 7.5px monospace' : '7.5px monospace';
                ctx.fillStyle = rawD.flashBp > 0 ? '#ff4081' : '#ff80ab';
                const fTag = rawD.flashBp > 0 ? `⚡F:${fmt(rawD.flashBp)} ` : '';
                const iceTag = rawD.iceBid > 0 ? `🧊+${fmt(rawD.iceBid)} ` : '';
                ctx.fillText(`${iceTag}${fTag}Pull:${fmt(totRawBp)}(${bSpoofPct}%)`, domSpineX - 4, ry + 18);
            }

            const execBuyRatio = Math.min(1, st.buy / maxRowVal);
            if (st.buy > 0) {
                ctx.fillStyle = 'rgba(0, 230, 118, 0.55)';
                ctx.fillRect(domSpineX, ry + 2, halfBattleW * execBuyRatio, 10);
            }
            const pullAskRatio = Math.min(1, totRawAp / maxRowVal);
            if (totRawAp > 0) {
                ctx.fillStyle = 'rgba(224, 64, 251, 0.40)';
                ctx.fillRect(domSpineX, ry + 12, halfBattleW * pullAskRatio, 10);
            }

            ctx.textAlign = 'left';
            ctx.font = 'bold 8px monospace';
            ctx.fillStyle = '#69f0ae';
            const wBuyHere = Math.max(st.wBuy, rawD.wBuy);
            const winAskTag = (rawD.iceAsk > effCellBench * 0.35 || (st.buy > st.sell * 1.4 && lastC.close <= p - domStep * 0.4)) ? ' 🧊CAP' : '';
            const wBuyTag = wBuyHere > 0 ? ` 🐋${fmt(wBuyHere)}` : '';
            ctx.fillText(`B:${fmt(st.buy)}${wBuyTag} (Rest:${fmt(aRest)})${winAskTag}`, domSpineX + 4, ry + 7);

            if (totRawAp > 0 || rawD.iceAsk > 0) {
                ctx.font = aSpoofPct >= 55 ? 'bold 7.5px monospace' : '7.5px monospace';
                ctx.fillStyle = rawD.flashAp > 0 ? '#e040fb' : '#ea80fc';
                const fTag = rawD.flashAp > 0 ? `⚡F:${fmt(rawD.flashAp)} ` : '';
                const iceTag = rawD.iceAsk > 0 ? `🧊+${fmt(rawD.iceAsk)} ` : '';
                ctx.fillText(`${iceTag}${fTag}Pull:${fmt(totRawAp)}(${aSpoofPct}%)`, domSpineX + 4, ry + 18);
            }

            ctx.fillStyle = isCurrentRow ? '#ffeb3b' : (mag ? 'rgba(224, 64, 251, 0.22)' : '#161c2b');
            ctx.fillRect(domPxScaleX, ry, pxScaleW, rH);

            ctx.textAlign = 'center';
            ctx.font = isCurrentRow ? 'bold 9.5px monospace' : 'bold 9px monospace';
            ctx.fillStyle = isCurrentRow ? '#000' : (p > lastC.close ? '#ff8a80' : '#69f0ae');
            ctx.fillText(p.toFixed(dec), domPxScaleX + pxScaleW * 0.5, ry + 7);

            let subLbl = '';
            let subCol = isCurrentRow ? '#000' : '#ffd700';
            if (mag) {
                subLbl = mag.lbl; subCol = isCurrentRow ? '#000' : '#ffeb3b';
            } else if (rawD.iceBid > rawD.iceAsk && rawD.iceBid > effCellBench * 0.3) {
                subLbl = '🧊BID CLUS'; subCol = isCurrentRow ? '#004d40' : '#00e5ff';
            } else if (rawD.iceAsk > rawD.iceBid && rawD.iceAsk > effCellBench * 0.3) {
                subLbl = '🧊ASK CLUS'; subCol = isCurrentRow ? '#b71c1c' : '#ff9100';
            } else if (st.buy > st.sell * 1.3) {
                subLbl = '🟢BUY WIN'; subCol = isCurrentRow ? '#004d40' : '#00e676';
            } else if (st.sell > st.buy * 1.3) {
                subLbl = '🔴SELL WIN'; subCol = isCurrentRow ? '#b71c1c' : '#ff5252';
            } else if (Math.abs(rowPDelta) > 1) {
                subLbl = `PΔ:${rowPDelta > 0 ? '+' : ''}${fmt(rowPDelta)}`; subCol = isCurrentRow ? '#000' : '#ea80fc';
            }

            if (subLbl) {
                ctx.font = 'bold 7.5px monospace';
                ctx.fillStyle = subCol;
                ctx.fillText(subLbl, domPxScaleX + pxScaleW * 0.5, ry + 18);
            }

            ctx.strokeStyle = '#1b2234';
            ctx.beginPath(); ctx.moveTo(domX, ry + rH); ctx.lineTo(domX + domW, ry + rH); ctx.stroke();

            ry += rH;
        });

        ctx.strokeStyle = '#00e5ff';
        ctx.lineWidth = 1.5;
        ctx.beginPath();
        ctx.moveTo(domSpineX, headerH); ctx.lineTo(domSpineX, ry);
        ctx.stroke();

        ctx.strokeStyle = '#2c354d';
        ctx.lineWidth = 1;
        ctx.beginPath();
        ctx.moveTo(domPxScaleX, headerH); ctx.lineTo(domPxScaleX, H);
        ctx.stroke();
    } catch (domErr) {
        console.error("DOM render error:", domErr);
        if (showDOM && domW > 0) {
            const domX = chartW + yAxisW;
            ctx.fillStyle = '#1a0f14';
            ctx.fillRect(domX, 0, domW, H);
            ctx.fillStyle = '#ff5252';
            ctx.font = 'bold 11px monospace';
            ctx.textAlign = 'left';
            ctx.fillText('⚠️ DOM ERR: ' + domErr.message, domX + 8, 30);
        }
    }
}

// SMART BANDWIDTH-SAVER POLLING: Auto-pauses when browser tab is minimized!
setInterval(() => {
    if (!document.hidden) fetchData();
}, 3200);
setInterval(() => {
    if (!document.hidden) fetchSmtPeer();
}, 15000);
document.addEventListener('visibilitychange', () => {
    if (!document.hidden) {
        fetchData();
        fetchPaperStatus();
    }
});
fetchData();
fetchSmtPeer();
requestAnimationFrame(drawLoop);
</script>
</body>
</html>"""


@app.route("/hello")
def hello():
    return "SA Footprint Engine OK"


@app.route("/api/test")
def api_test():
    return jsonify({"status": "ok", "message": "API working", "version": "V5"})


@app.route("/api/db-test")
def db_test():
    if not TURSO_DATABASE_URL:
        return jsonify({"ok": False, "error": "Turso env variables missing"}), 500
    try:
        with create_client_sync(TURSO_DATABASE_URL, auth_token=TURSO_AUTH_TOKEN) as client:
            result = client.execute("SELECT COUNT(*) AS count FROM candles")
            count = result.rows[0][0]
            return jsonify({"ok": True, "turso": "connected", "candles": count})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/status")
def api_status():
    with lock:
        symbol_status = {
            s: {
                "last_trade_time": last_trade[s]["time"],
                "price": last_trade[s]["price"],
                "quantity": last_trade[s]["quantity"],
            }
            for s in SYMBOLS
        }
        return jsonify({
            "status": "ok",
            "version": "V5",
            "rolling_hours": 48,
            "symbols": symbol_status,
            "timeframes": list(TIMEFRAMES.keys()),
            "price_steps": PRICE_STEP,
            "collector": dict(collector_state),
            "process": {
                "thread_alive": collector_thread is not None and collector_thread.is_alive(),
                "thread_name": collector_thread.name if collector_thread else None,
            },
            "orderbook": {
                s: {
                    "synchronized": orderbook[s]["initialized"],
                    "resyncing": orderbook[s]["resyncing"],
                    "last_update_id": orderbook[s]["last_update_id"],
                    "sequence_errors": orderbook[s]["sequence_errors"],
                    "resync_count": orderbook[s]["resync_count"],
                    "bid_count": len(orderbook[s]["bids"]),
                    "ask_count": len(orderbook[s]["asks"]),
                }
                for s in SYMBOLS
            },
        })


@app.route("/api/candles")
def api_candles():
    symbol = request.args.get("symbol", "BTCUSDT").upper()
    timeframe = request.args.get("timeframe", "1m").lower()
    try:
        limit = max(1, min(int(request.args.get("limit", 100)), 500))
    except ValueError:
        limit = 100

    if symbol not in SYMBOLS or timeframe not in TIMEFRAMES:
        return jsonify({"status": "error", "message": "Invalid symbol or timeframe"}), 400

    with lock:
        finished = list(candles[symbol][timeframe])[-limit:]
        current = current_candles[symbol][timeframe]
        if current is not None:
            current_final = finalize_candle(current)
            if not finished or current_final["start"] != finished[-1]["start"]:
                finished.append(current_final)

        return jsonify({
            "status": "ok",
            "symbol": symbol,
            "timeframe": timeframe,
            "count": len(finished),
            "candles": finished,
        })


@app.route("/api/footprint")
def api_footprint():
    symbol = request.args.get("symbol", "BTCUSDT").upper()
    timeframe = request.args.get("timeframe", "1m").lower()
    if symbol not in SYMBOLS or timeframe not in TIMEFRAMES:
        return jsonify({"status": "error", "message": "Invalid symbol or timeframe"}), 400
    try:
        start = int(request.args.get("start"))
    except (TypeError, ValueError):
        return jsonify({"status": "error", "message": "Provide candle start timestamp"}), 400

    with lock:
        for c in candles[symbol][timeframe]:
            if c["start"] == start:
                return jsonify({"status": "ok", "symbol": symbol, "timeframe": timeframe, "candle": c})
        current = current_candles[symbol][timeframe]
        if current is not None and current["start"] == start:
            return jsonify({
                "status": "ok",
                "symbol": symbol,
                "timeframe": timeframe,
                "candle": finalize_candle(current),
            })

    return jsonify({"status": "error", "message": "Candle not found"}), 404


@app.route("/api/snapshot")
def api_snapshot():
    symbol = request.args.get("symbol", "BTCUSDT").upper()
    timeframe = request.args.get("timeframe", "1m").lower()
    if symbol not in SYMBOLS or timeframe not in TIMEFRAMES:
        return jsonify({"status": "error", "message": "Invalid parameters"}), 400

    with lock:
        current = current_candles[symbol][timeframe]
        return jsonify({
            "status": "ok",
            "symbol": symbol,
            "timeframe": timeframe,
            "current_candle": finalize_candle(current) if current else None,
        })


@app.route("/api/orderbook")
def api_orderbook():
    symbol = request.args.get("symbol", "BTCUSDT").upper()
    if symbol not in SYMBOLS:
        return jsonify({"status": "error", "message": "Invalid symbol"}), 400
    try:
        limit = max(1, min(int(request.args.get("limit", 500)), 1000))
    except ValueError:
        limit = 500

    with lock:
        state = orderbook[symbol]
        live_px = last_trade.get(symbol, {}).get("price")
        step = PRICE_STEP.get(symbol, 1.0)
        bids = sorted(state["bids"].items(), key=lambda x: float(x[0]), reverse=True)[:limit]
        asks = sorted(state["asks"].items(), key=lambda x: float(x[0]))[:limit]

        raw_list = []
        for px_k, d_obj in list(dom_raw_telemetry[symbol].items()):
            if live_px is None or abs(float(px_k) - float(live_px)) <= step * 60.0:
                raw_list.append({"price": float(px_k), **d_obj})

        return jsonify({
            "status": "ok",
            "symbol": symbol,
            "synchronized": state["initialized"],
            "resyncing": state["resyncing"],
            "last_update_id": state["last_update_id"],
            "last_depth_update_id": state["last_depth_update_id"],
            "last_depth_event_time": state["last_depth_event_time"],
            "sequence_errors": state["sequence_errors"],
            "resync_count": state["resync_count"],
            "bid_count": len(state["bids"]),
            "ask_count": len(state["asks"]),
            "bids": [{"price": float(p), "quantity": q} for p, q in bids],
            "asks": [{"price": float(p), "quantity": q} for p, q in asks],
            "dom_raw": raw_list,
        })


@app.route("/api/liquidity-debug")
def liquidity_debug():
    symbol = request.args.get("symbol", "BTCUSDT").upper()
    if symbol not in orderbook:
        return jsonify({"error": "invalid_symbol", "symbol": symbol}), 400

    with lock:
        finalize_liquidity_records(symbol)
        state = orderbook[symbol]
        history = [r for r in state["liquidity_history"] if isinstance(r, dict)]

        finalized = [r for r in history if r.get("finalized") is True]
        executed = [r for r in finalized if float(r.get("executed_qty") or 0) > 0]
        pulled = [r for r in finalized if float(r.get("pulled_qty") or 0) > 0]
        reductions = [r for r in history if float(r.get("reduced_qty") or 0) > 0]

        def short_record(r):
            return {
                "time": r.get("time"),
                "price": r.get("price"),
                "side": r.get("side"),
                "reduced_qty": r.get("reduced_qty"),
                "executed_qty": r.get("executed_qty"),
                "aged_consumed_qty": r.get("aged_consumed_qty"),
                "aged_executed_qty": r.get("aged_executed_qty"),
                "aged_unmatched_qty": r.get("aged_unmatched_qty"),
                "pulled_qty": r.get("pulled_qty"),
                "status": r.get("status"),
                "finalized": r.get("finalized"),
            }

        return jsonify({
            "symbol": symbol,
            "orderbook": {
                "initialized": state.get("initialized"),
                "synchronized": state.get("initialized"),
                "resyncing": state.get("resyncing"),
                "sequence_errors": state.get("sequence_errors", 0),
                "bid_levels": len(state.get("bids", {})),
                "ask_levels": len(state.get("asks", {})),
            },
            "summary": {
                "history_records": len(history),
                "finalized_records": len(finalized),
                "reduction_records": len(reductions),
                "finalized_with_execution": len(executed),
                "finalized_with_pull": len(pulled),
            },
            "execution_examples": [short_record(r) for r in executed[-3:]],
            "recent_reductions": [short_record(r) for r in reductions[-3:]],
        })


@app.route("/api/scan")
def api_scan():
    symbol = request.args.get("symbol", "BTCUSDT").upper()
    if symbol not in SYMBOLS:
        return jsonify({"status": "error", "message": "Invalid symbol"}), 400

    step = PRICE_STEP.get(symbol, 1.0)
    bid_clusters = defaultdict(float)
    ask_clusters = defaultdict(float)

    with lock:
        for p, q in orderbook[symbol]["bids"].items():
            bid_clusters[round_price_to_step(float(p), step)] += q
        for p, q in orderbook[symbol]["asks"].items():
            ask_clusters[round_price_to_step(float(p), step)] += q

        current_candle = current_candles[symbol]["1m"]
        footprint_data = current_candle["footprint"] if current_candle else {}

    HEAVY_ORDER = 5.0
    MIN_EXECUTION = 1.0

    bids_out = []
    for p, q in sorted(bid_clusters.items(), reverse=True)[:20]:
        executed = footprint_data.get(str(p), {}).get("volume", 0.0)
        delta = footprint_data.get(str(p), {}).get("delta", 0.0)
        prev_q = previous_cluster_state[symbol]["bids"].get(p, 0.0)

        status = "NORMAL"
        if prev_q >= HEAVY_ORDER and q < (prev_q * 0.2) and executed < MIN_EXECUTION:
            status = "LIQUIDITY PULLED (TRAP)"
        elif executed >= HEAVY_ORDER:
            status = "ABSORPTION"

        bids_out.append({
            "price_zone": p,
            "resting_liquidity": round(q, 3),
            "executed_volume": round(executed, 3),
            "delta": round(delta, 3),
            "status": status,
        })
        previous_cluster_state[symbol]["bids"][p] = q

    asks_out = []
    for p, q in sorted(ask_clusters.items())[:20]:
        executed = footprint_data.get(str(p), {}).get("volume", 0.0)
        delta = footprint_data.get(str(p), {}).get("delta", 0.0)
        prev_q = previous_cluster_state[symbol]["asks"].get(p, 0.0)

        status = "NORMAL"
        if prev_q >= HEAVY_ORDER and q < (prev_q * 0.2) and executed < MIN_EXECUTION:
            status = "LIQUIDITY PULLED (TRAP)"
        elif executed >= HEAVY_ORDER:
            status = "ABSORPTION"

        asks_out.append({
            "price_zone": p,
            "resting_liquidity": round(q, 3),
            "executed_volume": round(executed, 3),
            "delta": round(delta, 3),
            "status": status,
        })
        previous_cluster_state[symbol]["asks"][p] = q

    return jsonify({
        "status": "ok",
        "symbol": symbol,
        "cluster_size": step,
        "bids_zone": bids_out,
        "asks_zone": asks_out,
    })


_news_cache = {"time": 0, "data": []}

def fetch_live_crypto_news(symbol="BTCUSDT"):
    """Fetches latest breaking crypto headlines & Fear/Greed Index (Cached for 60s, Zero Mobile Data!)."""
    now = time.time()
    if now - _news_cache["time"] < 60 and _news_cache["data"]:
        return _news_cache["data"]

    headlines = []
    coin = symbol.replace("USDT", "")
    try:
        url = f"https://min-api.cryptocompare.com/data/v2/news/?lang=EN&categories={coin},BTC,Market,Regulation"
        r = requests.get(url, timeout=4)
        if r.status_code == 200:
            items = r.json().get("Data", [])[:5]
            for it in items:
                title = it.get("title", "")
                src = it.get("source_info", {}).get("name", "News")
                mins_ago = max(1, int((now - int(it.get("published_on", now))) / 60))
                headlines.append(f"[{mins_ago}m ago | {src}] {title}")
    except Exception:
        pass

    try:
        fng_r = requests.get("https://api.alternative.me/fng/?limit=1", timeout=3)
        if fng_r.status_code == 200:
            fd = fng_r.json().get("data", [{}])[0]
            headlines.insert(0, f"[MACRO SENTIMENT] Crypto Fear & Greed Index: {fd.get('value')} ({fd.get('value_classification')})")
    except Exception:
        pass

    if headlines:
        _news_cache["time"] = now
        _news_cache["data"] = headlines
    return headlines or ["No major breaking news in the last hour."]


@app.route("/api/ai-mentor", methods=["POST"])
def api_ai_mentor():
    gemini_key = os.environ.get("GEMINI_API_KEY", "").strip().strip('"').strip("'")
    if not gemini_key:
        return jsonify({"status": "error", "message": "Render Environment me GEMINI_API_KEY nahi mili!"}), 400

    data = request.get_json(silent=True) or {}
    symbol = data.get("symbol", "BTCUSDT")
    timeframe = data.get("timeframe", "1m")
    question = data.get("question", "")
    engine_data = data.get("engine_data", {})

    live_news = fetch_live_crypto_news(symbol)
    news_block = "\n".join(live_news)

    prompt = f"""You are an Institutional Crypto Quant & Orderflow Mentor speaking in sharp, practical Hinglish.
STRICT RULE: DO NOT hallucinate any numbers. Base 100% of your analysis strictly on the JSON telemetry and Live News Headlines below.

Asset: {symbol} ({timeframe})
Trader's Question / TradingView Observation: "{question}"

LIVE BREAKING CRYPTO NEWS & MACRO SENTIMENT:
{news_block}

COMPLETE SA ENGINE, FOOTPRINT ANCHOR & DOM TELEMETRY:
{json.dumps(engine_data, indent=2)}

Analyze ALL parameters and reply in 5 short Hinglish points:
1. ⚖️ FINAL VERDICT & ALIGNMENT: State Scalp Bias ({engine_data.get('scalp_10factor_verdict', {}).get('final_bias')}) & Intraday Bias ({engine_data.get('intraday_10factor_verdict', {}).get('final_bias')}) with Quant Scores. Is the trader's view 🟢 100% ALIGNED, 🔴 TRAP, or ⏳ WAIT FOR TRIGGER?
2. 📰 LIVE NEWS & SENTIMENT IMPACT: Any breaking news risk/tailwind, or pure technical orderflow?
3. ⚔️ DOM & ABSORPTION X-RAY: Exact numbers from 6-Candle Anchor Score, Buy/Sell Executions, 10s Pull Delta (PΔ), Resting Walls, Absorptions, and Behavioral Whale Lots.
4. 🎯 EXACT SCALP PLAN: Entry Zone, Invalidation/SL level, and Target (using active UB-HIGH/LOW or nPOC magnets).
5. 🌊 INTRADAY ROADMAP: Session CVD & Big Player campaign direction."""

    try:
        list_url = f"https://generativelanguage.googleapis.com/v1beta/models?key={gemini_key}"
        list_resp = requests.get(list_url, timeout=8)
        if list_resp.status_code != 200:
            return jsonify({"status": "error", "message": f"Gemini Model list error: {list_resp.text}"}), 400

        models_json = list_resp.json()
        available_models = []
        for m in models_json.get("models", []):
            name = m.get("name", "")
            methods = m.get("supportedGenerationMethods", [])
            if "generateContent" in methods and "2.5" not in name and "omni" not in name.lower():
                available_models.append(name)

        def orig_priority(x):
            xl = x.lower()
            if "gemini-flash-lite-latest" in xl or "flash-lite" in xl:
                return 0
            if "flash" in xl:
                return 1
            return 2

        available_models.sort(key=orig_priority)

        reply_text = None
        used_model = ""
        for model_path in available_models[:5]:
            url = f"https://generativelanguage.googleapis.com/v1beta/{model_path}:generateContent?key={gemini_key}"
            resp = requests.post(
                url,
                json={
                    "contents": [{"parts": [{"text": prompt}]}],
                    "generationConfig": {"temperature": 0.1}
                },
                timeout=14
            )
            rjson = resp.json()
            if resp.status_code == 200 and "candidates" in rjson:
                parts = rjson["candidates"][0].get("content", {}).get("parts", [])
                if parts and "text" in parts[0]:
                    used_model = model_path.replace("models/", "")
                    reply_text = parts[0]["text"]
                    break

        if not reply_text:
            return jsonify({"status": "error", "message": "Gemini se response generate nahi ho paya."}), 500

        news_header = "📰 === LIVE CRYPTO NEWS & SENTIMENT DESK ===\n" + "\n".join(f"• {h}" for h in live_news[:4])
        final_output = f"{news_header}\n\n" + "="*55 + f"\n\n🧠 === GEMINI MENTOR AI ({used_model} | T=0.1) ===\n{reply_text}"
        return jsonify({"status": "ok", "reply": final_output})

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.after_request
def compress_response(response):
    if response.status_code != 200 or len(response.get_data()) < 500:
        return response
    accept_encoding = request.headers.get('Accept-Encoding', '')
    if 'gzip' not in accept_encoding.lower():
        return response
    response.direct_passthrough = False
    response.set_data(gzip.compress(response.get_data(), compresslevel=5))
    response.headers['Content-Encoding'] = 'gzip'
    response.headers['Content-Length'] = len(response.get_data())
    return response

# ============================================================
# START 24/7 SERVER-SIDE $10 DEMO PAPER BOT (IRON-ANCHOR V5)
# ============================================================

paper_bot_lock = threading.Lock()
paper_bot_thread = None
_last_self_ping = 0

def _default_paper_state():
    return {
        "enabled": False,  # Default OFF to save 100% bandwidth when not testing
        "symbol": "BTCUSDT",
        "tf": "5m",
        "leverage": 10,
        "scalp": {"balance": 10.0, "wins": 0, "losses": 0, "active": None, "history": [], "last_close": 0},
        "intraday": {"balance": 10.0, "wins": 0, "losses": 0, "active": None, "history": [], "last_close": 0},
    }

paper_bot_state = _default_paper_state()

def _load_paper_state_from_turso():
    global paper_bot_state
    if not TURSO_DATABASE_URL or not TURSO_AUTH_TOKEN:
        return
    try:
        with create_client_sync(TURSO_DATABASE_URL, auth_token=TURSO_AUTH_TOKEN) as client:
            client.execute("CREATE TABLE IF NOT EXISTS paper_bot_store (id INTEGER PRIMARY KEY, state_json TEXT)")
            res = client.execute("SELECT state_json FROM paper_bot_store WHERE id = 1")
            if res.rows:
                loaded = json.loads(res.rows[0][0])
                if isinstance(loaded, dict) and "scalp" in loaded and "intraday" in loaded:
                    paper_bot_state = loaded
    except Exception as e:
        print(f"[PAPER-BOT] Load skipped: {e}")

def _save_paper_state_to_turso():
    if not TURSO_DATABASE_URL or not TURSO_AUTH_TOKEN:
        return
    try:
        with paper_bot_lock:
            snap_json = json.dumps(paper_bot_state)
        sql = "INSERT OR REPLACE INTO paper_bot_store (id, state_json) VALUES (1, ?)"
        turso_write_queue.put({"sql": sql, "args": (snap_json,)})
    except Exception:
        pass

def _group_fp_server(fp_raw, low_px, high_px, eff_step, close_px, live_rest_bench):
    items = list(fp_raw.values()) if isinstance(fp_raw, dict) else (fp_raw or [])
    buckets = {}
    for l in items:
        p = float(l.get("price", 0.0))
        if p < low_px - eff_step * 2.5 or p > high_px + eff_step * 2.5:
            continue
        b_px = round(int(p / eff_step + 1e-9) * eff_step, 6)
        g = buckets.get(b_px)
        if g is None:
            g = {
                "price": b_px, "buy": 0.0, "sell": 0.0,
                "whale_buy": 0.0, "whale_sell": 0.0, "retail_buy": 0.0, "retail_sell": 0.0,
                "volume": 0.0, "trades": 0, "bid_pulled": 0.0, "ask_pulled": 0.0
            }
            buckets[b_px] = g
        g["buy"] += float(l.get("buy", 0.0))
        g["sell"] += float(l.get("sell", 0.0))
        g["whale_buy"] += float(l.get("whale_buy", 0.0))
        g["whale_sell"] += float(l.get("whale_sell", 0.0))
        g["retail_buy"] += float(l.get("retail_buy", 0.0))
        g["retail_sell"] += float(l.get("retail_sell", 0.0))
        g["volume"] += float(l.get("volume", 0.0))
        g["trades"] += int(l.get("trades", 0))
        bp = float(l.get("bid_pulled", 0.0))
        ap = float(l.get("ask_pulled", 0.0))
        pq = float(l.get("pulled_qty", 0.0))
        if bp == 0.0 and ap == 0.0 and pq > 0.0:
            if p <= close_px:
                bp = pq
            else:
                ap = pq
        g["bid_pulled"] += bp
        g["ask_pulled"] += ap

    rest_cap = max((live_rest_bench or 15.0) * 4.5, 45.0)
    out = []
    for g in buckets.values():
        if g["volume"] <= 0 and g["bid_pulled"] <= 0 and g["ask_pulled"] <= 0:
            continue
        max_pull = max(g["volume"] * 3.5, rest_cap)
        g["bid_pulled"] = min(g["bid_pulled"], max_pull)
        g["ask_pulled"] = min(g["ask_pulled"], max_pull)
        out.append(g)
    out.sort(key=lambda x: x["price"])
    return out

def _pick_server_magnet_tp(side, entry_px, sl_dist, min_rr, max_rr, magnets):
    min_d = sl_dist * min_rr
    max_d = sl_dist * max_rr
    valid = []
    for m_px, m_lbl in magnets:
        if side == "LONG" and (entry_px + min_d) <= m_px <= (entry_px + max_d):
            valid.append((abs(m_px - entry_px), m_px, m_lbl))
        elif side == "SHORT" and (entry_px - max_d) <= m_px <= (entry_px - min_d):
            valid.append((abs(m_px - entry_px), m_px, m_lbl))
    if valid:
        valid.sort(key=lambda x: x[0])
        return valid[0][1], valid[0][2]
    fallback_tp = (entry_px + sl_dist * min_rr) if side == "LONG" else (entry_px - sl_dist * min_rr)
    return fallback_tp, f"{min_rr}R-Target"

def _evaluate_server_orderflow(sym, tf_key):
    """100% Unified Iron-Anchor Evaluator (Matches Frontend Chart & DOM Logic)."""
    raw_step = PRICE_STEP.get(sym, 1.0)
    peer_sym = "ETHUSDT" if sym == "BTCUSDT" else "BTCUSDT"

    with lock:
        if not orderbook[sym]["initialized"]:
            return None
        c_list = list(candles[sym][tf_key])[-20:]
        curr = current_candles[sym][tf_key]
        if curr is not None:
            c_list.append(finalize_candle(curr))
        if len(c_list) < 7:
            return None

        peer_list = list(candles[peer_sym][tf_key])[-6:]
        peer_curr = current_candles[peer_sym][tf_key]
        if peer_curr is not None:
            peer_list.append(finalize_candle(peer_curr))

        last_c = c_list[-1]
        prev_c = c_list[-2]
        live_px = float(last_c["close"])
        bids_dict = dict(orderbook[sym]["bids"])
        asks_dict = dict(orderbook[sym]["asks"])
        raw_dom_snap = {k: dict(v) for k, v in dom_raw_telemetry[sym].items()}

    focus4 = c_list[-4:]
    f_lo = min(float(x["low"]) for x in focus4)
    f_hi = max(float(x["high"]) for x in focus4)
    p_span = max((f_hi - f_lo) * 1.26, raw_step * 10)
    raw_rows = p_span / raw_step
    mult = 1
    if raw_rows > 24:
        needed = raw_rows / 24.0
        nice_steps = [1, 2, 5, 10, 20, 25, 50, 100, 200, 500]
        mult = next((s for s in nice_steps if s >= needed), int(needed) + 1)
    eff_step = raw_step * mult

    bucket_book = defaultdict(float)
    bid_cluster_map = defaultdict(float)
    ask_cluster_map = defaultdict(float)
    near_bid_rest, near_ask_rest = 0.0, 0.0

    for p_s, q in bids_dict.items():
        pf = float(p_s)
        b_k = round(int(pf / eff_step + 1e-9) * eff_step, 6)
        bid_cluster_map[b_k] += q
        if abs(pf - live_px) <= eff_step * 25:
            bucket_book[f"B{int(pf / eff_step)}"] += q
        if (live_px - eff_step * 8) <= pf <= live_px:
            near_bid_rest += q

    for p_s, q in asks_dict.items():
        pf = float(p_s)
        a_k = round(int(pf / eff_step + 1e-9) * eff_step, 6)
        ask_cluster_map[a_k] += q
        if abs(pf - live_px) <= eff_step * 25:
            bucket_book[f"A{int(pf / eff_step)}"] += q
        if live_px <= pf <= (live_px + eff_step * 8):
            near_ask_rest += q

    raw_dom_cluster = defaultdict(lambda: {"w_buy": 0.0, "w_sell": 0.0, "r_buy": 0.0, "r_sell": 0.0, "flash_bp": 0.0, "flash_ap": 0.0, "aged_bp": 0.0, "aged_ap": 0.0, "ice_bid": 0.0, "ice_ask": 0.0})
    for px_k, d_obj in raw_dom_snap.items():
        if abs(float(px_k) - live_px) <= eff_step * 25:
            ck = round(int(float(px_k) / eff_step + 1e-9) * eff_step, 6)
            rc_d = raw_dom_cluster[ck]
            rc_d["w_buy"] += float(d_obj.get("w_buy", 0.0))
            rc_d["w_sell"] += float(d_obj.get("w_sell", 0.0))
            rc_d["r_buy"] += float(d_obj.get("r_buy", 0.0))
            rc_d["r_sell"] += float(d_obj.get("r_sell", 0.0))
            rc_d["flash_bp"] += float(d_obj.get("flash_bid_pull", 0.0))
            rc_d["flash_ap"] += float(d_obj.get("flash_ask_pull", 0.0))
            rc_d["aged_bp"] += float(d_obj.get("aged_bid_pull", 0.0))
            rc_d["aged_ap"] += float(d_obj.get("aged_ask_pull", 0.0))
            rc_d["ice_bid"] += float(d_obj.get("iceberg_bid", 0.0))
            rc_d["ice_ask"] += float(d_obj.get("iceberg_ask", 0.0))

    vals = [v for v in bucket_book.values() if v > 0]
    live_rest_bench = (sum(vals) / len(vals)) if vals else 0.0

    hist_c = c_list[:-1]
    avg_flow_vol = max(sum(float(x.get("volume", 0)) for x in hist_c) / max(1, len(hist_c)), 0.001)
    tot_hist_trades = sum(int(x.get("trades", 1)) for x in hist_c)
    base_lot = max(sum(float(x.get("volume", 0)) for x in hist_c) / max(1, tot_hist_trades), 0.0001)
    cur_rvol = float(last_c.get("volume", 0)) / avg_flow_vol

    vis_buckets, vis_vol = 0, 0.0
    for vc in c_list[-8:]:
        g_fp = _group_fp_server(vc.get("footprint"), float(vc["low"]), float(vc["high"]), eff_step, float(vc["close"]), live_rest_bench)
        vis_buckets += max(len([x for x in g_fp if x["volume"] > 0]), 1)
        vis_vol += float(vc.get("volume", 0.0))
    tf_avg_cell_vol = max(vis_vol / max(vis_buckets, 1), 0.001)
    min_inst_cell_floor = (avg_flow_vol / 12.0) * 0.85
    eff_cell_bench = max(tf_avg_cell_vol, min_inst_cell_floor)
    heavy_lvl_bench = max(eff_cell_bench * 1.40, min(eff_cell_bench * 2.1, live_rest_bench * 1.20)) if live_rest_bench > 0 else eff_cell_bench * 1.60

    smt_bias = 0
    if len(peer_list) >= 6:
        c6 = c_list[-6:]
        p6 = peer_list[-6:]
        if (float(c6[-1]["low"]) <= min(float(x["low"]) for x in c6[:-1])) != (float(p6[-1]["low"]) <= min(float(x["low"]) for x in p6[:-1])):
            smt_bias = 1
        elif (float(c6[-1]["high"]) >= max(float(x["high"]) for x in c6[:-1])) != (float(p6[-1]["high"]) >= max(float(x["high"]) for x in p6[:-1])):
            smt_bias = -1

    macro_hi = max(float(x["high"]) for x in c_list)
    macro_lo = min(float(x["low"]) for x in c_list)
    macro_span = max(macro_hi - macro_lo, raw_step)
    sum_ranges = sum(max(float(x["high"]) - float(x["low"]), raw_step) for x in c_list)
    net_move = live_px - float(c_list[0]["open"])
    efficiency = abs(net_move) / max(sum_ranges, raw_step)
    range_location = (live_px - macro_lo) / macro_span

    tot_cvd = 0.0
    poc_up, poc_dn = 0, 0
    macro_bid_abs, macro_ask_abs = 0, 0
    macro_bp, macro_ap = 0.0, 0.0
    macro_whale_b, macro_whale_s = 0, 0
    sess_w_buy, sess_w_sell = 0.0, 0.0

    for idx, mc in enumerate(c_list):
        tot_cvd += float(mc.get("delta", 0.0))
        sess_w_buy += float(mc.get("whale_buy_vol", 0.0))
        sess_w_sell += float(mc.get("whale_sell_vol", 0.0))
        if idx > 0 and mc.get("poc") and c_list[idx - 1].get("poc"):
            if float(mc["poc"]) > float(c_list[idx - 1]["poc"]):
                poc_up += 1
            elif float(mc["poc"]) < float(c_list[idx - 1]["poc"]):
                poc_dn += 1

        mfp = _group_fp_server(mc.get("footprint"), float(mc["low"]), float(mc["high"]), eff_step, float(mc["close"]), live_rest_bench)
        act_cells = [x for x in mfp if x["volume"] > 0]
        c_avg_v = max(float(mc.get("volume", 1.0)) / max(len(act_cells), 1), min_inst_cell_floor)
        c_heavy_cut = max(c_avg_v * 1.25, heavy_lvl_bench * 0.45)
        mc_hi, mc_lo, mc_cls = float(mc["high"]), float(mc["low"]), float(mc["close"])
        mc_span = max(mc_hi - mc_lo, eff_step)
        lo_swing_top = mc_lo + mc_span * 0.42
        hi_swing_bot = mc_hi - mc_span * 0.42

        for i_l, l in enumerate(mfp):
            sv, bv, tv = l["sell"], l["buy"], l["volume"]
            bp, ap = l["bid_pulled"], l["ask_pulled"]
            clus_s, clus_b = sv, bv
            if i_l > 0 and abs(mfp[i_l - 1]["price"] - l["price"]) <= eff_step * 1.5:
                clus_s += mfp[i_l - 1]["sell"]
                clus_b += mfp[i_l - 1]["buy"]
            if i_l + 1 < len(mfp) and abs(mfp[i_l + 1]["price"] - l["price"]) <= eff_step * 1.5:
                clus_s += mfp[i_l + 1]["sell"]
                clus_b += mfp[i_l + 1]["buy"]
            clus_tot = clus_s + clus_b
            clus_b_pct = (clus_s / clus_tot) * 100.0 if clus_tot > 0 else 0.0
            clus_a_pct = (clus_b / clus_tot) * 100.0 if clus_tot > 0 else 0.0

            macro_bp += bp
            macro_ap += ap
            if l["price"] <= lo_swing_top and clus_s >= c_heavy_cut * 1.3 and clus_b_pct >= 58.0 and mc_cls > l["price"] + eff_step * 0.4:
                macro_bid_abs += 1
            if l["price"] >= hi_swing_bot and clus_b >= c_heavy_cut * 1.3 and clus_a_pct >= 58.0 and mc_cls < l["price"] - eff_step * 0.4:
                macro_ask_abs += 1

            lt = max(l["trades"], 1)
            lot_ratio = (tv / lt) / base_lot
            has_tw = (l["whale_buy"] + l["whale_sell"]) > 0
            if has_tw or (tv >= max(c_avg_v * 1.35, heavy_lvl_bench * 0.65) and lot_ratio >= 3.2):
                if (l["whale_buy"] or bv) > (l["whale_sell"] or sv):
                    macro_whale_b += 1
                else:
                    macro_whale_s += 1

    macro_pull_delta = macro_bp - macro_ap
    net_w_vol = sess_w_buy - sess_w_sell
    net_macro = 0
    if macro_bid_abs > macro_ask_abs:
        net_macro += min(28, 12 + (macro_bid_abs - macro_ask_abs) * 4)
    elif macro_ask_abs > macro_bid_abs:
        net_macro -= min(28, 12 + (macro_ask_abs - macro_bid_abs) * 4)
    if macro_whale_b > macro_whale_s or net_w_vol > avg_flow_vol * 0.3:
        net_macro += 24
    elif macro_whale_s > macro_whale_b or net_w_vol < -avg_flow_vol * 0.3:
        net_macro -= 24
    if macro_pull_delta < -avg_flow_vol * 0.15:
        net_macro += 18
    elif macro_pull_delta > avg_flow_vol * 0.15:
        net_macro -= 18
    if poc_up > poc_dn:
        net_macro += 16
    elif poc_dn > poc_up:
        net_macro -= 16
    if tot_cvd > 0 and (net_move >= 0 or macro_bid_abs > macro_ask_abs):
        net_macro += 14
    elif tot_cvd < 0 and (net_move <= 0 or macro_ask_abs > macro_bid_abs):
        net_macro -= 14

    # 6-Candle Structure Cluster Evaluation
    tot_exec_buy, tot_exec_sell, tot_pull_b, tot_pull_a = 0.0, 0.0, 0.0, 0.0
    tot_w_buy, tot_w_sell, tot_r_buy, tot_r_sell = 0.0, 0.0, 0.0, 0.0
    tot_ice_bid = sum(v["ice_bid"] for v in raw_dom_cluster.values())
    tot_ice_ask = sum(v["ice_ask"] for v in raw_dom_cluster.values())
    tot_flash_bp = sum(v["flash_bp"] for v in raw_dom_cluster.values())
    tot_flash_ap = sum(v["flash_ap"] for v in raw_dom_cluster.values())
    r_bid_abs, r_ask_abs = 0, 0
    lowest_bid_abs_px, highest_ask_abs_px = None, None

    struct_set = c_list[-6:]
    for s_idx, rc in enumerate(struct_set):
        is_rec3 = s_idx >= len(struct_set) - 3
        rc_hi, rc_lo = float(rc["high"]), float(rc["low"])
        rc_cls, rc_opn = float(rc["close"]), float(rc["open"])
        rc_rng = max(rc_hi - rc_lo, raw_step * 0.5)
        rc_cls_pos = (rc_cls - rc_lo) / rc_rng
        lo_zone = rc_lo + rc_rng * 0.45
        hi_zone = rc_hi - rc_rng * 0.45

        g_list = _group_fp_server(rc.get("footprint"), rc_lo, rc_hi, eff_step, rc_cls, live_rest_bench)
        for i_g, gl in enumerate(g_list):
            if is_rec3:
                tot_exec_buy += gl["buy"]
                tot_exec_sell += gl["sell"]
                tot_w_buy += gl["whale_buy"]
                tot_w_sell += gl["whale_sell"]
                tot_r_buy += gl["retail_buy"]
                tot_r_sell += gl["retail_sell"]
                tot_pull_b += gl["bid_pulled"]
                tot_pull_a += gl["ask_pulled"]

            clus_s, clus_b = gl["sell"], gl["buy"]
            if i_g > 0 and abs(g_list[i_g - 1]["price"] - gl["price"]) <= eff_step * 1.5:
                clus_s += g_list[i_g - 1]["sell"]
                clus_b += g_list[i_g - 1]["buy"]
            if i_g + 1 < len(g_list) and abs(g_list[i_g + 1]["price"] - gl["price"]) <= eff_step * 1.5:
                clus_s += g_list[i_g + 1]["sell"]
                clus_b += g_list[i_g + 1]["buy"]
            clus_tot = clus_s + clus_b
            clus_b_pct = (clus_s / clus_tot) * 100.0 if clus_tot > 0 else 0.0
            clus_a_pct = (clus_b / clus_tot) * 100.0 if clus_tot > 0 else 0.0
            churn = clus_tot >= eff_cell_bench * 2.1 and clus_b_pct >= 42.0 and clus_a_pct >= 42.0
            b_rest_here = bid_cluster_map.get(gl["price"], 0.0)
            a_rest_here = ask_cluster_map.get(gl["price"], 0.0)
            raw_d = raw_dom_cluster.get(gl["price"])

            bid_rot_up = (rc_cls >= rc_opn or rc_cls_pos >= 0.45) and (rc_cls > gl["price"] + eff_step * 0.4)
            ask_rot_dn = (rc_cls <= rc_opn or rc_cls_pos <= 0.55) and (rc_cls < gl["price"] - eff_step * 0.4)

            is_bid_abs = gl["price"] <= lo_zone and bid_rot_up and (
                (clus_s >= eff_cell_bench * 1.6 and clus_b_pct >= 58.0 and gl["sell"] >= eff_cell_bench * 0.7)
                or churn
                or (gl["sell"] >= eff_cell_bench * 1.0 and b_rest_here >= max(live_rest_bench * 1.3, eff_cell_bench))
                or (raw_d and raw_d["ice_bid"] >= eff_cell_bench * 0.5)
            )
            is_ask_abs = gl["price"] >= hi_zone and ask_rot_dn and (
                (clus_b >= eff_cell_bench * 1.6 and clus_a_pct >= 58.0 and gl["buy"] >= eff_cell_bench * 0.7)
                or churn
                or (gl["buy"] >= eff_cell_bench * 1.0 and a_rest_here >= max(live_rest_bench * 1.3, eff_cell_bench))
                or (raw_d and raw_d["ice_ask"] >= eff_cell_bench * 0.5)
            )

            if is_bid_abs:
                r_bid_abs += 1
                if lowest_bid_abs_px is None or gl["price"] < lowest_bid_abs_px:
                    lowest_bid_abs_px = gl["price"]

            if is_ask_abs:
                r_ask_abs += 1
                if highest_ask_abs_px is None or gl["price"] > highest_ask_abs_px:
                    highest_ask_abs_px = gl["price"]

    tot_pull_b += tot_flash_bp
    tot_pull_a += tot_flash_ap
    dom_pull_delta = tot_pull_b - tot_pull_a

    c_range = max(float(last_c["high"]) - float(last_c["low"]), raw_step * 0.5)
    close_pos = (live_px - float(last_c["low"])) / c_range
    last_delta = float(last_c.get("delta", 0.0))

    cur_poc = float(last_c["poc"]) if last_c.get("poc") is not None else (float(last_c["high"]) + float(last_c["low"])) * 0.5
    prev_poc = float(prev_c["poc"]) if prev_c.get("poc") is not None else (float(prev_c["high"]) + float(prev_c["low"])) * 0.5

    s_poc_up, s_poc_dn = 0, 0
    last4 = c_list[-4:]
    for m in range(1, len(last4)):
        if last4[m].get("poc") is not None and last4[m - 1].get("poc") is not None:
            if float(last4[m]["poc"]) > float(last4[m - 1]["poc"]):
                s_poc_up += 1
            elif float(last4[m]["poc"]) < float(last4[m - 1]["poc"]):
                s_poc_dn += 1

    prev5_hi = max(float(x["high"]) for x in c_list[-6:-1])
    prev5_lo = min(float(x["low"]) for x in c_list[-6:-1])

    # Iron-Anchor Closed Candle Scoring (Matches Frontend JS 100%)
    closed6 = c_list[:-1][-6:]
    avg_c_rng = sum(max(float(x["high"]) - float(x["low"]), raw_step) for x in closed6) / max(1, len(closed6))
    anchor_bull, anchor_bear = 0, 0
    anchor_sup, anchor_res = None, None

    for cc in closed6:
        cc_rng = max(float(cc["high"]) - float(cc["low"]), raw_step * 0.5)
        cc_pos = (float(cc["close"]) - float(cc["low"])) / cc_rng
        cc_wb = float(cc.get("whale_buy_vol", 0.0))
        cc_ws = float(cc.get("whale_sell_vol", 0.0))
        cc_d = float(cc.get("delta", 0.0))

        if cc_d < -eff_cell_bench * 0.3 and cc_pos >= 0.55:
            anchor_bull += 18
            anchor_sup = float(cc["low"]) if anchor_sup is None else min(anchor_sup, float(cc["low"]))
        elif cc_d > eff_cell_bench * 0.3 and cc_pos <= 0.45:
            anchor_bear += 18
            anchor_res = float(cc["high"]) if anchor_res is None else max(anchor_res, float(cc["high"]))

        if cc_wb > cc_ws * 1.35 and cc_pos >= 0.50:
            anchor_bull += 14
        elif cc_ws > cc_wb * 1.35 and cc_pos <= 0.50:
            anchor_bear += 14

    if r_bid_abs > r_ask_abs:
        anchor_bull += min(26, 12 + (r_bid_abs - r_ask_abs) * 5)
    elif r_ask_abs > r_bid_abs:
        anchor_bear += min(26, 12 + (r_ask_abs - r_bid_abs) * 5)

    if tot_ice_bid > tot_ice_ask * 1.3:
        anchor_bull += 15
    elif tot_ice_ask > tot_ice_bid * 1.3:
        anchor_bear += 15

    if s_poc_up > s_poc_dn:
        anchor_bull += 14
    elif s_poc_dn > s_poc_up:
        anchor_bear += 14

    is_big_exp = c_range >= avg_c_rng * 1.55
    bull_sweep_reject = float(last_c["low"]) < prev5_lo and live_px > prev5_lo and (close_pos >= 0.48 or r_bid_abs > 0 or tot_ice_bid > 0)
    bear_sweep_reject = float(last_c["high"]) > prev5_hi and live_px < prev5_hi and (close_pos <= 0.52 or r_ask_abs > 0 or tot_ice_ask > 0)

    is_bull_exhaust = (
        bear_sweep_reject
        or (live_px > float(last_c["open"]) and is_big_exp and range_location >= 0.78 and (r_ask_abs > 0 or tot_ice_ask > eff_cell_bench * 0.4))
        or (live_px > float(last_c["open"]) and anchor_bear > anchor_bull + 20 and live_px < (anchor_res or macro_hi))
        or (last_delta > eff_cell_bench * 0.45 and close_pos <= 0.46)
    )
    is_bear_exhaust = (
        bull_sweep_reject
        or (live_px < float(last_c["open"]) and is_big_exp and range_location <= 0.22 and (r_bid_abs > 0 or tot_ice_bid > eff_cell_bench * 0.4))
        or (live_px < float(last_c["open"]) and anchor_bull > anchor_bear + 20 and live_px > (anchor_sup or macro_lo))
        or (last_delta < -eff_cell_bench * 0.45 and close_pos >= 0.54)
    )

    inst_bull_drive = (
        not is_bull_exhaust and anchor_bull >= anchor_bear
        and (tot_w_buy > tot_w_sell * 1.20 or dom_pull_delta < -eff_cell_bench * 0.30)
        and live_px > cur_poc and float(prev_c["close"]) >= prev_poc
        and close_pos >= 0.60 and range_location <= 0.82
    )
    inst_bear_drive = (
        not is_bear_exhaust and anchor_bear >= anchor_bull
        and (tot_w_sell > tot_w_buy * 1.20 or dom_pull_delta > eff_cell_bench * 0.30)
        and live_px < cur_poc and float(prev_c["close"]) <= prev_poc
        and close_pos <= 0.40 and range_location >= 0.18
    )

    cluster_long_buildup = (anchor_bull >= anchor_bear + 15) and not inst_bear_drive
    cluster_short_buildup = (anchor_bear >= anchor_bull + 15) and not inst_bull_drive
    retail_buys_trapped = is_bull_exhaust
    retail_sells_trapped = is_bear_exhaust

    naked_pocs, unfinished_levels = [], []
    min_ub_vol = max(base_lot * 0.8, 0.05)
    for i in range(len(c_list) - 1):
        hc = c_list[i]
        pc = hc.get("poc")
        if pc is not None and not any(float(c_list[j]["low"]) <= float(pc) <= float(c_list[j]["high"]) for j in range(i + 1, len(c_list))):
            naked_pocs.append(float(pc))
        g_fp = _group_fp_server(hc.get("footprint"), float(hc["low"]), float(hc["high"]), eff_step, float(hc["close"]), live_rest_bench)
        act_fp = [x for x in g_fp if x["volume"] > 0]
        if act_fp:
            hi_l, lo_l = max(act_fp, key=lambda x: x["price"]), min(act_fp, key=lambda x: x["price"])
            if hi_l["buy"] >= min_ub_vol and hi_l["sell"] >= min_ub_vol and not any(float(c_list[j]["high"]) > hi_l["price"] + eff_step * 0.2 for j in range(i + 1, len(c_list))):
                unfinished_levels.append((hi_l["price"], "UB-HIGH"))
            if lo_l["buy"] >= min_ub_vol and lo_l["sell"] >= min_ub_vol and not any(float(c_list[j]["low"]) < lo_l["price"] - eff_step * 0.2 for j in range(i + 1, len(c_list))):
                unfinished_levels.append((lo_l["price"], "UB-LOW"))
    magnets = [(p, "nPOC") for p in naked_pocs] + unfinished_levels

    scalp_pts = anchor_bull - anchor_bear
    if dom_pull_delta < -eff_cell_bench * 0.30:
        scalp_pts += 16
    elif dom_pull_delta > eff_cell_bench * 0.30:
        scalp_pts -= 16

    min_book_floor = eff_cell_bench * 1.3
    if near_bid_rest >= min_book_floor and near_bid_rest > near_ask_rest * 1.30:
        scalp_pts += 10
    elif near_ask_rest >= min_book_floor and near_ask_rest > near_bid_rest * 1.30:
        scalp_pts -= 10

    delta_state = "NEU"
    if is_bear_exhaust:
        scalp_pts += 20
        delta_state = "BULL"
    elif is_bull_exhaust:
        scalp_pts -= 20
        delta_state = "BEAR"
    elif inst_bull_drive:
        scalp_pts += 14
        delta_state = "BULL"
    elif inst_bear_drive:
        scalp_pts -= 14
        delta_state = "BEAR"

    whale_state = "NEU"
    if tot_w_buy > tot_w_sell * 1.25 and not inst_bear_drive:
        scalp_pts += 16
        whale_state = "BULL"
    elif tot_w_sell > tot_w_buy * 1.25 and not inst_bull_drive:
        scalp_pts -= 16
        whale_state = "BEAR"

    loc_state = "NEU"
    if live_px >= cur_poc and float(prev_c["close"]) >= prev_poc and not is_bull_exhaust:
        scalp_pts += 12
        loc_state = "BULL"
    elif live_px <= cur_poc and float(prev_c["close"]) <= prev_poc and not is_bear_exhaust:
        scalp_pts -= 12
        loc_state = "BEAR"

    if smt_bias == 1:
        scalp_pts += 10
    elif smt_bias == -1:
        scalp_pts -= 10

    if scalp_pts > 0 and range_location >= 0.78 and not bull_sweep_reject:
        scalp_pts = min(scalp_pts, 18)
    elif scalp_pts < 0 and range_location <= 0.22 and not bear_sweep_reject:
        scalp_pts = max(scalp_pts, -18)

    exec_vs_abs_state = "BULL" if inst_bull_drive else ("BEAR" if inst_bear_drive else ("BULL" if cluster_long_buildup else ("BEAR" if cluster_short_buildup else "NEU")))
    s_states = [
        exec_vs_abs_state,
        "BULL" if (r_bid_abs > r_ask_abs or tot_ice_bid > tot_ice_ask * 1.25) else ("BEAR" if (r_ask_abs > r_bid_abs or tot_ice_ask > tot_ice_bid * 1.25) else "NEU"),
        "BULL" if dom_pull_delta < -eff_cell_bench * 0.30 else ("BEAR" if dom_pull_delta > eff_cell_bench * 0.30 else "NEU"),
        "BULL" if (near_bid_rest >= min_book_floor and near_bid_rest > near_ask_rest * 1.30) else ("BEAR" if (near_ask_rest >= min_book_floor and near_ask_rest > near_bid_rest * 1.30) else "NEU"),
        delta_state,
        loc_state,
        whale_state,
        "BULL" if range_location <= 0.35 else ("BEAR" if range_location >= 0.65 else "NEU"),
        "BULL" if s_poc_up > s_poc_dn else ("BEAR" if s_poc_dn > s_poc_up else "NEU"),
        "BULL" if bull_sweep_reject else ("BEAR" if bear_sweep_reject else "NEU"),
        "BULL" if smt_bias == 1 else ("BEAR" if smt_bias == -1 else "NEU"),
        exec_vs_abs_state if cur_rvol >= 0.85 else "NEU",
    ]
    s_bull, s_bear, s_neu = s_states.count("BULL"), s_states.count("BEAR"), s_states.count("NEU")

    intra_pts = net_macro
    if s_poc_up > s_poc_dn:
        intra_pts += 14
    elif s_poc_dn > s_poc_up:
        intra_pts -= 14
    if smt_bias == 1:
        intra_pts += 12
    elif smt_bias == -1:
        intra_pts -= 12

    sess_pocs = [float(x["poc"]) for x in c_list[-15:] if x.get("poc") is not None]
    avg_sess_poc = (sum(sess_pocs) / len(sess_pocs)) if sess_pocs else live_px
    ub_hi_cnt = sum(1 for px, t in unfinished_levels if t == "UB-HIGH" and px > live_px)
    ub_lo_cnt = sum(1 for px, t in unfinished_levels if t == "UB-LOW" and px < live_px)
    npoc_above = sum(1 for px in naked_pocs if px > live_px)
    npoc_below = sum(1 for px in naked_pocs if px < live_px)

    i_states = [
        "BULL" if tot_cvd > 0 else ("BEAR" if tot_cvd < 0 else "NEU"),
        "BULL" if macro_bid_abs > macro_ask_abs else ("BEAR" if macro_ask_abs > macro_bid_abs else "NEU"),
        "BULL" if s_poc_up > s_poc_dn else ("BEAR" if s_poc_dn > s_poc_up else "NEU"),
        "BULL" if macro_pull_delta < 0 else ("BEAR" if macro_pull_delta > 0 else "NEU"),
        "BULL" if sess_w_buy > sess_w_sell else ("BEAR" if sess_w_sell > sess_w_buy else "NEU"),
        "BULL" if live_px > avg_sess_poc + eff_step else ("BEAR" if live_px < avg_sess_poc - eff_step else "NEU"),
        "BULL" if ub_hi_cnt > ub_lo_cnt else ("BEAR" if ub_lo_cnt > ub_hi_cnt else "NEU"),
        "BULL" if npoc_above > npoc_below else ("BEAR" if npoc_below > npoc_above else "NEU"),
        "BULL" if (near_bid_rest >= min_book_floor and near_bid_rest > near_ask_rest * 1.30) else ("BEAR" if (near_ask_rest >= min_book_floor and near_ask_rest > near_bid_rest * 1.30) else "NEU"),
        "BULL" if smt_bias == 1 else ("BEAR" if smt_bias == -1 else "NEU"),
    ]
    i_bull, i_bear, i_neu = i_states.count("BULL"), i_states.count("BEAR"), i_states.count("NEU")

    candle_age_sec = (now_ms() - int(last_c["start"])) / 1000.0

    return {
        "live_px": live_px,
        "high": float(last_c["high"]),
        "low": float(last_c["low"]),
        "open": float(last_c["open"]),
        "cur_poc": cur_poc,
        "prev_poc": prev_poc,
        "prev_lo": float(prev_c["low"]),
        "prev_hi": float(prev_c["high"]),
        "lowest_bid_abs_px": lowest_bid_abs_px,
        "highest_ask_abs_px": highest_ask_abs_px,
        "step": raw_step,
        "eff_step": eff_step,
        "rvol": cur_rvol,
        "efficiency": efficiency,
        "range_location": range_location,
        "candle_age_sec": candle_age_sec,
        "inst_bull_drive": inst_bull_drive,
        "inst_bear_drive": inst_bear_drive,
        "cluster_long_buildup": cluster_long_buildup,
        "cluster_short_buildup": cluster_short_buildup,
        "retail_buys_trapped": retail_buys_trapped,
        "retail_sells_trapped": retail_sells_trapped,
        "bull_sweep_reject": bull_sweep_reject,
        "bear_sweep_reject": bear_sweep_reject,
        "r_bid_abs": r_bid_abs,
        "r_ask_abs": r_ask_abs,
        "s_pts": scalp_pts, "s_bull": s_bull, "s_bear": s_bear, "s_neu": s_neu,
        "i_pts": intra_pts, "i_bull": i_bull, "i_bear": i_bear, "i_neu": i_neu,
        "prev5_hi": prev5_hi, "prev5_lo": prev5_lo,
        "magnets": magnets,
    }

def _check_book_sl_tp(book, sig, mode="scalp"):
    act = book.get("active")
    if not act:
        return False

    # STRICT POST-ENTRY TICK BOUNDS (Never triggers on pre-entry candle wicks!)
    bounds = paper_post_entry_bounds.get(mode, {})
    live_px = sig["live_px"]
    hi = bounds["high"] if bounds.get("high") is not None else live_px
    lo = bounds["low"] if bounds.get("low") is not None else live_px

    entry_px = float(act["entry"])
    init_risk = float(act.get("init_risk", abs(entry_px - float(act["sl"]))))
    dec = 4 if sig["step"] < 0.01 else (2 if sig["step"] < 1 else 1)

    if init_risk > 0:
        fee_plus_prof = max(entry_px * 0.0014, init_risk * 0.25)
        if act["side"] == "LONG":
            if hi >= (entry_px + init_risk * 1.35) and float(act["sl"]) < (entry_px + init_risk * 0.75):
                act["sl"] = round(entry_px + init_risk * 0.75, dec)
                act["be_locked"] = "TRAIL"
            elif not act.get("be_locked") and hi >= (entry_px + init_risk * 0.95):
                act["sl"] = round(entry_px + fee_plus_prof, dec)
                act["be_locked"] = "BE"
        else:
            if lo <= (entry_px - init_risk * 1.35) and float(act["sl"]) > (entry_px - init_risk * 0.75):
                act["sl"] = round(entry_px - init_risk * 0.75, dec)
                act["be_locked"] = "TRAIL"
            elif not act.get("be_locked") and lo <= (entry_px - init_risk * 0.95):
                act["sl"] = round(entry_px - fee_plus_prof, dec)
                act["be_locked"] = "BE"

    exit_px, outcome = None, None
    if act["side"] == "LONG":
        if lo <= act["sl"]:
            exit_px = act["sl"]
            outcome = "TRAIL WIN 🎯" if act.get("be_locked") == "TRAIL" else ("BE+ LOCK 🔒" if act.get("be_locked") == "BE" else "SL HIT ❌")
        elif hi >= act["tp"]:
            exit_px = act["tp"]
            outcome = f"TP HIT ({act.get('tp_tag', 'TARGET')}) ✅"
    else:
        if hi >= act["sl"]:
            exit_px = act["sl"]
            outcome = "TRAIL WIN 🎯" if act.get("be_locked") == "TRAIL" else ("BE+ LOCK 🔒" if act.get("be_locked") == "BE" else "SL HIT ❌")
        elif lo <= act["tp"]:
            exit_px = act["tp"]
            outcome = f"TP HIT ({act.get('tp_tag', 'TARGET')}) ✅"

    if exit_px is None:
        return False

    notional = float(act["margin"]) * float(act["leverage"])
    pct = ((exit_px - entry_px) / entry_px) if act["side"] == "LONG" else ((entry_px - exit_px) / entry_px)
    net_pnl = (notional * pct) - (notional * 0.0012)

    book["balance"] = round(max(0.0, float(book["balance"]) + net_pnl), 3)
    if net_pnl >= 0:
        book["wins"] += 1
    else:
        book["losses"] += 1

    book["history"].insert(0, {
        "time": time.strftime("%H:%M:%S", time.gmtime(time.time() + 19800)),
        "symbol": act["symbol"], "tf": act["tf"], "side": act["side"],
        "entry": act["entry"], "exit": round(exit_px, dec),
        "outcome": outcome, "pnl": round(net_pnl, 3), "reason": act["reason"]
    })
    book["history"] = book["history"][:30]
    book["active"] = None
    book["last_close"] = time.time()
    paper_post_entry_bounds[mode] = {"high": None, "low": None}
    return True

def _paper_bot_worker():
    global _last_self_ping
    _load_paper_state_from_turso()
    while True:
        try:
            with paper_bot_lock:
                enabled = paper_bot_state.get("enabled", False)
                sym = paper_bot_state.get("symbol", "BTCUSDT")
                tf_key = paper_bot_state.get("tf", "5m")
                lev = int(paper_bot_state.get("leverage", 10))

            if not enabled:
                time.sleep(8.0)
                continue

            time.sleep(4.0)

            if time.time() - _last_self_ping > 540:
                _last_self_ping = time.time()
                ext_url = os.environ.get("RENDER_EXTERNAL_URL")
                if ext_url:
                    try:
                        requests.get(f"{ext_url.rstrip('/')}/hello", timeout=4)
                    except Exception:
                        pass

            sig = _evaluate_server_orderflow(sym, tf_key)
            if not sig:
                continue

            changed = False
            now_s = time.time()
            dec = 4 if sig["step"] < 0.01 else (2 if sig["step"] < 1 else 1)

            with paper_bot_lock:
                if _check_book_sl_tp(paper_bot_state["scalp"], sig, "scalp"):
                    changed = True
                if _check_book_sl_tp(paper_bot_state["intraday"], sig, "intraday"):
                    changed = True

                if enabled and sig["candle_age_sec"] >= 25:
                    sb = paper_bot_state["scalp"]
                    if not sb["active"] and sb["balance"] >= 1.0 and (now_s - sb.get("last_close", 0) > 45):
                        s_long = (
                            sig["s_pts"] >= 46 and sig["s_bull"] >= 6 and (sig["s_bull"] - sig["s_bear"] >= 3)
                            and not sig["inst_bear_drive"] and not sig["retail_buys_trapped"]
                            and sig["i_pts"] >= -15 and sig["range_location"] <= 0.78
                            and sig["live_px"] >= sig["cur_poc"]
                            and (sig["inst_bull_drive"] or sig["cluster_long_buildup"] or sig["bull_sweep_reject"])
                        )
                        s_short = (
                            sig["s_pts"] <= -46 and sig["s_bear"] >= 6 and (sig["s_bear"] - sig["s_bull"] >= 3)
                            and not sig["inst_bull_drive"] and not sig["retail_sells_trapped"]
                            and sig["i_pts"] <= 15 and sig["range_location"] >= 0.22
                            and sig["live_px"] <= sig["cur_poc"]
                            and (sig["inst_bear_drive"] or sig["cluster_short_buildup"] or sig["bear_sweep_reject"])
                        )

                        if s_long or s_short:
                            side = "LONG" if s_long else "SHORT"
                            ep = sig["live_px"]
                            if side == "LONG":
                                struct_fl = sig["lowest_bid_abs_px"] if sig["lowest_bid_abs_px"] else min(sig["low"], sig["prev_lo"])
                                raw_d = (ep - struct_fl) + sig["eff_step"] * 1.5
                            else:
                                struct_cl = sig["highest_ask_abs_px"] if sig["highest_ask_abs_px"] else max(sig["high"], sig["prev_hi"])
                                raw_d = (struct_cl - ep) + sig["eff_step"] * 1.5

                            sl_d = max(ep * 0.0014, min(raw_d, ep * 0.0030))
                            sl = round(ep - sl_d if side == "LONG" else ep + sl_d, dec)
                            tp_raw, tp_tag = _pick_server_magnet_tp(side, ep, sl_d, 1.55, 2.5, sig["magnets"])
                            tp = round(tp_raw, dec)

                            sl_pct = sl_d / ep
                            max_loss_usd = sb["balance"] * 0.032
                            safe_notional = min(sb["balance"] * lev, max_loss_usd / max(sl_pct, 0.0005))
                            eff_margin = round(safe_notional / lev, 2)

                            paper_post_entry_bounds["scalp"] = {"high": ep, "low": ep}
                            sb["active"] = {
                                "symbol": sym, "tf": tf_key, "side": side,
                                "entry": round(ep, dec), "sl": sl, "tp": tp, "tp_tag": tp_tag,
                                "init_risk": sl_d, "be_locked": False, "opened_ts": now_s,
                                "margin": eff_margin, "leverage": lev,
                                "reason": f"Score:{sig['s_pts']} | 🟢{sig['s_bull']}🔴{sig['s_bear']}⚪{sig['s_neu']} | TP:{tp_tag}"
                            }
                            changed = True

                    ib = paper_bot_state["intraday"]
                    if not ib["active"] and ib["balance"] >= 1.0 and (now_s - ib.get("last_close", 0) > 60):
                        i_long = (
                            sig["i_pts"] >= 46 and sig["i_bull"] >= 6 and (sig["i_bull"] - sig["i_bear"] >= 3)
                            and sig["s_pts"] >= 28 and not sig["inst_bear_drive"] and not sig["retail_buys_trapped"]
                            and sig["live_px"] >= sig["cur_poc"] and sig["range_location"] <= 0.78
                        )
                        i_short = (
                            sig["i_pts"] <= -46 and sig["i_bear"] >= 6 and (sig["i_bear"] - sig["i_bull"] >= 3)
                            and sig["s_pts"] <= -28 and not sig["inst_bull_drive"] and not sig["retail_sells_trapped"]
                            and sig["live_px"] <= sig["cur_poc"] and sig["range_location"] >= 0.22
                        )

                        if i_long or i_short:
                            side = "LONG" if i_long else "SHORT"
                            ep = sig["live_px"]
                            sw_ref = min(sig["low"], sig["prev5_lo"]) if side == "LONG" else max(sig["high"], sig["prev5_hi"])
                            raw_d = abs(ep - sw_ref) + sig["eff_step"] * 2.0

                            sl_d = max(ep * 0.0022, min(raw_d, ep * 0.0045))
                            sl = round(ep - sl_d if side == "LONG" else ep + sl_d, dec)
                            tp_raw, tp_tag = _pick_server_magnet_tp(side, ep, sl_d, 1.85, 3.2, sig["magnets"])
                            tp = round(tp_raw, dec)

                            sl_pct = sl_d / ep
                            max_loss_usd = ib["balance"] * 0.045
                            safe_notional = min(ib["balance"] * lev, max_loss_usd / max(sl_pct, 0.0005))
                            eff_margin = round(safe_notional / lev, 2)

                            paper_post_entry_bounds["intraday"] = {"high": ep, "low": ep}
                            ib["active"] = {
                                "symbol": sym, "tf": tf_key, "side": side,
                                "entry": round(ep, dec), "sl": sl, "tp": tp, "tp_tag": tp_tag,
                                "init_risk": sl_d, "be_locked": False, "opened_ts": now_s,
                                "margin": eff_margin, "leverage": lev,
                                "reason": f"Intra:{sig['i_pts']} (🟢{sig['i_bull']}🔴{sig['i_bear']}⚪{sig['i_neu']}) | TP:{tp_tag}"
                            }
                            changed = True

            if changed:
                _save_paper_state_to_turso()
        except Exception:
            pass

@app.before_request
def _start_paper_bot_once():
    global paper_bot_thread
    if paper_bot_thread is None or not paper_bot_thread.is_alive():
        with paper_bot_lock:
            if paper_bot_thread is None or not paper_bot_thread.is_alive():
                paper_bot_thread = threading.Thread(target=_paper_bot_worker, name="server-paper-bot", daemon=True)
                paper_bot_thread.start()

@app.route("/api/paper-status")
def api_paper_status():
    with paper_bot_lock:
        sym = paper_bot_state.get("symbol", "BTCUSDT")
        px = last_trade.get(sym, {}).get("price")
        out = dict(paper_bot_state)
        out["live_price"] = px
    return jsonify({"status": "ok", "data": out})

@app.route("/api/paper-action", methods=["POST"])
def api_paper_action():
    data = request.get_json(silent=True) or {}
    act = data.get("action")
    with paper_bot_lock:
        if act == "toggle":
            paper_bot_state["enabled"] = not paper_bot_state.get("enabled", False)
        elif act == "config":
            if data.get("symbol") in SYMBOLS:
                paper_bot_state["symbol"] = data["symbol"]
            if data.get("tf") in TIMEFRAMES:
                paper_bot_state["tf"] = data["tf"]
            if data.get("leverage"):
                paper_bot_state["leverage"] = int(data["leverage"])
        elif act == "reset":
            sym, tf_k, lev = paper_bot_state["symbol"], paper_bot_state["tf"], paper_bot_state["leverage"]
            en_state = paper_bot_state.get("enabled", False)
            paper_bot_state.clear()
            paper_bot_state.update(_default_paper_state())
            paper_bot_state["symbol"], paper_bot_state["tf"], paper_bot_state["leverage"] = sym, tf_k, lev
            paper_bot_state["enabled"] = en_state
            paper_post_entry_bounds["scalp"] = {"high": None, "low": None}
            paper_post_entry_bounds["intraday"] = {"high": None, "low": None}
        elif act == "close":
            mode = data.get("mode", "scalp")
            book = paper_bot_state.get(mode)
            if book and book.get("active"):
                p = book["active"]
                px = float(last_trade.get(p["symbol"], {}).get("price") or p["entry"])
                notional = float(p["margin"]) * float(p["leverage"])
                move = ((px - p["entry"]) / p["entry"]) if p["side"] == "LONG" else ((p["entry"] - px) / p["entry"])
                net_pnl = (notional * move) - (notional * 0.0012)
                book["balance"] = round(max(0.0, float(book["balance"]) + net_pnl), 3)
                if net_pnl >= 0:
                    book["wins"] += 1
                else:
                    book["losses"] += 1
                book["history"].insert(0, {
                    "time": time.strftime("%H:%M:%S", time.gmtime(time.time() + 19800)),
                    "symbol": p["symbol"], "tf": p["tf"], "side": p["side"],
                    "entry": p["entry"], "exit": px,
                    "outcome": "MANUAL EXIT ✋", "pnl": round(net_pnl, 3), "reason": p["reason"]
                })
                book["active"] = None
                book["last_close"] = time.time()
                paper_post_entry_bounds[mode] = {"high": None, "low": None}
    _save_paper_state_to_turso()
    return jsonify({"status": "ok"})

# ============================================================
# END 24/7 SERVER-SIDE $10 DEMO PAPER BOT
# ============================================================


if __name__ == "__main__":
    ensure_collector_started()
    app.run(host="0.0.0.0", port=10000, debug=False)
