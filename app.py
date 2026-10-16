import encodings.idna
import json
import os
import queue
import threading
import time
import uuid
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
        "buffer": deque(maxlen=1000),
        "liquidity_history": deque(maxlen=300),
        "liquidity_lots": {
            "bid": defaultdict(deque),
            "ask": defaultdict(deque),
        },
        "trade_history": deque(maxlen=300),
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
                        # Filter out old garbage far-away price levels from RAM
                        fp_dict = {
                            str(item["price"]): item
                            for item in fp_list
                            if (l - margin) <= float(item.get("price", 0)) <= (h + margin)
                        }
                    except Exception:
                        fp_dict = {}

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
    except Exception as e:
        print(f"[TURSO] Hydration failed: {e}")

# ============================================================
# HELPERS & FOOTPRINT STAMPING
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
        "delta": 0.0,
        "trades": 0,
        "footprint": {},
    }


def add_trade_to_candle(candle, price, quantity, is_buyer_maker):
    candle["high"] = max(candle["high"], price)
    candle["low"] = min(candle["low"], price)
    candle["close"] = price
    candle["volume"] += quantity
    candle["trades"] += 1

    if is_buyer_maker:
        candle["sell_volume"] += quantity
    else:
        candle["buy_volume"] += quantity

    candle["delta"] = candle["buy_volume"] - candle["sell_volume"]

    symbol = candle["symbol"]
    step = PRICE_STEP[symbol]
    price_level = round_price_to_step(price, step)
    key = str(price_level)

    level = candle["footprint"].get(key)
    if level is None:
        level = {
            "price": price_level,
            "buy": 0.0,
            "sell": 0.0,
            "delta": 0.0,
            "volume": 0.0,
            "trades": 0,
            "pulled_qty": 0.0,
            "status": "NORMAL",
        }
        candle["footprint"][key] = level

    if is_buyer_maker:
        level["sell"] += quantity
    else:
        level["buy"] += quantity

    level["volume"] += quantity
    level["trades"] += 1
    level["delta"] = level["buy"] - level["sell"]


def stamp_liquidity_event_to_active_candles(symbol, price_str, side, pulled_qty, status_str):
    """Stamps pull/absorption metadata ONLY near the active candle price range to prevent RAM bloat."""
    step = PRICE_STEP.get(symbol, 1.0)
    try:
        p_float = float(price_str)
    except Exception:
        return
    bucket_price = round_price_to_step(p_float, step)
    key = str(bucket_price)
    max_distance = step * 25  # Only stamp within 25 price steps of candle High/Low

    with lock:
        for tf in TIMEFRAMES.keys():
            curr = current_candles[symbol][tf]
            if curr is not None and "footprint" in curr:
                # Ignore far-away orderbook noise
                if bucket_price < (curr["low"] - max_distance) or bucket_price > (curr["high"] + max_distance):
                    continue

                level = curr["footprint"].get(key)
                if level is None:
                    level = {
                        "price": bucket_price,
                        "buy": 0.0,
                        "sell": 0.0,
                        "delta": 0.0,
                        "volume": 0.0,
                        "trades": 0,
                        "pulled_qty": 0.0,
                        "status": "NORMAL",
                    }
                    curr["footprint"][key] = level
                
                if pulled_qty > 0.0:
                    level["pulled_qty"] = round(float(level.get("pulled_qty", 0.0)) + float(pulled_qty), 4)
                
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
    started_at = time.time()

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
# EXECUTION MATCHING & FIFO ACCOUNTING
# ============================================================

def finalize_liquidity_records(symbol, current_time_ms=None):
    if symbol not in orderbook:
        return 0

    has_event_time = current_time_ms is not None
    if current_time_ms is None:
        current_time_ms = now_ms()

    finalized_count = 0
    state = orderbook[symbol]
    liquidity_history = state["liquidity_history"]

    for record in liquidity_history:
        if not isinstance(record, dict) or record.get("finalized"):
            continue

        reduced_qty = float(record.get("reduced_qty", 0.0))
        if reduced_qty <= 0.0:
            continue

        record_time = int(record.get("time", current_time_ms))
        if current_time_ms - record_time < 1500:
            continue

        executed_qty = max(0.0, min(reduced_qty, float(record.get("executed_qty", 0.0))))
        unmatched_qty = reduced_qty - executed_qty

        epsilon = max(reduced_qty, executed_qty) * 1e-12
        if abs(unmatched_qty) <= epsilon:
            executed_qty = reduced_qty
            unmatched_qty = 0.0
        else:
            unmatched_qty = max(0.0, unmatched_qty)

        pulled_qty = unmatched_qty
        pull_pct = (pulled_qty / reduced_qty) * 100.0 if reduced_qty > 0 else 0.0

        record["executed_qty"] = executed_qty
        record["unmatched_qty"] = unmatched_qty
        record["pulled_qty"] = pulled_qty
        record["pull_pct"] = pull_pct
        record["finalized"] = True

        if executed_qty > 0.0 and pulled_qty > 0.0:
            record["status"] = "PARTIAL_EXECUTION_PULL"
        elif executed_qty > 0.0:
            record["status"] = "EXECUTED"
        elif pulled_qty > 0.0:
            record["status"] = "LIQUIDITY_PULLED"
        else:
            record["status"] = "REDUCTION_ZERO"

        # Stamp pull event onto active footprint candles
        if pulled_qty > 0.0:
            stamp_liquidity_event_to_active_candles(symbol, record.get("price"), record.get("side"), pulled_qty, record["status"])

        finalized_count += 1

    liquidity_index = state.get("liquidity_match_index")
    if isinstance(liquidity_index, dict):
        history_ids = {id(r) for r in liquidity_history if isinstance(r, dict)}
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
        trade_cutoff = current_time_ms - 300 if has_event_time else None
        for key, trades in list(trade_index.items()):
            retained_trades = deque()
            for trade in trades:
                if not isinstance(trade, dict):
                    continue
                t_time = int(trade.get("time", 0))
                rem_qty = float(trade.get("remaining_qty", 0.0))
                if rem_qty <= 1e-12:
                    continue
                if trade_cutoff is not None and t_time < trade_cutoff:
                    continue
                retained_trades.append(trade)
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
        liquidity["pulled_qty"] = 0.0
        liquidity["pull_pct"] = 0.0
        liquidity["status"] = "reduction_pending"

        fifo_consumption = liquidity.setdefault("fifo_consumption", [])
        fifo_remaining = actual_matched_qty

        for item in fifo_consumption:
            if fifo_remaining <= epsilon:
                break
            consumed_qty = max(0.0, float(item.get("consumed_qty", 0.0)))
            prior_exec = max(0.0, float(item.get("execution_qty", 0.0)))
            capacity = max(0.0, consumed_qty - prior_exec)
            if capacity <= epsilon:
                continue

            attrib = min(capacity, fifo_remaining)
            item["execution_qty"] = prior_exec + attrib
            item["unmatched_qty"] = max(0.0, consumed_qty - item["execution_qty"])
            item["execution_time"] = trade_time
            item["trade_price"] = price
            fifo_remaining -= attrib

        if fifo_remaining > epsilon:
            liquidity["fifo_unattributed_execution_qty"] = (
                float(liquidity.get("fifo_unattributed_execution_qty", 0.0)) + fifo_remaining
            )

        liquidity["fifo_executed_qty"] = sum(
            float(i.get("execution_qty", 0.0)) for i in fifo_consumption
        )
        liquidity["fifo_unmatched_qty"] = sum(
            max(0.0, float(i.get("consumed_qty", 0.0)) - float(i.get("execution_qty", 0.0)))
            for i in fifo_consumption
        )

        trade_record["remaining_qty"] = max(0.0, available_trade - actual_matched_qty)
        matched_total += actual_matched_qty

    state["trade_history"].append(trade_record)
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
# ORDERBOOK EVENT APPLICATION
# ============================================================

def apply_orderbook_event(symbol, event):
    state = orderbook[symbol]
    event_update_id = int(event["u"])
    event_time = int(event.get("E", now_ms()))

    finalize_liquidity_records(symbol, event_time)
    liquidity_index = state["liquidity_match_index"]

    def process_side(side, event_levels, book):
        for price, quantity in event_levels:
            price = str(price)
            new_quantity = float(quantity)
            old_quantity = float(book.get(price, 0.0))

            added_qty = max(new_quantity - old_quantity, 0.0)
            reduced_qty = max(old_quantity - new_quantity, 0.0)

            if new_quantity == 0.0:
                book.pop(price, None)
            else:
                book[price] = new_quantity

            lots = state["liquidity_lots"][side][price]

            if added_qty > 0.0:
                lots.append({
                    "lot_id": str(uuid.uuid4()),
                    "original_qty": added_qty,
                    "remaining_qty": added_qty,
                    "time": event_time,
                    "origin": "depth_add",
                    "update_id": event_update_id,
                })

            fifo_consumption = []
            fifo_unattributed_qty = 0.0

            if reduced_qty > 0.0:
                remaining_reduction = reduced_qty
                while remaining_reduction > 1e-12 and lots:
                    oldest_lot = lots[0]
                    lot_remaining = float(oldest_lot.get("remaining_qty", 0.0))
                    if lot_remaining <= 1e-12:
                        lots.popleft()
                        continue

                    consumed_qty = min(lot_remaining, remaining_reduction)
                    remaining_after = max(0.0, lot_remaining - consumed_qty)
                    oldest_lot["remaining_qty"] = remaining_after

                    fifo_consumption.append({
                        "lot_id": oldest_lot.get("lot_id"),
                        "origin": oldest_lot.get("origin"),
                        "origin_time": int(oldest_lot.get("time", event_time)),
                        "origin_update_id": oldest_lot.get("update_id"),
                        "original_qty": float(oldest_lot.get("original_qty", 0.0)),
                        "consumed_qty": consumed_qty,
                        "remaining_qty_after": remaining_after,
                        "execution_qty": 0.0,
                        "unmatched_qty": consumed_qty,
                    })

                    remaining_reduction -= consumed_qty
                    if oldest_lot["remaining_qty"] <= 1e-12:
                        lots.popleft()

                fifo_unattributed_qty = max(remaining_reduction, 0.0)

            if not lots:
                state["liquidity_lots"][side].pop(price, None)

            if old_quantity != new_quantity:
                executed_qty = 0.0
                if reduced_qty > 0.0:
                    executed_qty = match_trade_to_liquidity_reduction(
                        symbol, side, price, reduced_qty, event_time
                    )

                executed_qty = min(max(float(executed_qty), 0.0), reduced_qty)
                unmatched_qty = max(reduced_qty - executed_qty, 0.0)

                rem_exec = executed_qty
                for item in fifo_consumption:
                    if rem_exec <= 1e-12:
                        break
                    c_qty = float(item["consumed_qty"])
                    alloc = min(c_qty, rem_exec)
                    item["execution_qty"] = alloc
                    item["unmatched_qty"] = max(0.0, c_qty - alloc)
                    rem_exec -= alloc

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
                    "unmatched_qty": unmatched_qty,
                    "pulled_qty": 0.0,
                    "pull_pct": 0.0,
                    "finalized": False,
                    "fifo_reduction": (reduced_qty > 0.0),
                    "fifo_consumption": fifo_consumption,
                    "fifo_consumed_qty": sum(float(i["consumed_qty"]) for i in fifo_consumption),
                    "fifo_unattributed_qty": fifo_unattributed_qty,
                    "fifo_executed_qty": sum(float(i["execution_qty"]) for i in fifo_consumption),
                    "fifo_unmatched_qty": sum(float(i["unmatched_qty"]) for i in fifo_consumption),
                    "status": "reduction_pending" if reduced_qty > 0.0 else "liquidity_added",
                }

                state["liquidity_history"].append(liquidity_record)

                if reduced_qty > 0.0:
                    price_key = round(float(price), 8)
                    liquidity_index[(side, price_key)].append(liquidity_record)

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
                                    "lot_id": str(uuid.uuid4()),
                                    "original_qty": q,
                                    "remaining_qty": q,
                                    "time": snap_time,
                                    "origin": "snapshot",
                                    "update_id": snapshot_id,
                                })
                            for p, q in snapshot_asks.items():
                                state["liquidity_lots"]["ask"][p].append({
                                    "lot_id": str(uuid.uuid4()),
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
                # FIX: Agar uninitialized reh gaya aur buffer empty bhi hai, 
                # tab bhi recovery flag active rakh kar retry trigger karenge.
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
        # Convert footprint dict/list to sorted list for clean storage
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
                # Strict 48 Hours cutoff window (2 days)
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
# TRADE & MESSAGE ROUTER
# ============================================================

def process_trade(symbol, price, quantity, trade_time, is_buyer_maker):
    with lock:
        record_trade_for_execution_matching(symbol, price, quantity, trade_time, is_buyer_maker)

        for timeframe, seconds in TIMEFRAMES.items():
            start = floor_timestamp(trade_time, seconds)
            current = current_candles[symbol][timeframe]

            if current is None:
                current = create_empty_candle(symbol, timeframe, start, price)
                current_candles[symbol][timeframe] = current
            elif start != current["start"]:
                store_finished_candle(current)
                current = create_empty_candle(symbol, timeframe, start, price)
                current_candles[symbol][timeframe] = current

            add_trade_to_candle(current, price, quantity, is_buyer_maker)

        last_trade[symbol] = {
            "time": trade_time,
            "price": price,
            "quantity": quantity,
        }


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
        
        # Ensure Turso background worker & historical hydration starts on boot
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
# ROUTES
# ============================================================

from flask import Flask, jsonify, request, render_template_string # agar render_template_string na ho toh import mein jod lena
@app.route("/")
def home():
    return """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>SA Institutional Footprint Terminal</title>
<style>
    * { box-sizing: border-box; margin: 0; padding: 0; user-select: none; }
    body {
        background: #0b0e14;
        color: #d1d4dc;
        font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
        overflow: hidden;
        height: 100vh;
        display: flex;
        flex-direction: column;
    }
    /* TOP TRADINGVIEW BAR */
    .topbar {
        height: 42px;
        background: #131722;
        border-bottom: 1px solid #2a2e39;
        display: flex;
        align-items: center;
        padding: 0 10px;
        gap: 8px;
        font-size: 12px;
    }
    .brand {
        font-weight: 800;
        color: #00bcd4;
        letter-spacing: 0.5px;
        margin-right: 6px;
        font-size: 13px;
    }
    select, .btn {
        background: #1e222d;
        color: #d1d4dc;
        border: 1px solid #2a2e39;
        padding: 4px 8px;
        border-radius: 3px;
        font-size: 12px;
        cursor: pointer;
        outline: none;
    }
    select:hover, .btn:hover { background: #2a2e39; }
    .btn.active {
        background: #2962ff;
        color: #fff;
        border-color: #2962ff;
        font-weight: 600;
    }
    .sep { width: 1px; height: 22px; background: #2a2e39; margin: 0 4px; }
    .live-metrics {
        margin-left: auto;
        display: flex;
        gap: 14px;
        align-items: center;
        font-family: monospace;
        font-size: 12px;
    }
    .price-badge {
        font-size: 14px;
        font-weight: bold;
        padding: 2px 8px;
        border-radius: 3px;
        background: #1e222d;
    }
    /* MAIN WORKSPACE */
    .workspace {
        flex: 1;
        display: flex;
        overflow: hidden;
    }
    .chart-wrap {
        flex: 1;
        position: relative;
        background: #0b0e14;
        overflow: hidden;
    }
    canvas {
        display: block;
        cursor: crosshair;
    }
    /* RIGHT ORDERFLOW / DOM PANEL */
    .dom-panel {
        width: 310px;
        background: #131722;
        border-left: 1px solid #2a2e39;
        display: flex;
        flex-direction: column;
        font-size: 11px;
    }
    .dom-header {
        padding: 7px 10px;
        background: #1e222d;
        border-bottom: 1px solid #2a2e39;
        font-weight: bold;
        display: flex;
        justify-content: space-between;
        color: #9ba2b0;
    }
    .dom-section-title {
        padding: 4px 8px;
        background: #181c27;
        color: #787b86;
        font-size: 10px;
        font-weight: bold;
        text-transform: uppercase;
        border-bottom: 1px solid #2a2e39;
        border-top: 1px solid #2a2e39;
    }
    .dom-list {
        flex: 1;
        overflow-y: auto;
        font-family: monospace;
    }
    .dom-row {
        display: flex;
        align-items: center;
        justify-content: space-between;
        padding: 2px 8px;
        position: relative;
        height: 20px;
        border-bottom: 1px solid rgba(42,46,57,0.3);
    }
    .dom-bar {
        position: absolute;
        top: 1px;
        bottom: 1px;
        right: 0;
        z-index: 0;
        opacity: 0.22;
    }
    .dom-cell { position: relative; z-index: 1; }
    .ask-text { color: #f23645; }
    .bid-text { color: #089981; }
    .wall-badge {
        font-size: 9px;
        padding: 1px 4px;
        border-radius: 2px;
        font-weight: bold;
    }
    .badge-pull { background: #e91e63; color: #fff; }
    .badge-abs { background: #ff9800; color: #000; }
    .badge-wall { background: #2962ff; color: #fff; }
    .spread-box {
        padding: 5px 10px;
        background: #1e222d;
        text-align: center;
        font-weight: bold;
        font-family: monospace;
        border-top: 1px solid #2a2e39;
        border-bottom: 1px solid #2a2e39;
        color: #ffeb3b;
    }
    /* HELP OVERLAY */
    .hint-box {
        position: absolute;
        top: 8px;
        left: 10px;
        background: rgba(19, 23, 34, 0.85);
        border: 1px solid #2a2e39;
        padding: 5px 10px;
        border-radius: 4px;
        font-size: 11px;
        color: #9ba2b0;
        pointer-events: none;
    }
</style>
</head>
<body>

<div class="topbar">
    <span class="brand">⚡ SA FOOTPRINT PRO</span>

    <select id="symSelect" onchange="changeSymbol(this.value)">
        <option value="BTCUSDT">BTCUSDT (Step: 1.0)</option>
        <option value="ETHUSDT">ETHUSDT (Step: 0.1)</option>
        <option value="SOLUSDT">SOLUSDT (Step: 0.01)</option>
        <option value="XRPUSDT">XRPUSDT (Step: 0.0001)</option>
    </select>

    <div class="sep"></div>

    <div id="tfGroup">
        <button class="btn active" onclick="changeTF('1m', this)">1m</button>
        <button class="btn" onclick="changeTF('3m', this)">3m</button>
        <button class="btn" onclick="changeTF('5m', this)">5m</button>
        <button class="btn" onclick="changeTF('15m', this)">15m</button>
        <button class="btn" onclick="changeTF('30m', this)">30m</button>
        <button class="btn" onclick="changeTF('1h', this)">1h</button>
        <button class="btn" onclick="changeTF('4h', this)">4h</button>
        <button class="btn" onclick="changeTF('1d', this)">1d</button>
    </div>

    <div class="sep"></div>

    <select id="fpMode" onchange="changeFpMode(this.value)">
        <option value="bidask">Footprint: Sell x Buy</option>
        <option value="delta">Footprint: Delta + Pulls</option>
        <option value="volume">Footprint: Total Vol + Pulls</option>
    </select>

    <button class="btn" onclick="resetView()">Auto-Fit (Reset)</button>

    <div class="live-metrics">
        <span id="syncBadge" style="color:#089981;">● SYNCED</span>
        <span id="livePrice" class="price-badge">--</span>
    </div>
</div>

<div class="workspace">
    <div class="chart-wrap" id="chartWrap">
        <canvas id="chartCanvas"></canvas>
        <div class="hint-box" id="chartInfo">
            Scroll: Zoom In/Out | Drag Chart: Move | Drag Price Axis: Scale Y | Double-Click: Auto-Fit
        </div>
    </div>

    <div class="dom-panel">
        <div class="dom-header">
            <span>PRICE ZONE</span>
            <span>RESTING / EXEC</span>
            <span>SIGNAL</span>
        </div>
        <div class="dom-section-title">Heavy Ask Clusters (Sell Walls)</div>
        <div class="dom-list" id="asksList"></div>
        <div class="spread-box" id="spreadBox">LIVE SPREAD</div>
        <div class="dom-section-title">Heavy Bid Clusters (Buy Walls)</div>
        <div class="dom-list" id="bidsList"></div>
    </div>
</div>

<script>
const canvas = document.getElementById('chartCanvas');
const ctx = canvas.getContext('2d', { alpha: false });
const wrap = document.getElementById('chartWrap');

let symbol = 'BTCUSDT';
let timeframe = '1m';
let fpMode = 'bidask';

let candles = [];
let scanData = { bids_zone: [], asks_zone: [] };
let priceStep = 1.0;

// Viewport Camera State (Pan & Zoom)
let candleSpacing = 115; // Wide enough to show Footprint numbers by default!
let panOffsetX = 0;      // Pixels shifted horizontally
let manualMinP = null;
let manualMaxP = null;

// Mouse Interaction State
let isDraggingChart = false;
let isDraggingYAxis = false;
let dragStartX = 0;
let dragStartY = 0;
let mouseX = -1;
let mouseY = -1;

const PRICE_AXIS_W = 78;
const DELTA_PANEL_H = 72;
const TIME_AXIS_H = 22;

function resizeCanvas() {
    canvas.width = wrap.clientWidth;
    canvas.height = wrap.clientHeight;
    requestAnimationFrame(render);
}
window.addEventListener('resize', resizeCanvas);
resizeCanvas();

function changeSymbol(val) {
    symbol = val;
    resetView();
    fetchAll();
}

function changeTF(tf, btn) {
    timeframe = tf;
    document.querySelectorAll('#tfGroup .btn').forEach(b => b.classList.remove('active'));
    btn.classList.add('active');
    resetView();
    fetchAll();
}

function changeFpMode(val) {
    fpMode = val;
    requestAnimationFrame(render);
}

function resetView() {
    panOffsetX = 0;
    manualMinP = null;
    manualMaxP = null;
    candleSpacing = 115;
    requestAnimationFrame(render);
}

// ---------------- MOUSE PAN & ZOOM ----------------
canvas.addEventListener('mousedown', e => {
    const rect = canvas.getBoundingClientRect();
    const x = e.clientX - rect.left;
    const y = e.clientY - rect.top;
    dragStartX = x;
    dragStartY = y;

    if (x > canvas.width - PRICE_AXIS_W) {
        isDraggingYAxis = true;
    } else {
        isDraggingChart = true;
    }
});

window.addEventListener('mousemove', e => {
    const rect = canvas.getBoundingClientRect();
    const x = e.clientX - rect.left;
    const y = e.clientY - rect.top;
    mouseX = x;
    mouseY = y;

    if (isDraggingChart) {
        const dx = x - dragStartX;
        const dy = y - dragStartY;
        panOffsetX += dx;

        if (manualMinP !== null && manualMaxP !== null) {
            const chartH = canvas.height - DELTA_PANEL_H - TIME_AXIS_H;
            const range = manualMaxP - manualMinP;
            const priceShift = (dy / chartH) * range;
            manualMinP += priceShift;
            manualMaxP += priceShift;
        }
        dragStartX = x;
        dragStartY = y;
        requestAnimationFrame(render);
    } else if (isDraggingYAxis) {
        const dy = y - dragStartY;
        if (manualMinP !== null && manualMaxP !== null) {
            const mid = (manualMaxP + manualMinP) / 2;
            let half = (manualMaxP - manualMinP) / 2;
            half *= (1 + dy * 0.005);
            if (half > priceStep * 2) {
                manualMinP = mid - half;
                manualMaxP = mid + half;
            }
        }
        dragStartY = y;
        requestAnimationFrame(render);
    } else {
        requestAnimationFrame(render);
    }
});

window.addEventListener('mouseup', () => {
    isDraggingChart = false;
    isDraggingYAxis = false;
});

canvas.addEventListener('dblclick', () => resetView());

canvas.addEventListener('wheel', e => {
    e.preventDefault();
    const zoomFactor = e.deltaY < 0 ? 1.15 : 0.87;
    candleSpacing = Math.max(12, Math.min(260, candleSpacing * zoomFactor));
    requestAnimationFrame(render);
}, { passive: false });

// ---------------- DATA FETCHING ----------------
async function fetchAll() {
    try {
        const [cRes, sRes] = await Promise.all([
            fetch(`/api/candles?symbol=${symbol}&timeframe=${timeframe}&limit=120`),
            fetch(`/api/scan?symbol=${symbol}`)
        ]);
        const cJson = await cRes.json();
        const sJson = await sRes.json();

        if (cJson.status === 'ok' && Array.isArray(cJson.candles)) {
            candles = cJson.candles;
            if (candles.length > 0) {
                const last = candles[candles.length - 1];
                const pEl = document.getElementById('livePrice');
                pEl.innerText = last.close;
                pEl.style.color = last.close >= last.open ? '#089981' : '#f23645';
            }
        }
        if (sJson.status === 'ok') {
            scanData = sJson;
            priceStep = sJson.cluster_size || 1.0;
            renderDOM(sJson);
        }
        document.getElementById('syncBadge').innerText = '● LIVE SYNCED';
        document.getElementById('syncBadge').style.color = '#089981';
        requestAnimationFrame(render);
    } catch (err) {
        document.getElementById('syncBadge').innerText = '● RECONNECTING...';
        document.getElementById('syncBadge').style.color = '#ff9800';
    }
}

// ---------------- RIGHT PANEL: VISUAL ORDERFLOW DOM ----------------
function renderDOM(data) {
    const asksEl = document.getElementById('asksList');
    const bidsEl = document.getElementById('bidsList');
    const asks = (data.asks_zone || []).slice(0, 18).reverse();
    const bids = (data.bids_zone || []).slice(0, 18);

    let maxRest = 1;
    asks.concat(bids).forEach(z => {
        if (z.resting_liquidity > maxRest) maxRest = z.resting_liquidity;
    });

    // Find top 2 heavy walls on each side
    const sortedAskWalls = [...asks].sort((a,b) => b.resting_liquidity - a.resting_liquidity);
    const heavyAskCut = sortedAskWalls[1] ? sortedAskWalls[1].resting_liquidity : 999999;

    const sortedBidWalls = [...bids].sort((a,b) => b.resting_liquidity - a.resting_liquidity);
    const heavyBidCut = sortedBidWalls[1] ? sortedBidWalls[1].resting_liquidity : 999999;

    const decimals = priceStep < 0.01 ? 4 : (priceStep < 1 ? 2 : 1);

    asksEl.innerHTML = asks.map(a => {
        const pct = Math.min(100, (a.resting_liquidity / maxRest) * 100);
        let badge = '';
        if (a.status.includes('PULLED')) badge = `<span class="wall-badge badge-pull">PULLED</span>`;
        else if (a.status.includes('ABSORPTION')) badge = `<span class="wall-badge badge-abs">ABSORB</span>`;
        else if (a.resting_liquidity >= heavyAskCut && a.resting_liquidity > 0) badge = `<span class="wall-badge badge-wall">WALL</span>`;

        return `<div class="dom-row">
            <div class="dom-bar" style="width:${pct}%; background:#f23645;"></div>
            <span class="dom-cell ask-text">${Number(a.price_zone).toFixed(decimals)}</span>
            <span class="dom-cell">${a.resting_liquidity} <small style="color:#787b86;">(${a.executed_volume})</small></span>
            <span class="dom-cell">${badge}</span>
        </div>`;
    }).join('');

    bidsEl.innerHTML = bids.map(b => {
        const pct = Math.min(100, (b.resting_liquidity / maxRest) * 100);
        let badge = '';
        if (b.status.includes('PULLED')) badge = `<span class="wall-badge badge-pull">PULLED</span>`;
        else if (b.status.includes('ABSORPTION')) badge = `<span class="wall-badge badge-abs">ABSORB</span>`;
        else if (b.resting_liquidity >= heavyBidCut && b.resting_liquidity > 0) badge = `<span class="wall-badge badge-wall">WALL</span>`;

        return `<div class="dom-row">
            <div class="dom-bar" style="width:${pct}%; background:#089981;"></div>
            <span class="dom-cell bid-text">${Number(b.price_zone).toFixed(decimals)}</span>
            <span class="dom-cell">${b.resting_liquidity} <small style="color:#787b86;">(${b.executed_volume})</small></span>
            <span class="dom-cell">${badge}</span>
        </div>`;
    }).join('');

    if (candles.length > 0) {
        const last = candles[candles.length - 1];
        document.getElementById('spreadBox').innerText = `${symbol} @ ${last.close} | Δ ${last.delta.toFixed(2)}`;
    }
}

// ---------------- MAIN CANVAS FOOTPRINT ENGINE ----------------
function getFootprintArray(fp) {
    if (!fp) return [];
    if (Array.isArray(fp)) return fp;
    return Object.values(fp);
}

function render() {
    const W = canvas.width;
    const H = canvas.height;
    const chartW = W - PRICE_AXIS_W;
    const chartH = H - DELTA_PANEL_H - TIME_AXIS_H;

    // Clear Background
    ctx.fillStyle = '#0b0e14';
    ctx.fillRect(0, 0, W, H);

    if (!candles || candles.length === 0) {
        ctx.fillStyle = '#787b86';
        ctx.font = '14px sans-serif';
        ctx.fillText('Loading Institutional Footprint Data...', chartW / 2 - 110, chartH / 2);
        return;
    }

    // Calculate visible candles based on X pan & spacing
    const rightAnchorX = chartW - 90 + panOffsetX;
    const visibleCandles = [];

    for (let i = candles.length - 1; i >= 0; i--) {
        const offsetFromLast = candles.length - 1 - i;
        const cx = rightAnchorX - offsetFromLast * candleSpacing;
        if (cx + candleSpacing < 0) break;
        if (cx - candleSpacing <= chartW) {
            visibleCandles.push({ candle: candles[i], cx, index: i });
        }
    }

    if (visibleCandles.length === 0) return;

    // Auto-compute Y price bounds if not manually scaled
    if (manualMinP === null || manualMaxP === null) {
        let minP = Infinity, maxP = -Infinity;
        visibleCandles.forEach(({ candle }) => {
            if (candle.low < minP) minP = candle.low;
            if (candle.high > maxP) maxP = candle.high;
        });
        const pad = Math.max((maxP - minP) * 0.12, priceStep * 4);
        manualMinP = minP - pad;
        manualMaxP = maxP + pad;
    }

    const minP = manualMinP;
    const maxP = manualMaxP;
    const priceRange = Math.max(maxP - minP, priceStep);

    function priceToY(p) {
        return chartH - ((p - minP) / priceRange) * chartH;
    }
    function yToPrice(y) {
        return minP + ((chartH - y) / chartH) * priceRange;
    }

    const decimals = priceStep < 0.01 ? 4 : (priceStep < 1 ? 2 : 1);

    // 1. Horizontal Grid Lines & Price Axis Labels
    const gridSteps = 10;
    ctx.strokeStyle = '#181c27';
    ctx.lineWidth = 1;
    ctx.font = '11px monospace';
    ctx.fillStyle = '#787b86';

    for (let i = 0; i <= gridSteps; i++) {
        const y = (chartH / gridSteps) * i;
        const p = yToPrice(y);
        ctx.beginPath();
        ctx.moveTo(0, y);
        ctx.lineTo(chartW, y);
        ctx.stroke();
    }

    // Row height for 1 priceStep
    const rowH = Math.abs(priceToY(minP + priceStep) - priceToY(minP));
    const showFpText = candleSpacing >= 65 && rowH >= 11;

    // 2. Render Each Visible Candle + Footprint Table
    let cvd = 0;
    visibleCandles.slice().reverse().forEach(({ candle, cx }) => {
        cvd += (candle.delta || 0);
        const isBull = candle.close >= candle.open;
        const mainColor = isBull ? '#089981' : '#f23645';

        const yHigh = priceToY(candle.high);
        const yLow = priceToY(candle.low);
        const yOpen = priceToY(candle.open);
        const yClose = priceToY(candle.close);

        const fpList = getFootprintArray(candle.footprint).filter(
            lvl => lvl.price >= candle.low - priceStep * 3 && lvl.price <= candle.high + priceStep * 3
        );

        let maxLvlVol = 1;
        fpList.forEach(l => { if (l.volume > maxLvlVol) maxLvlVol = l.volume; });

        if (candleSpacing >= 45) {
            // PRO FOOTPRINT MODE: Left thin OHLC bar + Footprint Cluster Box
            const boxW = candleSpacing * 0.86;
            const boxLeft = cx - boxW / 2;
            const ohlcX = boxLeft + 4;

            // Thin OHLC Wick & Body on Left Spine
            ctx.strokeStyle = mainColor;
            ctx.lineWidth = 1.5;
            ctx.beginPath();
            ctx.moveTo(ohlcX, yHigh);
            ctx.lineTo(ohlcX, yLow);
            ctx.stroke();

            ctx.fillStyle = mainColor;
            const bTop = Math.min(yOpen, yClose);
            const bH = Math.max(Math.abs(yClose - yOpen), 2);
            ctx.fillRect(ohlcX - 2.5, bTop, 5, bH);

            // VAH / VAL Value Area Shaded Zone
            if (candle.vah != null && candle.val != null) {
                const yVah = priceToY(candle.vah + priceStep / 2);
                const yVal = priceToY(candle.val - priceStep / 2);
                ctx.fillStyle = 'rgba(41, 98, 255, 0.08)';
                ctx.fillRect(boxLeft + 8, yVah, boxW - 10, Math.max(2, yVal - yVah));

                // VAH & VAL Horizontal Lines
                ctx.strokeStyle = 'rgba(41, 98, 255, 0.65)';
                ctx.lineWidth = 1;
                ctx.beginPath();
                ctx.moveTo(boxLeft + 8, yVah);
                ctx.lineTo(boxLeft + boxW, yVah);
                ctx.moveTo(boxLeft + 8, yVal);
                ctx.lineTo(boxLeft + boxW, yVal);
                ctx.stroke();
            }

            // Draw Each Price Level Cell Inside Candle
            fpList.forEach(lvl => {
                const py = priceToY(lvl.price);
                const cellH = Math.max(rowH - 1, 2);
                const cellTop = py - cellH / 2;
                const cellLeft = boxLeft + 9;
                const cellW = boxW - 10;

                const volPct = Math.min(1, (lvl.volume || 0) / maxLvlVol);
                const lvlDelta = (lvl.buy || 0) - (lvl.sell || 0);

                // Background Heatmap Bar inside cell
                ctx.fillStyle = lvlDelta >= 0
                    ? `rgba(8, 153, 129, ${0.12 + volPct * 0.45})`
                    : `rgba(242, 54, 69, ${0.12 + volPct * 0.45})`;
                ctx.fillRect(cellLeft, cellTop, cellW * volPct, cellH);

                // POC Highlight (Yellow Border)
                if (candle.poc != null && Math.abs(lvl.price - candle.poc) < priceStep * 0.4) {
                    ctx.strokeStyle = '#ffeb3b';
                    ctx.lineWidth = 1.5;
                    ctx.strokeRect(cellLeft, cellTop, cellW, cellH);
                }

                // Pulled Liquidity Trap / Absorption Highlight Marker
                if (lvl.pulled_qty && lvl.pulled_qty > 0 && (lvl.volume > 0 || lvl.price >= candle.low && lvl.price <= candle.high)) {
                    ctx.fillStyle = '#e91e63';
                    ctx.fillRect(cellLeft + cellW - 4, cellTop, 4, cellH);
                }

                // Print Bid x Ask or Delta Numbers when zoomed in
                if (showFpText) {
                    ctx.font = '10px monospace';
                    ctx.textBaseline = 'middle';
                    const midX = cellLeft + cellW / 2;

                    if (fpMode === 'bidask') {
                        // Left = Sell, Right = Buy
                        ctx.fillStyle = lvl.sell > lvl.buy * 2.5 ? '#ff5252' : '#d1d4dc';
                        ctx.textAlign = 'right';
                        ctx.fillText(formatQty(lvl.sell), midX - 3, py);

                        ctx.fillStyle = '#555';
                        ctx.textAlign = 'center';
                        ctx.fillText('x', midX, py);

                        ctx.fillStyle = lvl.buy > lvl.sell * 2.5 ? '#00e676' : '#d1d4dc';
                        ctx.textAlign = 'left';
                        ctx.fillText(formatQty(lvl.buy), midX + 3, py);
                    } else if (fpMode === 'delta') {
                        ctx.fillStyle = lvlDelta >= 0 ? '#00e676' : '#ff5252';
                        ctx.textAlign = 'center';
                        const pullTxt = lvl.pulled_qty > 0 ? ` [P:${formatQty(lvl.pulled_qty)}]` : '';
                        ctx.fillText(`${lvlDelta >= 0 ? '+' : ''}${formatQty(lvlDelta)}${pullTxt}`, midX, py);
                    } else {
                        ctx.fillStyle = '#ffffff';
                        ctx.textAlign = 'center';
                        const pullTxt = lvl.pulled_qty > 0 ? ` P:${formatQty(lvl.pulled_qty)}` : '';
                        ctx.fillText(`${formatQty(lvl.volume)}${pullTxt}`, midX, py);
                    }
                }
            });

        } else {
            // COMPACT CANDLE MODE (When Zoomed Out)
            const bodyW = Math.max(candleSpacing * 0.65, 3);
            ctx.strokeStyle = mainColor;
            ctx.lineWidth = 1;
            ctx.beginPath();
            ctx.moveTo(cx, yHigh);
            ctx.lineTo(cx, yLow);
            ctx.stroke();

            ctx.fillStyle = mainColor;
            const bTop = Math.min(yOpen, yClose);
            const bH = Math.max(Math.abs(yClose - yOpen), 2);
            ctx.fillRect(cx - bodyW / 2, bTop, bodyW, bH);

            if (candle.poc != null) {
                const yPoc = priceToY(candle.poc);
                ctx.fillStyle = '#ffeb3b';
                ctx.fillRect(cx - bodyW / 2 - 1, yPoc - 1, bodyW + 2, 2.5);
            }
        }

        // 3. BOTTOM DELTA & VOLUME STATS BAR (Exocharts Footer)
        const footerTop = chartH + 2;
        ctx.fillStyle = '#131722';
        ctx.fillRect(cx - candleSpacing / 2, footerTop, candleSpacing, DELTA_PANEL_H - 2);
        ctx.strokeStyle = '#2a2e39';
        ctx.strokeRect(cx - candleSpacing / 2, footerTop, candleSpacing, DELTA_PANEL_H - 2);

        if (candleSpacing >= 45) {
            ctx.font = '10px monospace';
            ctx.textAlign = 'center';
            ctx.textBaseline = 'top';

            // Row 1: Delta
            ctx.fillStyle = candle.delta >= 0 ? '#089981' : '#f23645';
            ctx.fillText(`Δ ${formatQty(candle.delta)}`, cx, footerTop + 6);

            // Row 2: Volume
            ctx.fillStyle = '#d1d4dc';
            ctx.fillText(`V ${formatQty(candle.volume)}`, cx, footerTop + 24);

            // Row 3: POC
            ctx.fillStyle = '#ffeb3b';
            ctx.fillText(`POC ${candle.poc ? candle.poc.toFixed(decimals) : '--'}`, cx, footerTop + 42);
        }

        // 4. TIME AXIS LABELS
        const timeY = H - TIME_AXIS_H + 5;
        if (candleSpacing >= 55) {
            const dt = new Date(candle.start);
            const tStr = dt.toTimeString().slice(0, 5);
            ctx.fillStyle = '#787b86';
            ctx.font = '10px monospace';
            ctx.textAlign = 'center';
            ctx.fillText(tStr, cx, timeY);
        }
    });

    // 5. RIGHT-SIDE PRICE SCALE (Y-AXIS)
    ctx.fillStyle = '#131722';
    ctx.fillRect(chartW, 0, PRICE_AXIS_W, H);
    ctx.strokeStyle = '#2a2e39';
    ctx.beginPath();
    ctx.moveTo(chartW, 0);
    ctx.lineTo(chartW, H);
    ctx.stroke();

    ctx.fillStyle = '#9ba2b0';
    ctx.font = '11px monospace';
    ctx.textAlign = 'left';
    ctx.textBaseline = 'middle';
    for (let i = 0; i <= gridSteps; i++) {
        const y = (chartH / gridSteps) * i;
        const p = yToPrice(y);
        ctx.fillText(p.toFixed(decimals), chartW + 6, y);
    }

    // Current Live Price Line & Tag on Y-Axis
    if (candles.length > 0) {
        const last = candles[candles.length - 1];
        const curY = priceToY(last.close);
        if (curY >= 0 && curY <= chartH) {
            const isUp = last.close >= last.open;
            ctx.strokeStyle = isUp ? '#089981' : '#f23645';
            ctx.setLineDash([4, 4]);
            ctx.beginPath();
            ctx.moveTo(0, curY);
            ctx.lineTo(chartW, curY);
            ctx.stroke();
            ctx.setLineDash([]);

            ctx.fillStyle = isUp ? '#089981' : '#f23645';
            ctx.fillRect(chartW, curY - 10, PRICE_AXIS_W, 20);
            ctx.fillStyle = '#ffffff';
            ctx.font = 'bold 11px monospace';
            ctx.fillText(last.close.toFixed(decimals), chartW + 6, curY);
        }
    }

    // 6. CROSSHAIR & HOVER INSPECTOR
    if (mouseX > 0 && mouseX < chartW && mouseY > 0 && mouseY < chartH) {
        ctx.strokeStyle = 'rgba(209, 212, 220, 0.35)';
        ctx.setLineDash([3, 3]);
        ctx.beginPath();
        ctx.moveTo(mouseX, 0);
        ctx.lineTo(mouseX, chartH);
        ctx.moveTo(0, mouseY);
        ctx.lineTo(chartW, mouseY);
        ctx.stroke();
        ctx.setLineDash([]);

        const hoverPrice = yToPrice(mouseY);
        ctx.fillStyle = '#2a2e39';
        ctx.fillRect(chartW, mouseY - 10, PRICE_AXIS_W, 20);
        ctx.fillStyle = '#fff';
        ctx.fillText(hoverPrice.toFixed(decimals), chartW + 6, mouseY);
    }
}

function formatQty(val) {
    if (val === undefined || val === null) return '0';
    const abs = Math.abs(val);
    if (abs >= 1000) return (val / 1000).toFixed(1) + 'K';
    if (abs >= 100) return val.toFixed(0);
    if (abs >= 1) return val.toFixed(1);
    return val.toFixed(2);
}

// Fast 1-second live polling without memory leak
setInterval(fetchAll, 1000);
fetchAll();
</script>
</body>
</html>"""

@app.route("/hello")
def hello():
    return "SA Footprint Engine OK"


@app.route("/api/test")
def api_test():
    return jsonify({"status": "ok", "message": "API working", "version": "V3"})


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
            "version": "V3",
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
        limit = max(1, min(int(request.args.get("limit", 20)), 100))
    except ValueError:
        limit = 20

    with lock:
        state = orderbook[symbol]
        bids = sorted(state["bids"].items(), key=lambda x: float(x[0]), reverse=True)[:limit]
        asks = sorted(state["asks"].items(), key=lambda x: float(x[0]))[:limit]
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
        trades = list(state["trade_flow_diagnostics"])
        diagnostics = list(state["match_diagnostics"])

        finalized = [r for r in history if r.get("finalized") is True]
        executed = [r for r in finalized if float(r.get("executed_qty") or 0) > 0]
        pulled = [r for r in finalized if float(r.get("pulled_qty") or 0) > 0]
        reductions = [r for r in history if float(r.get("reduced_qty") or 0) > 0]

        def short_record(r):
            fc = r.get("fifo_consumption") or []
            return {
                "time": r.get("time"),
                "price": r.get("price"),
                "side": r.get("side"),
                "reduced_qty": r.get("reduced_qty"),
                "executed_qty": r.get("executed_qty"),
                "pulled_qty": r.get("pulled_qty"),
                "status": r.get("status"),
                "finalized": r.get("finalized"),
                "fifo_consumed_qty": r.get("fifo_consumed_qty"),
                "fifo_executed_qty": r.get("fifo_executed_qty"),
                "fifo_unattributed_qty": r.get("fifo_unattributed_qty"),
                "fifo_unmatched_qty": r.get("fifo_unmatched_qty"),
                "fifo_consumption_count": len(fc),
                "fifo_consumption": fc[:5],
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
                "trade_diagnostic_entries": len(trades),
                "match_diagnostic_entries": len(diagnostics),
            },
            "execution_examples": [short_record(r) for r in executed[-3:]],
            "recent_reductions": [short_record(r) for r in reductions[-3:]],
            "trade_examples": trades[-3:],
            "match_diagnostics": diagnostics[-3:],
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


if __name__ == "__main__":
    ensure_collector_started()
    app.run(host="0.0.0.0", port=10000, debug=False)
