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
            "bid_pulled": 0.0,
            "ask_pulled": 0.0,
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
    """Stamps Bid-Pull and Ask-Pull separately near active candle price range."""
    step = PRICE_STEP.get(symbol, 1.0)
    try:
        p_float = float(price_str)
    except Exception:
        return
    bucket_price = round_price_to_step(p_float, step)
    key = str(bucket_price)
    max_distance = step * 25

    with lock:
        for tf in TIMEFRAMES.keys():
            curr = current_candles[symbol][tf]
            if curr is not None and "footprint" in curr:
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
                        "bid_pulled": 0.0,
                        "ask_pulled": 0.0,
                        "status": "NORMAL",
                    }
                    curr["footprint"][key] = level

                if pulled_qty > 0.0:
                    p_val = float(pulled_qty)
                    level["pulled_qty"] = round(float(level.get("pulled_qty", 0.0)) + p_val, 4)
                    if side == "bid":
                        level["bid_pulled"] = round(float(level.get("bid_pulled", 0.0)) + p_val, 4)
                    else:
                        level["ask_pulled"] = round(float(level.get("ask_pulled", 0.0)) + p_val, 4)

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

@app.route("/")
def home():
    return """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no">
<title>SA Institutional Footprint & Macro Decision Engine V4.1</title>
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
        min-height: 36px;
        background: #11151f;
        border-bottom: 1px solid #232838;
        display: flex;
        align-items: center;
        padding: 2px 8px;
        gap: 5px;
        font-size: 11px;
        overflow-x: auto;
        white-space: nowrap;
        scrollbar-width: none;
        flex-shrink: 0;
    }
    .topbar::-webkit-scrollbar, .decision-bar::-webkit-scrollbar { display: none; }
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
        scrollbar-width: none;
        flex-shrink: 0;
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
        min-width: 300px;
        padding: 3px 10px;
        border-radius: 3px;
        font-weight: bold;
        font-size: 11px;
        display: flex;
        justify-content: space-between;
        align-items: center;
        gap: 10px;
        border: 1px solid #2c3347;
        background: #161b26;
    }
    .brand { color: #00e5ff; font-weight: bold; margin-right: 4px; font-size: 12px; flex-shrink: 0; }
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
        background: rgba(41, 98, 255, 0.92);
        color: #ffffff;
        border: 1px solid #00e5ff;
        border-right: none;
        padding: 12px 5px;
        border-radius: 6px 0 0 6px;
        cursor: pointer;
        font-weight: bold;
        font-size: 11px;
        writing-mode: vertical-rl;
        text-orientation: mixed;
        letter-spacing: 1px;
        box-shadow: -3px 0 10px rgba(0,0,0,0.6);
        transition: right 0.15s ease, background 0.15s ease;
    }
    .dom-slide-tab:hover { background: #00b0ff; color: #000; }

    #ruleModal {
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
        -webkit-overflow-scrolling: touch;
    }
    .rule-sec {
        background: #131824;
        border: 1px solid #232a3d;
        border-radius: 4px;
        padding: 10px 12px;
        margin-bottom: 10px;
    }
    .rule-sec h4 { color: #ffeb3b; margin-bottom: 6px; font-size: 13px; border-bottom: 1px solid #232a3d; padding-bottom: 4px; }
</style>
</head>
<body>

<div class="topbar">
    <span class="brand">⚡ SA V4.1</span>
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
    <button class="btn active" id="viewModeBtn" onclick="toggleViewMode()" title="Switch between Full Pro Tags and Clean Footprint">👁️ PRO</button>
    <button class="btn" id="domToggleBtn" onclick="toggleDOM()">DOM</button>
    <button class="btn" onclick="toggleRules()" style="border-color:#ffeb3b;color:#ffeb3b;font-weight:bold;">📖 RULES</button>

    <div class="legend">
        <span class="lg" style="background:#00e5ff;color:#000;">H-B.ABS</span>
        <span class="lg" style="background:#ff9100;color:#000;">H-A.ABS</span>
        <span class="lg" style="background:#d500f9;color:#fff;">H-BP</span>
        <span class="lg" style="background:#aa00ff;color:#fff;">H-AP</span>
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
        <span id="bmHeavy" style="color:#ffeb3b;font-weight:bold;">HeavyC:>--</span>
        <span style="color:#444;">|</span>
        <span id="bmLvl" style="color:#00e5ff;font-weight:bold;">HeavyL:>--</span>
        <span style="color:#444;">|</span>
        <span id="bmCur" style="color:#fff;">Cur:--</span>
    </div>
    <div class="smt-box" id="smtRadarBox" title="Real-Time BTC vs ETH Cross-Market SMT & Orderflow Radar">
        🔗 SMT: SCANNING...
    </div>
    <div class="verdict-box" id="masterVerdictBox">
        <span id="masterVerdictTitle">SCANNING MACRO MARKET ORDERFLOW...</span>
        <span id="masterVerdictReasons" style="font-size:10px;font-weight:normal;color:#cfd8dc;">Calculating multi-candle structure...</span>
    </div>
</div>

<div id="wrap">
    <canvas id="cv"></canvas>
    <div id="domSlideTab" class="dom-slide-tab" onclick="toggleDOM()" title="Slide Orderbook Open/Close">❮ DOM</div>
</div>

<!-- BUILT-IN COMPLETE MASTER RULEBOOK POPUP (UPDATED WITH V4.1 DECISION MATRIX) -->
<div id="ruleModal">
    <div style="display:flex; justify-content:space-between; align-items:center; border-bottom:1px solid #00e5ff; padding-bottom:8px; margin-bottom:12px; position:sticky; top:0; background:#0b0e16; z-index:2;">
        <b style="color:#00e5ff; font-size:14px;">⚡ SA ORDERFLOW & FOOTPRINT V4.1 — MASTER EXECUTION RULEBOOK</b>
        <button class="btn" onclick="toggleRules()" style="background:#ff1744;color:#fff;border:none;padding:4px 12px;font-weight:bold;">✕ CLOSE</button>
    </div>

    <div class="rule-sec">
        <h4>1. TOP MACRO DECISION ENGINE (9-Factor Multi-Candle Institutional Matrix)</h4>
        Top Macro Bar pichli 20 candles ke <b>9 Institutional Parameters</b> ko jod kar final market bias deta hai:<br>
        1) <b>POC Migration</b> &nbsp;2) <b>Macro CVD vs Price Divergence</b> &nbsp;3) <b>Bid/Ask Absorption Floors & Ceilings</b> &nbsp;4) <b>Net Liquidity Pulled (Spoofs)</b> &nbsp;5) <b>Live DOM Depth</b> &nbsp;6) <b style="color:#ffd700;">◆ Whale Lot Buy vs Sell Dominance</b> &nbsp;7) <b style="color:#00e5ff;">⚡ BAAP-FVG & Stacked Imbalance Count</b> &nbsp;8) <b style="color:#00e5ff;">🔗 BTC/ETH SMT Divergence</b> &nbsp;9) <b style="color:#ffeb3b;">◀ nPOC / UNF Magnet Gravity</b>.<br><br>
        • <b style="color:#00e676;">🟢 MACRO BULLISH DOMINANCE:</b> Price + POC + Whale Lots + Stacked Imbalances sab upar ki taraf aligned hain (Look for Longs).<br>
        • <b style="color:#00e5ff;">🐳 MACRO BULL ACCUMULATION:</b> Pichli 20 candles mein net selling (-CVD) aayi hai, par Whales ne Limit Buy + ◆ Whale Lots se saara maal absorb kar liya aur price girne nahi diya (Strongest Reversal Long Setup).<br>
        • <b style="color:#ff5252;">🔴 MACRO BEARISH DOMINANCE:</b> Price + POC + Whale Lots + Stacked Sell Imbalances neeche ki taraf hain (Look for Shorts).<br>
        • <b style="color:#ff9100;">🐳 MACRO BEAR DISTRIBUTION:</b> Retail lagatar Market Buy (+CVD) maar raha hai, par Whales Limit Sell se top par maal chipka rahe hain (Reversal Short Setup).<br>
        • <b style="color:#ffca28;">🟡 MACRO SIDEWAYS ROTATIONAL CHOP:</b> Trend efficiency &lt; 16% hai. Mid-range mein trade mat le, sirf Range High/Low sweep hone par fade kar.
    </div>

    <div class="rule-sec">
        <h4>2. INSIDE CANDLE FOOTPRINT & 8 SIDE-SPECIFIC TAGS (Left = Bid/Sell | Right = Ask/Buy)</h4>
        • <b>Center Spine Split:</b> Spine ke Left mein <b>Market Sell (hitting Bids)</b> aur Right mein <b>Market Buy (hitting Asks)</b> hota hai.<br>
        • <b style="color:#ffeb3b;">POC (Yellow Box):</b> Candle ka sabse heavy volume price. Candle POC ke <b>upar</b> band ho = Buyers won; POC ke <b>neeche</b> band ho = Sellers won.<br>
        • <b style="color:#29b6f6;">VAH & VAL (Blue Lines):</b> Value Area High & Low (70% volume zone).<br>
        • <b style="color:#00e5ff;">H-B.ABS (xx%) [Bright Cyan - Left]:</b> <b>Heavy Bid Absorption.</b> Aggressive Sellers ne sell mara, par Passive Limit Buyers ne <code>xx%</code> maal absorb kar liya = <b>STRONG INSTITUTIONAL FLOOR (BUY)</b>.<br>
        • <b style="color:#ff9100;">H-A.ABS (xx%) [Orange/Gold - Right]:</b> <b>Heavy Ask Absorption.</b> Aggressive Buyers ne buy mara, par Passive Limit Sellers ne <code>xx%</code> maal absorb karke rok liya = <b>STRONG INSTITUTIONAL CEILING (SELL)</b>.<br>
        • <b style="color:#ff4081;">H-BP:qty (xx%) [Neon Pink - Left]:</b> <b>Heavy Bid Pulled (Spoof).</b> Support par rakhe Buy orders price aate hi cancel kar liye gaye = <b>FAKE SUPPORT (Floor khali hai, price neeche tootega!)</b>.<br>
        • <b style="color:#e040fb;">H-AP:qty (xx%) [Violet - Right]:</b> <b>Heavy Ask Pulled (Spoof).</b> Resistance par rakhe Sell orders cancel kar liye gaye = <b>FAKE RESISTANCE (Rasta saaf hai, price upar phatega!)</b>.
    </div>

    <div class="rule-sec" style="border-color:#ffd700;">
        <h4 style="color:#ffd700;">3. V4 STEALTH WEAPONS (Whale Lots, Stacked Imb Bracket, nPOC/UNF Magnets & SMT)</h4>
        • <b style="color:#ffd700;">◆ Gold Diamond (Whale Lot Size Detector):</b> Jab kisi price cell mein bada volume <b>bahut kam trades (Big Average Trade Size &gt;= 2.0x BaseLot)</b> mein execute hota hai, toh number ke paas <b style="color:#ffd700;">◆</b> chhapta hai. Yeh prove karta hai ki wahan Retail bheed nahi, balki <b>Institutional Whale / Liquidation Engine</b> ne order mara hai!<br>
        • <b style="color:#00e5ff;">Stacked Imbalance Neon Bracket (Left Outer Edge):</b> Jab किसी candle mein lagatar <b>2 ya 3+ price levels par ek-tarfa 240% Imbalance</b> aata hai, toh candle ke left kinare ke bahar ek patli <b>Cyan (Buy Stack)</b> ya <b>Pink (Sell Stack)</b> bracket line banti hai (bina candle ko chhupaye!). Aur agar wahan FVG hai, toh Floating Pill mein <b style="color:#00e676;">⚡ BAAP-FVG</b> ya <b style="color:#ff9100;">⚠️ BETA-FVG</b> print hota hai!<br>
        • <b style="color:#ffeb3b;">◀ nPOC</b> & <b style="color:#e040fb;">◀ UNF (Y-Axis Price Scale Laser Magnets):</b><br>
          &nbsp;&nbsp;- <b style="color:#ffeb3b;">◀ nPOC (Naked POC):</b> Pichli candles ka untouched POC. Jaise hi future mein price isko touch karega, yeh pin automatic hat jayega. Jab tak yeh dikh raha hai, yeh <b>Pending Price Magnet</b> hai!<br>
          &nbsp;&nbsp;- <b style="color:#e040fb;">◀ UNF (Unfinished Auction):</b> Pichli candle ka High/Low jahan Buy aur Sell dono <code>&gt; 0</code> chhoot gaye. Market 85% cases mein aisi Unfinished High/Low ko sweep karne wapas aata hai, aur sweep hote hi yeh pin hat jata hai!<br>
        • <b>Interactive Scales:</b> <b>Right Price Scale</b> ko upar-neeche drag karne se Y-axis zoom hota hai, aur <b>Bottom Time Scale</b> ko left-right drag karne se X-axis (Candle Width) phailta aur sikudta hai!
    </div>

    <div class="rule-sec">
        <h4>4. THE 4-LINE FLOATING PILL (V4.1 Upgraded Candle Decision Matrix)</h4>
        • <b>Line 1 (Δ | V | RVOL x):</b> Agar RVOL <code>&lt; 0.70x</code> hai toh <code>LOW FLOW</code> aayega—aisi halka volume candle ke signal par kabhi trade mat lena!<br>
        • <b>Line 2 (CVD | B vs S):</b> Running Cumulative Delta aur us candle ka exact Market Buy (<code>B</code>) vs Market Sell (<code>S</code>) volume.<br>
        • <b>Line 3 (Mechanism + FVG + Magnet Sweep):</b> <code>🐳LIM-BUY ACCUM</code>, <code>🔥MKT IGNITION</code>, <code>⚡BAAP-FVG</code>, ya <code>🎯MAG-SWEPT</code> (jab candle ne purana nPOC/UNF sweep kiya ho)!<br>
        • <b>Line 4 (V4.1 Final Candle Verdict):</b><br>
          &nbsp;&nbsp;- <b style="color:#ffd700;">◆🐳 WHALE BULL ABSORB (xx%):</b> Whale lot + Passive Bid Absorption + Green Close (Sellers 100% Trapped = Best Long Trigger!).<br>
          &nbsp;&nbsp;- <b style="color:#00e676;">◆🟢 WHALE BUY WIN (xx%):</b> Whale lots + Stacked Buy Imbalance + Close above POC.<br>
          &nbsp;&nbsp;- <b style="color:#ff9100;">🐑 RETAIL FOMO TRAP:</b> Volume aaya par saare chote-chote retail trades the (AvgLot &lt; 0.75x) aur price reject ho gaya = Fakeout!
    </div>

    <div class="rule-sec" style="border-color:#00e676;">
        <h4 style="color:#00e676;">🎯 MASTER SNIPER CHECKLIST (FVG / OB / SWEEP ENTRY)</h4>
        <b>1. Macro & Location Check:</b> Top Bar mein Macro Bias tere trade ki direction mein ho (ya <code>🐳 ACCUM/DIST</code> reversal de raha ho) aur FVG ke paas <b style="color:#ffeb3b;">◀ nPOC</b> / <b style="color:#e040fb;">◀ UNF</b> ya <b style="color:#00e676;">⚡ BAAP-FVG</b> ho.<br>
        <b>2. Reaction Check:</b> Jab price FVG mein tap kare, toh wahan <b style="color:#00e5ff;">H-B.ABS</b> (Long) ya <b style="color:#ff9100;">H-A.ABS</b> (Short) + <b style="color:#ffd700;">◆ Whale Lot</b> dikhna chahiye.<br>
        <b>3. Trigger Check:</b> Candle pill ki Line 4 mein <b style="color:#00e676;">◆🐳 WHALE BULL ABSORB / BUYERS WIN</b> (Long) ya <b style="color:#ff5252;">◆🐳 WHALE BEAR ABSORB / SELLERS WIN</b> (Short) chhapna chahiye!
    </div>
</div>

<script>
const wrap = document.getElementById('wrap');
const cv = document.getElementById('cv');
const ctx = cv.getContext('2d', { alpha: false });
const slideTab = document.getElementById('domSlideTab');
const ruleModal = document.getElementById('ruleModal');

function toggleRules() {
    ruleModal.style.display = (ruleModal.style.display === 'block') ? 'none' : 'block';
}

const STEP_MAP = { BTCUSDT: 1.0, ETHUSDT: 0.1, SOLUSDT: 0.01, XRPUSDT: 0.0001 };

let symbol = document.getElementById('symSel').value || 'BTCUSDT';
let tf = '1m';
let tickMultMode = document.getElementById('tickMultSel').value || 'auto';
let proViewMode = true;
let candles = [];
let peerCandles = [];
let smtState = { bias: 0, text: 'SYNCED' }; // +1 = Bullish SMT, -1 = Bearish SMT, 0 = Synced
let obData = { bids: [], asks: [] };
let rawStep = STEP_MAP[symbol] || 1.0;

let isMobile = window.innerWidth < 768;
let showDOM = !isMobile;
let candleW = isMobile ? 165 : 235;
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
const PILL_SAFE_H = 66;

function toggleViewMode() {
    proViewMode = !proViewMode;
    const btn = document.getElementById('viewModeBtn');
    btn.innerText = proViewMode ? '👁️ PRO' : '👁️ CLEAN';
    btn.classList.toggle('active', proViewMode);
    needsRender = true;
}

function updateSlideTabUI() {
    isMobile = cv.width < 768;
    const domW = !showDOM ? 0 : (isMobile ? Math.min(200, Math.floor(cv.width * 0.52)) : 300);
    const btn = document.getElementById('domToggleBtn');
    if (showDOM) {
        slideTab.style.right = domW + 'px';
        slideTab.innerText = '❯ HIDE';
        btn.innerText = 'DOM: ON';
        btn.classList.add('active');
    } else {
        slideTab.style.right = '0px';
        slideTab.innerText = '❮ DOM';
        btn.innerText = 'DOM: OFF';
        btn.classList.remove('active');
    }
}

function getLayout() {
    isMobile = cv.width < 768;
    const yAxisW = isMobile ? 62 : 78;
    const domW = !showDOM ? 0 : (isMobile ? Math.min(200, Math.floor(cv.width * 0.52)) : 300);
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
    candleW = Math.max(35, Math.min(480, candleW * factor));
    needsRender = true;
}
function resetCamera() {
    panX = 0;
    minP = null;
    maxP = null;
    domScrollRows = 0;
    candleW = window.innerWidth < 768 ? 165 : 235;
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
        candleW = Math.max(35, Math.min(480, candleW * (1 + dx * 0.006)));
    } else if (dragMode === 'yaxis') {
        if (minP !== null && maxP !== null) {
            const mid = (maxP + minP) * 0.5;
            let half = (maxP - minP) * 0.5 * (1 + dy * 0.006);
            if (half >= rawStep * 2) { minP = mid - half; maxP = mid + half; }
        }
    } else if (dragMode === 'dom') {
        domScrollRows += (dy / 14);
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
    const x = e.clientX - r.left;
    const y = e.clientY - r.top;
    if (detectZone(x, y) === 'dom') {
        domScrollRows = 0;
        needsRender = true;
    } else {
        resetCamera();
    }
});

cv.addEventListener('wheel', e => {
    e.preventDefault();
    const { chartW, yAxisW, chartH } = getLayout();
    const zone = detectZone(curX, curY);
    const zoomIn = e.deltaY < 0;

    if (zone === 'dom') {
        domScrollRows += (e.deltaY > 0 ? -3 : 3);
    } else if (zone === 'yaxis') {
        scaleY(zoomIn ? 0.84 : 1.19);
    } else if (zone === 'xaxis') {
        scaleX(zoomIn ? 1.15 : 0.87);
    } else {
        const zx = zoomIn ? 1.15 : 0.87;
        candleW = Math.max(35, Math.min(480, candleW * zx));
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
        curX = lastX;
        curY = lastY;
        dragMode = detectZone(lastX, lastY);
        pinchDist = null;
    } else if (e.touches.length === 2) {
        dragMode = null;
        const dx = e.touches[0].clientX - e.touches[1].clientX;
        const dy = e.touches[0].clientY - e.touches[1].clientY;
        pinchDist = Math.hypot(dx, dy);
    }
}, { passive: false });

cv.addEventListener('touchmove', e => {
    e.preventDefault();
    const r = cv.getBoundingClientRect();
    if (e.touches.length === 1 && dragMode) {
        const tx = e.touches[0].clientX - r.left;
        const ty = e.touches[0].clientY - r.top;
        applyDrag(tx - lastX, ty - lastY);
        lastX = tx;
        lastY = ty;
        curX = tx;
        curY = ty;
    } else if (e.touches.length === 2 && pinchDist !== null) {
        const dx = e.touches[0].clientX - e.touches[1].clientX;
        const dy = e.touches[0].clientY - e.touches[1].clientY;
        const newDist = Math.hypot(dx, dy);
        if (newDist > 10) {
            const ratio = newDist / pinchDist;
            candleW = Math.max(35, Math.min(480, candleW * ratio));
            scaleY(1 / ratio);
            pinchDist = newDist;
            needsRender = true;
        }
    }
}, { passive: false });

cv.addEventListener('touchend', () => {
    dragMode = null;
    pinchDist = null;
});

let fetching = false;
async function fetchData() {
    if (fetching) return;
    fetching = true;
    try {
        const [cRes, obRes] = await Promise.all([
            fetch(`/api/candles?symbol=${symbol}&timeframe=${tf}&limit=30`),
            fetch(`/api/orderbook?symbol=${symbol}&limit=1000`)
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

    const c3Delta = curSet.slice(-2).reduce((a, b) => a + (b.delta || 0), 0);
    const p3Delta = peerSet.slice(-2).reduce((a, b) => a + (b.delta || 0), 0);

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
    } else if ((c3Delta > 0 && p3Delta < 0) || (c3Delta < 0 && p3Delta > 0)) {
        smtState = { bias: c3Delta > 0 ? 0.5 : -0.5, text: `Delta SMT Split` };
        el.style.background = 'rgba(213, 0, 249, 0.16)';
        el.style.borderColor = '#e040fb';
        el.style.color = '#ea80fc';
        el.innerText = `⚡ DELTA SMT: ${shortSym}/${shortPeer} Split`;
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

function groupFootprint(fpRaw, low, high, effStep, closePx) {
    const map = new Map();
    const arr = Array.isArray(fpRaw) ? fpRaw : Object.values(fpRaw || {});
    arr.forEach(l => {
        if (l.price < low - effStep * 2 || l.price > high + effStep * 2) return;
        const bucket = Math.floor(l.price / effStep + 1e-9) * effStep;
        const key = bucket.toFixed(6);
        let g = map.get(key);
        if (!g) {
            g = { price: bucket, buy: 0, sell: 0, volume: 0, trades: 0, bid_pulled: 0, ask_pulled: 0, pulled_qty: 0 };
            map.set(key, g);
        }
        g.buy += (l.buy || 0);
        g.sell += (l.sell || 0);
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
        g.pulled_qty += (bp + ap);
    });
    return Array.from(map.values()).sort((a, b) => a.price - b.price);
}

// =========================================================================
// V4.1 UPGRADED 9-FACTOR MACRO INSTITUTIONAL DECISION ENGINE (TOP BAR)
// Integrates POC + CVD + Absorptions + Pulls + DOM + Whale Lots + Baap FVGs + SMT + nPOC/UNF Magnets!
// =========================================================================
function updateMacroMarketDecision(avgFlowVol, effStep, baselineLotSize, nakedPocs, unfinishedLvls) {
    if (!candles || candles.length < 3) return;

    const sample = candles.slice(-20);
    const last = sample[sample.length - 1];
    const first = sample[0];
    const curRvol = (last.volume || 0) / avgFlowVol;

    const lastFp = groupFootprint(last.footprint, last.low, last.high, effStep, last.close);
    const avgLvlVol = lastFp.length > 0 ? (avgFlowVol / Math.max(6, lastFp.length)) : avgFlowVol * 0.15;
    document.getElementById('bmAvg').innerText = `Avg:${fmt(avgFlowVol)}`;
    document.getElementById('bmHeavy').innerText = `HeavyC:>${fmt(avgFlowVol * 1.35)}`;
    document.getElementById('bmLvl').innerText = `HeavyL:>${fmt(avgLvlVol * 1.85)}`;
    document.getElementById('bmCur').innerText = `Cur:${fmt(last.volume)}(${curRvol.toFixed(1)}x)`;
    document.getElementById('bmCur').style.color = curRvol >= 1.3 ? '#00e676' : '#d1d4dc';

    let macroHigh = -Infinity, macroLow = Infinity;
    let sumCandleRanges = 0;
    let totalCvd = 0, totalVol = 0;
    let pocUpSteps = 0, pocDownSteps = 0;
    let macroBidAbsorbs = 0, macroAskAbsorbs = 0;
    let macroBidPulled = 0, macroAskPulled = 0;
    let whaleBuyLots = 0, whaleSellLots = 0;
    let bullBaapFvgs = 0, bearBaapFvgs = 0;

    sample.forEach((c, idx) => {
        if (c.high > macroHigh) macroHigh = c.high;
        if (c.low < macroLow) macroLow = c.low;
        const cr = Math.max(c.high - c.low, rawStep);
        sumCandleRanges += cr;
        totalCvd += (c.delta || 0);
        totalVol += (c.volume || 0);

        if (idx > 0 && c.poc && sample[idx - 1].poc) {
            if (c.poc > sample[idx - 1].poc) pocUpSteps++;
            else if (c.poc < sample[idx - 1].poc) pocDownSteps++;
        }

        const cRvol = (c.volume || 0) / avgFlowVol;
        const closePos = (c.close - c.low) / cr;
        const mbPct = (c.volume > 0) ? ((c.buy_volume || 0) / c.volume) * 100 : 50;
        const msPct = 100 - mbPct;

        if (cRvol >= 1.1) {
            if (msPct >= 56 && closePos >= 0.52) macroBidAbsorbs += cRvol;
            if (mbPct >= 56 && closePos <= 0.48) macroAskAbsorbs += cRvol;
        }

        // Scan V4 Whale Lots & Stacked Imbalances across the 20-candle macro window
        const fpArr = Array.isArray(c.footprint) ? c.footprint : Object.values(c.footprint || {});
        const cAvgCellV = fpArr.length > 0 ? (c.volume || 1) / fpArr.length : 1;
        let bStack = 0, sStack = 0, maxBS = 0, maxSS = 0, cWhales = 0;

        fpArr.forEach(l => {
            macroBidPulled += (l.bid_pulled || 0);
            macroAskPulled += (l.ask_pulled || 0);

            const lv = l.volume || 0;
            const lt = Math.max(l.trades || 1, 1);
            if (lv >= cAvgCellV * 0.85 && (lv / lt) >= baselineLotSize * 2.0) {
                cWhales++;
                if ((l.buy || 0) > (l.sell || 0)) whaleBuyLots++;
                else if ((l.sell || 0) > (l.buy || 0)) {
                    // If heavy whale sell lot happened in lower half of a green/defended candle, it's Passive Whale Bid Absorption!
                    if (c.close >= c.open && l.price <= (c.high + c.low) * 0.5) whaleBuyLots++;
                    else whaleSellLots++;
                }
            }

            if ((l.buy || 0) > (l.sell || 0) * 2.4 && (l.buy || 0) >= cAvgCellV * 0.45) {
                bStack++; sStack = 0; if (bStack > maxBS) maxBS = bStack;
            } else if ((l.sell || 0) > (l.buy || 0) * 2.4 && (l.sell || 0) >= cAvgCellV * 0.45) {
                sStack++; bStack = 0; if (sStack > maxSS) maxSS = sStack;
            } else { bStack = 0; sStack = 0; }
        });

        // Count Institutional BAAP FVGs in macro window
        if (idx >= 1 && idx < sample.length - 1) {
            const pC = sample[idx - 1], nC = sample[idx + 1];
            if (nC.low > pC.high && (maxBS >= 2 || cWhales >= 1) && cRvol >= 1.05) bullBaapFvgs++;
            if (nC.high < pC.low && (maxSS >= 2 || cWhales >= 1) && cRvol >= 1.05) bearBaapFvgs++;
        }
    });

    const macroSpan = Math.max(macroHigh - macroLow, rawStep);
    const netPriceMove = last.close - first.open;
    const efficiency = Math.abs(netPriceMove) / Math.max(sumCandleRanges, rawStep);
    const posInMacroRange = (last.close - macroLow) / macroSpan;

    let bullPts = 0, bearPts = 0;
    let macroNotes = [];

    // 1. POC Migration
    if (netPriceMove > macroSpan * 0.22 && pocUpSteps > pocDownSteps + 1) {
        bullPts += 24;
        macroNotes.push(`POC Up(${pocUpSteps}↑)`);
    } else if (netPriceMove < -macroSpan * 0.22 && pocDownSteps > pocUpSteps + 1) {
        bearPts += 24;
        macroNotes.push(`POC Down(${pocDownSteps}↓)`);
    }

    // 2. Macro CVD vs Price Structure
    const cvdRatio = totalVol > 0 ? (totalCvd / totalVol) * 100 : 0;
    if (totalCvd > 0 && netPriceMove > macroSpan * 0.15) {
        bullPts += 20;
        macroNotes.push(`CVD +${fmt(totalCvd)}`);
    } else if (totalCvd < 0 && netPriceMove < -macroSpan * 0.15) {
        bearPts += 20;
        macroNotes.push(`CVD ${fmt(totalCvd)}`);
    } else if (totalCvd < -avgFlowVol * 0.8 && netPriceMove >= -macroSpan * 0.10) {
        bullPts += 32;
        macroNotes.push(`Whale Bid Absorb(CVD ${fmt(totalCvd)} Held)`);
    } else if (totalCvd > avgFlowVol * 0.8 && netPriceMove <= macroSpan * 0.10) {
        bearPts += 32;
        macroNotes.push(`Whale Ask Cap(CVD +${fmt(totalCvd)} Stalled)`);
    }

    // 3. V4 Whale Lot Dominance (◆)
    if (whaleBuyLots > whaleSellLots + 2) {
        bullPts += 20;
        macroNotes.push(`◆Whale Lots(${whaleBuyLots}B/${whaleSellLots}S)`);
    } else if (whaleSellLots > whaleBuyLots + 2) {
        bearPts += 20;
        macroNotes.push(`◆Whale Lots(${whaleSellLots}S/${whaleBuyLots}B)`);
    }

    // 4. V4 Institutional BAAP-FVG Dominance (⚡)
    if (bullBaapFvgs > bearBaapFvgs) {
        bullPts += 16;
        macroNotes.push(`⚡${bullBaapFvgs} Bull Baap-FVG`);
    } else if (bearBaapFvgs > bullBaapFvgs) {
        bearPts += 16;
        macroNotes.push(`⚡${bearBaapFvgs} Bear Baap-FVG`);
    }

    // 5. V4 Cross-Market SMT Divergence Boost (🚨)
    if (smtState.bias >= 1) {
        bullPts += 18;
        macroNotes.push(`🚨${smtState.text}`);
    } else if (smtState.bias <= -1) {
        bearPts += 18;
        macroNotes.push(`🚨${smtState.text}`);
    }

    // 6. Multi-Candle Floor vs Ceiling Defense & Spoofs
    if (macroBidAbsorbs > macroAskAbsorbs + 1.2) {
        bullPts += 16;
        macroNotes.push(`Bid Floors Held`);
    } else if (macroAskAbsorbs > macroBidAbsorbs + 1.2) {
        bearPts += 16;
        macroNotes.push(`Ask Ceilings Held`);
    }
    if (macroAskPulled > macroBidPulled * 1.35 && macroAskPulled > 0) {
        bullPts += 10;
        macroNotes.push(`Asks Pulled`);
    } else if (macroBidPulled > macroAskPulled * 1.35 && macroBidPulled > 0) {
        bearPts += 10;
        macroNotes.push(`Bids Pulled`);
    }

    // 7. Live DOM Imbalance
    let bookBids = 0, bookAsks = 0;
    const scanDist = effStep * 25;
    (obData.bids || []).forEach(b => { if (last.close - b.price <= scanDist) bookBids += b.quantity; });
    (obData.asks || []).forEach(a => { if (a.price - last.close <= scanDist) bookAsks += a.quantity; });
    if (bookBids > bookAsks * 1.30) {
        bullPts += 10;
        macroNotes.push(`DOM Bid Wall`);
    } else if (bookAsks > bookBids * 1.30) {
        bearPts += 10;
        macroNotes.push(`DOM Ask Wall`);
    }

    // 8. Pending Magnet Gravity (◀ nPOC & ◀ UNF Pull Direction)
    const magnetsAbove = nakedPocs.filter(p => p > last.close).length + unfinishedLvls.filter(u => u.price > last.close).length;
    const magnetsBelow = nakedPocs.filter(p => p < last.close).length + unfinishedLvls.filter(u => u.price < last.close).length;
    if (magnetsAbove > magnetsBelow + 1) macroNotes.push(`🎯MagPull:UP(${magnetsAbove})`);
    else if (magnetsBelow > magnetsAbove + 1) macroNotes.push(`🎯MagPull:DOWN(${magnetsBelow})`);

    const vBox = document.getElementById('masterVerdictBox');
    const vTitle = document.getElementById('masterVerdictTitle');
    const vReasons = document.getElementById('masterVerdictReasons');
    const netMacro = bullPts - bearPts;

    const isChopRange = efficiency < 0.16 && Math.abs(netMacro) < 28;

    if (isChopRange) {
        vBox.style.background = 'rgba(255, 193, 7, 0.15)';
        vBox.style.borderColor = '#ffca28';
        vTitle.style.color = '#ffca28';
        const zoneLoc = posInMacroRange > 0.7 ? 'Near High' : (posInMacroRange < 0.3 ? 'Near Low' : 'Mid-Range');
        vTitle.innerText = `🟡 MACRO: SIDEWAYS ROTATIONAL CHOP (${zoneLoc}) — FADE EXTREMES`;
        vReasons.innerText = macroNotes.slice(0, 4).join(' | ') || `Low efficiency (${(efficiency * 100).toFixed(0)}%) | Wait for sweep`;
    } else if (netMacro >= 18) {
        const conf = Math.min(97, Math.round(52 + (netMacro / 100) * 45));
        const isAccum = totalCvd < 0 && netPriceMove >= -macroSpan * 0.1;
        vBox.style.background = 'rgba(0, 200, 83, 0.18)';
        vBox.style.borderColor = '#00e676';
        vTitle.style.color = '#00e676';
        vTitle.innerText = isAccum
            ? `🐳 MACRO: WHALE BULL ACCUMULATION (${conf}%) — LONG BIAS`
            : `🟢 MACRO: BULLISH ORDERFLOW DOMINANCE (${conf}%) — LONG BIAS`;
        vReasons.innerText = macroNotes.slice(0, 4).join(' | ');
    } else if (netMacro <= -18) {
        const conf = Math.min(97, Math.round(52 + (Math.abs(netMacro) / 100) * 45));
        const isDist = totalCvd > 0 && netPriceMove <= macroSpan * 0.1;
        vBox.style.background = 'rgba(255, 23, 68, 0.18)';
        vBox.style.borderColor = '#ff5252';
        vTitle.style.color = '#ff5252';
        vTitle.innerText = isDist
            ? `🐳 MACRO: WHALE BEAR DISTRIBUTION (${conf}%) — SHORT BIAS`
            : `🔴 MACRO: BEARISH ORDERFLOW DOMINANCE (${conf}%) — SHORT BIAS`;
        vReasons.innerText = macroNotes.slice(0, 4).join(' | ');
    } else {
        vBox.style.background = 'rgba(38, 50, 56, 0.85)';
        vBox.style.borderColor = '#90a4ae';
        vTitle.style.color = '#eceff1';
        vTitle.innerText = `⚖️ MACRO: BALANCED TWO-WAY AUCTION — WAIT FOR DISPLACEMENT`;
        vReasons.innerText = macroNotes.slice(0, 4).join(' | ') || `CVD:${fmt(totalCvd)} | Equal control`;
    }
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

    let totalHistVol = 0, totalHistTrades = 0, totalHistRange = 0, histCount = 0;
    candles.forEach((hc, idx) => {
        if (idx < candles.length - 1 || candles.length === 1) {
            totalHistVol += (hc.volume || 0);
            totalHistTrades += (hc.trades || 1);
            totalHistRange += Math.max(hc.high - hc.low, rawStep);
            histCount++;
        }
    });
    const avgFlowVol = Math.max(totalHistVol / Math.max(1, histCount), 0.001);
    const avgFlowRange = Math.max(totalHistRange / Math.max(1, histCount), rawStep);
    const baselineLotSize = Math.max(totalHistVol / Math.max(1, totalHistTrades), 0.0001);

    const nakedPocs = [];
    const unfinishedLvls = [];
    for (let i = 0; i < candles.length - 1; i++) {
        const c = candles[i];
        if (c.poc != null) {
            let touched = false;
            for (let j = i + 1; j < candles.length; j++) {
                if (candles[j].low <= c.poc && candles[j].high >= c.poc) { touched = true; break; }
            }
            if (!touched) nakedPocs.push(c.poc);
        }
        const fpArr = Array.isArray(c.footprint) ? c.footprint : Object.values(c.footprint || {});
        if (fpArr.length > 2) {
            const sortedFp = [...fpArr].sort((a, b) => a.price - b.price);
            const lowCell = sortedFp[0];
            const highCell = sortedFp[sortedFp.length - 1];
            if (lowCell.buy > 0 && lowCell.sell > 0 && (lowCell.volume || 0) >= avgFlowVol * 0.08) {
                let swept = false;
                for (let j = i + 1; j < candles.length; j++) {
                    if (candles[j].low < lowCell.price) { swept = true; break; }
                }
                if (!swept) unfinishedLvls.push({ price: lowCell.price, type: 'LOW' });
            }
            if (highCell.buy > 0 && highCell.sell > 0 && (highCell.volume || 0) >= avgFlowVol * 0.08) {
                let swept = false;
                for (let j = i + 1; j < candles.length; j++) {
                    if (candles[j].high > highCell.price) { swept = true; break; }
                }
                if (!swept) unfinishedLvls.push({ price: highCell.price, type: 'HIGH' });
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
        const maxRowsOnScreen = Math.max(10, Math.floor(chartH / 15));
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

    // Run V4.1 9-Factor Macro Decision Engine!
    updateMacroMarketDecision(avgFlowVol, effStep, baselineLotSize, nakedPocs, unfinishedLvls);

    const pToY = p => chartH - ((p - minP) / pSpan) * chartH;
    const yToP = y => minP + ((chartH - y) / chartH) * pSpan;
    const rowH = Math.max(3, Math.abs(pToY(minP + effStep) - pToY(minP)));

    ctx.strokeStyle = '#131722';
    ctx.lineWidth = 1;
    const gCount = isMobile ? 7 : 10;
    for (let i = 0; i <= gCount; i++) {
        const gy = (chartH / gCount) * i;
        ctx.beginPath();
        ctx.moveTo(0, gy); ctx.lineTo(chartW, gy);
        ctx.stroke();
    }

    ctx.strokeStyle = '#192030';
    ctx.beginPath();
    ctx.moveTo(0, chartH); ctx.lineTo(chartW, chartH);
    ctx.stroke();

    const timeBarTop = H - TIME_H;
    ctx.fillStyle = '#11151f';
    ctx.fillRect(0, timeBarTop, chartW, TIME_H);
    ctx.strokeStyle = '#232838';
    ctx.beginPath();
    ctx.moveTo(0, timeBarTop); ctx.lineTo(chartW, timeBarTop);
    ctx.stroke();

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

        const fp = groupFootprint(c.footprint, c.low, c.high, effStep, c.close);

        let maxV = 1, sumV = 0;
        let maxPull = 0.001, sumPull = 0, pullCnt = 0;
        let pocBucketPrice = c.poc;
        let maxBucketV = -1;

        fp.forEach(l => {
            if (l.volume > maxV) maxV = l.volume;
            if (l.volume > maxBucketV) { maxBucketV = l.volume; pocBucketPrice = l.price; }
            sumV += l.volume;
            if (l.pulled_qty > 0) {
                if (l.pulled_qty > maxPull) maxPull = l.pulled_qty;
                sumPull += l.pulled_qty;
                pullCnt++;
            }
        });

        const avgV = fp.length > 0 ? sumV / fp.length : 1;
        const avgPull = pullCnt > 0 ? sumPull / pullCnt : 0.1;

        const normAbsCut = Math.max(avgV * 1.15, maxV * 0.30);
        const heavyAbsCut = Math.max(avgV * 1.85, maxV * 0.55);
        const normPullCut = Math.max(avgPull * 0.45, 0.001);
        const heavyPullCut = Math.max(avgPull * 1.5, maxPull * 0.50);

        if (c.vah != null && c.val != null) {
            const yVah = pToY(c.vah + effStep * 0.5);
            const yVal = pToY(c.val - effStep * 0.5);
            ctx.fillStyle = 'rgba(41, 182, 246, 0.06)';
            ctx.fillRect(boxL, yVah, boxW, Math.max(2, yVal - yVah));

            ctx.strokeStyle = '#29b6f6';
            ctx.lineWidth = 1;
            ctx.beginPath();
            ctx.moveTo(boxL, yVah); ctx.lineTo(boxR, yVah);
            ctx.moveTo(boxL, yVal); ctx.lineTo(boxR, yVal);
            ctx.stroke();

            if (candleW >= 65 && proViewMode) {
                ctx.fillStyle = '#29b6f6';
                ctx.font = 'bold 9px monospace';
                ctx.textAlign = 'left';
                ctx.fillText(`VAH`, boxL + 7, yVah - 3);
                ctx.fillText(`VAL`, boxL + 7, yVal + 9);
            }
        }

        let buyImbStreak = 0, sellImbStreak = 0;
        let maxBuyStack = 0, maxSellStack = 0;
        let whaleBuyCount = 0, whaleSellCount = 0;

        fp.forEach(l => {
            const sImb = l.sell > l.buy * 2.4 && l.sell >= avgV * 0.45;
            const bImb = l.buy > l.sell * 2.4 && l.buy >= avgV * 0.45;
            if (bImb) { buyImbStreak++; sellImbStreak = 0; if (buyImbStreak > maxBuyStack) maxBuyStack = buyImbStreak; }
            else if (sImb) { sellImbStreak++; buyImbStreak = 0; if (sellImbStreak > maxSellStack) maxSellStack = sellImbStreak; }
            else { buyImbStreak = 0; sellImbStreak = 0; }

            if (proViewMode && (buyImbStreak >= 2 || sellImbStreak >= 2)) {
                const py = pToY(l.price + effStep * 0.5);
                const cellH = Math.max(rowH, 3);
                ctx.fillStyle = buyImbStreak >= 2 ? 'rgba(0, 229, 255, 0.10)' : 'rgba(255, 0, 127, 0.10)';
                ctx.fillRect(boxL, py - cellH * 1.5, boxW, cellH * 2);
                ctx.fillStyle = buyImbStreak >= 2 ? '#00e5ff' : '#ff007f';
                ctx.fillRect(boxL - 2, py - cellH * 1.5, 2, cellH * 2);
            }
        });

        const barX = boxL + 3;
        ctx.strokeStyle = col;
        ctx.lineWidth = 2;
        ctx.beginPath();
        ctx.moveTo(barX, yH); ctx.lineTo(barX, yL);
        ctx.stroke();
        ctx.fillStyle = col;
        ctx.fillRect(barX - 3, Math.min(yO, yC), 6, Math.max(Math.abs(yC - yO), 2));

        ctx.strokeStyle = '#262c3d';
        ctx.lineWidth = 1;
        ctx.beginPath();
        ctx.moveTo(spineX, yH); ctx.lineTo(spineX, yL);
        ctx.stroke();

        const halfW = (boxW - 10) * 0.5;
        const canPrint = candleW >= 65 && rowH >= 10;

        let topSummary = null;
        let botSummary = null;
        let cBidPullSum = 0, cAskPullSum = 0;

        fp.forEach(l => {
            const py = pToY(l.price + effStep * 0.5);
            const sellV = l.sell || 0;
            const buyV = l.buy || 0;
            const totV = l.volume || 0;
            const trd = Math.max(l.trades || 1, 1);
            const lvlLotSize = totV / trd;
            const lotRatio = lvlLotSize / baselineLotSize;
            const isWhaleLot = totV >= avgV * 0.85 && lotRatio >= 2.0;

            if (isWhaleLot) {
                if (buyV > sellV) whaleBuyCount++;
                else if (sellV > buyV) {
                    if (c.close >= c.open && l.price <= (c.high + c.low) * 0.5) whaleBuyCount++; // Passive Whale Bid Absorption
                    else whaleSellCount++;
                }
            }

            const bPull = l.bid_pulled || 0;
            const aPull = l.ask_pulled || 0;
            cBidPullSum += bPull;
            cAskPullSum += aPull;

            const bPullPct = bPull > 0 ? Math.round((bPull / (bPull + sellV)) * 100) : 0;
            const aPullPct = aPull > 0 ? Math.round((aPull / (aPull + buyV)) * 100) : 0;
            const bAbsPct = totV > 0 ? Math.round((sellV / totV) * 100) : 0;
            const aAbsPct = totV > 0 ? Math.round((buyV / totV) * 100) : 0;

            let bidAbsType = null;
            if (sellV >= heavyAbsCut && (sellV >= buyV * 1.15 || l.price <= c.close)) {
                bidAbsType = 'heavy';
                if (!botSummary || sellV > botSummary.val) botSummary = { type: 'ABS', val: sellV, pct: bAbsPct };
            } else if (sellV >= normAbsCut && sellV >= buyV * 1.1) {
                bidAbsType = 'norm';
            }

            let askAbsType = null;
            if (buyV >= heavyAbsCut && (buyV >= sellV * 1.15 || l.price >= c.close)) {
                askAbsType = 'heavy';
                if (!topSummary || buyV > topSummary.val) topSummary = { type: 'ABS', val: buyV, pct: aAbsPct };
            } else if (buyV >= normAbsCut && buyV >= sellV * 1.1) {
                askAbsType = 'norm';
            }

            if (py < -20 || py > chartH + 10) return;

            const cellH = Math.max(rowH - 1, 2);
            const topY = py - cellH * 0.5;

            let bidPullType = bPull >= heavyPullCut ? 'heavy' : (bPull >= normPullCut ? 'norm' : null);
            let askPullType = aPull >= heavyPullCut ? 'heavy' : (aPull >= normPullCut ? 'norm' : null);

            if (proViewMode) {
                if (bidAbsType === 'heavy') {
                    ctx.fillStyle = 'rgba(0, 229, 255, 0.22)';
                    ctx.fillRect(spineX - halfW, topY, halfW, cellH);
                } else if (bidPullType === 'heavy') {
                    ctx.fillStyle = 'rgba(213, 0, 249, 0.22)';
                    ctx.fillRect(spineX - halfW, topY, halfW, cellH);
                }

                if (askAbsType === 'heavy') {
                    ctx.fillStyle = 'rgba(255, 145, 0, 0.25)';
                    ctx.fillRect(spineX, topY, halfW, cellH);
                } else if (askPullType === 'heavy') {
                    ctx.fillStyle = 'rgba(170, 0, 255, 0.22)';
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
                    price: l.price, effStep, sellV, buyV, bPull, aPull,
                    bPullPct, aPullPct, bAbsPct, aAbsPct, bidAbsType, askAbsType,
                    trades: trd, lvlLotSize, lotRatio, isWhaleLot
                };
                ctx.strokeStyle = '#ffffff';
                ctx.lineWidth = 1;
                ctx.strokeRect(spineX - halfW, topY, halfW * 2, cellH);
            }

            if (canPrint) {
                ctx.textBaseline = 'middle';

                const whaleSellMark = (proViewMode && isWhaleLot && sellV >= buyV) ? '◆' : '';
                const whaleBuyMark = (proViewMode && isWhaleLot && buyV > sellV) ? '◆' : '';

                ctx.font = (sellImb || isPoc || whaleSellMark) ? 'bold 10px monospace' : '10px monospace';
                ctx.fillStyle = whaleSellMark ? '#ffd700' : (sellImb ? '#ff5252' : (isPoc ? '#ffeb3b' : '#e0e3eb'));
                ctx.textAlign = 'right';
                ctx.fillText(`${whaleSellMark}${fmt(sellV)}`, spineX - 3, py);

                ctx.font = (buyImb || isPoc || whaleBuyMark) ? 'bold 10px monospace' : '10px monospace';
                ctx.fillStyle = whaleBuyMark ? '#ffd700' : (buyImb ? '#00e676' : (isPoc ? '#ffeb3b' : '#e0e3eb'));
                ctx.textAlign = 'left';
                ctx.fillText(`${fmt(buyV)}${whaleBuyMark}`, spineX + 3, py);

                if (proViewMode && candleW >= 115) {
                    let leftTag = '';
                    let leftCol = '#fff';
                    if (bidAbsType === 'heavy') { leftTag = `H-B.ABS(${bAbsPct}%)`; leftCol = '#00e5ff'; }
                    else if (bidAbsType === 'norm') { leftTag = `b-abs(${bAbsPct}%)`; leftCol = '#84ffff'; }

                    if (bidPullType === 'heavy') {
                        leftTag = `H-BP:${fmt(bPull)}(${bPullPct}%)`;
                        leftCol = '#ff4081';
                    } else if (bidPullType === 'norm' && !leftTag) {
                        leftTag = `bp:${fmt(bPull)}(${bPullPct}%)`;
                        leftCol = '#ff80ab';
                    }

                    if (leftTag) {
                        ctx.font = (bidAbsType === 'heavy' || bidPullType === 'heavy') ? 'bold 9px monospace' : '9px monospace';
                        ctx.fillStyle = leftCol;
                        ctx.textAlign = 'left';
                        ctx.fillText(leftTag, spineX - halfW + 2, py);
                    }

                    let rightTag = '';
                    let rightCol = '#fff';
                    if (askAbsType === 'heavy') { rightTag = `H-A.ABS(${aAbsPct}%)`; rightCol = '#ff9100'; }
                    else if (askAbsType === 'norm') { rightTag = `a-abs(${aAbsPct}%)`; rightCol = '#ffd180'; }

                    if (askPullType === 'heavy') {
                        rightTag = `H-AP:${fmt(aPull)}(${aPullPct}%)`;
                        rightCol = '#e040fb';
                    } else if (askPullType === 'norm' && !rightTag) {
                        rightTag = `ap:${fmt(aPull)}(${aPullPct}%)`;
                        rightCol = '#ea80fc';
                    }

                    if (!rightTag && isPoc) {
                        rightTag = 'POC';
                        rightCol = '#ffeb3b';
                    }

                    if (rightTag) {
                        ctx.font = (askAbsType === 'heavy' || askPullType === 'heavy' || isPoc) ? 'bold 9px monospace' : '9px monospace';
                        ctx.fillStyle = rightCol;
                        ctx.textAlign = 'right';
                        ctx.fillText(rightTag, boxR - 2, py);
                    }
                }
            }
        });

        const totalWhalesInCandle = whaleBuyCount + whaleSellCount;

        // Check if this candle created an ICT FVG (BAAP vs BETA)
        let fvgBadge = '';
        let fvgScoreBoost = 0;
        if (idx >= 1 && idx < candles.length - 1) {
            const prevC = candles[idx - 1];
            const nextC = candles[idx + 1];
            const bullFvg = nextC.low > prevC.high;
            const bearFvg = nextC.high < prevC.low;
            if (bullFvg) {
                const isBaap = (maxBuyStack >= 2 || totalWhalesInCandle >= 1) && (c.volume >= avgFlowVol * 1.05);
                fvgBadge = isBaap ? ' ⚡BAAP-FVG' : ' ⚠️BETA-FVG';
                fvgScoreBoost = isBaap ? 18 : -8;
            } else if (bearFvg) {
                const isBaap = (maxSellStack >= 2 || totalWhalesInCandle >= 1) && (c.volume >= avgFlowVol * 1.05);
                fvgBadge = isBaap ? ' ⚡BAAP-FVG' : ' ⚠️BETA-FVG';
                fvgScoreBoost = isBaap ? -18 : 8;
            }
        }

        // Check if this candle swept & mitigated a prior candle's POC (Magnet Sweep!)
        let sweptPriorPoc = false;
        if (idx >= 2) {
            for (let k = Math.max(0, idx - 6); k < idx - 1; k++) {
                const oldPoc = candles[k].poc;
                if (oldPoc != null && c.low <= oldPoc && c.high >= oldPoc) {
                    sweptPriorPoc = true;
                    break;
                }
            }
        }

        // =========================================================================
        // V4.1 UPGRADED CANDLE-WISE DECISION MATRIX (USES WHALE LOTS + STACKED IMB + FVG)
        // =========================================================================
        const cVol = c.volume || 0;
        const cTrades = Math.max(c.trades || 1, 1);
        const candleLotRatio = (cVol / cTrades) / baselineLotSize;
        const rvol = cVol / avgFlowVol;
        const mktBuyPct = cVol > 0 ? Math.round(((c.buy_volume || 0) / cVol) * 100) : 50;
        const mktSellPct = 100 - mktBuyPct;

        const cRange = Math.max(c.high - c.low, rawStep * 0.5);
        const bodySize = Math.abs(c.close - c.open);
        const upperWick = c.high - Math.max(c.open, c.close);
        const lowerWick = Math.min(c.open, c.close) - c.low;
        const closePos = (c.close - c.low) / cRange;

        let flowVerdict = `MB:${mktBuyPct}%|MS:${mktSellPct}%${fvgBadge}`;
        let candleDecision = `⚪ NEUTRAL / CHOP`;
        let verdictCol = '#8b93a7';
        let decisionCol = '#9ba2b0';
        let isHighConviction = false;

        if (rvol < 0.70) {
            flowVerdict = `LOW FLOW (${rvol.toFixed(1)}x)${fvgBadge}`;
            candleDecision = `⚪ SKIP (RETAIL NOISE)`;
            verdictCol = '#6c738a';
            decisionCol = '#78909c';
        } else {
            let cBull = 0, cBear = 0;

            // 1. Absorption & Aggression
            if (mktSellPct >= 56 && (c.close >= c.open || lowerWick > bodySize * 0.75)) cBull += 34;
            if (mktBuyPct >= 56 && (c.close <= c.open || upperWick > bodySize * 0.75)) cBear += 34;
            if (mktBuyPct >= 58 && c.close > c.open && closePos >= 0.6) cBull += 30;
            if (mktSellPct >= 58 && c.close < c.open && closePos <= 0.4) cBear += 30;

            // 2. Heavy Cell Absorptions & Spoof Pulls
            if (botSummary && closePos >= 0.45) cBull += 22;
            if (topSummary && closePos <= 0.55) cBear += 22;
            if (cAskPullSum > cBidPullSum * 1.4) cBull += 12;
            if (cBidPullSum > cAskPullSum * 1.4) cBear += 12;

            // 3. POC Position
            if (pocBucketPrice != null) {
                if (c.close > pocBucketPrice) cBull += 14;
                else if (c.close < pocBucketPrice) cBear += 14;
            }

            // 4. V4 NEW: Whale Lot Execution Bonus (◆)
            if (whaleBuyCount > whaleSellCount) cBull += 22;
            else if (whaleSellCount > whaleBuyCount) cBear += 22;

            // 5. V4 NEW: Stacked Imbalance & BAAP-FVG Bonus (⚡)
            if (maxBuyStack >= 2) cBull += 18;
            if (maxSellStack >= 2) cBear += 18;
            if (fvgScoreBoost > 0) cBull += fvgScoreBoost;
            if (fvgScoreBoost < 0) cBear += Math.abs(fvgScoreBoost);

            const cNet = cBull - cBear;
            const isRetailCrowd = totalWhalesInCandle === 0 && candleLotRatio < 0.78;

            if (mktBuyPct >= 58 && (c.close < c.open || upperWick > bodySize * 0.8)) {
                flowVerdict = `🐳LIM-SELL DUMP${fvgBadge}`;
                verdictCol = '#ff9100';
                isHighConviction = rvol >= 1.15;
            } else if (mktSellPct >= 58 && (c.close > c.open || lowerWick > bodySize * 0.8)) {
                flowVerdict = `🐳LIM-BUY ACCUM${fvgBadge}`;
                verdictCol = '#00e5ff';
                isHighConviction = rvol >= 1.15;
            } else if (mktSellPct >= 60 && c.close < c.open && bodySize >= cRange * 0.5) {
                flowVerdict = `🔥MKT SELL DUMP${fvgBadge}`;
                verdictCol = '#ff1744';
                isHighConviction = rvol >= 1.15;
            } else if (mktBuyPct >= 60 && c.close > c.open && bodySize >= cRange * 0.5) {
                flowVerdict = `🔥MKT IGNITION${fvgBadge}`;
                verdictCol = '#00e676';
                isHighConviction = rvol >= 1.15;
            } else {
                const magTxt = sweptPriorPoc ? ' 🎯MAG-SWEPT' : '';
                flowVerdict = `MB:${mktBuyPct}%|MS:${mktSellPct}%${fvgBadge || magTxt}`;
                verdictCol = fvgBadge.includes('BAAP') ? '#00e5ff' : '#cfd8dc';
            }

            const whalePrefix = totalWhalesInCandle > 0 ? '◆' : '';

            // Detect Retail FOMO Trap (Volume came in tiny retail lots and failed to hold!)
            if (isRetailCrowd && ((mktBuyPct >= 58 && closePos < 0.45) || (mktSellPct >= 58 && closePos > 0.55))) {
                candleDecision = `🐑 RETAIL FOMO TRAP!`;
                decisionCol = '#ffab00';
            } else if (cNet >= 20) {
                const cPct = Math.min(97, Math.round(52 + (cNet / 110) * 45));
                if (c.delta < 0 && c.close >= c.open) {
                    candleDecision = `${whalePrefix}🐳 WHALE BULL ABS (${cPct}%)`;
                } else if (maxBuyStack >= 2 && totalWhalesInCandle > 0) {
                    candleDecision = `◆🟢 WHALE BUY WIN (${cPct}%)`;
                } else {
                    candleDecision = `${whalePrefix}🟢 BUYERS WIN (${cPct}%)`;
                }
                decisionCol = '#00e676';
                if (rvol >= 1.15 || totalWhalesInCandle > 0) isHighConviction = true;
            } else if (cNet <= -20) {
                const cPct = Math.min(97, Math.round(52 + (Math.abs(cNet) / 110) * 45));
                if (c.delta > 0 && c.close <= c.open) {
                    candleDecision = `${whalePrefix}🐳 WHALE BEAR ABS (${cPct}%)`;
                } else if (maxSellStack >= 2 && totalWhalesInCandle > 0) {
                    candleDecision = `◆🔴 WHALE SELL WIN (${cPct}%)`;
                } else {
                    candleDecision = `${whalePrefix}🔴 SELLERS WIN (${cPct}%)`;
                }
                decisionCol = '#ff5252';
                if (rvol >= 1.15 || totalWhalesInCandle > 0) isHighConviction = true;
            } else {
                candleDecision = `${whalePrefix}⚖️ TWO-WAY FIGHT`;
                decisionCol = '#ffca28';
            }
        }

        let botYBanner = yL + 16;
        if (proViewMode && candleW >= 70 && rvol >= 0.85) {
            ctx.font = 'bold 9px monospace';
            ctx.textAlign = 'center';

            let topYBanner = Math.max(16, yH - 18);
            if (topSummary && c.close < c.high - rawStep) {
                ctx.fillStyle = 'rgba(255, 145, 0, 0.90)';
                ctx.fillRect(cx - 64, topYBanner, 128, 13);
                ctx.fillStyle = '#000';
                ctx.fillText(`▼ ASK ABS:${fmt(topSummary.val)}(${topSummary.pct}%)`, cx, topYBanner + 7);
            }

            if (botSummary && c.close > c.low + rawStep) {
                ctx.fillStyle = 'rgba(0, 229, 255, 0.90)';
                ctx.fillRect(cx - 64, botYBanner, 128, 13);
                ctx.fillStyle = '#000';
                ctx.fillText(`▲ BID ABS:${fmt(botSummary.val)}(${botSummary.pct}%)`, cx, botYBanner + 7);
                botYBanner += 18;
            }
        }

        if (candleW >= 60) {
            const pillW = Math.min(candleW * 0.95, 178);
            const pillH = 52;
            const maxAllowedY = H - TIME_H - pillH - 3;
            const pillY = Math.min(Math.max(botYBanner + 6, yL + 20), maxAllowedY);
            const pillX = cx - pillW * 0.5;

            ctx.strokeStyle = 'rgba(108, 115, 138, 0.35)';
            ctx.lineWidth = 1;
            ctx.beginPath();
            ctx.moveTo(cx, yL + 2);
            ctx.lineTo(cx, pillY);
            ctx.stroke();

            ctx.fillStyle = isHighConviction ? 'rgba(16, 20, 34, 0.92)' : 'rgba(12, 16, 24, 0.84)';
            ctx.fillRect(pillX, pillY, pillW, pillH);

            ctx.strokeStyle = isHighConviction ? decisionCol : 'rgba(60, 72, 98, 0.65)';
            ctx.lineWidth = isHighConviction ? 1.5 : 1;
            ctx.strokeRect(pillX, pillY, pillW, pillH);

            ctx.textAlign = 'center';
            ctx.textBaseline = 'middle';

            ctx.font = 'bold 9px monospace';
            ctx.fillStyle = c.delta >= 0 ? '#00e676' : '#ff5252';
            ctx.fillText(`Δ${c.delta >= 0 ? '+' : ''}${fmt(c.delta)} | V:${fmt(c.volume)} (${rvol.toFixed(1)}x)`, cx, pillY + 8);

            ctx.font = '9px monospace';
            ctx.fillStyle = '#b0b8c8';
            ctx.fillText(`CVD:${cvd >= 0 ? '+' : ''}${fmt(cvd)} | B:${fmt(c.buy_volume)} S:${fmt(c.sell_volume)}`, cx, pillY + 19);

            ctx.font = '9px monospace';
            ctx.fillStyle = verdictCol;
            ctx.fillText(flowVerdict, cx, pillY + 31);

            ctx.fillStyle = 'rgba(255,255,255,0.06)';
            ctx.fillRect(pillX + 2, pillY + 38, pillW - 4, 12);
            ctx.font = 'bold 9px monospace';
            ctx.fillStyle = decisionCol;
            ctx.fillText(candleDecision, cx, pillY + 44);
        }

        if (candleW >= 40) {
            const tStr = new Date(c.start).toTimeString().slice(0, 5);
            ctx.strokeStyle = '#2c3347';
            ctx.beginPath();
            ctx.moveTo(cx, timeBarTop);
            ctx.lineTo(cx, timeBarTop + 4);
            ctx.stroke();

            ctx.fillStyle = '#9ba2b0';
            ctx.font = 'bold 10px monospace';
            ctx.textAlign = 'center';
            ctx.textBaseline = 'middle';
            ctx.fillText(tStr, cx, timeBarTop + TIME_H * 0.55);
        }
    });

    const hudW = Math.min(chartW - 12, 440);
    ctx.fillStyle = 'rgba(17, 21, 31, 0.90)';
    ctx.fillRect(6, 6, hudW, hoveredCellInfo ? 66 : 22);
    ctx.strokeStyle = '#2962ff';
    ctx.strokeRect(6, 6, hudW, hoveredCellInfo ? 66 : 22);
    ctx.fillStyle = '#00e5ff';
    ctx.font = 'bold 10px monospace';
    ctx.textAlign = 'left';
    ctx.fillText(`Scale:${mult}x (${effStep.toFixed(dec)}) | AvgVol:${fmt(avgFlowVol)} | BaseLot:${fmt(baselineLotSize)}`, 12, 17);

    if (hoveredCellInfo) {
        const h = hoveredCellInfo;
        ctx.fillStyle = '#ffeb3b';
        ctx.fillText(`Px:${h.price.toFixed(dec)} | MS:${fmt(h.sellV)}(${h.bAbsPct}%) | MB:${fmt(h.buyV)}(${h.aAbsPct}%)`, 12, 33);
        ctx.fillStyle = '#ff80ab';
        ctx.fillText(`BPull:${fmt(h.bPull)}(${h.bPullPct}%) | APull:${fmt(h.aPull)}(${h.aPullPct}%)`, 12, 47);
        ctx.fillStyle = h.isWhaleLot ? '#ffd700' : '#b0bec5';
        const playerTag = h.isWhaleLot ? '🐋 WHALE LOT EXECUTION!' : (h.lotRatio < 0.7 ? '🐑 RETAIL CROWD' : '⚡ STANDARD FLOW');
        ctx.fillText(`Trades:${h.trades} | AvgLot:${fmt(h.lvlLotSize)} (${h.lotRatio.toFixed(1)}x) [${playerTag}]`, 12, 61);
    }

    const yAxisX = chartW;
    ctx.fillStyle = '#11151f';
    ctx.fillRect(yAxisX, 0, yAxisW, H);
    ctx.strokeStyle = '#232838';
    ctx.beginPath();
    ctx.moveTo(yAxisX, 0); ctx.lineTo(yAxisX, H);
    ctx.stroke();

    ctx.fillStyle = '#8b93a7';
    ctx.font = isMobile ? '10px monospace' : '11px monospace';
    ctx.textAlign = 'left';
    ctx.textBaseline = 'middle';
    for (let i = 0; i <= gCount; i++) {
        const gy = (chartH / gCount) * i;
        ctx.fillText(yToP(gy).toFixed(dec), yAxisX + 4, gy);
    }

    const lastC = candles[candles.length - 1];

    if (proViewMode) {
        nakedPocs.slice(-6).forEach(np => {
            const ny = pToY(np);
            if (ny > 10 && ny < chartH - 10) {
                if (Math.abs(lastC.close - np) <= effStep * 6) {
                    ctx.strokeStyle = 'rgba(255, 235, 59, 0.35)';
                    ctx.setLineDash([3, 3]);
                    ctx.beginPath(); ctx.moveTo(chartW * 0.5, ny); ctx.lineTo(chartW, ny); ctx.stroke();
                    ctx.setLineDash([]);
                }
                ctx.fillStyle = '#ffeb3b';
                ctx.fillRect(yAxisX - 34, ny - 6, 34, 12);
                ctx.fillStyle = '#000';
                ctx.font = 'bold 8px monospace';
                ctx.textAlign = 'center';
                ctx.fillText('◀nPOC', yAxisX - 17, ny);
            }
        });

        unfinishedLvls.slice(-5).forEach(u => {
            const uy = pToY(u.price);
            if (uy > 10 && uy < chartH - 10) {
                if (Math.abs(lastC.close - u.price) <= effStep * 5) {
                    ctx.strokeStyle = 'rgba(224, 64, 251, 0.35)';
                    ctx.setLineDash([2, 3]);
                    ctx.beginPath(); ctx.moveTo(chartW * 0.6, uy); ctx.lineTo(chartW, uy); ctx.stroke();
                    ctx.setLineDash([]);
                }
                ctx.fillStyle = '#e040fb';
                ctx.fillRect(yAxisX - 30, uy - 6, 30, 12);
                ctx.fillStyle = '#fff';
                ctx.font = 'bold 8px monospace';
                ctx.textAlign = 'center';
                ctx.fillText('◀UNF', yAxisX - 15, uy);
            }
        });
    }

    const curPy = pToY(lastC.close);
    if (curPy >= 0 && curPy <= chartH) {
        const pCol = lastC.close >= lastC.open ? '#00c853' : '#ff1744';
        ctx.strokeStyle = pCol;
        ctx.setLineDash([4, 4]);
        ctx.beginPath();
        ctx.moveTo(0, curPy); ctx.lineTo(chartW, curPy);
        ctx.stroke();
        ctx.setLineDash([]);

        ctx.fillStyle = pCol;
        ctx.fillRect(yAxisX, curPy - 10, yAxisW, 20);
        ctx.fillStyle = '#fff';
        ctx.font = 'bold 10px monospace';
        ctx.textAlign = 'left';
        ctx.fillText(lastC.close.toFixed(dec), yAxisX + 4, curPy);
    }

    if (curX > 0 && curX < chartW && curY > 0 && curY < chartH) {
        ctx.strokeStyle = 'rgba(255,255,255,0.25)';
        ctx.setLineDash([2, 2]);
        ctx.beginPath();
        ctx.moveTo(curX, 0); ctx.lineTo(curX, chartH);
        ctx.moveTo(0, curY); ctx.lineTo(chartW, curY);
        ctx.stroke();
        ctx.setLineDash([]);

        ctx.fillStyle = '#2962ff';
        ctx.fillRect(yAxisX, curY - 10, yAxisW, 20);
        ctx.fillStyle = '#fff';
        ctx.textAlign = 'left';
        ctx.fillText(yToP(curY).toFixed(dec), yAxisX + 4, curY);
    }

    if (!showDOM || domW <= 0) return;

    const domX = chartW + yAxisW;
    ctx.fillStyle = '#0d1017';
    ctx.fillRect(domX, 0, domW, H);
    ctx.strokeStyle = '#232838';
    ctx.beginPath();
    ctx.moveTo(domX, 0); ctx.lineTo(domX, H);
    ctx.stroke();

    ctx.fillStyle = '#161b26';
    ctx.fillRect(domX, 0, domW, 26);
    ctx.font = 'bold 9px monospace';
    ctx.fillStyle = '#00e676';
    ctx.textAlign = 'left';
    ctx.fillText(isMobile ? 'BID' : 'BID [EXEC]', domX + 4, 14);
    ctx.fillStyle = '#ffeb3b';
    ctx.textAlign = 'center';
    ctx.fillText(domScrollRows !== 0 ? 'PRICE(SCR)' : 'PRICE', domX + domW * 0.5, 14);
    ctx.fillStyle = '#ff5252';
    ctx.textAlign = 'right';
    ctx.fillText(isMobile ? 'ASK' : 'ASK [EXEC]', domX + domW - 4, 14);

    const ladderRows = Math.floor((H - 32) / 18);
    const halfRows = Math.floor(ladderRows / 2);

    let domStep = effStep;
    const bidsArr = obData.bids || [];
    const asksArr = obData.asks || [];
    if (bidsArr.length > 1 && asksArr.length > 1) {
        const minBookBid = bidsArr[bidsArr.length - 1].price;
        const maxBookAsk = asksArr[asksArr.length - 1].price;
        const maxBookSpan = Math.max(lastC.close - minBookBid, maxBookAsk - lastC.close);
        if (maxBookSpan > 0 && (halfRows * domStep) > maxBookSpan * 1.15) {
            domStep = Math.max(rawStep, Math.floor((maxBookSpan / halfRows) / rawStep) * rawStep);
        }
    }

    const liveCenterP = Math.floor(lastC.close / domStep + 1e-9) * domStep;
    const centerP = liveCenterP + Math.round(domScrollRows) * domStep;

    const bidMap = new Map();
    const askMap = new Map();
    bidsArr.forEach(b => {
        const bp = Math.floor(b.price / domStep + 1e-9) * domStep;
        const k = bp.toFixed(6);
        bidMap.set(k, (bidMap.get(k) || 0) + b.quantity);
    });
    asksArr.forEach(a => {
        const ap = Math.floor(a.price / domStep + 1e-9) * domStep;
        const k = ap.toFixed(6);
        askMap.set(k, (askMap.get(k) || 0) + a.quantity);
    });

    const curFpList = groupFootprint(lastC.footprint, centerP - domStep * (halfRows + 2), centerP + domStep * (halfRows + 2), domStep, lastC.close);
    const fpMap = new Map();
    curFpList.forEach(l => fpMap.set(l.price.toFixed(6), l));

    let maxDomRest = 1;
    const allBookVals = [];
    for (let idx = -halfRows; idx <= halfRows; idx++) {
        const p = centerP + idx * domStep;
        const k = p.toFixed(6);
        const bq = bidMap.get(k) || 0;
        const aq = askMap.get(k) || 0;
        if (bq > maxDomRest) maxDomRest = bq;
        if (aq > maxDomRest) maxDomRest = aq;
        if (bq > 0) allBookVals.push(bq);
        if (aq > 0) allBookVals.push(aq);
    }
    allBookVals.sort((a, b) => b - a);
    const heavyWallCut = allBookVals[Math.min(3, Math.max(0, allBookVals.length - 1))] || maxDomRest * 0.7;

    const pxColW = isMobile ? 54 : 70;
    const colW = (domW - pxColW) * 0.5;
    const pxColLeft = domX + colW;
    const pxColRight = pxColLeft + pxColW;

    let ry = 28;
    const rH = 18;

    for (let idx = halfRows; idx >= -halfRows; idx--) {
        const p = centerP + idx * domStep;
        const k = p.toFixed(6);
        const bRest = bidMap.get(k) || 0;
        const aRest = askMap.get(k) || 0;
        const fpLvl = fpMap.get(k) || { buy: 0, sell: 0, bid_pulled: 0, ask_pulled: 0 };

        const isCurrentRow = Math.abs(p - liveCenterP) < domStep * 0.4;

        if (isCurrentRow) {
            ctx.fillStyle = 'rgba(255, 235, 59, 0.20)';
            ctx.fillRect(domX, ry, domW, rH);
        } else {
            ctx.fillStyle = idx % 2 === 0 ? '#0d1017' : '#10141d';
            ctx.fillRect(domX, ry, domW, rH);
        }

        ctx.fillStyle = isCurrentRow ? '#ffeb3b' : (p > lastC.close ? '#ff8a80' : '#69f0ae');
        ctx.font = isCurrentRow ? 'bold 9px monospace' : '9px monospace';
        ctx.textAlign = 'center';
        ctx.fillText(p.toFixed(dec), domX + domW * 0.5, ry + 9);

        if (bRest > 0 || fpLvl.sell > 0) {
            const bPct = Math.min(1, bRest / maxDomRest);
            const isWall = bRest >= heavyWallCut && bRest > 0;
            ctx.fillStyle = isWall ? 'rgba(0, 229, 255, 0.45)' : 'rgba(0, 230, 118, 0.22)';
            ctx.fillRect(pxColLeft - colW * bPct, ry + 1, colW * bPct, rH - 2);

            ctx.font = isWall ? 'bold 9px monospace' : '9px monospace';
            ctx.fillStyle = isWall ? '#00e5ff' : '#00e676';
            ctx.textAlign = 'right';
            const execTxt = (!isMobile && fpLvl.sell > 0) ? `[${fmt(fpLvl.sell)}]` : '';
            const wallTxt = isWall ? '★' : '';
            ctx.fillText(`${wallTxt}${fmt(bRest)}${execTxt}`, pxColLeft - 3, ry + 9);
        }

        if (aRest > 0 || fpLvl.buy > 0) {
            const aPct = Math.min(1, aRest / maxDomRest);
            const isWall = aRest >= heavyWallCut && aRest > 0;
            ctx.fillStyle = isWall ? 'rgba(255, 145, 0, 0.48)' : 'rgba(255, 23, 68, 0.22)';
            ctx.fillRect(pxColRight, ry + 1, colW * aPct, rH - 2);

            ctx.font = isWall ? 'bold 9px monospace' : '9px monospace';
            ctx.fillStyle = isWall ? '#ffab00' : '#ff5252';
            ctx.textAlign = 'left';
            const execTxt = (!isMobile && fpLvl.buy > 0) ? `[${fmt(fpLvl.buy)}]` : '';
            const wallTxt = isWall ? '★' : '';
            ctx.fillText(`${wallTxt}${fmt(aRest)}${execTxt}`, pxColRight + 3, ry + 9);
        }

        ry += rH;
    }

    ctx.strokeStyle = '#232838';
    ctx.beginPath();
    ctx.moveTo(pxColLeft, 26); ctx.lineTo(pxColLeft, H);
    ctx.moveTo(pxColRight, 26); ctx.lineTo(pxColRight, H);
    ctx.stroke();
}

setInterval(fetchData, 1500);
setInterval(fetchSmtPeer, 4500);
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
        limit = max(1, min(int(request.args.get("limit", 500)), 1000))
    except ValueError:
        limit = 500

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
