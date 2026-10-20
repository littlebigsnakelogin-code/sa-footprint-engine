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
        "buffer": deque(maxlen=1500),
        "liquidity_history": deque(maxlen=3500),
        "liquidity_lots": {
            "bid": defaultdict(deque),
            "ask": defaultdict(deque),
        },
        "trade_history": deque(maxlen=2500),
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

def stamp_liquidity_event_to_active_candles(symbol, price_str, side, pulled_qty, old_book_qty, status_str):
    """Stamps ONLY when price actually approaches within 2 ticks of the 10s+ Resting Wall!"""
    step = PRICE_STEP.get(symbol, 1.0)
    try:
        p_float = float(price_str)
    except Exception:
        return
    bucket_price = round_price_to_step(p_float, step)
    key = str(bucket_price)
    max_distance = step * 2.0  # STRICT: Only count wall pull when live price arrives within 2 ticks of the wall!

    with lock:
        live_px = last_trade.get(symbol, {}).get("price")
        if live_px is None or abs(bucket_price - float(live_px)) > max_distance:
            return

        for tf in TIMEFRAMES.keys():
            curr = current_candles[symbol][tf]
            if curr is not None and "footprint" in curr:
                if bucket_price > curr["high"] + step * 2.0 or bucket_price < curr["low"] - step * 2.0:
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
                        "max_book_seen": 0.0,
                        "status": "NORMAL",
                    }
                    curr["footprint"][key] = level

                if pulled_qty > 0.0:
                    p_val = float(pulled_qty)
                    peak_seen = max(float(level.get("max_book_seen", 0.0)), float(old_book_qty), p_val)
                    level["max_book_seen"] = peak_seen
                    max_allowed_pull = peak_seen

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

        # STRICT 10-SECOND AGED FIFO/LIFO RULE + LATE TRADE EXECUTION DEDUCTION:
        # Subtract any late-arriving trade executions (within 450ms window) so eaten walls are NEVER counted as pulls!
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

        # Stamp ONLY genuine >=10s Aged Resting Wall pulls that were NOT executed!
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

    # RAM CLEANUP: Prevent index memory leaks over 12H+ sessions
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

    # Minimum single pull event size to care about
    min_pull_map = {"BTCUSDT": 1.5, "ETHUSDT": 12.0, "SOLUSDT": 80.0, "XRPUSDT": 15000.0}
    min_track_qty = min_pull_map.get(symbol, 1.0)
    MIN_WALL_AGE_MS = 10000  # Strict 10 Seconds Minimum Age

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

            # FIX 1: Track ALL additions (even small ones) so LIFO cancellation NEVER eats old 10s lots by mistake!
            if added_qty > 1e-8:
                lots.append({
                    "lot_id": str(uuid.uuid4()),
                    "original_qty": added_qty,
                    "remaining_qty": added_qty,
                    "time": event_time,
                    "origin": "depth_add",
                    "update_id": event_update_id,
                })

            fifo_consumption = []
            aged_consumed_qty = 0.0
            aged_executed_qty = 0.0
            aged_unmatched_qty = 0.0

            if reduced_qty > 1e-8:
                executed_qty = match_trade_to_liquidity_reduction(
                    symbol, side, price, reduced_qty, event_time
                )
                executed_qty = min(max(float(executed_qty), 0.0), reduced_qty)
                canceled_qty = max(reduced_qty - executed_qty, 0.0)

                # 1. EXECUTIONS consume from FRONT of queue (FIFO: Oldest Resting Orders Execute First)
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
                    is_aged = (lot_age >= MIN_WALL_AGE_MS)

                    if is_aged:
                        aged_consumed_qty += take
                        aged_executed_qty += take

                    rem_exec -= take
                    if oldest_lot["remaining_qty"] <= 1e-12:
                        lots.popleft()

                # 2. CANCELLATIONS remove from BACK of queue FIRST (LIFO: All new <10s flickers stripped first!)
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
                    is_aged = (lot_age >= MIN_WALL_AGE_MS)

                    # ONLY count as Pull if this specific canceled lot was resting for >= 10 seconds
                    # AND the total single pull event was meaningful (>= min_track_qty)!
                    if is_aged and canceled_qty >= min_track_qty:
                        aged_consumed_qty += take
                        aged_unmatched_qty += take

                    rem_cancel -= take
                    if newest_lot["remaining_qty"] <= 1e-12:
                        lots.pop()

                if not lots:
                    state["liquidity_lots"][side].pop(price, None)

                # Only create a pull record if an actual >=10s wall of meaningful size was pulled!
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
                        "fifo_consumption": fifo_consumption,
                        "status": "reduction_pending",
                    }
                    state["liquidity_history"].append(liquidity_record)
                    price_key = round(float(price), 8)
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
<title>SA Institutional Footprint & Pulling-Delta Engine V4.5</title>
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
    <span class="brand">⚡ SA V4.5</span>
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
    <button class="btn" onclick="toggleRules()" style="border-color:#ffeb3b;color:#ffeb3b;font-weight:bold;">📖 RULES</button>

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
    <div class="verdict-box" id="masterVerdictBox">
        <span id="masterVerdictTitle">SCANNING MACRO MARKET ORDERFLOW...</span>
        <span id="masterVerdictReasons" style="font-size:10px;font-weight:normal;color:#cfd8dc;">Calculating structure...</span>
    </div>
</div>

<div id="wrap">
    <canvas id="cv"></canvas>
    <div id="domSlideTab" class="dom-slide-tab" onclick="toggleDOM()">❮ BATTLE DOM</div>
</div>

<div id="ruleModal">
    <div style="display:flex; justify-content:space-between; align-items:center; border-bottom:1px solid #00e5ff; padding-bottom:8px; margin-bottom:12px; position:sticky; top:0; background:#0b0e16; z-index:2;">
        <b style="color:#00e5ff; font-size:14px;">⚡ SA INSTITUTIONAL FOOTPRINT & ORDERFLOW ENGINE — COMPLETE A-TO-Z MASTER RULEBOOK</b>
        <button class="btn" onclick="toggleRules()" style="background:#ff1744;color:#fff;border:none;padding:4px 12px;font-weight:bold;">✕ CLOSE</button>
    </div>

    <div class="rule-sec" style="border-color:#00e5ff;">
        <h4 style="color:#00e5ff;">1. 10-SECOND AGED RESTING WALL PULLS (H-BP / bp / H-AP / ap) &amp; LIFO NOISE FILTER</h4>
        • <b>10-Second Heatmap Wall Gate:</b> Orderbook me har millisecond aane-jane wale HFT flickers ko LIFO (Last-In-First-Out) queue se 0% spoof count karke filter kiya jata hai. Sirf wahi limit orders jo <b>&ge; 10 Seconds</b> se orderbook me resting the aur jab price unke <b>2 ticks</b> ke andar aaya tab pull huye, unhe genuine <b>Institutional Wall Pull</b> maana jata hai.<br>
        • <b>Price Level Tags:</b> Left (Bid) side par <code>H-BP:Qty(%)</code> (Heavy Bid Pull) ya <code>bp:Qty(%)</code> aur Right (Ask) side par <code>H-AP:Qty(%)</code> (Heavy Ask Pull) ya <code>ap:Qty(%)</code> dikhta hai. Price levels par Pulling Delta nahi dikhaya jata taaki chart 100% clean rahe.
    </div>

    <div class="rule-sec">
        <h4>2. CANDLE PILL PULLING DELTA (P&Delta; = Total 10s+ Bid Pulled &minus; Total 10s+ Ask Pulled)</h4>
        • <b>Formula (Candle Pill Line 3):</b> <code>P&Delta; = Total Bid Pull (BP) &minus; Total Ask Pull (AP)</code>.<br>
        • <b>Negative P&Delta; (AP &gt; BP) + Bullish Close:</b> Upar Ask side par bada fake sell wall dikha kar retail ko short karwaya gaya, aur price aate hi wall pull kar li gayi! (<b>⚠️ FAKE-ASK WALL PULLED &rarr; BULL BREAK!</b>).<br>
        • <b>Positive P&Delta; (BP &gt; AP) + Bearish Close:</b> Niche Bid side par fake support wall dikha kar retail ko long karwaya gaya, aur price aate hi wall kheench li gayi! (<b>⚠️ FAKE-BID WALL PULLED &rarr; BEAR DUMP!</b>).
    </div>

    <div class="rule-sec" style="border-color:#ff9100;">
        <h4 style="color:#ff9100;">3. TRUE INSTITUTIONAL ABSORPTION (H-B.ABS / H-A.ABS) vs SOLID WALLS</h4>
        • <b>Bid Absorption (<code>H-B.ABS &ge; 65%</code> — Cyan):</b> Jab candle ke lower/bid zone me bhari Market Sell orders maare jayein (<code>Sell Vol &ge; Heavy Cut</code>), lekin Resting Limit Buyers saara maal absorb kar lein aur price level ke niche close na hone dein (<code>🛡️ SOLID-BID WALL: Absorbed &amp; Held!</code>).<br>
        • <b>Ask Absorption (<code>H-A.ABS &ge; 65%</code> — Orange):</b> Jab candle ke upper/ask zone me bhari Market Buy orders maare jayein, lekin Resting Limit Sellers saara maal absorb karke price ko cap kar dein (<code>🛡️ SOLID-ASK WALL: Absorbed &amp; Capped!</code>).
    </div>

    <div class="rule-sec" style="border-color:#ffd700;">
        <h4 style="color:#ffd700;">4. WHALE BLOCKS (◆), RETAIL SL-SWEEPS (🎯) &amp; CANDLE PILL 5-LINE DECODER</h4>
        • <b>◆ Whale Lot Detector (Gold Diamond):</b> Jab kisi price level par average trade size (<code>Volume / Trades</code>) normal baseline lot se <b>2.8x se 4.5x+ bada</b> ho aur heavy volume ho, toh wahan <code>◆</code> (Institutional Block Execution) lagta hai.<br>
        • <b>🎯 BSL-SWEEP / SSL-SWEEP:</b> Jab price pichli candles ka Swing High ya Swing Low tode, chote-chote retail stop-loss lots trigger hon (high trade count, small avg lot, low wall pull), aur candle wapas range ke andar close ho jaye = <b>100% Retail Stop-Hunt Reversal!</b><br>
        • <b>5-Line Candle Pill:</b> Line 1: <code>Delta | Vol (RVOL)</code> &bull; Line 2: <code>CVD | Market Buy vs Sell</code> &bull; Line 3: <code>BP vs AP &amp; P&Delta;</code> &bull; Line 4: <code>Mechanism (SL-Sweep / Fake-Wall / Solid-Wall)</code> &bull; Line 5: <code>Final Winner &amp; Conviction %</code>.
    </div>

    <div class="rule-sec" style="border-color:#e040fb;">
        <h4 style="color:#e040fb;">5. NAKED POC (nPOC) &amp; UNFINISHED BUSINESS (UB-HIGH / UB-LOW) MAGNETS</h4>
        • <b>🟡 Naked POC (<code>nPOC</code>):</b> Pichli kisi bhi candle ka Point of Control (sabse jyada volume wala price) jisko baad ki kisi bhi candle ne abhi tak touch/test nahi kiya hai. Ye high-volume nodes <b>Price Magnets</b> ka kaam karte hain.<br>
        • <b>🎯 Unfinished Business (<code>UB-HIGH</code> / <code>UB-LOW</code>):</b> Normal candle ke exact High par Bid (`Sell`) `0` hona chahiye aur exact Low par Ask (`Buy`) `0` hona chahiye (Completed Auction). Agar kisi candle ke exact High ya Low extreme par <b>Bid aur Ask DONO side Significant Volume (&gt;0)</b> trade hua hai aur baad me price ne us extreme ko sweep nahi kiya, toh wo <b>Unfinished Auction (Adhura Business)</b> hai—market us level ko revisit karke sweep karne zaroor aata hai!
    </div>

    <div class="rule-sec" style="border-color:#00e676;">
        <h4 style="color:#00e676;">6. TOP BENCHMARK BAR, SMT RADAR &amp; SPINE-SPLIT BATTLE DOM</h4>
        • <b>Flow Benchmarks:</b> <code>Avg</code> = Average Candle Vol | <code>HeavyL</code> = Heavy Footprint Cell Cutoff | <code>RestBook</code> = Live Nearby Orderbook Average Wall Size | <code>Cur</code> = Current Candle Volume &amp; RVOL.<br>
        • <b>🔗 SMT Divergence Radar:</b> BTC aur ETH ke Swing Highs/Lows ko real-time compare karta hai. Agar ek asset Low sweep kare aur dusra Hold kare = <code>🚨 BULL SMT</code>. Agar ek High sweep kare aur dusra Fail kare = <code>🚨 BEAR SMT</code>.<br>
        • <b>⚔️ Spine-Split Battle DOM:</b> Auto-cropped active ladder jahan Center Spine ke Left me Bid Battle (Upper Red Bar = Genuine Executed Sell, Lower Pink Bar = Pulled Bid Spoof) aur Right me Ask Battle (Upper Green Bar = Genuine Executed Buy, Lower Purple Bar = Pulled Ask Spoof) + Right-End Price Scale par clear Winner dikhta hai.
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

// Strictly clips to actual candle High-Low + de-flickers raw pulls so 0|0 ghost rows never appear!
function groupFootprint(fpRaw, low, high, effStep, closePx, liveRestBench) {
    const map = new Map();
    const arr = Array.isArray(fpRaw) ? fpRaw : Object.values(fpRaw || {});
    arr.forEach(l => {
        // Strict range clamp: Only show levels inside actual candle High/Low (+1 step tolerance)
        if (l.price < low - effStep * 1.1 || l.price > high + effStep * 1.1) return;
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
    });

    // Frontend Anti-Flicker Sanity Clamp: prevents pre-fix historical candles from showing 900 BTC fake flicker!
    const restCap = Math.max((liveRestBench || 15) * 3.2, 25);
    const out = [];
    map.forEach(g => {
        // Ignore completely empty ghost rows where 0 volume traded
        if (g.volume <= 0 && g.bid_pulled <= 0 && g.ask_pulled <= 0) return;
        const maxReasonablePull = Math.max(g.volume * 2.5, restCap);
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

    sample.forEach((c, idx) => {
        if (c.high > macroHigh) macroHigh = c.high;
        if (c.low < macroLow) macroLow = c.low;
        sumCandleRanges += Math.max(c.high - c.low, rawStep);
        totalCvd += (c.delta || 0);

        if (idx > 0 && c.poc && sample[idx - 1].poc) {
            if (c.poc > sample[idx - 1].poc) pocUpSteps++;
            else if (c.poc < sample[idx - 1].poc) pocDownSteps++;
        }

        const fpGrouped = groupFootprint(c.footprint, c.low, c.high, effStep, c.close, liveRestBench);
        const activeCells = fpGrouped.filter(x => x.volume > 0);
        const cAvgV = activeCells.length > 0 ? (c.volume || 1) / activeCells.length : 1;
        const cHeavyCut = Math.max(cAvgV * 1.35, heavyLvlBenchmark * 0.50);

        fpGrouped.forEach(l => {
            const sv = l.sell || 0, bv = l.buy || 0, tv = l.volume || 0;
            const bp = l.bid_pulled || 0, ap = l.ask_pulled || 0;
            const bAbsPct = tv > 0 ? (sv / tv) * 100 : 0;
            const aAbsPct = tv > 0 ? (bv / tv) * 100 : 0;

            macroBidPulled += bp;
            macroAskPulled += ap;
            if (sv >= cHeavyCut && bAbsPct >= 65 && c.close >= l.price) macroBidAbsorbs++;
            if (bv >= cHeavyCut && aAbsPct >= 65 && c.close <= l.price) macroAskAbsorbs++;

            const lt = Math.max(l.trades || 1, 1);
            const lotRatio = (tv / lt) / baselineLotSize;
            if (tv >= Math.max(cAvgV * 1.15, heavyLvlBenchmark * 0.50) && ((lotRatio >= 2.8 && lt <= 48) || lotRatio >= 4.5)) {
                if (bv > sv) whaleBuyLots++;
                else whaleSellLots++;
            }
        });
    });

    const macroSpan = Math.max(macroHigh - macroLow, rawStep);
    const netPriceMove = last.close - first.open;
    const efficiency = Math.abs(netPriceMove) / Math.max(sumCandleRanges, rawStep);
    const macroPullDelta = macroBidPulled - macroAskPulled;

    let bullPts = 0, bearPts = 0, macroNotes = [];

    if (netPriceMove > macroSpan * 0.20 && pocUpSteps > pocDownSteps) { bullPts += 24; macroNotes.push(`POC Up(${pocUpSteps}↑)`); }
    else if (netPriceMove < -macroSpan * 0.20 && pocDownSteps > pocUpSteps) { bearPts += 24; macroNotes.push(`POC Down(${pocDownSteps}↓)`); }

    if (totalCvd > 0 && netPriceMove > 0) { bullPts += 20; macroNotes.push(`CVD +${fmt(totalCvd)}`); }
    else if (totalCvd < 0 && netPriceMove < 0) { bearPts += 20; macroNotes.push(`CVD ${fmt(totalCvd)}`); }

    if (macroPullDelta < 0 && netPriceMove > 0) {
        bullPts += 22;
        macroNotes.push(`PΔ:${fmt(macroPullDelta)} (Ask Spoof Swept UP)`);
    } else if (macroPullDelta > 0 && netPriceMove < 0) {
        bearPts += 22;
        macroNotes.push(`PΔ:+${fmt(macroPullDelta)} (Bid Spoof Swept DN)`);
    }

    if (whaleBuyLots > whaleSellLots) { bullPts += 18; macroNotes.push(`◆Whale(${whaleBuyLots}B/${whaleSellLots}S)`); }
    else if (whaleSellLots > whaleBuyLots) { bearPts += 18; macroNotes.push(`◆Whale(${whaleSellLots}S/${whaleBuyLots}B)`); }

    const vBox = document.getElementById('masterVerdictBox');
    const vTitle = document.getElementById('masterVerdictTitle');
    const vReasons = document.getElementById('masterVerdictReasons');
    const netMacro = bullPts - bearPts;

    if (efficiency < 0.15 && Math.abs(netMacro) < 25) {
        vBox.style.background = 'rgba(255, 193, 7, 0.15)';
        vBox.style.borderColor = '#ffca28';
        vTitle.style.color = '#ffca28';
        vTitle.innerText = `🟡 MACRO: SIDEWAYS ROTATIONAL CHOP — FADE EXTREMES`;
        vReasons.innerText = macroNotes.slice(0, 4).join(' | ');
    } else if (netMacro >= 18) {
        const conf = Math.min(97, Math.round(52 + (netMacro / 100) * 45));
        vBox.style.background = 'rgba(0, 200, 83, 0.18)';
        vBox.style.borderColor = '#00e676';
        vTitle.style.color = '#00e676';
        vTitle.innerText = `🟢 MACRO: BULLISH ORDERFLOW DOMINANCE (${conf}%) — LONG BIAS`;
        vReasons.innerText = macroNotes.slice(0, 4).join(' | ');
    } else if (netMacro <= -18) {
        const conf = Math.min(97, Math.round(52 + (Math.abs(netMacro) / 100) * 45));
        vBox.style.background = 'rgba(255, 23, 68, 0.18)';
        vBox.style.borderColor = '#ff5252';
        vTitle.style.color = '#ff5252';
        vTitle.innerText = `🔴 MACRO: BEARISH ORDERFLOW DOMINANCE (${conf}%) — SHORT BIAS`;
        vReasons.innerText = macroNotes.slice(0, 4).join(' | ');
    } else {
        vBox.style.background = 'rgba(38, 50, 56, 0.85)';
        vBox.style.borderColor = '#90a4ae';
        vTitle.style.color = '#eceff1';
        vTitle.innerText = `⚖️ MACRO: BALANCED TWO-WAY AUCTION — WAIT FOR DISPLACEMENT`;
        vReasons.innerText = macroNotes.slice(0, 4).join(' | ');
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
        // 1. Track Untouched Naked POCs (nPOC)
        if (c.poc != null) {
            let touched = false;
            for (let j = i + 1; j < candles.length; j++) {
                if (candles[j].low <= c.poc && candles[j].high >= c.poc) { touched = true; break; }
            }
            if (!touched) nakedPocs.push({ price: c.poc, startIdx: i, vol: c.poc_volume || 0 });
        }

        // 2. Track Untouched Unfinished Business (UB-HIGH & UB-LOW where BOTH Buy > 0 AND Sell > 0 at extreme!)
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

    updateMacroMarketDecision(avgFlowVol, effStep, baselineLotSize, nakedPocs, heavyLvlBenchmark, liveRestBench);

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

    // DRAW NAKED POCs (nPOC) & UNFINISHED BUSINESS (UB-HIGH / UB-LOW) MAGNET LINES
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

        const avgV = tradedCells.length > 0 ? sumV / tradedCells.length : tfAvgCellVol;
        const effectiveHeavyAbsCut = Math.max(avgV * 1.35, heavyLvlBenchmark * 0.60);
        const effectiveNormAbsCut = Math.max(avgV * 0.70, heavyLvlBenchmark * 0.28);

        // STRICT WALL THRESHOLD: Use MAX (not min!) of RestBook & HeavyLvl so 2-4 BTC small pulls NEVER get tagged as Heavy Wall!
        const baseRestRef = liveRestBench > 0 ? Math.max(liveRestBench, heavyLvlBenchmark) : heavyLvlBenchmark;
        const effectiveHeavyPullQty = Math.max(baseRestRef * 1.10, avgV * 1.25);
        const effectiveNormPullQty = Math.max(baseRestRef * 0.65, avgV * 0.75);

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
        const cMidPrice = (c.high + c.low) * 0.5;

        fp.forEach(l => {
            const py = pToY(l.price + effStep * 0.5);
            const sellV = l.sell || 0;
            const buyV = l.buy || 0;
            const totV = l.volume || 0;
            // Do not render rows with 0 executed volume (Eliminates fake 100% pull rows on empty ticks!)
            if (totV <= 0) return;

            const trd = Math.max(l.trades || 1, 1);
            const lvlLotSize = totV / trd;
            const lotRatio = lvlLotSize / baselineLotSize;

            const isWhaleLot = totV >= Math.max(avgV * 1.15, heavyLvlBenchmark * 0.50) && ((lotRatio >= 2.8 && trd <= 48) || lotRatio >= 4.5);
            if (isWhaleLot) {
                if (buyV > sellV) whaleBuyCount++;
                else whaleSellCount++;
            }

            const bPull = l.bid_pulled || 0;
            const aPull = l.ask_pulled || 0;
            const lvlPullDelta = bPull - aPull; // Level Pulling Delta (BP - AP)
            cBidPullSum += bPull;
            cAskPullSum += aPull;

            const bPullPct = bPull > 0 ? Math.round((bPull / (bPull + sellV)) * 100) : 0;
            const aPullPct = aPull > 0 ? Math.round((aPull / (aPull + buyV)) * 100) : 0;
            const bAbsPct = Math.round((sellV / totV) * 100);
            const aAbsPct = Math.round((buyV / totV) * 100);

            const inBidZone = l.price <= Math.max(cMidPrice + effStep, Math.min(c.open, c.close) + effStep * 2);
            let bidAbsType = null;
            if (sellV >= effectiveHeavyAbsCut && bAbsPct >= 65 && c.close >= l.price && inBidZone) {
                bidAbsType = 'heavy';
                if (!botSummary || sellV > botSummary.val) botSummary = { type: 'ABS', val: sellV, pct: bAbsPct };
            } else if (sellV >= effectiveNormAbsCut && bAbsPct >= 52 && c.close >= l.price && inBidZone) {
                bidAbsType = 'norm';
            }

            const inAskZone = l.price >= Math.min(cMidPrice - effStep, Math.max(c.open, c.close) - effStep * 2);
            let askAbsType = null;
            if (buyV >= effectiveHeavyAbsCut && aAbsPct >= 65 && c.close <= l.price && inAskZone) {
                askAbsType = 'heavy';
                if (!topSummary || buyV > topSummary.val) topSummary = { type: 'ABS', val: buyV, pct: aAbsPct };
            } else if (buyV >= effectiveNormAbsCut && aAbsPct >= 52 && c.close <= l.price && inAskZone) {
                askAbsType = 'norm';
            }

            // ONLY show Pull Tags where executed volume is meaningful (totV >= avgV * 0.35) so every row isn't spammed!
            const isSignificantOrderflowLvl = totV >= avgV * 0.35;
            let bidPullType = null;
            if (isSignificantOrderflowLvl && bPull >= effectiveHeavyPullQty && bPullPct >= 60) {
                bidPullType = 'heavy';
            } else if (isSignificantOrderflowLvl && bPull >= effectiveNormPullQty && bPullPct >= 40) {
                bidPullType = 'norm';
            }

            let askPullType = null;
            if (isSignificantOrderflowLvl && aPull >= effectiveHeavyPullQty && aPullPct >= 60) {
                askPullType = 'heavy';
            } else if (isSignificantOrderflowLvl && aPull >= effectiveNormPullQty && aPullPct >= 40) {
                askPullType = 'norm';
            }

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
                    price: l.price, sellV, buyV, bPull, aPull, lvlPullDelta,
                    bPullPct, aPullPct, bAbsPct, aAbsPct,
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

                ctx.font = (sellImb || isPoc || whaleSellMark) ? 'bold 9.5px monospace' : '9.5px monospace';
                ctx.fillStyle = whaleSellMark ? '#ffd700' : (sellImb ? '#ff5252' : (isPoc ? '#ffeb3b' : '#e0e3eb'));
                ctx.textAlign = 'right';
                ctx.fillText(`${whaleSellMark}${fmt(sellV)}`, spineX - 4, py);

                ctx.font = (buyImb || isPoc || whaleBuyMark) ? 'bold 9.5px monospace' : '9.5px monospace';
                ctx.fillStyle = whaleBuyMark ? '#ffd700' : (buyImb ? '#00e676' : (isPoc ? '#ffeb3b' : '#e0e3eb'));
                ctx.textAlign = 'left';
                ctx.fillText(`${fmt(buyV)}${whaleBuyMark}`, spineX + 4, py);

                // LEVEL TAGS: Pure 10s+ Resting Wall Pull Qty & Pull % (H-BP / bp / H-AP / ap) + Absorption % (NO P-Delta on price levels!)
                if (proViewMode && candleW >= 135) {
                    let bParts = [];
                    let leftCol = '#84ffff';
                    let leftBold = false;

                    if (bidAbsType === 'heavy') { bParts.push(`H-B.ABS:${bAbsPct}%`); leftCol = '#00e5ff'; leftBold = true; }
                    else if (bidAbsType === 'norm' && candleW >= 165) { bParts.push(`b-abs:${bAbsPct}%`); }

                    if (bidPullType === 'heavy') {
                        bParts.push(`H-BP:${fmt(bPull)}(${bPullPct}%)`);
                        if (!bidAbsType) leftCol = '#ff4081';
                        leftBold = true;
                    } else if (bidPullType === 'norm' && candleW >= 165) {
                        bParts.push(`bp:${fmt(bPull)}(${bPullPct}%)`);
                        if (!bidAbsType) leftCol = '#ff80ab';
                    }

                    if (bParts.length > 0) {
                        ctx.font = leftBold ? 'bold 8px monospace' : '8px monospace';
                        ctx.fillStyle = leftCol;
                        ctx.textAlign = 'left';
                        ctx.fillText(bParts.join('|'), boxL + 5, py);
                    }

                    let aParts = [];
                    let rightCol = '#ffd180';
                    let rightBold = false;

                    if (askAbsType === 'heavy') { aParts.push(`H-A.ABS:${aAbsPct}%`); rightCol = '#ff9100'; rightBold = true; }
                    else if (askAbsType === 'norm' && candleW >= 165) { aParts.push(`a-abs:${aAbsPct}%`); }

                    if (askPullType === 'heavy') {
                        aParts.push(`H-AP:${fmt(aPull)}(${aPullPct}%)`);
                        if (!askAbsType) rightCol = '#e040fb';
                        rightBold = true;
                    } else if (askPullType === 'norm' && candleW >= 165) {
                        aParts.push(`ap:${fmt(aPull)}(${aPullPct}%)`);
                        if (!askAbsType) rightCol = '#ea80fc';
                    }

                    if (aParts.length === 0 && isPoc) {
                        aParts.push('POC');
                        rightCol = '#ffeb3b';
                        rightBold = true;
                    }

                    if (aParts.length > 0) {
                        ctx.font = rightBold ? 'bold 8px monospace' : '8px monospace';
                        ctx.fillStyle = rightCol;
                        ctx.textAlign = 'right';
                        ctx.fillText(aParts.join('|'), boxR - 3, py);
                    }
                }
            }
        });

        const totalWhalesInCandle = whaleBuyCount + whaleSellCount;
        const cVol = c.volume || 0;
        const rvol = cVol / avgFlowVol;
        const mktBuyPct = cVol > 0 ? Math.round(((c.buy_volume || 0) / cVol) * 100) : 50;
        const mktSellPct = 100 - mktBuyPct;
        const cRange = Math.max(c.high - c.low, rawStep * 0.5);
        const bodySize = Math.abs(c.close - c.open);
        const upperWick = c.high - Math.max(c.open, c.close);
        const lowerWick = Math.min(c.open, c.close) - c.low;
        const closePos = (c.close - c.low) / cRange;

        // 1. SWING HIGH / SWING LOW RETAIL SL-SWEEP vs INSTITUTIONAL FAKE-WALL vs SOLID-WALL DETECTOR
        let prevSwingHigh = -Infinity, prevSwingLow = Infinity;
        for (let k = Math.max(0, idx - 5); k < idx; k++) {
            if (candles[k].high > prevSwingHigh) prevSwingHigh = candles[k].high;
            if (candles[k].low < prevSwingLow) prevSwingLow = candles[k].low;
        }
        const sweptSwingHigh = idx > 0 && c.high > prevSwingHigh;
        const sweptSwingLow = idx > 0 && c.low < prevSwingLow;
        const candleAvgLot = cVol / Math.max(c.trades || 1, 1);
        const isRetailLotBurst = (candleAvgLot < baselineLotSize * 1.15) && (rvol >= 0.85);

        // 2. CANDLE-LEVEL PULLING DELTA (PΔ = Total 10s+ Bid Pull - Total 10s+ Ask Pull) -> Stays ONLY in Candle Pill!
        const candlePullDelta = cBidPullSum - cAskPullSum;
        const cBpPct = cBidPullSum > 0 ? Math.round((cBidPullSum / (cBidPullSum + (c.sell_volume || 1))) * 100) : 0;
        const cApPct = cAskPullSum > 0 ? Math.round((cAskPullSum / (cAskPullSum + (c.buy_volume || 1))) * 100) : 0;

        let pullDeltaLine = `BP:${fmt(cBidPullSum)}(${cBpPct}%) AP:${fmt(cAskPullSum)}(${cApPct}%) PΔ:${candlePullDelta >= 0 ? '+' : ''}${fmt(candlePullDelta)}`;
        let pullDeltaCol = '#b0bec5';
        if (candlePullDelta < -avgV * 0.4 && c.close > c.open) {
            pullDeltaCol = '#00e5ff'; // Ask Spoof pulled + Price UP = Bull Win!
        } else if (candlePullDelta > avgV * 0.4 && c.close < c.open) {
            pullDeltaCol = '#ff4081'; // Bid Spoof pulled + Price DOWN = Bear Win!
        }

        let flowVerdict = `MB:${mktBuyPct}%|MS:${mktSellPct}%`;
        let candleDecision = `⚪ NEUTRAL / CHOP`;
        let verdictCol = '#8b93a7';
        let decisionCol = '#9ba2b0';
        let isHighConviction = false;

        if (rvol < 0.65 && totalWhalesInCandle === 0) {
            flowVerdict = `LOW FLOW (${rvol.toFixed(1)}x)`;
            candleDecision = `⚪ SKIP (RETAIL NOISE)`;
        } else {
            let cBull = 0, cBear = 0;
            if (mktSellPct >= 56 && (c.close >= c.open || lowerWick > bodySize * 0.75)) cBull += 34;
            if (mktBuyPct >= 56 && (c.close <= c.open || upperWick > bodySize * 0.75)) cBear += 34;
            if (mktBuyPct >= 58 && c.close > c.open && closePos >= 0.6) cBull += 30;
            if (mktSellPct >= 58 && c.close < c.open && closePos <= 0.4) cBear += 30;
            if (botSummary && closePos >= 0.45) cBull += 24;
            if (topSummary && closePos <= 0.55) cBear += 24;
            if (candlePullDelta < 0 && c.close > c.open) cBull += 20;
            if (candlePullDelta > 0 && c.close < c.open) cBear += 20;
            if (whaleBuyCount > whaleSellCount) cBull += 22;
            else if (whaleSellCount > whaleBuyCount) cBear += 22;

            // Priority Classification: Retail SL-Sweep vs Institutional Fake-Wall vs Solid-Wall
            if (sweptSwingLow && c.close > prevSwingLow && isRetailLotBurst && cBidPullSum < cVol * 0.25) {
                flowVerdict = `🎯SSL-SWEEP: Retail SLs Eaten ➔ Reversal!`; verdictCol = '#00e5ff'; isHighConviction = true; cBull += 25;
            } else if (sweptSwingHigh && c.close < prevSwingHigh && isRetailLotBurst && cAskPullSum < cVol * 0.25) {
                flowVerdict = `🎯BSL-SWEEP: Retail SLs Eaten ➔ Reject!`; verdictCol = '#ff9100'; isHighConviction = true; cBear += 25;
            } else if (candlePullDelta < -avgV * 0.6 && cApPct >= 55 && c.close > c.open) {
                flowVerdict = `⚠️FAKE-ASK WALL PULLED ➔ BULL BREAK!`; verdictCol = '#e040fb'; isHighConviction = true;
            } else if (candlePullDelta > avgV * 0.6 && cBpPct >= 55 && c.close < c.open) {
                flowVerdict = `⚠️FAKE-BID WALL PULLED ➔ BEAR DUMP!`; verdictCol = '#ff4081'; isHighConviction = true;
            } else if (topSummary && cAskPullSum < topSummary.val * 0.3 && c.close <= c.open) {
                flowVerdict = `🛡️SOLID-ASK WALL: Absorbed & Capped!`; verdictCol = '#ff9100'; isHighConviction = true;
            } else if (botSummary && cBidPullSum < botSummary.val * 0.3 && c.close >= c.open) {
                flowVerdict = `🛡️SOLID-BID WALL: Absorbed & Held!`; verdictCol = '#00e5ff'; isHighConviction = true;
            }

            const cNet = cBull - cBear;
            const whalePrefix = totalWhalesInCandle > 0 ? '◆' : '';

            if (cNet >= 20) {
                const cPct = Math.min(97, Math.round(52 + (cNet / 110) * 45));
                candleDecision = `${whalePrefix}🟢 BUYERS WIN (${cPct}%)`;
                decisionCol = '#00e676';
            } else if (cNet <= -20) {
                const cPct = Math.min(97, Math.round(52 + (Math.abs(cNet) / 110) * 45));
                candleDecision = `${whalePrefix}🔴 SELLERS WIN (${cPct}%)`;
                decisionCol = '#ff5252';
            } else {
                candleDecision = `${whalePrefix}⚖️ TWO-WAY FIGHT`;
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

        // 5-LINE CANDLE PILL (With Candle-Level Pulling Delta & SL-Sweep / Wall Verdict)
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

            // Line 3: Candle Pulling Delta (10s+ Old Resting Walls Only)
            ctx.font = 'bold 8.5px monospace';
            ctx.fillStyle = pullDeltaCol;
            ctx.fillText(pullDeltaLine, cx, pillY + 31);

            // Line 4: Retail SL-Sweep / Fake-Wall / Solid-Wall Verdict
            ctx.font = '8.5px monospace';
            ctx.fillStyle = verdictCol;
            ctx.fillText(flowVerdict, cx, pillY + 43);

            // Line 5: Final Winner
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

    const hudW = Math.min(chartW - 12, 480);
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
        ctx.fillText(`Px:${h.price.toFixed(dec)} | MS:${fmt(h.sellV)}(${h.bAbsPct}%) | MB:${fmt(h.buyV)}(${h.aAbsPct}%)`, 12, 33);
        ctx.fillStyle = '#ff80ab';
        ctx.fillText(`BPull:${fmt(h.bPull)}(${h.bPullPct}%) | APull:${fmt(h.aPull)}(${h.aPullPct}%) | PΔ:${h.lvlPullDelta >= 0 ? '+' : ''}${fmt(h.lvlPullDelta)}`, 12, 47);
        ctx.fillStyle = h.isWhaleLot ? '#ffd700' : '#b0bec5';
        const playerTag = h.isWhaleLot ? '🐋 WHALE BLOCK!' : (h.lotRatio < 1.2 ? '🐑 RETAIL / BOT SPLIT' : '⚡ STANDARD FLOW');
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
    // NEW SPINE-SPLIT BATTLE DOM (AUTO-CROPPED TO ACTIVE ORDERS + RIGHT PRICE SCALE)
    // =========================================================================
    if (!showDOM || domW <= 0) return;

    const domX = chartW + yAxisW;
    ctx.fillStyle = '#0a0d14';
    ctx.fillRect(domX, 0, domW, H);
    ctx.strokeStyle = '#00e5ff';
    ctx.lineWidth = 1;
    ctx.beginPath(); ctx.moveTo(domX, 0); ctx.lineTo(domX, H); ctx.stroke();

    const headerH = 56;
    const domStep = effStep;
    const bidsArr = obData.bids || [];
    const asksArr = obData.asks || [];
    const liveCenterP = Math.floor(lastC.close / domStep + 1e-9) * domStep;

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

    // Aggregate last 3 candles for genuine executed vs pulled battle in active zone
    const stickyMap = new Map();
    let minActivePrice = lastC.low;
    let maxActivePrice = lastC.high;

    candles.slice(-3).forEach(rc => {
        if (rc.low < minActivePrice) minActivePrice = rc.low;
        if (rc.high > maxActivePrice) maxActivePrice = rc.high;
        const gList = groupFootprint(rc.footprint, rc.low, rc.high, domStep, rc.close, liveRestBench);
        gList.forEach(gl => {
            const k = gl.price.toFixed(6);
            let cur = stickyMap.get(k) || { buy: 0, sell: 0, bp: 0, ap: 0 };
            cur.buy += gl.buy;
            cur.sell += gl.sell;
            cur.bp += gl.bid_pulled;
            cur.ap += gl.ask_pulled;
            stickyMap.set(k, cur);
        });
    });

    // AUTO-CROP: Only build rows where actual orders/executions exist (+ 4 buffer ticks at top/bottom)!
    const minVisibleBucket = Math.floor((minActivePrice - domStep * 4) / domStep) * domStep;
    const maxVisibleBucket = Math.ceil((maxActivePrice + domStep * 4) / domStep) * domStep;

    const activeDomRows = [];
    for (let p = maxVisibleBucket; p >= minVisibleBucket; p -= domStep) {
        const k = p.toFixed(6);
        const bRest = bidMap.get(k) || 0;
        const aRest = askMap.get(k) || 0;
        const st = stickyMap.get(k) || { buy: 0, sell: 0, bp: 0, ap: 0 };
        const distTicks = Math.round(Math.abs(p - liveCenterP) / domStep);

        // Keep row ONLY if it has genuine execution, resting depth near price, or is within 4 ticks of live price!
        if (st.buy > 0 || st.sell > 0 || distTicks <= 4 || ((bRest > 0 || aRest > 0) && distTicks <= 14)) {
            activeDomRows.push({ p, k, bRest, aRest, st });
        }
    }

    let maxRowVal = 1;
    let totExecBuy = 0, totExecSell = 0, totPullB = 0, totPullA = 0;
    activeDomRows.forEach(r => {
        totExecBuy += r.st.buy;
        totExecSell += r.st.sell;
        totPullB += r.st.bp;
        totPullA += r.st.ap;
        maxRowVal = Math.max(maxRowVal, r.st.buy, r.st.sell, r.st.bp, r.st.ap, r.bRest, r.aRest);
    });

    // Top Battle Header
    ctx.fillStyle = '#141926';
    ctx.fillRect(domX + 1, 0, domW - 1, headerH);
    ctx.strokeStyle = '#263147';
    ctx.beginPath(); ctx.moveTo(domX, headerH); ctx.lineTo(domX + domW, headerH); ctx.stroke();

    const domPullDelta = totPullB - totPullA;
    ctx.font = 'bold 10px monospace';
    ctx.textAlign = 'left';
    ctx.fillStyle = '#00e5ff';
    ctx.fillText(`⚔️ REAL EXEC: B:${fmt(totExecBuy)} vs S:${fmt(totExecSell)} | PΔ:${domPullDelta >= 0 ? '+' : ''}${fmt(domPullDelta)}`, domX + 6, 14);

    let battleWinner = '⚖️ TWO-WAY BATTLE';
    let bwCol = '#ffd700';
    if (totExecBuy > totExecSell * 1.15 && domPullDelta < 0) {
        battleWinner = '🟢 BUYERS WINNING (ASK SPOOF PULLED + REAL BUYING)';
        bwCol = '#00e676';
    } else if (totExecSell > totExecBuy * 1.15 && domPullDelta > 0) {
        battleWinner = '🔴 SELLERS WINNING (BID SPOOF PULLED + REAL SELLING)';
        bwCol = '#ff5252';
    } else if (totExecBuy > totExecSell * 1.2) {
        battleWinner = '🟢 GENUINE BUY FLOW DOMINATING';
        bwCol = '#00e676';
    } else if (totExecSell > totExecBuy * 1.2) {
        battleWinner = '🔴 GENUINE SELL FLOW DOMINATING';
        bwCol = '#ff5252';
    }
    ctx.fillStyle = bwCol;
    ctx.fillText(battleWinner, domX + 6, 29);

    // Layout: [BID SIDE (Left of Spine)] | SPINE | [ASK SIDE (Right of Spine)] | [PRICE SCALE (Right End)]
    const pxScaleW = isMobile ? 64 : 78;
    const battleAreaW = domW - pxScaleW;
    const halfBattleW = battleAreaW * 0.5;
    const domSpineX = domX + halfBattleW;
    const domPxScaleX = domX + battleAreaW;

    ctx.fillStyle = '#90a4ae';
    ctx.font = 'bold 9px monospace';
    ctx.textAlign = 'center';
    ctx.fillText('◀ BID [REAL SELL vs BID SPOOF]', domX + halfBattleW * 0.5, 46);
    ctx.fillText('ASK [REAL BUY vs ASK SPOOF] ▶', domSpineX + halfBattleW * 0.5, 46);
    ctx.fillStyle = '#ffeb3b';
    ctx.fillText('PRICE', domPxScaleX + pxScaleW * 0.5, 46);

    const rH = 24; // Taller 2-tier rows so Line 1 = Executed (Green/Red) & Line 2 = Pulled Spoof (Pink/Purple)!
    const maxVisDomRows = Math.floor((H - headerH - 6) / rH);
    const startIdx = Math.max(0, Math.min(activeDomRows.length - maxVisDomRows, Math.floor((activeDomRows.length - maxVisDomRows) * 0.5 - domScrollRows)));
    const visibleSlice = activeDomRows.slice(startIdx, startIdx + maxVisDomRows);

    let ry = headerH + 2;

    visibleSlice.forEach((row, idx) => {
        const { p, bRest, aRest, st } = row;
        const isCurrentRow = Math.abs(p - liveCenterP) < domStep * 0.4;

        ctx.fillStyle = isCurrentRow ? 'rgba(255, 235, 59, 0.16)' : (idx % 2 === 0 ? '#0c1018' : '#101520');
        ctx.fillRect(domX + 1, ry, domW - 1, rH);

        const bSpoofPct = (st.bp + st.sell) > 0 ? Math.round((st.bp / (st.bp + st.sell)) * 100) : 0;
        const aSpoofPct = (st.ap + st.buy) > 0 ? Math.round((st.ap / (st.ap + st.buy)) * 100) : 0;
        const rowPDelta = st.bp - st.ap;

        // 1. LEFT SIDE OF SPINE (BID SIDE):
        // Upper Half Bar (Red) = Genuine Executed Market Sells hitting Bid
        const execSellRatio = Math.min(1, st.sell / maxRowVal);
        if (st.sell > 0) {
            ctx.fillStyle = 'rgba(255, 23, 68, 0.55)';
            ctx.fillRect(domSpineX - halfBattleW * execSellRatio, ry + 2, halfBattleW * execSellRatio, 10);
        }
        // Lower Half Bar (Pink) = Pulled Bid Spoof Orders (Separate line!)
        const pullBidRatio = Math.min(1, st.bp / maxRowVal);
        if (st.bp > 0) {
            ctx.fillStyle = 'rgba(255, 64, 129, 0.40)';
            ctx.fillRect(domSpineX - halfBattleW * pullBidRatio, ry + 12, halfBattleW * pullBidRatio, 10);
        }

        ctx.textAlign = 'right';
        ctx.textBaseline = 'middle';
        ctx.font = 'bold 8.5px monospace';
        ctx.fillStyle = '#ff8a80';
        const winBidTag = (st.sell > st.buy * 1.4 && lastC.close >= p) ? '🛡️ABS ' : '';
        ctx.fillText(`${winBidTag}Exec:${fmt(st.sell)} (Rest:${fmt(bRest)})`, domSpineX - 4, ry + 7);

        if (st.bp > 0) {
            ctx.font = bSpoofPct >= 60 ? 'bold 8px monospace' : '8px monospace';
            ctx.fillStyle = bSpoofPct >= 60 ? '#ff4081' : '#ff80ab';
            ctx.fillText(`BID SPOOF:${fmt(st.bp)} (${bSpoofPct}%)`, domSpineX - 4, ry + 18);
        }

        // 2. RIGHT SIDE OF SPINE (ASK SIDE):
        // Upper Half Bar (Green) = Genuine Executed Market Buys hitting Ask
        const execBuyRatio = Math.min(1, st.buy / maxRowVal);
        if (st.buy > 0) {
            ctx.fillStyle = 'rgba(0, 230, 118, 0.55)';
            ctx.fillRect(domSpineX, ry + 2, halfBattleW * execBuyRatio, 10);
        }
        // Lower Half Bar (Purple) = Pulled Ask Spoof Orders (Separate line!)
        const pullAskRatio = Math.min(1, st.ap / maxRowVal);
        if (st.ap > 0) {
            ctx.fillStyle = 'rgba(224, 64, 251, 0.40)';
            ctx.fillRect(domSpineX, ry + 12, halfBattleW * pullAskRatio, 10);
        }

        ctx.textAlign = 'left';
        ctx.font = 'bold 8.5px monospace';
        ctx.fillStyle = '#69f0ae';
        const winAskTag = (st.buy > st.sell * 1.4 && lastC.close <= p) ? ' 🛡️CAP' : '';
        ctx.fillText(`Exec:${fmt(st.buy)} (Rest:${fmt(aRest)})${winAskTag}`, domSpineX + 4, ry + 7);

        if (st.ap > 0) {
            ctx.font = aSpoofPct >= 60 ? 'bold 8px monospace' : '8px monospace';
            ctx.fillStyle = aSpoofPct >= 60 ? '#e040fb' : '#ea80fc';
            ctx.fillText(`ASK SPOOF:${fmt(st.ap)} (${aSpoofPct}%)`, domSpineX + 4, ry + 18);
        }

        // 3. RIGHT-END PRICE SCALE + WINNER / P-DELTA TAG
        ctx.fillStyle = isCurrentRow ? '#ffeb3b' : '#161c2b';
        ctx.fillRect(domPxScaleX, ry, pxScaleW, rH);

        ctx.textAlign = 'center';
        ctx.font = isCurrentRow ? 'bold 9.5px monospace' : 'bold 9px monospace';
        ctx.fillStyle = isCurrentRow ? '#000' : (p > lastC.close ? '#ff8a80' : '#69f0ae');
        ctx.fillText(p.toFixed(dec), domPxScaleX + pxScaleW * 0.5, ry + 7);

        // Winner / P-Delta Sub-label under Price on Right Scale
        let subLbl = '';
        let subCol = isCurrentRow ? '#000' : '#ffd700';
        if (st.buy > st.sell * 1.3) { subLbl = '🟢BUY WIN'; subCol = isCurrentRow ? '#004d40' : '#00e676'; }
        else if (st.sell > st.buy * 1.3) { subLbl = '🔴SELL WIN'; subCol = isCurrentRow ? '#b71c1c' : '#ff5252'; }
        else if (Math.abs(rowPDelta) > 1) { subLbl = `PΔ:${rowPDelta > 0 ? '+' : ''}${fmt(rowPDelta)}`; subCol = isCurrentRow ? '#000' : '#ea80fc'; }

        if (subLbl) {
            ctx.font = 'bold 7.5px monospace';
            ctx.fillStyle = subCol;
            ctx.fillText(subLbl, domPxScaleX + pxScaleW * 0.5, ry + 18);
        }

        ctx.strokeStyle = '#1b2234';
        ctx.beginPath(); ctx.moveTo(domX, ry + rH); ctx.lineTo(domX + domW, ry + rH); ctx.stroke();

        ry += rH;
    });

    // Draw Center Spine & Right Price Scale Divider
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
}

setInterval(fetchData, 2500);
setInterval(fetchSmtPeer, 10000);
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


import gzip
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


if __name__ == "__main__":
    ensure_collector_started()
    app.run(host="0.0.0.0", port=10000, debug=False)
