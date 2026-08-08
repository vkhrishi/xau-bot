# ============================================================
#  XAU LIQUIDITY SWEEP v1.0 - Gold (XAUUSDT) Perpetual Futures
#  Binance USDT-M Futures API  (XAUUSDT = TradFi gold perpetual)
#
#  STRATEGY (15-minute liquidity-sweep, 1:1 RR):
#    - Trend filter: 30 EMA.
#    - Support / Resistance = pivots (left=1, right=1) -> recent liquidity.
#    - LONG : price above 30 EMA. A candle wicks BELOW a support (pivot low)
#             but CLOSES back above it (stops swept = liquidity grab). Enter on
#             the break of that grab candle's HIGH. SL below the grab LOW.
#    - SHORT: mirror (below 30 EMA, sweep a resistance, reclaim, break grab low).
#    - Target: 1:1 risk-to-reward. Mechanical, no discretion.
#    - Session gate: 07:00-15:00 UTC (12:30-20:30 IST) London+NY overlap.
#
#  Standalone bot: mirrors the Binance/testnet plumbing of SM$/bot.py
#  (client, sizing, exchange SL/TP with fallbacks, WebSocket monitor).
#
#  Run (cron-style, every 1-5 min):  python xau_sweep_bot.py
#  Monitor mode (auto-spawned):      python xau_sweep_bot.py monitor
# ============================================================

from binance.um_futures import UMFutures
from binance.websocket.um_futures.websocket_client import UMFuturesWebsocketClient
import datetime
import logging
import json
import time
import os
import sys
import signal
from decimal import Decimal, ROUND_HALF_UP, ROUND_DOWN

# =============================================================
#  SECTION 1 - CONFIGURATION
# =============================================================

# Credentials: prefer environment variables; fall back to the shared testnet
# keys so the bot runs out-of-the-box on testnet. For LIVE, ALWAYS set
# BINANCE_API_KEY / BINANCE_API_SECRET in the environment and never commit them.
API_KEY    = "loIzlExfyBbyQI6OaL5FRL90Gnw1jtczafGic2JgTfPomajYipfASLSrdqIE80n8"
API_SECRET = "asSDoyc8wIMyI0pw1dzlIKfqEDboJQVJ9yFqvaM8aZl0vIUTx2lbupdBPr4WOKde"

# -- Account / market --
USE_TESTNET  = True        # START on testnet. Flip to False only after validating.
SYMBOL       = "XAUUSDT"   # Gold TradFi perpetual (listed on live AND testnet).
INTERVAL     = "15m"
LEVERAGE     = 10
CAPITAL_USDT = 200

# -- Risk --
RISK_PER_TRADE_PCT     = 1.0
MAX_RISK_USDT          = CAPITAL_USDT * RISK_PER_TRADE_PCT / 100
MAX_TRADES_SESSION     = 3
MAX_DAILY_LOSS_USDT    = 20
MAX_CONSECUTIVE_LOSSES = 3

# -- Strategy parameters --
EMA_TREND        = 30      # Trend filter EMA.
PIVOT_LEFT       = 1       # S/R pivot left bars  (TradingView "Left Bars" = 1).
PIVOT_RIGHT      = 1       # S/R pivot right bars (TradingView "Right Bars" = 1).
SWEEP_LOOKBACK   = 6       # Grab candle must be within the last N closed candles.
ENTRY_VALID_BARS = 3       # Stop entry stays live this many candles after the grab.
RR_RATIO         = 1.0     # 1:1 risk-to-reward target.

# Volume filter - replicates LuxAlgo "S/R Levels with Breaks" volume oscillator
# EXACTLY (EMA5 vs EMA10 of volume). Their chart setting is threshold=20, but on
# FXCM spot gold; Binance XAUUSDT volume differs, so ~10 works better here.
# Calibrated on Binance data - re-validate forward. Set 20 to match their config.
VOLUME_FILTER    = True
VOLUME_THRESHOLD = 20
VOLUME_FAST      = 5
VOLUME_SLOW      = 10

SL_BUFFER_TICKS       = 2    # Place SL this many ticks beyond the grab extreme.
MIN_SL_PCT            = 0.05  # Floor SL distance at 0.05% of price (avoid huge qty).
MAX_SL_PCT            = 1.0   # Skip setups whose grab range > 1.0% of price.
ENTRY_MAX_CHASE_FRAC  = 0.5   # Skip if price already ran > 0.5x the stop distance past it.

# -- Session (UTC): London + NY overlap = 07:00-15:00 UTC (12:30-20:30 IST) --
SESSION_START_UTC = (7, 0)
SESSION_END_UTC   = (15, 0)
TRADE_WEEKENDS    = False   # Gold TradFi desks shut on weekends.

# -- Files (cross-platform; override root via XAU_BOT_DIR) --
BASE_DIR = os.environ.get(
    "XAU_BOT_DIR",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "xau_data"))
os.makedirs(BASE_DIR, exist_ok=True)
STATE_FILE  = os.path.join(BASE_DIR, "state.json")
MONITOR_PID = os.path.join(BASE_DIR, "monitor.pid")
LOG_FILE    = os.path.join(BASE_DIR, "bot.log")
MONITOR_LOG = os.path.join(BASE_DIR, "monitor.log")
LEDGER_FILE = os.path.join(BASE_DIR, "forward_trades.jsonl")
SUMMARY_MARKER = os.path.join(BASE_DIR, "last_summary.txt")

# -- Forward-test loop cadence --
LOOP_INTERVAL_SEC = 60    # Scan every 60s while in-session or a trade is open.
LOOP_IDLE_SEC     = 300   # Idle poll every 5 min when outside the session.

# -- Logging --
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler()],
)


# =============================================================
#  SECTION 2 - UTILITIES
# =============================================================

def utc_now():
    return datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)

def in_session(now=None):
    """True if `now` (UTC) is inside the trading session and not a weekend."""
    now = now or utc_now()
    if not TRADE_WEEKENDS and now.weekday() >= 5:
        return False
    cur = now.hour * 60 + now.minute
    start = SESSION_START_UTC[0] * 60 + SESSION_START_UTC[1]
    end = SESSION_END_UTC[0] * 60 + SESSION_END_UTC[1]
    if start <= end:
        return start <= cur < end
    return cur >= start or cur < end   # crosses midnight

def load_state():
    today = utc_now().strftime("%Y-%m-%d")
    try:
        with open(STATE_FILE) as f:
            data = json.load(f)
        if data.get("date") == today:
            return data
    except Exception:
        pass
    return {"date": today, "trade_count": 0, "trades": [],
            "daily_pnl_usdt": 0.0, "last_grab_ts": None}

def save_state(state):
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)

def _append_ledger(trade):
    """Append a completed trade to the persistent forward-test ledger. Unlike
    the daily state file, this survives the midnight reset so the out-of-sample
    result accumulates across the whole forward test."""
    try:
        with open(LEDGER_FILE, "a") as f:
            f.write(json.dumps(trade) + "\n")
    except Exception as e:
        logging.warning("Ledger append failed: %s" % str(e))


# =============================================================
#  SECTION 3 - BINANCE CLIENT
# =============================================================

def get_client():
    if USE_TESTNET:
        client = UMFutures(key=API_KEY, secret=API_SECRET,
                           base_url="https://testnet.binancefuture.com")
        logging.info("Binance client: TESTNET")
    else:
        client = UMFutures(key=API_KEY, secret=API_SECRET)
        logging.info("Binance client: LIVE")
    return client

def setup_leverage(client):
    try:
        client.change_leverage(symbol=SYMBOL, leverage=LEVERAGE)
        logging.info("Leverage set: %dx" % LEVERAGE)
    except Exception as e:
        if "No need to change" not in str(e):
            logging.warning("Leverage error: %s" % str(e))
    try:
        client.change_margin_type(symbol=SYMBOL, marginType="ISOLATED")
    except Exception as e:
        if "No need to change" not in str(e):
            logging.warning("Margin type error: %s" % str(e))

def get_symbol_info(client):
    info = client.exchange_info()
    for s in info["symbols"]:
        if s["symbol"] == SYMBOL:
            tick_size = step_size = min_notional = None
            for f in s["filters"]:
                if f["filterType"] == "PRICE_FILTER":
                    tick_size = float(f["tickSize"])
                elif f["filterType"] == "LOT_SIZE":
                    step_size = float(f["stepSize"])
                elif f["filterType"] == "MIN_NOTIONAL":
                    min_notional = float(f.get("notional", 5))
            return {
                "tick_size": tick_size or 0.1,
                "step_size": step_size or 0.001,
                "min_notional": min_notional or 5.0,
                "price_precision": s.get("pricePrecision", 2),
                "qty_precision": s.get("quantityPrecision", 3),
            }
    return None

def round_price(price, tick_size):
    tick = Decimal(str(tick_size))
    steps = (Decimal(str(price)) / tick).to_integral_value(rounding=ROUND_HALF_UP)
    return float(steps * tick)

def round_qty(qty, step_size):
    step = Decimal(str(step_size))
    steps = (Decimal(str(qty)) / step).to_integral_value(rounding=ROUND_DOWN)
    return float(steps * step)

def get_price(client):
    return float(client.ticker_price(symbol=SYMBOL)["price"])

def get_balance(client):
    try:
        for a in client.account().get("assets", []):
            if a["asset"] == "USDT":
                return float(a["availableBalance"])
    except Exception as e:
        logging.warning("Balance read error: %s" % str(e))
    return 0.0


# =============================================================
#  SECTION 4 - FETCH CANDLES
# =============================================================

def fetch_candles(client, interval=INTERVAL, limit=150):
    try:
        raw = client.klines(symbol=SYMBOL, interval=interval, limit=limit)
        candles = []
        for k in raw:
            dt = datetime.datetime.fromtimestamp(
                k[0] / 1000, datetime.timezone.utc).replace(tzinfo=None)
            candles.append({
                "ts": dt.strftime("%Y-%m-%d %H:%M:%S"),
                "open": float(k[1]), "high": float(k[2]),
                "low": float(k[3]), "close": float(k[4]),
                "volume": float(k[5]),
            })
        logging.info("Fetched %d candles" % len(candles))
        return candles
    except Exception as e:
        logging.error("Candle fetch failed: %s" % str(e))
        return []


# =============================================================
#  SECTION 5 - INDICATORS
# =============================================================

def ema(data, period):
    """EMA aligned to input length (None padding for the warmup region)."""
    if len(data) < period:
        return [None] * len(data)
    k = 2 / (period + 1)
    result = [None] * (period - 1)
    result.append(sum(data[:period]) / period)
    for p in data[period:]:
        result.append(p * k + result[-1] * (1 - k))
    return result


def volume_osc(candles, fast=VOLUME_FAST, slow=VOLUME_SLOW):
    """Volume oscillator: % by which fast-EMA volume exceeds slow-EMA volume.
    Mirrors the LuxAlgo S/R 'volume threshold' break filter."""
    vols = [c.get("volume", 0.0) for c in candles]
    ef, es = ema(vols, fast), ema(vols, slow)
    out = []
    for a, b in zip(ef, es):
        out.append(100.0 * (a - b) / b if (a is not None and b not in (None, 0)) else None)
    return out


# =============================================================
#  SECTION 6 - STRATEGY ENGINE (liquidity sweep)
# =============================================================

def find_pivot_lows(candles, left=PIVOT_LEFT, right=PIVOT_RIGHT):
    """Indices where low[i] is a strict local minimum over [i-left, i+right]."""
    lows = [c["low"] for c in candles]
    out = []
    for i in range(left, len(lows) - right):
        seg = lows[i - left:i] + lows[i + 1:i + 1 + right]
        if seg and all(lows[i] < x for x in seg):
            out.append(i)
    return out

def find_pivot_highs(candles, left=PIVOT_LEFT, right=PIVOT_RIGHT):
    """Indices where high[i] is a strict local maximum over [i-left, i+right]."""
    highs = [c["high"] for c in candles]
    out = []
    for i in range(left, len(highs) - right):
        seg = highs[i - left:i] + highs[i + 1:i + 1 + right]
        if seg and all(highs[i] > x for x in seg):
            out.append(i)
    return out

def _grab_at(candles, g, ema30, plows, phighs, vol_osc):
    """If candle `g` is a valid liquidity-grab candle, return
    (direction, entry_stop, sl_level, ref_level); else (None, 0, 0, 0).

    LONG : closes above the 30 EMA, wicks BELOW a recent support (pivot low)
           but CLOSES back above it. Entry = STOP at the grab HIGH, SL = grab LOW.
    SHORT: mirror (below EMA, sweep a resistance/pivot high, reclaim).
           Entry = STOP at the grab LOW, SL = grab HIGH.
    A volume surge (VOLUME_THRESHOLD) is required when VOLUME_FILTER is on.
    """
    e = ema30[g]
    if e is None:
        return None, 0.0, 0.0, 0.0
    if VOLUME_FILTER and (vol_osc[g] is None or vol_osc[g] <= VOLUME_THRESHOLD):
        return None, 0.0, 0.0, 0.0
    G = candles[g]
    rng = G["high"] - G["low"]
    if rng <= 0 or rng > MAX_SL_PCT / 100 * G["close"]:
        return None, 0.0, 0.0, 0.0
    support = next((candles[p]["low"] for p in reversed(plows) if p < g), None)
    if support is not None and G["close"] > e and G["low"] < support and G["close"] > support:
        return "LONG", G["high"], G["low"], support
    resistance = next((candles[p]["high"] for p in reversed(phighs) if p < g), None)
    if resistance is not None and G["close"] < e and G["high"] > resistance and G["close"] < resistance:
        return "SHORT", G["low"], G["high"], resistance
    return None, 0.0, 0.0, 0.0


def check_sweep_signal(candles):
    """Find the most recent PENDING liquidity-grab setup on CLOSED candles.

    'Pending' = a valid grab formed within the last ENTRY_VALID_BARS, its
    stop-entry level has NOT yet been hit by a later closed candle, and the
    setup was NOT invalidated (SL level not touched first). The scanner enters
    via a STOP at the grab's extreme (grab high for LONG / grab low for SHORT)
    when price breaks it. Pass only closed candles.
    """
    n = len(candles)
    if n < EMA_TREND + SWEEP_LOOKBACK + 3:
        return {"signal": "NO_TRADE", "reason": "warmup (%d candles)" % n}

    ema30 = ema([c["close"] for c in candles], EMA_TREND)
    vol_osc = volume_osc(candles)
    plows = find_pivot_lows(candles)
    phighs = find_pivot_highs(candles)
    last = n - 1
    lo_bound = max(last - ENTRY_VALID_BARS, PIVOT_LEFT)

    for g in range(last, lo_bound - 1, -1):
        direction, entry_stop, sl_level, ref = _grab_at(candles, g, ema30, plows, phighs, vol_osc)
        if direction is None:
            continue
        after = candles[g + 1:]
        if direction == "LONG":
            if any(c["high"] >= entry_stop for c in after):   # break already fired
                continue
            if any(c["low"] <= sl_level for c in after):       # invalidated first
                continue
        else:
            if any(c["low"] <= entry_stop for c in after):
                continue
            if any(c["high"] >= sl_level for c in after):
                continue
        G = candles[g]
        return {"signal": direction, "grab_ts": G["ts"],
                "grab_low": G["low"], "grab_high": G["high"],
                "entry_stop": entry_stop, "sl": sl_level, "ref": ref,
                "reason": "pending %s grab, stop @ $%.2f" % (direction, entry_stop)}

    return {"signal": "NO_TRADE", "reason": "no pending grab"}


# =============================================================
#  SECTION 7 - POSITION SIZING & RISK
# =============================================================

def calculate_position(direction, entry_price, grab_low, grab_high, sym_info):
    """Structural SL (grab extreme) + 1:1 TP; qty from fixed-risk sizing."""
    tick = sym_info["tick_size"]
    if direction == "LONG":
        sl_price = grab_low - SL_BUFFER_TICKS * tick
        sl_distance = entry_price - sl_price
    else:
        sl_price = grab_high + SL_BUFFER_TICKS * tick
        sl_distance = sl_price - entry_price

    if sl_distance <= 0:
        return None

    min_sl = MIN_SL_PCT / 100 * entry_price
    if sl_distance < min_sl:
        sl_distance = min_sl
        sl_price = (entry_price - sl_distance) if direction == "LONG" \
            else (entry_price + sl_distance)

    tp_distance = sl_distance * RR_RATIO
    tp_price = (entry_price + tp_distance) if direction == "LONG" \
        else (entry_price - tp_distance)

    qty = MAX_RISK_USDT / sl_distance
    qty = min(qty, CAPITAL_USDT * LEVERAGE / entry_price)
    qty = round_qty(qty, sym_info["step_size"])

    if qty <= 0 or qty * entry_price < sym_info["min_notional"]:
        logging.warning("Position too small: qty=%s notional=$%.2f (min $%.2f)" % (
            qty, qty * entry_price, sym_info["min_notional"]))
        return None

    return {
        "qty": qty,
        "sl_price": round_price(sl_price, tick),
        "tp_price": round_price(tp_price, tick),
        "sl_distance": round(sl_distance, 2),
        "tp_distance": round(tp_distance, 2),
        "risk_usdt": round(qty * sl_distance, 2),
        "reward_usdt": round(qty * tp_distance, 2),
        "notional": round(qty * entry_price, 2),
        "leverage_used": round(qty * entry_price / CAPITAL_USDT, 1),
    }


# =============================================================
#  SECTION 8 - ORDER EXECUTION
# =============================================================

def place_entry(client, direction, qty):
    side = "BUY" if direction == "LONG" else "SELL"
    try:
        order = client.new_order(symbol=SYMBOL, side=side, type="MARKET", quantity=qty)
        oid = order.get("orderId", "N/A")
        logging.info("ENTRY: %s | Side:%s | Qty:%s | ID:%s" % (SYMBOL, side, qty, oid))
        return oid, order
    except Exception as e:
        logging.error("ENTRY FAILED: %s" % str(e))
        return None, None

def place_exit(client, direction, qty, reason):
    side = "SELL" if direction == "LONG" else "BUY"
    try:
        client.new_order(symbol=SYMBOL, side=side, type="MARKET",
                         quantity=qty, reduceOnly="true")
        logging.info("EXIT: %s | Qty:%s | %s" % (SYMBOL, qty, reason))
        return True
    except Exception as e:
        logging.error("EXIT FAILED: %s" % str(e))
        return False

def _place_stop(client, close_side, otype, stop_price, qty):
    """Place one conditional stop, trying three methods for exchange quirks.
    Method C (CONTRACT_PRICE) is what makes testnet accept STOP/TP orders that
    otherwise fail with -4120 ("use the Algo Order API")."""
    # A: closePosition (whole-position stop) — works on LIVE futures.
    try:
        client.new_order(symbol=SYMBOL, side=close_side, type=otype,
                         stopPrice=str(stop_price), closePosition="true",
                         workingType="MARK_PRICE")
        return True
    except Exception as e_a:
        last = e_a
    # B: reduceOnly + explicit qty.
    try:
        client.new_order(symbol=SYMBOL, side=close_side, type=otype,
                         stopPrice=str(stop_price), quantity=qty,
                         reduceOnly="true", workingType="MARK_PRICE")
        return True
    except Exception as e_b:
        last = e_b
    # C: reduceOnly + CONTRACT_PRICE working type (testnet-friendly).
    try:
        client.new_order(symbol=SYMBOL, side=close_side, type=otype,
                         stopPrice=str(stop_price), quantity=qty,
                         reduceOnly="true", workingType="CONTRACT_PRICE")
        return True
    except Exception as e_c:
        last = e_c
    raise last

def place_sl_tp_orders(client, direction, qty, sl_price, tp_price):
    """Exchange-side SL/TP as a SAFETY NET behind the WebSocket monitor."""
    close_side = "SELL" if direction == "LONG" else "BUY"
    sl_ok = tp_ok = False
    try:
        _place_stop(client, close_side, "STOP_MARKET", sl_price, qty)
        logging.info("SL order placed: %s at $%s" % (close_side, sl_price))
        sl_ok = True
    except Exception as e:
        logging.warning("Exchange SL not placed (%s) — monitor will manage SL" % str(e))
    try:
        _place_stop(client, close_side, "TAKE_PROFIT_MARKET", tp_price, qty)
        logging.info("TP order placed: %s at $%s" % (close_side, tp_price))
        tp_ok = True
    except Exception as e:
        logging.warning("Exchange TP not placed (%s) — monitor will manage TP" % str(e))
    if not (sl_ok or tp_ok):
        logging.warning("No exchange-side SL/TP active. Exits depend on the monitor.")
    return sl_ok and tp_ok

def cancel_open_orders(client):
    try:
        client.cancel_open_orders(symbol=SYMBOL)
    except Exception as e:
        if "No open orders" not in str(e) and "-2011" not in str(e):
            logging.warning("Cancel orders error: %s" % str(e))


# =============================================================
#  SECTION 9 - MONITOR PROCESS
# =============================================================

def _pid_alive(pid):
    """Cross-platform, NON-destructive check that a PID is running."""
    if not pid or pid <= 0:
        return False
    if os.name == "nt":
        import ctypes
        PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
        STILL_ACTIVE = 259
        k32 = ctypes.windll.kernel32
        h = k32.OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, False, pid)
        if not h:
            return False
        try:
            code = ctypes.c_ulong()
            if k32.GetExitCodeProcess(h, ctypes.byref(code)):
                return code.value == STILL_ACTIVE
            return False
        finally:
            k32.CloseHandle(h)
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True

def write_monitor_pid():
    with open(MONITOR_PID, "w") as f:
        f.write(str(os.getpid()))

def clear_monitor_pid():
    try:
        os.remove(MONITOR_PID)
    except OSError:
        pass

def is_monitor_running():
    try:
        with open(MONITOR_PID) as f:
            pid = int(f.read().strip())
    except (FileNotFoundError, ValueError):
        return False
    if _pid_alive(pid):
        return True
    clear_monitor_pid()
    return False

def run_monitor(client, state):
    """WebSocket mark-price monitor. Enforces the fixed SL/TP from the FIRST
    tick (no min-hold blind spot) — mechanical 1:1, no trailing/partial."""
    trades = state.get("trades", [])
    if not trades or trades[-1].get("exited"):
        return
    trade = trades[-1]

    direction = trade["direction"]
    qty = trade["qty"]
    entry_price = trade["entry_price"]
    sl_price = trade["sl_price"]
    tp_price = trade["tp_price"]

    logging.info("=== XAU MONITOR START ===")
    logging.info("  %s %s | Entry:$%.2f | Qty:%s" % (direction, SYMBOL, entry_price, qty))
    logging.info("  SL:$%.2f | TP:$%.2f" % (sl_price, tp_price))
    write_monitor_pid()

    mon = {"exited": False, "last_log": time.time()}

    def _exit(price, reason):
        if mon["exited"]:
            return
        mon["exited"] = True
        pnl = (price - entry_price) * qty if direction == "LONG" \
            else (entry_price - price) * qty
        logging.info("=== EXIT: %s | Price:$%.2f | P&L:$%+.2f ===" % (reason, price, pnl))
        cancel_open_orders(client)
        place_exit(client, direction, qty, reason)
        trade["exited"] = True
        trade["exit_price"] = round(price, 2)
        trade["exit_pnl"] = round(pnl, 2)
        trade["exit_reason"] = reason
        trade["exit_time"] = utc_now().strftime("%Y-%m-%d %H:%M:%S")
        state["daily_pnl_usdt"] = state.get("daily_pnl_usdt", 0) + pnl
        save_state(state)
        _append_ledger(trade)
        clear_monitor_pid()
        logging.info("  Daily P&L: $%+.2f" % state["daily_pnl_usdt"])

    def on_message(_, msg):
        if mon["exited"]:
            return
        try:
            data = json.loads(msg) if isinstance(msg, str) else msg
            if data.get("e") != "markPriceUpdate":
                return
            price = float(data["p"])
        except Exception:
            return

        now = time.time()
        if now - mon["last_log"] >= 30:
            pnl = (price - entry_price) * qty if direction == "LONG" \
                else (entry_price - price) * qty
            logging.info("  TICK $%.2f | P&L:$%+.2f" % (price, pnl))
            mon["last_log"] = now

        if direction == "LONG":
            if price <= sl_price:
                _exit(price, "STOP LOSS")
            elif price >= tp_price:
                _exit(price, "TAKE PROFIT")
        else:
            if price >= sl_price:
                _exit(price, "STOP LOSS")
            elif price <= tp_price:
                _exit(price, "TAKE PROFIT")

    stream_url = "wss://stream.binancefuture.com" if USE_TESTNET \
        else "wss://fstream.binance.com"
    ws = UMFuturesWebsocketClient(stream_url=stream_url, on_message=on_message)
    ws.mark_price(symbol=SYMBOL.lower(), speed=1)
    logging.info("WebSocket connected — monitoring %s" % SYMBOL)

    def handle_shutdown(signum, frame):
        if not mon["exited"]:
            logging.info("Shutdown signal — closing position")
            _exit(get_price(client), "SHUTDOWN")
        ws.stop()
        sys.exit(0)

    for sig in (getattr(signal, "SIGTERM", None), getattr(signal, "SIGINT", None)):
        if sig is not None:
            try:
                signal.signal(sig, handle_shutdown)
            except (ValueError, OSError):
                pass

    try:
        while not mon["exited"]:
            time.sleep(1)
    except KeyboardInterrupt:
        handle_shutdown(None, None)
    finally:
        ws.stop()
        clear_monitor_pid()

def _spawn_monitor():
    import subprocess
    script_path = os.path.abspath(__file__)
    kwargs = {"stdout": open(MONITOR_LOG, "a"), "stderr": subprocess.STDOUT}
    if os.name == "nt":
        kwargs["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP
    else:
        kwargs["start_new_session"] = True
    proc = subprocess.Popen([sys.executable, script_path, "monitor"], **kwargs)
    logging.info("Monitor spawned: PID %d" % proc.pid)


# =============================================================
#  SECTION 9b - BACKTEST HARNESS
# =============================================================
#  Replays the EXACT check_sweep_signal + calculate_position logic over
#  historical XAUUSDT candles so results are directly comparable to live.
#  Assumptions (kept explicit so you can reconcile with TradingView):
#    - Entry at the OPEN of the candle AFTER the trigger (no look-ahead).
#    - Exit at the SL/TP price; if a candle touches both, SL is assumed first
#      (conservative). One position at a time; identical-grab re-entry blocked.
#    - Round-trip taker fee applied on notional. Slippage/funding NOT modelled.
#    - Risk-circuit-breakers (daily loss / consec-loss) are NOT applied here —
#      this measures the raw edge, like a TradingView strategy test.

BACKTEST_RT_FEE_PCT    = 0.08   # Round-trip taker fee, % of notional (~2 x 0.04%).
BACKTEST_MAX_HOLD_BARS = 200    # Force-close an unresolved trade after N bars.
BACKTEST_SESSION_ONLY  = True   # Apply the 07:00-15:00 UTC session filter.


def fetch_history(client, interval=INTERVAL, days=90):
    """Page klines backward to collect ~`days` of candles (max 1500/call)."""
    ms = {"1m": 60_000, "3m": 180_000, "5m": 300_000, "15m": 900_000,
          "30m": 1_800_000, "1h": 3_600_000}
    step = ms.get(interval, 900_000)
    want = int(days * 24 * 60 * 60 * 1000 / step)
    end = int(time.time() * 1000)
    rows, guard = [], 0
    while len(rows) < want and guard < 200:
        guard += 1
        limit = min(1500, want - len(rows))
        try:
            batch = client.klines(symbol=SYMBOL, interval=interval, limit=limit, endTime=end)
        except Exception as e:
            logging.error("History fetch failed: %s" % str(e))
            break
        if not batch:
            break
        rows = batch + rows
        end = batch[0][0] - 1
        if len(batch) < limit:
            break
        time.sleep(0.15)

    seen, candles = set(), []
    for k in sorted(rows, key=lambda r: r[0]):
        if k[0] in seen:
            continue
        seen.add(k[0])
        dt = datetime.datetime.fromtimestamp(
            k[0] / 1000, datetime.timezone.utc).replace(tzinfo=None)
        candles.append({
            "ts": dt.strftime("%Y-%m-%d %H:%M:%S"),
            "open": float(k[1]), "high": float(k[2]),
            "low": float(k[3]), "close": float(k[4]), "volume": float(k[5]),
        })
    logging.info("History: %d candles (%s -> %s)" % (
        len(candles), candles[0]["ts"] if candles else "-",
        candles[-1]["ts"] if candles else "-"))
    return candles


def _simulate_forward(direction, sl_price, tp_price, candles, start_idx):
    """Walk forward from the entry candle; return (exit_price, reason, idx)."""
    end = min(start_idx + BACKTEST_MAX_HOLD_BARS, len(candles) - 1)
    for j in range(start_idx, end + 1):
        hi, lo = candles[j]["high"], candles[j]["low"]
        if direction == "LONG":
            if lo <= sl_price:      # SL assumed first if both touched
                return sl_price, "SL", j
            if hi >= tp_price:
                return tp_price, "TP", j
        else:
            if hi >= sl_price:
                return sl_price, "SL", j
            if lo <= tp_price:
                return tp_price, "TP", j
    return candles[end]["close"], "OPEN_END", end


def _replay(candles, sym_info):
    """Bar-by-bar replay of the STOP-entry liquidity-grab model.

    For each grab candle: rest a stop at its extreme for up to ENTRY_VALID_BARS
    candles. Fill at the stop when price breaks it; invalidate if the SL level
    is hit first. Reuses the exact _grab_at + calculate_position live logic.
    """
    trades = []
    last_grab_ts = None
    n = len(candles)
    ema30 = ema([c["close"] for c in candles], EMA_TREND)
    vol_osc = volume_osc(candles)
    plows = find_pivot_lows(candles)
    phighs = find_pivot_highs(candles)
    i = EMA_TREND + SWEEP_LOOKBACK + 2
    while i < n - 1:
        direction, entry_stop, sl_level, ref = _grab_at(candles, i, ema30, plows, phighs, vol_osc)
        if direction is None or candles[i]["ts"] == last_grab_ts:
            i += 1
            continue
        if BACKTEST_SESSION_ONLY and not in_session(
                datetime.datetime.strptime(candles[i]["ts"], "%Y-%m-%d %H:%M:%S")):
            i += 1
            continue

        # Rest the stop for up to ENTRY_VALID_BARS candles.
        entry_idx = None
        limit = min(i + ENTRY_VALID_BARS, n - 1)
        for j in range(i + 1, limit + 1):
            hi, lo = candles[j]["high"], candles[j]["low"]
            if direction == "LONG":
                if lo <= sl_level and hi < entry_stop:   # SL hit before the break
                    break
                if hi >= entry_stop:
                    entry_idx = j
                    break
            else:
                if hi >= sl_level and lo > entry_stop:
                    break
                if lo <= entry_stop:
                    entry_idx = j
                    break
        if entry_idx is None:
            i += 1
            continue

        entry = entry_stop   # stop fills at the grab extreme
        grab_low, grab_high = (sl_level, entry_stop) if direction == "LONG" \
            else (entry_stop, sl_level)
        pos = calculate_position(direction, entry, grab_low, grab_high, sym_info)
        if pos is None:
            i += 1
            continue

        exit_price, reason, jexit = _simulate_forward(
            direction, pos["sl_price"], pos["tp_price"], candles, entry_idx)
        gross = (exit_price - entry) * pos["qty"] if direction == "LONG" \
            else (entry - exit_price) * pos["qty"]
        fee = pos["notional"] * BACKTEST_RT_FEE_PCT / 100
        pnl = gross - fee
        trades.append({
            "direction": direction, "entry_ts": candles[entry_idx]["ts"],
            "exit_ts": candles[jexit]["ts"], "entry": round(entry, 2),
            "sl": pos["sl_price"], "tp": pos["tp_price"], "qty": pos["qty"],
            "risk": pos["risk_usdt"], "reason": reason,
            "pnl": round(pnl, 2), "R": round(pnl / pos["risk_usdt"], 3) if pos["risk_usdt"] else 0,
            "bars": jexit - entry_idx,
        })
        last_grab_ts = candles[i]["ts"]
        i = jexit if jexit > i else i + 1
    return trades


def _print_report(trades, days, candles):
    line = "=" * 60
    print("\n" + line)
    print(" XAU LIQUIDITY SWEEP - BACKTEST REPORT")
    print(line)
    print(" Symbol      : %s   Timeframe: %s" % (SYMBOL, INTERVAL))
    if candles:
        print(" Period      : %s  ->  %s  (~%d days)" % (
            candles[0]["ts"], candles[-1]["ts"], days))
    print(" Filters     : session_only=%s  RR=1:%.1f  fee(rt)=%.3f%%" % (
        BACKTEST_SESSION_ONLY, RR_RATIO, BACKTEST_RT_FEE_PCT))
    print(" Risk/trade  : $%.2f (%.1f%% of $%d)   Note: circuit-breakers NOT applied" % (
        MAX_RISK_USDT, RISK_PER_TRADE_PCT, CAPITAL_USDT))
    print(line)

    if not trades:
        print(" No trades generated.")
        print(line + "\n")
        return

    wins = [t for t in trades if t["pnl"] > 0]
    losses = [t for t in trades if t["pnl"] <= 0]
    n = len(trades)
    gp = sum(t["pnl"] for t in wins)
    gl = -sum(t["pnl"] for t in losses)
    pf = (gp / gl) if gl > 0 else float("inf")
    total = sum(t["pnl"] for t in trades)
    total_r = sum(t["R"] for t in trades)

    # Equity curve -> max drawdown ($ and R).
    eq = peak = maxdd = eqr = peakr = maxddr = 0.0
    mcw = mcl = cw = cl = 0
    for t in trades:
        eq += t["pnl"]; peak = max(peak, eq); maxdd = max(maxdd, peak - eq)
        eqr += t["R"]; peakr = max(peakr, eqr); maxddr = max(maxddr, peakr - eqr)
        if t["pnl"] > 0:
            cw += 1; cl = 0
        else:
            cl += 1; cw = 0
        mcw = max(mcw, cw); mcl = max(mcl, cl)

    def _split(dirn):
        d = [t for t in trades if t["direction"] == dirn]
        w = sum(1 for t in d if t["pnl"] > 0)
        return len(d), w, (100 * w / len(d) if d else 0)

    lc, lw, lwr = _split("LONG")
    sc, sw, swr = _split("SHORT")
    unresolved = sum(1 for t in trades if t["reason"] == "OPEN_END")

    print(" Trades      : %d   (LONG %d / SHORT %d)" % (n, lc, sc))
    print(" Win rate    : %.2f%%   (%d W / %d L)" % (100 * len(wins) / n, len(wins), len(losses)))
    print("   - LONG    : %.2f%% (%d)   SHORT: %.2f%% (%d)" % (lwr, lc, swr, sc))
    print(" Profit factor: %.2f" % pf)
    print(" Net P&L     : $%+.2f   (%.2f R)" % (total, total_r))
    print(" Gross W/L   : +$%.2f / -$%.2f" % (gp, gl))
    print(" Avg trade   : $%+.2f   Avg win $%+.2f   Avg loss $%+.2f" % (
        total / n, (gp / len(wins)) if wins else 0, (-gl / len(losses)) if losses else 0))
    print(" Max drawdown: $%.2f (%.2f%% of capital)   %.2f R" % (
        maxdd, 100 * maxdd / CAPITAL_USDT, maxddr))
    print(" Max consec  : %d wins / %d losses" % (mcw, mcl))
    print(" Avg hold    : %.1f bars (%.0f min)" % (
        sum(t["bars"] for t in trades) / n, sum(t["bars"] for t in trades) / n * 15))
    if unresolved:
        print(" Unresolved  : %d (hit BACKTEST_MAX_HOLD_BARS, closed at last close)" % unresolved)
    print(line)
    print(" Last 10 trades:")
    for t in trades[-10:]:
        print("  %s %-5s @ %8.2f  ->  %-8s %8.2f  %-8s $%+7.2f (%.2fR)" % (
            t["entry_ts"], t["direction"], t["entry"], t["reason"],
            t["tp"] if t["reason"] == "TP" else t["sl"], "", t["pnl"], t["R"]))
    print(line + "\n")


def run_backtest(days=90):
    # Backtest always uses REAL mainnet klines (public endpoints, no keys) so
    # results are comparable to TradingView — independent of USE_TESTNET.
    client = UMFutures()
    logging.info("Backtest data source: Binance mainnet (public klines)")
    sym_info = get_symbol_info(client)
    if sym_info is None:
        logging.error("Cannot get symbol info for %s" % SYMBOL)
        return
    candles = fetch_history(client, INTERVAL, days)
    if len(candles) < EMA_TREND + SWEEP_LOOKBACK + 5:
        logging.error("Not enough history: %d candles" % len(candles))
        return
    trades = _replay(candles, sym_info)
    _print_report(trades, days, candles)


# =============================================================
#  SECTION 10 - SCAN (one cycle) + MAIN
# =============================================================

def scan():
    now = utc_now()
    state = load_state()
    _maybe_session_summary()

    if is_monitor_running():
        logging.info("Monitor active — skip scan")
        return

    trades = state.get("trades", [])
    if trades and not trades[-1].get("exited"):
        logging.info("Unexited trade — restarting monitor")
        _spawn_monitor()
        return

    if not in_session(now):
        logging.info("[%s UTC] Outside session (07:00-15:00 UTC)" % now.strftime("%H:%M"))
        return

    # Risk gates -----------------------------------------------------------
    session_trades = sum(1 for t in trades if t.get("in_session"))
    if session_trades >= MAX_TRADES_SESSION:
        logging.info("Session trade limit reached (%d/%d)" % (session_trades, MAX_TRADES_SESSION))
        return
    if state.get("daily_pnl_usdt", 0) <= -MAX_DAILY_LOSS_USDT:
        logging.info("Daily loss limit hit: $%.2f" % state["daily_pnl_usdt"])
        return
    losses = 0
    for t in reversed(trades):
        if t.get("exited") and t.get("exit_pnl", 0) < 0:
            losses += 1
        else:
            break
    if losses >= MAX_CONSECUTIVE_LOSSES:
        logging.info("Circuit breaker: %d consecutive losses" % losses)
        return

    tag = "TESTNET" if USE_TESTNET else "LIVE"
    logging.info("=== %s SCAN | %s UTC | P&L:$%+.2f ===" % (
        tag, now.strftime("%Y-%m-%d %H:%M:%S"), state.get("daily_pnl_usdt", 0)))

    client = get_client()
    setup_leverage(client)
    sym_info = get_symbol_info(client)
    if sym_info is None:
        logging.error("Cannot get symbol info for %s" % SYMBOL)
        return

    raw = fetch_candles(client)
    if len(raw) < EMA_TREND + SWEEP_LOOKBACK + 3:
        logging.error("Not enough candles: %d" % len(raw))
        return
    closed = raw[:-1]   # drop the still-forming candle

    sig = check_sweep_signal(closed)
    direction = sig["signal"]
    logging.info("  Signal:%s | %s" % (direction, sig.get("reason", "")))
    if direction == "NO_TRADE":
        return

    # Avoid re-entering the identical setup after it already traded/exited.
    if sig.get("grab_ts") and sig["grab_ts"] == state.get("last_grab_ts"):
        logging.info("  -> Setup already traded (grab %s) — skip" % sig["grab_ts"])
        return

    price = get_price(client)
    entry_stop = sig["entry_stop"]
    sl_level = sig["sl"]
    stop_dist = abs(entry_stop - sl_level)

    # Stop entry: only enter once price BREAKS the grab extreme; skip if it has
    # already run too far past the stop (would wreck the 1:1 geometry).
    if direction == "LONG":
        if price < entry_stop:
            logging.info("  -> Waiting for break of $%.2f (px $%.2f)" % (entry_stop, price))
            return
        if price > entry_stop + ENTRY_MAX_CHASE_FRAC * stop_dist:
            logging.info("  -> Missed: px $%.2f too far above stop $%.2f" % (price, entry_stop))
            return
        grab_low, grab_high = sl_level, entry_stop
    else:
        if price > entry_stop:
            logging.info("  -> Waiting for break of $%.2f (px $%.2f)" % (entry_stop, price))
            return
        if price < entry_stop - ENTRY_MAX_CHASE_FRAC * stop_dist:
            logging.info("  -> Missed: px $%.2f too far below stop $%.2f" % (price, entry_stop))
            return
        grab_low, grab_high = entry_stop, sl_level

    pos = calculate_position(direction, price, grab_low, grab_high, sym_info)
    if pos is None:
        return

    logging.info("  PLAN | %s @ $%.2f | Qty:%s | SL:$%.2f | TP:$%.2f" % (
        direction, price, pos["qty"], pos["sl_price"], pos["tp_price"]))
    logging.info("       | Risk:$%.2f | Reward:$%.2f | RR:1:%.1f | Notional:$%.2f | Lev:%.1fx" % (
        pos["risk_usdt"], pos["reward_usdt"], RR_RATIO, pos["notional"], pos["leverage_used"]))

    balance = get_balance(client)
    required = pos["notional"] / LEVERAGE * 1.05
    if balance < required:
        logging.warning("Insufficient balance: $%.2f < $%.2f required" % (balance, required))
        return

    # EXECUTE ---------------------------------------------------------------
    oid, order = place_entry(client, direction, pos["qty"])
    if oid is None:
        return
    time.sleep(1)

    fill_price = price
    try:
        if order and order.get("avgPrice") and float(order["avgPrice"]) > 0:
            fill_price = float(order["avgPrice"])
    except (TypeError, ValueError):
        pass

    # Recompute SL/TP off the actual fill so the 1:1 geometry is exact.
    if direction == "LONG":
        sl_price = round_price(pos["sl_price"], sym_info["tick_size"])
        tp_price = round_price(fill_price + (fill_price - sl_price) * RR_RATIO, sym_info["tick_size"])
    else:
        sl_price = round_price(pos["sl_price"], sym_info["tick_size"])
        tp_price = round_price(fill_price - (sl_price - fill_price) * RR_RATIO, sym_info["tick_size"])

    place_sl_tp_orders(client, direction, pos["qty"], sl_price, tp_price)

    state["trade_count"] = state.get("trade_count", 0) + 1
    state["last_grab_ts"] = sig.get("grab_ts")
    state["trades"].append({
        "direction": direction, "symbol": SYMBOL, "qty": pos["qty"],
        "entry_price": round(fill_price, 2), "entry_id": str(oid),
        "sl_price": sl_price, "tp_price": tp_price,
        "sl_distance": pos["sl_distance"], "risk_usdt": pos["risk_usdt"],
        "grab_ts": sig.get("grab_ts"), "in_session": True,
        "testnet": USE_TESTNET, "exited": False,
        "time": utc_now().strftime("%Y-%m-%d %H:%M:%S"),
    })
    save_state(state)
    logging.info("=== %s TRADE | %s %s | $%.2f | Qty:%s | RR:1:%.1f ===" % (
        tag, direction, SYMBOL, fill_price, pos["qty"], RR_RATIO))
    _spawn_monitor()


def _session_summary(date_str, quiet_if_empty=False):
    """Log a one-line summary of `date_str`'s trades from the ledger, plus the
    cumulative forward-test P&L, so progress is visible without running report."""
    day, cum = [], 0.0
    try:
        with open(LEDGER_FILE) as f:
            for ln in f:
                ln = ln.strip()
                if not ln:
                    continue
                try:
                    t = json.loads(ln)
                except json.JSONDecodeError:
                    continue
                cum += t.get("exit_pnl", 0.0)
                if str(t.get("time", "")).startswith(date_str):
                    day.append(t)
    except FileNotFoundError:
        return
    if not day:
        if not quiet_if_empty:
            logging.info("SESSION SUMMARY %s | no trades | cum P&L:$%+.2f" % (date_str, cum))
        return
    w = sum(1 for t in day if t.get("exit_pnl", 0) > 0)
    pnl = sum(t.get("exit_pnl", 0.0) for t in day)
    logging.info("SESSION SUMMARY %s | Trades:%d W:%d L:%d WR:%.0f%% | P&L:$%+.2f | cum:$%+.2f" % (
        date_str, len(day), w, len(day) - w, 100 * w / len(day), pnl, cum))


def _maybe_session_summary():
    """Emit today's session summary ONCE, on the first scan after the session
    has closed. Stateless via a marker file, so it works under cron or loop.
    Waits for an open trade to settle; only logs on days that had trades."""
    now = utc_now()
    if now.hour * 60 + now.minute < SESSION_END_UTC[0] * 60 + SESSION_END_UTC[1]:
        return                                  # today's session hasn't closed yet
    today = now.strftime("%Y-%m-%d")
    try:
        with open(SUMMARY_MARKER) as f:
            if f.read().strip() == today:
                return                          # already summarized today
    except FileNotFoundError:
        pass
    if is_monitor_running():
        return                                  # a trade is still open; try next tick
    _session_summary(today, quiet_if_empty=True)
    try:
        with open(SUMMARY_MARKER, "w") as f:
            f.write(today)
    except OSError as e:
        logging.warning("Summary marker write failed: %s" % str(e))


def loop():
    """Continuous forward-test driver: scan on a cadence until Ctrl+C. Scans
    every LOOP_INTERVAL_SEC while in-session or a trade is open, otherwise idles
    at LOOP_IDLE_SEC. Per-cycle errors are logged, never fatal. The session
    summary is emitted by scan() itself, so cron and loop behave identically."""
    logging.info("=== XAU FORWARD-TEST LOOP | %s | scan=%ds idle=%ds ===" % (
        "TESTNET" if USE_TESTNET else "LIVE", LOOP_INTERVAL_SEC, LOOP_IDLE_SEC))
    try:
        while True:
            try:
                scan()
            except Exception as e:
                logging.error("Scan cycle error: %s" % str(e))
            active = is_monitor_running() or in_session()
            time.sleep(LOOP_INTERVAL_SEC if active else LOOP_IDLE_SEC)
    except KeyboardInterrupt:
        logging.info("Forward-test loop stopped by user")



def report():
    """Summarize the persistent forward-test ledger (real out-of-sample result)."""
    trades = []
    try:
        with open(LEDGER_FILE) as f:
            for ln in f:
                ln = ln.strip()
                if ln:
                    try:
                        trades.append(json.loads(ln))
                    except json.JSONDecodeError:
                        continue
    except FileNotFoundError:
        print("No forward-test trades logged yet: %s" % LEDGER_FILE)
        return

    bar = "=" * 60
    print("\n" + bar)
    print(" XAU FORWARD-TEST REPORT (testnet ledger)")
    print(bar)
    if not trades:
        print(" Ledger is empty.")
        print(bar + "\n")
        return

    def pnl(t):
        return t.get("exit_pnl", 0.0)
    n = len(trades)
    wins = [t for t in trades if pnl(t) > 0]
    losses = [t for t in trades if pnl(t) <= 0]
    gp = sum(pnl(t) for t in wins)
    gl = -sum(pnl(t) for t in losses)
    pf = (gp / gl) if gl > 0 else float("inf")
    net = sum(pnl(t) for t in trades)

    eq = peak = maxdd = 0.0
    cw = cl = mcw = mcl = 0
    for t in trades:
        eq += pnl(t); peak = max(peak, eq); maxdd = max(maxdd, peak - eq)
        if pnl(t) > 0:
            cw += 1; cl = 0
        else:
            cl += 1; cw = 0
        mcw = max(mcw, cw); mcl = max(mcl, cl)

    def split(d):
        s = [t for t in trades if t.get("direction") == d]
        w = sum(1 for t in s if pnl(t) > 0)
        return len(s), (100 * w / len(s) if s else 0)
    lc, lwr = split("LONG")
    sc, swr = split("SHORT")
    reasons = {}
    for t in trades:
        r = t.get("exit_reason", "?")
        reasons[r] = reasons.get(r, 0) + 1

    print(" Period      : %s  ->  %s" % (
        trades[0].get("time", "?"), trades[-1].get("exit_time", "?")))
    print(" Trades      : %d   (LONG %d / SHORT %d)" % (n, lc, sc))
    print(" Win rate    : %.2f%%   (%d W / %d L)" % (100 * len(wins) / n, len(wins), len(losses)))
    print("   - LONG %.1f%%   SHORT %.1f%%" % (lwr, swr))
    print(" Profit factor: %.2f" % pf)
    print(" Net P&L     : $%+.2f" % net)
    print(" Max drawdown: $%.2f (%.2f%% of $%d)" % (maxdd, 100 * maxdd / CAPITAL_USDT, CAPITAL_USDT))
    print(" Max consec  : %d W / %d L" % (mcw, mcl))
    print(" Exits       : " + ", ".join("%s=%d" % (k, v) for k, v in sorted(reasons.items())))
    print(bar + "\n")


def main():
    mode = sys.argv[1] if len(sys.argv) > 1 else "scan"
    if mode == "backtest":
        days = int(sys.argv[2]) if len(sys.argv) > 2 else 90
        run_backtest(days=days)
        return
    if mode == "report":
        report()
        return
    if mode == "loop":
        loop()
        return
    if mode == "monitor":
        logging.info("=== XAU MONITOR MODE ===")
        state = load_state()
        trades = state.get("trades", [])
        if not trades or trades[-1].get("exited"):
            logging.info("No active trade")
            return
        run_monitor(get_client(), state)
        return
    scan()


if __name__ == "__main__":
    main()
