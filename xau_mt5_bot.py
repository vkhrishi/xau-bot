# ============================================================
#  XAU LIQUIDITY SWEEP (MT5) v1.0 - Spot Gold (XAUUSD) via MetaTrader 5
#  Broker: IC Markets (or any MT5 broker) - legal in India, FXCM-equivalent feed
#
#  WHY THIS EXISTS
#  ---------------
#  The 76.53% backtest was measured on TradingView spot-gold (FXCM feed) using
#  LuxAlgo "S/R Levels with Breaks", whose volume filter reads TICK VOLUME.
#  Binance XAUUSDT perp uses traded-contract volume -> different distribution ->
#  the edge does not transfer. MT5 brokers (IC Markets) serve the SAME spot-gold
#  price series and expose TICK VOLUME, so this stack is the faithful replica.
#
#  STRATEGY (identical to xau_sweep_bot.py, 15m liquidity sweep, 1:1 RR):
#    - 30 EMA trend filter.
#    - S/R pivots (left=1, right=1) = recent liquidity pools.
#    - LONG : above 30 EMA, wick BELOW a support then CLOSE back above it
#             (stops swept). STOP-entry at the grab HIGH, SL below the grab LOW.
#    - SHORT: mirror. Target = 1:1 RR. Volume oscillator (EMA5 vs EMA10 of tick
#             volume) must exceed VOLUME_THRESHOLD - exactly LuxAlgo's rule.
#    - Session gate 07:00-15:00 UTC (London+NY overlap), weekdays only.
#
#  SETUP (Windows only - MetaTrader5 package needs the MT5 terminal):
#    1. Install the MetaTrader 5 terminal and log into an IC Markets DEMO account.
#    2. In the terminal: Tools > Options > Expert Advisors > "Allow algo trading".
#    3. pip install MetaTrader5
#    4. Add "XAUUSD" (or your broker's gold symbol) to Market Watch.
#    5. Fill MT5_LOGIN / MT5_PASSWORD / MT5_SERVER below (or set env vars).
#
#  RUN:
#    python xau_mt5_bot.py test        # verify connection + symbol
#    python xau_mt5_bot.py backtest 90 # replay N days of real spot-gold history
#    python xau_mt5_bot.py scan        # one scan; place a pending stop if a setup exists
#    python xau_mt5_bot.py positions   # show open positions / pending orders
#    python xau_mt5_bot.py loop        # continuous scan loop (cron alternative)
# ============================================================

import datetime
import logging
import time
import os
import sys
from decimal import Decimal, ROUND_HALF_UP, ROUND_DOWN

try:
    import MetaTrader5 as mt5
except ImportError:
    mt5 = None   # Allow --help / reading; every runtime path re-checks this.


# =============================================================
#  SECTION 1 - CONFIGURATION
# =============================================================

# -- MT5 / broker credentials (prefer env vars; DEMO account to start) --
MT5_LOGIN    = int(os.environ.get("MT5_LOGIN", "0") or 0)   # e.g. 51234567
MT5_PASSWORD = os.environ.get("MT5_PASSWORD", "")
MT5_SERVER   = os.environ.get("MT5_SERVER", "ICMarketsSC-Demo")
MT5_PATH     = os.environ.get("MT5_PATH", "")              # optional terminal64.exe path

USE_DEMO     = True          # Keep True until forward-validated on a demo account.
SYMBOL       = os.environ.get("XAU_MT5_SYMBOL", "XAUUSD")  # IC Markets gold symbol.
INTERVAL     = "15m"
CAPITAL_USDT = 200           # Nominal account balance for risk sizing.

# -- Risk --
RISK_PER_TRADE_PCT     = 1.0
MAX_RISK_USDT          = CAPITAL_USDT * RISK_PER_TRADE_PCT / 100   # $2 per trade.
MAX_TRADES_SESSION     = 3
MAX_DAILY_LOSS_USDT    = 20
MAX_CONSECUTIVE_LOSSES = 3
MAGIC                  = 776530   # EA magic number to tag this bot's orders.

# -- Strategy parameters (IDENTICAL to xau_sweep_bot.py) --
EMA_TREND        = 30
PIVOT_LEFT       = 1
PIVOT_RIGHT      = 1
SWEEP_LOOKBACK   = 6
ENTRY_VALID_BARS = 3
RR_RATIO         = 1.0

# Volume filter - LuxAlgo volume oscillator (EMA5 vs EMA10 of TICK volume).
# On MT5 spot gold this is tick volume, matching TradingView -> use their
# default threshold=20 to replicate the 76.53% study.
VOLUME_FILTER    = True
VOLUME_THRESHOLD = 20
VOLUME_FAST      = 5
VOLUME_SLOW      = 10

SL_BUFFER_TICKS      = 2
MIN_SL_PCT           = 0.05
MAX_SL_PCT           = 1.0
ENTRY_MAX_CHASE_FRAC = 0.5

# -- Session (UTC): London + NY overlap --
SESSION_START_UTC = (7, 0)
SESSION_END_UTC   = (15, 0)
TRADE_WEEKENDS    = False

# -- Order behaviour --
DEVIATION_POINTS  = 20        # Max slippage (points) for market fills.
PENDING_EXPIRY_BARS = ENTRY_VALID_BARS   # Cancel a stop order after N candles.

# -- Backtest --
BACKTEST_RT_FEE_PCT    = 0.03   # Round-trip cost ~ spread; gold ~0.15-0.30 typical.
BACKTEST_MAX_HOLD_BARS = 200
BACKTEST_SESSION_ONLY  = True

# -- Files --
BASE_DIR = os.environ.get(
    "XAU_MT5_DIR",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "xau_mt5_data"))
os.makedirs(BASE_DIR, exist_ok=True)
LOG_FILE = os.path.join(BASE_DIR, "mt5_bot.log")

LOOP_INTERVAL_SEC = 60
LOOP_IDLE_SEC     = 300

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler()],
)

# MT5 M15 timeframe constant is resolved lazily (mt5 may be None at import).
def _tf():
    return mt5.TIMEFRAME_M15


# =============================================================
#  SECTION 2 - MT5 CONNECTION
# =============================================================

def mt5_connect():
    """Initialise the MT5 terminal session. Returns True on success."""
    if mt5 is None:
        logging.error("MetaTrader5 package not installed. Run: pip install MetaTrader5")
        return False
    kwargs = {}
    if MT5_PATH:
        kwargs["path"] = MT5_PATH
    if MT5_LOGIN:
        kwargs.update(login=MT5_LOGIN, password=MT5_PASSWORD, server=MT5_SERVER)
    if not mt5.initialize(**kwargs):
        logging.error("mt5.initialize failed: %s", mt5.last_error())
        return False
    info = mt5.account_info()
    if info is None:
        logging.error("No account. Log into MT5 terminal or set MT5_LOGIN/PASSWORD/SERVER.")
        mt5.shutdown()
        return False
    logging.info("MT5 connected: login=%s server=%s balance=%.2f %s",
                 info.login, info.server, info.balance, info.currency)
    if not mt5.symbol_select(SYMBOL, True):
        logging.error("Symbol %s not found. Check broker's gold symbol name.", SYMBOL)
        mt5.shutdown()
        return False
    return True

def mt5_disconnect():
    if mt5 is not None:
        mt5.shutdown()

def get_symbol_info():
    """Return sizing metadata for SYMBOL, or None."""
    si = mt5.symbol_info(SYMBOL)
    if si is None:
        return None
    return {
        "tick_size": si.trade_tick_size or si.point or 0.01,
        "point": si.point or 0.01,
        "digits": si.digits or 2,
        "volume_min": si.volume_min or 0.01,
        "volume_max": si.volume_max or 100.0,
        "volume_step": si.volume_step or 0.01,
        "contract_size": si.trade_contract_size or 100.0,   # gold: 100 oz / lot
        "tick_value": si.trade_tick_value or 1.0,
        "stops_level": getattr(si, "trade_stops_level", 0),
    }


# =============================================================
#  SECTION 3 - UTILITIES
# =============================================================

def utc_now():
    return datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)

def in_session(now=None):
    now = now or utc_now()
    if not TRADE_WEEKENDS and now.weekday() >= 5:
        return False
    cur = now.hour * 60 + now.minute
    start = SESSION_START_UTC[0] * 60 + SESSION_START_UTC[1]
    end = SESSION_END_UTC[0] * 60 + SESSION_END_UTC[1]
    if start <= end:
        return start <= cur < end
    return cur >= start or cur < end

def round_price(price, tick_size):
    tick = Decimal(str(tick_size))
    steps = (Decimal(str(price)) / tick).to_integral_value(rounding=ROUND_HALF_UP)
    return float(steps * tick)

def round_lots(lots, step):
    s = Decimal(str(step))
    steps = (Decimal(str(lots)) / s).to_integral_value(rounding=ROUND_DOWN)
    return float(steps * s)


# =============================================================
#  SECTION 4 - FETCH CANDLES (MT5 rates -> candle dicts)
# =============================================================

def _rates_to_candles(rates):
    """Convert an MT5 rates structured array to the shared candle dict list.
    Uses tick_volume for the volume oscillator (matches LuxAlgo on spot gold)."""
    candles = []
    for r in rates:
        dt = datetime.datetime.utcfromtimestamp(int(r["time"]))
        candles.append({
            "ts": dt.strftime("%Y-%m-%d %H:%M:%S"),
            "open": float(r["open"]), "high": float(r["high"]),
            "low": float(r["low"]), "close": float(r["close"]),
            "volume": float(r["tick_volume"]),
        })
    return candles

def fetch_candles(limit=150):
    rates = mt5.copy_rates_from_pos(SYMBOL, _tf(), 0, limit)
    if rates is None or len(rates) == 0:
        logging.error("copy_rates_from_pos returned nothing: %s", mt5.last_error())
        return []
    candles = _rates_to_candles(rates)
    logging.info("Fetched %d candles (%s -> %s)",
                 len(candles), candles[0]["ts"], candles[-1]["ts"])
    return candles

def fetch_history(days=90):
    """Pull ~`days` of M15 spot-gold history from the broker feed."""
    end = utc_now()
    start = end - datetime.timedelta(days=days + 3)   # pad for weekends/warmup
    rates = mt5.copy_rates_range(SYMBOL, _tf(), start, end)
    if rates is None or len(rates) == 0:
        logging.error("copy_rates_range returned nothing: %s", mt5.last_error())
        return []
    candles = _rates_to_candles(rates)
    logging.info("History: %d candles (%s -> %s)",
                 len(candles), candles[0]["ts"], candles[-1]["ts"])
    return candles


# =============================================================
#  SECTION 5 - INDICATORS (identical logic)
# =============================================================

def ema(data, period):
    if len(data) < period:
        return [None] * len(data)
    k = 2 / (period + 1)
    result = [None] * (period - 1)
    result.append(sum(data[:period]) / period)
    for p in data[period:]:
        result.append(p * k + result[-1] * (1 - k))
    return result

def volume_osc(candles, fast=VOLUME_FAST, slow=VOLUME_SLOW):
    vols = [c.get("volume", 0.0) for c in candles]
    ef, es = ema(vols, fast), ema(vols, slow)
    out = []
    for a, b in zip(ef, es):
        out.append(100.0 * (a - b) / b if (a is not None and b not in (None, 0)) else None)
    return out


# =============================================================
#  SECTION 6 - STRATEGY ENGINE (identical logic)
# =============================================================

def find_pivot_lows(candles, left=PIVOT_LEFT, right=PIVOT_RIGHT):
    lows = [c["low"] for c in candles]
    out = []
    for i in range(left, len(lows) - right):
        seg = lows[i - left:i] + lows[i + 1:i + 1 + right]
        if seg and all(lows[i] < x for x in seg):
            out.append(i)
    return out

def find_pivot_highs(candles, left=PIVOT_LEFT, right=PIVOT_RIGHT):
    highs = [c["high"] for c in candles]
    out = []
    for i in range(left, len(highs) - right):
        seg = highs[i - left:i] + highs[i + 1:i + 1 + right]
        if seg and all(highs[i] > x for x in seg):
            out.append(i)
    return out

def _grab_at(candles, g, ema30, plows, phighs, vol_osc):
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
            if any(c["high"] >= entry_stop for c in after):
                continue
            if any(c["low"] <= sl_level for c in after):
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
                "reason": "pending %s grab, stop @ %.2f" % (direction, entry_stop)}
    return {"signal": "NO_TRADE", "reason": "no pending grab"}


# =============================================================
#  SECTION 7 - POSITION SIZING (lots) & RISK
# =============================================================

def calculate_position(direction, entry_price, grab_low, grab_high, sym):
    """Structural SL (grab extreme) + 1:1 TP; lot size from fixed-risk sizing.
    For gold: $ risk = lots * contract_size * sl_distance_price."""
    tick = sym["tick_size"]
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

    per_lot_risk = sym["contract_size"] * sl_distance   # $ lost per 1.0 lot at SL
    if per_lot_risk <= 0:
        return None
    lots = MAX_RISK_USDT / per_lot_risk
    lots = max(sym["volume_min"], min(lots, sym["volume_max"]))
    lots = round_lots(lots, sym["volume_step"])
    if lots < sym["volume_min"]:
        logging.warning("Computed lots %.4f below broker minimum %.2f", lots, sym["volume_min"])
        return None

    risk_usdt = lots * sym["contract_size"] * sl_distance
    return {
        "lots": lots,
        "sl_price": round_price(sl_price, tick),
        "tp_price": round_price(tp_price, tick),
        "sl_distance": round(sl_distance, 2),
        "tp_distance": round(tp_distance, 2),
        "risk_usdt": round(risk_usdt, 2),
        "reward_usdt": round(lots * sym["contract_size"] * tp_distance, 2),
    }


# =============================================================
#  SECTION 8 - ORDER EXECUTION (pending stop entries)
# =============================================================

def _count_bot_orders():
    """Open positions + pending orders tagged with our MAGIC."""
    pos = mt5.positions_get(symbol=SYMBOL) or []
    orders = mt5.orders_get(symbol=SYMBOL) or []
    p = sum(1 for x in pos if x.magic == MAGIC)
    o = sum(1 for x in orders if x.magic == MAGIC)
    return p, o

def place_pending_stop(direction, sig, sym):
    """Place a BUY_STOP / SELL_STOP at the grab extreme with attached SL/TP.
    This mirrors the strategy's stop-entry model natively in MT5."""
    entry = round_price(sig["entry_stop"], sym["tick_size"])
    grab_low, grab_high = sig["grab_low"], sig["grab_high"]
    pos = calculate_position(direction, entry, grab_low, grab_high, sym)
    if pos is None:
        logging.warning("Sizing rejected the setup; no order placed.")
        return None

    otype = mt5.ORDER_TYPE_BUY_STOP if direction == "LONG" else mt5.ORDER_TYPE_SELL_STOP
    expiry = datetime.datetime.now() + datetime.timedelta(
        minutes=15 * PENDING_EXPIRY_BARS)
    request = {
        "action": mt5.TRADE_ACTION_PENDING,
        "symbol": SYMBOL,
        "volume": pos["lots"],
        "type": otype,
        "price": entry,
        "sl": pos["sl_price"],
        "tp": pos["tp_price"],
        "deviation": DEVIATION_POINTS,
        "magic": MAGIC,
        "comment": "xau_sweep",
        "type_time": mt5.ORDER_TIME_SPECIFIED,
        "expiration": int(expiry.timestamp()),
        "type_filling": mt5.ORDER_FILLING_RETURN,
    }
    result = mt5.order_send(request)
    if result is None or result.retcode != mt5.TRADE_RETCODE_DONE:
        logging.error("Pending order failed: %s",
                      result.retcode if result else mt5.last_error())
        return None
    logging.info("PENDING %s STOP @ %.2f | lots=%.2f SL=%.2f TP=%.2f risk=$%.2f (ticket %s)",
                 direction, entry, pos["lots"], pos["sl_price"], pos["tp_price"],
                 pos["risk_usdt"], result.order)
    return result.order


# =============================================================
#  SECTION 9 - SCAN / LOOP
# =============================================================

def scan():
    """One scan cycle: honour session gate, skip if already exposed, else
    place a pending stop when a fresh liquidity-grab setup exists."""
    now = utc_now()
    if not in_session(now):
        logging.info("[%s] Outside session (%s UTC) - skip.", SYMBOL, now.strftime("%H:%M"))
        return
    open_pos, pending = _count_bot_orders()
    if open_pos > 0:
        logging.info("[%s] Position already open - monitor via MT5 (SL/TP attached).", SYMBOL)
        return
    if pending > 0:
        logging.info("[%s] Pending stop already resting - skip.", SYMBOL)
        return

    candles = fetch_candles(limit=EMA_TREND + SWEEP_LOOKBACK + 60)
    if not candles:
        return
    closed = candles[:-1]   # drop the still-forming candle
    sig = check_sweep_signal(closed)
    if sig["signal"] == "NO_TRADE":
        logging.info("[%s] No setup: %s", SYMBOL, sig["reason"])
        return

    sym = get_symbol_info()
    if sym is None:
        logging.error("No symbol info for %s.", SYMBOL)
        return
    logging.info("[%s] SETUP %s - %s", SYMBOL, sig["signal"], sig["reason"])
    place_pending_stop(sig["signal"], sig, sym)

def loop():
    logging.info("Loop mode - Ctrl+C to stop.")
    while True:
        try:
            scan()
        except Exception as e:
            logging.error("scan error: %s", e)
        _, pending = (0, 0)
        try:
            op, pend = _count_bot_orders()
        except Exception:
            op = pend = 0
        active = in_session() or op > 0 or pend > 0
        time.sleep(LOOP_INTERVAL_SEC if active else LOOP_IDLE_SEC)


# =============================================================
#  SECTION 10 - BACKTEST (real spot-gold history)
# =============================================================

def _simulate_forward(direction, sl_price, tp_price, candles, start_idx):
    end = min(start_idx + BACKTEST_MAX_HOLD_BARS, len(candles) - 1)
    for j in range(start_idx, end + 1):
        hi, lo = candles[j]["high"], candles[j]["low"]
        if direction == "LONG":
            if lo <= sl_price:
                return sl_price, "SL", j
            if hi >= tp_price:
                return tp_price, "TP", j
        else:
            if hi >= sl_price:
                return sl_price, "SL", j
            if lo <= tp_price:
                return tp_price, "TP", j
    return candles[end]["close"], "OPEN_END", end

def _replay(candles, sym):
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
        entry_idx = None
        limit = min(i + ENTRY_VALID_BARS, n - 1)
        for j in range(i + 1, limit + 1):
            hi, lo = candles[j]["high"], candles[j]["low"]
            if direction == "LONG":
                if lo <= sl_level and hi < entry_stop:
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
        entry = entry_stop
        grab_low, grab_high = (sl_level, entry_stop) if direction == "LONG" \
            else (entry_stop, sl_level)
        pos = calculate_position(direction, entry, grab_low, grab_high, sym)
        if pos is None:
            i += 1
            continue
        exit_price, reason, jexit = _simulate_forward(
            direction, pos["sl_price"], pos["tp_price"], candles, entry_idx)
        cs = sym["contract_size"]
        gross = (exit_price - entry) * pos["lots"] * cs if direction == "LONG" \
            else (entry - exit_price) * pos["lots"] * cs
        notional = entry * pos["lots"] * cs
        fee = notional * BACKTEST_RT_FEE_PCT / 100
        pnl = gross - fee
        trades.append({
            "direction": direction, "entry_ts": candles[entry_idx]["ts"],
            "exit_ts": candles[jexit]["ts"], "entry": round(entry, 2),
            "sl": pos["sl_price"], "tp": pos["tp_price"], "lots": pos["lots"],
            "risk": pos["risk_usdt"], "reason": reason,
            "pnl": round(pnl, 2),
            "R": round(pnl / pos["risk_usdt"], 3) if pos["risk_usdt"] else 0,
            "bars": jexit - entry_idx,
        })
        last_grab_ts = candles[i]["ts"]
        i = jexit if jexit > i else i + 1
    return trades

def _print_report(trades, days, candles):
    line = "=" * 60
    print("\n" + line)
    print(" XAU LIQUIDITY SWEEP (MT5) - BACKTEST REPORT")
    print(line)
    print(" Symbol      : %s   Timeframe: %s   Feed: MT5 tick-volume" % (SYMBOL, INTERVAL))
    if candles:
        print(" Period      : %s  ->  %s  (~%d days)" % (
            candles[0]["ts"], candles[-1]["ts"], days))
    print(" Filters     : session_only=%s  RR=1:%.1f  cost(rt)=%.3f%%  vol_thr=%d" % (
        BACKTEST_SESSION_ONLY, RR_RATIO, BACKTEST_RT_FEE_PCT, VOLUME_THRESHOLD))
    print(" Risk/trade  : $%.2f (%.1f%% of $%d)   Note: circuit-breakers NOT applied" % (
        MAX_RISK_USDT, RISK_PER_TRADE_PCT, CAPITAL_USDT))
    print(line)
    if not trades:
        print(" Trades      : 0   (no setups in window)")
        print(line)
        return
    wins = [t for t in trades if t["pnl"] > 0]
    losses = [t for t in trades if t["pnl"] <= 0]
    longs = [t for t in trades if t["direction"] == "LONG"]
    shorts = [t for t in trades if t["direction"] == "SHORT"]
    net = sum(t["pnl"] for t in trades)
    gW = sum(t["pnl"] for t in wins)
    gL = sum(t["pnl"] for t in losses)
    pf = (gW / abs(gL)) if gL else float("inf")
    total_R = sum(t["R"] for t in trades)
    wr = 100.0 * len(wins) / len(trades)
    # Max drawdown (equity curve in $).
    eq = 0.0
    peak = 0.0
    mdd = 0.0
    for t in trades:
        eq += t["pnl"]
        peak = max(peak, eq)
        mdd = max(mdd, peak - eq)
    unresolved = sum(1 for t in trades if t["reason"] == "OPEN_END")
    print(" Trades      : %d   (LONG %d / SHORT %d)" % (len(trades), len(longs), len(shorts)))
    print(" Win rate    : %.2f%%   (%d W / %d L)" % (wr, len(wins), len(losses)))
    print(" Profit factor: %.2f" % pf)
    print(" Net P&L     : $%+.2f   (%.2f R)" % (net, total_R))
    print(" Gross W/L   : +$%.2f / -$%.2f" % (gW, abs(gL)))
    if wins:
        print(" Avg win     : $%+.2f" % (gW / len(wins)))
    if losses:
        print(" Avg loss    : $%+.2f" % (gL / len(losses)))
    print(" Max drawdown: $%.2f (%.2f%% of capital)" % (mdd, 100.0 * mdd / CAPITAL_USDT))
    if unresolved:
        print(" Unresolved  : %d (hit BACKTEST_MAX_HOLD_BARS, closed at last close)" % unresolved)
    print(line)
    print(" Last 10 trades:")
    for t in trades[-10:]:
        print("  %s %-5s @ %9.2f  ->  %-8s %9.2f   $ %+7.2f (%.2fR)" % (
            t["entry_ts"], t["direction"], t["entry"], t["reason"], t["tp"]
            if t["reason"] == "TP" else t["sl"], t["pnl"], t["R"]))
    print(line)

def run_backtest(days=90):
    candles = fetch_history(days=days)
    if not candles:
        print("No history fetched - is the symbol correct and terminal logged in?")
        return
    sym = get_symbol_info()
    if sym is None:
        print("No symbol info.")
        return
    trades = _replay(candles, sym)
    _print_report(trades, days, candles)


# =============================================================
#  SECTION 11 - INSPECTION HELPERS
# =============================================================

def show_positions():
    pos = mt5.positions_get(symbol=SYMBOL) or []
    orders = mt5.orders_get(symbol=SYMBOL) or []
    print("Open positions (%d):" % len(pos))
    for p in pos:
        side = "LONG" if p.type == mt5.POSITION_TYPE_BUY else "SHORT"
        print("  #%s %s %.2f lots @ %.2f  SL=%.2f TP=%.2f  P&L=%.2f  magic=%s" % (
            p.ticket, side, p.volume, p.price_open, p.sl, p.tp, p.profit, p.magic))
    print("Pending orders (%d):" % len(orders))
    for o in orders:
        print("  #%s type=%s %.2f lots @ %.2f  SL=%.2f TP=%.2f  magic=%s" % (
            o.ticket, o.type, o.volume_current, o.price_open, o.sl, o.tp, o.magic))

def test_connection():
    candles = fetch_candles(limit=5)
    sym = get_symbol_info()
    tick = mt5.symbol_info_tick(SYMBOL)
    print("\nConnection OK.")
    if sym:
        print(" Symbol      : %s  digits=%d  point=%.5f  contract=%.0f" % (
            SYMBOL, sym["digits"], sym["point"], sym["contract_size"]))
        print(" Lots        : min=%.2f step=%.2f max=%.2f" % (
            sym["volume_min"], sym["volume_step"], sym["volume_max"]))
    if tick:
        print(" Price       : bid=%.2f ask=%.2f spread=%.2f" % (
            tick.bid, tick.ask, tick.ask - tick.bid))
    if candles:
        print(" Last candle : %s  close=%.2f  tick_vol=%.0f" % (
            candles[-1]["ts"], candles[-1]["close"], candles[-1]["volume"]))
    print(" In session  : %s (now %s UTC)\n" % (in_session(), utc_now().strftime("%H:%M")))


# =============================================================
#  SECTION 12 - MAIN
# =============================================================

def main():
    mode = sys.argv[1] if len(sys.argv) > 1 else "scan"

    if mode == "backtest":
        days = int(sys.argv[2]) if len(sys.argv) > 2 else 90
        if not mt5_connect():
            return
        try:
            run_backtest(days)
        finally:
            mt5_disconnect()
        return

    if not mt5_connect():
        return
    try:
        if mode == "test":
            test_connection()
        elif mode == "positions":
            show_positions()
        elif mode == "loop":
            loop()
        else:   # scan (default)
            scan()
    finally:
        mt5_disconnect()


if __name__ == "__main__":
    main()
