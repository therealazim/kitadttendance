"""
XAUUSD SESSION-SPECIFIC SIGNAL BOT v8.1
Backtest asosida optimallashtirilgan + TP-Extender funksiyasi qo'shilgan
"""

import asyncio
import logging
import os
import threading
import json
import ssl
import websocket
from datetime import datetime, timedelta
from http.server import HTTPServer, BaseHTTPRequestHandler
from dotenv import load_dotenv

load_dotenv()

from telegram import Update, Bot
from telegram.ext import Application, CommandHandler, ContextTypes

# ─── CONFIG ─────────────────────────────────────────────────
TELEGRAM_TOKEN   = os.environ.get("TELEGRAM_TOKEN", "")
CHAT_ID          = os.environ.get("CHAT_ID", "")
CHAT_ID_FALLBACK = os.environ.get("CHAT_ID_FALLBACK", "")

SYMBOL_MAIN    = "frxXAUUSD"
DERIV_APP_ID   = "1089"
TIMEFRAME_5M   = 300
TIMEFRAME_1M   = 60
BARS           = 200
EMA_FAST       = 50
EMA_SLOW       = 200

MIN_SCORE      = 2.5
MIN_RR_RATIO   = 1.2
VOLATILITY_THRESHOLD = 2.0

# ─── SESSION-SPECIFIC STRATEGIYALAR ────────────────────────
SESSION_CONFIG = {
    "OSIYO_TUGASHI": {
        "uzb_hours": [9], "active": False,
    },
    "LONDON_OPEN": {
        "uzb_hours": [10, 11], "active": True,
        "min_score": 2.5, "risk_mult": 0.7,
        "tp_mult": 1.8, "sl_mult": 1.2,
        "bull_weights": {
            "OB": 2.0, "EMA_CROSS": 2.0, "TREND_UP": 1.5,
            "FVG": 1.0, "CANDLE": 0.5,
        },
        "bear_weights": {
            "OB": 2.0, "EMA_CROSS": 2.0, "TREND_DOWN": 1.5,
            "FVG": 1.0, "CANDLE": 0.5,
        },
    },
    "LONDON_PEAK": {
        "uzb_hours": [12, 13, 14], "active": True,
        "min_score": 2.5, "risk_mult": 1.5,
        "tp_mult": 2.0, "sl_mult": 1.3,
        "bull_weights": {
            "OB": 1.5, "FVG": 1.5, "TREND_UP": 2.0,
            "SWEEP": 2.5, "CANDLE": 1.0, "SESSION": 0.5,
        },
        "bear_weights": {
            "OB": 1.5, "FVG": 1.5, "TREND_DOWN": 2.0,
            "SWEEP": 2.5, "CANDLE": 1.0, "SESSION": 0.5,
        },
    },
    "LONDON_LUNCH": {
        "uzb_hours": [15], "active": True,
        "min_score": 3.0, "risk_mult": 1.2,
        "tp_mult": 1.7, "sl_mult": 1.1,
        "bull_weights": {
            "OB": 2.0, "TREND_UP": 2.0, "CANDLE": 1.5,
            "SWEEP": 1.5, "RSI": 0.5,
        },
        "bear_weights": {
            "OB": 2.0, "TREND_DOWN": 2.0, "CANDLE": 1.5,
            "SWEEP": 1.5, "RSI": 0.5,
        },
    },
    "NY_OPEN": {
        "uzb_hours": [16, 17], "active": True,
        "min_score": 3.0, "risk_mult": 1.0,
        "tp_mult": 1.8, "sl_mult": 1.2,
        "bull_weights": {
            "OB": 2.0, "TREND_UP": 2.0, "RSI": 1.5,
            "EMA_CROSS": 1.0, "CANDLE": 0.5,
        },
        "bear_weights": {
            "OB": 2.0, "TREND_DOWN": 2.0, "RSI": 1.5,
            "EMA_CROSS": 1.0, "CANDLE": 0.5,
        },
    },
    "NY_PEAK": {
        "uzb_hours": [18, 19, 20], "active": True,
        "min_score": 4.5, "risk_mult": 0.5,
        "tp_mult": 2.0, "sl_mult": 1.5,
        "bull_weights": {
            "FVG": 2.0, "OB": 2.0, "TREND_UP": 2.0,
            "SWEEP": 2.0, "CANDLE": 0.5,
        },
        "bear_weights": {
            "FVG": 2.0, "OB": 2.0, "TREND_DOWN": 2.0,
            "SWEEP": 2.0, "CANDLE": 0.5,
        },
    },
    "NY_CLOSE": {
        "uzb_hours": [21, 22], "active": True,
        "min_score": 2.5, "risk_mult": 1.0,
        "tp_mult": 1.6, "sl_mult": 1.0,
        "bull_weights": {
            "FVG": 3.0, "OB": 2.0, "SWEEP": 1.5, "RSI": 0.5,
        },
        "bear_weights": {
            "FVG": 3.0, "OB": 2.0, "SWEEP": 1.5, "RSI": 0.5,
        },
    },
    "OSIYO_SEKIN": {
        "uzb_hours": [23, 0, 1], "active": True,
        "min_score": 3.5, "risk_mult": 0.5,
        "tp_mult": 1.5, "sl_mult": 1.0,
        "bull_weights": {
            "OB": 2.0, "TREND_UP": 2.0, "CANDLE": 1.5, "SWEEP": 1.0,
        },
        "bear_weights": {
            "OB": 2.0, "TREND_DOWN": 2.0, "CANDLE": 1.5, "SWEEP": 1.0,
        },
    },
}

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(message)s",
    level=logging.INFO,
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("bot_signals.log")
    ]
)
log = logging.getLogger(__name__)

# ─── STATE ──────────────────────────────────────────────────
auto_running  = True
active_signal = None
signal_stats  = {
    "total": 0, "buy": 0, "sell": 0,
    "tp_hit": 0, "sl_hit": 0,
    "avg_score": 0, "total_pips": 0,
    "last_direction": None, "last_time": None,
    "session_pnl": {},
}

def get_uzb_time():
    return datetime.utcnow() + timedelta(hours=5)

# ─── INDICATORS ─────────────────────────────────────────────
def calc_ema(values, period):
    k = 2.0 / (period + 1)
    result = [values[0]]
    for v in values[1:]:
        result.append(v * k + result[-1] * (1 - k))
    return result

def calc_rsi(closes, period=14):
    result = [None] * len(closes)
    gains, losses = [], []
    for i in range(1, len(closes)):
        diff = closes[i] - closes[i-1]
        gains.append(max(diff, 0))
        losses.append(max(-diff, 0))
        if i >= period:
            ag = sum(gains[-period:]) / period
            al = sum(losses[-period:]) / period
            rs = ag / al if al != 0 else 100
            result[i] = 100 - (100 / (1 + rs))
    return result

def calc_atr(highs, lows, closes, period=14):
    trs = [highs[0] - lows[0]]
    for i in range(1, len(closes)):
        trs.append(max(highs[i]-lows[i], abs(highs[i]-closes[i-1]), abs(lows[i]-closes[i-1])))
    result = [None] * (period - 1)
    for i in range(period-1, len(trs)):
        result.append(sum(trs[i-period+1:i+1]) / period)
    return result

# ─── DETECTORS ─────────────────────────────────────────────
def get_session_config(uzb_hour):
    for sname, config in SESSION_CONFIG.items():
        if uzb_hour in config["uzb_hours"]:
            return sname, config
    return "OSIYO_SEKIN", SESSION_CONFIG["OSIYO_SEKIN"]

def detect_liquidity_sweep(highs, lows, closes, lookback=8):
    if len(highs) < lookback + 3: return None
    swing_highs_set = set()
    swing_lows_set = set()
    for i in range(3, len(highs) - 3):
        if highs[i] == max(highs[i-3:i+4]): swing_highs_set.add(highs[i])
        if lows[i] == min(lows[i-3:i+4]): swing_lows_set.add(lows[i])
    if not swing_highs_set and not swing_lows_set: return None
    swing_highs_list = sorted(swing_highs_set)
    swing_lows_list = sorted(swing_lows_set)
    cur_close = closes[-1]
    for i in range(-min(lookback, len(highs)), -1):
        h = highs[i]; l = lows[i]
        for sh in swing_highs_list[-3:]:
            if abs(h - sh) / sh < 0.002 and h > sh * 1.001 and cur_close < sh: return "BEARISH_SWEEP"
        for sl in swing_lows_list[-3:]:
            if abs(l - sl) / sl < 0.002 and l < sl * 0.999 and cur_close > sl: return "BULLISH_SWEEP"
    return None

def detect_fvg(highs, lows, lookback=15):
    if len(highs) < 3: return None
    for i in range(-lookback, -2):
        try:
            if highs[i-1] < lows[i+1]: return "BULLISH_FVG", highs[i-1], lows[i+1]
            if lows[i-1] > highs[i+1]: return "BEARISH_FVG", lows[i-1], highs[i+1]
        except IndexError: pass
    return None

def detect_order_block(opens, closes, highs, lows, lookback=10):
    if len(closes) < lookback + 2: return None
    for i in range(-lookback, -1):
        body = abs(closes[i] - opens[i])
        if body == 0: continue
        prev_body = abs(closes[i-1] - opens[i-1])
        if prev_body == 0: continue
        if body / prev_body > 1.5:
            if closes[i] < opens[i] and closes[i+1] > opens[i+1]: return "BULLISH_OB", lows[i], highs[i]
            if closes[i] > opens[i] and closes[i+1] < opens[i+1]: return "BEARISH_OB", lows[i], highs[i]
    return None

def detect_ema_crossover(ema_fast, ema_slow, lookback=3):
    if len(ema_fast) < lookback + 1: return None
    for i in range(-lookback, -1):
        pf = ema_fast[i-1]; ps = ema_slow[i-1]
        cf = ema_fast[i]; cs = ema_slow[i]
        if pf <= ps and cf > cs: return "BULLISH_CROSS"
        if pf >= ps and cf < cs: return "BEARISH_CROSS"
    return None

def detect_ema_trend(ema_fast, ema_slow, lookback=5):
    if len(ema_fast) < lookback: return None
    fn = ema_fast[-1]; sn = ema_slow[-1]
    fp = ema_fast[-5]
    gap_pct = abs(fn - sn) / sn * 100
    if gap_pct < 0.05: return None
    if fn > sn and fn > fp: return "BULL_TREND"
    if fn < sn and fn < fp: return "BEAR_TREND"
    return None

def detect_rsi_signal(rsi_values, overbought=62, oversold=38):
    if len(rsi_values) < 2: return None
    cur = rsi_values[-1]; prv = rsi_values[-2]
    if cur is None or prv is None: return None
    if prv < oversold and cur >= oversold: return "RSI_BULLISH"
    if prv > overbought and cur <= overbought: return "RSI_BEARISH"
    if cur < oversold: return "RSI_OVERSOLD"
    if cur > overbought: return "RSI_OVERBOUGHT"
    return None

def detect_candle_pattern(opens, closes, highs, lows):
    if len(closes) < 2: return None
    o1, c1, h1, l1 = opens[-1], closes[-1], highs[-1], lows[-1]
    o2, c2 = opens[-2], closes[-2]
    body1 = abs(c1 - o1); total1 = h1 - l1
    if total1 == 0: return None
    ratio = body1 / total1
    uw = h1 - max(o1, c1); lw = min(o1, c1) - l1
    if ratio < 0.35:
        if uw > body1 * 2.5 and lw < body1 * 0.5: return "PIN_BAR_BEARISH"
        if lw > body1 * 2.5 and uw < body1 * 0.5: return "PIN_BAR_BULLISH"
    body2 = abs(c2 - o2)
    if body1 > body2 * 1.2:
        if c1 > o1 and c2 < o2: return "BULLISH_ENGULFING"
        if c1 < o1 and c2 > o2: return "BEARISH_ENGULFING"
    return None

# ─── DERIV API ────────────────────────────────────────────────
def deriv_request(req):
    result = []
    event = threading.Event()
    def on_open(ws): ws.send(json.dumps(req))
    def on_message(ws, msg):
        result.append(json.loads(msg))
        event.set()
        ws.close()
    ws = websocket.WebSocketApp(
        f"wss://ws.derivws.com/websockets/v3?app_id={DERIV_APP_ID}",
        on_open=on_open, on_message=on_message
    )
    t = threading.Thread(target=ws.run_forever, kwargs={"sslopt": {"cert_reqs": ssl.CERT_NONE}})
    t.daemon = True; t.start()
    event.wait(timeout=15)
    return result[0] if result else None

def deriv_candles(granularity, count):
    data = deriv_request({
        "ticks_history": SYMBOL_MAIN,
        "adjust_start_time": 1, "end": "latest",
        "style": "candles", "granularity": granularity, "count": count
    })
    if not data or "candles" not in data:
        log.warning(f"No data for granularity={granularity}")
        return None
    candles = data["candles"]
    return (
        [float(c["close"]) for c in candles],
        [float(c["open"]) for c in candles],
        [float(c["high"]) for c in candles],
        [float(c["low"]) for c in candles],
        [int(c["epoch"]) for c in candles],
    )

async def get_rates(granularity, outputsize):
    try:
        result = deriv_candles(granularity, outputsize)
        if result is None: return None
        closes, opens, highs, lows, times = result
        return {
            "closes": closes, "opens": opens, "highs": highs,
            "lows": lows, "times": times,
            "ema_fast": calc_ema(closes, EMA_FAST),
            "ema_slow": calc_ema(closes, EMA_SLOW),
            "rsi": calc_rsi(closes),
            "atr": calc_atr(highs, lows, closes),
        }
    except Exception as e:
        log.error(f"get_rates error: {e}")
        return None

# ─── SESSION-SPECIFIC SIGNAL GENERATION ────────────────────
def generate_signal(d_5m, d_1m):
    global active_signal, signal_stats
    if not d_5m or not d_1m: return None

    uzb_hour = (datetime.utcnow().hour + 5) % 24
    sess_name, session_config = get_session_config(uzb_hour)
    
    if not session_config.get("active", False):
        return None

    i_5 = len(d_5m["closes"]) - 1
    i_1 = len(d_1m["closes"]) - 1

    price = d_1m["closes"][i_1]
    atr_v = d_5m["atr"][i_5]
    if atr_v is None: return None

    atr_list = [x for x in d_5m["atr"][-20:] if x is not None]
    if len(atr_list) >= 10:
        atr_avg = sum(atr_list) / len(atr_list)
        if atr_v / atr_avg > VOLATILITY_THRESHOLD:
            log.info(f"HIGH VOLATILITY blocked: ATR={atr_v:.2f} avg={atr_avg:.2f}")
            return None
    else:
        atr_avg = atr_v

    ema_fast = d_5m["ema_fast"]
    ema_slow = d_5m["ema_slow"]
    ema50 = ema_fast[i_5] if ema_fast else None

    sweep_5 = detect_liquidity_sweep(d_5m["highs"], d_5m["lows"], d_5m["closes"], lookback=8)
    fvg_5 = detect_fvg(d_5m["highs"], d_5m["lows"], lookback=15)
    ob_5 = detect_order_block(d_5m["opens"], d_5m["closes"], d_5m["highs"], d_5m["lows"], lookback=10)
    ema_cross = detect_ema_crossover(ema_fast, ema_slow, lookback=3)
    ema_trend = detect_ema_trend(ema_fast, ema_slow)
    rsi_sig_5 = detect_rsi_signal(d_5m["rsi"])
    candle_5 = detect_candle_pattern(d_5m["opens"], d_5m["closes"], d_5m["highs"], d_5m["lows"])

    bull_score = 0; bear_score = 0
    bull_reasons = []; bear_reasons = []

    bw = session_config.get("bull_weights", {})
    bw2 = session_config.get("bear_weights", {})

    # Scoring
    if sweep_5 == "BULLISH_SWEEP" and "SWEEP" in bw: bull_score += bw["SWEEP"]; bull_reasons.append("Sweep")
    if sweep_5 == "BEARISH_SWEEP" and "SWEEP" in bw2: bear_score += bw2["SWEEP"]; bear_reasons.append("Sweep")

    if fvg_5 and fvg_5[0] == "BULLISH_FVG" and "FVG" in bw: bull_score += bw["FVG"]; bull_reasons.append("FVG")
    if fvg_5 and fvg_5[0] == "BEARISH_FVG" and "FVG" in bw2: bear_score += bw2["FVG"]; bear_reasons.append("FVG")

    if ob_5 and ob_5[0] == "BULLISH_OB" and "OB" in bw: bull_score += bw["OB"]; bull_reasons.append("OB")
    if ob_5 and ob_5[0] == "BEARISH_OB" and "OB" in bw2: bear_score += bw2["OB"]; bear_reasons.append("OB")

    if ema_cross == "BULLISH_CROSS" and "EMA_CROSS" in bw: bull_score += bw["EMA_CROSS"]; bull_reasons.append("EMA✕")
    if ema_cross == "BEARISH_CROSS" and "EMA_CROSS" in bw2: bear_score += bw2["EMA_CROSS"]; bear_reasons.append("EMA✕")

    if ema_trend == "BULL_TREND" and ema50 and price > ema50 and "TREND_UP" in bw: bull_score += bw["TREND_UP"]; bull_reasons.append("Trend↑")
    if ema_trend == "BEAR_TREND" and ema50 and price < ema50 and "TREND_DOWN" in bw2: bear_score += bw2["TREND_DOWN"]; bear_reasons.append("Trend↓")

    if rsi_sig_5 in ["RSI_BULLISH", "RSI_OVERSOLD"] and "RSI" in bw: bull_score += bw["RSI"]; bull_reasons.append("RSI")
    if rsi_sig_5 in ["RSI_BEARISH", "RSI_OVERBOUGHT"] and "RSI" in bw2: bear_score += bw2["RSI"]; bear_reasons.append("RSI")

    if candle_5 in ["PIN_BAR_BULLISH", "BULLISH_ENGULFING"] and "CANDLE" in bw: bull_score += bw["CANDLE"]; bull_reasons.append("Candle")
    if candle_5 in ["PIN_BAR_BEARISH", "BEARISH_ENGULFING"] and "CANDLE" in bw2: bear_score += bw2["CANDLE"]; bear_reasons.append("Candle")

    if "SESSION" in bw and bull_score > bear_score: bull_score += bw["SESSION"]
    if "SESSION" in bw2 and bear_score > bull_score: bear_score += bw2["SESSION"]

    min_score = session_config.get("min_score", 2.5)

    direction = None; score = 0; reasons = []
    if bull_score >= min_score and bull_score > bear_score:
        direction = "BUY"; score = bull_score; reasons = bull_reasons
    elif bear_score >= min_score and bear_score > bull_score:
        direction = "SELL"; score = bear_score; reasons = bear_reasons

    if direction is None: return None

    tp_mult = session_config.get("tp_mult", 1.8)
    sl_mult = session_config.get("sl_mult", 1.2)
    risk_mult = session_config.get("risk_mult", 1.0)

    tp_d = atr_avg * tp_mult
    sl_d = atr_avg * sl_mult
    sl_d = max(sl_d, atr_avg * 1.0)

    if tp_d / sl_d < MIN_RR_RATIO: return None

    # Stats
    signal_stats["total"] += 1
    if direction == "BUY": signal_stats["buy"] += 1
    else: signal_stats["sell"] += 1
    signal_stats["avg_score"] = (
        (signal_stats["avg_score"] * (signal_stats["total"] - 1)) + score
    ) / signal_stats["total"]
    signal_stats["last_direction"] = direction
    signal_stats["last_time"] = datetime.now()

    rsi_5_val = d_5m["rsi"][i_5]
    rsi_1_val = d_1m["rsi"][i_1] if "rsi" in d_1m else 0

    return {
        "type": direction, "symbol": SYMBOL_MAIN, "price": price,
        "tp": round(price + tp_d, 2) if direction == "BUY" else round(price - tp_d, 2),
        "sl": round(price - sl_d, 2) if direction == "BUY" else round(price + sl_d, 2),
        "score": round(score, 1), "reasons": reasons,
        "rsi_5": round(rsi_5_val, 1) if rsi_5_val else 0,
        "rsi_1": round(rsi_1_val, 1) if rsi_1_val else 0,
        "atr": round(atr_v, 2),
        "uzb_time": get_uzb_time().strftime('%H:%M'),
        "time": datetime.now(),
        "session": sess_name,
        "risk_mult": risk_mult,
    }

def format_signal(s):
    direction = s["type"]
    emoji = "🟢" if direction == "BUY" else "🔴"
    arrow = "⬆️" if direction == "BUY" else "⬇️"
    reasons_str = " | ".join(s["reasons"]) if s["reasons"] else "—"
    rr = round(abs(s["tp"] - s["price"]) / max(abs(s["sl"] - s["price"]), 0.01), 2)
    session = s.get("session", "N/A")
    risk = s.get("risk_mult", 1.0)
    risk_emoji = "🟢" if risk >= 1.5 else ("🟡" if risk >= 1.0 else "🔴")
    
    return (
        f"{emoji} *{direction} SIGNAL — XAUUSD* {arrow}\n"
        f"━━━━━━━━━━━━━━━━━\n"
        f"📊 Sessiya: `{session}`\n"
        f"💰 Entry:  `{s['price']}`\n"
        f"🎯 TP:     `{s['tp']}`\n"
        f"🛡 SL:     `{s['sl']}`\n"
        f"📐 R:R:    `1:{rr}`\n"
        f"━━━━━━━━━━━━━━━━━\n"
        f"⚡ Score:  `{s['score']}`\n"
        f"📊 Sabab:  `{reasons_str}`\n"
        f"📉 RSI M5: `{s['rsi_5']}` | M1: `{s['rsi_1']}`\n"
        f"📏 ATR:    `{s['atr']}`\n"
        f"{risk_emoji} Risk:   `{risk}x`\n"
        f"⏰ Vaqt:   `{s['uzb_time']}` UZB\n"
        f"━━━━━━━━━━━━━━━━━\n"
        f"⚠️ Risk boshqaruvi shart!"
    )

# ─── TP/SL CHECK ────────────────────────────────────────────
async def check_tp_sl(bot: Bot):
    global active_signal
    if not active_signal: return

    direction  = active_signal["type"]
    tp_price   = active_signal["tp"]
    sl_price   = active_signal["sl"]
    entry_price= active_signal["price"]
    entry_time = active_signal["time"]
    elapsed    = datetime.now() - entry_time

    if elapsed.total_seconds() > 14400:
        uzb_time = get_uzb_time()
        hours = elapsed.seconds // 3600
        minutes = (elapsed.seconds % 3600) // 60
        msg = (
            f"⏰ *SIGNAL EXPIRED*\n"
            f"📊 Signal: `{direction}`\n"
            f"💰 Entry: `{entry_price}`\n"
            f"⏱ Vaqt: `{hours}s {minutes}daq`\n"
            f"⏰ `{uzb_time.strftime('%H:%M')}` UZB"
        )
        await safe_send(bot, msg)
        active_signal = None
        return

    bars_needed = min(100, int(elapsed.total_seconds() / 60) + 5)
    data_1m = await get_rates(TIMEFRAME_1M, bars_needed)
    if not data_1m: return

    hit = None; hit_price = None
    entry_ts = int(entry_time.timestamp())

    for i in range(len(data_1m["highs"])):
        if i < len(data_1m["times"]) and data_1m["times"][i] < entry_ts: continue
        h = data_1m["highs"][i]; l = data_1m["lows"][i]
        if direction == "BUY":
            if h >= tp_price: hit = "TP"; hit_price = tp_price; break
            elif l <= sl_price: hit = "SL"; hit_price = sl_price; break
        else:
            if l <= tp_price: hit = "TP"; hit_price = tp_price; break
            elif h >= sl_price: hit = "SL"; hit_price = sl_price; break

    if not hit: return

    pips = round((hit_price - entry_price) * 10, 1) if direction == "BUY" else round((entry_price - hit_price) * 10, 1)
    signal_stats["total_pips"] += pips

    if hit == "TP": signal_stats["tp_hit"] += 1; emoji = "🎯"; result_text = "✅ TAKE PROFIT!"
    else: signal_stats["sl_hit"] += 1; emoji = "🛑"; result_text = "❌ STOP LOSS!"

    uzb_time = get_uzb_time()
    message = (
        f"{emoji} *SIGNAL NATIJASI*\n"
        f"━━━━━━━━━━━━━━━━━\n"
        f"{result_text}\n"
        f"📊 Signal: `{direction}`\n"
        f"💰 Entry: `{entry_price}`\n"
        f"💵 {hit}: `{hit_price}`\n"
        f"📐 Pips: `{'+' if pips > 0 else ''}{pips}`\n"
        f"⏱ Vaqt: `{elapsed.seconds//3600}s {(elapsed.seconds%3600)//60}daq`\n"
        f"━━━━━━━━━━━━━━━━━\n"
        f"📈 Stats: TP/SL: `{signal_stats['tp_hit']}/{signal_stats['sl_hit']}` | "
        f"Pips: `{'+' if signal_stats['total_pips'] > 0 else ''}{round(signal_stats['total_pips'], 1)}`\n"
        f"⏰ `{uzb_time.strftime('%H:%M')}` UZB\n"
        f"🔄 Yangi signal uchun yo'l ochildi!"
    )
    await safe_send(bot, message)
    active_signal = None


# ─── SAFE SEND ────────────────────────────────────────────────
async def safe_send(bot: Bot, text: str):
    targets = []
    if CHAT_ID: targets.append(CHAT_ID)
    if CHAT_ID_FALLBACK and CHAT_ID_FALLBACK != CHAT_ID: targets.append(CHAT_ID_FALLBACK)
    for cid in targets:
        try:
            await bot.send_message(chat_id=cid, text=text, parse_mode="Markdown")
            return
        except Exception as e:
            log.error(f"Failed to send to {cid}: {e}")

# ─── COMMANDS ────────────────────────────────────────────────
async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("🤖 *XAUUSD SESSION BOT v8.1*")

async def cmd_chatid(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(f"📋 *Chat ID:* `{update.effective_chat.id}`", parse_mode="Markdown")

async def cmd_signal(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    global active_signal
    await update.message.reply_text("⏳ Signal qidirilmoqda...")
    d_5m = await get_rates(TIMEFRAME_5M, BARS)
    d_1m = await get_rates(TIMEFRAME_1M, BARS)
    if not d_5m or not d_1m: return
    s = generate_signal(d_5m, d_1m)
    if s:
        await update.message.reply_text(format_signal(s), parse_mode="Markdown")
    else:
        await update.message.reply_text("⏳ Signal yo'q", parse_mode="Markdown")

async def cmd_clear(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    global active_signal
    active_signal = None
    await update.message.reply_text("🔄 Signal o'chirildi!")

async def cmd_analiz(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uzb_time = get_uzb_time()
    await update.message.reply_text(f"📊 *Analiz*\n⏰ `{uzb_time.strftime('%H:%M')}` UZB", parse_mode="Markdown")

async def cmd_sessions(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("📋 Sessiyalar", parse_mode="Markdown")

async def cmd_status(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("🤖 Holat", parse_mode="Markdown")

async def cmd_stats(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("📊 Statistika", parse_mode="Markdown")

async def cmd_start_bot(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    global auto_running
    auto_running = True
    await update.message.reply_text("🚀 Auto boshlandi!")

async def cmd_stop_bot(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    global auto_running
    auto_running = False
    await update.message.reply_text("🛑 To'xtatildi.")


# ─── SIGNAL LOOP (YANGILANGAN) ─────────────────────────────────
async def signal_loop(bot: Bot):
    global active_signal
    log.info("Signal loop started (v8.1 TP-Extender)")
    
    while auto_running:
        try:
            # 1. Faol tradening holatini tekshirish
            if active_signal is not None:
                await check_tp_sl(bot)

            # 2. Yangi tahlil qilish (Cheklov olib tashlandi: bot har doim tekshiradi)
            d_5m = await get_rates(TIMEFRAME_5M, BARS)
            d_1m = await get_rates(TIMEFRAME_1M, BARS)

            if d_5m and d_1m:
                s = generate_signal(d_5m, d_1m)
                
                if s:
                    # ─── YANGI: TP Extender Mantiqi ───
                    if active_signal is not None:
                        # Faol signal bilan bir xil yo'nalishdagi yangi signal paydo bo'lsa
                        if s["type"] == active_signal["type"]:
                            
                            # BUY trendda yuqoriroq TP chiqsa
                            if s["type"] == "BUY" and s["tp"] > active_signal["tp"]:
                                old_tp = active_signal["tp"]
                                active_signal["tp"] = s["tp"] # TP yangilandi
                                
                                alert_msg = (
                                    f"🔄 *TP KO'TARILDI (BUY)* 🔼\n"
                                    f"━━━━━━━━━━━━━━━━━\n"
                                    f"📈 Trend kuchaymoqda!\n"
                                    f"🎯 Eski TP: `{old_tp}`\n"
                                    f"🎯 Yangi TP: `{s['tp']}`\n\n"
                                    f"ℹ️ Foydani kengaytirish uchun TP-ni yangilang."
                                )
                                await safe_send(bot, alert_msg)
                                log.info(f"TP Extended (BUY) from {old_tp} to {s['tp']}")
                                
                            # SELL trendda pastroq TP chiqsa
                            elif s["type"] == "SELL" and s["tp"] < active_signal["tp"]:
                                old_tp = active_signal["tp"]
                                active_signal["tp"] = s["tp"] # TP yangilandi
                                
                                alert_msg = (
                                    f"🔄 *TP PASAYTIRILDI (SELL)* 🔽\n"
                                    f"━━━━━━━━━━━━━━━━━\n"
                                    f"📉 Trend kuchaymoqda!\n"
                                    f"🎯 Eski TP: `{old_tp}`\n"
                                    f"🎯 Yangi TP: `{s['tp']}`\n\n"
                                    f"ℹ️ Foydani kengaytirish uchun TP-ni yangilang."
                                )
                                await safe_send(bot, alert_msg)
                                log.info(f"TP Extended (SELL) from {old_tp} to {s['tp']}")
                    else:
                        # Hech qanday faol signal yo'q bo'lsa, odatiy yangi signal yuborish
                        active_signal = s
                        log.info(f"NEW SIGNAL [{s.get('session')}]: {s['type']} @ {s['price']}")
                        await safe_send(bot, format_signal(s))
            else:
                log.warning("No data from Deriv")

        except Exception as e:
            log.error(f"Loop error: {e}", exc_info=True)

        await asyncio.sleep(30) # 30 soniyalik kutish (Deriv server yuklamasini kamaytirish uchun)


# ─── HEALTH SERVER ───────────────────────────────────────────
class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200); self.end_headers()
        self.wfile.write(b"OK v8.1 | running")
    def log_message(self, *a): pass

def run_health_server():
    port = int(os.environ.get("PORT", 8080))
    HTTPServer(("0.0.0.0", port), HealthHandler).serve_forever()


# ─── MAIN ────────────────────────────────────────────────────
async def main():
    if not TELEGRAM_TOKEN: raise SystemExit(1)
    app = Application.builder().token(TELEGRAM_TOKEN).build()
    
    for cmd, handler in [
        ("start",     cmd_start), ("signal",    cmd_signal),
        ("clear",     cmd_clear), ("analiz",    cmd_analiz),
        ("status",    cmd_status), ("stats",     cmd_stats),
        ("sessions",  cmd_sessions), ("chatid",    cmd_chatid),
        ("start_bot", cmd_start_bot), ("stop_bot",  cmd_stop_bot),
    ]:
        app.add_handler(CommandHandler(cmd, handler))

    await app.initialize()
    await app.start()
    await app.updater.start_polling(drop_pending_updates=True)
    log.info("Bot polling started (v8.1)")
    asyncio.create_task(signal_loop(app.bot))
    await asyncio.Event().wait()

async def run_all():
    threading.Thread(target=run_health_server, daemon=True).start()
    await main()

if __name__ == "__main__":
    asyncio.run(run_all())
