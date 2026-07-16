"""risk.py — Gestión de riesgo con SL calibrado en ATR 1h y break-even lock."""

import logging
import math

import config
import exchange as _exchange

log = logging.getLogger("risk")

SL_MIN_PCT = 0.008
SL_MAX_PCT = 0.030

BE_ATR_MULT = 1.5
BE_BUFFER_MULT = 0.2
TRAIL_STEP_MULT = 0.6
HIGH_SCORE_BE_BUFFER_MULT = 0.25
HIGH_SCORE_TRAIL_STEP_MULT = 0.75


def _tp_rr(score: int, regime: str) -> float:
    if score >= 85:
        return 2.5
    return 1.5


def _atr(candles: list[dict], period: int = 14) -> float:
    trs = []
    for i in range(1, len(candles)):
        h, l, pc = candles[i]["high"], candles[i]["low"], candles[i - 1]["close"]
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    return sum(trs[-period:]) / min(period, len(trs)) if trs else 0.0


def _rp(coin: str | None, price: float) -> float:
    if coin:
        try:
            return _exchange._round_price(coin, price)
        except Exception:
            pass
    return round(price, 8)


def calc(
    side: str,
    entry: float,
    candles: list[dict],
    score: int = 78,
    symbol: str | None = None,
    regime: str = "bull",
    candles_1h: list[dict] | None = None,
) -> dict:
    coin = _exchange._hl_symbol(symbol) if symbol else None

    if candles_1h and len(candles_1h) >= 16:
        atr = _atr(candles_1h[:-1], period=14)
        mult = 1.2
        source = "1h"
    else:
        atr = _atr(candles, period=14)
        mult = 1.5
        source = "15m"

    if atr > 0 and entry > 0:
        atr_pct = atr / entry
    else:
        log.warning("ATR=0 o entry=0, usando fallback SL_PCT")
        atr_pct = config.SL_PCT / 100 / mult
        source = "fallback"

    raw_sl_pct = atr_pct * mult
    sl_pct = max(SL_MIN_PCT, min(SL_MAX_PCT, raw_sl_pct))
    rr = _tp_rr(score, regime)
    tp_pct = sl_pct * rr

    sl_dist = entry * sl_pct
    tp_dist = entry * tp_pct

    sl_raw = (entry - sl_dist) if side == "long" else (entry + sl_dist)
    tp_raw = (entry + tp_dist) if side == "long" else (entry - tp_dist)
    sl = _rp(coin, sl_raw)
    tp = _rp(coin, tp_raw)

    be_buffer_mult = HIGH_SCORE_BE_BUFFER_MULT if score >= 85 else BE_BUFFER_MULT
    trail_mult = HIGH_SCORE_TRAIL_STEP_MULT if score >= 85 else TRAIL_STEP_MULT

    if atr > 0:
        be_trigger_raw = (entry + BE_ATR_MULT * atr) if side == "long" else (entry - BE_ATR_MULT * atr)
        be_sl_raw = (entry + be_buffer_mult * atr) if side == "long" else (entry - be_buffer_mult * atr)
        be_trigger = _rp(coin, be_trigger_raw)
        be_sl = _rp(coin, be_sl_raw)
    else:
        be_trigger = None
        be_sl = None

    margin = config.MARGIN_USDT
    raw_qty = (margin * config.LEVERAGE) / entry
    if symbol:
        try:
            qty = _exchange.floor_qty(raw_qty, symbol)
        except Exception as exc:
            log.warning("No se pudo calcular floor_qty para %s: %s — truncando a 3 dec", symbol, exc)
            qty = math.floor(raw_qty * 1000) / 1000
    else:
        qty = math.floor(raw_qty * 1000) / 1000

    if coin:
        try:
            tick_dec = _exchange._get_tick_decimals(coin)
            min_step = 10 ** (-tick_dec)
        except Exception:
            min_step = 1e-6
    else:
        min_step = 1e-6

    raw_trail = trail_mult * sl_dist
    trail_step = round(max(raw_trail, min_step), 8)

    log.info(
        "[%s] score=%d regime=%s RR=%.1f margin=%.2f ATR_%s=%.8f atr_pct=%.4f%% sl_pct=%.3f%% tp_pct=%.3f%% SL=%.8f TP=%.8f be_trigger=%s be_sl=%s qty=%.8f trail=%.8f",
        side.upper(),
        score,
        regime,
        rr,
        margin,
        source,
        atr,
        atr_pct * 100,
        sl_pct * 100,
        tp_pct * 100,
        sl,
        tp,
        f"{be_trigger:.8f}" if be_trigger else "N/A",
        f"{be_sl:.8f}" if be_sl else "N/A",
        qty,
        trail_step,
    )

    return {
        "qty": qty,
        "sl": sl,
        "tp": tp,
        "atr": round(atr, 8),
        "trail_step": trail_step,
        "score": score,
        "tp_rr": rr,
        "be_trigger": be_trigger,
        "be_sl": be_sl,
    }


def check_breakeven(symbol: str, pos: dict, current_price: float) -> bool:
    if pos.get("be_locked"):
        return False
    be_trigger = pos.get("be_trigger")
    be_sl = pos.get("be_sl")
    if be_trigger is None or be_sl is None:
        return False
    side = pos["side"]
    triggered = (side == "long" and current_price >= be_trigger) or (side == "short" and current_price <= be_trigger)
    if not triggered:
        return False
    current_sl = pos.get("sl")
    if current_sl is not None:
        if side == "long" and be_sl <= current_sl:
            return False
        if side == "short" and be_sl >= current_sl:
            return False
    log.info(
        "[%s] Break-even lock activado | precio=%.8f trigger=%.8f be_sl=%.8f (antes sl=%.8f)",
        symbol,
        current_price,
        be_trigger,
        be_sl,
        current_sl or 0,
    )
    coin = _exchange._hl_symbol(symbol)
    pos["sl"] = _rp(coin, be_sl)
    pos["be_locked"] = True
    return True
