# -*- coding: utf-8 -*-
"""
signal_engine.py — Motor de señales técnicas puras.

EXPORTA:
  SignalResult, analyze_pair, format_signal_block,
  MIN_SCORE, MIN_RR, SignalFlipGuard, signal_flip_guard,
  ManualCloseCooldown, manual_close_cooldown

ARQUITECTURA:
  signal_engine → lógica técnica pura (OHLCV + indicadores, SIN imports de strategy)
  strategy      → orquesta: llama analyze_pair + enriched_filter + IA
  trader        → ejecuta órdenes usando strategy.decide()

SISTEMA DE SCORING (max_score = 10):
  El bot detecta uno de tres tipos de setup. Si no encaja en ninguno, NEUTRAL.

CAMBIOS v15 (TP conservadores, trailing eliminado):
  - TP1 reducidos a multiplicadores alcanzables:
      TENDENCIA:  2.3x ATR → 1.5x ATR
      BREAKOUT:   2.3x ATR → 1.4x ATR
      REVERSAL:   2.0x ATR → 1.3x ATR
  - MIN_RR bajado de 1.5 a 1.2 para que señales con TP conservador pasen el filtro
  - STRONG lev now requires RR >= 1.8 (antes 2.0) para adaptarse a nuevos TPs
  - trailing_hl.py convertido en stub vacío (no ejecuta lógica)

CAMBIOS v14 (mejora integral estrategia):
  1. RR alcanzable: TP1 tendencia/breakout sube a 2.3x ATR; reversal a 2.0x ATR
  2. Breakout anti-fakeout: exige rotura mínima de 0.3x ATR fuera del rango
  3. Reversal con confirmación real: exige vela de giro (body en favor)
  4. EARLY leverage más conservador: 0.3x → 0.2x del máximo
  5. Volumen más reactivo: media 60 → 20 velas en 15m
  6. Pullback más fresco: lookback 3 → 2 velas
  7. Confluencia total de tendencia ahora vale +2
  8. Cooldown específico tras SL/MANUAL_CLOSE se gestiona en DecisionEngine
"""
from __future__ import annotations

import asyncio
import logging
import os
import time
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, Tuple

from bot.indicators import ema, rsi, macd, supertrend, atr as calc_atr

log = logging.getLogger(__name__)

MIN_SCORE: int  = int(os.getenv("MIN_SIGNAL_SCORE", "5"))
MIN_RR: float   = float(os.getenv("MIN_RR_REQUIRED", "1.2"))

_BARS_NEEDED = int(os.getenv("BARS_NEEDED", "100"))

_SL_ATR_MULT       = float(os.getenv("SL_ATR_MULT",  "1.5"))
# TP1 conservadores v15: alcanzables con alta probabilidad
_TP1_ATR_MULT      = float(os.getenv("TP1_ATR_MULT", "1.5"))   # TENDENCIA (era 2.3)
_TP2_ATR_MULT      = float(os.getenv("TP2_ATR_MULT", "3.5"))   # referencia interna, no se usa en órdenes
_MAX_LEV           = int(os.getenv("LEVERAGE", "15"))
_SL_CANDLE_BUFFER  = float(os.getenv("SL_CANDLE_BUFFER", "0.2"))

_VOL_AVG_WINDOW    = int(os.getenv("VOL_AVG_WINDOW", "20"))

_EMA_SPREAD_TREND_MIN  = float(os.getenv("EMA_SPREAD_TREND_MIN",  "0.002"))
_EMA_SPREAD_RANGE_MAX  = float(os.getenv("EMA_SPREAD_RANGE_MAX",  "0.0015"))
_BREAKOUT_WINDOW       = int(os.getenv("BREAKOUT_WINDOW", "20"))
_BREAKOUT_VOL_MIN      = float(os.getenv("BREAKOUT_VOL_MIN",  "1.4"))
_BREAKOUT_ATR_CONFIRM  = float(os.getenv("BREAKOUT_ATR_CONFIRM", "0.3"))
_REVERSAL_RSI_LOW      = float(os.getenv("REVERSAL_RSI_LOW",  "28"))
_REVERSAL_RSI_HIGH     = float(os.getenv("REVERSAL_RSI_HIGH", "72"))
_VOL_MIN_GLOBAL        = float(os.getenv("VOL_MIN_GLOBAL",    "0.6"))
_VOL_CONFIRM_MIN       = float(os.getenv("VOL_CONFIRM_MIN",   "1.2"))
_PULLBACK_LOOKBACK     = int(os.getenv("PULLBACK_LOOKBACK", "2"))
_PULLBACK_TOLERANCE    = float(os.getenv("PULLBACK_TOLERANCE", "0.005"))
_EARLY_LEV_FACTOR      = float(os.getenv("EARLY_LEV_FACTOR", "0.2"))


def _to_ccxt_symbol(symbol: str) -> str:
    if "/USDC:USDC" in symbol:
        return symbol
    coin = (
        symbol
        .replace(":USDT", "").replace(":USDC", "")
        .replace("/USDT", "").replace("/USDC", "")
        .replace("/USD",  "").replace("USDT",  "")
        .upper().strip()
    )
    return f"{coin}/USDC:USDC"


@dataclass
class SignalResult:
    symbol:        str
    signal:        str
    entry_mode:    str
    score:         int
    max_score:     int
    entry:         float
    sl:            float
    tp1:           float
    tp2:           float
    atr:           float
    rr:            float
    suggested_lev: int
    indicators:    Dict
    is_valid:      bool = True
    reason:        str  = ""
    signal_block:  str  = ""
    extra:         Dict = field(default_factory=dict)


async def analyze_pair(
    exch,
    symbol: str,
    ohlcv_fn: Optional[Callable] = None,
) -> SignalResult:
    try:
        if ohlcv_fn is not None:
            bars_15m, bars_1h, bars_4h = await asyncio.gather(
                ohlcv_fn("15m"),
                ohlcv_fn("1h"),
                ohlcv_fn("4h"),
                return_exceptions=False,
            )
            bars_15m = bars_15m or []
            bars_1h  = bars_1h  or []
            bars_4h  = bars_4h  or []
        else:
            bars_15m, bars_1h, bars_4h = await asyncio.gather(
                _fetch_bars(exch, symbol, "15m", _BARS_NEEDED),
                _fetch_bars(exch, symbol, "1h",  _BARS_NEEDED),
                _fetch_bars(exch, symbol, "4h",  max(50, _BARS_NEEDED // 2)),
                return_exceptions=False,
            )
    except Exception as e:
        log.error("[signal_engine] OHLCV fetch error %s: %s", symbol, e)
        return _hold_result(symbol, f"OHLCV error: {e}")

    if len(bars_15m) < 30:
        return _hold_result(symbol, f"Insuficientes velas 15m ({len(bars_15m)})")

    ind_15m = _compute_indicators(bars_15m)
    ind_1h  = _compute_indicators(bars_1h) if len(bars_1h) >= 30 else {}
    ind_4h  = _compute_indicators(bars_4h) if len(bars_4h) >= 20 else {}

    indicators = {
        "15m": ind_15m, "1h": ind_1h, "4h": ind_4h,
        "_closes_15m": [b[4] for b in bars_15m[-5:]],
    }

    vol_ratio_15m = ind_15m.get("vol_ratio", 1.0)
    if vol_ratio_15m < _VOL_MIN_GLOBAL:
        return _hold_result(symbol, f"Vol={vol_ratio_15m:.2f}x — mercado dormido (min {_VOL_MIN_GLOBAL}x)")

    setup_type, signal_str, score, max_score, reasons = _detect_setup(
        ind_15m, ind_1h, ind_4h, bars_15m
    )

    if signal_str == "NEUTRAL" or setup_type is None:
        return _hold_result(symbol, f"NEUTRAL ({', '.join(reasons[-3:])})")

    last_bar    = bars_15m[-1]
    close_price = float(last_bar[4])
    high_price  = float(last_bar[2])
    low_price   = float(last_bar[3])
    entry = close_price

    atr_val = float(ind_15m.get("atr", 0) or 0)
    if atr_val <= 0:
        return _hold_result(symbol, "ATR=0")

    _atr_buf = _SL_CANDLE_BUFFER * atr_val

    # v15: multiplicadores TP1 conservadores por tipo de setup
    if setup_type == "REVERSAL":
        sl_mult  = float(os.getenv("SL_ATR_MULT_REVERSAL",  "1.2"))
        tp1_mult = float(os.getenv("TP1_ATR_MULT_REVERSAL", "1.3"))  # era 2.0
        tp2_mult = float(os.getenv("TP2_ATR_MULT_REVERSAL", "3.5"))
    elif setup_type == "BREAKOUT":
        sl_mult  = _SL_ATR_MULT
        tp1_mult = float(os.getenv("TP1_ATR_MULT_BREAKOUT", "1.4"))  # era 2.3
        tp2_mult = float(os.getenv("TP2_ATR_MULT_BREAKOUT", "3.5"))
    else:  # TENDENCIA
        sl_mult  = _SL_ATR_MULT
        tp1_mult = _TP1_ATR_MULT   # 1.5x (env override disponible)
        tp2_mult = _TP2_ATR_MULT

    if signal_str == "LONG":
        sl  = round(min(low_price  - _atr_buf, entry - sl_mult  * atr_val), 6)
        tp1 = round(entry + tp1_mult * atr_val, 6)
        tp2 = round(entry + tp2_mult * atr_val, 6)
    else:
        sl  = round(max(high_price + _atr_buf, entry + sl_mult  * atr_val), 6)
        tp1 = round(entry - tp1_mult * atr_val, 6)
        tp2 = round(entry - tp2_mult * atr_val, 6)

    risk   = abs(entry - sl)
    reward = abs(tp1 - entry)
    rr     = round(reward / risk, 2) if risk > 0 else 0.0

    if score >= max_score - 1:
        entry_mode = "STRONG"
    elif score >= MIN_SCORE + 1:
        entry_mode = "NORMAL"
    else:
        entry_mode = "EARLY"

    # v15: STRONG lev threshold bajado a 1.8 (antes 2.0) por TPs más conservadores
    if entry_mode == "STRONG" and rr >= 1.8:
        suggested_lev = _MAX_LEV
    elif entry_mode == "NORMAL":
        suggested_lev = max(1, int(_MAX_LEV * 0.6))
    else:
        suggested_lev = max(1, int(_MAX_LEV * _EARLY_LEV_FACTOR))

    is_valid = score >= MIN_SCORE and rr >= MIN_RR

    log.info(
        "[signal_engine] %s %s [%s] score=%d/%d RR=%.2f entry=%.6f sl=%.6f tp1=%.6f atr=%.6f lev=%dx mode=%s valid=%s | %s",
        symbol, signal_str, setup_type, score, max_score, rr,
        entry, sl, tp1, atr_val, suggested_lev, entry_mode, is_valid,
        " · ".join(reasons),
    )

    return SignalResult(
        symbol=symbol,
        signal=signal_str,
        entry_mode=entry_mode,
        score=score,
        max_score=max_score,
        entry=entry,
        sl=sl,
        tp1=tp1,
        tp2=tp2,
        atr=atr_val,
        rr=rr,
        suggested_lev=suggested_lev,
        indicators=indicators,
        is_valid=is_valid,
        reason="" if is_valid else f"[{setup_type}] score={score}/{max_score} rr={rr:.2f} (min {MIN_RR})",
        extra={"setup_type": setup_type},
    )


def _detect_setup(i15: dict, i1h: dict, i4h: dict, bars_15m: list) -> Tuple[Optional[str], str, int, int, List[str]]:
    for mode_fn in (_score_tendencia, _score_breakout, _score_reversal):
        setup_type, signal_str, score, max_score, reasons = mode_fn(i15, i1h, i4h, bars_15m)
        if signal_str != "NEUTRAL" and score >= MIN_SCORE:
            return setup_type, signal_str, score, max_score, reasons
    return None, "NEUTRAL", 0, 10, ["Ningún setup alcanzó MIN_SCORE"]


def _score_tendencia(i15: dict, i1h: dict, i4h: dict, bars_15m: list) -> Tuple[str, str, int, int, List[str]]:
    MAX = 10
    reasons: List[str] = []

    if not i1h:
        return "TENDENCIA", "NEUTRAL", 0, MAX, ["Sin datos 1h"]

    ema21_1h = i1h.get("ema21")
    ema50_1h = i1h.get("ema50")
    if not ema21_1h or not ema50_1h or ema50_1h == 0:
        return "TENDENCIA", "NEUTRAL", 0, MAX, ["EMA 1h no calculada"]

    ema_spread_1h = abs(ema21_1h - ema50_1h) / ema50_1h
    if ema_spread_1h < _EMA_SPREAD_RANGE_MAX:
        return "TENDENCIA", "NEUTRAL", 0, MAX, [f"Mercado en rango (spread EMA 1h={ema_spread_1h*100:.2f}%)"]

    trend_1h_up   = i1h.get("ema_bull", False)
    trend_1h_down = i1h.get("ema_bear", False)
    if not trend_1h_up and not trend_1h_down:
        return "TENDENCIA", "NEUTRAL", 0, MAX, ["Sin tendencia definida en 1h"]

    direction = "LONG" if trend_1h_up else "SHORT"
    score = 0

    ema_15m_ok = (direction == "LONG" and i15.get("ema_bull")) or (direction == "SHORT" and i15.get("ema_bear"))
    if ema_15m_ok:
        score += 2
        reasons.append(f"EMA15m+1h alineados {direction} (spread={ema_spread_1h*100:.2f}%) +2")
    else:
        score += 1
        reasons.append(f"EMA1h en {direction} pero 15m aun no (spread={ema_spread_1h*100:.2f}%) +1")

    st1h_ok = (direction == "LONG" and i1h.get("st_bull")) or (direction == "SHORT" and i1h.get("st_bear"))
    if st1h_ok:
        score += 1
        reasons.append("ST1h en favor +1")
    else:
        reasons.append("ST1h en contra")

    st4h_ok = False
    if i4h:
        st4h_ok = (direction == "LONG" and i4h.get("st_bull")) or (direction == "SHORT" and i4h.get("st_bear"))
        if st4h_ok:
            score += 1
            reasons.append("ST4h en favor +1")
        else:
            reasons.append("ST4h en contra")

    macd_ok = (direction == "LONG" and i15.get("macd_bull")) or (direction == "SHORT" and i15.get("macd_bear"))
    if macd_ok:
        score += 1
        reasons.append("MACD15m en favor +1")
    else:
        reasons.append("MACD15m en contra")

    ema21_15m = i15.get("ema21")
    close_15m = i15.get("close", 0)
    if ema21_15m and close_15m:
        recent_bars = bars_15m[-(_PULLBACK_LOOKBACK + 1):-1]
        touched_ema = False
        for bar in recent_bars:
            bar_low  = float(bar[3])
            bar_high = float(bar[2])
            if direction == "LONG":
                if bar_low <= ema21_15m * (1 + _PULLBACK_TOLERANCE):
                    touched_ema = True
                    break
            else:
                if bar_high >= ema21_15m * (1 - _PULLBACK_TOLERANCE):
                    touched_ema = True
                    break
        if touched_ema:
            score += 1
            reasons.append("Pullback a EMA21_15m detectado +1")
        else:
            reasons.append("Sin pullback a EMA21_15m")

    rsi_15m = i15.get("rsi_val")
    if rsi_15m is not None:
        rsi_ok = 35 <= rsi_15m <= 65
        if rsi_ok:
            score += 1
            reasons.append(f"RSI15m={rsi_15m:.0f} zona rebote +1")
        elif (direction == "LONG" and rsi_15m > 72) or (direction == "SHORT" and rsi_15m < 28):
            reasons.append(f"RSI15m={rsi_15m:.0f} SOBREEXTENDIDO — filtro duro")
            score = 0
        else:
            reasons.append(f"RSI15m={rsi_15m:.0f} zona neutra")

    vol_ratio = i15.get("vol_ratio", 1.0)
    if vol_ratio >= _VOL_CONFIRM_MIN:
        score += 1
        reasons.append(f"Vol15m={vol_ratio:.1f}x confirma +1")
    else:
        reasons.append(f"Vol15m={vol_ratio:.1f}x débil")

    st15m_ok = (direction == "LONG" and i15.get("st_bull")) or (direction == "SHORT" and i15.get("st_bear"))
    confluencia = st15m_ok and st1h_ok and (not i4h or st4h_ok)
    if confluencia:
        score += 2
        reasons.append("Confluencia total ST 15m+1h+4h +2")
    else:
        reasons.append("Sin confluencia total ST")

    if not st1h_ok:
        reasons.append("⚠️ ST1h en contra — filtro duro")
        score = max(0, score - 3)

    return "TENDENCIA", direction, score, MAX, reasons


def _score_breakout(i15: dict, i1h: dict, i4h: dict, bars_15m: list) -> Tuple[str, str, int, int, List[str]]:
    MAX = 8
    reasons: List[str] = []

    if len(bars_15m) < _BREAKOUT_WINDOW + 2:
        return "BREAKOUT", "NEUTRAL", 0, MAX, ["Velas insuficientes para breakout"]

    window = bars_15m[-(_BREAKOUT_WINDOW + 1):-1]
    range_high = max(b[2] for b in window)
    range_low  = min(b[3] for b in window)
    current_close = float(bars_15m[-1][4])
    vol_ratio = i15.get("vol_ratio", 1.0)
    atr_val = float(i15.get("atr", 0) or 0)
    breakout_pad = atr_val * _BREAKOUT_ATR_CONFIRM

    broke_up = current_close > (range_high + breakout_pad)
    broke_down = current_close < (range_low - breakout_pad)

    if not broke_up and not broke_down:
        return "BREAKOUT", "NEUTRAL", 0, MAX, [
            f"Sin rotura válida/fakeout: close={current_close:.4f} rango=[{range_low:.4f}-{range_high:.4f}] pad={breakout_pad:.4f}"
        ]

    direction = "LONG" if broke_up else "SHORT"
    score = 0
    score += 2
    reasons.append(f"Ruptura {'alcista' if broke_up else 'bajista'} confirmada fuera del rango +2")

    if vol_ratio >= _BREAKOUT_VOL_MIN:
        score += 2
        reasons.append(f"Vol={vol_ratio:.1f}x breakout confirmado +2")
    elif vol_ratio >= 1.1:
        score += 1
        reasons.append(f"Vol={vol_ratio:.1f}x moderado +1")
    else:
        reasons.append(f"Vol={vol_ratio:.1f}x BAJO — posible fakeout")

    if i1h:
        st1h_ok = (direction == "LONG" and i1h.get("st_bull")) or (direction == "SHORT" and i1h.get("st_bear"))
        if st1h_ok:
            score += 1
            reasons.append("ST1h confirma dirección +1")
        else:
            reasons.append("ST1h no confirma")

    if i4h:
        st4h_ok = (direction == "LONG" and i4h.get("st_bull")) or (direction == "SHORT" and i4h.get("st_bear"))
        if st4h_ok:
            score += 1
            reasons.append("ST4h confirma dirección +1")
        else:
            reasons.append("ST4h no confirma")

    rsi_15m = i15.get("rsi_val")
    if rsi_15m is not None:
        rsi_ok = (direction == "LONG" and 45 <= rsi_15m <= 70) or (direction == "SHORT" and 30 <= rsi_15m <= 55)
        if rsi_ok:
            score += 1
            reasons.append(f"RSI15m={rsi_15m:.0f} zona razonable +1")
        else:
            reasons.append(f"RSI15m={rsi_15m:.0f} sobreextendido")

    if i1h:
        macd_ok = (direction == "LONG" and i1h.get("macd_bull")) or (direction == "SHORT" and i1h.get("macd_bear"))
        if macd_ok:
            score += 1
            reasons.append("MACD1h en favor +1")
        else:
            reasons.append("MACD1h en contra")

    return "BREAKOUT", direction, score, MAX, reasons


def _score_reversal(i15: dict, i1h: dict, i4h: dict, bars_15m: list) -> Tuple[str, str, int, int, List[str]]:
    MAX = 9
    reasons: List[str] = []

    rsi_1h = i1h.get("rsi_val") if i1h else None
    if rsi_1h is None:
        return "REVERSAL", "NEUTRAL", 0, MAX, ["Sin datos 1h"]

    is_long = rsi_1h <= _REVERSAL_RSI_LOW
    is_short = rsi_1h >= _REVERSAL_RSI_HIGH
    if not is_long and not is_short:
        return "REVERSAL", "NEUTRAL", 0, MAX, [f"RSI1h={rsi_1h:.0f} no es extremo"]

    direction = "LONG" if is_long else "SHORT"
    score = 0
    score += 2
    reasons.append(f"RSI1h={rsi_1h:.0f} extremo {'sobreventa' if is_long else 'sobrecompra'} +2")

    hist_15m = i15.get("macd_hist")
    if hist_15m is not None:
        if is_long and hist_15m > 0:
            score += 2
            reasons.append(f"MACD15m hist={hist_15m:.4f} gira alcista +2")
        elif is_short and hist_15m < 0:
            score += 2
            reasons.append(f"MACD15m hist={hist_15m:.4f} gira bajista +2")
        else:
            reasons.append(f"MACD15m hist={hist_15m:.4f} aun no confirma")
    else:
        reasons.append("MACD15m no disponible")

    last_open = float(bars_15m[-1][1])
    last_close = float(bars_15m[-1][4])
    bullish_reversal_candle = last_close > last_open
    bearish_reversal_candle = last_close < last_open
    if (is_long and bullish_reversal_candle) or (is_short and bearish_reversal_candle):
        score += 1
        reasons.append("Vela de giro confirmada +1")
    else:
        reasons.append("Sin vela de giro confirmada")

    vol_ratio = i15.get("vol_ratio", 1.0)
    if vol_ratio >= 1.5:
        score += 1
        reasons.append(f"Vol15m={vol_ratio:.1f}x capitulación +1")
    else:
        reasons.append(f"Vol15m={vol_ratio:.1f}x sin capitulación")

    if i4h:
        st4h_against = (is_long and i4h.get("st_bear")) or (is_short and i4h.get("st_bull"))
        if st4h_against:
            score += 1
            reasons.append("ST4h en contra — agotamiento tendencia +1")
        else:
            reasons.append("ST4h no confirma agotamiento")

    rsi_4h = i4h.get("rsi_val") if i4h else None
    if rsi_4h is not None and 40 <= rsi_4h <= 60:
        score += 1
        reasons.append(f"RSI4h={rsi_4h:.0f} neutro — reversión posible +1")
    else:
        reasons.append("RSI4h no neutro")

    close_15m = i15.get("close", 0)
    ema21_1h = i1h.get("ema21") if i1h else None
    if ema21_1h and close_15m:
        dist_pct = abs(close_15m - ema21_1h) / ema21_1h
        if dist_pct <= 0.005:
            score += 1
            reasons.append(f"Precio toca EMA21_1h (dist={dist_pct*100:.2f}%) +1")
        else:
            reasons.append(f"Precio lejos de EMA21_1h ({dist_pct*100:.2f}%)")

    return "REVERSAL", direction, score, MAX, reasons


def _compute_indicators(bars: list) -> dict:
    if not bars or len(bars) < 10:
        return {}

    closes = [b[4] for b in bars]
    highs  = [b[2] for b in bars]
    lows   = [b[3] for b in bars]
    vols   = [b[5] for b in bars]

    ema21 = ema(closes, 21)
    ema50 = ema(closes, 50)
    rsi14 = rsi(closes, 14)
    m_line, s_line, hist = macd(closes, 12, 26, 9)
    st_dir, st_val = supertrend(highs, lows, closes, 10, 3.0)
    atr14  = calc_atr(highs, lows, closes, 14)

    vol_window = min(_VOL_AVG_WINDOW, len(vols))
    avg_vol = sum(vols[-vol_window:]) / vol_window if vol_window > 0 else 1.0
    vol_ratio = round(vols[-1] / avg_vol, 3) if avg_vol > 0 else 1.0
    price_dir = "rising" if len(closes) >= 2 and closes[-1] > closes[-2] else "falling"

    return {
        "ema21": ema21[-1] if ema21 else None,
        "ema50": ema50[-1] if ema50 else None,
        "ema_bull": bool(ema21 and ema50 and ema21[-1] > ema50[-1]),
        "ema_bear": bool(ema21 and ema50 and ema21[-1] < ema50[-1]),
        "rsi_val": rsi14,
        "macd_hist": hist,
        "macd_bull": hist > 0,
        "macd_bear": hist < 0,
        "st_dir": st_dir,
        "st_bull": st_dir == 1,
        "st_bear": st_dir == -1,
        "atr": atr14,
        "vol_ratio": vol_ratio,
        "price_dir": price_dir,
        "close": closes[-1],
    }


async def _fetch_bars(exch, symbol: str, timeframe: str, limit: int) -> list:
    ccxt_sym = _to_ccxt_symbol(symbol)
    try:
        bars = await exch.fetch_ohlcv(ccxt_sym, timeframe=timeframe, limit=limit)
        return bars or []
    except Exception as e:
        log.warning("[signal_engine] fetch_ohlcv(%s, %s) error: %s", ccxt_sym, timeframe, e)
        return []


def _hold_result(symbol: str, reason: str) -> SignalResult:
    return SignalResult(
        symbol=symbol,
        signal="NEUTRAL",
        entry_mode="HOLD",
        score=0,
        max_score=10,
        entry=0.0,
        sl=0.0,
        tp1=0.0,
        tp2=0.0,
        atr=0.0,
        rr=0.0,
        suggested_lev=1,
        indicators={},
        is_valid=False,
        reason=reason,
    )


def format_signal_block(signal) -> str:
    if signal is None:
        return ""
    arrow = "\U0001f7e2 LONG" if signal.signal == "LONG" else "\U0001f534 SHORT" if signal.signal == "SHORT" else "⚪ NEUTRAL"
    lev = f"{signal.suggested_lev}x" if signal.suggested_lev else "—"
    rr = f"{signal.rr:.2f}" if signal.rr else "—"
    mode = signal.extra.get("setup_type", signal.entry_mode)
    lines = [
        f"**{signal.symbol}** · {arrow} [{mode}]",
        f"Score: `{signal.score}/{signal.max_score}` · Mode: `{signal.entry_mode}` · Lev: `{lev}` · R/R: `{rr}`",
    ]
    if signal.entry:
        lines.append(f"Entry: `{signal.entry}` | SL: `{signal.sl}` | TP: `{signal.tp1}`")
    if signal.reason:
        lines.append(f"_{signal.reason}_")
    return "\n".join(lines)


_FLIP_COOLDOWN_S = float(os.getenv("SIGNAL_FLIP_COOLDOWN_S", "120"))


class SignalFlipGuard:
    def __init__(self, cooldown_s: float = _FLIP_COOLDOWN_S):
        self._cooldown = cooldown_s
        self._last: Dict[str, Tuple[str, float]] = {}

    def allow(self, symbol: str, signal) -> bool:
        if self._cooldown <= 0:
            return True
        if signal is None:
            return True
        side = getattr(signal, "side", None) or getattr(signal, "signal", None)
        if not side:
            if isinstance(signal, str) and signal.upper() in ("LONG", "SHORT", "BUY", "SELL"):
                side = signal
            else:
                return True
        side_norm = "long" if str(side).upper() in ("LONG", "BUY") else "short"
        last = self._last.get(symbol)
        if last is not None:
            last_side, last_ts = last
            elapsed = time.monotonic() - last_ts
            if last_side != side_norm and elapsed < self._cooldown:
                log.warning("[SignalFlipGuard] %s: señal %s BLOQUEADA (flip en %.1fs)", symbol, side_norm, elapsed)
                return False
        self._last[symbol] = (side_norm, time.monotonic())
        return True

    def reset(self, symbol: str) -> None:
        self._last.pop(symbol, None)

    def update(self, symbol: str, side: str) -> None:
        side_norm = "long" if str(side).upper() in ("LONG", "BUY") else "short"
        self._last[symbol] = (side_norm, time.monotonic())


signal_flip_guard = SignalFlipGuard()

_MANUAL_CLOSE_COOLDOWN_S = int(os.getenv("MANUAL_CLOSE_COOLDOWN_S", "600"))


class ManualCloseCooldown:
    def __init__(self, cooldown_s: int = _MANUAL_CLOSE_COOLDOWN_S):
        self._cooldown = cooldown_s
        self._closed: Dict[str, float] = {}

    def register(self, symbol: str) -> None:
        self._closed[symbol] = time.monotonic()
        log.info("[ManualCloseCooldown] %s: cooldown manual activado (%ds)", symbol, self._cooldown)

    def is_blocked(self, symbol: str) -> bool:
        ts = self._closed.get(symbol)
        if ts is None:
            return False
        elapsed = time.monotonic() - ts
        if elapsed < self._cooldown:
            log.debug("[ManualCloseCooldown] %s: bloqueado — %ds restantes", symbol, int(self._cooldown - elapsed))
            return True
        del self._closed[symbol]
        return False

    def clear(self, symbol: str) -> None:
        self._closed.pop(symbol, None)


manual_close_cooldown = ManualCloseCooldown()
