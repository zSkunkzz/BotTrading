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

  MODO TENDENCIA (EMA spread >= 0.3% en 1h):
    +2  EMA21 > EMA50 en 15m Y 1h (tendencia clara en ambos TF)
    +1  SuperTrend 1h en favor
    +1  SuperTrend 4h en favor
    +1  MACD 15m histograma en favor Y cambiando (no estancado)
    +1  Pullback: precio en 15m tocó EMA21 en las últimas 3 velas (rebote)
    +1  RSI 15m en zona de rebote (40-58 LONG, 42-60 SHORT) — NO sobreextendido
    +1  Vol 15m >= 1.2x en vela actual
    +1  Confluencia total: ST 15m + 1h + 4h todos en misma dirección
    Bonus max: 9 pts

  MODO BREAKOUT (precio rompe rango 20 velas en 15m):
    +2  Rotura de máximo (LONG) o mínimo (SHORT) de las últimas 20 velas en 15m
    +2  Vol 15m >= 1.8x en vela de ruptura (sin volumen no es breakout)
    +1  ST 1h en favor
    +1  ST 4h en favor
    +1  RSI 15m entre 45-70 (LONG) o 30-55 (SHORT) — no sobreextendido
    +1  MACD 1h en favor
    Bonus max: 8 pts

  MODO REVERSAL (RSI 1h extremo: <= 28 o >= 72):
    +2  RSI 1h <= 28 (LONG) o >= 72 (SHORT) — extremo real de mercado
    +2  Divergencia MACD 15m: hist estaba en contra pero ahora cambia
    +1  Vol 15m >= 1.5x (capitulación o climax)
    +1  ST 4h en contra del precio actual (agotamiento tendencia dominante)
    +1  RSI 4h en zona neutra (45-55) — no sobre-extendido en 4h
    +1  Precio toca EMA21 del 1h o la cruza (punto de inflexion)
    Bonus max: 8 pts

  FILTROS DUROS (bloquean independientemente del score):
    - EMA spread 1h < 0.15%: mercado en rango, no entrar (solo en modo tendencia)
    - RSI 15m > 72 para LONG o < 28 para SHORT: sobreextendido, no entrar
    - Los 3 ST (15m/1h/4h) NO coinciden en modo tendencia: no entrar
    - Vol 15m < 0.6x: mercado dormido, no entrar

  MIN_SCORE  = 6 / 10  (60% de confluencia)
  MIN_RR     = 2.5     ← sólo trades con RR mínimo real

SL/TP (multiplicadores ATR para garantizar RR >= 2.5):
  SL ATR mult = 1.5 (base)
  Para RR=2.5 necesitamos TP >= SL_dist * 2.5, es decir TP_mult >= SL_mult * 2.5 = 3.75

  Modo tendencia : SL=1.5×ATR | TP=3.8×ATR  → RR ~2.53
  Modo breakout  : SL=1.5×ATR | TP=3.8×ATR  → RR ~2.53
  Modo reversal  : SL=1.2×ATR | TP=3.1×ATR  → RR ~2.58

  TP2 mantenido como referencia interna (no se usa en órdenes).
  Trailing TP eliminado.
"""
from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, Tuple

from bot.indicators import ema, rsi, macd, supertrend, atr as calc_atr

log = logging.getLogger(__name__)

# ─── Constantes ──────────────────────────────────────────────────────────────────────────────
MIN_SCORE: int  = int(os.getenv("MIN_SIGNAL_SCORE", "6"))
MIN_RR: float   = float(os.getenv("MIN_RR_REQUIRED", "2.5"))

_BARS_NEEDED = int(os.getenv("BARS_NEEDED", "100"))

_SL_ATR_MULT       = float(os.getenv("SL_ATR_MULT",  "1.5"))
# TP calibrado para garantizar RR >= 2.5 con SL de 1.5x ATR:
#   TP_mult >= SL_mult * MIN_RR  → 1.5 * 2.5 = 3.75 → usamos 3.8 con margen
_TP1_ATR_MULT      = float(os.getenv("TP1_ATR_MULT", "3.8"))
_TP2_ATR_MULT      = float(os.getenv("TP2_ATR_MULT", "5.5"))
_MAX_LEV           = int(os.getenv("LEVERAGE", "15"))
_SL_CANDLE_BUFFER  = float(os.getenv("SL_CANDLE_BUFFER", "0.2"))

# Umbrales de setup
_EMA_SPREAD_TREND_MIN  = float(os.getenv("EMA_SPREAD_TREND_MIN",  "0.003"))  # 0.30%
_EMA_SPREAD_RANGE_MAX  = float(os.getenv("EMA_SPREAD_RANGE_MAX",  "0.0015")) # 0.15% → rango
_BREAKOUT_WINDOW       = int(os.getenv("BREAKOUT_WINDOW", "20"))
_BREAKOUT_VOL_MIN      = float(os.getenv("BREAKOUT_VOL_MIN",  "1.8"))
_REVERSAL_RSI_LOW      = float(os.getenv("REVERSAL_RSI_LOW",  "28"))
_REVERSAL_RSI_HIGH     = float(os.getenv("REVERSAL_RSI_HIGH", "72"))
_VOL_MIN_GLOBAL        = float(os.getenv("VOL_MIN_GLOBAL",    "0.6"))
_VOL_CONFIRM_MIN       = float(os.getenv("VOL_CONFIRM_MIN",   "1.2"))
_PULLBACK_LOOKBACK     = int(os.getenv("PULLBACK_LOOKBACK", "3"))
_PULLBACK_TOLERANCE    = float(os.getenv("PULLBACK_TOLERANCE", "0.003"))


# ─── _to_ccxt_symbol ──────────────────────────────────────────────────────────────────────────────
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


# ─── SignalResult ──────────────────────────────────────────────────────────────────────────────

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


# ─── analyze_pair ──────────────────────────────────────────────────────────────────────────────

async def analyze_pair(
    exch,
    symbol: str,
    ohlcv_fn: Optional[Callable] = None,
) -> SignalResult:
    try:
        if ohlcv_fn is not None:
            bars_15m = await ohlcv_fn("15m") or []
            bars_1h  = await ohlcv_fn("1h")  or []
            bars_4h  = await ohlcv_fn("4h")  or []
        else:
            bars_15m = await _fetch_bars(exch, symbol, "15m", _BARS_NEEDED)
            bars_1h  = await _fetch_bars(exch, symbol, "1h",  _BARS_NEEDED)
            bars_4h  = await _fetch_bars(exch, symbol, "4h",  max(50, _BARS_NEEDED // 2))
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

    if setup_type == "REVERSAL":
        sl_mult  = float(os.getenv("SL_ATR_MULT_REVERSAL",  "1.2"))
        # Reversal con SL=1.2×ATR: TP_mult >= 1.2 * 2.5 = 3.0 → usamos 3.1 con margen
        tp1_mult = float(os.getenv("TP1_ATR_MULT_REVERSAL", "3.1"))
        tp2_mult = float(os.getenv("TP2_ATR_MULT_REVERSAL", "5.0"))
    elif setup_type == "BREAKOUT":
        sl_mult  = _SL_ATR_MULT
        # Breakout con SL=1.5×ATR: TP_mult >= 1.5 * 2.5 = 3.75 → usamos 3.8
        tp1_mult = float(os.getenv("TP1_ATR_MULT_BREAKOUT", "3.8"))
        tp2_mult = float(os.getenv("TP2_ATR_MULT_BREAKOUT", "5.5"))
    else:  # TENDENCIA
        sl_mult  = _SL_ATR_MULT
        # Tendencia con SL=1.5×ATR: TP_mult >= 3.75 → usamos 3.8
        tp1_mult = _TP1_ATR_MULT
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

    # STRONG necesita RR >= 2.5 para apalancamiento máximo
    if entry_mode == "STRONG" and rr >= 2.5:
        suggested_lev = _MAX_LEV
    elif entry_mode == "NORMAL":
        suggested_lev = max(1, int(_MAX_LEV * 0.6))
    else:
        suggested_lev = max(1, int(_MAX_LEV * 0.4))

    is_valid = score >= MIN_SCORE and rr >= MIN_RR

    log.info(
        "[signal_engine] %s %s [%s] score=%d/%d RR=%.2f entry=%.6f sl=%.6f tp1=%.6f "
        "atr=%.6f lev=%dx mode=%s valid=%s | %s",
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
        reason="" if is_valid else f"[{setup_type}] score={score}/{max_score} rr={rr:.2f}",
        extra={"setup_type": setup_type},
    )


# ─── _detect_setup ───────────────────────────────────────────────────────────────────────────────

def _detect_setup(
    i15: dict, i1h: dict, i4h: dict, bars_15m: list
) -> Tuple[Optional[str], str, int, int, List[str]]:
    for mode_fn in (_score_reversal, _score_breakout, _score_tendencia):
        setup_type, signal_str, score, max_score, reasons = mode_fn(i15, i1h, i4h, bars_15m)
        if signal_str != "NEUTRAL" and score >= MIN_SCORE:
            return setup_type, signal_str, score, max_score, reasons

    return None, "NEUTRAL", 0, 10, ["Ningún setup alcanzó MIN_SCORE"]


# ─── MODO REVERSAL ────────────────────────────────────────────────────────────────────────────

def _score_reversal(
    i15: dict, i1h: dict, i4h: dict, bars_15m: list
) -> Tuple[str, str, int, int, List[str]]:
    MAX = 8
    reasons: List[str] = []

    rsi_1h = i1h.get("rsi_val") if i1h else None
    if rsi_1h is None:
        return "REVERSAL", "NEUTRAL", 0, MAX, ["Sin datos 1h"]

    is_long  = rsi_1h <= _REVERSAL_RSI_LOW
    is_short = rsi_1h >= _REVERSAL_RSI_HIGH

    if not is_long and not is_short:
        return "REVERSAL", "NEUTRAL", 0, MAX, [f"RSI1h={rsi_1h:.0f} no es extremo"]

    direction = "LONG" if is_long else "SHORT"
    score = 0

    score += 2
    reasons.append(f"RSI1h={rsi_1h:.0f} extremo {'sobreventa' if is_long else 'sobrecompra'} +2")

    hist_15m = i15.get("macd_hist")
    if hist_15m is not None:
        closes = [b[4] for b in bars_15m]
        try:
            from bot.indicators import macd as _macd
            _, _, hists = _macd(closes, 12, 26, 9)
            if is_long and hist_15m > 0:
                score += 2
                reasons.append(f"MACD15m hist={hist_15m:.4f} gira alcista +2")
            elif is_short and hist_15m < 0:
                score += 2
                reasons.append(f"MACD15m hist={hist_15m:.4f} gira bajista +2")
            else:
                reasons.append(f"MACD15m hist={hist_15m:.4f} aun no confirma")
        except Exception:
            reasons.append("MACD calc error")

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
        reasons.append(f"RSI4h no neutro")

    close_15m = i15.get("close", 0)
    ema21_1h  = i1h.get("ema21") if i1h else None
    if ema21_1h and close_15m:
        dist_pct = abs(close_15m - ema21_1h) / ema21_1h
        if dist_pct <= 0.005:
            score += 1
            reasons.append(f"Precio toca EMA21_1h (dist={dist_pct*100:.2f}%) +1")
        else:
            reasons.append(f"Precio lejos de EMA21_1h ({dist_pct*100:.2f}%)")

    return "REVERSAL", direction, score, MAX, reasons


# ─── MODO BREAKOUT ────────────────────────────────────────────────────────────────────────────

def _score_breakout(
    i15: dict, i1h: dict, i4h: dict, bars_15m: list
) -> Tuple[str, str, int, int, List[str]]:
    MAX = 8
    reasons: List[str] = []

    if len(bars_15m) < _BREAKOUT_WINDOW + 2:
        return "BREAKOUT", "NEUTRAL", 0, MAX, ["Velas insuficientes para breakout"]

    window = bars_15m[-(_BREAKOUT_WINDOW + 1):-1]
    range_high = max(b[2] for b in window)
    range_low  = min(b[3] for b in window)

    current_close = float(bars_15m[-1][4])
    vol_ratio     = i15.get("vol_ratio", 1.0)

    broke_up   = current_close > range_high
    broke_down = current_close < range_low

    if not broke_up and not broke_down:
        return "BREAKOUT", "NEUTRAL", 0, MAX, [
            f"Sin rotura: close={current_close:.4f} rango=[{range_low:.4f}-{range_high:.4f}]"
        ]

    direction = "LONG" if broke_up else "SHORT"
    score = 0

    score += 2
    reasons.append(
        f"Ruptura {'alcista' if broke_up else 'bajista'}: close={current_close:.4f} "
        f"{'>' if broke_up else '<'} {'rango ' + str(round(range_high, 4)) if broke_up else 'rango ' + str(round(range_low, 4))} +2"
    )

    if vol_ratio >= _BREAKOUT_VOL_MIN:
        score += 2
        reasons.append(f"Vol={vol_ratio:.1f}x breakout confirmado +2")
    elif vol_ratio >= 1.2:
        score += 1
        reasons.append(f"Vol={vol_ratio:.1f}x moderado +1")
    else:
        reasons.append(f"Vol={vol_ratio:.1f}x BAJO — posible fakeout")

    if i1h:
        st1h_ok = (direction == "LONG" and i1h.get("st_bull")) or \
                  (direction == "SHORT" and i1h.get("st_bear"))
        if st1h_ok:
            score += 1
            reasons.append("ST1h confirma dirección +1")
        else:
            reasons.append("ST1h no confirma")

    if i4h:
        st4h_ok = (direction == "LONG" and i4h.get("st_bull")) or \
                  (direction == "SHORT" and i4h.get("st_bear"))
        if st4h_ok:
            score += 1
            reasons.append("ST4h confirma dirección +1")
        else:
            reasons.append("ST4h no confirma")

    rsi_15m = i15.get("rsi_val")
    if rsi_15m is not None:
        rsi_ok = (direction == "LONG" and 45 <= rsi_15m <= 70) or \
                 (direction == "SHORT" and 30 <= rsi_15m <= 55)
        if rsi_ok:
            score += 1
            reasons.append(f"RSI15m={rsi_15m:.0f} zona razonable +1")
        else:
            reasons.append(f"RSI15m={rsi_15m:.0f} sobreextendido")

    if i1h:
        macd_ok = (direction == "LONG" and i1h.get("macd_bull")) or \
                  (direction == "SHORT" and i1h.get("macd_bear"))
        if macd_ok:
            score += 1
            reasons.append("MACD1h en favor +1")
        else:
            reasons.append("MACD1h en contra")

    return "BREAKOUT", direction, score, MAX, reasons


# ─── MODO TENDENCIA ────────────────────────────────────────────────────────────────────────────

def _score_tendencia(
    i15: dict, i1h: dict, i4h: dict, bars_15m: list
) -> Tuple[str, str, int, int, List[str]]:
    MAX = 9
    reasons: List[str] = []

    if not i1h:
        return "TENDENCIA", "NEUTRAL", 0, MAX, ["Sin datos 1h"]

    ema21_1h = i1h.get("ema21")
    ema50_1h = i1h.get("ema50")
    if not ema21_1h or not ema50_1h or ema50_1h == 0:
        return "TENDENCIA", "NEUTRAL", 0, MAX, ["EMA 1h no calculada"]

    ema_spread_1h = abs(ema21_1h - ema50_1h) / ema50_1h
    if ema_spread_1h < _EMA_SPREAD_RANGE_MAX:
        return "TENDENCIA", "NEUTRAL", 0, MAX, [
            f"Mercado en rango (spread EMA 1h={ema_spread_1h*100:.2f}%)"
        ]

    trend_1h_up   = i1h.get("ema_bull", False)
    trend_1h_down = i1h.get("ema_bear", False)

    if not trend_1h_up and not trend_1h_down:
        return "TENDENCIA", "NEUTRAL", 0, MAX, ["Sin tendencia definida en 1h"]

    direction = "LONG" if trend_1h_up else "SHORT"
    score = 0

    ema_15m_ok = (direction == "LONG" and i15.get("ema_bull")) or \
                 (direction == "SHORT" and i15.get("ema_bear"))
    if ema_15m_ok:
        score += 2
        reasons.append(f"EMA15m+1h alineados {direction} (spread={ema_spread_1h*100:.2f}%) +2")
    else:
        score += 1
        reasons.append(f"EMA1h en {direction} pero 15m aun no (spread={ema_spread_1h*100:.2f}%) +1")

    st1h_ok = (direction == "LONG" and i1h.get("st_bull")) or \
              (direction == "SHORT" and i1h.get("st_bear"))
    if st1h_ok:
        score += 1
        reasons.append("ST1h en favor +1")
    else:
        reasons.append("ST1h en contra")

    if i4h:
        st4h_ok = (direction == "LONG" and i4h.get("st_bull")) or \
                  (direction == "SHORT" and i4h.get("st_bear"))
        if st4h_ok:
            score += 1
            reasons.append("ST4h en favor +1")
        else:
            reasons.append("ST4h en contra")

    macd_ok = (direction == "LONG" and i15.get("macd_bull")) or \
              (direction == "SHORT" and i15.get("macd_bear"))
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
        price_dir = i15.get("price_dir", "")
        returning = (direction == "LONG" and price_dir == "rising") or \
                    (direction == "SHORT" and price_dir == "falling")
        if touched_ema and returning:
            score += 1
            reasons.append(f"Pullback a EMA21_15m confirmado +1")
        elif touched_ema:
            reasons.append("Tocó EMA21 pero aun no rebota")
        else:
            reasons.append(f"Sin pullback a EMA21_15m")

    rsi_15m = i15.get("rsi_val")
    if rsi_15m is not None:
        rsi_ok_long  = direction == "LONG"  and 40 <= rsi_15m <= 60
        rsi_ok_short = direction == "SHORT" and 40 <= rsi_15m <= 60
        if rsi_ok_long or rsi_ok_short:
            score += 1
            reasons.append(f"RSI15m={rsi_15m:.0f} zona rebote +1")
        elif (direction == "LONG" and rsi_15m > 72) or \
             (direction == "SHORT" and rsi_15m < 28):
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

    st15m_ok = (direction == "LONG" and i15.get("st_bull")) or \
               (direction == "SHORT" and i15.get("st_bear"))
    confluencia = st15m_ok and st1h_ok and (not i4h or st4h_ok)
    if confluencia:
        score += 1
        reasons.append("Confluencia total ST 15m+1h+4h +1")
    else:
        reasons.append("Sin confluencia total ST")

    if not st1h_ok:
        reasons.append("⚠️ ST1h en contra — filtro duro")
        score = max(0, score - 3)

    return "TENDENCIA", direction, score, MAX, reasons


# ─── _compute_indicators ───────────────────────────────────────────────────────────────────────────

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

    avg_vol   = sum(vols[-20:]) / 20 if len(vols) >= 20 else (sum(vols) / len(vols) if vols else 1)
    vol_ratio = round(vols[-1] / avg_vol, 3) if avg_vol > 0 else 1.0
    price_dir = "rising" if len(closes) >= 2 and closes[-1] > closes[-2] else "falling"

    return {
        "ema21":     ema21[-1] if ema21 else None,
        "ema50":     ema50[-1] if ema50 else None,
        "ema_bull":  bool(ema21 and ema50 and ema21[-1] > ema50[-1]),
        "ema_bear":  bool(ema21 and ema50 and ema21[-1] < ema50[-1]),
        "rsi_val":   rsi14,
        "macd_hist": hist,
        "macd_bull": hist > 0,
        "macd_bear": hist < 0,
        "st_dir":    st_dir,
        "st_bull":   st_dir == 1,
        "st_bear":   st_dir == -1,
        "atr":       atr14,
        "vol_ratio": vol_ratio,
        "price_dir": price_dir,
        "close":     closes[-1],
    }


# ─── _fetch_bars ────────────────────────────────────────────────────────────────────────────────
async def _fetch_bars(exch, symbol: str, timeframe: str, limit: int) -> list:
    ccxt_sym = _to_ccxt_symbol(symbol)
    try:
        bars = await exch.fetch_ohlcv(ccxt_sym, timeframe=timeframe, limit=limit)
        return bars or []
    except Exception as e:
        log.warning("[signal_engine] fetch_ohlcv(%s, %s) error: %s", ccxt_sym, timeframe, e)
        return []


# ─── _hold_result ────────────────────────────────────────────────────────────────────────────────
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


# ─── format_signal_block ────────────────────────────────────────────────────────────────────────────
def format_signal_block(signal: Optional[SignalResult]) -> str:
    if signal is None:
        return ""

    arrow = "\U0001f7e2 LONG" if signal.signal == "LONG" else \
            "\U0001f534 SHORT" if signal.signal == "SHORT" else "⚪ NEUTRAL"
    lev  = f"{signal.suggested_lev}x" if signal.suggested_lev else "—"
    rr   = f"{signal.rr:.2f}" if signal.rr else "—"
    mode = signal.extra.get("setup_type", signal.entry_mode)

    lines = [
        f"**{signal.symbol}** · {arrow} [{mode}]",
        f"Score: `{signal.score}/{signal.max_score}` · Mode: `{signal.entry_mode}` · Lev: `{lev}` · R/R: `{rr}`",
    ]
    if signal.entry:
        lines.append(
            f"Entry: `{signal.entry}` | SL: `{signal.sl}` | TP: `{signal.tp1}`"
        )
    if signal.reason:
        lines.append(f"_{signal.reason}_")

    return "\n".join(lines)


# ─── SignalFlipGuard ─────────────────────────────────────────────────────────────────────────────

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
                log.warning(
                    "[SignalFlipGuard] %s: señal %s BLOQUEADA (flip en %.1fs)",
                    symbol, side_norm, elapsed,
                )
                return False
        self._last[symbol] = (side_norm, time.monotonic())
        return True

    def reset(self, symbol: str) -> None:
        self._last.pop(symbol, None)

    def update(self, symbol: str, side: str) -> None:
        side_norm = "long" if str(side).upper() in ("LONG", "BUY") else "short"
        self._last[symbol] = (side_norm, time.monotonic())


signal_flip_guard = SignalFlipGuard()


# ─── ManualCloseCooldown ──────────────────────────────────────────────────────────────────────────────

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
