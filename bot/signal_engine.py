# -*- coding: utf-8 -*-
"""
signal_engine.py — Motor de señales técnicas puras.

Exporta:
  - SignalResult          : dataclass con el resultado de analyze_pair()
  - analyze_pair()        : analiza un par con OHLCV real → SignalResult
  - format_signal_block() : formatea SignalResult como bloque Markdown
  - MIN_SCORE             : puntuación mínima para señal válida (env: MIN_SIGNAL_SCORE)
  - MIN_RR                : ratio R/R mínimo (env: MIN_RR_REQUIRED)
  - SignalFlipGuard       : previene flip-flop de señales opuestas
  - signal_flip_guard     : singleton exportado de SignalFlipGuard

ARQUITECTURA:
  signal_engine  →  lógica técnica pura (OHLCV + indicadores, SIN imports de strategy)
  strategy       →  orquesta: llama analyze_pair + enriched_filter + IA
  trader         →  ejecuta órdenes usando strategy.decide()

IMPORTANTE: Este módulo NO importa nada de bot.strategy.
  Hacerlo causa un ciclo de importación circular que devuelve score=0 en cada ciclo.

OHLCV:
  analyze_pair() acepta un parámetro opcional `ohlcv_fn` (callable async).
  Si se pasa, se usa en lugar de exch.fetch_ohlcv() para aprovechar la ruta
  WS→caché→REST del trader (trader.get_ohlcv). Si no se pasa, se usa ccxt directamente.
  Esto evita 3 llamadas REST extra por ciclo cuando el trader ya tiene los datos.
"""
from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, Tuple

from bot.indicators import ema, rsi, macd, supertrend, atr as calc_atr

log = logging.getLogger(__name__)

# ─── Constantes exportadas ────────────────────────────────────────────────────

MIN_SCORE: int   = int(os.getenv("MIN_SIGNAL_SCORE", "6"))
MIN_RR: float    = float(os.getenv("MIN_RR_REQUIRED", "1.8"))

_TIMEFRAMES  = ["15m", "1h", "4h"]
_BARS_NEEDED = int(os.getenv("BARS_NEEDED", "100"))

# Parámetros SL/TP
_SL_ATR_MULT  = float(os.getenv("SL_ATR_MULT",  "1.5"))
_TP1_ATR_MULT = float(os.getenv("TP1_ATR_MULT", "2.8"))
_TP2_ATR_MULT = float(os.getenv("TP2_ATR_MULT", "4.5"))
_MAX_LEV      = int(os.getenv("LEVERAGE", "15"))

# FIX 1: Zonas RSI asimétricas y no solapadas
# LONG: zona 45-65 (momentum alcista, sin sobrecompra)
# SHORT: zona 35-55 (momentum bajista, sin sobreventa)
# Solapamiento 45-55 → zona neutra, no puntúa nadie
_RSI_LONG_MIN  = float(os.getenv("RSI_SCORE_LONG_MIN",  "45"))
_RSI_LONG_MAX  = float(os.getenv("RSI_SCORE_LONG_MAX",  "65"))
_RSI_SHORT_MIN = float(os.getenv("RSI_SCORE_SHORT_MIN", "35"))
_RSI_SHORT_MAX = float(os.getenv("RSI_SCORE_SHORT_MAX", "55"))

_VOL_DIRECTIONAL_MIN = float(os.getenv("VOL_DIRECTIONAL_MIN", "1.3"))
_VOL_WEAK_MAX        = float(os.getenv("VOL_WEAK_MAX",        "0.8"))

# FIX 3: Margen adicional al anclar SL a high/low de vela
_SL_CANDLE_BUFFER = float(os.getenv("SL_CANDLE_BUFFER", "0.2"))  # ATR multiplicador buffer


# ─── SignalResult ─────────────────────────────────────────────────────────────

@dataclass
class SignalResult:
    """Resultado completo de analyze_pair()."""
    symbol:       str
    signal:       str           # "LONG" | "SHORT" | "NEUTRAL"
    entry_mode:   str           # "STRONG" | "NORMAL" | "EARLY" | "HOLD"
    score:        int
    max_score:    int
    entry:        float
    sl:           float
    tp1:          float
    tp2:          float
    atr:          float
    rr:           float
    suggested_lev: int
    indicators:   Dict
    is_valid:     bool = True
    reason:       str  = ""
    signal_block: str  = ""
    extra:        Dict = field(default_factory=dict)


# ─── analyze_pair ─────────────────────────────────────────────────────────────

async def analyze_pair(
    exch,
    symbol: str,
    ohlcv_fn: Optional[Callable] = None,
) -> SignalResult:
    """
    Descarga OHLCV para 15m, 1h y 4h, calcula indicadores y devuelve SignalResult.
    """
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
    ind_1h  = _compute_indicators(bars_1h)  if len(bars_1h)  >= 30 else {}
    ind_4h  = _compute_indicators(bars_4h)  if len(bars_4h)  >= 20 else {}

    indicators = {"15m": ind_15m, "1h": ind_1h, "4h": ind_4h,
                  "_closes_15m": [b[4] for b in bars_15m[-5:]]}

    score, max_score, signal_str, reasons = _score_signal(ind_15m, ind_1h, ind_4h)

    if signal_str == "NEUTRAL":
        return _hold_result(symbol, f"NEUTRAL (score={score}/{max_score})")

    # ── Precio y ATR ──────────────────────────────────────────────────────────
    last_bar    = bars_15m[-1]
    close_price = float(last_bar[4])
    high_price  = float(last_bar[2])
    low_price   = float(last_bar[3])
    entry = round((high_price + low_price) / 2, 8)

    atr_val = float(ind_15m.get("atr", 0) or 0)
    if atr_val <= 0:
        return _hold_result(symbol, "ATR=0 — no se puede calcular SL/TP")

    # FIX 3: SL anclado al extremo de la vela + buffer ATR
    # LONG: sl = min(low - buffer, entry - 1.5*atr)  → siempre fuera del range de la vela
    # SHORT: sl = max(high + buffer, entry + 1.5*atr)
    # Así el SL nunca queda dentro del body de la vela actual aunque sea grande.
    _atr_sl   = _SL_ATR_MULT  * atr_val
    _atr_buf  = _SL_CANDLE_BUFFER * atr_val
    _atr_tp1  = _TP1_ATR_MULT * atr_val
    _atr_tp2  = _TP2_ATR_MULT * atr_val

    if signal_str == "LONG":
        sl  = round(min(low_price - _atr_buf, entry - _atr_sl), 6)
        tp1 = round(entry + _atr_tp1, 6)
        tp2 = round(entry + _atr_tp2, 6)
    else:  # SHORT
        sl  = round(max(high_price + _atr_buf, entry + _atr_sl), 6)
        tp1 = round(entry - _atr_tp1, 6)
        tp2 = round(entry - _atr_tp2, 6)

    risk   = abs(entry - sl)
    reward = abs(tp1 - entry)
    rr     = round(reward / risk, 2) if risk > 0 else 0.0

    # ── Entry mode ────────────────────────────────────────────────────────────
    if score >= max_score - 1:
        entry_mode = "STRONG"
    elif score >= MIN_SCORE + 2:
        entry_mode = "NORMAL"
    else:
        entry_mode = "EARLY"

    # FIX 2: Leverage dinámico basado en calidad de señal
    # STRONG + RR >= 2.5 → leverage máximo configurado
    # NORMAL          → 60% del máximo
    # EARLY           → 40% del máximo
    if entry_mode == "STRONG" and rr >= 2.5:
        suggested_lev = _MAX_LEV
    elif entry_mode == "NORMAL":
        suggested_lev = max(1, int(_MAX_LEV * 0.6))
    else:  # EARLY
        suggested_lev = max(1, int(_MAX_LEV * 0.4))
    # El trader lo capará al máximo del par en el exchange

    is_valid = score >= MIN_SCORE and rr >= MIN_RR

    log.info(
        "[signal_engine] %s %s score=%d/%d RR=%.2f entry=%.6f sl=%.6f tp1=%.6f "
        "atr=%.6f lev=%dx mode=%s valid=%s",
        symbol, signal_str, score, max_score, rr, entry, sl, tp1,
        atr_val, suggested_lev, entry_mode, is_valid,
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
        reason="" if is_valid else f"score={score}/{max_score} rr={rr:.2f}",
    )


# ─── OHLCV fetch (fallback sin caché) ─────────────────────────────────────────

async def _fetch_bars(exch, symbol: str, timeframe: str, limit: int) -> list:
    try:
        bars = await exch.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
        return bars or []
    except Exception as e:
        log.warning("[signal_engine] fetch_ohlcv(%s, %s) error: %s", symbol, timeframe, e)
        return []


# ─── Indicadores ──────────────────────────────────────────────────────────────

def _compute_indicators(bars: list) -> dict:
    """Calcula EMA21/50, RSI14, MACD, SuperTrend, ATR14 y vol_ratio."""
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
    avg_vol = sum(vols[-20:]) / 20 if len(vols) >= 20 else (sum(vols) / len(vols) if vols else 1)
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


# ─── Scoring ──────────────────────────────────────────────────────────────────

def _score_signal(
    i15: dict, i1h: dict, i4h: dict
) -> Tuple[int, int, str, List[str]]:
    """
    Sistema de puntuación multi-timeframe. max_score = 10.

    15m: EMA(1) + MACD(1) + SuperTrend(1) + RSI(1) + VOL(1) = 5 pts
    1h:  EMA(1) + MACD(1) + SuperTrend(1)                    = 3 pts
    4h:  EMA(1) + MACD(1)                                    = 2 pts

    FIX 1: RSI con zonas asimétricas y NO solapadas:
      LONG  puntúa si 45 <= RSI <= 65  (momentum alcista, sin sobrecompra)
      SHORT puntúa si 35 <= RSI <= 55  (momentum bajista, sin sobreventa)
      Zona 45-55 neutral: ambas condiciones podrían cumplirse, pero se evalúan
      de forma excluyente: si RSI >= 45 y <= 65 entra por LONG; SHORT solo
      si RSI < 45 (no hay solapamiento en la práctica para la misma vela).

    FIX 4: Score en empate devuelve long_pts (total) no 0, para que el caller
      pueda distinguir mercado-partido (5L/5S) de sin-datos (0/0).
    """
    max_score = 10
    long_pts  = 0
    short_pts = 0
    reasons   = []

    # ── 15m ───────────────────────────────────────────────────────────────────
    if i15.get("ema_bull"):  long_pts  += 1; reasons.append("EMA15m↑")
    if i15.get("ema_bear"):  short_pts += 1; reasons.append("EMA15m↓")
    if i15.get("macd_bull"): long_pts  += 1; reasons.append("MACD15m↑")
    if i15.get("macd_bear"): short_pts += 1; reasons.append("MACD15m↓")
    if i15.get("st_bull"):   long_pts  += 1; reasons.append("ST15m↑")
    if i15.get("st_bear"):   short_pts += 1; reasons.append("ST15m↓")

    # FIX 1: RSI zonas asimétricas no solapadas
    # LONG:  45-65  (momentum alcista sin sobrecompra)
    # SHORT: 35-55  (momentum bajista sin sobreventa)
    # Zona 45-55: primero se evalúa LONG; SHORT solo aplica si RSI < 45
    # (en la práctica nunca hay solapamiento real en la misma dirección)
    rsi15 = i15.get("rsi_val")
    if rsi15 is not None:
        if _RSI_LONG_MIN <= rsi15 <= _RSI_LONG_MAX:           # 45-65
            long_pts += 1
            reasons.append(f"RSI15m={rsi15:.0f} zona LONG (45-65)")
        elif _RSI_SHORT_MIN <= rsi15 < _RSI_LONG_MIN:         # 35-44
            short_pts += 1
            reasons.append(f"RSI15m={rsi15:.0f} zona SHORT (35-44)")
        elif _RSI_SHORT_MAX < rsi15 < _RSI_LONG_MIN:          # 55-44 → dead zone
            reasons.append(f"RSI15m={rsi15:.0f} zona neutra — no puntúa")
        elif rsi15 > 70:
            reasons.append(f"RSI15m={rsi15:.0f} sobrecompra — no puntúa")
        elif rsi15 < 30:
            reasons.append(f"RSI15m={rsi15:.0f} sobreventa — no puntúa")
        else:
            reasons.append(f"RSI15m={rsi15:.0f} zona neutra — no puntúa")

    # Volumen direccional
    vol15     = i15.get("vol_ratio", 1.0)
    price_dir = i15.get("price_dir", "unknown")
    if vol15 >= _VOL_DIRECTIONAL_MIN:
        if price_dir == "rising":
            long_pts  += 1
            reasons.append(f"Vol15m={vol15:.1f}x subiendo ↑")
        elif price_dir == "falling":
            short_pts += 1
            reasons.append(f"Vol15m={vol15:.1f}x bajando ↓")
        else:
            reasons.append(f"Vol15m={vol15:.1f}x sin dir clara")
    elif vol15 < _VOL_WEAK_MAX:
        reasons.append(f"Vol15m={vol15:.1f}x débil — no puntúa")
    else:
        reasons.append(f"Vol15m={vol15:.1f}x normal — no puntúa")

    # ── 1h ────────────────────────────────────────────────────────────────────
    if i1h:
        if i1h.get("ema_bull"):  long_pts  += 1; reasons.append("EMA1h↑")
        if i1h.get("ema_bear"):  short_pts += 1; reasons.append("EMA1h↓")
        if i1h.get("macd_bull"): long_pts  += 1; reasons.append("MACD1h↑")
        if i1h.get("macd_bear"): short_pts += 1; reasons.append("MACD1h↓")
        if i1h.get("st_bull"):   long_pts  += 1; reasons.append("ST1h↑")
        if i1h.get("st_bear"):   short_pts += 1; reasons.append("ST1h↓")

    # ── 4h ────────────────────────────────────────────────────────────────────
    if i4h:
        if i4h.get("ema_bull"):  long_pts  += 1; reasons.append("EMA4h↑")
        if i4h.get("ema_bear"):  short_pts += 1; reasons.append("EMA4h↓")
        if i4h.get("macd_bull"): long_pts  += 1; reasons.append("MACD4h↑")
        if i4h.get("macd_bear"): short_pts += 1; reasons.append("MACD4h↓")

    # ── Decisión ──────────────────────────────────────────────────────────────
    if long_pts > short_pts:
        tf1h_ok = (not i1h) or i1h.get("ema_bull") or i1h.get("st_bull")
        if tf1h_ok:
            return long_pts, max_score, "LONG", reasons
        return long_pts, max_score, "NEUTRAL", reasons + ["1h_no_confirma"]

    if short_pts > long_pts:
        tf1h_ok = (not i1h) or i1h.get("ema_bear") or i1h.get("st_bear")
        if tf1h_ok:
            return short_pts, max_score, "SHORT", reasons
        return short_pts, max_score, "NEUTRAL", reasons + ["1h_no_confirma"]

    # FIX 4: Empate — devuelve long_pts (total puntos acumulados) en lugar de 0
    # El caller ve score>0 con NEUTRAL → mercado partido (no sin datos)
    return long_pts, max_score, "NEUTRAL", reasons + ["empate"]


# ─── _hold_result ─────────────────────────────────────────────────────────────

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


# ─── format_signal_block ──────────────────────────────────────────────────────

def format_signal_block(signal: Optional[SignalResult]) -> str:
    if signal is None:
        return ""

    arrow = "🟢 LONG" if signal.signal == "LONG" else "🔴 SHORT" if signal.signal == "SHORT" else "⚪ NEUTRAL"
    lev   = f"{signal.suggested_lev}x" if signal.suggested_lev else "—"
    rr    = f"{signal.rr:.2f}" if signal.rr else "—"

    lines = [
        f"**{signal.symbol}** · {arrow}",
        f"Score: `{signal.score}/{signal.max_score}` · Mode: `{signal.entry_mode}` · Lev: `{lev}` · R/R: `{rr}`",
    ]
    if signal.entry:
        lines.append(
            f"Entry: `{signal.entry}` | SL: `{signal.sl}` | TP1: `{signal.tp1}` | TP2: `{signal.tp2}`"
        )
    if signal.reason:
        lines.append(f"_{signal.reason}_")

    return "\n".join(lines)


# ─── SignalFlipGuard ───────────────────────────────────────────────────────────

_FLIP_COOLDOWN_S = float(os.getenv("SIGNAL_FLIP_COOLDOWN_S", "120"))


class SignalFlipGuard:
    """
    Previene flip-flop de señales opuestas en ventana corta.
    """

    def __init__(self, cooldown_s: float = _FLIP_COOLDOWN_S):
        self._cooldown = cooldown_s
        self._last: Dict[str, Tuple[str, float]] = {}

    def allow(self, symbol: str, signal) -> bool:
        if self._cooldown <= 0:
            return True
        if signal is None:
            return True

        side = getattr(signal, "side", None)
        if not side:
            if isinstance(signal, str) and signal in ("long", "short", "buy", "sell"):
                side = signal
            else:
                return True

        side_norm = "long" if side in ("long", "buy") else "short"

        last = self._last.get(symbol)
        if last is not None:
            last_side, last_ts = last
            elapsed = time.monotonic() - last_ts
            if last_side != side_norm and elapsed < self._cooldown:
                log.warning(
                    "[SignalFlipGuard] %s: señal %s BLOQUEADA — inversión de %s a %s "
                    "en %.1fs (cooldown=%.0fs).",
                    symbol, side_norm, last_side, side_norm, elapsed, self._cooldown,
                )
                return False

        self._last[symbol] = (side_norm, time.monotonic())
        return True

    def reset(self, symbol: str) -> None:
        self._last.pop(symbol, None)

    def update(self, symbol: str, side: str) -> None:
        side_norm = "long" if side in ("long", "buy") else "short"
        self._last[symbol] = (side_norm, time.monotonic())


signal_flip_guard = SignalFlipGuard()
