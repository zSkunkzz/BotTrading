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

FIX symbol_format:
  ccxt-Hyperliquid requiere el formato completo "SOL/USDC:USDC" para fetch_ohlcv.
  Si se pasa solo el coin corto ("SOL", "BTC") el exchange lanza
  'hyperliquid does not have market symbol SOL'.
  _to_ccxt_symbol() normaliza cualquier formato al completo antes de llamar al exchange.

FIX ST4h:
  SuperTrend 4h añadido al scoring (+1 LONG / +1 SHORT).
  max_score sube de 10 a 12. Los umbrales de entry_mode se ajustan proporcionalmente.

FIX rsi_vol_1h:
  RSI y volumen del 1h añadidos al scoring (+1 LONG / +1 SHORT cada uno).
  max_score sube de 12 a 14. MIN_SCORE sube de 8 a 10 (mantiene ~71% efectivo).
  El 4h NO recibe RSI/vol: la vela 4h cambia cada 4h y es demasiado lenta
  para añadir información direccional util en ciclos de 15m.
  Umbrales recalibrados:
    STRONG : score >= max_score - 2 = 12
    NORMAL : score >= MIN_SCORE + 2 = 12  (igual que STRONG en este punto)
    EARLY  : score >= MIN_SCORE     = 10

FIX entry=close:
  entry usa close_price en lugar del mid de vela (high+low)/2.

FIX FlipGuard .signal:
  SignalFlipGuard.allow() ahora lee .signal de SignalResult además de .side.

FIX manual_close_cooldown:
  ManualCloseCooldown registra cierres manuales y bloquea reentradas.

FIX tf1h_empty (CRÍTICO):
  tf1h_ok ahora requiere que i1h NO esté vacío Y tenga ema_bull/st_bull.

FIX min_score_max_score (CRÍTICO):
  MIN_SCORE default 10 (antes 8) para max_score=14.
  Umbral efectivo: 10/14 = 71%.
"""
from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, Tuple

from bot.indicators import ema, rsi, macd, supertrend, atr as calc_atr

log = logging.getLogger(__name__)

# ─── Constantes exportadas ──────────────────────────────────────────────────────────────────────────

# FIX rsi_vol_1h: MIN_SCORE sube a 10 para compensar max_score=14
MIN_SCORE: int   = int(os.getenv("MIN_SIGNAL_SCORE", "10"))
MIN_RR: float    = float(os.getenv("MIN_RR_REQUIRED", "1.8"))

_TIMEFRAMES  = ["15m", "1h", "4h"]
_BARS_NEEDED = int(os.getenv("BARS_NEEDED", "100"))

# Parámetros SL/TP
_SL_ATR_MULT  = float(os.getenv("SL_ATR_MULT",  "1.5"))
_TP1_ATR_MULT = float(os.getenv("TP1_ATR_MULT", "2.8"))
_TP2_ATR_MULT = float(os.getenv("TP2_ATR_MULT", "4.5"))
_MAX_LEV      = int(os.getenv("LEVERAGE", "15"))

# Zonas RSI asimétricas y no solapadas (aplicadas en 15m y 1h)
_RSI_LONG_MIN  = float(os.getenv("RSI_SCORE_LONG_MIN",  "45"))
_RSI_LONG_MAX  = float(os.getenv("RSI_SCORE_LONG_MAX",  "65"))
_RSI_SHORT_MIN = float(os.getenv("RSI_SCORE_SHORT_MIN", "35"))
_RSI_SHORT_MAX = float(os.getenv("RSI_SCORE_SHORT_MAX", "55"))

_VOL_DIRECTIONAL_MIN = float(os.getenv("VOL_DIRECTIONAL_MIN", "1.3"))
_VOL_WEAK_MAX        = float(os.getenv("VOL_WEAK_MAX",        "0.8"))

_SL_CANDLE_BUFFER = float(os.getenv("SL_CANDLE_BUFFER", "0.2"))


# ─── _to_ccxt_symbol ─────────────────────────────────────────────────────────────────────────────────────

def _to_ccxt_symbol(symbol: str) -> str:
    """
    FIX symbol_format: normaliza cualquier variante al formato ccxt-Hyperliquid.

    Ejemplos:
      "SOL"            -> "SOL/USDC:USDC"
      "SOL/USDT"       -> "SOL/USDC:USDC"
      "SOL/USDC:USDC"  -> "SOL/USDC:USDC"  (ya correcto, no tocamos)
      "SOL/USD:USDC"   -> "SOL/USDC:USDC"
      "SOLUSDT"        -> "SOL/USDC:USDC"
    """
    if "/USDC:USDC" in symbol:
        return symbol

    coin = (
        symbol
        .replace(":USDT", "")
        .replace(":USDC", "")
        .replace("/USDT", "")
        .replace("/USDC", "")
        .replace("/USD",  "")
        .replace("USDT",  "")
        .upper()
        .strip()
    )
    return f"{coin}/USDC:USDC"


# ─── SignalResult ───────────────────────────────────────────────────────────────────────────────────────

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


# ─── analyze_pair ──────────────────────────────────────────────────────────────────────────────────────

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

    last_bar    = bars_15m[-1]
    close_price = float(last_bar[4])
    high_price  = float(last_bar[2])
    low_price   = float(last_bar[3])
    entry = close_price

    atr_val = float(ind_15m.get("atr", 0) or 0)
    if atr_val <= 0:
        return _hold_result(symbol, "ATR=0 — no se puede calcular SL/TP")

    _atr_sl   = _SL_ATR_MULT  * atr_val
    _atr_buf  = _SL_CANDLE_BUFFER * atr_val
    _atr_tp1  = _TP1_ATR_MULT * atr_val
    _atr_tp2  = _TP2_ATR_MULT * atr_val

    if signal_str == "LONG":
        sl  = round(min(low_price - _atr_buf, entry - _atr_sl), 6)
        tp1 = round(entry + _atr_tp1, 6)
        tp2 = round(entry + _atr_tp2, 6)
    else:
        sl  = round(max(high_price + _atr_buf, entry + _atr_sl), 6)
        tp1 = round(entry - _atr_tp1, 6)
        tp2 = round(entry - _atr_tp2, 6)

    risk   = abs(entry - sl)
    reward = abs(tp1 - entry)
    rr     = round(reward / risk, 2) if risk > 0 else 0.0

    # FIX rsi_vol_1h: max_score=14, STRONG >= 12, NORMAL >= 12, EARLY >= 10
    if score >= max_score - 2:      # >= 12
        entry_mode = "STRONG"
    elif score >= MIN_SCORE + 2:    # >= 12 (con MIN_SCORE=10)
        entry_mode = "NORMAL"
    else:
        entry_mode = "EARLY"

    if entry_mode == "STRONG" and rr >= 2.5:
        suggested_lev = _MAX_LEV
    elif entry_mode == "NORMAL":
        suggested_lev = max(1, int(_MAX_LEV * 0.6))
    else:
        suggested_lev = max(1, int(_MAX_LEV * 0.4))

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


# ─── OHLCV fetch (fallback sin caché) ──────────────────────────────────────────────────────────────────────────────

async def _fetch_bars(exch, symbol: str, timeframe: str, limit: int) -> list:
    """
    FIX symbol_format: ccxt-Hyperliquid necesita "SOL/USDC:USDC", no "SOL".
    _to_ccxt_symbol() normaliza antes de llamar a fetch_ohlcv().
    """
    ccxt_sym = _to_ccxt_symbol(symbol)
    try:
        bars = await exch.fetch_ohlcv(ccxt_sym, timeframe=timeframe, limit=limit)
        return bars or []
    except Exception as e:
        log.warning("[signal_engine] fetch_ohlcv(%s, %s) error: %s", ccxt_sym, timeframe, e)
        return []


# ─── Indicadores ──────────────────────────────────────────────────────────────────────────────────────

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


# ─── _score_rsi_vol: helper reutilizable para RSI+vol en cualquier TF ─────────────────────────

def _score_rsi_vol(
    ind: dict,
    tf_label: str,
    long_pts: int,
    short_pts: int,
    reasons: List[str],
) -> Tuple[int, int]:
    """
    Aplica scoring de RSI y volumen de un timeframe dado.
    Retorna (long_pts, short_pts) actualizados.
    Usado en 15m y 1h. No se aplica a 4h (vela demasiado lenta).
    """
    rsi_val   = ind.get("rsi_val")
    vol_ratio = ind.get("vol_ratio", 1.0)
    price_dir = ind.get("price_dir", "unknown")

    if rsi_val is not None:
        if _RSI_LONG_MIN <= rsi_val <= _RSI_LONG_MAX:
            long_pts += 1
            reasons.append(f"RSI{tf_label}={rsi_val:.0f} zona LONG (45-65)")
        elif _RSI_SHORT_MIN <= rsi_val < _RSI_LONG_MIN:
            short_pts += 1
            reasons.append(f"RSI{tf_label}={rsi_val:.0f} zona SHORT (35-44)")
        elif rsi_val > 70:
            reasons.append(f"RSI{tf_label}={rsi_val:.0f} sobrecompra — no puntua")
        elif rsi_val < 30:
            reasons.append(f"RSI{tf_label}={rsi_val:.0f} sobreventa — no puntua")
        else:
            reasons.append(f"RSI{tf_label}={rsi_val:.0f} zona neutra — no puntua")

    if vol_ratio >= _VOL_DIRECTIONAL_MIN:
        if price_dir == "rising":
            long_pts  += 1
            reasons.append(f"Vol{tf_label}={vol_ratio:.1f}x subiendo \u2191")
        elif price_dir == "falling":
            short_pts += 1
            reasons.append(f"Vol{tf_label}={vol_ratio:.1f}x bajando \u2193")
        else:
            reasons.append(f"Vol{tf_label}={vol_ratio:.1f}x sin dir clara")
    elif vol_ratio < _VOL_WEAK_MAX:
        reasons.append(f"Vol{tf_label}={vol_ratio:.1f}x debil — no puntua")
    else:
        reasons.append(f"Vol{tf_label}={vol_ratio:.1f}x normal — no puntua")

    return long_pts, short_pts


# ─── Scoring ──────────────────────────────────────────────────────────────────────────────────────────────

def _score_signal(
    i15: dict, i1h: dict, i4h: dict
) -> Tuple[int, int, str, List[str]]:
    """
    Puntuacion maxima por timeframe:
      15m: EMA(1) + MACD(1) + ST(1) + RSI(1) + Vol(1) = 5 pts
       1h: EMA(1) + MACD(1) + ST(1) + RSI(1) + Vol(1) = 5 pts  <- FIX rsi_vol_1h
       4h: EMA(1) + MACD(1) + ST(1)                   = 4 pts
    Total max_score = 14
    """
    # FIX rsi_vol_1h: max_score sube a 14
    max_score = 14
    long_pts  = 0
    short_pts = 0
    reasons   = []

    # —— 15m: EMA + MACD + ST ——
    if i15.get("ema_bull"):  long_pts  += 1; reasons.append("EMA15m\u2191")
    if i15.get("ema_bear"):  short_pts += 1; reasons.append("EMA15m\u2193")
    if i15.get("macd_bull"): long_pts  += 1; reasons.append("MACD15m\u2191")
    if i15.get("macd_bear"): short_pts += 1; reasons.append("MACD15m\u2193")
    if i15.get("st_bull"):   long_pts  += 1; reasons.append("ST15m\u2191")
    if i15.get("st_bear"):   short_pts += 1; reasons.append("ST15m\u2193")

    # —— 15m: RSI + Volumen ——
    long_pts, short_pts = _score_rsi_vol(i15, "15m", long_pts, short_pts, reasons)

    # —— 1h: EMA + MACD + ST ——
    if i1h:
        if i1h.get("ema_bull"):  long_pts  += 1; reasons.append("EMA1h\u2191")
        if i1h.get("ema_bear"):  short_pts += 1; reasons.append("EMA1h\u2193")
        if i1h.get("macd_bull"): long_pts  += 1; reasons.append("MACD1h\u2191")
        if i1h.get("macd_bear"): short_pts += 1; reasons.append("MACD1h\u2193")
        if i1h.get("st_bull"):   long_pts  += 1; reasons.append("ST1h\u2191")
        if i1h.get("st_bear"):   short_pts += 1; reasons.append("ST1h\u2193")

        # FIX rsi_vol_1h: RSI + Volumen en 1h
        long_pts, short_pts = _score_rsi_vol(i1h, "1h", long_pts, short_pts, reasons)

    # —— 4h: EMA + MACD + ST (sin RSI/vol: vela demasiado lenta) ——
    if i4h:
        if i4h.get("ema_bull"):  long_pts  += 1; reasons.append("EMA4h\u2191")
        if i4h.get("ema_bear"):  short_pts += 1; reasons.append("EMA4h\u2193")
        if i4h.get("macd_bull"): long_pts  += 1; reasons.append("MACD4h\u2191")
        if i4h.get("macd_bear"): short_pts += 1; reasons.append("MACD4h\u2193")
        if i4h.get("st_bull"):   long_pts  += 1; reasons.append("ST4h\u2191")
        if i4h.get("st_bear"):   short_pts += 1; reasons.append("ST4h\u2193")

    # —— Confirmacion de tendencia en 1h (obligatoria) ——
    if long_pts > short_pts:
        # FIX tf1h_empty: i1h vacio ({}) NO cuenta como confirmacion
        tf1h_ok = bool(i1h) and (i1h.get("ema_bull") or i1h.get("st_bull"))
        if tf1h_ok:
            return long_pts, max_score, "LONG", reasons
        return long_pts, max_score, "NEUTRAL", reasons + ["1h_no_confirma"]

    if short_pts > long_pts:
        tf1h_ok = bool(i1h) and (i1h.get("ema_bear") or i1h.get("st_bear"))
        if tf1h_ok:
            return short_pts, max_score, "SHORT", reasons
        return short_pts, max_score, "NEUTRAL", reasons + ["1h_no_confirma"]

    return long_pts, max_score, "NEUTRAL", reasons + ["empate"]


# ─── _hold_result ────────────────────────────────────────────────────────────────────────────────────────

def _hold_result(symbol: str, reason: str) -> SignalResult:
    return SignalResult(
        symbol=symbol,
        signal="NEUTRAL",
        entry_mode="HOLD",
        score=0,
        max_score=14,
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


# ─── format_signal_block ───────────────────────────────────────────────────────────────────────────────────────

def format_signal_block(signal: Optional[SignalResult]) -> str:
    if signal is None:
        return ""

    arrow = "\U0001f7e2 LONG" if signal.signal == "LONG" else "\U0001f534 SHORT" if signal.signal == "SHORT" else "\u26aa NEUTRAL"
    lev   = f"{signal.suggested_lev}x" if signal.suggested_lev else "—"
    rr    = f"{signal.rr:.2f}" if signal.rr else "—"

    lines = [
        f"**{signal.symbol}** \u00b7 {arrow}",
        f"Score: `{signal.score}/{signal.max_score}` \u00b7 Mode: `{signal.entry_mode}` \u00b7 Lev: `{lev}` \u00b7 R/R: `{rr}`",
    ]
    if signal.entry:
        lines.append(
            f"Entry: `{signal.entry}` | SL: `{signal.sl}` | TP1: `{signal.tp1}` | TP2: `{signal.tp2}`"
        )
    if signal.reason:
        lines.append(f"_{signal.reason}_")

    return "\n".join(lines)


# ─── SignalFlipGuard ───────────────────────────────────────────────────────────────────────────────────────

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

        # FIX FlipGuard .signal: leer .signal de SignalResult ademas de .side
        side = getattr(signal, "side", None) or getattr(signal, "signal", None)
        if not side:
            if isinstance(signal, str) and signal in ("long", "short", "buy", "sell",
                                                       "LONG", "SHORT", "BUY", "SELL"):
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
        side_norm = "long" if str(side).upper() in ("LONG", "BUY") else "short"
        self._last[symbol] = (side_norm, time.monotonic())


signal_flip_guard = SignalFlipGuard()


# ─── ManualCloseCooldown ────────────────────────────────────────────────────────────────────────────────────

_MANUAL_CLOSE_COOLDOWN_S = int(os.getenv("MANUAL_CLOSE_COOLDOWN_S", "600"))  # 10 min default


class ManualCloseCooldown:
    """
    FIX manual_close_cooldown: registra cierres manuales (sin SL/TP hit) y bloquea
    reentradas al mismo simbolo durante MANUAL_CLOSE_COOLDOWN_S segundos.
    """

    def __init__(self, cooldown_s: int = _MANUAL_CLOSE_COOLDOWN_S):
        self._cooldown = cooldown_s
        self._closed: Dict[str, float] = {}

    def register(self, symbol: str) -> None:
        self._closed[symbol] = time.monotonic()
        log.info(
            "[ManualCloseCooldown] %s: cooldown manual activado (%ds)",
            symbol, self._cooldown,
        )

    def is_blocked(self, symbol: str) -> bool:
        ts = self._closed.get(symbol)
        if ts is None:
            return False
        elapsed = time.monotonic() - ts
        if elapsed < self._cooldown:
            remaining = int(self._cooldown - elapsed)
            log.debug(
                "[ManualCloseCooldown] %s: bloqueado — %ds restantes de cooldown manual",
                symbol, remaining,
            )
            return True
        del self._closed[symbol]
        return False

    def clear(self, symbol: str) -> None:
        self._closed.pop(symbol, None)


manual_close_cooldown = ManualCloseCooldown()
