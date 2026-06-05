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

SISTEMA DE SCORING (max_score = 11 desde v21-P3):
  El bot detecta uno de tres tipos de setup. Si no encaja en ninguno, NEUTRAL.

CAMBIOS v23 (fix/ohlcv-resilience):
  1. Modo degradado operativo para 1h: si bars_1h llega vacío tras el fetch
     directo (ohlcv_fn=None), se reintenta UNA vez más sólo la petición 1h
     con asyncio.sleep(1.5s) antes de continuar con MTF degradado.
     Esto cubre el caso típico de saturación puntual de HL sin descartar
     señales 15m válidas por un timeout de 1h.
  2. Semáforo interno ANALYZE_PAIR_CONCURRENCY (default 6): cuando
     pair_scanner llama a analyze_pair en paralelo para múltiples pares,
     el semáforo evita que más de 6 análisis emitan fetch simultáneos.
     Configurable via env ANALYZE_PAIR_CONCURRENCY.
  Nota: cuando ohlcv_fn es suministrado por trader.py (que ya usa
  ohlcv_cache), estos guards son irrelevantes — el semáforo y el stale
  fallback ya viven en ohlcv_cache.py.

CAMBIOS v22 (mejoras de calidad):
  1. MTF bias filter: si el bias de 1h va contra la señal de 15m, se
     requiere score >= MTF_BLOCK_SCORE_OVERRIDE (default=10) para pasar.
     Añade campo extra["mtf_aligned"] True/False en SignalResult.
  2. R/R mínimo dinámico por régimen (MIN_RR_REGIME_*):
     TRENDING  → 1.6 (tendencia ayuda)
     RANGING   → 2.0 (más incertidumbre)
     VOLATILE  → 2.2 (ruido alto)
     Si no se pasa régimen, usa MIN_RR (env) como antes.
  El régimen se pasa como parámetro opcional `regime` a analyze_pair.

CAMBIOS v21 (Prioridades 1, 2 y 3):
  P1 — OHLCV robustos:
  - _clean_bars(): elimina barras con None en cualquier campo OHLCV antes
    de pasarlas a los indicadores. Previene TypeError en EMA/RSI/ST cuando
    el exchange devuelve velas incompletas.
  - Guard diferenciado 1h: si len(bars_1h) < 20 → warning log, sin return.
    ind_1h quedará {} y el scoring lo penalizará correctamente.
  P2 — Filtros de calidad en analyze_pair:
  - VOL_SIGNAL_MIN (default 1.0): la vela de señal debe superar el
    promedio de las _VOL_AVG_WINDOW velas anteriores. Bloquea entradas
    en velas de rango muerto / trampa.
  - funding_rate: float = 0.0 añadido como parámetro a analyze_pair.
    FUNDING_LONG_MAX (+0.05%) y FUNDING_SHORT_MIN (-0.05%): bloquean
    señales cuyo funding extremo contradice la dirección.
  P3 — VWAP:
  - vwap(bars) en bot/indicators.py: VWAP acumulado (H+L+C)/3 × vol.
  - _compute_indicators: añade clave "vwap" usando las mismas bars.
  - _score_tendencia: +1 si close > VWAP en LONG (o < en SHORT).
    No penaliza si falla — es confirmación adicional, no requisito.
    MAX pasa de 10 a 11; MIN_SCORE=7 inalterado.

CAMBIOS v20 (Opción A early entry + Opción B SL ATR dinámico):
  Opción A — Early entry para señales de máxima convicción:
  - FAST_ENTRY_MIN_SCORE (default=9): score >= este valor activa modo FAST.
    Si score>=FAST_ENTRY_MIN_SCORE y rr>=FAST_ENTRY_MIN_RR (default=1.2) →
    is_valid=True aunque rr < MIN_RR (1.5). entry_mode="FAST".
  Opción B — SL dinámico ATR puro:
  - SL_ATR_DYNAMIC (default=false): SL base = entry ± SL_ATR_MULT×ATR.

CAMBIOS v19 (trend following puro — calidad sobre cantidad):
  - MACD15m a favor es REQUISITO OBLIGATORIO en _score_tendencia.
  - ST1h Y ST4h ambos requeridos. ST4h en contra → penalización -2.
  - Volumen mínimo 1.0x para puntuar. Vol<0.8x → penalización -1.
  - TP1_ATR_MULT 2.25 · TP2_ATR_MULT 4.5 · MIN_RR 1.5 · MIN_SCORE 7.

CAMBIOS v18 (fix SL estructura demasiado ancho):
  - _structure_sl añade cap SL_STRUCTURE_MAX_DIST_PCT (default 4.0%).

CAMBIOS v17: _detect_setup evalúa los 3 setups, elige mayor score/max.
CAMBIOS v16: SL por estructura 1h + session_filter.
CAMBIOS v15: TP conservadores, trailing eliminado.
"""
from __future__ import annotations

import asyncio
import logging
import os
import time
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, Tuple

import pandas as pd

from bot.indicators import ema, rsi, macd, supertrend, atr as calc_atr, vwap as calc_vwap

log = logging.getLogger(__name__)

MIN_SCORE: int  = int(os.getenv("MIN_SIGNAL_SCORE", "7"))
MIN_RR: float   = float(os.getenv("MIN_RR_REQUIRED", "1.5"))

# ── v22: R/R dinámico por régimen ──────────────────────────────────────────
_MIN_RR_TRENDING  = float(os.getenv("MIN_RR_TRENDING",  "1.6"))
_MIN_RR_RANGING   = float(os.getenv("MIN_RR_RANGING",   "2.0"))
_MIN_RR_VOLATILE  = float(os.getenv("MIN_RR_VOLATILE",  "2.2"))

def _min_rr_for_regime(regime: Optional[str]) -> float:
    """Devuelve el R/R mínimo según el régimen de mercado."""
    if not regime:
        return MIN_RR
    r = regime.upper()
    if "TREND" in r:
        return _MIN_RR_TRENDING
    if "RANG" in r:
        return _MIN_RR_RANGING
    if "VOL" in r:
        return _MIN_RR_VOLATILE
    return MIN_RR

# ── v22: MTF bias filter ────────────────────────────────────────────────────
_MTF_BLOCK_SCORE_OVERRIDE = int(os.getenv("MTF_BLOCK_SCORE_OVERRIDE", "10"))

# ── v23: semáforo interno para fetch paralelo desde pair_scanner ────────────
# Cuando pair_scanner llama analyze_pair en paralelo para N pares sin ohlcv_fn,
# este semáforo evita que >ANALYZE_PAIR_CONCURRENCY fetches directos ocurran
# a la vez. Con ohlcv_fn (trader.py), ohlcv_cache ya gestiona la concurrencia.
_ANALYZE_PAIR_CONCURRENCY = int(os.getenv("ANALYZE_PAIR_CONCURRENCY", "6"))
_analyze_pair_sem: Optional[asyncio.Semaphore] = None

def _get_analyze_sem() -> asyncio.Semaphore:
    global _analyze_pair_sem
    if _analyze_pair_sem is None:
        _analyze_pair_sem = asyncio.Semaphore(_ANALYZE_PAIR_CONCURRENCY)
        log.info(
            "[signal_engine] Semáforo analyze_pair inicializado: max=%d (env ANALYZE_PAIR_CONCURRENCY)",
            _ANALYZE_PAIR_CONCURRENCY,
        )
    return _analyze_pair_sem

_FAST_ENTRY_MIN_SCORE = int(os.getenv("FAST_ENTRY_MIN_SCORE", "9"))
_FAST_ENTRY_MIN_RR    = float(os.getenv("FAST_ENTRY_MIN_RR", "1.2"))
_SL_ATR_DYNAMIC = os.getenv("SL_ATR_DYNAMIC", "false").lower() == "true"
_BARS_NEEDED = int(os.getenv("BARS_NEEDED", "100"))
_SL_ATR_MULT       = float(os.getenv("SL_ATR_MULT",  "1.5"))
_TP1_ATR_MULT      = float(os.getenv("TP1_ATR_MULT", "2.25"))
_TP2_ATR_MULT      = float(os.getenv("TP2_ATR_MULT", "4.5"))
_MAX_LEV           = int(os.getenv("LEVERAGE", "15"))
_SL_CANDLE_BUFFER  = float(os.getenv("SL_CANDLE_BUFFER", "0.2"))
_SL_STRUCTURE_ENABLED = os.getenv("SL_STRUCTURE_ENABLED", "true").lower() != "false"
_SL_STRUCTURE_MAX_DIST_PCT = float(os.getenv("SL_STRUCTURE_MAX_DIST_PCT", "4.0")) / 100.0
_VOL_AVG_WINDOW    = int(os.getenv("VOL_AVG_WINDOW", "20"))
_VOL_SIGNAL_MIN    = float(os.getenv("VOL_SIGNAL_MIN", "1.0"))
_FUNDING_LONG_MAX  = float(os.getenv("FUNDING_LONG_MAX",  "0.0005"))
_FUNDING_SHORT_MIN = float(os.getenv("FUNDING_SHORT_MIN", "-0.0005"))
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

# Tiempo de espera antes del retry de 1h en modo degradado (segundos)
_1H_RETRY_DELAY_S = float(os.getenv("OHLCV_1H_RETRY_DELAY_S", "1.5"))


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


# ─────────────────────────────────────────────────────────────────────────────
# v21 P1: limpieza OHLCV
# ─────────────────────────────────────────────────────────────────────────────
def _clean_bars(bars: list) -> list:
    """Elimina barras con None en cualquier campo OHLCV [t,o,h,l,c,v]."""
    return [b for b in (bars or []) if b is not None and all(v is not None for v in b)]


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


def _bars_to_df(bars: list) -> pd.DataFrame:
    df = pd.DataFrame(bars, columns=["timestamp", "open", "high", "low", "close", "volume"])
    df = df.astype({"open": float, "high": float, "low": float, "close": float, "volume": float})
    return df


def _structure_sl(
    bars_1h: list,
    signal_str: str,
    entry: float,
    sl_atr: float,
    atr_val: float,
) -> float:
    if not _SL_STRUCTURE_ENABLED or len(bars_1h) < 20:
        return sl_atr
    try:
        from bot.structure_analyzer import analyze_structure, STRUCTURE_SWING_N
        df = _bars_to_df(bars_1h)
        direction = 1 if signal_str == "LONG" else -1
        struct = analyze_structure(df, direction)
        swing_sl_buffer = 0.1 * atr_val
        if signal_str == "LONG":
            swing_low = struct.get("last_sl", 0.0)
            if swing_low > 0:
                candidate = round(swing_low - swing_sl_buffer, 6)
                if entry > 0:
                    dist_pct = abs(entry - candidate) / entry
                    if dist_pct > _SL_STRUCTURE_MAX_DIST_PCT:
                        log.debug("[signal_engine] SL estructura LONG cap → fallback ATR SL=%.6f", sl_atr)
                        return sl_atr
                if candidate < sl_atr:
                    return candidate
        else:
            swing_high = struct.get("last_sh", 0.0)
            if swing_high > 0:
                candidate = round(swing_high + swing_sl_buffer, 6)
                if entry > 0:
                    dist_pct = abs(candidate - entry) / entry
                    if dist_pct > _SL_STRUCTURE_MAX_DIST_PCT:
                        log.debug("[signal_engine] SL estructura SHORT cap → fallback ATR SL=%.6f", sl_atr)
                        return sl_atr
                if candidate > sl_atr:
                    return candidate
    except Exception as e:
        log.debug("[signal_engine] _structure_sl error (fallback ATR): %s", e)
    return sl_atr


# ── v22: MTF bias helper ─────────────────────────────────────────────────────

def _mtf_bias(ind_1h: dict) -> Optional[str]:
    """Devuelve 'LONG', 'SHORT' o None según el bias de 1h (EMA21 vs EMA50)."""
    if not ind_1h:
        return None
    if ind_1h.get("ema_bull"):
        return "LONG"
    if ind_1h.get("ema_bear"):
        return "SHORT"
    return None


async def analyze_pair(
    exch,
    symbol: str,
    ohlcv_fn: Optional[Callable] = None,
    funding_rate: float = 0.0,
    regime: Optional[str] = None,          # v22: régimen de mercado
) -> SignalResult:
    """
    v23: cuando ohlcv_fn es None (llamada directa sin caché),
    se usa el semáforo interno _analyze_pair_sem para limitar
    la concurrencia de fetches directos desde pair_scanner.
    """
    if ohlcv_fn is not None:
        # ohlcv_fn viene de trader.py → ohlcv_cache gestiona semáforo/backoff
        return await _analyze_pair_inner(exch, symbol, ohlcv_fn, funding_rate, regime)

    # Sin ohlcv_fn: adquirir semáforo interno antes de los fetches directos
    async with _get_analyze_sem():
        return await _analyze_pair_inner(exch, symbol, None, funding_rate, regime)


async def _analyze_pair_inner(
    exch,
    symbol: str,
    ohlcv_fn: Optional[Callable],
    funding_rate: float,
    regime: Optional[str],
) -> SignalResult:
    try:
        if ohlcv_fn is not None:
            bars_15m, bars_1h, bars_4h = await asyncio.gather(
                ohlcv_fn("15m"), ohlcv_fn("1h"), ohlcv_fn("4h"),
                return_exceptions=False,
            )
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

    # v21 P1
    bars_15m = _clean_bars(bars_15m)
    bars_1h  = _clean_bars(bars_1h)
    bars_4h  = _clean_bars(bars_4h)

    if len(bars_15m) < 30:
        return _hold_result(symbol, f"Insuficientes velas 15m ({len(bars_15m)})")

    # v23: modo degradado — retry 1h si llegó vacío y estamos en fetch directo
    if ohlcv_fn is None and len(bars_1h) < 20:
        log.warning(
            "[signal_engine] %s 1h vacío tras fetch inicial (%d velas) — reintentando en %.1fs",
            symbol, len(bars_1h), _1H_RETRY_DELAY_S,
        )
        await asyncio.sleep(_1H_RETRY_DELAY_S)
        retry_1h = await _fetch_bars(exch, symbol, "1h", _BARS_NEEDED)
        retry_1h = _clean_bars(retry_1h)
        if len(retry_1h) >= 20:
            bars_1h = retry_1h
            log.info("[signal_engine] %s 1h recuperado en retry (%d velas)", symbol, len(bars_1h))
        else:
            log.warning(
                "[signal_engine] %s 1h sigue incompleto tras retry (%d velas) → MTF degradado",
                symbol, len(retry_1h),
            )
    elif len(bars_1h) < 20:
        log.warning("[signal_engine] %s 1h incompleto (%d velas) → MTF degradado", symbol, len(bars_1h))

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

    # v21 P2: volumen vela de señal
    if len(bars_15m) >= _VOL_AVG_WINDOW + 1:
        vol_last    = float(bars_15m[-1][5])
        vol_avg_ref = sum(float(b[5]) for b in bars_15m[-_VOL_AVG_WINDOW - 1:-1]) / _VOL_AVG_WINDOW
        vol_signal  = round(vol_last / vol_avg_ref, 3) if vol_avg_ref > 0 else 1.0
        if vol_signal < _VOL_SIGNAL_MIN:
            return _hold_result(symbol, f"Vol señal {vol_signal:.2f}x < {_VOL_SIGNAL_MIN}x (vela sin convicción)")
        log.debug("[signal_engine] %s vol_signal=%.2fx (min %.1fx)", symbol, vol_signal, _VOL_SIGNAL_MIN)

    setup_type, signal_str, score, max_score, reasons = _detect_setup(
        ind_15m, ind_1h, ind_4h, bars_15m
    )

    if signal_str == "NEUTRAL" or setup_type is None:
        return _hold_result(symbol, f"NEUTRAL ({', '.join(reasons[-3:])})")

    # ── v22: MTF bias filter ─────────────────────────────────────────────────
    bias_1h = _mtf_bias(ind_1h)
    mtf_aligned = (bias_1h is None) or (bias_1h == signal_str)
    if not mtf_aligned:
        if score < _MTF_BLOCK_SCORE_OVERRIDE:
            return _hold_result(
                symbol,
                f"MTF bloqueado: señal 15m={signal_str} vs bias 1h={bias_1h} "
                f"(score={score} < {_MTF_BLOCK_SCORE_OVERRIDE})",
            )
        log.warning(
            "[signal_engine] %s MTF desalineado (%s vs 1h=%s) — PERMITIDO por score alto (%d)",
            symbol, signal_str, bias_1h, score,
        )

    from bot.session_filter import check_session
    session_block = check_session(setup_type)
    if session_block:
        return _hold_result(symbol, session_block)

    # v21 P2: filtro funding
    if signal_str == "LONG" and funding_rate > _FUNDING_LONG_MAX:
        return _hold_result(symbol, f"Funding {funding_rate:.4%} > {_FUNDING_LONG_MAX:.4%} → no LONG")
    if signal_str == "SHORT" and funding_rate < _FUNDING_SHORT_MIN:
        return _hold_result(symbol, f"Funding {funding_rate:.4%} < {_FUNDING_SHORT_MIN:.4%} → no SHORT")

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
        tp1_mult = float(os.getenv("TP1_ATR_MULT_REVERSAL", "1.8"))
        tp2_mult = float(os.getenv("TP2_ATR_MULT_REVERSAL", "2.5"))
    elif setup_type == "BREAKOUT":
        sl_mult  = _SL_ATR_MULT
        tp1_mult = float(os.getenv("TP1_ATR_MULT_BREAKOUT", "2.1"))
        tp2_mult = float(os.getenv("TP2_ATR_MULT_BREAKOUT", "4.0"))
    else:
        sl_mult  = _SL_ATR_MULT
        tp1_mult = _TP1_ATR_MULT
        tp2_mult = float(os.getenv("TP2_ATR_MULT_TENDENCIA", str(_TP2_ATR_MULT)))

    if signal_str == "LONG":
        if _SL_ATR_DYNAMIC:
            sl_atr = round(entry - sl_mult * atr_val, 6)
        else:
            sl_atr = round(min(low_price - _atr_buf, entry - sl_mult * atr_val), 6)
        tp1 = round(entry + tp1_mult * atr_val, 6)
        tp2 = round(entry + tp2_mult * atr_val, 6)
    else:
        if _SL_ATR_DYNAMIC:
            sl_atr = round(entry + sl_mult * atr_val, 6)
        else:
            sl_atr = round(max(high_price + _atr_buf, entry + sl_mult * atr_val), 6)
        tp1 = round(entry - tp1_mult * atr_val, 6)
        tp2 = round(entry - tp2_mult * atr_val, 6)

    sl = _structure_sl(bars_1h, signal_str, entry, sl_atr, atr_val)

    risk   = abs(entry - sl)
    reward = abs(tp1 - entry)
    rr     = round(reward / risk, 2) if risk > 0 else 0.0

    if score >= max_score - 1:
        entry_mode = "STRONG"
    elif score >= _FAST_ENTRY_MIN_SCORE:
        entry_mode = "FAST"
    elif score >= MIN_SCORE + 1:
        entry_mode = "NORMAL"
    else:
        entry_mode = "EARLY"

    if entry_mode == "STRONG" and rr >= 1.8:
        suggested_lev = _MAX_LEV
    elif entry_mode in ("NORMAL", "FAST"):
        suggested_lev = max(1, int(_MAX_LEV * 0.6))
    else:
        suggested_lev = max(1, int(_MAX_LEV * _EARLY_LEV_FACTOR))

    # ── v22: R/R mínimo dinámico por régimen ────────────────────────────────
    effective_min_rr = _min_rr_for_regime(regime)

    is_fast_valid = (
        entry_mode in ("FAST", "STRONG")
        and score >= _FAST_ENTRY_MIN_SCORE
        and rr >= _FAST_ENTRY_MIN_RR
    )
    is_valid = (score >= MIN_SCORE and rr >= effective_min_rr) or is_fast_valid

    log.info(
        "[signal_engine] %s %s [%s] score=%d/%d RR=%.2f(min=%.2f) entry=%.6f sl=%.6f "
        "tp1=%.6f tp2=%.6f atr=%.6f lev=%dx mode=%s valid=%s vwap=%.6f "
        "funding=%.4f%% mtf_aligned=%s regime=%s | %s",
        symbol, signal_str, setup_type, score, max_score, rr, effective_min_rr,
        