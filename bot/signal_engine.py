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
  2. Semáforo interno ANALYZE_PAIR_CONCURRENCY (default 6): cuando
     pair_scanner llama a analyze_pair en paralelo para múltiples pares,
     el semáforo evita que más de 6 análisis emitan fetch simultáneos.
     Configurable via env ANALYZE_PAIR_CONCURRENCY.
  3. _analyze_pair_inner(): extrae la lógica de analyze_pair para permitir
     que el semáforo la envuelva sin duplicar código.
  Nota: cuando ohlcv_fn es suministrado por trader.py (que ya usa
  ohlcv_cache), estos guards son irrelevantes.

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
_ANALYZE_PAIR_CONCURRENCY = int(os.getenv("ANALYZE_PAIR_CONCURRENCY", "6"))
_analyze_pair_sem: Optional[asyncio.Semaphore] = None

def _get_analyze_sem() -> asyncio.Semaphore:
    global _analyze_pair_sem
    if _analyze_pair_sem is None:
        _analyze_pair_sem = asyncio.Semaphore(_ANALYZE_PAIR_CONCURRENCY)
        log.info(
            "[signal_engine] Semáforo analyze_pair inicializado: max=%d",
            _ANALYZE_PAIR_CONCURRENCY,
        )
    return _analyze_pair_sem

# Tiempo de espera antes del retry de 1h en modo degradado (segundos)
_1H_RETRY_DELAY_S = float(os.getenv("OHLCV_1H_RETRY_DELAY_S", "1.5"))

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
    regime: Optional[str] = None,
) -> SignalResult:
    """
    v23: cuando ohlcv_fn es None (llamada directa sin caché),
    se usa el semáforo interno _analyze_pair_sem para limitar
    la concurrencia de fetches directos desde pair_scanner.
    """
    if ohlcv_fn is not None:
        return await _analyze_pair_inner(exch, symbol, ohlcv_fn, funding_rate, regime)
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
        entry, sl, tp1, tp2, atr_val, suggested_lev, entry_mode, is_valid,
        ind_15m.get("vwap", 0.0), funding_rate * 100,
        mtf_aligned, regime or "none",
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
        reason="" if is_valid else (
            f"[{setup_type}] score={score}/{max_score} rr={rr:.2f} "
            f"(min_rr={effective_min_rr:.2f} regime={regime or 'none'})"
        ),
        extra={
            "setup_type":       setup_type,
            "sl_atr":           sl_atr,
            "sl_used":          sl,
            "is_fast":          is_fast_valid,
            "funding_rate":     funding_rate,
            "mtf_aligned":      mtf_aligned,
            "bias_1h":          bias_1h,
            "regime":           regime,
            "effective_min_rr": effective_min_rr,
        },
    )


def _detect_setup(
    i15: dict, i1h: dict, i4h: dict, bars_15m: list
) -> Tuple[Optional[str], str, int, int, List[str]]:
    candidates = []
    for mode_fn in (_score_tendencia, _score_breakout, _score_reversal):
        setup_type, signal_str, score, max_score, reasons = mode_fn(i15, i1h, i4h, bars_15m)
        if signal_str != "NEUTRAL" and score >= MIN_SCORE:
            candidates.append((setup_type, signal_str, score, max_score, reasons))
    if not candidates:
        return None, "NEUTRAL", 0, 10, ["Ningún setup alcanzó MIN_SCORE"]
    best = max(candidates, key=lambda x: x[2] / x[3])
    if len(candidates) > 1:
        log.debug(
            "[signal_engine] %d setups válidos — elegido %s (%d/%d) sobre %s",
            len(candidates), best[0], best[2], best[3],
            ", ".join(f"{c[0]}({c[2]}/{c[3]})" for c in candidates if c is not best),
        )
    return best


def _score_tendencia(i15: dict, i1h: dict, i4h: dict, bars_15m: list) -> Tuple[str, str, int, int, List[str]]:
    """
    v21-P3: MAX=11 (se añade +1 VWAP).
    REQUISITOS OBLIGATORIOS:
      - EMA 1h en tendencia definida
      - MACD15m a favor
      - ST1h a favor
    """
    MAX = 11
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

    macd_ok = (direction == "LONG" and i15.get("macd_bull")) or (direction == "SHORT" and i15.get("macd_bear"))
    if not macd_ok:
        reasons.append(f"MACD15m en contra de {direction} — requisito obligatorio")
        return "TENDENCIA", "NEUTRAL", 0, MAX, reasons

    st1h_ok = (direction == "LONG" and i1h.get("st_bull")) or (direction == "SHORT" and i1h.get("st_bear"))
    if not st1h_ok:
        reasons.append(f"ST1h en contra de {direction} — requisito obligatorio")
        return "TENDENCIA", "NEUTRAL", 0, MAX, reasons

    score = 0

    ema_15m_ok = (direction == "LONG" and i15.get("ema_bull")) or (direction == "SHORT" and i15.get("ema_bear"))
    if ema_15m_ok:
        score += 2
        reasons.append(f"EMA15m+1h alineados {direction} (spread={ema_spread_1h*100:.2f}%) +2")
    else:
        score += 1
        reasons.append(f"EMA1h en {direction} pero 15m aun no (spread={ema_spread_1h*100:.2f}%) +1")

    score += 1
    reasons.append("ST1h en favor +1")

    st4h_ok = False
    if i4h:
        st4h_ok = (direction == "LONG" and i4h.get("st_bull")) or (direction == "SHORT" and i4h.get("st_bear"))
        if st4h_ok:
            score += 1
            reasons.append("ST4h en favor +1")
        else:
            score = max(0, score - 2)
            reasons.append("ST4h en contra — penalización -2")
    else:
        reasons.append("ST4h sin datos")

    score += 1
    reasons.append("MACD15m en favor +1")

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
                    touched_ema = True; break
            else:
                if bar_high >= ema21_15m * (1 - _PULLBACK_TOLERANCE):
                    touched_ema = True; break
        if touched_ema:
            score += 1
            reasons.append("Pullback a EMA21_15m +1")
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
    elif vol_ratio >= 1.0:
        reasons.append(f"Vol15m={vol_ratio:.1f}x aceptable")
    elif vol_ratio < 0.8:
        score = max(0, score - 1)
        reasons.append(f"Vol15m={vol_ratio:.1f}x muy débil — penalización -1")
    else:
        reasons.append(f"Vol15m={vol_ratio:.1f}x débil")

    st15m_ok = (direction == "LONG" and i15.get("st_bull")) or (direction == "SHORT" and i15.get("st_bear"))
    confluencia = st15m_ok and st1h_ok and (not i4h or st4h_ok)
    if confluencia:
        score += 2
        reasons.append("Confluencia total ST 15m+1h+4h +2")
    else:
        reasons.append(f"Confluencia ST parcial (15m={st15m_ok} 1h={st1h_ok} 4h={st4h_ok})")

    # v21 P3: VWAP
    vwap_val = i15.get("vwap", 0.0)
    if vwap_val and vwap_val > 0 and close_15m:
        vwap_ok = (direction == "LONG" and close_15m > vwap_val) or \
                  (direction == "SHORT" and close_15m < vwap_val)
        if vwap_ok:
            score += 1
            reasons.append(f"Precio {'>' if direction == 'LONG' else '<'} VWAP({vwap_val:.4f}) +1")
        else:
            reasons.append(f"Precio {'<' if direction == 'LONG' else '>'} VWAP({vwap_val:.4f}) — sin confirmación")
    else:
        reasons.append("VWAP no disponible")

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
    broke_up   = current_close > (range_high + breakout_pad)
    broke_down = current_close < (range_low  - breakout_pad)
    if not broke_up and not broke_down:
        return "BREAKOUT", "NEUTRAL", 0, MAX, [
            f"Sin rotura: close={current_close:.4f} rango=[{range_low:.4f}-{range_high:.4f}]"
        ]
    direction = "LONG" if broke_up else "SHORT"
    score = 2
    reasons.append(f"Ruptura {'alcista' if broke_up else 'bajista'} confirmada +2")
    if vol_ratio >= _BREAKOUT_VOL_MIN:
        score += 2; reasons.append(f"Vol={vol_ratio:.1f}x breakout +2")
    elif vol_ratio >= 1.1:
        score += 1; reasons.append(f"Vol={vol_ratio:.1f}x moderado +1")
    else:
        reasons.append(f"Vol={vol_ratio:.1f}x BAJO — posible fakeout")
    if i1h:
        st1h_ok = (direction == "LONG" and i1h.get("st_bull")) or (direction == "SHORT" and i1h.get("st_bear"))
        if st1h_ok: score += 1; reasons.append("ST1h confirma +1")
        else: reasons.append("ST1h no confirma")
    if i4h:
        st4h_ok = (direction == "LONG" and i4h.get("st_bull")) or (direction == "SHORT" and i4h.get("st_bear"))
        if st4h_ok: score += 1; reasons.append("ST4h confirma +1")
        else: reasons.append("ST4h no confirma")
    rsi_15m = i15.get("rsi_val")
    if rsi_15m is not None:
        rsi_ok = (direction == "LONG" and 45 <= rsi_15m <= 70) or (direction == "SHORT" and 30 <= rsi_15m <= 55)
        if rsi_ok: score += 1; reasons.append(f"RSI15m={rsi_15m:.0f} razonable +1")
        else: reasons.append(f"RSI15m={rsi_15m:.0f} sobreextendido")
    if i1h:
        macd_ok = (direction == "LONG" and i1h.get("macd_bull")) or (direction == "SHORT" and i1h.get("macd_bear"))
        if macd_ok: score += 1; reasons.append("MACD1h en favor +1")
        else: reasons.append("MACD1h en contra")
    return "BREAKOUT", direction, score, MAX, reasons


def _score_reversal(i15: dict, i1h: dict, i4h: dict, bars_15m: list) -> Tuple[str, str, int, int, List[str]]:
    MAX = 9
    reasons: List[str] = []
    rsi_1h = i1h.get("rsi_val") if i1h else None
    if rsi_1h is None:
        return "REVERSAL", "NEUTRAL", 0, MAX, ["Sin datos 1h"]
    is_long  = rsi_1h <= _REVERSAL_RSI_LOW
    is_short = rsi_1h >= _REVERSAL_RSI_HIGH
    if not is_long and not is_short:
        return "REVERSAL", "NEUTRAL", 0, MAX, [f"RSI1h={rsi_1h:.0f} no es extremo"]
    direction = "LONG" if is_long else "SHORT"
    score = 2
    reasons.append(f"RSI1h={rsi_1h:.0f} extremo {'sobreventa' if is_long else 'sobrecompra'} +2")
    hist_15m = i15.get("macd_hist")
    if hist_15m is not None:
        if is_long and hist_15m > 0:
            score += 2; reasons.append(f"MACD15m hist={hist_15m:.4f} alcista +2")
        elif is_short and hist_15m < 0:
            score += 2; reasons.append(f"MACD15m hist={hist_15m:.4f} bajista +2")
        else:
            reasons.append(f"MACD15m hist={hist_15m:.4f} sin confirmar")
    else:
        reasons.append("MACD15m no disponible")
    last_open  = float(bars_15m[-1][1])
    last_close = float(bars_15m[-1][4])
    if (is_long and last_close > last_open) or (is_short and last_close < last_open):
        score += 1; reasons.append("Vela de giro confirmada +1")
    else:
        reasons.append("Sin vela de giro")
    vol_ratio = i15.get("vol_ratio", 1.0)
    if vol_ratio >= 1.5:
        score += 1; reasons.append(f"Vol15m={vol_ratio:.1f}x capitulación +1")
    else:
        reasons.append(f"Vol15m={vol_ratio:.1f}x sin capitulación")
    if i4h:
        st4h_against = (is_long and i4h.get("st_bear")) or (is_short and i4h.get("st_bull"))
        if st4h_against: score += 1; reasons.append("ST4h agotamiento +1")
        else: reasons.append("ST4h no confirma agotamiento")
    rsi_4h = i4h.get("rsi_val") if i4h else None
    if rsi_4h is not None and 40 <= rsi_4h <= 60:
        score += 1; reasons.append(f"RSI4h={rsi_4h:.0f} neutro +1")
    else:
        reasons.append("RSI4h no neutro")
    close_15m = i15.get("close", 0)
    ema21_1h  = i1h.get("ema21") if i1h else None
    if ema21_1h and close_15m:
        dist_pct = abs(close_15m - ema21_1h) / ema21_1h
        if dist_pct <= 0.005:
            score += 1; reasons.append(f"Precio toca EMA21_1h (dist={dist_pct*100:.2f}%) +1")
        else:
            reasons.append(f"Precio lejos EMA21_1h ({dist_pct*100:.2f}%)")
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
    vwap_v = calc_vwap(bars)
    vol_window = min(_VOL_AVG_WINDOW, len(vols))
    avg_vol   = sum(vols[-vol_window:]) / vol_window if vol_window > 0 else 1.0
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
        "vwap":      vwap_v,
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
        symbol=symbol, signal="NEUTRAL", entry_mode="HOLD",
        score=0, max_score=10, entry=0.0, sl=0.0, tp1=0.0, tp2=0.0,
        atr=0.0, rr=0.0, suggested_lev=1, indicators={},
        is_valid=False, reason=reason,
    )


def format_signal_block(signal) -> str:
    if signal is None:
        return ""
    arrow = "\U0001f7e2 LONG" if signal.signal == "LONG" else "\U0001f534 SHORT" if signal.signal == "SHORT" else "⚪ NEUTRAL"
    lev  = f"{signal.suggested_lev}x" if signal.suggested_lev else "—"
    rr   = f"{signal.rr:.2f}" if signal.rr else "—"
    mode = signal.extra.get("setup_type", signal.entry_mode)
    sl_note   = " [struct]" if signal.extra.get("sl_atr") and signal.extra.get("sl_used") != signal.extra.get("sl_atr") else ""
    fast_note = " ⚡FAST" if signal.extra.get("is_fast") else ""
    mtf_note  = "" if signal.extra.get("mtf_aligned", True) else " ⚠️MTF"
    lines = [
        f"**{signal.symbol}** · {arrow} [{mode}]{fast_note}{mtf_note}",
        f"Score: `{signal.score}/{signal.max_score}` · Mode: `{signal.entry_mode}` · Lev: `{lev}` · R/R: `{rr}`",
    ]
    if signal.entry:
        lines.append(f"Entry: `{signal.entry}` | SL: `{signal.sl}`{sl_note} | TP1: `{signal.tp1}` | TP2: `{signal.tp2}`")
    if signal.reason:
        lines.append(f"_{signal.reason}_")
    return "\n".join(lines)


_FLIP_COOLDOWN_S = float(os.getenv("SIGNAL_FLIP_COOLDOWN_S", "120"))


class SignalFlipGuard:
    def __init__(self, cooldown_s: float = _FLIP_COOLDOWN_S):
        self._cooldown = cooldown_s
        self._last: Dict[str, Tuple[str, float]] = {}

    def allow(self, symbol: str, signal) -> bool:
        if self._cooldown <= 0 or signal is None:
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
