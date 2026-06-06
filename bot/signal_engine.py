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

SISTEMA DE SCORING:
  El bot detecta uno de tres tipos de setup. Si no encaja en ninguno, NEUTRAL.

CAMBIOS v27 (señales premium máxima calidad):
  1. TP DINÁMICO por vol_ratio: cuando vol_ratio > 2.0 los TPs se escalan x1.2
     (mercado expansivo); cuando vol_ratio < 0.9 el TP1 se reduce x0.85
     (objetivo conservador en mercados lentos).
  2. MIN_SCORE_RATIO DINÁMICO por regime: en regímenes BEAR o VOLATILE el
     umbral sube automáticamente a MIN_SCORE_RATIO_BEAR (default 0.72) para
     que solo entren las señales más sólidas en mercados adversos.
  3. BREAKOUT — detección de retesteo: si el precio actual está dentro del
     0.5% del nivel roto (range_high/range_low) se interpreta como retest y
     suma +2 adicionales (entrada de mayor calidad y menor riesgo de fakeout).
  4. REVERSAL — swing levels de estructura: cruza el precio con los
     swing_low/swing_high reales de structure_analyzer. Si el precio está
     dentro del 0.5% de un swing histórico suma +2 (más relevante que EMA50).
  5. TENDENCIA — ADX + slope EMA21_15m: filtra tendencias planas. Requiere
     que el slope de EMA21_15m sea positivo (LONG) o negativo (SHORT) y que
     ADX simple (DX proxy) supere un umbral configurable (ADX_MIN, default 20).
     Un ADX bajo indica rango disfrazado de tendencia → penalización -2.
  6. TENDENCIA — confluencia 4h extendida: si 4h tiene MACD alcista/bajista
     además de ST4h en favor, suma +2 adicionales (señal institucional total).
     MAX de TENDENCIA sube de 13 a 15.

CAMBIOS v26 (MIN_SCORE_RATIO):
  1. MIN_SCORE_RATIO: nuevo filtro que exige que score/max_score >= umbral.
  2. Variable de entorno: MIN_SCORE_RATIO (float, default "0.62").

CAMBIOS v25 (señales premium):
  1. MIN_SCORE: 7→8.
  2. PREMIUM_SCORE=10: señales con score>=10 se marcan con ⭐.
  3. BREAKOUT — fix squeeze filter: umbral corregido a hist_atr*0.60.
  4. TENDENCIA — estructura HH/HL o LL/LH: +1 de calidad.
  5. REVERSAL — EMA50_1h como nivel clave: +1 si dentro del 0.3%.
  6. Filtro vela doji: bloquea señales cuyo cuerpo sea <20% del rango.
  7. MIN_RR_REVERSAL=2.0.

CAMBIOS v24 (fix/signal-quality):
  1. VWAP diario real.
  2. REVERSAL — ST1h obligatorio.
  3. BREAKOUT — filtro de compresión previa.
  4. Divergencias RSI en REVERSAL: +2.
  5. TENDENCIA — volumen decreciente en pullback: +1.

CAMBIOS v23 (fix/ohlcv-resilience):
  1. Modo degradado operativo para 1h.
  2. Semáforo interno ANALYZE_PAIR_CONCURRENCY.
  3. _analyze_pair_inner().

CAMBIOS v22: MTF bias filter + R/R dinámico por régimen.
CAMBIOS v21: OHLCV robustos + VWAP.
CAMBIOS v20: early entry + SL ATR dinámico.
CAMBIOS v19: trend following puro.
"""
from __future__ import annotations

import asyncio
import logging
import os
import time
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, Tuple

import pandas as pd

from bot.indicators import ema, rsi, macd, supertrend, atr as calc_atr, vwap as calc_vwap, rsi_divergence

log = logging.getLogger(__name__)

# ── Umbrales globales ────────────────────────────────────────────────────────
MIN_SCORE: int     = int(os.getenv("MIN_SIGNAL_SCORE", "8"))
MIN_RR: float      = float(os.getenv("MIN_RR_REQUIRED", "1.5"))
PREMIUM_SCORE: int = int(os.getenv("PREMIUM_SIGNAL_SCORE", "10"))

# v26: ratio mínimo base
MIN_SCORE_RATIO: float = float(os.getenv("MIN_SCORE_RATIO", "0.62"))
# v27: ratio elevado para regímenes adversos (BEAR / VOLATILE)
MIN_SCORE_RATIO_BEAR: float = float(os.getenv("MIN_SCORE_RATIO_BEAR", "0.72"))

# ── v22: R/R dinámico por régimen ────────────────────────────────────────────
_MIN_RR_TRENDING  = float(os.getenv("MIN_RR_TRENDING",  "1.6"))
_MIN_RR_RANGING   = float(os.getenv("MIN_RR_RANGING",   "2.0"))
_MIN_RR_VOLATILE  = float(os.getenv("MIN_RR_VOLATILE",  "2.2"))
_MIN_RR_REVERSAL  = float(os.getenv("MIN_RR_REVERSAL",  "2.0"))

def _min_rr_for_regime(regime: Optional[str]) -> float:
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

# v27: ratio efectivo según régimen
def _min_score_ratio_for_regime(regime: Optional[str]) -> float:
    """Eleva MIN_SCORE_RATIO en mercados adversos (BEAR o VOLATILE)."""
    if not regime:
        return MIN_SCORE_RATIO
    r = regime.upper()
    if "BEAR" in r or "VOL" in r:
        return MIN_SCORE_RATIO_BEAR
    return MIN_SCORE_RATIO

# ── v22: MTF bias filter ─────────────────────────────────────────────────────
_MTF_BLOCK_SCORE_OVERRIDE = int(os.getenv("MTF_BLOCK_SCORE_OVERRIDE", "10"))

# ── v23: semáforo interno ────────────────────────────────────────────────────
_ANALYZE_PAIR_CONCURRENCY = int(os.getenv("ANALYZE_PAIR_CONCURRENCY", "6"))
_analyze_pair_sem: Optional[asyncio.Semaphore] = None

def _get_analyze_sem() -> asyncio.Semaphore:
    global _analyze_pair_sem
    if _analyze_pair_sem is None:
        _analyze_pair_sem = asyncio.Semaphore(_ANALYZE_PAIR_CONCURRENCY)
        log.info("[signal_engine] Semáforo analyze_pair inicializado: max=%d", _ANALYZE_PAIR_CONCURRENCY)
    return _analyze_pair_sem

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
# fix: limpiar símbolo '%' en caso de que la variable venga como "2.5%"
_SL_STRUCTURE_MAX_DIST_PCT = float(os.getenv("SL_STRUCTURE_MAX_DIST_PCT", "4.0").replace("%", "").strip()) / 100.0
_VOL_AVG_WINDOW    = int(os.getenv("VOL_AVG_WINDOW", "20"))
_VOL_SIGNAL_MIN    = float(os.getenv("VOL_SIGNAL_MIN", "1.0"))
_FUNDING_LONG_MAX  = float(os.getenv("FUNDING_LONG_MAX",  "0.0005"))
_FUNDING_SHORT_MIN = float(os.getenv("FUNDING_SHORT_MIN", "-0.0005"))
_EMA_SPREAD_TREND_MIN  = float(os.getenv("EMA_SPREAD_TREND_MIN",  "0.002"))
_EMA_SPREAD_RANGE_MAX  = float(os.getenv("EMA_SPREAD_RANGE_MAX",  "0.0015"))
_BREAKOUT_WINDOW       = int(os.getenv("BREAKOUT_WINDOW", "20"))
_BREAKOUT_VOL_MIN      = float(os.getenv("BREAKOUT_VOL_MIN",  "1.4"))
_BREAKOUT_ATR_CONFIRM  = float(os.getenv("BREAKOUT_ATR_CONFIRM", "0.3"))
_BREAKOUT_SQUEEZE_PCT  = float(os.getenv("BREAKOUT_SQUEEZE_PCT", "40"))
# v27: tolerancia para detectar retesteo en BREAKOUT (porcentaje del nivel roto)
_BREAKOUT_RETEST_TOL   = float(os.getenv("BREAKOUT_RETEST_TOL", "0.005"))
_REVERSAL_RSI_LOW      = float(os.getenv("REVERSAL_RSI_LOW",  "28"))
_REVERSAL_RSI_HIGH     = float(os.getenv("REVERSAL_RSI_HIGH", "72"))
_VOL_MIN_GLOBAL        = float(os.getenv("VOL_MIN_GLOBAL",    "0.6"))
_VOL_CONFIRM_MIN       = float(os.getenv("VOL_CONFIRM_MIN",   "1.2"))
_PULLBACK_LOOKBACK     = int(os.getenv("PULLBACK_LOOKBACK", "2"))
_PULLBACK_TOLERANCE    = float(os.getenv("PULLBACK_TOLERANCE", "0.005"))
_EARLY_LEV_FACTOR      = float(os.getenv("EARLY_LEV_FACTOR", "0.2"))
_DOJI_BODY_MIN_PCT     = float(os.getenv("DOJI_BODY_MIN_PCT", "0.20"))
# v27: umbral ADX mínimo para confirmar tendencia real (evita rangos disfrazados)
_ADX_MIN               = float(os.getenv("ADX_MIN", "20.0"))
# v27: tolerancia para detectar swing level en REVERSAL
_REVERSAL_SWING_TOL    = float(os.getenv("REVERSAL_SWING_TOL", "0.005"))
# v27: multiplicadores dinámicos de TP por volatilidad
_TP_VOL_HIGH_THRESHOLD = float(os.getenv("TP_VOL_HIGH_THRESHOLD", "2.0"))
_TP_VOL_HIGH_MULT      = float(os.getenv("TP_VOL_HIGH_MULT",      "1.2"))
_TP_VOL_LOW_THRESHOLD  = float(os.getenv("TP_VOL_LOW_THRESHOLD",  "0.9"))
_TP_VOL_LOW_MULT       = float(os.getenv("TP_VOL_LOW_MULT",       "0.85"))


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


# ── v22: MTF bias helper ──────────────────────────────────────────────────────
def _mtf_bias(ind_1h: dict) -> Optional[str]:
    if not ind_1h:
        return None
    if ind_1h.get("ema_bull"):
        return "LONG"
    if ind_1h.get("ema_bear"):
        return "SHORT"
    return None


# ── v27: ADX simple (proxy DX medio) ─────────────────────────────────────────
def _adx_simple(highs: list, lows: list, closes: list, period: int = 14) -> float:
    """
    Calcula un ADX simplificado (media del DX) para period velas.
    Suficiente para detectar tendencias reales vs rangos disfrazados.
    Retorna 0.0 si no hay suficientes datos.
    """
    if len(closes) < period + 2:
        return 0.0
    try:
        plus_dm, minus_dm, tr_list = [], [], []
        for i in range(1, len(closes)):
            h_diff = highs[i] - highs[i - 1]
            l_diff = lows[i - 1] - lows[i]
            plus_dm.append(h_diff if h_diff > l_diff and h_diff > 0 else 0.0)
            minus_dm.append(l_diff if l_diff > h_diff and l_diff > 0 else 0.0)
            tr_list.append(max(
                highs[i] - lows[i],
                abs(highs[i] - closes[i - 1]),
                abs(lows[i] - closes[i - 1]),
            ))
        if len(tr_list) < period:
            return 0.0
        # Wilder smoothing
        atr_s  = sum(tr_list[:period])
        pdm_s  = sum(plus_dm[:period])
        mdm_s  = sum(minus_dm[:period])
        dx_vals = []
        for i in range(period, len(tr_list)):
            atr_s  = atr_s  - atr_s  / period + tr_list[i]
            pdm_s  = pdm_s  - pdm_s  / period + plus_dm[i]
            mdm_s  = mdm_s  - mdm_s  / period + minus_dm[i]
            pdi = 100 * pdm_s / atr_s if atr_s > 0 else 0.0
            mdi = 100 * mdm_s / atr_s if atr_s > 0 else 0.0
            denom = pdi + mdi
            dx_vals.append(100 * abs(pdi - mdi) / denom if denom > 0 else 0.0)
        return round(sum(dx_vals[-period:]) / period, 2) if dx_vals else 0.0
    except Exception:
        return 0.0


# ── v27: slope EMA21 en 15m ───────────────────────────────────────────────────
def _ema_slope(ema_series: list, lookback: int = 3) -> float:
    """
    Retorna el slope normalizado de los últimos `lookback` valores de la EMA.
    Positivo = tendencia acelerando hacia arriba; negativo = hacia abajo.
    """
    if not ema_series or len(ema_series) < lookback + 1:
        return 0.0
    tail = ema_series[-(lookback + 1):]
    delta = tail[-1] - tail[0]
    base = tail[0]
    return delta / base if base != 0 else 0.0


async def analyze_pair(
    exch,
    symbol: str,
    ohlcv_fn: Optional[Callable] = None,
    funding_rate: float = 0.0,
    regime: Optional[str] = None,
) -> SignalResult:
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

    bars_15m = _clean_bars(bars_15m)
    bars_1h  = _clean_bars(bars_1h)
    bars_4h  = _clean_bars(bars_4h)

    if len(bars_15m) < 30:
        return _hold_result(symbol, f"Insuficientes velas 15m ({len(bars_15m)})")

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

    if len(bars_15m) >= _VOL_AVG_WINDOW + 1:
        vol_last    = float(bars_15m[-1][5])
        vol_avg_ref = sum(float(b[5]) for b in bars_15m[-_VOL_AVG_WINDOW - 1:-1]) / _VOL_AVG_WINDOW
        vol_signal  = round(vol_last / vol_avg_ref, 3) if vol_avg_ref > 0 else 1.0
        if vol_signal < _VOL_SIGNAL_MIN:
            return _hold_result(symbol, f"Vol señal {vol_signal:.2f}x < {_VOL_SIGNAL_MIN}x (vela sin convicción)")
        log.debug("[signal_engine] %s vol_signal=%.2fx (min %.1fx)", symbol, vol_signal, _VOL_SIGNAL_MIN)

    setup_type, signal_str, score, max_score, reasons = _detect_setup(
        ind_15m, ind_1h, ind_4h, bars_15m, regime
    )

    if signal_str == "NEUTRAL" or setup_type is None:
        return _hold_result(symbol, f"NEUTRAL ({', '.join(reasons[-3:])})")

    # ── v22: MTF bias filter ──────────────────────────────────────────────────
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

    if signal_str == "LONG" and funding_rate > _FUNDING_LONG_MAX:
        return _hold_result(symbol, f"Funding {funding_rate:.4%} > {_FUNDING_LONG_MAX:.4%} → no LONG")
    if signal_str == "SHORT" and funding_rate < _FUNDING_SHORT_MIN:
        return _hold_result(symbol, f"Funding {funding_rate:.4%} < {_FUNDING_SHORT_MIN:.4%} → no SHORT")

    last_bar    = bars_15m[-1]
    close_price = float(last_bar[4])
    high_price  = float(last_bar[2])
    low_price   = float(last_bar[3])
    open_price  = float(last_bar[1])
    entry = close_price

    # v25: filtro vela doji
    candle_body  = abs(close_price - open_price)
    candle_range = high_price - low_price
    if candle_range > 0 and candle_body / candle_range < _DOJI_BODY_MIN_PCT:
        return _hold_result(
            symbol,
            f"Vela indecisa (doji): cuerpo={candle_body/candle_range*100:.0f}% del rango "
            f"(mín {_DOJI_BODY_MIN_PCT*100:.0f}%)",
        )

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

    # ── v27: TP dinámico por vol_ratio ────────────────────────────────────────
    vol_ratio_now = ind_15m.get("vol_ratio", 1.0)
    tp_vol_scale = 1.0
    if vol_ratio_now >= _TP_VOL_HIGH_THRESHOLD:
        tp_vol_scale = _TP_VOL_HIGH_MULT
        log.debug(
            "[signal_engine] %s TP expandido (vol_ratio=%.2fx >= %.1fx) scale=%.2f",
            symbol, vol_ratio_now, _TP_VOL_HIGH_THRESHOLD, tp_vol_scale,
        )
    elif vol_ratio_now < _TP_VOL_LOW_THRESHOLD:
        # Solo TP1 se comprime; TP2 mantiene para respetar el objetivo largo
        tp_vol_scale = None  # señal de escala diferenciada
        log.debug(
            "[signal_engine] %s TP1 conservador (vol_ratio=%.2fx < %.1fx)",
            symbol, vol_ratio_now, _TP_VOL_LOW_THRESHOLD,
        )

    if signal_str == "LONG":
        if _SL_ATR_DYNAMIC:
            sl_atr = round(entry - sl_mult * atr_val, 6)
        else:
            sl_atr = round(min(low_price - _atr_buf, entry - sl_mult * atr_val), 6)
        if tp_vol_scale is None:
            # vol bajo: TP1 conservador, TP2 normal
            tp1 = round(entry + tp1_mult * atr_val * _TP_VOL_LOW_MULT, 6)
            tp2 = round(entry + tp2_mult * atr_val, 6)
        else:
            tp1 = round(entry + tp1_mult * atr_val * tp_vol_scale, 6)
            tp2 = round(entry + tp2_mult * atr_val * tp_vol_scale, 6)
    else:
        if _SL_ATR_DYNAMIC:
            sl_atr = round(entry + sl_mult * atr_val, 6)
        else:
            sl_atr = round(max(high_price + _atr_buf, entry + sl_mult * atr_val), 6)
        if tp_vol_scale is None:
            tp1 = round(entry - tp1_mult * atr_val * _TP_VOL_LOW_MULT, 6)
            tp2 = round(entry - tp2_mult * atr_val, 6)
        else:
            tp1 = round(entry - tp1_mult * atr_val * tp_vol_scale, 6)
            tp2 = round(entry - tp2_mult * atr_val * tp_vol_scale, 6)

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

    effective_min_rr = _min_rr_for_regime(regime)
    if setup_type == "REVERSAL":
        effective_min_rr = max(effective_min_rr, _MIN_RR_REVERSAL)

    # v27: ratio efectivo dinámico según régimen
    effective_min_ratio = _min_score_ratio_for_regime(regime)

    is_fast_valid = (
        entry_mode in ("FAST", "STRONG")
        and score >= _FAST_ENTRY_MIN_SCORE
        and rr >= _FAST_ENTRY_MIN_RR
    )

    score_ratio = score / max_score if max_score > 0 else 0.0
    ratio_ok = score_ratio >= effective_min_ratio

    if entry_mode == "EARLY":
        is_valid = False
    else:
        is_valid = (score >= MIN_SCORE and rr >= effective_min_rr and ratio_ok) or is_fast_valid

    # v27: nota de TP dinámico para el log
    _tp_note = (
        f"TP×{tp_vol_scale:.2f}" if tp_vol_scale and tp_vol_scale != 1.0
        else ("TP1×{:.2f}".format(_TP_VOL_LOW_MULT) if tp_vol_scale is None else "TP=std")
    )

    log.info(
        "[signal_engine] %s %s [%s] score=%d/%d ratio=%.2f(min=%.2f) RR=%.2f(min=%.2f) "
        "entry=%.6f sl=%.6f tp1=%.6f tp2=%.6f atr=%.6f lev=%dx mode=%s valid=%s "
        "vwap=%.6f funding=%.4f%% mtf_aligned=%s regime=%s %s | %s",
        symbol, signal_str, setup_type, score, max_score, score_ratio, effective_min_ratio,
        rr, effective_min_rr,
        entry, sl, tp1, tp2, atr_val, suggested_lev, entry_mode, is_valid,
        ind_15m.get("vwap", 0.0), funding_rate * 100,
        mtf_aligned, regime or "none", _tp_note,
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
            f"[{setup_type}] score={score}/{max_score} ratio={score_ratio:.2f}(min={effective_min_ratio:.2f}) "
            f"rr={rr:.2f}(min_rr={effective_min_rr:.2f} regime={regime or 'none'})"
            + (" [EARLY bloqueado]" if entry_mode == "EARLY" else "")
            + ("" if ratio_ok else " [RATIO insuficiente]")
        ),
        extra={
            "setup_type":         setup_type,
            "sl_atr":             sl_atr,
            "sl_used":            sl,
            "is_fast":            is_fast_valid,
            "funding_rate":       funding_rate,
            "mtf_aligned":        mtf_aligned,
            "bias_1h":            bias_1h,
            "regime":             regime,
            "effective_min_rr":   effective_min_rr,
            "effective_min_ratio": effective_min_ratio,
            "is_premium":         score >= PREMIUM_SCORE,
            "score_ratio":        round(score_ratio, 3),
            "tp_vol_scale":       tp_vol_scale if tp_vol_scale else _TP_VOL_LOW_MULT,
        },
    )


def _detect_setup(
    i15: dict, i1h: dict, i4h: dict, bars_15m: list,
    regime: Optional[str] = None,
) -> Tuple[Optional[str], str, int, int, List[str]]:
    # v27: ratio efectivo pasado a la selección de candidatos
    effective_ratio = _min_score_ratio_for_regime(regime)
    candidates = []
    for mode_fn in (_score_tendencia, _score_breakout, _score_reversal):
        setup_type, signal_str, score, max_score, reasons = mode_fn(i15, i1h, i4h, bars_15m)
        score_ratio = score / max_score if max_score > 0 else 0.0
        if signal_str != "NEUTRAL" and score >= MIN_SCORE and score_ratio >= effective_ratio:
            candidates.append((setup_type, signal_str, score, max_score, reasons))
    if not candidates:
        return None, "NEUTRAL", 0, 10, [
            f"Ningún setup alcanzó MIN_SCORE o MIN_SCORE_RATIO(regime={regime or 'none'})"
        ]
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
    v27: MAX=15 (v26 era 13).
      - +2 ADX + slope: confirma tendencia real vs rango disfrazado.
      - +2 confluencia 4h extendida: MACD4h alineado además de ST4h.
    REQUISITOS OBLIGATORIOS (sin cambios desde v25):
      - EMA 1h en tendencia definida
      - MACD15m a favor
      - ST1h a favor
    """
    MAX = 15
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
    macd4h_ok = False
    if i4h:
        st4h_ok = (direction == "LONG" and i4h.get("st_bull")) or (direction == "SHORT" and i4h.get("st_bear"))
        macd4h_ok = (direction == "LONG" and i4h.get("macd_bull")) or (direction == "SHORT" and i4h.get("macd_bear"))
        if st4h_ok:
            score += 1
            reasons.append("ST4h en favor +1")
            # v27: confluencia 4h extendida — MACD4h alineado además de ST4h
            if macd4h_ok:
                score += 2
                reasons.append("MACD4h + ST4h alineados — confluencia institucional total +2")
            else:
                reasons.append("ST4h OK pero MACD4h no confirma (sin bonus institucional)")
        else:
            score = max(0, score - 2)
            reasons.append("ST4h en contra — penalización -2")
    else:
        reasons.append("ST4h sin datos")

    score += 1
    reasons.append("MACD15m en favor +1")

    # v27: ADX + slope EMA21_15m — filtro de tendencia real
    closes_15m = [float(b[4]) for b in bars_15m]
    highs_15m  = [float(b[2]) for b in bars_15m]
    lows_15m   = [float(b[3]) for b in bars_15m]
    adx_val = _adx_simple(highs_15m, lows_15m, closes_15m, 14)
    ema21_series = i15.get("_ema21_series", [])
    slope = _ema_slope(ema21_series, lookback=3) if ema21_series else 0.0
    slope_ok = (direction == "LONG" and slope > 0) or (direction == "SHORT" and slope < 0)

    if adx_val >= _ADX_MIN and slope_ok:
        reasons.append(f"ADX={adx_val:.1f}(≥{_ADX_MIN}) + slope EMA21={slope*100:.3f}% — tendencia real confirmada")
    elif adx_val < _ADX_MIN:
        score = max(0, score - 2)
        reasons.append(f"ADX={adx_val:.1f} < {_ADX_MIN} — rango disfrazado de tendencia — penalización -2")
    elif not slope_ok:
        score = max(0, score - 1)
        reasons.append(f"Slope EMA21={slope*100:.3f}% en contra de {direction} — penalización -1")

    ema21_15m = i15.get("ema21")
    close_15m = i15.get("close", 0)
    pullback_detected = False
    pullback_vol_low  = False
    if ema21_15m and close_15m:
        recent_bars = bars_15m[-(_PULLBACK_LOOKBACK + 1):-1]
        for bar in recent_bars:
            bar_low  = float(bar[3])
            bar_high = float(bar[2])
            bar_vol  = float(bar[5])
            if direction == "LONG":
                if bar_low <= ema21_15m * (1 + _PULLBACK_TOLERANCE):
                    pullback_detected = True
                    avg_vol_raw = i15.get("_avg_vol", 0.0)
                    if avg_vol_raw > 0 and bar_vol < avg_vol_raw * 0.8:
                        pullback_vol_low = True
                    break
            else:
                if bar_high >= ema21_15m * (1 - _PULLBACK_TOLERANCE):
                    pullback_detected = True
                    avg_vol_raw = i15.get("_avg_vol", 0.0)
                    if avg_vol_raw > 0 and bar_vol < avg_vol_raw * 0.8:
                        pullback_vol_low = True
                    break
        if pullback_detected:
            score += 1
            reasons.append("Pullback a EMA21_15m +1")
            if pullback_vol_low:
                score += 1
                reasons.append("Pullback con volumen bajo (corrección sana) +1")
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

    vwap_val = i15.get("vwap", 0.0)
    if vwap_val and vwap_val > 0 and close_15m:
        vwap_ok = (direction == "LONG" and close_15m > vwap_val) or \
                  (direction == "SHORT" and close_15m < vwap_val)
        if vwap_ok:
            score += 1
            reasons.append(f"Precio {'>' if direction == 'LONG' else '<'} VWAP_diario({vwap_val:.4f}) +1")
        else:
            reasons.append(f"Precio {'<' if direction == 'LONG' else '>'} VWAP_diario({vwap_val:.4f}) — sin confirmación")
    else:
        reasons.append("VWAP diario no disponible")

    # v25: estructura HH/HL (LONG) o LL/LH (SHORT) en los últimos cierres 15m
    if len(bars_15m) >= 4:
        closes_recent = [float(b[4]) for b in bars_15m[-4:]]
        if direction == "LONG":
            hh_hl = closes_recent[-1] > closes_recent[-2] > closes_recent[-3]
            if hh_hl:
                score += 1
                reasons.append("Estructura HH/HL confirmada en 15m +1")
            else:
                reasons.append("Sin estructura HH/HL en 15m")
        else:
            ll_lh = closes_recent[-1] < closes_recent[-2] < closes_recent[-3]
            if ll_lh:
                score += 1
                reasons.append("Estructura LL/LH confirmada en 15m +1")
            else:
                reasons.append("Sin estructura LL/LH en 15m")

    return "TENDENCIA", direction, score, MAX, reasons


def _score_breakout(i15: dict, i1h: dict, i4h: dict, bars_15m: list) -> Tuple[str, str, int, int, List[str]]:
    """
    v27: detección de retesteo post-ruptura (+2 si el precio actual está
    dentro del _BREAKOUT_RETEST_TOL% del nivel roto, indicando una entrada
    de mayor calidad con menor riesgo de fakeout).
    MAX sube de 8 a 10.
    """
    MAX = 10
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

    # v27: detección de retesteo — el precio está cerca del nivel roto (sin haberlo perforado de nuevo)
    retest_up   = (not broke_up and not broke_down and
                   abs(current_close - range_high) / range_high <= _BREAKOUT_RETEST_TOL and
                   current_close >= range_high * (1 - _BREAKOUT_RETEST_TOL))
    retest_down = (not broke_up and not broke_down and
                   abs(current_close - range_low) / range_low <= _BREAKOUT_RETEST_TOL and
                   current_close <= range_low * (1 + _BREAKOUT_RETEST_TOL))

    if not broke_up and not broke_down and not retest_up and not retest_down:
        return "BREAKOUT", "NEUTRAL", 0, MAX, [
            f"Sin rotura ni retesteo: close={current_close:.4f} rango=[{range_low:.4f}-{range_high:.4f}]"
        ]

    # squeeze filter (v25 fix)
    if atr_val > 0 and len(bars_15m) >= _BREAKOUT_WINDOW * 2:
        hist_highs  = [float(b[2]) for b in bars_15m[-(_BREAKOUT_WINDOW * 2):-_BREAKOUT_WINDOW]]
        hist_lows   = [float(b[3]) for b in bars_15m[-(_BREAKOUT_WINDOW * 2):-_BREAKOUT_WINDOW]]
        hist_closes = [float(b[4]) for b in bars_15m[-(_BREAKOUT_WINDOW * 2):-_BREAKOUT_WINDOW]]
        hist_atr = calc_atr(hist_highs, hist_lows, hist_closes, min(14, len(hist_closes) - 1))
        if hist_atr > 0:
            squeeze_threshold = hist_atr * (1.0 - _BREAKOUT_SQUEEZE_PCT / 100.0)
            if atr_val > squeeze_threshold:
                log.debug(
                    "[signal_engine] BREAKOUT bloqueado: ATR actual=%.6f > umbral=%.6f",
                    atr_val, squeeze_threshold,
                )
                return "BREAKOUT", "NEUTRAL", 0, MAX, [
                    f"Sin compresión previa: ATR ({atr_val:.6f}) > umbral ({squeeze_threshold:.6f})"
                ]
            reasons.append(
                f"ATR squeeze OK: actual={atr_val:.6f} < umbral={squeeze_threshold:.6f} "
                f"(hist={hist_atr:.6f}) +0"
            )

    # dirección
    if broke_up or retest_up:
        direction = "LONG"
    else:
        direction = "SHORT"

    is_retest = retest_up or retest_down

    score = 2
    if is_retest:
        # Retesteo: el precio vuelve al nivel roto — entrada premium
        score += 2
        reasons.append(
            f"Retesteo del nivel {'superior' if retest_up else 'inferior'} "
            f"(close={current_close:.4f} ≈ {range_high if retest_up else range_low:.4f}) +4 total"
        )
    else:
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
    """
    v27: swing levels de estructura — si el precio está dentro del
    _REVERSAL_SWING_TOL% de un swing_low/swing_high histórico real,
    suma +2 (más relevante que proximidad a EMA50).
    MAX sube de 12 a 14.
    """
    MAX = 14
    reasons: List[str] = []
    rsi_1h = i1h.get("rsi_val") if i1h else None
    if rsi_1h is None:
        return "REVERSAL", "NEUTRAL", 0, MAX, ["Sin datos 1h"]
    is_long  = rsi_1h <= _REVERSAL_RSI_LOW
    is_short = rsi_1h >= _REVERSAL_RSI_HIGH
    if not is_long and not is_short:
        return "REVERSAL", "NEUTRAL", 0, MAX, [f"RSI1h={rsi_1h:.0f} no es extremo"]
    direction = "LONG" if is_long else "SHORT"

    # v24: ST1h OBLIGATORIO
    if i1h:
        st1h_ok = (direction == "LONG" and i1h.get("st_bull")) or (direction == "SHORT" and i1h.get("st_bear"))
        if not st1h_ok:
            reasons.append(f"ST1h en contra de {direction} — REQUISITO OBLIGATORIO en REVERSAL")
            return "REVERSAL", "NEUTRAL", 0, MAX, reasons
    else:
        reasons.append("ST1h no disponible — REVERSAL requiere confirmación 1h")
        return "REVERSAL", "NEUTRAL", 0, MAX, reasons

    score = 2
    reasons.append(f"RSI1h={rsi_1h:.0f} extremo {'sobreventa' if is_long else 'sobrecompra'} +2")
    reasons.append("ST1h en favor del giro +0 (requisito cumplido)")

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

    # v25: EMA50_1h nivel clave
    ema50_1h = i1h.get("ema50") if i1h else None
    if ema50_1h and close_15m:
        dist_ema50 = abs(close_15m - ema50_1h) / ema50_1h
        if dist_ema50 <= 0.003:
            score += 1; reasons.append(f"Precio en EMA50_1h (dist={dist_ema50*100:.2f}%) +1")
        else:
            reasons.append(f"Precio lejos EMA50_1h ({dist_ema50*100:.2f}%)")

    # v27: swing levels de estructura — confirmación de nivel histórico real
    swing_level_hit = False
    try:
        from bot.structure_analyzer import analyze_structure
        df_15m = _bars_to_df(bars_15m)
        direction_int = 1 if is_long else -1
        struct = analyze_structure(df_15m, direction_int)
        if is_long:
            swing_ref = struct.get("last_sl", 0.0)
            if swing_ref > 0 and close_15m > 0:
                dist_swing = abs(close_15m - swing_ref) / swing_ref
                if dist_swing <= _REVERSAL_SWING_TOL:
                    score += 2
                    swing_level_hit = True
                    reasons.append(
                        f"Precio en swing_low histórico ({swing_ref:.4f}, dist={dist_swing*100:.2f}%) — nivel clave +2"
                    )
                else:
                    reasons.append(f"Precio lejos del swing_low histórico ({swing_ref:.4f}, dist={dist_swing*100:.2f}%)")
        else:
            swing_ref = struct.get("last_sh", 0.0)
            if swing_ref > 0 and close_15m > 0:
                dist_swing = abs(close_15m - swing_ref) / swing_ref
                if dist_swing <= _REVERSAL_SWING_TOL:
                    score += 2
                    swing_level_hit = True
                    reasons.append(
                        f"Precio en swing_high histórico ({swing_ref:.4f}, dist={dist_swing*100:.2f}%) — nivel clave +2"
                    )
                else:
                    reasons.append(f"Precio lejos del swing_high histórico ({swing_ref:.4f}, dist={dist_swing*100:.2f}%)")
    except Exception as e:
        log.debug("[signal_engine] swing_level REVERSAL error: %s", e)
        reasons.append("Swing levels no disponibles")

    # v24: divergencias RSI 15m
    try:
        div = rsi_divergence(bars_15m)
        if div == "BULLISH" and is_long:
            score += 2; reasons.append("Divergencia RSI alcista confirmada +2")
        elif div == "BEARISH" and is_short:
            score += 2; reasons.append("Divergencia RSI bajista confirmada +2")
        elif div != "NONE":
            reasons.append(f"Divergencia RSI {div} — no coincide con dirección")
        else:
            reasons.append("Sin divergencia RSI detectada")
    except Exception as e:
        log.debug("[signal_engine] rsi_divergence error: %s", e)
        reasons.append("Divergencia RSI no calculada")

    return "REVERSAL", direction, score, MAX, reasons


def _compute_indicators(bars: list) -> dict:
    if not bars or len(bars) < 10:
        return {}
    closes = [b[4] for b in bars]
    highs  = [b[2] for b in bars]
    lows   = [b[3] for b in bars]
    vols   = [b[5] for b in bars]
    ema21_series = ema(closes, 21)
    ema50_series = ema(closes, 50)
    ema21 = ema21_series[-1] if ema21_series else None
    ema50 = ema50_series[-1] if ema50_series else None
    rsi14 = rsi(closes, 14)
    m_line, s_line, hist = macd(closes, 12, 26, 9)
    st_dir, st_val = supertrend(highs, lows, closes, 10, 3.0)
    atr14  = calc_atr(highs, lows, closes, 14)
    vwap_v = calc_vwap(bars, reset_daily=True)
    vol_window = min(_VOL_AVG_WINDOW, len(vols))
    avg_vol   = sum(vols[-vol_window:]) / vol_window if vol_window > 0 else 1.0
    vol_ratio = round(vols[-1] / avg_vol, 3) if avg_vol > 0 else 1.0
    price_dir = "rising" if len(closes) >= 2 and closes[-1] > closes[-2] else "falling"
    return {
        "ema21":          ema21,
        "ema50":          ema50,
        "_ema21_series":  ema21_series,   # v27: necesario para slope
        "ema_bull":       bool(ema21 and ema50 and ema21 > ema50),
        "ema_bear":       bool(ema21 and ema50 and ema21 < ema50),
        "rsi_val":        rsi14,
        "macd_hist":      hist,
        "macd_bull":      hist > 0,
        "macd_bear":      hist < 0,
        "st_dir":         st_dir,
        "st_bull":        st_dir == 1,
        "st_bear":        st_dir == -1,
        "atr":            atr14,
        "vol_ratio":      vol_ratio,
        "_avg_vol":       avg_vol,
        "price_dir":      price_dir,
        "close":          closes[-1],
        "vwap":           vwap_v,
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
    sl_note    = " [struct]" if signal.extra.get("sl_atr") and signal.extra.get("sl_used") != signal.extra.get("sl_atr") else ""
    fast_note  = " ⚡FAST" if signal.extra.get("is_fast") else ""
    mtf_note   = "" if signal.extra.get("mtf_aligned", True) else " ⚠️MTF"
    premium_note = " ⭐PREMIUM" if signal.extra.get("is_premium") else ""
    # v27: nota de TP dinámico si hubo scaling
    tp_scale = signal.extra.get("tp_vol_scale", 1.0)
    tp_note = f" 📈TP×{tp_scale:.2f}" if tp_scale and tp_scale != 1.0 else ""
    lines = [
        f"**{signal.symbol}** · {arrow} [{mode}]{fast_note}{premium_note}{mtf_note}{tp_note}",
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
