from __future__ import annotations

import asyncio
import logging
import os
import time
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, Tuple

import pandas as pd

from bot.indicators import ema, rsi, macd, supertrend, atr as calc_atr, rsi_divergence

log = logging.getLogger(__name__)

MIN_SCORE: int     = int(os.getenv("MIN_SIGNAL_SCORE", "8"))
MIN_RR: float      = float(os.getenv("MIN_RR_REQUIRED", "1.5"))
PREMIUM_SCORE: int = int(os.getenv("PREMIUM_SIGNAL_SCORE", "10"))

MAX_SCORE_NEUTRAL: int = 10

MIN_SCORE_RATIO: float = float(os.getenv("MIN_SCORE_RATIO", "0.62"))
MIN_SCORE_RATIO_BEAR: float = float(os.getenv("MIN_SCORE_RATIO_BEAR", "0.72"))

_MIN_RR_TRENDING  = float(os.getenv("MIN_RR_TRENDING",  "1.6"))
_MIN_RR_RANGING   = float(os.getenv("MIN_RR_RANGING",   "2.0"))
_MIN_RR_VOLATILE  = float(os.getenv("MIN_RR_VOLATILE",  "2.2"))
_MIN_RR_REVERSAL  = float(os.getenv("MIN_RR_REVERSAL",  "2.0"))

# ---------------------------------------------------------------------------
# Pesos diferenciales por indicador — Mejora #6
# ---------------------------------------------------------------------------

def _w(name: str, default: float) -> float:
    """Lee un peso desde env var. Si no está definida devuelve el default."""
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return float(raw)
    except (ValueError, TypeError):
        log.warning("[signal_engine] Peso %s='%s' inválido — usando default=%.2f", name, raw, default)
        return default


# TENDENCIA
_WT = {
    "EMA_ALIGN_FULL":    _w("W_TREND_EMA_ALIGN_FULL",    2.0),
    "EMA_ALIGN_PARTIAL": _w("W_TREND_EMA_ALIGN_PARTIAL", 1.0),
    "ST_1H":             _w("W_TREND_ST_1H",             1.0),
    "ST_4H":             _w("W_TREND_ST_4H",             1.0),
    "ST_4H_PENALTY":     _w("W_TREND_ST_4H_PENALTY",     2.0),
    "MACD_4H_CONF":      _w("W_TREND_MACD_4H_CONFLUENCE",2.0),
    "MACD_15M":          _w("W_TREND_MACD_15M",          1.0),
    "ADX_PENALTY":       _w("W_TREND_ADX_PENALTY",       2.0),
    "ADX_SLOPE_PENALTY": _w("W_TREND_ADX_SLOPE_PENALTY", 1.0),
    "ADX1H_PENALTY":     _w("W_TREND_ADX1H_PENALTY",     1.0),
    "PULLBACK":          _w("W_TREND_PULLBACK",          1.0),
    "PULLBACK_VOL":      _w("W_TREND_PULLBACK_VOL",      1.0),
    "RSI":               _w("W_TREND_RSI",               1.0),
    "VOLUME":            _w("W_TREND_VOLUME",            1.0),
    "VOLUME_PENALTY":    _w("W_TREND_VOLUME_PENALTY",    1.0),
    "ST_CONFLUENCE":     _w("W_TREND_ST_CONFLUENCE",     2.0),
    "VWAP":              _w("W_TREND_VWAP",              1.0),
    "VWAP_PENALTY":      _w("W_TREND_VWAP_PENALTY",      1.0),
    "STRUCTURE":         _w("W_TREND_STRUCTURE",         1.0),
}

# BREAKOUT
_WB = {
    "BASE":     _w("W_BO_BASE",     2.0),
    "RETEST":   _w("W_BO_RETEST",   2.0),
    "VOL_HIGH": _w("W_BO_VOL_HIGH", 2.0),
    "VOL_MID":  _w("W_BO_VOL_MID",  1.0),
    "ST_1H":    _w("W_BO_ST_1H",    1.0),
    "ST_4H":    _w("W_BO_ST_4H",    1.0),
    "RSI":      _w("W_BO_RSI",      1.0),
    "MACD_1H":  _w("W_BO_MACD_1H",  1.0),
}

# REVERSAL
_WR = {
    "BASE":    _w("W_REV_BASE",    2.0),
    "SWING":   _w("W_REV_SWING",   2.0),
    "EMA50":   _w("W_REV_EMA50",   1.0),
    "MACD":    _w("W_REV_MACD",    1.0),
    "VOL":     _w("W_REV_VOL",     1.0),
    "RSI":     _w("W_REV_RSI",     1.0),
    "VWAP":    _w("W_REV_VWAP",    1.0),
    "ST_4H":   _w("W_REV_ST_4H",   1.0),
}

log.info(
    "[signal_engine] Pesos TENDENCIA: %s",
    " | ".join(f"{k}={v:.2f}" for k, v in _WT.items()),
)
log.info(
    "[signal_engine] Pesos BREAKOUT: %s",
    " | ".join(f"{k}={v:.2f}" for k, v in _WB.items()),
)
log.info(
    "[signal_engine] Pesos REVERSAL: %s",
    " | ".join(f"{k}={v:.2f}" for k, v in _WR.items()),
)

# FIX #1: _min_rr_for_regime ahora cubre el caso REVERSAL explicitamente
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
    if "REVERSAL" in r:
        return _MIN_RR_REVERSAL
    return MIN_RR

def _min_score_ratio_for_regime(regime: Optional[str]) -> float:
    if not regime:
        return MIN_SCORE_RATIO
    r = regime.upper()
    if "BEAR" in r or "VOL" in r:
        return MIN_SCORE_RATIO_BEAR
    return MIN_SCORE_RATIO

_MTF_BLOCK_SCORE_OVERRIDE = int(os.getenv("MTF_BLOCK_SCORE_OVERRIDE", "999"))

_ANALYZE_PAIR_CONCURRENCY = int(os.getenv("ANALYZE_PAIR_CONCURRENCY", "6"))
_analyze_pair_sem: Optional[asyncio.Semaphore] = None
_analyze_pair_sem_loop: Optional[asyncio.AbstractEventLoop] = None

def _get_analyze_sem() -> asyncio.Semaphore:
    global _analyze_pair_sem, _analyze_pair_sem_loop
    try:
        current_loop = asyncio.get_event_loop()
    except RuntimeError:
        current_loop = None
    if _analyze_pair_sem is None or _analyze_pair_sem_loop is not current_loop:
        _analyze_pair_sem = asyncio.Semaphore(_ANALYZE_PAIR_CONCURRENCY)
        _analyze_pair_sem_loop = current_loop
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
_SL_STRUCTURE_MAX_DIST_PCT = float(os.getenv("SL_STRUCTURE_MAX_DIST_PCT", "4.0").replace("%", "").strip()) / 100.0
_VOL_AVG_WINDOW    = int(os.getenv("VOL_AVG_WINDOW", "20"))
_VOL_SIGNAL_MIN    = float(os.getenv("VOL_SIGNAL_MIN", "1.0"))
_FUNDING_LONG_MAX  = float(os.getenv("FUNDING_LONG_MAX",  "0.0005"))
_FUNDING_SHORT_MIN = float(os.getenv("FUNDING_SHORT_MIN", "-0.0005"))
_EMA_SPREAD_TREND_MIN  = float(os.getenv("EMA_SPREAD_TREND_MIN",  "0.002"))
_EMA_SPREAD_RANGE_MAX  = float(os.getenv("EMA_SPREAD_RANGE_MAX",  "0.0015"))
_BREAKOUT_WINDOW       = int(os.getenv("BREAKOUT_WINDOW", "20"))
_BREAKOUT_VOL_MIN      = float(os.getenv("BREAKOUT_VOL_MIN",  "1.4"))
_BREAKOUT_VOL_MIN_HARD = float(os.getenv("BREAKOUT_VOL_MIN_HARD", "1.2"))
_BREAKOUT_ATR_CONFIRM  = float(os.getenv("BREAKOUT_ATR_CONFIRM", "0.3"))
_BREAKOUT_SQUEEZE_PCT  = float(os.getenv("BREAKOUT_SQUEEZE_PCT", "40"))
_BREAKOUT_RETEST_TOL   = float(os.getenv("BREAKOUT_RETEST_TOL", "0.005"))
_REVERSAL_RSI_LOW      = float(os.getenv("REVERSAL_RSI_LOW",  "25"))
_REVERSAL_RSI_HIGH     = float(os.getenv("REVERSAL_RSI_HIGH", "75"))
_VOL_MIN_GLOBAL        = float(os.getenv("VOL_MIN_GLOBAL",    "0.6"))
_VOL_CONFIRM_MIN       = float(os.getenv("VOL_CONFIRM_MIN",   "1.2"))
_PULLBACK_LOOKBACK     = int(os.getenv("PULLBACK_LOOKBACK", "2"))
_PULLBACK_TOLERANCE    = float(os.getenv("PULLBACK_TOLERANCE", "0.005"))
_EARLY_LEV_FACTOR      = float(os.getenv("EARLY_LEV_FACTOR", "0.2"))
_DOJI_BODY_MIN_PCT     = float(os.getenv("DOJI_BODY_MIN_PCT", "0.20"))
_ADX_MIN               = float(os.getenv("ADX_MIN", "20.0"))
_REVERSAL_SWING_TOL    = float(os.getenv("REVERSAL_SWING_TOL", "0.005"))
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

def _bar_val(b, idx: int, key: str):
    if isinstance(b, dict):
        return b[key]
    return b[idx]

def _b_ts(b):    return _bar_val(b, 0, "timestamp")
def _b_open(b):  return _bar_val(b, 1, "open")
def _b_high(b):  return _bar_val(b, 2, "high")
def _b_low(b):   return _bar_val(b, 3, "low")
def _b_close(b): return _bar_val(b, 4, "close")
def _b_vol(b):   return _bar_val(b, 5, "volume")

def _normalize_bar(b) -> list:
    if isinstance(b, dict):
        return [
            b.get("timestamp", b.get("ts", 0)),
            b["open"], b["high"], b["low"], b["close"], b["volume"],
        ]
    return list(b)

def _clean_bars(bars: list) -> list:
    cleaned = []
    for b in (bars or []):
        if b is None:
            continue
        nb = _normalize_bar(b)
        if all(v is not None for v in nb):
            cleaned.append(nb)
    return cleaned

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

def _mtf_bias(ind_1h: dict) -> Optional[str]:
    if not ind_1h:
        return None
    if ind_1h.get("ema_bull"):
        return "LONG"
    if ind_1h.get("ema_bear"):
        return "SHORT"
    return None

def _adx_simple(highs: list, lows: list, closes: list, period: int = 14) -> float:
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

def _ema_slope(ema_series: list, lookback: int = 3) -> float:
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
        "_closes_15m": [float(_b_close(b)) for b in bars_15m[-5:]],
    }

    vol_ratio_15m = ind_15m.get("vol_ratio", 1.0)
    if vol_ratio_15m < _VOL_MIN_GLOBAL:
        return _hold_result(symbol, f"Vol={vol_ratio_15m:.2f}x — mercado dormido (min {_VOL_MIN_GLOBAL}x)")

    if len(bars_15m) >= _VOL_AVG_WINDOW + 2:
        vol_last    = float(_b_vol(bars_15m[-2]))
        vol_avg_ref = sum(float(_b_vol(b)) for b in bars_15m[-_VOL_AVG_WINDOW - 2:-2]) / _VOL_AVG_WINDOW
        vol_signal  = round(vol_last / vol_avg_ref, 3) if vol_avg_ref > 0 else 1.0
        if vol_signal < _VOL_SIGNAL_MIN:
            return _hold_result(symbol, f"Vol señal {vol_signal:.2f}x < {_VOL_SIGNAL_MIN}x (vela sin convicción)")
        log.debug("[signal_engine] %s vol_signal=%.2fx (min %.1fx)", symbol, vol_signal, _VOL_SIGNAL_MIN)

    setup_type, signal_str, score, max_score, reasons = _detect_setup(
        ind_15m, ind_1h, ind_4h, bars_15m, bars_1h, regime
    )

    if signal_str == "NEUTRAL" or setup_type is None:
        return _hold_result(symbol, f"NEUTRAL ({', '.join(reasons[-3:])})", max_score=max_score)

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
    close_price = float(_b_close(last_bar))
    high_price  = float(_b_high(last_bar))
    low_price   = float(_b_low(last_bar))
    open_price  = float(_b_open(last_bar))
    entry = close_price

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

    vol_ratio_now = ind_15m.get("vol_ratio", 1.0)
    tp_vol_scale = 1.0
    if vol_ratio_now >= _TP_VOL_HIGH_THRESHOLD:
        tp_vol_scale = _TP_VOL_HIGH_MULT
        log.debug(
            "[signal_engine] %s TP expandido (vol_ratio=%.2fx >= %.1fx) scale=%.2f",
            symbol, vol_ratio_now, _TP_VOL_HIGH_THRESHOLD, tp_vol_scale,
        )
    elif vol_ratio_now < _TP_VOL_LOW_THRESHOLD:
        tp_vol_scale = None
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

    effective_min_ratio = _min_score_ratio_for_regime(regime)

    is_fast_valid = (
        entry_mode in ("FAST", "STRONG")
        and score >= _FAST_ENTRY_MIN_SCORE
        and rr >= _FAST_ENTRY_MIN_RR
    )

    score_ratio = score / max_score if max_score > 0 else 0.0
    ratio_ok = score_ratio >= effective_min_ratio

    is_valid = (score >= MIN_SCORE and rr >= effective_min_rr and ratio_ok) or is_fast_valid

    _tp_note = (
        f"TP×{tp_vol_scale:.2f}" if tp_vol_scale and tp_vol_scale != 1.0
        else ("TP1×{:.2f}".format(_TP_VOL_LOW_MULT) if tp_vol_scale is None else "TP=std")
    )

    log.info(
        "[signal_engine] %s %s [%s] score=%.2f/%d ratio=%.2f(min=%.2f) RR=%.2f(min=%.2f) "
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
        score=int(round(score)),
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
            f"[{setup_type}] score={score:.2f}/{max_score} ratio={score_ratio:.2f}(min={effective_min_ratio:.2f}) "
            f"rr={rr:.2f}(min_rr={effective_min_rr:.2f} regime={regime or 'none'})"
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
    bars_1h: list = None,
    regime: Optional[str] = None,
) -> Tuple[Optional[str], str, int, int, List[str]]:
    effective_ratio = _min_score_ratio_for_regime(regime)
    candidates = []
    for mode_fn in (_score_tendencia, _score_breakout, _score_reversal):
        if mode_fn == _score_tendencia:
            setup_type, signal_str, score, max_score, reasons = mode_fn(
                i15, i1h, i4h, bars_15m, bars_1h or []
            )
        else:
            setup_type, signal_str, score, max_score, reasons = mode_fn(i15, i1h, i4h, bars_15m)
        score_ratio = score / max_score if max_score > 0 else 0.0
        if signal_str != "NEUTRAL" and score >= MIN_SCORE and score_ratio >= effective_ratio:
            candidates.append((setup_type, signal_str, score, max_score, reasons))
    if not candidates:
        return None, "NEUTRAL", 0, MAX_SCORE_NEUTRAL, [
            f"Ningún setup alcanzó MIN_SCORE o MIN_SCORE_RATIO(regime={regime or 'none'})"
        ]
    best = max(candidates, key=lambda x: x[2] / x[3])
    if len(candidates) > 1:
        log.debug(
            "[signal_engine] %d setups válidos — elegido %s (%.2f/%d=%.2f) sobre %s",
            len(candidates), best[0], best[2], best[3], best[2] / best[3],
            ", ".join(f"{c[0]}({c[2]:.2f}/{c[3]}={c[2]/c[3]:.2f})" for c in candidates if c is not best),
        )
    return best

def _score_tendencia(
    i15: dict, i1h: dict, i4h: dict, bars_15m: list,
    bars_1h: list = None,
) -> Tuple[str, str, float, int, List[str]]:
    MAX = 15
    reasons: List[str] = []

    # --- EARLY-EXIT DIAGNOSTICS ---
    if not i1h:
        log.info("[signal_engine] TENDENCIA early-exit: sin datos 1h")
        return "TENDENCIA", "NEUTRAL", 0, MAX, ["Sin datos 1h"]

    ema21_1h = i1h.get("ema21")
    ema50_1h = i1h.get("ema50")
    if not ema21_1h or not ema50_1h or ema50_1h == 0:
        log.info(
            "[signal_engine] TENDENCIA early-exit: EMA 1h no calculada (ema21=%s ema50=%s)",
            ema21_1h, ema50_1h,
        )
        return "TENDENCIA", "NEUTRAL", 0, MAX, ["EMA 1h no calculada"]

    ema_spread_1h = abs(ema21_1h - ema50_1h) / ema50_1h
    if ema_spread_1h < _EMA_SPREAD_RANGE_MAX:
        log.info(
            "[signal_engine] TENDENCIA early-exit: mercado en rango "
            "(spread EMA 1h=%.4f%% < umbral=%.4f%%)",
            ema_spread_1h * 100, _EMA_SPREAD_RANGE_MAX * 100,
        )
        return "TENDENCIA", "NEUTRAL", 0, MAX, [f"Mercado en rango (spread EMA 1h={ema_spread_1h*100:.2f}%)"]

    trend_1h_up   = i1h.get("ema_bull", False)
    trend_1h_down = i1h.get("ema_bear", False)
    if not trend_1h_up and not trend_1h_down:
        log.info(
            "[signal_engine] TENDENCIA early-exit: sin tendencia definida en 1h "
            "(ema21=%.6f ema50=%.6f spread=%.4f%%)",
            ema21_1h, ema50_1h, ema_spread_1h * 100,
        )
        return "TENDENCIA", "NEUTRAL", 0, MAX, ["Sin tendencia definida en 1h"]

    direction = "LONG" if trend_1h_up else "SHORT"
    log.debug(
        "[signal_engine] _score_tendencia direction=%s trend_1h_up=%s trend_1h_down=%s "
        "ema21_1h=%.4f ema50_1h=%.4f",
        direction, trend_1h_up, trend_1h_down, ema21_1h, ema50_1h,
    )

    macd_ok = (direction == "LONG" and i15.get("macd_bull")) or (direction == "SHORT" and i15.get("macd_bear"))
    if not macd_ok:
        log.info(
            "[signal_engine] TENDENCIA early-exit: MACD15m en contra de %s "
            "(macd_bull=%s macd_bear=%s)",
            direction, i15.get("macd_bull"), i15.get("macd_bear"),
        )
        reasons.append(f"MACD15m en contra de {direction} — requisito obligatorio")
        return "TENDENCIA", "NEUTRAL", 0, MAX, reasons

    st1h_ok = (direction == "LONG" and i1h.get("st_bull")) or (direction == "SHORT" and i1h.get("st_bear"))
    if not st1h_ok:
        log.info(
            "[signal_engine] TENDENCIA early-exit: ST1h en contra de %s "
            "(st_bull=%s st_bear=%s)",
            direction, i1h.get("st_bull"), i1h.get("st_bear"),
        )
        reasons.append(f"ST1h en contra de {direction} — requisito obligatorio")
        return "TENDENCIA", "NEUTRAL", 0, MAX, reasons

    score = 0.0

    ema_15m_ok = (direction == "LONG" and i15.get("ema_bull")) or (direction == "SHORT" and i15.get("ema_bear"))
    if ema_15m_ok:
        w = _WT["EMA_ALIGN_FULL"]
        score += w
        reasons.append(f"EMA15m+1h alineados {direction} (spread={ema_spread_1h*100:.2f}%) +{w:.2f}")
    else:
        w = _WT["EMA_ALIGN_PARTIAL"]
        score += w
        reasons.append(f"EMA1h en {direction} pero 15m aun no (spread={ema_spread_1h*100:.2f}%) +{w:.2f}")

    w = _WT["ST_1H"]
    score += w
    reasons.append(f"ST1h en favor +{w:.2f}")

    st4h_ok = False
    macd4h_ok = False
    if i4h:
        st4h_ok = (direction == "LONG" and i4h.get("st_bull")) or (direction == "SHORT" and i4h.get("st_bear"))
        macd4h_ok = (direction == "LONG" and i4h.get("macd_bull")) or (direction == "SHORT" and i4h.get("macd_bear"))
        if st4h_ok:
            w_st4 = _WT["ST_4H"]
            score += w_st4
            reasons.append(f"ST4h en favor +{w_st4:.2f}")
            if macd4h_ok:
                w_conf = _WT["MACD_4H_CONF"]
                score += w_conf
                reasons.append(f"MACD4h + ST4h alineados — confluencia institucional total +{w_conf:.2f}")
            else:
                reasons.append("ST4h OK pero MACD4h no confirma (sin bonus institucional)")
        else:
            w_pen = _WT["ST_4H_PENALTY"]
            score = max(0.0, score - w_pen)
            reasons.append(f"ST4h en contra — penalización -{w_pen:.2f}")
    else:
        reasons.append("ST4h sin datos")

    w = _WT["MACD_15M"]
    score += w
    reasons.append(f"MACD15m en favor +{w:.2f}")

    closes_15m = [float(_b_close(b)) for b in bars_15m]
    highs_15m  = [float(_b_high(b))  for b in bars_15m]
    lows_15m   = [float(_b_low(b))   for b in bars_15m]
    adx_val = _adx_simple(highs_15m, lows_15m, closes_15m, 14)
    ema21_series = i15.get("_ema21_series", [])

    if ema21_series:
        slope = _ema_slope(ema21_series, lookback=3)
        slope_ok = (direction == "LONG" and slope > 0) or (direction == "SHORT" and slope < 0)

        if adx_val >= _ADX_MIN and slope_ok:
            reasons.append(f"ADX15m={adx_val:.1f}(≥{_ADX_MIN}) + slope EMA21={slope*100:.3f}% — tendencia real confirmada")
        elif adx_val < _ADX_MIN:
            w_pen = _WT["ADX_PENALTY"]
            score = max(0.0, score - w_pen)
            reasons.append(f"ADX15m={adx_val:.1f} < {_ADX_MIN} — rango disfrazado de tendencia — penalización -{w_pen:.2f}")
        else:
            w_pen = _WT["ADX_SLOPE_PENALTY"]
            score = max(0.0, score - w_pen)
            reasons.append(f"Slope EMA21={slope*100:.3f}% en contra de {direction} — penalización -{w_pen:.2f}")
    else:
        reasons.append("EMA21 series no disponible — slope omitido")
        if adx_val < _ADX_MIN:
            w_pen = _WT["ADX_PENALTY"]
            score = max(0.0, score - w_pen)
            reasons.append(f"ADX15m={adx_val:.1f} < {_ADX_MIN} — rango disfrazado de tendencia — penalización -{w_pen:.2f}")
        else:
            reasons.append(f"ADX15m={adx_val:.1f}(≥{_ADX_MIN}) — tendencia confirmada por ADX (slope no disponible)")

    if bars_1h and len(bars_1h) >= 16:
        highs_1h  = [float(_b_high(b))  for b in bars_1h]
        lows_1h   = [float(_b_low(b))   for b in bars_1h]
        closes_1h = [float(_b_close(b)) for b in bars_1h]
        adx_1h = _adx_simple(highs_1h, lows_1h, closes_1h, 14)
        if adx_1h > 0:
            if adx_1h < _ADX_MIN:
                w_pen = _WT["ADX1H_PENALTY"]
                score = max(0.0, score - w_pen)
                reasons.append(f"ADX1h={adx_1h:.1f} < {_ADX_MIN} — tendencia 1h débil — penalización -{w_pen:.2f}")
            else:
                reasons.append(f"ADX1h={adx_1h:.1f}(≥{_ADX_MIN}) — tendencia 1h confirmada")
        else:
            reasons.append("ADX1h no calculable (pocos datos)")
    else:
        reasons.append("ADX1h omitido (sin datos 1h suficientes)")

    ema21_15m = i15.get("ema21")
    close_15m = i15.get("close", 0)
    pullback_detected = False
    pullback_vol_low  = False
    if ema21_15m and close_15m:
        recent_bars = bars_15m[-(_PULLBACK_LOOKBACK + 1):-1]
        for bar in recent_bars:
            bar_low  = float(_b_low(bar))
            bar_high = float(_b_high(bar))
            bar_vol  = float(_b_vol(bar))
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
            w = _WT["PULLBACK"]
            score += w
            reasons.append(f"Pullback a EMA21_15m +{w:.2f}")
            if pullback_vol_low:
                wv = _WT["PULLBACK_VOL"]
                score += wv
                reasons.append(f"Pullback con volumen bajo (corrección sana) +{wv:.2f}")
        else:
            reasons.append("Sin pullback a EMA21_15m")

    rsi_15m = i15.get("rsi_val")
    if rsi_15m is not None:
        rsi_ok = 35 <= rsi_15m <= 65
        if rsi_ok:
            w = _WT["RSI"]
            score += w
            reasons.append(f"RSI15m={rsi_15m:.0f} zona rebote +{w:.2f}")
        elif (direction == "LONG" and rsi_15m > 72) or (direction == "SHORT" and rsi_15m < 28):
            reasons.append(f"RSI15m={rsi_15m:.0f} SOBREEXTENDIDO — filtro duro")
            score = 0.0
        else:
            reasons.append(f"RSI15m={rsi_15m:.0f} zona neutra")

    vol_ratio = i15.get("vol_ratio", 1.0)
    if vol_ratio >= _VOL_CONFIRM_MIN:
        w = _WT["VOLUME"]
        score += w
        reasons.append(f"Vol15m={vol_ratio:.1f}x confirma +{w:.2f}")
    elif vol_ratio >= 1.0:
        reasons.append(f"Vol15m={vol_ratio:.1f}x aceptable")
    elif vol_ratio < 0.8:
        w_pen = _WT["VOLUME_PENALTY"]
        score = max(0.0, score - w_pen)
        reasons.append(f"Vol15m={vol_ratio:.1f}x muy débil — penalización -{w_pen:.2f}")
    else:
        reasons.append(f"Vol15m={vol_ratio:.1f}x débil")

    st15m_ok = (direction == "LONG" and i15.get("st_bull")) or (direction == "SHORT" and i15.get("st_bear"))
    confluencia = st15m_ok and st1h_ok and (not i4h or st4h_ok)
    if confluencia:
        w = _WT["ST_CONFLUENCE"]
        score += w
        reasons.append(f"Confluencia total ST 15m+1h+4h +{w:.2f}")
    else:
        reasons.append(f"Confluencia ST parcial (15m={st15m_ok} 1h={st1h_ok} 4h={st4h_ok})")

    vwap_val = i15.get("vwap", 0.0)
    if vwap_val and vwap_val > 0 and close_15m:
        vwap_ok = (direction == "LONG" and close_15m > vwap_val) or (direction == "SHORT" and close_15m < vwap_val)
        if vwap_ok:
            w = _WT["VWAP"]
            score += w
            reasons.append(f"Precio {'>' if direction == 'LONG' else '<'} VWAP_diario({vwap_val:.4f}) +{w:.2f}")
        else:
            w_pen = _WT["VWAP_PENALTY"]
            score = max(0.0, score - w_pen)
            reasons.append(f"Precio al lado equivocado del VWAP_diario({vwap_val:.4f}) — penalización -{w_pen:.2f}")
    else:
        reasons.append("VWAP diario no disponible")

    if len(bars_15m) >= 4:
        closes_recent = [float(_b_close(b)) for b in bars_15m[-4:]]
        if direction == "LONG":
            hh_hl = closes_recent[-1] > closes_recent[-2] > closes_recent[-3]
            if hh_hl:
                w = _WT["STRUCTURE"]
                score += w
                reasons.append(f"Estructura HH/HL confirmada en 15m +{w:.2f}")
            else:
                reasons.append("Sin estructura HH/HL en 15m")
        else:
            ll_lh = closes_recent[-1] < closes_recent[-2] < closes_recent[-3]
            if ll_lh:
                w = _WT["STRUCTURE"]
                score += w
                reasons.append(f"Estructura LL/LH confirmada en 15m +{w:.2f}")
            else:
                reasons.append("Sin estructura LL/LH en 15m")

    # --- LOG DE DIAGNÓSTICO TENDENCIA ---
    st15m_diag = i15.get("st_bull") if direction == "LONG" else i15.get("st_bear")
    adx_diag   = _adx_simple(
        [float(_b_high(b)) for b in bars_15m],
        [float(_b_low(b))  for b in bars_15m],
        [float(_b_close(b)) for b in bars_15m],
        14,
    )
    log.info(
        "[%s] EVAL TENDENCIA(%s) → score=%.2f/%d | "
        "EMA15m=%s | ST1h=%s | ST4h=%s | MACD15m=%s | MACD4h=%s | "
        "RSI=%.1f | Vol=%.2fx | ADX=%.1f | VWAP=%s | Pullback=%s | "
        "umbral=%d | ratio=%.2f(min=%.2f)",
        _score_tendencia.__module__ if hasattr(_score_tendencia, "__module__") else "signal_engine",
        direction,
        score, MAX,
        "✅" if ema_15m_ok  else "❌",
        "✅" if st1h_ok     else "❌",
        "✅" if st4h_ok     else "❌",
        "✅" if macd_ok     else "❌",
        "✅" if macd4h_ok   else "❌",
        i15.get("rsi_val") or 0.0,
        i15.get("vol_ratio", 1.0),
        adx_diag,
        "✅" if (vwap_val and close_15m and ((direction == "LONG" and close_15m > vwap_val) or (direction == "SHORT" and close_15m < vwap_val))) else "❌",
        "✅" if pullback_detected else "❌",
        MIN_SCORE,
        score / MAX if MAX > 0 else 0.0,
        _min_score_ratio_for_regime(None),
    )

    return "TENDENCIA", direction, score, MAX, reasons

def _score_breakout(i15: dict, i1h: dict, i4h: dict, bars_15m: list) -> Tuple[str, str, float, int, List[str]]:
    MAX = 10
    reasons: List[str] = []
    if len(bars_15m) < _BREAKOUT_WINDOW + 2:
        return "BREAKOUT", "NEUTRAL", 0, MAX, ["Velas insuficientes para breakout"]

    vol_ratio = i15.get("vol_ratio", 1.0)
    if vol_ratio < _BREAKOUT_VOL_MIN_HARD:
        return "BREAKOUT", "NEUTRAL", 0, MAX, [
            f"Vol={vol_ratio:.2f}x < {_BREAKOUT_VOL_MIN_HARD}x — breakout sin liquidez suficiente"
        ]

    window = bars_15m[-(_BREAKOUT_WINDOW + 1):-1]
    range_high = max(float(_b_high(b)) for b in window)
    range_low  = min(float(_b_low(b))  for b in window)
    current_close = float(_b_close(bars_15m[-1]))
    atr_val = float(i15.get("atr", 0) or 0)
    breakout_pad = atr_val * _BREAKOUT_ATR_CONFIRM
    broke_up   = current_close > (range_high + breakout_pad)
    broke_down = current_close < (range_low  - breakout_pad)

    retest_up   = (not broke_up and not broke_down and
                   abs(current_close - range_high) / range_high <= _BREAKOUT_RETEST_TOL and
                   current_close >= range_high * (1 - _BREAKOUT_RETEST_TOL))
    retest_down = (not broke_up and not broke_down and
                   abs(current_close - range_low) / range_low <= _BREAKOUT_RETEST_TOL and
                   current_close <= range_low * (1 + _BREAKOUT_RETEST_TOL))

    if not broke_up and not broke_down and not retest_up and not retest_down:
        # --- LOG DE DIAGNÓSTICO BREAKOUT (sin rotura) ---
        rsi_val = i15.get("rsi_val") or 0.0
        st1h_ok = (i1h.get("st_bull") or i1h.get("st_bear")) if i1h else False
        st4h_ok = (i4h.get("st_bull") or i4h.get("st_bear")) if i4h else False
        macd_1h_ok = (i1h.get("macd_bull") or i1h.get("macd_bear")) if i1h else False
        log.info(
            "[signal_engine] EVAL BREAKOUT → score=0/%d | "
            "BrokeUp=%s | BrokeDown=%s | RetestUp=%s | RetestDown=%s | "
            "close=%.6f range=[%.6f-%.6f] pad=%.6f | "
            "Vol=%.2fx(min=%.1fx) | ST1h=%s | ST4h=%s | RSI=%.1f | MACD1h=%s | umbral=%d",
            MAX,
            "✅" if broke_up    else "❌",
            "✅" if broke_down  else "❌",
            "✅" if retest_up   else "❌",
            "✅" if retest_down else "❌",
            current_close, range_low, range_high, breakout_pad,
            vol_ratio, _BREAKOUT_VOL_MIN,
            "✅" if st1h_ok   else "❌",
            "✅" if st4h_ok   else "❌",
            rsi_val,
            "✅" if macd_1h_ok else "❌",
            MIN_SCORE,
        )
        return "BREAKOUT", "NEUTRAL", 0, MAX, [
            f"Sin rotura ni retesteo: close={current_close:.4f} rango=[{range_low:.4f}-{range_high:.4f}]"
        ]

    if atr_val > 0 and len(bars_15m) >= _BREAKOUT_WINDOW * 2:
        hist_highs  = [float(_b_high(b))  for b in bars_15m[-(_BREAKOUT_WINDOW * 2):-_BREAKOUT_WINDOW]]
        hist_lows   = [float(_b_low(b))   for b in bars_15m[-(_BREAKOUT_WINDOW * 2):-_BREAKOUT_WINDOW]]
        hist_closes = [float(_b_close(b)) for b in bars_15m[-(_BREAKOUT_WINDOW * 2):-_BREAKOUT_WINDOW]]
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

    if broke_up or retest_up:
        direction = "LONG"
    else:
        direction = "SHORT"

    is_retest = retest_up or retest_down

    score = float(_WB["BASE"])
    if is_retest:
        w = _WB["RETEST"]
        score += w
        reasons.append(
            f"Retesteo del nivel {'superior' if retest_up else 'inferior'} "
            f"(close={current_close:.4f} ≈ {range_high if retest_up else range_low:.4f}) "
            f"+{w:.2f} bonus (score base={_WB['BASE']:.2f}, score total={score:.2f})"
        )
    else:
        reasons.append(f"Ruptura {'alcista' if broke_up else 'bajista'} confirmada +{_WB['BASE']:.2f}")

    if vol_ratio >= _BREAKOUT_VOL_MIN:
        w = _WB["VOL_HIGH"]
        score += w
        reasons.append(f"Vol={vol_ratio:.1f}x breakout +{w:.2f}")
    elif vol_ratio >= 1.1:
        w = _WB["VOL_MID"]
        score += w
        reasons.append(f"Vol={vol_ratio:.1f}x moderado +{w:.2f}")
    else:
        reasons.append(f"Vol={vol_ratio:.1f}x aceptable (superó mínimo duro de {_BREAKOUT_VOL_MIN_HARD}x)")

    st1h_ok = False
    st4h_ok = False
    macd_1h_ok = False

    if i1h:
        st1h_ok = (direction == "LONG" and i1h.get("st_bull")) or (direction == "SHORT" and i1h.get("st_bear"))
        if st1h_ok:
            w = _WB["ST_1H"]
            score += w
            reasons.append(f"ST1h confirma +{w:.2f}")
        else:
            reasons.append("ST1h no confirma")
    if i4h:
        st4h_ok = (direction == "LONG" and i4h.get("st_bull")) or (direction == "SHORT" and i4h.get("st_bear"))
        if st4h_ok:
            w = _WB["ST_4H"]
            score += w
            reasons.append(f"ST4h confirma +{w:.2f}")
        else:
            reasons.append("ST4h no confirma")
    rsi_15m = i15.get("rsi_val")
    rsi_ok = False
    if rsi_15m is not None:
        rsi_ok = (direction == "LONG" and 45 <= rsi_15m <= 70) or (direction == "SHORT" and 30 <= rsi_15m <= 55)
        if rsi_ok:
            w = _WB["RSI"]
            score += w
            reasons.append(f"RSI15m={rsi_15m:.0f} razonable +{w:.2f}")
        else:
            reasons.append(f"RSI15m={rsi_15m:.0f} sobreextendido")
    if i1h:
        macd_1h_ok = (direction == "LONG" and i1h.get("macd_bull")) or (direction == "SHORT" and i1h.get("macd_bear"))
        if macd_1h_ok:
            w = _WB["MACD_1H"]
            score += w
            reasons.append(f"MACD1h en favor +{w:.2f}")
        else:
            reasons.append("MACD1h en contra")

    # --- LOG DE DIAGNÓSTICO BREAKOUT (con rotura/retest) ---
    log.info(
        "[signal_engine] EVAL BREAKOUT(%s) → score=%.2f/%d | "
        "BrokeUp=%s | BrokeDown=%s | Retest=%s | "
        "Vol=%.2fx(min=%.1fx) | ST1h=%s | ST4h=%s | RSI=%.1f(%s) | MACD1h=%s | "
        "umbral=%d | ratio=%.2f(min=%.2f)",
        direction, score, MAX,
        "✅" if broke_up   else "❌",
        "✅" if broke_down else "❌",
        "✅" if is_retest  else "❌",
        vol_ratio, _BREAKOUT_VOL_MIN,
        "✅" if st1h_ok    else "❌",
        "✅" if st4h_ok    else "❌",
        rsi_15m or 0.0,
        "✅" if rsi_ok     else "❌",
        "✅" if macd_1h_ok else "❌",
        MIN_SCORE,
        score / MAX if MAX > 0 else 0.0,
        _min_score_ratio_for_regime(None),
    )

    return "BREAKOUT", direction, score, MAX, reasons

def _score_reversal(i15: dict, i1h: dict, i4h: dict, bars_15m: list) -> Tuple[str, str, float, int, List[str]]:
    MAX = 14
    reasons: List[str] = []
    rsi_1h = i1h.get("rsi_val") if i1h else None
    if rsi_1h is None:
        return "REVERSAL", "NEUTRAL", 0, MAX, ["Sin datos 1h"]
    is_long  = rsi_1h <= _REVERSAL_RSI_LOW
    is_short = rsi_1h >= _REVERSAL_RSI_HIGH
    if not is_long and not is_short:
        # --- LOG DE DIAGNÓSTICO REVERSAL (RSI no extremo) ---
        log.info(
            "[signal_engine] EVAL REVERSAL → score=0/%d | "
            "RSI1h=%.1f (umbral: LOW≤%.0f HIGH≥%.0f) | ST1h=%s | Div=%s | umbral=%d",
            MAX,
            rsi_1h, _REVERSAL_RSI_LOW, _REVERSAL_RSI_HIGH,
            "✅" if (i1h and (i1h.get("st_bull") or i1h.get("st_bear"))) else "❌",
            "❌",
            MIN_SCORE,
        )
        return "REVERSAL", "NEUTRAL", 0, MAX, [f"RSI1h={rsi_1h:.0f} no es extremo (umbral {_REVERSAL_RSI_LOW}/{_REVERSAL_RSI_HIGH})"]
    direction = "LONG" if is_long else "SHORT"

    if i1h:
        st1h_ok = (direction == "LONG" and i1h.get("st_bull")) or (direction == "SHORT" and i1h.get("st_bear"))
        if not st1h_ok:
            reasons.append(f"ST1h en contra de {direction} — requisito obligatorio")
            log.info(
                "[signal_engine] EVAL REVERSAL(%s) → score=0/%d | "
                "RSI1h=%.1f ✅ | ST1h=❌ | umbral=%d",
                direction, MAX, rsi_1h, MIN_SCORE,
            )
            return "REVERSAL", "NEUTRAL", 0, MAX, reasons
    else:
        reasons.append("ST1h no disponible — requisito obligatorio")
        return "REVERSAL", "NEUTRAL", 0, MAX, reasons

    if direction == "LONG":
        has_div = i15.get("rsi_bull_div", False)
    else:
        has_div = i15.get("rsi_bear_div", False)

    if not has_div:
        reasons.append(f"Divergencia RSI no confirmada para {direction} — requisito obligatorio")
        log.info(
            "[signal_engine] EVAL REVERSAL(%s) → score=0/%d | "
            "RSI1h=%.1f ✅ | ST1h=✅ | Div=❌ | umbral=%d",
            direction, MAX, rsi_1h, MIN_SCORE,
        )
        return "REVERSAL", "NEUTRAL", 0, MAX, reasons

    score = float(_WR["BASE"])
    reasons.append(f"RSI1h={rsi_1h:.0f} extremo + ST1h alineado (score base={_WR['BASE']:.2f})")

    try:
        from bot.structure_analyzer import analyze_structure, STRUCTURE_SWING_N
        if len(bars_15m) >= 30:
            df = _bars_to_df(bars_15m)
            swing_dir = -1 if direction == "LONG" else 1
            struct = analyze_structure(df, swing_dir)
            current_price = float(_b_close(bars_15m[-1]))
            if direction == "LONG":
                swing_level = struct.get("last_sl", 0.0)
            else:
                swing_level = struct.get("last_sh", 0.0)
            if swing_level > 0:
                dist_pct = abs(current_price - swing_level) / swing_level
                if dist_pct <= _REVERSAL_SWING_TOL:
                    w = _WR["SWING"]
                    score += w
                    reasons.append(f"Swing level {swing_level:.4f} cerca (dist={dist_pct*100:.2f}%) +{w:.2f}")
                else:
                    reasons.append(f"Swing level lejano (dist={dist_pct*100:.2f}%)")
            else:
                reasons.append("Sin swing level reciente")
    except Exception as e:
        log.debug("[signal_engine] _score_reversal swing level error: %s", e)
        reasons.append("Swing levels no disponible")

    ema50_1h = i1h.get("ema50")
    ema50_ok = False
    if ema50_1h:
        current_price = float(_b_close(bars_15m[-1]))
        dist_pct = abs(current_price - ema50_1h) / ema50_1h
        if dist_pct <= 0.003:
            ema50_ok = True
            w = _WR["EMA50"]
            score += w
            reasons.append(f"Precio cerca de EMA50_1h ({ema50_1h:.4f}) +{w:.2f}")
        else:
            reasons.append(f"Precio lejos de EMA50_1h (dist={dist_pct*100:.2f}%)")
    else:
        reasons.append("EMA50_1h no disponible")

    macd_ok = False
    if i1h:
        macd_ok = (direction == "LONG" and i1h.get("macd_bull")) or (direction == "SHORT" and i1h.get("macd_bear"))
        if macd_ok:
            w = _WR["MACD"]
            score += w
            reasons.append(f"MACD1h en favor +{w:.2f}")
        else:
            reasons.append("MACD1h en contra")

    vol_ratio = i15.get("vol_ratio", 1.0)
    vol_ok = vol_ratio >= _VOL_CONFIRM_MIN
    if vol_ok:
        w = _WR["VOL"]
        score += w
        reasons.append(f"Vol={vol_ratio:.1f}x confirma +{w:.2f}")
    else:
        reasons.append(f"Vol={vol_ratio:.1f}x bajo")

    rsi_15m = i15.get("rsi_val")
    rsi_15m_ok = False
    if rsi_15m is not None:
        rsi_15m_ok = (direction == "LONG" and rsi_15m < 40) or (direction == "SHORT" and rsi_15m > 60)
        if rsi_15m_ok:
            w = _WR["RSI"]
            score += w
            reasons.append(f"RSI15m={rsi_15m:.0f} alineado +{w:.2f}")
        else:
            reasons.append(f"RSI15m={rsi_15m:.0f} no extremo")

    vwap_val = i15.get("vwap", 0.0)
    vwap_ok = False
    if vwap_val and vwap_val > 0:
        current_price = float(_b_close(bars_15m[-1]))
        vwap_ok = (direction == "LONG" and current_price < vwap_val) or (direction == "SHORT" and current_price > vwap_val)
        if vwap_ok:
            w = _WR["VWAP"]
            score += w
            reasons.append(f"Precio del lado correcto de VWAP ({vwap_val:.4f}) +{w:.2f}")
        else:
            reasons.append("Precio del lado equivocado de VWAP — sin penalización")

    st4h_ok = False
    if i4h:
        st4h_ok = (direction == "LONG" and i4h.get("st_bull")) or (direction == "SHORT" and i4h.get("st_bear"))
        if st4h_ok:
            w = _WR["ST_4H"]
            score += w
            reasons.append(f"ST4h en favor +{w:.2f}")
        else:
            reasons.append("ST4h en contra (sin penalización)")

    # --- LOG DE DIAGNÓSTICO REVERSAL ---
    log.info(
        "[signal_engine] EVAL REVERSAL(%s) → score=%.2f/%d | "
        "RSI1h=%.1f ✅ | ST1h=✅ | Div=✅ | EMA50=%s | MACD1h=%s | "
        "Vol=%.2fx(%s) | RSI15m=%.1f(%s) | VWAP=%s | ST4h=%s | "
        "umbral=%d | ratio=%.2f(min=%.2f)",
        direction, score, MAX,
        rsi_1h,
        "✅" if ema50_ok    else "❌",
        "✅" if macd_ok     else "❌",
        vol_ratio,
        "✅" if vol_ok      else "❌",
        rsi_15m or 0.0,
        "✅" if rsi_15m_ok  else "❌",
        "✅" if vwap_ok     else "❌",
        "✅" if st4h_ok     else "❌",
        MIN_SCORE,
        score / MAX if MAX > 0 else 0.0,
        _min_score_ratio_for_regime(None),
    )

    return "REVERSAL", direction, score, MAX, reasons

def _compute_indicators(bars: list) -> dict:
    if not bars or len(bars) < 30:
        return {}
    bars = _clean_bars(bars)
    if len(bars) < 30:
        return {}

    closed_bars = bars[:-1]
    closes = [float(_b_close(b)) for b in closed_bars]
    highs  = [float(_b_high(b))  for b in closed_bars]
    lows   = [float(_b_low(b))   for b in closed_bars]
    volumes= [float(_b_vol(b))   for b in closed_bars]

    if len(closes) < 14:
        return {}

    def _safe_last(value):
        return value[-1] if isinstance(value, (list, tuple)) else value

    # EMA
    ema21_raw = ema(closes, 21) if len(closes) >= 21 else None
    ema50_raw = ema(closes, 50) if len(closes) >= 50 else None
    ema21 = _safe_last(ema21_raw) if ema21_raw is not None else 0.0
    ema50 = _safe_last(ema50_raw) if ema50_raw is not None else 0.0
    ema21_series = ema21_raw if isinstance(ema21_raw, (list, tuple)) else []
    ema_bull = ema21 > ema50 if ema21 and ema50 else False
    ema_bear = ema21 < ema50 if ema21 and ema50 else False

    # RSI
    rsi_raw = rsi(closes, 14) if len(closes) >= 14 else None
    rsi_val = _safe_last(rsi_raw) if rsi_raw is not None else None

    # MACD
    macd_raw = macd(closes)
    if isinstance(macd_raw, tuple) and len(macd_raw) >= 2:
        macd_line_raw, signal_line_raw, _ = macd_raw
        macd_line = _safe_last(macd_line_raw) if macd_line_raw else 0.0
        signal_line = _safe_last(signal_line_raw) if signal_line_raw else 0.0
        macd_bull = macd_line > signal_line
        macd_bear = macd_line < signal_line
    else:
        macd_bull = macd_bear = False

    # SuperTrend
    st_bull, st_bear = False, False
    if len(closes) >= 20:
        try:
            st_raw = supertrend(highs, lows, closes, period=10, factor=3.0)
        except TypeError:
            st_raw = supertrend(highs, lows, closes, 10, 3.0)
        if st_raw:
            last_st = _safe_last(st_raw)
            st_bull = last_st == 1
            st_bear = last_st == -1

    # FIX #3: VWAP diario reseteado por día UTC — solo velas del día actual
    vwap_val = 0.0
    if len(closed_bars) > 0 and len(closes) == len(volumes):
        import datetime
        now_utc = datetime.datetime.utcnow()
        day_start_ms = int(datetime.datetime(now_utc.year, now_utc.month, now_utc.day,
                                             tzinfo=datetime.timezone.utc).timestamp() * 1000)
        today_closes = []
        today_volumes = []
        for b, c, v in zip(closed_bars, closes, volumes):
            ts = int(_b_ts(b))
            if ts >= day_start_ms:
                today_closes.append(c)
                today_volumes.append(v)
        if today_closes and sum(today_volumes) > 0:
            cumulative_pv = sum(c * v for c, v in zip(today_closes, today_volumes))
            cumulative_vol = sum(today_volumes)
            vwap_val = cumulative_pv / cumulative_vol
        elif sum(volumes) > 0:
            # fallback: VWAP acumulado si no hay velas del día (e.g. timeframe > 1d)
            cumulative_pv = sum(c * v for c, v in zip(closes, volumes))
            cumulative_vol = sum(volumes)
            vwap_val = cumulative_pv / cumulative_vol if cumulative_vol > 0 else 0.0

    # Volumen ratio
    vol_avg = sum(volumes[-_VOL_AVG_WINDOW:]) / _VOL_AVG_WINDOW if len(volumes) >= _VOL_AVG_WINDOW else volumes[-1]
    vol_ratio = volumes[-1] / vol_avg if vol_avg > 0 else 1.0
    avg_vol_raw = vol_avg

    # ATR
    atr_val = calc_atr(
        [float(_b_high(b)) for b in bars],
        [float(_b_low(b))  for b in bars],
        [float(_b_close(b)) for b in bars],
        14,
    )

    # Divergencias RSI
    rsi_series = rsi(closes, 14)
    if isinstance(rsi_series, (list, tuple)):
        bull_div, bear_div = rsi_divergence(closes, rsi_series, 14)
    else:
        bull_div = bear_div = False

    return {
        "close": closes[-1],
        "ema21": ema21,
        "ema50": ema50,
        "ema_bull": ema_bull,
        "ema_bear": ema_bear,
        "_ema21_series": ema21_series,
        "rsi_val": rsi_val,
        "macd_bull": macd_bull,
        "macd_bear": macd_bear,
        "st_bull": st_bull,
        "st_bear": st_bear,
        "vwap": vwap_val,
        "vol_ratio": vol_ratio,
        "_avg_vol": avg_vol_raw,
        "atr": atr_val,
        "rsi_bull_div": bull_div,
        "rsi_bear_div": bear_div,
    }

def _hold_result(symbol: str, reason: str, max_score: int = MAX_SCORE_NEUTRAL) -> SignalResult:
    return SignalResult(
        symbol=symbol,
        signal="NEUTRAL",
        entry_mode="NONE",
        score=0,
        max_score=max_score,
        entry=0.0,
        sl=0.0,
        tp1=0.0,
        tp2=0.0,
        atr=0.0,
        rr=0.0,
        suggested_lev=0,
        indicators={},
        is_valid=False,
        reason=reason,
    )

def format_signal_block(res: SignalResult) -> str:
    if not res.is_valid:
        return f"🚫 {res.symbol} | NEUTRAL | {res.reason}"
    premium = " ⭐" if res.extra.get("is_premium") else ""
    block = (
        f"{res.symbol} | {res.signal}{premium} [{res.entry_mode}] | "
        f"score {res.score}/{res.max_score} (ratio {res.extra.get('score_ratio', 0):.2f}) | "
        f"RR {res.rr:.2f} | entry {res.entry:.6f} | sl {res.sl:.6f} | tp1 {res.tp1:.6f} | "
        f"tp2 {res.tp2:.6f} | lev {res.suggested_lev}x | {res.extra.get('setup_type', '')}"
    )
    return block

class SignalFlipGuard:
    def __init__(self, cooldown_seconds: int = 300):
        self.cooldown = cooldown_seconds
        self.last_close_time: Dict[str, float] = {}

    def record_close(self, symbol: str):
        self.last_close_time[symbol] = time.time()

    def can_enter(self, symbol: str, new_signal: str, last_signal: Optional[str] = None) -> bool:
        if symbol not in self.last_close_time:
            return True
        if time.time() - self.last_close_time[symbol] < self.cooldown:
            if last_signal and last_signal != new_signal:
                log.debug("[SignalFlipGuard] %s cooldown activo: %s -> %s", symbol, last_signal, new_signal)
                return False
        return True

def manual_close_cooldown(guard: SignalFlipGuard, symbol: str, new_signal: str, last_signal: Optional[str] = None) -> bool:
    return guard.can_enter(symbol, new_signal, last_signal)

# FIX #2: usar hasattr(exch, 'markets') en lugar de 'market' (CCXT usa .markets, no .market)
async def _fetch_bars(exch, symbol: str, tf: str, limit: int) -> list:
    try:
        ccxt_symbol = _to_ccxt_symbol(symbol) if hasattr(exch, 'markets') else symbol
        ohlcv = await exch.fetch_ohlcv(ccxt_symbol, timeframe=tf, limit=limit)
        return [[ts, o, h, l, c, v] for ts, o, h, l, c, v in ohlcv]
    except Exception as e:
        log.error("[signal_engine] Error fetching %s %s: %s", symbol, tf, e)
        raise


# ---------------------------------------------------------------------------
# evaluate() — wrapper para DecisionEngine
# ---------------------------------------------------------------------------

async def evaluate(
    symbol: str,
    price: float,  # noqa: ARG001 — no usado, mantenido para compatibilidad con DecisionEngine
    ohlcv_fn: Optional[Callable] = None,
    exch=None,
    funding_rate: float = 0.0,
    regime: Optional[str] = None,
) -> Optional[dict]:
    """
    Wrapper para DecisionEngine.evaluate().
    Traduce la interfaz (symbol, price, ohlcv_fn) → analyze_pair().
    Retorna None si la señal no es válida (NEUTRAL / score insuficiente).
    """
    result = await analyze_pair(
        exch=exch,
        symbol=symbol,
        ohlcv_fn=ohlcv_fn,
        funding_rate=funding_rate,
        regime=regime,
    )

    if result is None or not result.is_valid:
        return None

    return {
        "action":          result.signal,   # "LONG" / "SHORT"
        "side":            result.signal,   # alias — TradingLoop usa signal.get("side")
        "signal":          result.signal,
        "entry_mode":      result.entry_mode,
        "score":           result.score,
        "max_score":       result.max_score,
        "min_score_ratio": result.extra.get("effective_min_ratio", MIN_SCORE_RATIO),
        "entry":           result.entry,
        "sl":              result.sl,
        "tp1":             result.tp1,
        "tp2":             result.tp2,
        "atr":             result.atr,
        "rr":              result.rr,
        "suggested_lev":   result.suggested_lev,
        "indicators":      result.indicators,
        "extra":           result.extra,
        "reason":          result.reason,
    }
