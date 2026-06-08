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

  