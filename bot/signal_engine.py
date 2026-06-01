#!/usr/bin/env python3
"""
signal_engine.py — Motor de análisis técnico multi-timeframe (ASYNC)

MEJORAS v4:
  #1 Trailing stop → position_manager.py
  #2 Pesos diferenciados por TF/indicador:
       4h: ema_trend=2, macd=1, ema200=1        (total max=4)
       1h: ema_trend=2, rsi=1, supertrend=1     (total max=4)
       15m: ema_trend=1, macd=1, stoch=1, vol=1 (total max=4)
       BB bonus (1h+15m)                        (+1)
       Structure bonus (BOS+HH/HL via structure_analyzer) (+1/+2)
       Total máximo                             =13 → capeado a 10
  #3 Filtro ADX anti-chop:
       ADX calculado sobre 1h. Si ADX < ADX_MIN_THRESHOLD (default 20) → NEUTRAL
       Desactivar con ADX_FILTER=false en Railway
  #4 Cooldown diferenciado por entry_mode → signal_cooldown.py
  #5 Sesión aiohttp persistente a nivel de módulo: evita crear/destruir
     ClientSession en cada llamada a _fetch_ohlcv_hl.
  #6 SL anclado a estructura de mercado (swing low/high + buffer ATR*0.2)
     en vez de ATR plano. Fallback a ATR plano si no hay swings.
     Config: SL_STRUCTURE_ENABLED (default true)
             SL_STRUCTURE_BUFFER_MULT (default 0.2)

Modos de entrada (se exporta entry_mode en SignalResult):
  EARLY   score 5,   1h+15m alineados                         → lev 5-8x, size 50%
  NORMAL  score 6-7, todos los TF alineados                   → lev 8-14x
  STRONG  score 8+,  confluencia máxima                       → lev 14-15x
"""

from __future__ import annotations

import asyncio
import logging
import os
from dataclasses import dataclass, field
from typing import Optional

import numpy as np
import pandas as pd

try:
    import ta as ta_lib
except ImportError:
    ta_lib = None

log = logging.getLogger(__name__)

MIN_SCORE       = int(os.getenv("MIN_SCORE",      "6"))
MIN_SCORE_FULL  = int(os.getenv("MIN_SCORE_FULL", "6"))
MIN_RR          = float(os.getenv("MIN_RR",       "1.8"))
ATR_MULT_SL     = float(os.getenv("ATR_MULT_SL",  "1.8"))
TP1_MULT        = float(os.getenv("TP1_MULT",     "3.5"))
TP2_MULT        = float(os.getenv("TP2_MULT",     "5.0"))
TP3_MULT        = float(os.getenv("TP3_MULT",     "8.0"))

REQUIRE_4H_ALIGNMENT = os.getenv("REQUIRE_4H_ALIGNMENT", "true").lower() != "false"

# #3 Filtro ADX
ADX_FILTER        = os.getenv("ADX_FILTER",         "true").lower() != "false"
ADX_MIN_THRESHOLD = float(os.getenv("ADX_MIN_THRESHOLD", "20"))

# TP dinámicos por ADX fuerte
ADX_STRONG_THRESHOLD = float(os.getenv("ADX_STRONG_THRESHOLD", "30"))
TP1_STRONG_MULT      = float(os.getenv("TP1_STRONG_MULT", "4.5"))
TP2_STRONG_MULT      = float(os.getenv("TP2_STRONG_MULT", "7.0"))
TP3_STRONG_MULT      = float(os.getenv("TP3_STRONG_MULT", "11.0"))

# #6 SL estructural
SL_STRUCTURE_ENABLED     = os.getenv("SL_STRUCTURE_ENABLED",     "true").lower() != "false"
SL_STRUCTURE_BUFFER_MULT = float(os.getenv("SL_STRUCTURE_BUFFER_MULT", "0.2"))

LEV_EARLY_MIN   = 5
LEV_EARLY_MAX   = 8
LEV_NORMAL_MIN  = 8
LEV_NORMAL_MAX  = 14
LEV_STRONG_MIN  = 14
LEV_STRONG_MAX  = 15

EARLY_SIZE_RATIO = 0.5

OB_IMBALANCE_THRESHOLD    = 0.15
FUNDING_EXTREME_THRESHOLD = 0.0005

SCORE_MAX = 10

_OHLCV_SEM = asyncio.Semaphore(4)

# ── Sesión HTTP persistente ────────────────────────────────────────────────────
_http_session: Optional["aiohttp.ClientSession"] = None


def _get_http_session() -> "aiohttp.ClientSession":
    global _http_session
    import aiohttp
    if _http_session is None or _http_session.closed:
        _http_session = aiohttp.ClientSession()
    return _http_session


async def close_http_session() -> None:
    global _http_session
    if _http_session and not _http_session.closed:
        await _http_session.close()
        _http_session = None


# ─────────────────────────────────────────────────────────────────────────────


def _norm_coin(symbol: str) -> str:
    s = symbol.replace("/", "").replace(":USDT", "").upper()
    if s.endswith("USDTUSDT"):
        s = s[:-4]
    if s.endswith("USDT"):
        s = s[:-4]
    return s


@dataclass
class SignalResult:
    symbol: str
    signal: str      = "NEUTRAL"
    score: int       = 0
    max_score: int   = SCORE_MAX
    entry_mode: str  = "NONE"
    entry: float     = 0.0
    sl: float        = 0.0
    tp1: float       = 0.0
    tp2: float       = 0.0
    tp3: float       = 0.0
    rr: float        = 0.0
    atr: float       = 0.0
    adx: float       = 0.0
    suggested_lev: int  = 1
    size_ratio: float   = 1.0
    pct_tp3: float      = 0.0
    indicators: dict    = field(default_factory=dict)
    ob_imbalance: Optional[float]  = None
    funding_rate: Optional[float]  = None
    error: Optional[str] = None
    sl_source: str = "atr"  # 'structure' | 'atr' — para logging/debug

    @property
    def is_valid(self) -> bool:
        return (
            self.signal in ("LONG", "SHORT")
            and self.score >= MIN_SCORE
            and self.rr >= MIN_RR
            and self.entry_mode != "NONE"
        )

    def summary(self) -> str:
        if not self.is_valid:
            return f"{self.symbol} · NEUTRAL · Score {self.score}/{self.max_score}"
        em   = f"[{self.entry_mode}]"
        icon = "🟢" if self.signal == "LONG" else "🔴"
        extras = []
        if self.adx:
            extras.append(f"ADX {self.adx:.0f}")
        if self.ob_imbalance is not None:
            extras.append(f"OB {self.ob_imbalance:+.2f}")
        if self.funding_rate is not None:
            extras.append(f"FR {self.funding_rate*100:+.4f}%")
        extra_str = " · " + " · ".join(extras) if extras else ""
        return (
            f"{icon} {self.symbol} · {self.signal} {em} · Score {self.score}/{self.max_score} · "
            f"R/R {self.rr:.1f} · Lev {self.suggested_lev}x · "
            f"Entry {self.entry:.4f} · SL {self.sl:.4f} [{self.sl_source}] · TP1 {self.tp1:.4f}{extra_str}"
        )


async def _fetch_ohlcv_hl(coin: str, tf: str, limit: int = 200) -> pd.DataFrame:
    import time as _time
    import json as _json

    _USE_TESTNET = os.getenv("HL_TESTNET", "").lower() in ("true", "1", "yes")
    api_url      = "https://api.hyperliquid-testnet.xyz" if _USE_TESTNET else "https://api.hyperliquid.xyz"
    tf_ms = {"15m": 15*60*1000, "1h": 60*60*1000, "4h": 4*60*60*1000}.get(tf, 15*60*1000)
    now   = int(_time.time() * 1000)
    start = now - limit * tf_ms
    payload = {"type": "candleSnapshot", "req": {
        "coin": coin, "interval": tf, "startTime": start, "endTime": now
    }}

    import aiohttp
    session = _get_http_session()

    for attempt in range(3):
        try:
            async with _OHLCV_SEM:
                async with session.post(
                    f"{api_url}/info", json=payload,
                    headers={"Content-Type": "application/json"},
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as r:
                    if r.status == 429:
                        wait = 2 ** attempt
                        log.debug("[OHLCV] %s %s 429 → reintentando en %ds (intento %d/3)",
                                  coin, tf, wait, attempt + 1)
                        await asyncio.sleep(wait)
                        continue
                    data = _json.loads(await r.text())

            if not isinstance(data, list) or len(data) == 0:
                log.warning("[OHLCV] %s %s REST HL respuesta vacía: %s", coin, tf, str(data)[:120])
                return pd.DataFrame()

            bars = [
                [int(c["t"]), float(c["o"]), float(c["h"]),
                 float(c["l"]), float(c["c"]), float(c["v"])]
                for c in data
            ]
            df = pd.DataFrame(bars, columns=["ts", "open", "high", "low", "close", "volume"])
            df["ts"] = pd.to_datetime(df["ts"], unit="ms", utc=True)
            return df.set_index("ts").astype(float)

        except Exception as e:
            log.warning("[OHLCV] %s %s REST HL error: %s", coin, tf, e)
            return pd.DataFrame()

    log.warning("[OHLCV] %s %s REST HL: 3 intentos fallidos", coin, tf)
    return pd.DataFrame()


async def _fetch_ohlcv(exch, symbol: str, tf: str, limit: int = 200) -> pd.DataFrame:
    coin = _norm_coin(symbol)

    try:
        from bot.ws_feed import ws_feed
        df = ws_feed.get_ohlcv(coin, tf)
        if not df.empty and len(df) >= 55:
            return df
    except Exception as e:
        log.debug("[OHLCV] %s %s WS error: %s", coin, tf, e)

    df = await _fetch_ohlcv_hl(coin, tf, limit)
    if not df.empty and len(df) >= 55:
        return df

    if exch is not None:
        try:
            raw = await exch.fetch_ohlcv(coin, tf, limit=limit)
            df  = pd.DataFrame(raw, columns=["ts", "open", "high", "low", "close", "volume"])
            df["ts"] = pd.to_datetime(df["ts"], unit="ms", utc=True)
            return df.set_index("ts").astype(float)
        except Exception as e:
            log.warning("[OHLCV] %s %s ccxt: %s", coin, tf, e)

    return pd.DataFrame()


def _supertrend_dir(df: pd.DataFrame, period: int = 10, mult: float = 3.0) -> int:
    try:
        h = df["high"].values
        l = df["low"].values
        c = df["close"].values
        if len(c) < period + 5:
            return 0

        tr = np.maximum(
            h[1:] - l[1:],
            np.maximum(np.abs(h[1:] - c[:-1]), np.abs(l[1:] - c[:-1]))
        )
        tr_full = np.concatenate([[np.nan], tr])

        atr = np.full(len(c), np.nan)
        atr[period] = np.nanmean(tr_full[1:period + 1])
        for i in range(period + 1, len(c)):
            atr[i] = (atr[i - 1] * (period - 1) + tr_full[i]) / period

        hl2  = (h + l) / 2.0
        ub   = hl2 + mult * atr
        lb   = hl2 - mult * atr

        st_arr    = np.full(len(c), np.nan)
        trend_arr = np.ones(len(c), dtype=int)

        for i in range(1, len(c)):
            prev_st = st_arr[i - 1] if not np.isnan(st_arr[i - 1]) else lb[i]
            if c[i] > prev_st:
                st_arr[i]    = lb[i]
                trend_arr[i] = 1
            else:
                st_arr[i]    = ub[i]
                trend_arr[i] = -1

        return int(trend_arr[-1])
    except Exception:
        return 0


def _calc_adx(df: pd.DataFrame, period: int = 14) -> float:
    try:
        if ta_lib is None or len(df) < period * 2:
            return 0.0
        adx_ind = ta_lib.trend.ADXIndicator(
            df["high"], df["low"], df["close"], window=period
        )
        val = adx_ind.adx().iloc[-1]
        return round(float(val), 1) if not np.isnan(val) else 0.0
    except Exception:
        return 0.0


def _analyze_tf(df: pd.DataFrame) -> dict:
    if ta_lib is None:
        raise ImportError("Instala 'ta': pip install ta")
    if df.empty or len(df) < 55:
        return {}

    c, h, l, v = df["close"], df["high"], df["low"], df["volume"]
    s: dict = {}

    try:
        e9   = ta_lib.trend.ema_indicator(c, window=9).iloc[-1]
        e21  = ta_lib.trend.ema_indicator(c, window=21).iloc[-1]
        e50  = ta_lib.trend.ema_indicator(c, window=50).iloc[-1]
        e200 = ta_lib.trend.ema_indicator(c, window=200).iloc[-1]
        cl   = c.iloc[-1]
        bull = e9 > e21 > e50 > e200 and cl > e50
        bear = e9 < e21 < e50 < e200 and cl < e50
        s["ema_trend"] = 1 if bull else (-1 if bear else 0)
        s["ema200"]    = 1 if cl > e200 else -1
        s["e9"] = e9; s["e21"] = e21; s["e50"] = e50; s["e200"] = e200
    except Exception:
        s["ema_trend"] = s["ema200"] = 0

    try:
        r = ta_lib.momentum.rsi(c, window=14).iloc[-1]
        s["rsi_val"] = round(r, 1)
        if 55 < r < 78:   s["rsi"] = 1
        elif 22 < r < 45: s["rsi"] = -1
        elif r <= 22:     s["rsi"] = 1
        elif r >= 78:     s["rsi"] = -1
        else:             s["rsi"] = 0
    except Exception:
        s["rsi"] = 0; s["rsi_val"] = 50

    try:
        macd_ind = ta_lib.trend.MACD(c, window_slow=26, window_fast=12, window_sign=9)
        ml  = macd_ind.macd().iloc[-1]
        sl_ = macd_ind.macd_signal().iloc[-1]
        mh  = macd_ind.macd_diff().iloc[-1]
        ph  = macd_ind.macd_diff().iloc[-2]
        if   ml > sl_ and mh > 0 and mh > ph: s["macd"] = 1
        elif ml < sl_ and mh < 0 and mh < ph: s["macd"] = -1
        else:                                   s["macd"] = 0
    except Exception:
        s["macd"] = 0

    try:
        bb   = ta_lib.volatility.BollingerBands(c, window=20, window_dev=2)
        mid  = bb.bollinger_mavg().iloc[-1]
        pmid = bb.bollinger_mavg().iloc[-2]
        bbu  = bb.bollinger_hband().iloc[-1]
        bbl  = bb.bollinger_lband().iloc[-1]
        px   = c.iloc[-1]; ppx = c.iloc[-2]
        if   px > mid  and ppx <= pmid: s["bb"] = 1
        elif px < mid  and ppx >= pmid: s["bb"] = -1
        elif px < bbl:                   s["bb"] = 1
        elif px > bbu:                   s["bb"] = -1
        else:                            s["bb"] = 0
    except Exception:
        s["bb"] = 0

    try:
        stoch = ta_lib.momentum.StochRSIIndicator(c, window=14, smooth1=3, smooth2=3)
        k = stoch.stochrsi_k().iloc[-1] * 100
        d = stoch.stochrsi_d().iloc[-1] * 100
        s["stoch_k"] = round(k, 1); s["stoch_d"] = round(d, 1)
        if   k > d and k < 80:  s["stoch"] = 1
        elif k < d and k > 20:  s["stoch"] = -1
        elif k < 20 and k > d:  s["stoch"] = 1
        elif k > 80 and k < d:  s["stoch"] = -1
        else:                    s["stoch"] = 0
    except Exception:
        s["stoch"] = 0; s["stoch_k"] = s["stoch_d"] = 50

    s["supertrend"] = _supertrend_dir(df)

    try:
        vm = v.rolling(20).mean().iloc[-1]
        vr = v.iloc[-1] / vm if vm > 0 else 1.0
        up = c.iloc[-1] > c.iloc[-2]
        s["vol_ratio"] = round(vr, 2)
        s["volume"] = (1 if up else -1) if vr > 1.5 else 0
    except Exception:
        s["volume"] = 0; s["vol_ratio"] = 1.0

    return s


def _compute_score(s4h: dict, s1h: dict, s15: dict) -> tuple[int, str]:
    """
    Pesos diferenciados por TF:
      4h: ema_trend×2, macd×1, ema200×1  → max 4
      1h: ema_trend×2, rsi×1, supertrend×1 → max 4
      15m: ema_trend×1, macd×1, stoch×1, volume×1 → max 4
      BB bonus (1h+15m) → +1
      Total → capeado a 10 (structure bonus se añade en analyze_pair)
    """
    sl = ss = 0

    # 4h
    sl += max(0,  s4h.get("ema_trend", 0)) * 2
    ss += max(0, -s4h.get("ema_trend", 0)) * 2
    for key in ("macd", "ema200"):
        sl += max(0,  s4h.get(key, 0))
        ss += max(0, -s4h.get(key, 0))

    # 1h
    sl += max(0,  s1h.get("ema_trend", 0)) * 2
    ss += max(0, -s1h.get("ema_trend", 0)) * 2
    for key in ("rsi", "supertrend"):
        sl += max(0,  s1h.get(key, 0))
        ss += max(0, -s1h.get(key, 0))

    # 15m
    for key in ("ema_trend", "macd", "stoch", "volume"):
        sl += max(0,  s15.get(key, 0))
        ss += max(0, -s15.get(key, 0))

    # BB bonus
    if s15.get("bb", 0) == 1  and s1h.get("bb", 0) == 1:  sl += 1
    if s15.get("bb", 0) == -1 and s1h.get("bb", 0) == -1: ss += 1

    best      = max(sl, ss)
    score     = min(best, SCORE_MAX)
    direction = "LONG" if sl > ss else "SHORT"
    return score, direction


def _calc_structural_sl(
    df1h: pd.DataFrame,
    entry: float,
    direction: str,
    atr: float,
) -> tuple[float, str]:
    """
    Calcula el SL anclado al swing low/high más cercano.

    Lógica:
      LONG:  SL = swing_low más alto que sea < entry, con buffer -ATR*MULT
             Si ningún swing está < entry → fallback ATR plano
      SHORT: SL = swing_high más bajo que sea > entry, con buffer +ATR*MULT
             Si ningún swing está > entry → fallback ATR plano

    Returns:
        (sl_price, source) donde source es 'structure' o 'atr'
    """
    if not SL_STRUCTURE_ENABLED or df1h.empty or len(df1h) < 15:
        sl_atr = entry - atr * ATR_MULT_SL if direction == "LONG" else entry + atr * ATR_MULT_SL
        return sl_atr, "atr"

    try:
        from bot.structure_analyzer import _find_swings, STRUCTURE_SWING_N
        swing_highs, swing_lows = _find_swings(df1h, STRUCTURE_SWING_N)
        buffer = atr * SL_STRUCTURE_BUFFER_MULT

        if direction == "LONG" and swing_lows:
            # Swing lows por debajo del entry, tomar el más alto (el más cercano)
            candidates = [sl for _, sl in swing_lows if sl < entry]
            if candidates:
                nearest_low = max(candidates)
                sl_struct = nearest_low - buffer
                # Validar que el SL estructural no sea demasiado lejos (> 3x ATR)
                max_sl_dist = atr * ATR_MULT_SL * 2.0
                if entry - sl_struct <= max_sl_dist:
                    log.debug(
                        "[SL] %s LONG SL estructural=%.4f (swing_low=%.4f, buffer=%.4f)",
                        "", sl_struct, nearest_low, buffer,
                    )
                    return sl_struct, "structure"

        elif direction == "SHORT" and swing_highs:
            # Swing highs por encima del entry, tomar el más bajo (el más cercano)
            candidates = [sh for _, sh in swing_highs if sh > entry]
            if candidates:
                nearest_high = min(candidates)
                sl_struct = nearest_high + buffer
                max_sl_dist = atr * ATR_MULT_SL * 2.0
                if sl_struct - entry <= max_sl_dist:
                    log.debug(
                        "[SL] %s SHORT SL estructural=%.4f (swing_high=%.4f, buffer=%.4f)",
                        "", sl_struct, nearest_high, buffer,
                    )
                    return sl_struct, "structure"

    except Exception as e:
        log.debug("[SL] _calc_structural_sl error: %s", e)

    # Fallback
    sl_atr = entry - atr * ATR_MULT_SL if direction == "LONG" else entry + atr * ATR_MULT_SL
    return sl_atr, "atr"


def _apply_microstructure(
    score: int,
    direction: str,
    ob_metrics,
    funding_rate,
) -> tuple[int, Optional[float], Optional[float]]:
    ob_imbalance_val = None
    fr_val           = None
    bonus            = 0

    if ob_metrics and isinstance(ob_metrics, dict):
        imbalance = ob_metrics.get("imbalance", 0.0)
        ob_imbalance_val = imbalance
        if direction == "LONG"  and imbalance >  OB_IMBALANCE_THRESHOLD: bonus += 1
        elif direction == "SHORT" and imbalance < -OB_IMBALANCE_THRESHOLD: bonus += 1
        elif direction == "LONG"  and imbalance < -OB_IMBALANCE_THRESHOLD: bonus -= 1
        elif direction == "SHORT" and imbalance >  OB_IMBALANCE_THRESHOLD: bonus -= 1

    if funding_rate is not None:
        fr_val = funding_rate
        if direction == "LONG":
            if funding_rate >  FUNDING_EXTREME_THRESHOLD: bonus -= 1
            elif funding_rate < -FUNDING_EXTREME_THRESHOLD: bonus += 1
        else:
            if funding_rate < -FUNDING_EXTREME_THRESHOLD: bonus -= 1
            elif funding_rate >  FUNDING_EXTREME_THRESHOLD: bonus += 1

    adjusted = max(0, min(score + bonus, SCORE_MAX))
    return adjusted, ob_imbalance_val, fr_val


def _classify_entry_mode(score: int, s4h: dict, s1h: dict, s15: dict, direction: str) -> tuple[str, int, float]:
    sign = 1 if direction == "LONG" else -1

    if REQUIRE_4H_ALIGNMENT:
        tf4h_trend = s4h.get("ema_trend", 0)
        if tf4h_trend * sign < 0:
            log.info(
                "[signal] RECHAZADO por gate 4h — 4h ema_trend=%d contrario a %s",
                tf4h_trend, direction,
            )
            return "NONE", 1, 0.0

    tf1h_aligned = s1h.get("ema_trend", 0) * sign
    tf15_aligned = s15.get("ema_trend", 0) * sign

    extra_1h  = sum(1 for k in ("rsi", "supertrend", "macd") if s1h.get(k, 0) * sign > 0)
    extra_15m = sum(1 for k in ("macd", "stoch", "volume")   if s15.get(k, 0) * sign > 0)

    if score >= 8:
        ratio = min((score - 8) / 2.0, 1.0)
        lev   = round(LEV_STRONG_MIN + ratio * (LEV_STRONG_MAX - LEV_STRONG_MIN))
        return "STRONG", lev, 1.0

    if score >= MIN_SCORE_FULL:
        ratio = min((score - MIN_SCORE_FULL) / 2.0, 1.0)
        lev   = round(LEV_NORMAL_MIN + ratio * (LEV_NORMAL_MAX - LEV_NORMAL_MIN))
        return "NORMAL", lev, 1.0

    if MIN_SCORE <= score < MIN_SCORE_FULL and tf1h_aligned > 0 and tf15_aligned > 0:
        quality = extra_1h + extra_15m
        ratio   = min(quality / 6.0, 1.0)
        lev     = round(LEV_EARLY_MIN + ratio * (LEV_EARLY_MAX - LEV_EARLY_MIN))
        return "EARLY", lev, EARLY_SIZE_RATIO

    return "NONE", 1, 0.0


async def analyze_pair(exch, symbol: str) -> SignalResult:
    coin   = _norm_coin(symbol)
    result = SignalResult(symbol=coin)

    try:
        df15 = await _fetch_ohlcv(exch, coin, "15m", 200)
        df1h = await _fetch_ohlcv(exch, coin, "1h",  200)
        df4h = await _fetch_ohlcv(exch, coin, "4h",  200)

        if df15.empty or len(df15) < 55:
            result.error = "Datos insuficientes 15m"
            return result

        s15 = _analyze_tf(df15)
        s1h = _analyze_tf(df1h) if not df1h.empty else {}
        s4h = _analyze_tf(df4h) if not df4h.empty else {}

        # Filtro ADX
        adx_df  = df1h if (not df1h.empty and len(df1h) >= 28) else df15
        adx_val = _calc_adx(adx_df)
        result.adx = adx_val

        if ADX_FILTER and adx_val > 0 and adx_val < ADX_MIN_THRESHOLD:
            log.info(
                "[signal_engine] %s RECHAZADO por ADX=%.1f < %.0f (mercado en rango)",
                coin, adx_val, ADX_MIN_THRESHOLD,
            )
            result.error = f"ADX={adx_val:.1f} < {ADX_MIN_THRESHOLD} (chop)"
            return result

        result.indicators = {
            "15m": s15,
            "1h":  s1h,
            "4h":  s4h,
            "_closes_15m": df15["close"].tolist() if not df15.empty else [],
            "_closes_1h":  df1h["close"].tolist() if not df1h.empty else [],
            "adx": adx_val,
        }

        score_base, direction = _compute_score(s4h, s1h, s15)

        # ── #6 Structure bonus (BOS + HH/HL) ──────────────────────────────
        struct_result = {"score": 0, "bos": False, "hh_hl": 0, "last_sh": 0.0, "last_sl": 0.0}
        try:
            from bot.structure_analyzer import analyze_structure
            struct_df = df1h if (not df1h.empty and len(df1h) >= 20) else df15
            dir_int   = 1 if direction == "LONG" else -1
            struct_result = analyze_structure(struct_df, direction=dir_int)
            struct_bonus  = struct_result.get("score", 0)
            if struct_bonus != 0:
                log.debug(
                    "[signal_engine] %s structure bonus=%d (BOS=%s, HH/HL=%d)",
                    coin, struct_bonus, struct_result.get("bos"), struct_result.get("hh_hl"),
                )
        except Exception as e:
            struct_bonus = 0
            log.debug("[signal_engine] structure_analyzer error: %s", e)

        result.indicators["structure"] = struct_result

        ob_metrics   = None
        funding_rate = None
        try:
            from bot.ws_feed import ws_feed
            ob_metrics   = ws_feed.get_orderbook_metrics(coin)
            funding_rate = ws_feed.get_funding_rate(coin)
        except Exception as e:
            log.debug("[signal_engine] microestructura no disponible: %s", e)

        score_with_struct = min(score_base + struct_bonus, SCORE_MAX)
        score, ob_imbalance_val, fr_val = _apply_microstructure(
            score_with_struct, direction, ob_metrics, funding_rate
        )
        result.score        = score
        result.ob_imbalance = ob_imbalance_val
        result.funding_rate = fr_val

        mode, lev, size_ratio = _classify_entry_mode(score, s4h, s1h, s15, direction)

        if mode == "NONE":
            log.info(
                "[signal_engine] %s descartado — score=%d/%d, modo=NONE, dir=%s",
                coin, score, SCORE_MAX, direction,
            )
            return result

        # ATR sobre 1h
        atr_df = df1h if (not df1h.empty and len(df1h) >= 20) else df15
        try:
            atr_s = ta_lib.volatility.AverageTrueRange(
                atr_df["high"], atr_df["low"], atr_df["close"], window=14
            ).average_true_range()
            atr = float(atr_s.iloc[-1])
        except Exception:
            atr = float(df15["close"].iloc[-1]) * 0.005

        result.atr = round(atr, 8)
        entry = float(df15["close"].iloc[-1])

        # ── #6 SL estructural (ancla a swing, fallback ATR) ────────────────
        sl, sl_source = _calc_structural_sl(df1h, entry, direction, atr)
        result.sl_source = sl_source
        risk = abs(entry - sl)  # distancia real entry→SL (cualquier fuente)

        if risk <= 0:
            # Fallback defensivo
            risk = atr * ATR_MULT_SL
            sl   = entry - risk if direction == "LONG" else entry + risk
            result.sl_source = "atr_fallback"

        # TP dinámicos por ADX
        if adx_val >= ADX_STRONG_THRESHOLD:
            tp1_m, tp2_m, tp3_m = TP1_STRONG_MULT, TP2_STRONG_MULT, TP3_STRONG_MULT
        else:
            tp1_m, tp2_m, tp3_m = TP1_MULT, TP2_MULT, TP3_MULT

        if direction == "LONG":
            tp1 = entry + risk * tp1_m
            tp2 = entry + risk * tp2_m
            tp3 = entry + risk * tp3_m
        else:
            tp1 = entry - risk * tp1_m
            tp2 = entry - risk * tp2_m
            tp3 = entry - risk * tp3_m

        dist_entry_sl = abs(entry - sl)
        rr = round(abs(tp1 - entry) / dist_entry_sl, 2) if dist_entry_sl > 0 else 0

        if rr < MIN_RR:
            log.info(
                "[signal_engine] %s descartado — R/R %.2f < %.1f (score=%d, modo=%s, sl_src=%s)",
                coin, rr, MIN_RR, score, mode, sl_source,
            )
            return result

        pct_tp3 = round(abs(tp3 - entry) / entry * 100, 2)

        result.signal        = direction
        result.entry_mode    = mode
        result.entry         = round(entry, 6)
        result.sl            = round(sl,    6)
        result.tp1           = round(tp1,   6)
        result.tp2           = round(tp2,   6)
        result.tp3           = round(tp3,   6)
        result.rr            = rr
        result.pct_tp3       = pct_tp3
        result.suggested_lev = lev
        result.size_ratio    = size_ratio

    except Exception as e:
        result.error = str(e)
        log.error("[signal_engine] %s: %s", coin, e)

    return result


def _ei(v: int) -> str:
    return "🟢" if v == 1 else ("🔴" if v == -1 else "⚪")


def _mode_emoji(mode: str) -> str:
    return {"EARLY": "🔸", "NORMAL": "🔷", "STRONG": "💥"}.get(mode, "⚪")


def format_signal_block(r: SignalResult) -> str:
    if not r.is_valid:
        adx_txt = f" · ADX `{r.adx:.0f}`" if r.adx else ""
        return f"📊 Score técnico: `{r.score}/{r.max_score}`{adx_txt} — sin señal clara"

    i15 = r.indicators.get("15m", {})
    i1h = r.indicators.get("1h",  {})
    i4h = r.indicators.get("4h",  {})
    ist = r.indicators.get("structure", {})
    d   = r.signal
    me  = _mode_emoji(r.entry_mode)

    size_txt = f" · Size `{int(r.size_ratio*100)}%`" if r.size_ratio < 1.0 else ""
    adx_icon = "🔥" if r.adx >= ADX_STRONG_THRESHOLD else ("📉" if r.adx < ADX_MIN_THRESHOLD else "📊")
    adx_txt  = f" · {adx_icon} ADX `{r.adx:.0f}`" if r.adx else ""

    ob_txt = ""
    if r.ob_imbalance is not None:
        arrow = "↑" if r.ob_imbalance > 0.05 else ("↓" if r.ob_imbalance < -0.05 else "→")
        ob_txt = f"\n  OB {arrow} `{r.ob_imbalance:+.3f}`"

    fr_txt = ""
    if r.funding_rate is not None:
        fr_pct = r.funding_rate * 100
        emoji  = "🔥" if abs(fr_pct) > 0.05 else "⚪"
        fr_txt = f"\n  Funding {emoji} `{fr_pct:+.4f}%`"

    sl_src_icon = "🏗" if r.sl_source == "structure" else "📐"
    struct_txt = ""
    if ist:
        bos_txt = "✅ BOS" if ist.get("bos") else ""
        hhhl    = ist.get("hh_hl", 0)
        hhhl_txt = "HH/HL" if hhhl == 1 else ("LH/LL" if hhhl == -1 else "")
        parts = [x for x in [bos_txt, hhhl_txt] if x]
        if parts:
            struct_txt = f"\n  Estructura {' · '.join(parts)}"

    lines = [
        f"📊 *Análisis técnico* · Score `{r.score}/{r.max_score}` · R/R `{r.rr}:1`{adx_txt}",
        f"{'🟢 LONG' if d == 'LONG' else '🔴 SHORT'} · Modo {me}`{r.entry_mode}` · Lev `{r.suggested_lev}x`{size_txt}",
        f"",
        f"  Entry `{r.entry}` · SL {sl_src_icon}`{r.sl}` · TP1 `{r.tp1}`",
        f"",
        f"  `4h·1h·15m`",
        f"  EMA   {_ei(i4h.get('ema_trend',0))}·{_ei(i1h.get('ema_trend',0))}·{_ei(i15.get('ema_trend',0))}",
        f"  MACD  {_ei(i4h.get('macd',0))}·{_ei(i1h.get('macd',0))}·{_ei(i15.get('macd',0))}",
        f"  RSI   {_ei(i4h.get('rsi',0))}·{_ei(i1h.get('rsi',0))}·{_ei(i15.get('rsi',0))} _({i15.get('rsi_val',0)})",
        f"  ST    {_ei(i4h.get('supertrend',0))}·{_ei(i1h.get('supertrend',0))}·{_ei(i15.get('supertrend',0))}",
        f"  Vol   {_ei(i15.get('volume',0))} ×{i15.get('vol_ratio',1.0)}",
    ]
    return "\n".join(lines) + ob_txt + fr_txt + struct_txt
