#!/usr/bin/env python3
"""
signal_engine.py — Motor de análisis técnico multi-timeframe (ASYNC)

Modos de entrada (se exporta entry_mode en SignalResult):
  EARLY   score 5-6, 4h neutral/débil, 1h+15m alineados → lev 5-8x
  NORMAL  score 6-7, todos los TF alineados               → lev 8-14x
  STRONG  score 8+, confluencia máxima                    → lev 14-15x
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Optional

import numpy as np
import pandas as pd

try:
    import ta as ta_lib
except ImportError:
    ta_lib = None

log = logging.getLogger(__name__)

MIN_SCORE       = 5
MIN_SCORE_FULL  = 6
MIN_RR          = 1.8
ATR_MULT_SL     = 1.2
TP1_MULT        = 2.5
TP2_MULT        = 4.0
TP3_MULT        = 7.0

# Leverage por modo (rango 5-15x)
LEV_EARLY_MIN   = 5
LEV_EARLY_MAX   = 8
LEV_NORMAL_MIN  = 8
LEV_NORMAL_MAX  = 14
LEV_STRONG_MIN  = 14
LEV_STRONG_MAX  = 15   # techo bajado de 20x a 15x

EARLY_SIZE_RATIO = 0.5


@dataclass
class SignalResult:
    symbol: str
    signal: str      = "NEUTRAL"
    score: int       = 0
    max_score: int   = 10
    entry_mode: str  = "NONE"
    entry: float     = 0.0
    sl: float        = 0.0
    tp1: float       = 0.0
    tp2: float       = 0.0
    tp3: float       = 0.0
    rr: float        = 0.0
    atr: float       = 0.0
    suggested_lev: int  = 1
    size_ratio: float   = 1.0
    pct_tp3: float      = 0.0
    indicators: dict    = field(default_factory=dict)
    error: Optional[str] = None

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
            return f"{self.symbol} · NEUTRAL · Score {self.score}/10"
        em   = f"[{self.entry_mode}]"
        icon = "🟢" if self.signal == "LONG" else "🔴"
        return (
            f"{icon} {self.symbol} · {self.signal} {em} · Score {self.score}/10 · "
            f"R/R {self.rr:.1f} · Lev {self.suggested_lev}x · "
            f"Entry {self.entry:.4f} · SL {self.sl:.4f} · TP1 {self.tp1:.4f}"
        )


async def _fetch_ohlcv(exch, symbol: str, tf: str, limit: int = 200) -> pd.DataFrame:
    """
    Intenta obtener OHLCV desde el WS feed (caché en memoria).
    Si no hay datos suficientes cae al REST de ccxt.
    """
    # ── 1. Intentar WS feed ──────────────────────────────────────────
    try:
        from bot.ws_feed import ws_feed
        # El símbolo en el feed va sin '/' ni ':USDT' (ej: BTCUSDT)
        sym_clean = symbol.replace("/", "").replace(":USDT", "")
        df = ws_feed.get_ohlcv(sym_clean, tf)
        if not df.empty and len(df) >= 55:
            log.debug(f"[OHLCV] {symbol} {tf} ← WS ({len(df)} velas)")
            return df
        log.debug(f"[OHLCV] {symbol} {tf} WS insuficiente ({len(df)} velas), usando REST")
    except Exception as e:
        log.debug(f"[OHLCV] {symbol} {tf} WS error: {e}, usando REST")

    # ── 2. Fallback REST ccxt ──────────────────────────────────────────
    try:
        raw = await exch.fetch_ohlcv(symbol, tf, limit=limit)
        df = pd.DataFrame(raw, columns=["ts", "open", "high", "low", "close", "volume"])
        df["ts"] = pd.to_datetime(df["ts"], unit="ms", utc=True)
        log.debug(f"[OHLCV] {symbol} {tf} ← REST ({len(df)} velas)")
        return df.set_index("ts").astype(float)
    except Exception as e:
        log.warning(f"[OHLCV] {symbol} {tf}: {e}")
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

        hl2 = (h + l) / 2.0
        ub  = hl2 + mult * atr
        lb  = hl2 - mult * atr

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
        bb = ta_lib.volatility.BollingerBands(c, window=20, window_dev=2)
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


def _compute_score(s4h: dict, s1h: dict, s15: dict) -> tuple[int, int, str]:
    sl = ss = 0

    for key in ("ema_trend", "macd", "ema200"):
        sl += max(0,  s4h.get(key, 0))
        ss += max(0, -s4h.get(key, 0))

    for key in ("ema_trend", "rsi", "supertrend"):
        sl += max(0,  s1h.get(key, 0))
        ss += max(0, -s1h.get(key, 0))

    for key in ("ema_trend", "macd", "stoch", "volume"):
        sl += max(0,  s15.get(key, 0))
        ss += max(0, -s15.get(key, 0))

    if s15.get("bb", 0) == 1  and s1h.get("bb", 0) == 1:  sl += 1
    if s15.get("bb", 0) == -1 and s1h.get("bb", 0) == -1: ss += 1

    best      = max(sl, ss)
    score     = min(best, 10)
    direction = "LONG" if sl >= ss else "SHORT"
    return score, min(sl, 10), direction


def _classify_entry_mode(score: int, s4h: dict, s1h: dict, s15: dict, direction: str) -> tuple[str, int, float]:
    sign = 1 if direction == "LONG" else -1

    tf4h_aligned = s4h.get("ema_trend", 0) * sign
    tf1h_aligned = s1h.get("ema_trend", 0) * sign
    tf15_aligned = s15.get("ema_trend", 0) * sign

    extra_1h  = sum(1 for k in ("rsi", "supertrend", "macd") if s1h.get(k, 0) * sign > 0)
    extra_15m = sum(1 for k in ("macd", "stoch", "volume") if s15.get(k, 0) * sign > 0)

    if score >= 8:
        mode  = "STRONG"
        ratio = min((score - 8) / 2.0, 1.0)
        lev   = round(LEV_STRONG_MIN + ratio * (LEV_STRONG_MAX - LEV_STRONG_MIN))
        return mode, lev, 1.0

    if score >= MIN_SCORE_FULL:
        mode  = "NORMAL"
        ratio = min((score - MIN_SCORE_FULL) / 2.0, 1.0)
        lev   = round(LEV_NORMAL_MIN + ratio * (LEV_NORMAL_MAX - LEV_NORMAL_MIN))
        return mode, lev, 1.0

    if score == 5 and tf1h_aligned > 0 and tf15_aligned > 0 and tf4h_aligned <= 0:
        quality = extra_1h + extra_15m
        ratio   = min(quality / 6.0, 1.0)
        lev     = round(LEV_EARLY_MIN + ratio * (LEV_EARLY_MAX - LEV_EARLY_MIN))
        return "EARLY", lev, EARLY_SIZE_RATIO

    return "NONE", 1, 0.0


async def analyze_pair(exch, symbol: str) -> SignalResult:
    result = SignalResult(symbol=symbol)

    try:
        df15 = await _fetch_ohlcv(exch, symbol, "15m", 200)
        df1h = await _fetch_ohlcv(exch, symbol, "1h",  200)
        df4h = await _fetch_ohlcv(exch, symbol, "4h",  200)

        if df15.empty or len(df15) < 55:
            result.error = "Datos insuficientes 15m"
            return result

        s15 = _analyze_tf(df15)
        s1h = _analyze_tf(df1h) if not df1h.empty else {}
        s4h = _analyze_tf(df4h) if not df4h.empty else {}
        result.indicators = {"15m": s15, "1h": s1h, "4h": s4h}

        score, _, direction = _compute_score(s4h, s1h, s15)
        result.score = score

        mode, lev, size_ratio = _classify_entry_mode(score, s4h, s1h, s15, direction)

        if mode == "NONE":
            return result

        try:
            atr_s = ta_lib.volatility.AverageTrueRange(
                df15["high"], df15["low"], df15["close"], window=14
            ).average_true_range()
            atr = float(atr_s.iloc[-1])
        except Exception:
            atr = float(df15["close"].iloc[-1]) * 0.005

        result.atr = round(atr, 8)
        entry = float(df15["close"].iloc[-1])
        risk  = atr * ATR_MULT_SL

        if direction == "LONG":
            sl  = entry - risk
            tp1 = entry + risk * TP1_MULT
            tp2 = entry + risk * TP2_MULT
            tp3 = entry + risk * TP3_MULT
        else:
            sl  = entry + risk
            tp1 = entry - risk * TP1_MULT
            tp2 = entry - risk * TP2_MULT
            tp3 = entry - risk * TP3_MULT

        dist_entry_sl = abs(entry - sl)
        rr = round(abs(tp1 - entry) / dist_entry_sl, 2) if dist_entry_sl > 0 else 0

        if rr < MIN_RR:
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
        log.error(f"[signal_engine] {symbol}: {e}")

    return result


def _ei(v: int) -> str:
    return "🟢" if v == 1 else ("🔴" if v == -1 else "⚪")


def _mode_emoji(mode: str) -> str:
    return {"EARLY": "🔸", "NORMAL": "🔷", "STRONG": "💥"}.get(mode, "⚪")


def format_signal_block(r: SignalResult) -> str:
    if not r.is_valid:
        return f"📊 Score técnico: `{r.score}/10` — sin señal clara"

    i15 = r.indicators.get("15m", {})
    i1h = r.indicators.get("1h",  {})
    i4h = r.indicators.get("4h",  {})
    d   = r.signal
    me  = _mode_emoji(r.entry_mode)

    size_txt = f" · Size `{int(r.size_ratio*100)}%`" if r.size_ratio < 1.0 else ""

    lines = [
        f"📊 *Análisis técnico* · Score `{r.score}/10` · R/R `{r.rr}:1`",
        f"{'🟢 LONG' if d == 'LONG' else '🔴 SHORT'} · Modo {me}`{r.entry_mode}` · Lev `{r.suggested_lev}x`{size_txt}",
        f"",
        f"  Entry `{r.entry}` · SL `{r.sl}` · TP1 `{r.tp1}`",
        f"",
        f"  `4h·1h·15m`",
        f"  EMA   {_ei(i4h.get('ema_trend',0))}·{_ei(i1h.get('ema_trend',0))}·{_ei(i15.get('ema_trend',0))}",
        f"  MACD  {_ei(i4h.get('macd',0))}·{_ei(i1h.get('macd',0))}·{_ei(i15.get('macd',0))}",
        f"  RSI   {_ei(i4h.get('rsi',0))}·{_ei(i1h.get('rsi',0))}·{_ei(i15.get('rsi',0))} _({i15.get('rsi_val',0)})",
        f"  ST    {_ei(i4h.get('supertrend',0))}·{_ei(i1h.get('supertrend',0))}·{_ei(i15.get('supertrend',0))}",
        f"  Vol   {_ei(i15.get('volume',0))} ×{i15.get('vol_ratio',1.0)}",
    ]
    return "\n".join(lines)
