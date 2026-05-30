#!/usr/bin/env python3
"""
signal_engine.py — Motor de análisis técnico multi-timeframe (ASYNC)

Modos de entrada (se exporta entry_mode en SignalResult):
  EARLY   score 5-6, cualquier alineación de 1h+15m            → lev 5-8x
  NORMAL  score 6-7, todos los TF alineados                    → lev 8-14x
  STRONG  score 8+, confluencia máxima                         → lev 14-15x

Cambios v3:
  - CVD (15m): delta acumulado de volumen, confirma presión compradora/vendedora
  - ADX (4h + 1h): filtra mercados sin tendencia (ADX<20 penaliza, ADX>25 suma)
  - VWAP desviación (15m): evita entradas sobreextendidas (>2% del VWAP)
  - Filtro 1D: penaliza -2 si la tendencia diaria va en contra de la señal
  - Score máximo teórico sube de 10 a 14 antes de microestructura

Variables de entorno:
  DAILY_TF_FILTER   (default: true) — activa/desactiva penalización por 1D
  DAILY_TF_PENALTY  (default: 2)    — puntos a restar cuando 1D va en contra
"""

from __future__ import annotations

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

MIN_SCORE       = 5
MIN_SCORE_FULL  = 6
MIN_RR          = 1.8
ATR_MULT_SL     = 1.2
TP1_MULT        = 2.5
TP2_MULT        = 4.0
TP3_MULT        = 7.0

LEV_EARLY_MIN   = 5
LEV_EARLY_MAX   = 8
LEV_NORMAL_MIN  = 8
LEV_NORMAL_MAX  = 14
LEV_STRONG_MIN  = 14
LEV_STRONG_MAX  = 15

EARLY_SIZE_RATIO = 0.5

OB_IMBALANCE_THRESHOLD    = 0.15
FUNDING_EXTREME_THRESHOLD = 0.0005

DAILY_TF_FILTER  = os.getenv("DAILY_TF_FILTER",  "true").lower() != "false"
DAILY_TF_PENALTY = int(os.getenv("DAILY_TF_PENALTY", "2"))


@dataclass
class SignalResult:
    symbol: str
    signal: str      = "NEUTRAL"
    score: int       = 0
    max_score: int   = 14
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
    ob_imbalance: Optional[float]  = None
    funding_rate: Optional[float]  = None
    # v3 extras
    adx_1h: Optional[float]   = None
    vwap_dev: Optional[float] = None
    cvd_slope: Optional[float] = None
    daily_trend: Optional[int] = None
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
            return f"{self.symbol} · NEUTRAL · Score {self.score}/14"
        em   = f"[{self.entry_mode}]"
        icon = "🟢" if self.signal == "LONG" else "🔴"
        extras = []
        if self.ob_imbalance is not None:
            extras.append(f"OB {self.ob_imbalance:+.2f}")
        if self.funding_rate is not None:
            extras.append(f"FR {self.funding_rate*100:+.4f}%")
        if self.adx_1h is not None:
            extras.append(f"ADX {self.adx_1h:.1f}")
        if self.vwap_dev is not None:
            extras.append(f"VWAP {self.vwap_dev:+.3f}%")
        extra_str = " · " + " · ".join(extras) if extras else ""
        return (
            f"{icon} {self.symbol} · {self.signal} {em} · Score {self.score}/14 · "
            f"R/R {self.rr:.1f} · Lev {self.suggested_lev}x · "
            f"Entry {self.entry:.4f} · SL {self.sl:.4f} · TP1 {self.tp1:.4f}{extra_str}"
        )


async def _fetch_ohlcv(exch, symbol: str, tf: str, limit: int = 200) -> pd.DataFrame:
    try:
        from bot.ws_feed import ws_feed
        sym_clean = symbol.replace("/", "").replace(":USDT", "")
        df = ws_feed.get_ohlcv(sym_clean, tf)
        if not df.empty and len(df) >= 55:
            log.debug(f"[OHLCV] {symbol} {tf} ← WS ({len(df)} velas)")
            return df
        log.debug(f"[OHLCV] {symbol} {tf} WS insuficiente ({len(df)} velas), usando REST")
    except Exception as e:
        log.debug(f"[OHLCV] {symbol} {tf} WS error: {e}, usando REST")

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

    # --- EMA stack ---
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

    # --- RSI ---
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

    # --- MACD ---
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

    # --- Bollinger Bands ---
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

    # --- StochRSI ---
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

    # --- Supertrend ---
    s["supertrend"] = _supertrend_dir(df)

    # --- Volume ratio ---
    try:
        vm = v.rolling(20).mean().iloc[-1]
        vr = v.iloc[-1] / vm if vm > 0 else 1.0
        up = c.iloc[-1] > c.iloc[-2]
        s["vol_ratio"] = round(vr, 2)
        s["volume"] = (1 if up else -1) if vr > 1.5 else 0
    except Exception:
        s["volume"] = 0; s["vol_ratio"] = 1.0

    # --- v3: ADX ---
    try:
        adx_ind = ta_lib.trend.ADXIndicator(h, l, c, window=14)
        adx_val = adx_ind.adx().iloc[-1]
        s["adx"] = round(adx_val, 1)
        # >25 = tendencia clara (+1), <20 = rango (-1), entre medio = neutro
        s["adx_trend"] = 1 if adx_val > 25 else (-1 if adx_val < 20 else 0)
    except Exception:
        s["adx"] = 0.0; s["adx_trend"] = 0

    # --- v3: CVD (Cumulative Volume Delta) slope últimas 5 velas ---
    try:
        prev_close = c.shift(1)
        delta = np.where(c.values > prev_close.values, v.values, -v.values)
        cvd = pd.Series(delta, index=c.index).cumsum()
        cvd_slope = float(cvd.iloc[-1] - cvd.iloc[-6]) if len(cvd) >= 6 else 0.0
        s["cvd_slope"] = round(cvd_slope, 2)
        s["cvd"] = 1 if cvd_slope > 0 else (-1 if cvd_slope < 0 else 0)
    except Exception:
        s["cvd"] = 0; s["cvd_slope"] = 0.0

    # --- v3: VWAP deviation ---
    try:
        typical = (h + l + c) / 3.0
        vwap = (typical * v).cumsum() / v.cumsum()
        vwap_last = float(vwap.iloc[-1])
        px_last   = float(c.iloc[-1])
        dev = (px_last - vwap_last) / vwap_last  # fraccion, e.g. 0.003 = +0.3%
        s["vwap_dev"] = round(dev * 100, 3)       # guardamos en %
        # +1: ligeramente sobre VWAP para LONG (0% a +0.5%), -1 si sobreextendido (>2%)
        # La direccionalidad se aplica en _compute_score segun direction
        if   0 < dev < 0.005:   s["vwap"] = 1    # precio justo sobre VWAP → bueno para long
        elif -0.005 < dev < 0:  s["vwap"] = -1   # precio justo bajo VWAP  → bueno para short
        elif dev > 0.02:        s["vwap"] = -1   # sobreextendido arriba   → malo para long
        elif dev < -0.02:       s["vwap"] = 1    # sobreextendido abajo    → malo para short
        else:                   s["vwap"] = 0
    except Exception:
        s["vwap"] = 0; s["vwap_dev"] = 0.0

    return s


def _compute_score(s4h: dict, s1h: dict, s15: dict) -> tuple[int, int, str]:
    sl = ss = 0

    # 4h: EMA stack, MACD, EMA200, ADX (v3)
    for key in ("ema_trend", "macd", "ema200", "adx_trend"):
        sl += max(0,  s4h.get(key, 0))
        ss += max(0, -s4h.get(key, 0))

    # 1h: EMA stack, RSI, Supertrend, ADX (v3)
    for key in ("ema_trend", "rsi", "supertrend", "adx_trend"):
        sl += max(0,  s1h.get(key, 0))
        ss += max(0, -s1h.get(key, 0))

    # 15m: EMA stack, MACD, StochRSI, Volume, CVD (v3), VWAP (v3)
    for key in ("ema_trend", "macd", "stoch", "volume", "cvd", "vwap"):
        sl += max(0,  s15.get(key, 0))
        ss += max(0, -s15.get(key, 0))

    # BB conjunto 15m+1h (sin cambios)
    if s15.get("bb", 0) == 1  and s1h.get("bb", 0) == 1:  sl += 1
    if s15.get("bb", 0) == -1 and s1h.get("bb", 0) == -1: ss += 1

    best      = max(sl, ss)
    score     = min(best, 14)   # techo subido de 10 a 14
    direction = "LONG" if sl >= ss else "SHORT"
    return score, min(sl, 14), direction


def _apply_microstructure(
    score: int,
    direction: str,
    ob_metrics: Optional[dict],
    funding_rate: Optional[float],
) -> tuple[int, Optional[float], Optional[float]]:
    ob_imbalance_val = None
    fr_val           = None
    bonus            = 0

    if ob_metrics and isinstance(ob_metrics, dict):
        imbalance = ob_metrics.get("imbalance", 0.0)
        ob_imbalance_val = imbalance
        if direction == "LONG"  and imbalance >  OB_IMBALANCE_THRESHOLD:
            bonus += 1
        elif direction == "SHORT" and imbalance < -OB_IMBALANCE_THRESHOLD:
            bonus += 1
        elif direction == "LONG"  and imbalance < -OB_IMBALANCE_THRESHOLD:
            bonus -= 1
        elif direction == "SHORT" and imbalance >  OB_IMBALANCE_THRESHOLD:
            bonus -= 1

    if funding_rate is not None:
        fr_val = funding_rate
        if direction == "LONG":
            if funding_rate > FUNDING_EXTREME_THRESHOLD:
                bonus -= 1
            elif funding_rate < -FUNDING_EXTREME_THRESHOLD:
                bonus += 1
        else:
            if funding_rate < -FUNDING_EXTREME_THRESHOLD:
                bonus -= 1
            elif funding_rate > FUNDING_EXTREME_THRESHOLD:
                bonus += 1

    adjusted = max(0, score + bonus)
    return adjusted, ob_imbalance_val, fr_val


def _classify_entry_mode(score: int, s4h: dict, s1h: dict, s15: dict, direction: str) -> tuple[str, int, float]:
    sign = 1 if direction == "LONG" else -1

    tf1h_aligned = s1h.get("ema_trend", 0) * sign
    tf15_aligned = s15.get("ema_trend", 0) * sign

    extra_1h  = sum(1 for k in ("rsi", "supertrend", "macd") if s1h.get(k, 0) * sign > 0)
    extra_15m = sum(1 for k in ("macd", "stoch", "volume") if s15.get(k, 0) * sign > 0)

    if score >= 8:
        mode  = "STRONG"
        ratio = min((score - 8) / 6.0, 1.0)   # ajustado al nuevo techo de 14
        lev   = round(LEV_STRONG_MIN + ratio * (LEV_STRONG_MAX - LEV_STRONG_MIN))
        return mode, lev, 1.0

    if score >= MIN_SCORE_FULL:  # >= 6
        mode  = "NORMAL"
        ratio = min((score - MIN_SCORE_FULL) / 4.0, 1.0)  # ajustado al nuevo techo
        lev   = round(LEV_NORMAL_MIN + ratio * (LEV_NORMAL_MAX - LEV_NORMAL_MIN))
        return mode, lev, 1.0

    if score == MIN_SCORE and tf1h_aligned > 0 and tf15_aligned > 0:
        quality = extra_1h + extra_15m
        ratio   = min(quality / 6.0, 1.0)
        lev     = round(LEV_EARLY_MIN + ratio * (LEV_EARLY_MAX - LEV_EARLY_MIN))
        log.debug(
            f"[signal] EARLY activado — score=5, 1h={tf1h_aligned}, 15m={tf15_aligned}, "
            f"quality={quality}, lev={lev}x"
        )
        return "EARLY", lev, EARLY_SIZE_RATIO

    log.debug(
        f"[signal] NONE — score={score}, 1h_aligned={tf1h_aligned}, "
        f"15m_aligned={tf15_aligned} (señal descartada)"
    )
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

        # Guardar métricas v3 en el resultado
        result.adx_1h    = s1h.get("adx")      if s1h else None
        result.vwap_dev  = s15.get("vwap_dev")  if s15 else None
        result.cvd_slope = s15.get("cvd_slope") if s15 else None

        score_base, _, direction = _compute_score(s4h, s1h, s15)

        # --- v3: Filtro 1D ---
        daily_trend = 0
        if DAILY_TF_FILTER:
            try:
                df1d = await _fetch_ohlcv(exch, symbol, "1d", 50)
                if not df1d.empty and len(df1d) >= 20:
                    s1d = _analyze_tf(df1d)
                    daily_trend = s1d.get("ema_trend", 0)
                    sign = 1 if direction == "LONG" else -1
                    if daily_trend != 0 and daily_trend * sign < 0:
                        penalty = DAILY_TF_PENALTY
                        log.info(
                            f"[signal_engine] {symbol} 1D contratendencia "
                            f"(daily={daily_trend}, signal={direction}) → −{penalty} score"
                        )
                        score_base = max(0, score_base - penalty)
            except Exception as e:
                log.debug(f"[signal_engine] {symbol} 1D filter error: {e}")
        result.daily_trend = daily_trend

        ob_metrics   = None
        funding_rate = None
        try:
            from bot.ws_feed import ws_feed
            sym_clean    = symbol.replace("/", "").replace(":USDT", "")
            ob_metrics   = ws_feed.get_orderbook_metrics(sym_clean)
            funding_rate = ws_feed.get_funding_rate(sym_clean)
        except Exception as e:
            log.debug(f"[signal_engine] microestructura no disponible: {e}")

        score, ob_imbalance_val, fr_val = _apply_microstructure(
            score_base, direction, ob_metrics, funding_rate
        )
        result.score        = score
        result.ob_imbalance = ob_imbalance_val
        result.funding_rate = fr_val

        mode, lev, size_ratio = _classify_entry_mode(score, s4h, s1h, s15, direction)

        if mode == "NONE":
            log.info(
                f"[signal_engine] {symbol} descartado — score={score}/14, "
                f"modo=NONE, dir={direction}"
            )
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
            log.info(
                f"[signal_engine] {symbol} descartado — R/R {rr:.2f} < {MIN_RR} "
                f"(score={score}, modo={mode})"
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
        log.error(f"[signal_engine] {symbol}: {e}")

    return result


def _ei(v: int) -> str:
    return "🟢" if v == 1 else ("🔴" if v == -1 else "⚪")


def _mode_emoji(mode: str) -> str:
    return {"EARLY": "🔸", "NORMAL": "🔷", "STRONG": "💥"}.get(mode, "⚪")


def format_signal_block(r: SignalResult) -> str:
    if not r.is_valid:
        return f"📊 Score técnico: `{r.score}/14` — sin señal clara"

    i15 = r.indicators.get("15m", {})
    i1h = r.indicators.get("1h",  {})
    i4h = r.indicators.get("4h",  {})
    d   = r.signal
    me  = _mode_emoji(r.entry_mode)

    size_txt = f" · Size `{int(r.size_ratio*100)}%`" if r.size_ratio < 1.0 else ""

    ob_txt = ""
    if r.ob_imbalance is not None:
        arrow = "↑" if r.ob_imbalance > 0.05 else ("↓" if r.ob_imbalance < -0.05 else "→")
        ob_txt = f"\n  OB {arrow} `{r.ob_imbalance:+.3f}`"

    fr_txt = ""
    if r.funding_rate is not None:
        fr_pct = r.funding_rate * 100
        emoji  = "🔥" if abs(fr_pct) > 0.05 else "⚪"
        fr_txt = f"\n  Funding {emoji} `{fr_pct:+.4f}%`"

    # v3 extras
    adx_txt = ""
    if r.adx_1h is not None:
        adx_emoji = "🟢" if r.adx_1h > 25 else ("🔴" if r.adx_1h < 20 else "⚪")
        adx_txt = f"\n  ADX(1h) {adx_emoji} `{r.adx_1h:.1f}`"

    vwap_txt = ""
    if r.vwap_dev is not None:
        vwap_txt = f"\n  VWAP dev `{r.vwap_dev:+.3f}%`"

    cvd_txt = ""
    if r.cvd_slope is not None:
        cvd_arrow = "↑" if r.cvd_slope > 0 else "↓"
        cvd_txt = f"\n  CVD {cvd_arrow} `{r.cvd_slope:+.0f}`"

    daily_txt = ""
    if r.daily_trend is not None and r.daily_trend != 0:
        daily_emoji = "🟢" if r.daily_trend == 1 else "🔴"
        daily_txt = f"\n  1D trend {daily_emoji}"

    lines = [
        f"📊 *Análisis técnico* · Score `{r.score}/14` · R/R `{r.rr}:1`",
        f"{'\U0001f7e2 LONG' if d == 'LONG' else '\U0001f534 SHORT'} · Modo {me}`{r.entry_mode}` · Lev `{r.suggested_lev}x`{size_txt}",
        f"",
        f"  Entry `{r.entry}` · SL `{r.sl}` · TP1 `{r.tp1}`",
        f"",
        f"  `4h·1h·15m`",
        f"  EMA   {_ei(i4h.get('ema_trend',0))}·{_ei(i1h.get('ema_trend',0))}·{_ei(i15.get('ema_trend',0))}",
        f"  MACD  {_ei(i4h.get('macd',0))}·{_ei(i1h.get('macd',0))}·{_ei(i15.get('macd',0))}",
        f"  RSI   {_ei(i4h.get('rsi',0))}·{_ei(i1h.get('rsi',0))}·{_ei(i15.get('rsi',0))} _({i15.get('rsi_val',0)})",
        f"  ST    {_ei(i4h.get('supertrend',0))}·{_ei(i1h.get('supertrend',0))}·{_ei(i15.get('supertrend',0))}",
        f"  ADX   {_ei(i4h.get('adx_trend',0))}·{_ei(i1h.get('adx_trend',0))}·⚪",
        f"  CVD   ⚪·⚪·{_ei(i15.get('cvd',0))} _{i15.get('cvd_slope',0):+.0f}_",
        f"  Vol   {_ei(i15.get('volume',0))} ×{i15.get('vol_ratio',1.0)}",
    ]
    return "\n".join(lines) + ob_txt + fr_txt + adx_txt + vwap_txt + cvd_txt + daily_txt
