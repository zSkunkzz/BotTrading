#!/usr/bin/env python3
"""
signal_engine.py — Motor de análisis técnico multi-timeframe
Portado de SignalBot v5.0 y adaptado para BotTrading (Bitget swap)

Uso:
    from bot.signal_engine import analyze_pair, SignalResult

    result = analyze_pair(exch, "BTC/USDT:USDT")
    if result.signal in ("LONG", "SHORT") and result.score >= 6:
        # ejecutar trade con result.entry, result.sl, result.tp1...
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
    ta_lib = None  # se lanza error claro más abajo

log = logging.getLogger(__name__)

# ── Parámetros por defecto (sobreescribibles por env) ────────────────────────
MIN_SCORE   = 6      # Score mínimo /10 para considerar señal válida
MIN_RR      = 1.8    # Ratio Riesgo/Beneficio mínimo
ATR_MULT_SL = 1.5    # Multiplicador ATR para Stop Loss
TP1_MULT    = 2.5    # R×2.5 → cerrar 40%
TP2_MULT    = 4.0    # R×4.0 → cerrar 35%
TP3_MULT    = 7.0    # R×7.0 → dejar correr 25%


@dataclass
class SignalResult:
    symbol: str
    signal: str = "NEUTRAL"   # LONG | SHORT | NEUTRAL
    score: int = 0
    max_score: int = 10
    entry: float = 0.0
    sl: float = 0.0
    tp1: float = 0.0
    tp2: float = 0.0
    tp3: float = 0.0
    rr: float = 0.0
    atr: float = 0.0
    suggested_lev: int = 1
    pct_tp3: float = 0.0
    indicators: dict = field(default_factory=dict)
    error: Optional[str] = None

    @property
    def is_valid(self) -> bool:
        return self.signal in ("LONG", "SHORT") and self.score >= MIN_SCORE and self.rr >= MIN_RR

    def summary(self) -> str:
        """Texto corto para logs / notificaciones."""
        if not self.is_valid:
            return f"{self.symbol} · NEUTRAL · Score {self.score}/10"
        dir_emoji = "🟢" if self.signal == "LONG" else "🔴"
        return (
            f"{dir_emoji} {self.symbol} · {self.signal} · Score {self.score}/10 · "
            f"R/R {self.rr:.1f} · Entry {self.entry:.4f} · "
            f"SL {self.sl:.4f} · TP1 {self.tp1:.4f}"
        )


# ── Utilidades OHLCV ────────────────────────────────────────────────────────

def _fetch_ohlcv(exch, symbol: str, tf: str, limit: int = 200) -> pd.DataFrame:
    """Devuelve DataFrame OHLCV con índice datetime UTC."""
    try:
        raw = exch.fetch_ohlcv(symbol, tf, limit=limit)
        df = pd.DataFrame(raw, columns=["ts", "open", "high", "low", "close", "volume"])
        df["ts"] = pd.to_datetime(df["ts"], unit="ms", utc=True)
        return df.set_index("ts").astype(float)
    except Exception as e:
        log.warning(f"[OHLCV] {symbol} {tf}: {e}")
        return pd.DataFrame()


# ── SuperTrend manual ────────────────────────────────────────────────────────

def _supertrend_dir(df: pd.DataFrame, period: int = 10, mult: float = 3.0) -> int:
    """Devuelve 1 (bullish), -1 (bearish) o 0 (indeterminado)."""
    try:
        h, l, c = df["high"], df["low"], df["close"]
        if len(c) < period + 5:
            return 0
        tr = pd.concat(
            [h - l, (h - c.shift()).abs(), (l - c.shift()).abs()], axis=1
        ).max(axis=1)
        atr = tr.rolling(period).mean()
        hl2 = (h + l) / 2
        ub = hl2 + mult * atr
        lb = hl2 - mult * atr
        st = pd.Series(np.nan, index=c.index)
        trend = pd.Series(1, index=c.index)
        for i in range(1, len(c)):
            prev = st.iloc[i - 1] if not pd.isna(st.iloc[i - 1]) else lb.iloc[i]
            if c.iloc[i] > prev:
                st.iloc[i] = lb.iloc[i]
                trend.iloc[i] = 1
            else:
                st.iloc[i] = ub.iloc[i]
                trend.iloc[i] = -1
        return int(trend.iloc[-1])
    except Exception:
        return 0


# ── Análisis de un timeframe ─────────────────────────────────────────────────

def _analyze_tf(df: pd.DataFrame) -> dict:
    """Calcula todos los indicadores para un DataFrame OHLCV."""
    if ta_lib is None:
        raise ImportError("Instala 'ta': pip install ta")
    if df.empty or len(df) < 55:
        return {}

    c, h, l, v = df["close"], df["high"], df["low"], df["volume"]
    s: dict = {}

    # 1. EMA Trend 9/21/50/200
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

    # 2. RSI 14
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

    # 3. MACD 12/26/9
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

    # 4. Bollinger Bands 20/2
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

    # 5. Stochastic RSI 14/3/3
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

    # 6. SuperTrend
    s["supertrend"] = _supertrend_dir(df)

    # 7. Volumen
    try:
        vm = v.rolling(20).mean().iloc[-1]
        vr = v.iloc[-1] / vm if vm > 0 else 1.0
        up = c.iloc[-1] > c.iloc[-2]
        s["vol_ratio"] = round(vr, 2)
        s["volume"] = (1 if up else -1) if vr > 1.5 else 0
    except Exception:
        s["volume"] = 0; s["vol_ratio"] = 1.0

    return s


# ── Scoring ponderado multi-TF ───────────────────────────────────────────────

def _compute_score(s4h: dict, s1h: dict, s15: dict) -> tuple[int, int, str]:
    """
    Retorna (score_long, score_short, direction).
    Distribución: 4h=3pts, 1h=3pts, 15m=4pts + bonus BB = max 11 → cap 10
    """
    sl = ss = 0

    # 4H: EMA trend + MACD + EMA200
    for key in ("ema_trend", "macd", "ema200"):
        sl += max(0,  s4h.get(key, 0))
        ss += max(0, -s4h.get(key, 0))

    # 1H: EMA trend + RSI + SuperTrend
    for key in ("ema_trend", "rsi", "supertrend"):
        sl += max(0,  s1h.get(key, 0))
        ss += max(0, -s1h.get(key, 0))

    # 15M: EMA trend + MACD + StochRSI + Volumen
    for key in ("ema_trend", "macd", "stoch", "volume"):
        sl += max(0,  s15.get(key, 0))
        ss += max(0, -s15.get(key, 0))

    # Bonus Bollinger doble confirmación
    if s15.get("bb", 0) == 1  and s1h.get("bb", 0) == 1:  sl += 1
    if s15.get("bb", 0) == -1 and s1h.get("bb", 0) == -1: ss += 1

    best  = max(sl, ss)
    score = min(best, 10)  # cap a 10
    direction = "LONG" if sl >= ss else "SHORT"
    return score, min(sl, 10), direction


# ── Función principal ────────────────────────────────────────────────────────

def analyze_pair(exch, symbol: str) -> SignalResult:
    """
    Analiza un par en 3 timeframes y devuelve un SignalResult.

    exch    → instancia ccxt con options defaultType="swap" (Bitget)
    symbol  → p.ej. "BTC/USDT:USDT" (formato perpetuo Bitget)
    """
    result = SignalResult(symbol=symbol)

    try:
        df15 = _fetch_ohlcv(exch, symbol, "15m", 200)
        df1h = _fetch_ohlcv(exch, symbol, "1h",  200)
        df4h = _fetch_ohlcv(exch, symbol, "4h",  200)

        if df15.empty or len(df15) < 55:
            result.error = "Datos insuficientes 15m"
            return result

        s15 = _analyze_tf(df15)
        s1h = _analyze_tf(df1h) if not df1h.empty else {}
        s4h = _analyze_tf(df4h) if not df4h.empty else {}
        result.indicators = {"15m": s15, "1h": s1h, "4h": s4h}

        score, _, direction = _compute_score(s4h, s1h, s15)
        result.score = score

        if score < MIN_SCORE:
            return result  # NEUTRAL, score bajo

        # ── Calcular ATR y niveles ────────────────────────────────────────────
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
            return result  # R/R insuficiente

        pct_tp3    = round(abs(tp3 - entry) / entry * 100, 2)
        # Apalancamiento sugerido: inverso de la volatilidad (1-10x)
        atr_pct    = atr / entry * 100
        suggested  = min(10, max(1, round(5 / atr_pct))) if atr_pct > 0 else 1

        result.signal        = direction
        result.entry         = round(entry, 6)
        result.sl            = round(sl,    6)
        result.tp1           = round(tp1,   6)
        result.tp2           = round(tp2,   6)
        result.tp3           = round(tp3,   6)
        result.rr            = rr
        result.pct_tp3       = pct_tp3
        result.suggested_lev = suggested

    except Exception as e:
        result.error = str(e)
        log.error(f"[signal_engine] {symbol}: {e}")

    return result


# ── Helper: formato indicadores para Telegram ────────────────────────────────

def _ei(v: int) -> str:
    return "🟢" if v == 1 else ("🔴" if v == -1 else "⚪")


def format_signal_block(r: SignalResult) -> str:
    """Bloque de texto Markdown para añadir a notificaciones de BotTrading."""
    if not r.is_valid:
        return f"📊 Score técnico: `{r.score}/10` — sin señal clara"

    i15 = r.indicators.get("15m", {})
    i1h = r.indicators.get("1h",  {})
    i4h = r.indicators.get("4h",  {})
    d   = r.signal

    lines = [
        f"📊 *Análisis técnico* · Score `{r.score}/10` · R/R `{r.rr}:1`",
        f"{'🟢 LONG' if d == 'LONG' else '🔴 SHORT'} · Lev sugerido `{r.suggested_lev}x`",
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
