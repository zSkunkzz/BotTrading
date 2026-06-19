"""signals.py — Sistema de señales premium con scoring 0-100.

Filtros:
  1. Vela cerrada       : penúltima vela (ya cerrada)
  2. Régimen de mercado : clasificación por EMAs 1h — requiere alineación ESTRICTA
                          price > ema20 > ema50 > ema200 (bull) o inverso (bear).
                          Requiere REGIME_CONFIRM_BARS velas 1h consecutivas confirmando.
  3. Macro 4h           : EMA50 en 4h — bonus/penalización/neutro según disponibilidad
  4. EMA200 1h          : hard-guard de dirección
  5. ADX 15m            : <18 → HARD-GUARD (sin señal) | >35 +12 | >25 +6 | >18 -8
  6. ADX 1h             : >25 +5, <18 -5 (fuerza de tendencia en marco superior)
  7. Volumen            : última vela CERRADA >media×1.2 → +8
  8. RSI 15m            : cruce 50 +15, extremo +8, direccional +4, contrario -5
  9. MACD 15m + 1h      : histograma acelerando +10 | positivo/negativo sin acelerar 0
                          | contrario -5
  10. Divergencia RSI 15m: +8
  11. Divergencia RSI 1h : +12 (marco superior — más fiable, menos ruido)
  12. Sesgo horario     : hora alta +8, hora baja -10
  13. Filtro no-chase   : rango vela ≤2×ATR (hard-guard)

Score base: 20 pts (por superar hard-guards)
Macro 4h:  +15 a favor | 0 si sin datos | -10 en contra
Sizing en risk.py: mult=0.7 (score<70) | 1.0 (70-84) | 1.4 (≥85)
MIN_SCORE configurable via env var MIN_SCORE (default 55)

NOTA sobre divergencia RSI 1h:
  - Usa las últimas 15 velas 1h cerradas (ventana mayor = pivots más sólidos).
  - Bullish: precio hace nuevo mínimo pero RSI no — momentum alcista oculto.
  - Bearish: precio hace nuevo máximo pero RSI no — momentum bajista oculto.
  - Se calcula sobre closed_1h (no incluye la vela abierta actual).
  - Peso +12 vs +8 del 15m porque el marco superior tiene menos ruido.
  - Independiente de la divergencia 15m: pueden darse ambas a la vez (+20).
"""
from __future__ import annotations
import logging
import datetime
from datetime import timezone

import config

log = logging.getLogger("signals")

# ── Umbrales configurables ────────────────────────────────────────────
EMA200_MIN_DIST     = 0.003
NO_CHASE_MULT       = 2.0
VOLUME_MULT         = 1.2
MIN_SCORE           = config.MIN_SCORE
REGIME_CONFIRM_BARS = 2
ADX_MIN             = 18   # por debajo → hard-guard, sin señal

HIGH_BIAS_HOURS = {8, 9, 10, 14, 15, 16, 20, 21}
LOW_BIAS_HOURS  = {2, 3, 4, 5}


# ── Indicadores ─────────────────────────────────────────────────────────────────────────

def _ema(closes: list[float], period: int) -> list[float]:
    k = 2 / (period + 1)
    emas = [closes[0]]
    for c in closes[1:]:
        emas.append(c * k + emas[-1] * (1 - k))
    return emas


def _rsi(closes: list[float], period: int = 14) -> list[float]:
    rsi = [50.0] * len(closes)
    if len(closes) < period + 1:
        return rsi
    deltas = [closes[i] - closes[i - 1] for i in range(1, len(closes))]
    avg_gain = sum(d for d in deltas[:period] if d > 0) / period
    avg_loss = sum(-d for d in deltas[:period] if d < 0) / period
    for i in range(period, len(closes)):
        delta = deltas[i - 1]
        avg_gain = (avg_gain * (period - 1) + max(delta, 0)) / period
        avg_loss = (avg_loss * (period - 1) + max(-delta, 0)) / period
        rs = avg_gain / avg_loss if avg_loss else float("inf")
        rsi[i] = 100.0 if avg_loss == 0 else 100 - (100 / (1 + rs))
    return rsi


def _macd_histogram(closes: list[float], fast=12, slow=26, signal=9) -> list[float]:
    ema_fast  = _ema(closes, fast)
    ema_slow  = _ema(closes, slow)
    macd_line = [f - s for f, s in zip(ema_fast, ema_slow)]
    sig_line  = _ema(macd_line, signal)
    return [m - s for m, s in zip(macd_line, sig_line)]


def _atr(candles: list[dict], period: int = 14) -> float:
    trs = []
    for i in range(1, len(candles)):
        h, l, pc = candles[i]["high"], candles[i]["low"], candles[i - 1]["close"]
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    return sum(trs[-period:]) / min(period, len(trs)) if trs else 0.0


def _adx(candles: list[dict], period: int = 14) -> float:
    if len(candles) < period + 2:
        return 0.0
    plus_dm, minus_dm, tr_list = [], [], []
    for i in range(1, len(candles)):
        h, l   = candles[i]["high"],   candles[i]["low"]
        ph, pl = candles[i-1]["high"], candles[i-1]["low"]
        pc     = candles[i-1]["close"]
        up, down = h - ph, pl - l
        plus_dm.append(up   if up > down and up > 0   else 0.0)
        minus_dm.append(down if down > up and down > 0 else 0.0)
        tr_list.append(max(h - l, abs(h - pc), abs(l - pc)))

    def _smooth(lst):
        s = sum(lst[:period])
        out = [s]
        for v in lst[period:]:
            s = s - s / period + v
            out.append(s)
        return out

    atr_s = _smooth(tr_list)
    pdm_s = _smooth(plus_dm)
    mdm_s = _smooth(minus_dm)
    dx_vals = []
    for a, p, m in zip(atr_s, pdm_s, mdm_s):
        if a == 0:
            dx_vals.append(0.0)
            continue
        pdi = 100 * p / a
        mdi = 100 * m / a
        dx_vals.append(100 * abs(pdi - mdi) / (pdi + mdi) if (pdi + mdi) else 0.0)
    if len(dx_vals) < period:
        return 0.0
    adx = sum(dx_vals[:period]) / period
    for dx in dx_vals[period:]:
        adx = (adx * (period - 1) + dx) / period
    return adx


def _rsi_divergence(
    closes: list[float],
    candles: list[dict],
    lookback: int = 10,
) -> str | None:
    """Detecta divergencia RSI-precio en velas de cualquier timeframe.

    Bullish: precio hace nuevo mínimo pero RSI no — momentum alcista oculto.
    Bearish: precio hace nuevo máximo pero RSI no — momentum bajista oculto.

    Se evalúa sobre las últimas `lookback` velas ya cerradas.
    """
    if len(closes) < lookback + 14:
        return None
    rsi_series    = _rsi(closes, 14)
    recent_closes = closes[-lookback:]
    recent_rsi    = rsi_series[-lookback:]

    # Busca pivot de mínimo en los primeros (lookback - 3) puntos
    lo_val = min(recent_closes[:-3])
    lo_idx = max(i for i, v in enumerate(recent_closes[:-3]) if v == lo_val)
    if recent_closes[-1] < lo_val and recent_rsi[-1] > recent_rsi[lo_idx] + 2:
        return "bullish"

    # Busca pivot de máximo en los primeros (lookback - 3) puntos
    hi_val = max(recent_closes[:-3])
    hi_idx = max(i for i, v in enumerate(recent_closes[:-3]) if v == hi_val)
    if recent_closes[-1] > hi_val and recent_rsi[-1] < recent_rsi[hi_idx] - 2:
        return "bearish"

    return None


def _market_regime(candles_1h: list[dict]) -> tuple[str, float]:
    """Clasifica el régimen con alineación ESTRICTA de EMAs."""
    closes = [c["close"] for c in candles_1h]
    ema20  = _ema(closes, 20)[-1]
    ema50  = _ema(closes, 50)[-1]
    ema200 = _ema(closes, 200)[-1]
    adx    = _adx(candles_1h, 14)
    price  = closes[-1]

    if price > ema20 > ema50 > ema200:
        return "bull", adx
    if price < ema20 < ema50 < ema200:
        return "bear", adx
    return "range", adx


def _regime_confirmed(candles_1h: list[dict], n: int = REGIME_CONFIRM_BARS) -> tuple[str, float]:
    """Devuelve el régimen solo si las últimas n velas cerradas coinciden."""
    if len(candles_1h) < 200 + n + 1:
        return "range", 0.0

    regimes = []
    for offset in range(n, 0, -1):
        window = candles_1h[:-offset]
        regime, adx = _market_regime(window)
        regimes.append(regime)

    regime_now, adx_now = _market_regime(candles_1h)
    regimes.append(regime_now)

    if len(set(regimes)) == 1:
        log.debug("régimen confirmado ×%d: %s", n, regime_now)
        return regime_now, adx_now

    log.info("⚠️  Régimen inestable %s — esperando confirmación", regimes)
    return "range", adx_now


def _volume_ok(candles: list[dict], window: int = 20) -> bool:
    """Comprueba si el volumen de la última vela CERRADA supera la media."""
    if len(candles) < window + 2:
        return True
    vols = [c["volume"] for c in candles[-(window + 2):-2]]
    avg  = sum(vols) / len(vols)
    return candles[-2]["volume"] >= avg * VOLUME_MULT


# ── Función principal ────────────────────────────────────────────────────────────────────────

def evaluate(
    candles_15m: list[dict],
    candles_1h:  list[dict],
    candles_4h:  list[dict] | None = None,
    min_score:   int | None = None,
) -> tuple[str | None, int]:
    """Evalúa señales. min_score sobreescribe MIN_SCORE global si se especifica."""
    effective_min = min_score if min_score is not None else MIN_SCORE

    if len(candles_1h) < 210:
        return None, 0
    if len(candles_15m) < 60:
        return None, 0

    closed_15m = candles_15m[:-1]
    closed_1h  = candles_1h[:-1]
    closed_4h  = candles_4h[:-1] if candles_4h and len(candles_4h) > 1 else None

    closes_15m = [c["close"] for c in closed_15m]
    closes_1h  = [c["close"] for c in closed_1h]

    # ── Hard-guard 1: Régimen confirmado ────────────────────────────────
    regime, adx_1h = _regime_confirmed(closed_1h)
    if regime == "range":
        log.info("⬛ Régimen lateral o inestable — sin señal")
        return None, 0

    # ── Hard-guard 2: distancia EMA200 ──────────────────────────────────
    ema200 = _ema(closes_1h, 200)[-1]
    price  = closes_15m[-1]
    dist   = abs(price - ema200) / ema200
    if dist < EMA200_MIN_DIST:
        log.info("⚠️  Precio cerca del EMA200 (%.3f%%) — sin señal", dist * 100)
        return None, 0

    trend_long  = regime == "bull"
    trend_short = regime == "bear"

    # ── Hard-guard 3: no-chase ────────────────────────────────────────
    atr        = _atr(closed_15m, 14)
    last_range = closed_15m[-1]["high"] - closed_15m[-1]["low"]
    if atr > 0 and last_range > NO_CHASE_MULT * atr:
        log.info("⚠️  Vela explosiva — sin señal")
        return None, 0

    # ── Hard-guard 4: ADX 15m mínimo ─────────────────────────────────
    adx = _adx(closed_15m, 14)
    if adx < ADX_MIN:
        log.info("⚠️  ADX 15m demasiado bajo (%.1f < %d) — mercado sin momentum", adx, ADX_MIN)
        return None, 0

    # ── Componentes comunes ────────────────────────────────────────────
    hour_utc   = datetime.datetime.now(timezone.utc).hour
    hour_bonus = 8 if hour_utc in HIGH_BIAS_HOURS else (-10 if hour_utc in LOW_BIAS_HOURS else 0)

    macro_long = macro_short = None
    if closed_4h and len(closed_4h) >= 55:
        closes_4h   = [c["close"] for c in closed_4h]
        ema50_4h    = _ema(closes_4h, 50)[-1]
        price_4h    = closes_4h[-1]
        macro_long  = price_4h > ema50_4h
        macro_short = price_4h < ema50_4h

    rsi_series     = _rsi(closes_15m, 14)
    rsi_prev       = rsi_series[-2]
    rsi_curr       = rsi_series[-1]
    rsi_cross_up   = rsi_prev < 50 <= rsi_curr
    rsi_cross_down = rsi_prev > 50 >= rsi_curr
    rsi_bull        = rsi_curr > 50
    rsi_bear        = rsi_curr < 50
    rsi_ext_bull    = rsi_curr > 60
    rsi_ext_bear    = rsi_curr < 40

    # Divergencia RSI en 15m (ventana 10 velas, lookback corto = reacción rápida)
    divergence_15m = _rsi_divergence(closes_15m, closed_15m, lookback=10)

    # Divergencia RSI en 1h (ventana 15 velas, peso mayor porque el marco es superior).
    # Ventana más grande (15 vs 10) para capturar pivots de mayor entidad.
    # Requiere al menos 15+14=29 velas 1h cerradas.
    divergence_1h: str | None = None
    if len(closes_1h) >= 29:
        divergence_1h = _rsi_divergence(closes_1h, closed_1h, lookback=15)

    hist_15m = _macd_histogram(closes_15m)
    hist_1h  = _macd_histogram(closes_1h)

    macd15_bull_strong = hist_15m[-1] > 0 and hist_15m[-1] > hist_15m[-2]
    macd15_bull_weak   = hist_15m[-1] > 0
    macd15_bear_strong = hist_15m[-1] < 0 and hist_15m[-1] < hist_15m[-2]
    macd15_bear_weak   = hist_15m[-1] < 0

    macd1h_bull = hist_1h[-1] > 0
    macd1h_bear = hist_1h[-1] < 0

    vol_ok = _volume_ok(closed_15m)

    def _macro_pts(macro: bool | None, favor: bool) -> int:
        if macro is None:
            return 0
        return 15 if (macro and favor) else (-10 if (not macro and favor) else 0)

    # ── Scoring LONG ──────────────────────────────────────────────────
    def score_long() -> int:
        if not trend_long:
            return 0
        s = 20
        s += _macro_pts(macro_long, favor=True)
        if adx > 35:        s += 12
        elif adx > 25:      s += 6
        elif adx > 18:      s -= 8
        if adx_1h < 18:     s -= 5
        elif adx_1h > 25:   s += 5
        if rsi_cross_up:    s += 15
        elif rsi_ext_bull:  s += 8
        elif rsi_bull:      s += 4
        else:               s -= 5
        if macd15_bull_strong:    s += 10
        elif macd15_bull_weak:    s += 0
        elif not macd15_bear_weak: s += 0
        else:                     s -= 5
        if macd1h_bull:     s += 10
        else:               s -= 5
        if vol_ok:          s += 8
        # Divergencias (independientes entre sí, acumulables)
        if divergence_15m == "bullish":  s += 8
        if divergence_1h  == "bullish":  s += 12
        s += hour_bonus
        return min(max(s, 0), 100)

    # ── Scoring SHORT ─────────────────────────────────────────────────
    def score_short() -> int:
        if not trend_short:
            return 0
        s = 20
        s += _macro_pts(macro_short, favor=True)
        if adx > 35:         s += 12
        elif adx > 25:       s += 6
        elif adx > 18:       s -= 8
        if adx_1h < 18:      s -= 5
        elif adx_1h > 25:    s += 5
        if rsi_cross_down:   s += 15
        elif rsi_ext_bear:   s += 8
        elif rsi_bear:       s += 4
        else:                s -= 5
        if macd15_bear_strong:    s += 10
        elif macd15_bear_weak:    s += 0
        elif not macd15_bull_weak: s += 0
        else:                     s -= 5
        if macd1h_bear:      s += 10
        else:               s -= 5
        if vol_ok:           s += 8
        # Divergencias (independientes entre sí, acumulables)
        if divergence_15m == "bearish":  s += 8
        if divergence_1h  == "bearish":  s += 12
        s += hour_bonus
        return min(max(s, 0), 100)

    sc_long  = score_long()
    sc_short = score_short()

    macro_l_str = "None" if macro_long  is None else str(macro_long)
    macro_s_str = "None" if macro_short is None else str(macro_short)

    log.info(
        "regime=%s(×%d) adx1h=%.1f hour=%dUTC | price=%.4f ema200=%.4f dist=%.3f%% "
        "ADX15m=%.1f rsi=%.1f→%.1f vol=%s "
        "div15m=%s div1h=%s macro_l=%s macro_s=%s "
        "macd15=%.5f macd1h=%.5f | score_long=%d score_short=%d (min=%d)",
        regime, REGIME_CONFIRM_BARS, adx_1h, hour_utc, price, ema200, dist * 100,
        adx, rsi_prev, rsi_curr, vol_ok,
        divergence_15m, divergence_1h,
        macro_l_str, macro_s_str,
        hist_15m[-1], hist_1h[-1],
        sc_long, sc_short, effective_min,
    )

    if sc_long >= effective_min and sc_long >= sc_short:
        log.info("✅ LONG score=%d (div15m=%s div1h=%s)", sc_long, divergence_15m, divergence_1h)
        return "long", sc_long

    if sc_short >= effective_min:
        log.info("✅ SHORT score=%d (div15m=%s div1h=%s)", sc_short, divergence_15m, divergence_1h)
        return "short", sc_short

    log.info("⬛ Sin señal (score L=%d S=%d < %d)", sc_long, sc_short, effective_min)
    return None, 0
