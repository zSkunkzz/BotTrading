"""signals.py — Sistema de señales premium con scoring 0-100.

Filtros y mejoras implementadas:
  1. Vela cerrada       : penúltima vela (ya cerrada)
  2. Régimen de mercado : clasifica tendencia/rango — silencio en lateral puro
  3. Tendencia 4h       : EMA50 en 4h como filtro macro
  4. Tendencia 1h       : EMA200 + distancia mínima 0.3%
  5. ADX > 25           : mercado en tendencia
  6. Volumen            : vela de señal con volumen > media(20) × 1.2
  7. RSI 15m            : cruce del nivel 50 (vela cerrada)
  8. MACD 15m + 1h      : histograma confirma dirección en ambos TF
  9. Divergencia RSI    : bearish/bullish divergence como bonus de score
  10. Sesgo horario     : horas de alta directionalidad (08-10 UTC, 14-16 UTC)
  11. Filtro no-chase   : rango de vela ≤ 2×ATR
  12. Score 0-100       : señal proporcional, no binaria
"""
from __future__ import annotations
import logging
import datetime

log = logging.getLogger("signals")

# ── Umbrales configurables ────────────────────────────────────────────────────
EMA200_MIN_DIST   = 0.003   # 0.3% distancia mínima al EMA200 en 1h
ADX_THRESHOLD     = 25
NO_CHASE_MULT     = 2.0
VOLUME_MULT       = 1.2     # vela de señal debe tener volumen > 1.2× media(20)
MIN_SCORE         = 55      # score mínimo para emitir señal

# Horas UTC de alta directionalidad (sesgo estadístico)
HIGH_BIAS_HOURS   = {8, 9, 10, 14, 15, 16, 20, 21}
LOW_BIAS_HOURS    = {2, 3, 4, 5}   # horas con más ruido — penalización


# ── Indicadores ───────────────────────────────────────────────────────────────

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


def _rsi_divergence(closes: list[float], candles: list[dict], lookback: int = 10) -> str | None:
    """Detecta divergencia RSI en las últimas `lookback` velas.
    Devuelve 'bullish', 'bearish' o None."""
    if len(closes) < lookback + 14:
        return None
    rsi_series = _rsi(closes, 14)
    recent_closes = closes[-lookback:]
    recent_rsi    = rsi_series[-lookback:]

    # Buscar dos mínimos de precio con RSI creciente → divergencia alcista
    price_lo1 = min(recent_closes[:-3])
    price_lo2 = recent_closes[-1]
    idx_lo1   = recent_closes.index(price_lo1)
    rsi_lo1   = recent_rsi[idx_lo1]
    rsi_lo2   = recent_rsi[-1]
    if price_lo2 < price_lo1 and rsi_lo2 > rsi_lo1 + 2:
        return "bullish"

    # Buscar dos máximos de precio con RSI decreciente → divergencia bajista
    price_hi1 = max(recent_closes[:-3])
    price_hi2 = recent_closes[-1]
    idx_hi1   = recent_closes.index(price_hi1)
    rsi_hi1   = recent_rsi[idx_hi1]
    rsi_hi2   = recent_rsi[-1]
    if price_hi2 > price_hi1 and rsi_hi2 < rsi_hi1 - 2:
        return "bearish"

    return None


def _market_regime(candles_1h: list[dict]) -> str:
    """Clasifica el mercado: 'bull', 'bear', 'range'.
    Usa EMA20 vs EMA50 vs EMA200 + ADX.
    """
    closes = [c["close"] for c in candles_1h]
    ema20  = _ema(closes, 20)[-1]
    ema50  = _ema(closes, 50)[-1]
    ema200 = _ema(closes, 200)[-1]
    adx    = _adx(candles_1h, 14)
    price  = closes[-1]

    if adx < 18:
        return "range"   # lateral puro — silencio total

    if price > ema20 > ema50 > ema200:
        return "bull"
    if price < ema20 < ema50 < ema200:
        return "bear"
    return "range"


def _volume_ok(candles: list[dict], window: int = 20) -> bool:
    """True si el volumen de la última vela cerrada supera la media × VOLUME_MULT."""
    if len(candles) < window + 1:
        return True   # no hay datos suficientes, dejar pasar
    vols = [c["volume"] for c in candles[-(window + 1):-1]]
    avg  = sum(vols) / len(vols)
    last_vol = candles[-1]["volume"]
    return last_vol >= avg * VOLUME_MULT


# ── Función principal ─────────────────────────────────────────────────────────

def evaluate(
    candles_15m: list[dict],
    candles_1h:  list[dict],
    candles_4h:  list[dict] | None = None,
) -> tuple[str | None, int]:
    """
    Devuelve (direccion, score) donde:
      direccion : 'long' | 'short' | None
      score     : 0-100 — intensidad de la señal (usado para sizing)
    """
    if len(candles_1h) < 210:
        log.warning("Pocas velas 1h (%d/210)", len(candles_1h))
        return None, 0
    if len(candles_15m) < 60:
        log.warning("Pocas velas 15m (%d/60)", len(candles_15m))
        return None, 0

    closed_15m = candles_15m[:-1]
    closed_1h  = candles_1h[:-1]
    closed_4h  = candles_4h[:-1] if candles_4h and len(candles_4h) > 1 else None

    closes_15m = [c["close"] for c in closed_15m]
    closes_1h  = [c["close"] for c in closed_1h]

    # ── Régimen de mercado (filtro hard) ─────────────────────────────────────
    regime = _market_regime(closed_1h)
    if regime == "range":
        log.info("⬛ Régimen lateral — sin señal")
        return None, 0

    # ── Sesgo horario ────────────────────────────────────────────────────────
    hour_utc = datetime.datetime.utcnow().hour
    hour_bonus = 0
    if hour_utc in HIGH_BIAS_HOURS:
        hour_bonus = 10
    elif hour_utc in LOW_BIAS_HOURS:
        hour_bonus = -15   # penalización en horas ruidosas

    # ── Filtro macro 4h ──────────────────────────────────────────────────────
    macro_long = macro_short = True   # si no hay 4h, no bloquea
    if closed_4h and len(closed_4h) >= 55:
        closes_4h = [c["close"] for c in closed_4h]
        ema50_4h  = _ema(closes_4h, 50)[-1]
        price_4h  = closes_4h[-1]
        macro_long  = price_4h > ema50_4h
        macro_short = price_4h < ema50_4h

    # ── Tendencia 1h: EMA200 ─────────────────────────────────────────────────
    ema200 = _ema(closes_1h, 200)[-1]
    price  = closes_15m[-1]
    dist   = abs(price - ema200) / ema200
    if dist < EMA200_MIN_DIST:
        log.info("⚠️  Precio cerca del EMA200 (%.4f%%) — sin señal", dist * 100)
        return None, 0

    trend_long  = price > ema200
    trend_short = price < ema200

    # ── ADX ──────────────────────────────────────────────────────────────────
    adx = _adx(closed_15m, 14)
    if adx < ADX_THRESHOLD:
        log.info("⚠️  ADX=%.1f — lateral, sin señal", adx)
        return None, 0

    # ── Volumen ───────────────────────────────────────────────────────────────
    vol_ok = _volume_ok(closed_15m)

    # ── RSI cruce 50 ─────────────────────────────────────────────────────────
    rsi_series   = _rsi(closes_15m, 14)
    rsi_prev     = rsi_series[-2]
    rsi_curr     = rsi_series[-1]
    rsi_cross_up   = rsi_prev < 50 <= rsi_curr
    rsi_cross_down = rsi_prev > 50 >= rsi_curr

    # ── Divergencia RSI (bonus) ───────────────────────────────────────────────
    divergence = _rsi_divergence(closes_15m, closed_15m)

    # ── MACD 15m + 1h ────────────────────────────────────────────────────────
    hist_15m = _macd_histogram(closes_15m)
    hist_1h  = _macd_histogram(closes_1h)
    macd15_bull = hist_15m[-1] > 0 and hist_15m[-1] > hist_15m[-2]
    macd15_bear = hist_15m[-1] < 0 and hist_15m[-1] < hist_15m[-2]
    macd1h_bull = hist_1h[-1] > 0
    macd1h_bear = hist_1h[-1] < 0

    # ── Filtro no-chase ───────────────────────────────────────────────────────
    atr        = _atr(closed_15m, 14)
    last_range = closed_15m[-1]["high"] - closed_15m[-1]["low"]
    if atr > 0 and last_range > NO_CHASE_MULT * atr:
        log.info("⚠️  Vela explosiva (%.4f > 2×ATR=%.4f)", last_range, atr)
        return None, 0

    # ── Scoring ───────────────────────────────────────────────────────────────
    # Componentes base (pesos suman 90 en condición ideal)
    def score_long() -> int:
        s = 0
        if trend_long:                  s += 20   # EMA200 1h
        if macro_long:                  s += 15   # EMA50 4h
        if adx > 35:                    s += 10   # tendencia fuerte
        elif adx > 25:                  s += 5
        if rsi_cross_up:                s += 15   # cruce RSI 50
        if macd15_bull:                 s += 10   # MACD 15m
        if macd1h_bull:                 s += 10   # MACD 1h
        if vol_ok:                      s += 10   # volumen
        if divergence == "bullish":     s += 10   # divergencia bonus
        s += hour_bonus
        return min(max(s, 0), 100)

    def score_short() -> int:
        s = 0
        if trend_short:                 s += 20
        if macro_short:                 s += 15
        if adx > 35:                    s += 10
        elif adx > 25:                  s += 5
        if rsi_cross_down:              s += 15
        if macd15_bear:                 s += 10
        if macd1h_bear:                 s += 10
        if vol_ok:                      s += 10
        if divergence == "bearish":     s += 10
        s += hour_bonus
        return min(max(s, 0), 100)

    # Requisitos hard mínimos (los filtros no negociables)
    long_hard  = trend_long  and rsi_cross_up   and macd15_bull and macd1h_bull and macro_long
    short_hard = trend_short and rsi_cross_down  and macd15_bear and macd1h_bear and macro_short

    sc_long  = score_long()  if long_hard  else 0
    sc_short = score_short() if short_hard else 0

    log.info(
        "regime=%s hour=%dUTC | price=%.4f ema200=%.4f dist=%.2f%% ADX=%.1f "
        "rsi=%.1f→%.1f vol=%s diverg=%s | score_long=%d score_short=%d",
        regime, hour_utc, price, ema200, dist*100, adx,
        rsi_prev, rsi_curr, vol_ok, divergence,
        sc_long, sc_short,
    )

    if sc_long >= MIN_SCORE:
        log.info("✅ LONG score=%d", sc_long)
        return "long", sc_long

    if sc_short >= MIN_SCORE:
        log.info("✅ SHORT score=%d", sc_short)
        return "short", sc_short

    log.info("⬛ Sin señal (score L=%d S=%d < %d)", sc_long, sc_short, MIN_SCORE)
    return None, 0
