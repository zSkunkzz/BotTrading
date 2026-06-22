"""signals.py — Sistema de señales premium con scoring 0-100.

Filtros:
  1. Vela cerrada       : penúltima vela (ya cerrada)
  2. Régimen de mercado : clasificación por EMAs 1h (ADX no bloquea, penaliza)
                          NUEVO: requiere REGIME_CONFIRM_BARS velas 1h consecutivas
                          confirmando el mismo régimen antes de habilitar señales
  3. Macro 4h           : EMA50 en 4h — bonus/penalización/neutro según disponibilidad
  4. EMA200 1h          : hard-guard de dirección (calculado sobre velas cerradas)
  5. ATR volátil        : hard-guard — si atr_pct > 3.5% mercado en evento/noticia,
                          SL real desborda el cap de 2.5% → no entrar
  6. ADX 15m            : hard-guard <15 (sin tendencia, no operar)
                          >35 +12, >25 +6, >18 -8, <18 -15
  7. ADX 1h             : >25 +5, <18 -5 (fuerza de tendencia en marco superior)
                          hard-guard short: bear + adx_1h < 18 → no entrar (rebote choppy)
  8. Volumen            : última vela CERRADA >media×1.2 → +8
  9. RSI 15m            : cruce 50 +15, extremo +8, direccional +4, contrario -5
  10. MACD 15m + 1h     : histograma positivo/negativo → +10 | neutro 0 | contrario -5
  11. Divergencia RSI   : +8
  12. Sesgo horario     : hora alta +8, hora baja -10
  13. Filtro no-chase   : rango vela ≤2×ATR (hard-guard)

Score base: 20 pts (por superar hard-guards)
Macro 4h:  +15 a favor | 0 si sin datos | -10 en contra
Sizing en risk.py: mult=1.0 (score 70-84) | 1.4 (≥85)
MIN_SCORE configurable via env var MIN_SCORE (default 70)
"""
from __future__ import annotations
import logging
import datetime
from datetime import timezone

import config

log = logging.getLogger("signals")

# ── Umbrales configurables ────────────────────────────────────────────────
EMA200_MIN_DIST     = 0.003
NO_CHASE_MULT       = 2.0
VOLUME_MULT         = 1.2
MIN_SCORE           = config.MIN_SCORE
REGIME_CONFIRM_BARS = 2

# Guard de volatilidad extrema: si atr_pct supera este umbral el mercado
# está en modo evento/noticias y el SL se desborda del cap → no operar.
ATR_VOLATILE_PCT    = 0.035   # 3.5%

HIGH_BIAS_HOURS = {8, 9, 10, 14, 15, 16, 20, 21}
LOW_BIAS_HOURS  = {2, 3, 4, 5}


# ── Indicadores ───────────────────────────────────────────────────────────────────

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
    """Detecta divergencia RSI-precio en la ventana reciente."""
    if len(closes) < lookback + 14:
        return None
    rsi_series    = _rsi(closes, 14)
    recent_closes = closes[-lookback:]
    recent_rsi    = rsi_series[-lookback:]

    lo_val = min(recent_closes[:-1])
    lo_idx = max(i for i, v in enumerate(recent_closes[:-1]) if v == lo_val)
    if recent_closes[-1] < lo_val and recent_rsi[-1] > recent_rsi[lo_idx] + 2:
        return "bullish"

    hi_val = max(recent_closes[:-1])
    hi_idx = max(i for i, v in enumerate(recent_closes[:-1]) if v == hi_val)
    if recent_closes[-1] > hi_val and recent_rsi[-1] < recent_rsi[hi_idx] - 2:
        return "bearish"

    return None


def _market_regime(candles_1h: list[dict]) -> tuple[str, float]:
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
    if price > ema200 and price > ema50 and ema20 > ema200:
        return "bull", adx
    if price < ema200 and price < ema50 and ema20 < ema200:
        return "bear", adx
    return "range", adx


def _regime_confirmed(candles_1h: list[dict], n: int = REGIME_CONFIRM_BARS) -> tuple[str, float]:
    """Devuelve el régimen solo si las últimas n velas cerradas coinciden."""
    if len(candles_1h) < 200 + n + 1:
        return "range", 0.0

    regime_now, adx_now = _market_regime(candles_1h)

    regimes_prev = []
    for offset in range(n, 0, -1):
        window = candles_1h[:-offset]
        regime_hist, _ = _market_regime(window)
        regimes_prev.append(regime_hist)

    all_regimes = regimes_prev + [regime_now]

    if len(set(all_regimes)) == 1:
        log.debug("régimen confirmado ×%d: %s", n + 1, regime_now)
        return regime_now, adx_now

    log.info("⚠️  Régimen inestable %s — esperando confirmación", all_regimes)
    return "range", adx_now


def _volume_ok(candles: list[dict], window: int = 20) -> bool:
    """Comprueba si el volumen de la última vela CERRADA supera la media."""
    if len(candles) < window + 2:
        return False
    recent   = candles[-(window + 2):-1]
    avg_vol  = sum(c["volume"] for c in recent) / len(recent)
    last_vol = candles[-2]["volume"]
    return last_vol > avg_vol * VOLUME_MULT


def _macro_pts(candles_4h: list[dict] | None, direction: str) -> int:
    """Bonus/penalización según EMA50 en 4h.

    +15 a favor  |  0 si sin datos  |  -10 en contra
    """
    if not candles_4h or len(candles_4h) < 51:
        return 0
    closes = [c["close"] for c in candles_4h]
    ema50  = _ema(closes, 50)[-1]
    price  = closes[-1]
    if direction == "long":
        return 15 if price > ema50 else -10
    return 15 if price < ema50 else -10


def evaluate(
    candles_15m: list[dict],
    candles_1h:  list[dict],
    candles_4h:  list[dict] | None = None,
    min_score:   int = MIN_SCORE,
) -> tuple[str | None, int]:
    """Evalúa señal con scoring 0-100. Devuelve (signal, score) o (None, score)."""

    # ── 0. Datos mínimos ──────────────────────────────────────────────────
    if len(candles_15m) < 50 or len(candles_1h) < 210:
        return None, 0

    # ── 1. Vela cerrada ───────────────────────────────────────────────────
    candle = candles_15m[-2]          # penúltima = cerrada
    closes_15m = [c["close"] for c in candles_15m]

    # ── 2. Régimen de mercado ─────────────────────────────────────────────
    regime, adx_1h = _regime_confirmed(candles_1h)
    if regime == "range":
        return None, 0

    direction = "long" if regime == "bull" else "short"

    # ── 3. Score base ─────────────────────────────────────────────────────
    score = 20

    # ── 4. Macro 4h ───────────────────────────────────────────────────────
    score += _macro_pts(candles_4h, direction)

    # ── 5. EMA200 1h hard-guard (sobre velas cerradas) ────────────────────
    closes_1h = [c["close"] for c in candles_1h[:-1]]
    ema200_1h = _ema(closes_1h, 200)[-1]
    price     = closes_15m[-2]
    if direction == "long"  and price < ema200_1h * (1 - EMA200_MIN_DIST):
        return None, score
    if direction == "short" and price > ema200_1h * (1 + EMA200_MIN_DIST):
        return None, score

    # ── 5b. Guard ATR volátil ─────────────────────────────────────────────
    # Si el ATR 15m supera el 3.5% del precio, el mercado está en modo
    # evento/noticias: el SL real desbordaría el cap de 2.5% → no entrar.
    atr_15m_raw = _atr(candles_15m[:-1])
    if price > 0 and atr_15m_raw / price > ATR_VOLATILE_PCT:
        log.info(
            "Hard-guard ATR volátil: atr_pct=%.2f%% > %.1f%% — mercado en evento, señal descartada",
            atr_15m_raw / price * 100, ATR_VOLATILE_PCT * 100,
        )
        return None, score

    # ── 6. ADX 15m ────────────────────────────────────────────────────────
    adx_15m = _adx(candles_15m[:-1], 14)

    # Hard-guard: sin tendencia mínima en 15m, no operar
    if adx_15m < 15:
        log.debug("Hard-guard ADX 15m demasiado bajo: %.1f — señal descartada", adx_15m)
        return None, score

    if   adx_15m > 35: score += 12
    elif adx_15m > 25: score += 6
    elif adx_15m > 18: score -= 8
    else:              score -= 15

    # ── 7. ADX 1h ─────────────────────────────────────────────────────────
    if   adx_1h > 25: score += 5
    elif adx_1h < 18: score -= 5

    # Hard-guard short en bear choppy: régimen bear pero sin fuerza en 1h
    if direction == "short" and adx_1h < 18:
        log.debug(
            "Hard-guard short: regime=bear pero adx_1h=%.1f — mercado rebotando sin tendencia",
            adx_1h,
        )
        return None, score

    # ── 8. Volumen ────────────────────────────────────────────────────────
    if _volume_ok(candles_15m):
        score += 8

    # ── 9. RSI 15m ────────────────────────────────────────────────────────
    rsi_vals = _rsi(closes_15m[:-1], 14)
    rsi_now  = rsi_vals[-1]
    rsi_prev = rsi_vals[-2] if len(rsi_vals) > 1 else rsi_now

    if direction == "long":
        rsi_cross = rsi_prev < 50 <= rsi_now
        rsi_ext   = rsi_now > 55
        rsi_dir   = rsi_now > rsi_prev
        rsi_bad   = rsi_now < 45
    else:
        rsi_cross = rsi_prev > 50 >= rsi_now
        rsi_ext   = rsi_now < 45
        rsi_dir   = rsi_now < rsi_prev
        rsi_bad   = rsi_now > 55

    if   rsi_cross: score += 15
    elif rsi_ext:   score += 8
    elif rsi_dir:   score += 4
    elif rsi_bad:   score -= 5

    # ── 10. MACD 15m + 1h ─────────────────────────────────────────────────
    hist_15m = _macd_histogram(closes_15m[:-1])
    h_now    = hist_15m[-1]

    if direction == "long":
        if   h_now > 0: score += 10
        elif h_now < 0: score -= 5
    else:
        if   h_now < 0: score += 10
        elif h_now > 0: score -= 5

    hist_1h = _macd_histogram(closes_1h[:-1])
    h1_now  = hist_1h[-1]
    if direction == "long":
        if   h1_now > 0: score += 10
        elif h1_now < 0: score -= 5
    else:
        if   h1_now < 0: score += 10
        elif h1_now > 0: score -= 5

    # ── 11. Divergencia RSI ───────────────────────────────────────────────
    div = _rsi_divergence(closes_15m[:-1], candles_15m[:-1])
    if (direction == "long"  and div == "bullish") or \
       (direction == "short" and div == "bearish"):
        score += 8

    # ── 12. Sesgo horario ─────────────────────────────────────────────────
    hour = datetime.datetime.now(timezone.utc).hour
    if   hour in HIGH_BIAS_HOURS: score += 8
    elif hour in LOW_BIAS_HOURS:  score -= 10

    # ── 13. Filtro no-chase ───────────────────────────────────────────────
    atr_15m   = atr_15m_raw   # reutilizar el ya calculado en guard ATR
    candle_rng = candle["high"] - candle["low"]
    if atr_15m > 0 and candle_rng > NO_CHASE_MULT * atr_15m:
        log.debug("No-chase: rng=%.4f > %.1f×ATR=%.4f", candle_rng, NO_CHASE_MULT, atr_15m)
        return None, score

    # ── 14. Score mínimo ──────────────────────────────────────────────────
    if score < min_score:
        log.debug("Score insuficiente: %d < %d (%s)", score, min_score, direction)
        return None, score

    log.info(
        "✅ SEÑAL %s | score=%d | regime=%s adx1h=%.1f adx15m=%.1f "
        "rsi=%.1f macd15=%+.4f macd1h=%+.4f vol=%s div=%s",
        direction.upper(), score, regime, adx_1h, adx_15m,
        rsi_now, h_now, h1_now, _volume_ok(candles_15m), div,
    )
    return direction, score
