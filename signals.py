"""signals.py — Sistema de señales con estructura de precio real.

Mejoras estructurales v3:
  A. Estructura de precio 1h con swing highs/lows reales
     Detecta pivots locales (máximo/mínimo local en ventana ±2 velas).
     Requiere 2 HH+HL para confirmar bull, 2 LH+LL para bear.
     Mucho más preciso que comparar medias — elimina falsos positivos en rango ancho.

  B. Hard-guard ADX en rango lateral
     Si la estructura de precio es 'range' Y el ADX_1h < 25 → descartar señal.
     Evita entrar en tendencias falsas donde las EMAs están ordenadas pero el
     mercado no tiene momentum real (típico en rangos laterales con poca volatilidad).

  C. Contexto de vela diaria (heredado v2)
  D. Filtro de liquidez del par (heredado v2)
  E. Penalización por alejamiento del open diario (heredado v2)

Filtros heredados:
  1. Vela cerrada       : penúltima vela (ya cerrada)
  2. Régimen de mercado : EMAs 1h + estructura HH/HL (swings reales)
  3. Macro 4h           : EMA50 en 4h
  4. EMA200 1h          : hard-guard de dirección
  5. EMA200 15m         : hard-guard en SHORTs
  6. ATR volátil        : hard-guard >3.5%
  7. ADX 15m            : hard-guard <20
  8. ADX 1h             : scoring + hard-guard SHORT
  9. RSI 15m            : scoring + hard-guard SHORT sobrevendido
  10. MACD 15m + 1h     : scoring
  11. Volumen 15m       : scoring
  12. Divergencia RSI   : scoring
  13. Sesgo horario     : scoring
  14. No-chase          : hard-guard rango vela
  15. Pullback EMA20    : hard-guard sobreextensión 15m
  16. Score mínimo      : LONGs ≥ MIN_SCORE, SHORTs ≥ MIN_SCORE+8

REGLA FUNDAMENTAL:
  Bear → SOLO SHORT. Bull → SOLO LONG. Sin contra-tendencia.
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
VOLUME_WEAK         = 0.8
MIN_SCORE           = config.MIN_SCORE

PULLBACK_EMA20_DIST = 0.015

REGIME_CONFIRM_BARS   = 3
SHORT_MIN_SCORE_EXTRA = 8
ATR_VOLATILE_PCT      = 0.035
ADX_15M_MIN           = 20

HIGH_BIAS_HOURS = {8, 9, 10, 13, 14, 15, 16, 20, 21}
LOW_BIAS_HOURS  = {2, 3, 4, 5}

# ── Umbrales v2 ───────────────────────────────────────────────────────────
STRUCTURE_LOOKBACK    = 8    # velas 1h para detectar swings (aumentado de 6 a 8)
DAILY_CANDLE_BLOCK    = 0.015
DAILY_CANDLE_PENALTY  = 0.025
DAILY_CANDLE_GUARD    = 0.040
MIN_HOURLY_VOLUME     = 1_000_000

# ── Umbrales v3 ───────────────────────────────────────────────────────────
# ADX mínimo en 1h para confirmar que el régimen no es rango lateral disfrazado.
# Si ADX_1h < umbral Y structure != regime → hard-guard.
ADX_1H_STRUCTURE_MIN  = 25   # debajo de esto, si structure='range' → no entrar
# Número mínimo de swings HH+HL (o LH+LL) consecutivos para confirmar estructura
SWING_CONFIRM_COUNT   = 2


# ── Indicadores ──────────────────────────────────────────────────────────

def _ema(closes: list[float], period: int) -> list[float]:
    if not closes:
        return []
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
    if len(closes) < lookback + 14 or len(candles) < lookback + 1:
        return None
    rsi_series     = _rsi(closes, 14)
    recent_closes  = closes[-lookback:]
    recent_rsi     = rsi_series[-lookback:]
    recent_candles = candles[-lookback:]

    avg_vol  = sum(c["volume"] for c in recent_candles[:-1]) / max(1, len(recent_candles) - 1)
    last_vol = recent_candles[-1].get("volume", 0.0)
    if avg_vol > 0 and last_vol < avg_vol * 0.6:
        return None

    try:
        lo_val = min(recent_closes[:-1])
        lo_idx = max(i for i, v in enumerate(recent_closes[:-1]) if v == lo_val)
        if recent_closes[-1] < lo_val and recent_rsi[-1] > recent_rsi[lo_idx] + 2:
            return "bullish"
    except (ValueError, IndexError):
        pass

    try:
        hi_val = max(recent_closes[:-1])
        hi_idx = max(i for i, v in enumerate(recent_closes[:-1]) if v == hi_val)
        if recent_closes[-1] > hi_val and recent_rsi[-1] < recent_rsi[hi_idx] - 2:
            return "bearish"
    except (ValueError, IndexError):
        pass

    return None


def _market_regime(candles_1h: list[dict]) -> tuple[str, float]:
    closes        = [c["close"] for c in candles_1h]
    closes_closed = closes[:-1]
    ema20  = _ema(closes_closed, 20)[-1]
    ema50  = _ema(closes_closed, 50)[-1]
    ema200 = _ema(closes_closed, 200)[-1]
    adx    = _adx(candles_1h[:-1], 14)
    price  = closes[-2] if len(closes) >= 2 else closes[-1]

    if price > ema20 > ema50 > ema200:
        return "bull", adx
    if price < ema20 < ema50 < ema200:
        return "bear", adx
    if price > ema200 and price > ema50 and ema20 > ema200:
        return "bull", adx
    if price < ema200 and price < ema50 and ema20 < ema200:
        return "bear", adx
    return "range", adx


def _find_swing_highs(highs: list[float], wing: int = 2) -> list[int]:
    """Devuelve índices donde hay un pivot high local (máximo local con 'wing' velas a cada lado)."""
    pivots = []
    for i in range(wing, len(highs) - wing):
        if all(highs[i] >= highs[i - j] for j in range(1, wing + 1)) and \
           all(highs[i] >= highs[i + j] for j in range(1, wing + 1)):
            pivots.append(i)
    return pivots


def _find_swing_lows(lows: list[float], wing: int = 2) -> list[int]:
    """Devuelve índices donde hay un pivot low local (mínimo local con 'wing' velas a cada lado)."""
    pivots = []
    for i in range(wing, len(lows) - wing):
        if all(lows[i] <= lows[i - j] for j in range(1, wing + 1)) and \
           all(lows[i] <= lows[i + j] for j in range(1, wing + 1)):
            pivots.append(i)
    return pivots


def _price_structure(candles_1h: list[dict], lookback: int = STRUCTURE_LOOKBACK) -> str:
    """Detecta estructura de precio real usando swing highs/lows en las últimas N velas 1h cerradas.

    v3: Usa pivots locales reales (no medias). Requiere SWING_CONFIRM_COUNT swings
    consecutivos alcistas (HH+HL) o bajistas (LH+LL) para confirmar dirección.
    Un solo swing en la dirección correcta no es suficiente para confirmar estructura.
    """
    closed = candles_1h[:-1]  # excluir vela en curso
    if len(closed) < lookback + 4:  # necesitamos margen para los wings
        return "range"

    recent = closed[-(lookback + 4):]  # buffer extra para los wings de los extremos
    highs  = [c["high"]  for c in recent]
    lows   = [c["low"]   for c in recent]

    swing_high_idxs = _find_swing_highs(highs, wing=2)
    swing_low_idxs  = _find_swing_lows(lows,  wing=2)

    # Necesitamos al menos 2 pivots de cada tipo para evaluar la tendencia
    if len(swing_high_idxs) < 2 or len(swing_low_idxs) < 2:
        return "range"

    swing_high_vals = [highs[i] for i in swing_high_idxs]
    swing_low_vals  = [lows[i]  for i in swing_low_idxs]

    # Contar cuántos swings consecutivos son HH (máximo mayor al anterior)
    hh_count = sum(
        1 for i in range(1, len(swing_high_vals))
        if swing_high_vals[i] > swing_high_vals[i - 1] * 1.001
    )
    # Contar cuántos swings consecutivos son HL (mínimo mayor al anterior)
    hl_count = sum(
        1 for i in range(1, len(swing_low_vals))
        if swing_low_vals[i] > swing_low_vals[i - 1] * 1.001
    )
    # Contar LH y LL
    lh_count = sum(
        1 for i in range(1, len(swing_high_vals))
        if swing_high_vals[i] < swing_high_vals[i - 1] * 0.999
    )
    ll_count = sum(
        1 for i in range(1, len(swing_low_vals))
        if swing_low_vals[i] < swing_low_vals[i - 1] * 0.999
    )

    n = SWING_CONFIRM_COUNT

    if hh_count >= n and hl_count >= n:
        return "bull"
    if lh_count >= n and ll_count >= n:
        return "bear"
    # Estructura mixta o insuficiente → rango
    return "range"


def _daily_candle_context(candles_1h: list[dict]) -> tuple[float, float]:
    """Calcula la vela diaria sintética (desde medianoche UTC) usando las velas 1h disponibles.

    Devuelve (open_daily, close_actual) para evaluar si el día va a favor o en contra.
    """
    now_utc     = datetime.datetime.now(timezone.utc)
    midnight    = now_utc.replace(hour=0, minute=0, second=0, microsecond=0)
    midnight_ts = midnight.timestamp() * 1000  # ms

    # Velas 1h cerradas a partir de medianoche UTC
    today_candles = [
        c for c in candles_1h[:-1]  # excluir vela en curso
        if c.get("open_time", 0) >= midnight_ts
    ]

    if not today_candles:
        # Fallback: usar la última vela 1h cerrada
        if len(candles_1h) >= 2:
            c = candles_1h[-2]
            return c["open"], c["close"]
        return 0.0, 0.0

    open_daily  = today_candles[0]["open"]
    close_today = today_candles[-1]["close"]
    return open_daily, close_today


def _liquidity_ok(candles_1h: list[dict]) -> bool:
    """Comprueba que el par tiene suficiente liquidez (volumen medio por vela 1h)."""
    recent = candles_1h[-25:-1]  # últimas 24 velas cerradas
    if not recent:
        return False
    avg_vol = sum(c.get("volume", 0.0) for c in recent) / len(recent)
    return avg_vol >= MIN_HOURLY_VOLUME


def evaluate(
    candles_15m: list[dict],
    candles_1h:  list[dict],
    candles_4h:  list[dict] | None = None,
    min_score:   int = MIN_SCORE,
) -> tuple[str | None, int, str | None]:
    """Evalúa si hay señal de entrada. Devuelve (side, score, regime) o (None, score, None)."""

    # ── Datos mínimos ────────────────────────────────────────────────────
    if len(candles_15m) < 50 or len(candles_1h) < 220:
        return None, 0, None

    # ── Liquidez ─────────────────────────────────────────────────────────
    if not _liquidity_ok(candles_1h):
        return None, 0, None

    # ── Vela cerrada (penúltima) ──────────────────────────────────────────
    closed  = candles_15m[-2]
    c_open  = closed["open"]
    c_close = closed["close"]
    c_high  = closed["high"]
    c_low   = closed["low"]
    bullish_candle = c_close > c_open

    # ── Régimen de mercado 1h ────────────────────────────────────────────
    regime, adx_1h = _market_regime(candles_1h)

    # ── Estructura de precio 1h (v3: swings reales) ───────────────────────
    structure = _price_structure(candles_1h)

    # Hard-guard v3: si structure='range' y ADX_1h bajo → mercado lateral confirmado
    if structure == "range" and adx_1h < ADX_1H_STRUCTURE_MIN:
        log.debug(
            "[structure] range + ADX_1h=%.1f < %d → hard-guard (sin momentum)",
            adx_1h, ADX_1H_STRUCTURE_MIN,
        )
        return None, 0, None

    # Si las EMAs dicen bull/bear pero la estructura de swings dice range:
    # solo se permite entrar si el ADX 1h es suficientemente fuerte
    if regime != "range" and structure == "range" and adx_1h < ADX_1H_STRUCTURE_MIN:
        log.debug(
            "[structure] Régimen %s pero structure=range + ADX_1h=%.1f → hard-guard",
            regime, adx_1h,
        )
        return None, 0, None

    # Si regime es rango, no hay señal
    if regime == "range":
        return None, 0, None

    # ── Macro 4h ─────────────────────────────────────────────────────────
    if candles_4h and len(candles_4h) >= 55:
        closes_4h = [c["close"] for c in candles_4h[:-1]]
        ema50_4h  = _ema(closes_4h, 50)[-1]
        price_4h  = closes_4h[-1]
        if regime == "bull" and price_4h < ema50_4h:
            return None, 0, None
        if regime == "bear" and price_4h > ema50_4h:
            return None, 0, None

    # ── Indicadores 15m ──────────────────────────────────────────────────
    closes_15m = [c["close"] for c in candles_15m]
    price      = closes_15m[-2]  # precio de la vela cerrada

    ema20_15m  = _ema(closes_15m[:-1], 20)[-1]
    ema200_15m = _ema(closes_15m[:-1], 200)[-1]
    rsi_series = _rsi(closes_15m[:-1], 14)
    rsi        = rsi_series[-1]
    macd_hist  = _macd_histogram(closes_15m[:-1])[-1]
    atr_15m    = _atr(candles_15m[:-1], 14)
    adx_15m    = _adx(candles_15m[:-1], 14)
    volumes    = [c["volume"] for c in candles_15m]
    avg_vol    = sum(volumes[-21:-1]) / 20
    last_vol   = volumes[-2]

    # ── Hard-guards ───────────────────────────────────────────────────────

    # ATR volátil
    if price > 0 and atr_15m / price > ATR_VOLATILE_PCT:
        return None, 0, None

    # ADX 15m demasiado débil
    if adx_15m < ADX_15M_MIN:
        return None, 0, None

    # EMA200 1h
    closes_1h  = [c["close"] for c in candles_1h]
    ema200_1h  = _ema(closes_1h[:-1], 200)[-1]
    if regime == "bull" and price < ema200_1h * (1 - EMA200_MIN_DIST):
        return None, 0, None
    if regime == "bear" and price > ema200_1h * (1 + EMA200_MIN_DIST):
        return None, 0, None

    # EMA200 15m — hard-guard en SHORTs
    if regime == "bear" and price < ema200_15m * (1 - EMA200_MIN_DIST):
        return None, 0, None

    # No-chase: rango de la vela cerrada
    candle_range = c_high - c_low
    if candle_range > 0 and atr_15m > 0:
        if candle_range > NO_CHASE_MULT * atr_15m:
            return None, 0, None

    # Pullback EMA20 (sobreextensión)
    if price > 0 and ema20_15m > 0:
        dist_ema20 = abs(price - ema20_15m) / price
        if dist_ema20 > PULLBACK_EMA20_DIST:
            if regime == "bull" and price > ema20_15m:
                return None, 0, None
            if regime == "bear" and price < ema20_15m:
                return None, 0, None

    # Contexto vela diaria
    open_daily, close_today = _daily_candle_context(candles_1h)
    score = 0

    if open_daily > 0:
        daily_move = (close_today - open_daily) / open_daily
        if regime == "bull" and daily_move < -DAILY_CANDLE_BLOCK:
            return None, score, None
        if regime == "bear" and daily_move > DAILY_CANDLE_BLOCK:
            return None, score, None

        abs_move = abs(daily_move)
        if abs_move > DAILY_CANDLE_GUARD:
            # Hard-guard: el move del día ya ocurrió
            if (regime == "bull" and daily_move > 0) or (regime == "bear" and daily_move < 0):
                return None, score, None
        elif abs_move > DAILY_CANDLE_PENALTY:
            score -= 10

    # ── Scoring ───────────────────────────────────────────────────────────

    # Dirección de vela cerrada
    if regime == "bull" and bullish_candle:
        score += 8
    elif regime == "bear" and not bullish_candle:
        score += 8

    # RSI
    if regime == "bull":
        if 45 <= rsi <= 65:
            score += 8
        elif rsi > 70:
            score -= 8  # sobrecompra
            if rsi > 80:
                return None, score, None  # hard-guard SHORT sobrevendido no aplica aquí,
                                          # pero en bull con RSI>80 es sobreextensión
    else:  # bear
        if 35 <= rsi <= 55:
            score += 8
        elif rsi < 30:
            return None, score, None  # sobrevendido en SHORT

    # ADX 1h
    if adx_1h >= 30:
        score += 12
    elif adx_1h >= 25:
        score += 8
    elif adx_1h >= 20:
        score += 4
    # hard-guard SHORT con ADX 1h débil
    if regime == "bear" and adx_1h < 22:
        return None, score, None

    # MACD 15m
    if regime == "bull" and macd_hist > 0:
        score += 8
    elif regime == "bear" and macd_hist < 0:
        score += 8

    # MACD 1h
    closes_1h_closed = closes_1h[:-1]
    macd_1h = _macd_histogram(closes_1h_closed)[-1]
    if regime == "bull" and macd_1h > 0:
        score += 8
    elif regime == "bear" and macd_1h < 0:
        score += 8

    # Volumen
    if avg_vol > 0:
        if last_vol >= avg_vol * VOLUME_MULT:
            score += 8
        elif last_vol < avg_vol * VOLUME_WEAK:
            score -= 4

    # Sesgo horario
    hour = datetime.datetime.now(timezone.utc).hour
    if hour in HIGH_BIAS_HOURS:
        score += 4
    elif hour in LOW_BIAS_HOURS:
        score -= 4

    # Divergencia RSI
    div = _rsi_divergence(closes_15m[:-1], candles_15m[:-1])
    if regime == "bull" and div == "bullish":
        score += 8
    elif regime == "bear" and div == "bearish":
        score += 8

    # Estructura de precio vs régimen (v3: bonus si estructura confirma)
    if structure == regime:
        score += 8  # consenso completo EMA + swings
    elif structure != "range" and structure != regime:
        # Contradicción estructural — penalización más fuerte en v3
        score -= 12
        log.debug(
            "[structure] Régimen %s contradice estructura %s (adx_1h=%.1f) — penalización -12",
            regime, structure, adx_1h,
        )

    # ── Score mínimo ──────────────────────────────────────────────────────
    min_required = min_score + (SHORT_MIN_SCORE_EXTRA if regime == "bear" else 0)
    if score < min_required:
        return None, score, None

    side = "long" if regime == "bull" else "short"
    log.info(
        "✅ SEÑAL %s | score=%d | regime=%s structure=%s adx1h=%.1f adx15m=%.1f "
        "rsi=%.1f macd_hist=%.5f vol_ratio=%.2f",
        side.upper(), score, regime, structure, adx_1h, adx_15m,
        rsi, macd_hist, last_vol / avg_vol if avg_vol else 0,
    )
    return side, score, regime
