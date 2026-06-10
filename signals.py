"""signals.py — Sistema de señales premium con scoring 0-100.

Filtros y mejoras implementadas:
  1. Vela cerrada       : penúltima vela (ya cerrada)
  2. Régimen de mercado : clasifica tendencia/rango — SOLO por alineación de EMAs
                          (ADX ya NO es hard-block de régimen, pasa a penalización en score)
  3. Tendencia 4h       : EMA50 en 4h como componente de score (no hard-block)
  4. Tendencia 1h       : EMA200 — hard-guard de dirección
  5. ADX 15m            : >35 bonus alto, >25 bonus medio, <25 penalización, <18 penalización fuerte
  6. Volumen            : vela de señal con volumen > media(20) × 1.2 → bonus
  7. RSI 15m            : cruce del nivel 50 → bonus; RSI extremo contrario → penalización
  8. MACD 15m + 1h      : histograma confirma dirección → bonus por cada TF
  9. Divergencia RSI    : bearish/bullish divergence como bonus adicional
  10. Sesgo horario     : horas de alta directionalidad (08-10 UTC, 14-16 UTC) → bonus
  11. Filtro no-chase   : rango de vela ≤ 2×ATR (hard-guard)
  12. Score 0-100       : señal proporcional, no binaria

Hard-guards reales (únicos que bloquean totalmente):
  a) Régimen 'range' (EMAs 1h sin alineación mínima)
  b) Precio demasiado cerca del EMA200 (<0.3%)
  c) Vela explosiva > 2×ATR
  d) Régimen contradice EMA200 (price < ema200 en bull, price > ema200 en bear)

ADX bajo → ya NO bloquea. Penaliza −8 (ADX<25) o −15 (ADX<18) en el score.
"""
from __future__ import annotations
import logging
import datetime

log = logging.getLogger("signals")

# ── Umbrales configurables ────────────────────────────────────────────────────
EMA200_MIN_DIST   = 0.003   # 0.3% distancia mínima al EMA200 en 1h
ADX_THRESHOLD     = 25
NO_CHASE_MULT     = 2.0
VOLUME_MULT       = 1.2
MIN_SCORE         = 55

HIGH_BIAS_HOURS   = {8, 9, 10, 14, 15, 16, 20, 21}
LOW_BIAS_HOURS    = {2, 3, 4, 5}


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
    if len(closes) < lookback + 14:
        return None
    rsi_series = _rsi(closes, 14)
    recent_closes = closes[-lookback:]
    recent_rsi    = rsi_series[-lookback:]

    price_lo1 = min(recent_closes[:-3])
    price_lo2 = recent_closes[-1]
    idx_lo1   = recent_closes.index(price_lo1)
    rsi_lo1   = recent_rsi[idx_lo1]
    rsi_lo2   = recent_rsi[-1]
    if price_lo2 < price_lo1 and rsi_lo2 > rsi_lo1 + 2:
        return "bullish"

    price_hi1 = max(recent_closes[:-3])
    price_hi2 = recent_closes[-1]
    idx_hi1   = recent_closes.index(price_hi1)
    rsi_hi1   = recent_rsi[idx_hi1]
    rsi_hi2   = recent_rsi[-1]
    if price_hi2 > price_hi1 and rsi_hi2 < rsi_hi1 - 2:
        return "bearish"

    return None


def _market_regime(candles_1h: list[dict]) -> tuple[str, float]:
    """
    Devuelve (regime, adx_1h).
    regime: 'bull' | 'bear' | 'range'

    El régimen se determina SOLO por la alineación de EMAs.
    ADX se devuelve para usarlo como penalización en el score.

    Regla de consistencia (fix Bug 1+2):
      - 'bull' solo si precio >= EMA200 (evita bull con precio bajo EMA200)
      - 'bear' solo si precio <= EMA200 (evita bear con precio sobre EMA200)
      - Si la alineación parcial contradice el EMA200 → 'range'
    """
    closes = [c["close"] for c in candles_1h]
    ema20  = _ema(closes, 20)[-1]
    ema50  = _ema(closes, 50)[-1]
    ema200 = _ema(closes, 200)[-1]
    adx    = _adx(candles_1h, 14)
    price  = closes[-1]

    # Alineación perfecta alcista
    if price > ema20 > ema50 > ema200:
        return "bull", adx

    # Alineación perfecta bajista
    if price < ema20 < ema50 < ema200:
        return "bear", adx

    # Alineación parcial alcista:
    # precio por encima de EMA200 Y EMA50, y además EMA20 > EMA200
    # (garantiza que el precio NO contradice el EMA200)
    if price > ema200 and price > ema50 and ema20 > ema200:
        return "bull", adx

    # Alineación parcial bajista:
    # precio por debajo de EMA200 Y EMA50, y además EMA20 < EMA200
    if price < ema200 and price < ema50 and ema20 < ema200:
        return "bear", adx

    # Cualquier otra combinación = lateral / sin alineación clara
    return "range", adx


def _volume_ok(candles: list[dict], window: int = 20) -> bool:
    if len(candles) < window + 1:
        return True
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
      score     : 0-100

    Hard-guards (únicos que devuelven None directamente):
      1. Datos insuficientes
      2. EMAs 1h sin alineación mínima (regime == 'range')
      3. Precio demasiado cerca del EMA200 (<0.3%)
      4. Vela explosiva (rango > 2×ATR)
      5. EMA200 1h en contra de la dirección (usa régimen, no recalcula)

    ADX bajo ya NO es hard-guard. Penaliza en score:
      ADX < 18  → −15 pts  (mercado muy lateral)
      ADX < 25  → −8 pts   (tendencia débil)
      ADX 25-35 → +6 pts
      ADX > 35  → +12 pts
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

    # ── Hard-guard 1: Régimen de mercado (solo EMAs, sin ADX) ─────────────────
    regime, adx_1h = _market_regime(closed_1h)
    if regime == "range":
        log.info("⬛ Régimen lateral (EMAs sin alinear) — sin señal")
        return None, 0

    # ── Hard-guard 2: distancia mínima al EMA200 ─────────────────────────────
    ema200 = _ema(closes_1h, 200)[-1]
    price  = closes_15m[-1]
    dist   = abs(price - ema200) / ema200
    if dist < EMA200_MIN_DIST:
        log.info("⚠️  Precio cerca del EMA200 (%.4f%%) — sin señal", dist * 100)
        return None, 0

    # Dirección permitida según régimen (consistente con EMA200 por construcción)
    # Bug 1+2 fix: usamos el régimen como única fuente de verdad de dirección,
    # no recalculamos trend_long/short independientemente con solo EMA200.
    # El régimen ya garantiza consistencia con EMA200 tras el fix de _market_regime.
    trend_long  = regime == "bull"
    trend_short = regime == "bear"

    # ── Hard-guard 3: filtro no-chase ─────────────────────────────────────────
    atr        = _atr(closed_15m, 14)
    last_range = closed_15m[-1]["high"] - closed_15m[-1]["low"]
    if atr > 0 and last_range > NO_CHASE_MULT * atr:
        log.info("⚠️  Vela explosiva (%.4f > 2×ATR=%.4f)", last_range, atr)
        return None, 0

    # ── Componentes de score ──────────────────────────────────────────────────
    adx = _adx(closed_15m, 14)  # ADX en 15m para scoring (más reactivo)

    # Sesgo horario
    hour_utc = datetime.datetime.utcnow().hour
    hour_bonus = 0
    if hour_utc in HIGH_BIAS_HOURS:
        hour_bonus = 8
    elif hour_utc in LOW_BIAS_HOURS:
        hour_bonus = -10

    # Macro 4h (bonus, no hard-block)
    # Bug 5 fix: inicializar a False (conservador) en vez de True
    # para evitar +15 pts falsos cuando no hay datos 4h disponibles.
    macro_long = macro_short = False
    if closed_4h and len(closed_4h) >= 55:
        closes_4h = [c["close"] for c in closed_4h]
        ema50_4h  = _ema(closes_4h, 50)[-1]
        price_4h  = closes_4h[-1]
        macro_long  = price_4h > ema50_4h
        macro_short = price_4h < ema50_4h

    # RSI 15m
    rsi_series   = _rsi(closes_15m, 14)
    rsi_prev     = rsi_series[-2]
    rsi_curr     = rsi_series[-1]
    rsi_cross_up   = rsi_prev < 50 <= rsi_curr
    rsi_cross_down = rsi_prev > 50 >= rsi_curr
    rsi_bull = rsi_curr > 50
    rsi_bear = rsi_curr < 50
    rsi_extreme_bull = rsi_curr > 60
    rsi_extreme_bear = rsi_curr < 40

    # Divergencia RSI
    divergence = _rsi_divergence(closes_15m, closed_15m)

    # MACD 15m + 1h
    hist_15m = _macd_histogram(closes_15m)
    hist_1h  = _macd_histogram(closes_1h)
    macd15_bull = hist_15m[-1] > 0 and hist_15m[-1] > hist_15m[-2]
    macd15_bear = hist_15m[-1] < 0 and hist_15m[-1] < hist_15m[-2]
    macd1h_bull = hist_1h[-1] > 0
    macd1h_bear = hist_1h[-1] < 0

    # Volumen
    vol_ok = _volume_ok(closed_15m)

    # ── Scoring LONG ─────────────────────────────────────────────────────────
    def score_long() -> int:
        if not trend_long:              # hard-guard de dirección (regime == 'bull')
            return 0
        s = 20                          # base por pasar el hard-guard EMA200
        if macro_long:                  s += 15
        else:                           s -= 10
        # ADX como penalización/bonus (ya no es hard-block)
        if adx > 35:                    s += 12
        elif adx > 25:                  s += 6
        elif adx > 18:                  s -= 8    # tendencia débil
        else:                           s -= 15   # mercado muy lateral
        if rsi_cross_up:                s += 15
        elif rsi_extreme_bull:          s += 8
        elif rsi_bull:                  s += 4
        else:                           s -= 5
        if macd15_bull:                 s += 10
        else:                           s -= 5
        if macd1h_bull:                 s += 10
        else:                           s -= 5
        if vol_ok:                      s += 8
        if divergence == "bullish":     s += 8
        s += hour_bonus
        return min(max(s, 0), 100)

    # ── Scoring SHORT ────────────────────────────────────────────────────────
    def score_short() -> int:
        if not trend_short:             # hard-guard de dirección (regime == 'bear')
            return 0
        s = 20
        if macro_short:                 s += 15
        else:                           s -= 10
        # ADX como penalización/bonus
        if adx > 35:                    s += 12
        elif adx > 25:                  s += 6
        elif adx > 18:                  s -= 8
        else:                           s -= 15
        if rsi_cross_down:              s += 15
        elif rsi_extreme_bear:          s += 8
        elif rsi_bear:                  s += 4
        else:                           s -= 5
        if macd15_bear:                 s += 10
        else:                           s -= 5
        if macd1h_bear:                 s += 10
        else:                           s -= 5
        if vol_ok:                      s += 8
        if divergence == "bearish":     s += 8
        s += hour_bonus
        return min(max(s, 0), 100)

    sc_long  = score_long()
    sc_short = score_short()

    log.info(
        "regime=%s adx1h=%.1f hour=%dUTC | price=%.4f ema200=%.4f dist=%.2f%% ADX15m=%.1f "
        "rsi=%.1f→%.1f vol=%s diverg=%s macro_l=%s macro_s=%s "
        "macd15=%.4f macd1h=%.4f | score_long=%d score_short=%d (min=%d)",
        regime, adx_1h, hour_utc, price, ema200, dist*100, adx,
        rsi_prev, rsi_curr, vol_ok, divergence,
        macro_long, macro_short,
        hist_15m[-1], hist_1h[-1],
        sc_long, sc_short, MIN_SCORE,
    )

    if sc_long >= MIN_SCORE and sc_long >= sc_short:
        log.info("✅ LONG score=%d", sc_long)
        return "long", sc_long

    if sc_short >= MIN_SCORE:
        log.info("✅ SHORT score=%d", sc_short)
        return "short", sc_short

    log.info("⬛ Sin señal (score L=%d S=%d < %d)", sc_long, sc_short, MIN_SCORE)
    return None, 0
