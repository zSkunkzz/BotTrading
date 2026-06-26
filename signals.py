"""signals.py — Sistema de señales premium con scoring 0-100.

Filtros:
  1. Vela cerrada       : penúltima vela (ya cerrada)
  2. Régimen de mercado : clasificación por EMAs 1h (ADX no bloquea, penaliza)
                          Requiere REGIME_CONFIRM_BARS velas 1h consecutivas
                          confirmando el mismo régimen antes de habilitar señales
  3. Macro 4h           : EMA50 en 4h — bonus/penalización/neutro según disponibilidad
  4. EMA200 1h          : hard-guard de dirección (calculado sobre velas cerradas)
  5. EMA200 15m         : hard-guard de dirección en SHORTs únicamente
  6. ATR volátil        : hard-guard — si atr_pct > 3.5% mercado en evento/noticia,
                          SL real desborda el cap de 2.5% → no entrar
  7. ADX 15m            : hard-guard <15 (sin tendencia suficiente, no operar)
                          >35 +12, >25 +6, >18 neutro, 15-18 -8
  8. ADX 1h             : >25 +5, <18 -5 (fuerza de tendencia en marco superior)
                          hard-guard short: bear + adx_1h < 18 → no entrar (rebote choppy)
  9. RSI 15m            : cruce 50 +15, extremo >58/<42 +8, direccional +4,
                          contrario -5
                          hard-guard short: RSI < 38 → rebote probable, no entrar
  10. MACD 15m + 1h     : histograma positivo/negativo → +10 | neutro 0 | contrario -5
  11. Volumen           : >1.2×media +10 | <0.8×media -5
  12. Divergencia RSI   : +8 (con validación de volumen para evitar señales espurias)
  13. Sesgo horario     : hora alta +8, hora baja -10 (basado en ts de vela cerrada)
                          HIGH_BIAS_HOURS incluye 13 UTC (máx volumen EU/pre-NY)
  14. Filtro no-chase   : rango vela ≤2×ATR (hard-guard)
  15. Filtro pullback   : precio dentro del 1.5% de EMA20_15m — evita entrar en
                          sobreextensión.
  16. Score direccional : SHORTs requieren min_score+5 (default 77 vs 72 para LONGs)

Score base: 20 pts (por superar hard-guards)
Macro 4h:  +15 a favor | 0 si sin datos | -10 en contra
Sizing en risk.py: mult=1.0 (score 72-84) | 1.4 (≥85)
MIN_SCORE configurable via env var MIN_SCORE (default 72)

FIXES aplicados:
  - _market_regime: usa closes[-2] (vela 1h cerrada) para price, no closes[-1]
  - _market_regime: EMAs calculadas sobre closes[:-1] (velas cerradas) — FIX BUG
  - _market_regime: _adx calculado sobre candles_1h[:-1] — FIX BUG (excluye vela abierta)
  - _regime_confirmed: estado actual calculado con candles_1h[:-1] igual que histórico
  - _macro_pts: FIX CRÍTICO — usa closes[-2] (vela 4h cerrada) en lugar de closes[-1]
  - _rsi_divergence: guard len corregido para listas ya recortadas — FIX BUG
  - _rsi_divergence: valida volumen mínimo de la última vela para evitar divergencias espurias
  - _rsi_divergence: protegido max() sobre generador vacío con try/except — FIX BUG
  - rsi_dir: añadido umbral de zona (LONG>45, SHORT<55) para evitar +4 en territorio contrario
  - rsi_ext LONG: umbral 55→58 (zona 55-58 no es momentum confirmado)
  - ADX hard-guard bajado de <18 a <15 (zona 15-18 = neutro, no bloqueado)
  - evaluate(): devuelve (signal, score, regime) para que main.py no recalcule régimen
  - REGIME_CONFIRM_BARS: 4 → 2 (3 velas consecutivas en vez de 5)
  - SHORT_MIN_SCORE_EXTRA: +10 → +5 (umbral short 82 → 77)
  - Hard-guard RSI < 38 para SHORTs (rebote probable en sobrevendido)
  - Hard-guard EMA200 15m: solo para SHORTs (eliminado en LONGs — redundante con EMA200 1h)
  - _ema: guard lista vacía → devuelve [] en lugar de IndexError — FIX BUG
  - _volume_ok: excluye vela evaluada (candles[-2]) del cálculo de la media — FIX BUG
  - Volumen: bonus +8→+10, penalización -5 si volumen bajo (<0.8×media)
  - Filtro pullback EMA20_15m: ±1.5%
  - MIN_SCORE subido de 70 → 72 para filtrar señales borderline
  - HIGH_BIAS_HOURS: añadida hora 13 UTC (pre-NY / máx volumen europeo)
  - Log señal: añade margin= y pullback_dist= para diagnóstico en producción
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
VOLUME_WEAK         = 0.8   # por debajo → penalización
MIN_SCORE           = config.MIN_SCORE

# Margen pullback EMA20_15m: ±1.5% sobre la EMA20.
PULLBACK_EMA20_DIST = 0.015

REGIME_CONFIRM_BARS   = 2
SHORT_MIN_SCORE_EXTRA = 5
ATR_VOLATILE_PCT      = 0.035

HIGH_BIAS_HOURS = {8, 9, 10, 13, 14, 15, 16, 20, 21}   # 13 UTC = pre-NY / máx vol EU
LOW_BIAS_HOURS  = {2, 3, 4, 5}


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
    """Detecta divergencia RSI-precio en la ventana reciente.

    Recibe listas ya recortadas (vela en curso excluida).
    """
    # Guard: necesitamos al menos lookback + period para RSI fiable
    if len(closes) < lookback + 14 or len(candles) < lookback + 1:
        return None
    rsi_series     = _rsi(closes, 14)
    recent_closes  = closes[-lookback:]
    recent_rsi     = rsi_series[-lookback:]
    recent_candles = candles[-lookback:]

    # Validación de volumen: la última vela evaluada debe tener volumen decente
    avg_vol  = sum(c["volume"] for c in recent_candles[:-1]) / max(1, len(recent_candles) - 1)
    last_vol = recent_candles[-1].get("volume", 0.0)
    if avg_vol > 0 and last_vol < avg_vol * 0.6:
        log.debug(
            "_rsi_divergence descartada: volumen bajo (%.2f < 60%% de media %.2f)",
            last_vol, avg_vol,
        )
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
    # FIX: ADX calculado sobre velas cerradas para excluir la vela 1h en curso
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


def _regime_confirmed(candles_1h: list[dict], n: int = REGIME_CONFIRM_BARS) -> tuple[str, float]:
    if len(candles_1h) < 200 + n + 1:
        return "range", 0.0

    closed_1h = candles_1h[:-1]
    regime_now, adx_now = _market_regime(closed_1h)

    regimes_prev = []
    for k in range(1, n + 1):
        window = closed_1h[:-k]
        if len(window) < 200:
            return "range", adx_now
        regime_hist, _ = _market_regime(window)
        regimes_prev.append(regime_hist)

    all_regimes = regimes_prev + [regime_now]

    if len(set(all_regimes)) == 1:
        log.debug("régimen confirmado ×%d: %s", n + 1, regime_now)
        return regime_now, adx_now

    log.info("⚠️  Régimen inestable %s — esperando confirmación", all_regimes)
    return "range", adx_now


def _volume_ok(candles: list[dict], window: int = 20) -> bool:
    if len(candles) < window + 2:
        return False
    recent   = candles[-(window + 2):-2]
    if not recent:
        return False
    avg_vol  = sum(c["volume"] for c in recent) / len(recent)
    last_vol = candles[-2]["volume"]
    return last_vol > avg_vol * VOLUME_MULT


def _volume_weak(candles: list[dict], window: int = 20) -> bool:
    """Devuelve True si el volumen de la vela evaluada es débil (<0.8× media)."""
    if len(candles) < window + 2:
        return False
    recent   = candles[-(window + 2):-2]
    if not recent:
        return False
    avg_vol  = sum(c["volume"] for c in recent) / len(recent)
    last_vol = candles[-2]["volume"]
    return avg_vol > 0 and last_vol < avg_vol * VOLUME_WEAK


def _macro_pts(candles_4h: list[dict] | None, direction: str) -> int:
    if not candles_4h or len(candles_4h) < 52:
        return 0
    closes = [c["close"] for c in candles_4h]
    ema50  = _ema(closes[:-1], 50)[-1]
    price  = closes[-2] if len(closes) >= 2 else closes[-1]
    if direction == "long":
        return 15 if price > ema50 else -10
    return 15 if price < ema50 else -10


def evaluate(
    candles_15m: list[dict],
    candles_1h:  list[dict],
    candles_4h:  list[dict] | None = None,
    min_score:   int = MIN_SCORE,
) -> tuple[str | None, int, str | None]:
    """Evalúa señal con scoring 0-100.

    Devuelve (signal, score, regime) o (None, score, None).
    """

    # ── 0. Datos mínimos ──────────────────────────────────────────────────
    if len(candles_15m) < 50 or len(candles_1h) < 210:
        return None, 0, None

    # ── 1. Vela cerrada ───────────────────────────────────────────────────
    candle     = candles_15m[-2]
    closes_15m = [c["close"] for c in candles_15m]

    # ── 2. Régimen de mercado ─────────────────────────────────────────────
    regime, adx_1h = _regime_confirmed(candles_1h)
    if regime == "range":
        return None, 0, regime

    direction = "long" if regime == "bull" else "short"

    # ── 3. Score base ─────────────────────────────────────────────────────
    score = 20

    # ── 4. Macro 4h ───────────────────────────────────────────────────────
    score += _macro_pts(candles_4h, direction)

    # ── 5. EMA200 1h hard-guard ───────────────────────────────────────────
    closes_1h_closed = [c["close"] for c in candles_1h[:-1]]
    closes_1h_full   = [c["close"] for c in candles_1h]
    ema200_1h = _ema(closes_1h_closed, 200)[-1]
    price     = closes_15m[-2]
    if direction == "long"  and price < ema200_1h * (1 - EMA200_MIN_DIST):
        return None, score, regime
    if direction == "short" and price > ema200_1h * (1 + EMA200_MIN_DIST):
        return None, score, regime

    # ── 5b. EMA200 15m hard-guard — solo SHORTs ───────────────────────────
    # En LONGs es redundante con EMA200 1h. En SHORTs añade capa extra útil.
    if len(closes_15m) >= 202:
        ema200_15m = _ema(closes_15m[:-1], 200)[-1]
        if direction == "short" and price > ema200_15m * 1.002:
            log.debug(
                "Hard-guard EMA200 15m: precio %.6f > EMA200_15m %.6f — SHORT bloqueado",
                price, ema200_15m,
            )
            return None, score, regime

    # ── 5c. Guard ATR volátil ─────────────────────────────────────────────
    atr_15m_raw = _atr(candles_15m[:-1])
    if price > 0 and atr_15m_raw / price > ATR_VOLATILE_PCT:
        log.info(
            "Hard-guard ATR volátil: atr_pct=%.2f%% > %.1f%% — mercado en evento, señal descartada",
            atr_15m_raw / price * 100, ATR_VOLATILE_PCT * 100,
        )
        return None, score, regime

    # ── 6. ADX 15m ────────────────────────────────────────────────────────
    adx_15m = _adx(candles_15m[:-1], 14)

    if adx_15m < 15:
        log.debug("Hard-guard ADX 15m insuficiente: %.1f — señal descartada", adx_15m)
        return None, score, regime

    if   adx_15m > 35: score += 12
    elif adx_15m > 25: score += 6
    elif adx_15m > 18: score += 0   # zona media: neutro
    else:              score -= 8   # zona 15-18: penaliza pero no bloquea

    # ── 7. ADX 1h ─────────────────────────────────────────────────────────
    if   adx_1h > 25: score += 5
    elif adx_1h < 18: score -= 5

    if direction == "short" and adx_1h < 18:
        log.debug(
            "Hard-guard short: regime=bear pero adx_1h=%.1f — mercado rebotando sin tendencia",
            adx_1h,
        )
        return None, score, regime

    # ── 8. Volumen ────────────────────────────────────────────────────────
    vol_strong = _volume_ok(candles_15m)
    vol_weak   = _volume_weak(candles_15m)
    if   vol_strong: score += 10
    elif vol_weak:   score -= 5

    # ── 9. RSI 15m ────────────────────────────────────────────────────────
    rsi_vals = _rsi(closes_15m[:-1], 14)
    rsi_now  = rsi_vals[-1]
    rsi_prev = rsi_vals[-2] if len(rsi_vals) > 1 else rsi_now

    if direction == "short" and rsi_now < 38:
        log.debug(
            "Hard-guard SHORT: RSI sobrevendido %.1f < 38 — rebote probable, señal descartada",
            rsi_now,
        )
        return None, score, regime

    if direction == "long":
        rsi_cross = rsi_prev < 50 <= rsi_now
        rsi_ext   = rsi_now > 58          # FIX: 55→58, zona 55-58 no es momentum confirmado
        rsi_dir   = rsi_now > rsi_prev and rsi_now > 45
        rsi_bad   = rsi_now < 45
    else:
        rsi_cross = rsi_prev > 50 >= rsi_now
        rsi_ext   = rsi_now < 42          # simétrico: <42 en vez de <45
        rsi_dir   = rsi_now < rsi_prev and rsi_now < 55
        rsi_bad   = rsi_now > 60

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

    hist_1h = _macd_histogram(closes_1h_full[:-1])
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
    candle_ts = candle.get("ts")
    if candle_ts:
        hour = datetime.datetime.fromtimestamp(candle_ts / 1000, tz=timezone.utc).hour
    else:
        hour = datetime.datetime.now(timezone.utc).hour

    if   hour in HIGH_BIAS_HOURS: score += 8
    elif hour in LOW_BIAS_HOURS:  score -= 10

    # ── 13. Filtro no-chase (rango vela) ─────────────────────────────────
    candle_rng = candle["high"] - candle["low"]
    if atr_15m_raw > 0 and candle_rng > NO_CHASE_MULT * atr_15m_raw:
        log.debug("No-chase: rng=%.4f > %.1f×ATR=%.4f", candle_rng, NO_CHASE_MULT, atr_15m_raw)
        return None, score, regime

    # ── 14. Filtro pullback EMA20_15m ─────────────────────────────────────
    pullback_dist = None
    if len(closes_15m) >= 22:
        ema20_15m    = _ema(closes_15m[:-1], 20)[-1]
        pullback_dist = (price - ema20_15m) / ema20_15m  # positivo = precio sobre EMA20
        if direction == "long" and price > ema20_15m * (1 + PULLBACK_EMA20_DIST):
            log.debug(
                "Pullback-guard LONG: precio %.6f > EMA20_15m*%.3f %.6f — sobreextendido, esperar",
                price, 1 + PULLBACK_EMA20_DIST, ema20_15m * (1 + PULLBACK_EMA20_DIST),
            )
            return None, score, regime
        if direction == "short" and price < ema20_15m * (1 - PULLBACK_EMA20_DIST):
            log.debug(
                "Pullback-guard SHORT: precio %.6f < EMA20_15m*%.3f %.6f — sobreextendido, esperar",
                price, 1 - PULLBACK_EMA20_DIST, ema20_15m * (1 - PULLBACK_EMA20_DIST),
            )
            return None, score, regime

    # ── 15. Score mínimo (direccional) ───────────────────────────────────
    min_score_directional = min_score + SHORT_MIN_SCORE_EXTRA if direction == "short" else min_score
    if score < min_score_directional:
        log.debug(
            "Score insuficiente: %d < %d (%s, extra=%d)",
            score, min_score_directional, direction,
            SHORT_MIN_SCORE_EXTRA if direction == "short" else 0,
        )
        return None, score, regime

    log.info(
        "✅ SEÑAL %s | score=%d (min=%d, margin=%+d) | regime=%s "
        "adx1h=%.1f adx15m=%.1f rsi=%.1f macd15=%+.4f macd1h=%+.4f "
        "vol=%s div=%s pullback_dist=%s",
        direction.upper(), score, min_score_directional, score - min_score_directional,
        regime, adx_1h, adx_15m, rsi_now, h_now, h1_now,
        "strong" if vol_strong else ("weak" if vol_weak else "normal"),
        div,
        f"{pullback_dist:+.3%}" if pullback_dist is not None else "n/a",
    )
    return direction, score, regime
