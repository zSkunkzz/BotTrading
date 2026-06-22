"""signals.py — Sistema de señales premium con scoring 0-100.

Filtros:
  1. Vela cerrada       : penúltima vela (ya cerrada)
  2. Régimen de mercado : clasificación por EMAs 1h — requiere alineación ESTRICTA
                          price > ema20 > ema50 > ema200 (bull) o inverso (bear).
                          Requiere REGIME_CONFIRM_BARS velas 1h consecutivas confirmando.
  3. Macro 4h           : EMA50 en 4h — bonus/penalización/neutro según disponibilidad
  4. EMA200 1h          : hard-guard de dirección
  5. ADX 15m            : <18 → HARD-GUARD (sin señal) | >35 +12 | >25 +6 | 18-25 0
  6. ADX 1h             : >25 +5, <18 -5 (fuerza de tendencia en marco superior)
  7. Volumen            : última vela CERRADA >media×1.2 → +8
  8. RSI 15m            : cruce 50 +15 (PILAR), extremo >60/<40 +8 (PILAR),
                          direccional +4, contrario -5
  9. MACD 15m + 1h      : histograma acelerando +10 (PILAR 15m, PILAR 1h si acelerando)
                          | positivo/negativo sin acelerar +5/+10 (pts, no pilar)
                          | contrario -5
  10. Divergencia RSI 15m: +8 (PILAR) — zone_pct 0.5%
  11. Divergencia RSI 1h : +12 (PILAR) — zone_pct 1.2% (marco superior, menos ruido)
  12. Soporte/Resistencia: pivots 1h/4h — soporte +10 (PILAR), resistencia -10
  13. Sesgo horario     : dinámico desde CSV (≥20 trades) o fijo si pocos datos
                          HORA MALA = HARD BLOCK (no -10pts compensables, señal bloqueada)
  14. Filtro no-chase   : rango vela ≤2×ATR (hard-guard)
  15. Confluencia mínima: al menos 2 pilares fuertes antes de emitir señal
      Pilares (6 posibles): RSI cruce o extremo | MACD 15m acelerando | MACD 1h acelerando
                            | divergencia 15m/1h | S/R | ADX>28

Score base: 20 pts (por superar hard-guards)
Macro 4h:  +15 a favor | 0 si sin datos | -10 en contra
Sizing en risk.py: mult=0.6 (score 70-84) | 1.0 (≥85)
MIN_SCORE configurable via env var MIN_SCORE (default 55)

CAMBIOS v12:
  - MIN_CONFLUENCE bajado de 3 a 2. Con 3 pilares el bot se secaba: en tendencia
    madura sin cruce RSI reciente, sin ADX>28 y con MACD 1h sin acelerar se podía
    tener score 70+ y 0 señales. 2 pilares mantiene calidad sin secar el flujo.
  - RSI extremo (>60 bull / <40 bear) vuelve a ser pilar de confluencia.
    En v11 el pilar RSI quedó solo en cruce de 50, pero en tendencia establecida
    el RSI vive sobre 60 sin cruzar → el pilar nunca se activaba. Fix: cruce Y
    extremo son pilares válidos (no exclusivos). El cruce sigue sumando +15 y el
    extremo sin cruce +8 puntos, sin cambio en el scoring.

CAMBIOS v11:
  - RSI pilar SOLO en cruce de 50 (revertido en v12).
  - MACD 1h acelerando (hist_1h[-1] > hist_1h[-2]) cuenta como 6º pilar.
  - DIV_ZONE_PCT_1H = 0.012 (1.2%) para divergencias en 1h.

CAMBIOS v10:
  - MIN_CONFLUENCE subido de 2 a 3 pilares (bajado a 2 en v12).
  - Hour bonus negativo ahora es HARD BLOCK.
  - ADX_STRONG subido de 25 a 28.

CAMBIOS v9 (bugfix crítico):
  _rsi_divergence: condición imposible corregida. Fix: precio debe estar dentro
  del 0.5% del pivot (zona de retesteo real).

CAMBIOS v8 (calidad sobre cantidad):
  Guard de confluencia mínima (MIN_CONFLUENCE pilares fuertes requeridos).

CAMBIOS v7 (winrate):
  1. REGIME_CONFIRM_BARS bajado de 2 a 1.
  2. _rsi_divergence vuelve a usar closes para AMBOS pivots.
  3. MACD débil da +5 (antes 0).

CAMBIOS anteriores:
  v6 - _rsi_divergence: max() en lugar de min() para pivot más reciente.
  v5 - evaluate() retorna (signal, score, regime) para TP_RR dinámico.
  v5 - divergencia 1h guard subida de >=29 a >=35.
  v4 - _rsi_divergence usa highs/lows para extremo reciente (revertido en v7).
  v4 - _macro_pts simplificada.
  v4 - _volume_ok guard off-by-one.
  v4 - _ema guard lista vacía.
  v3 - dead branch MACD eliminado.
  v3 - _regime_confirmed offset loop empieza en 1.
  v3 - _hour_bonus usa timestamp de la vela.
  v2 - lookahead bias vela viva corregido.
  v2 - ADX 18-25 scoring corregido (0 en vez de -8).
  v2 - MACD 1h lógica corregida.
  v1 - _rsi_divergence max() con default=-1.
  v1 - _regime_confirmed guard de longitud mínima.
"""
from __future__ import annotations
import csv
import logging
import datetime
import os
import time
from datetime import timezone
from collections import defaultdict

import config
import indicators as ind

log = logging.getLogger("signals")

# ── Umbrales configurables ────────────────────────────────────────────────
EMA200_MIN_DIST     = 0.003
NO_CHASE_MULT       = 2.0
VOLUME_MULT         = 1.2
MIN_SCORE           = config.MIN_SCORE
REGIME_CONFIRM_BARS = 1
ADX_MIN             = 18
ADX_STRONG          = 28
SR_ZONE_PCT         = 0.004
SR_PIVOT_BARS_1H    = 3
SR_PIVOT_BARS_4H    = 2
MIN_CONFLUENCE      = 2
DIV_ZONE_PCT        = 0.005   # 15m: 0.5% — más ruido, más estricto
DIV_ZONE_PCT_1H     = 0.012   # 1h:  1.2% — velas más grandes, zona más amplia

HIGH_BIAS_HOURS = {8, 9, 10, 14, 15, 16, 20, 21}
LOW_BIAS_HOURS  = {2, 3, 4, 5}

HOUR_BIAS_MIN_TRADES = 20
HOUR_BIAS_GOOD_PCT   = 0.5
HOUR_BIAS_BAD_PCT    = -0.5

_hour_bias_cache:     dict[int, float] = {}
_hour_bias_cache_ts:  float = 0.0
_hour_bias_cache_ttl: float = 3600.0
_hour_bias_n_trades:  int   = 0


# ── Indicadores locales ───────────────────────────────────────────────────

def _ema(closes: list[float], period: int) -> list[float]:
    return ind.ema(closes, period)


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
    return ind.atr(candles, period)


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
    zone_pct: float = DIV_ZONE_PCT,
) -> str | None:
    """Detecta divergencia RSI-precio usando closes para ambos pivots.

    Divergencia alcista: precio retestea el mínimo reciente (dentro de zone_pct)
    mientras RSI forma un mínimo más alto → señal de agotamiento bajista.

    Divergencia bajista: precio retestea el máximo reciente (dentro de zone_pct)
    mientras RSI forma un máximo más bajo → señal de agotamiento alcista.
    """
    if len(closes) < lookback + 14:
        return None
    if len(candles) < lookback:
        return None

    rsi_series    = _rsi(closes, 14)
    recent_closes = closes[-lookback:]
    recent_rsi    = rsi_series[-lookback:]

    # ── Divergencia alcista ───────────────────────────────────────────────
    lo_val = min(recent_closes[:-3])
    lo_idx = max(
        (i for i, v in enumerate(recent_closes[:-3]) if v == lo_val),
        default=-1,
    )
    if lo_idx != -1 and recent_closes[-1] <= lo_val * (1 + zone_pct) and recent_rsi[-1] > recent_rsi[lo_idx] + 2:
        return "bullish"

    # ── Divergencia bajista ───────────────────────────────────────────────
    hi_val = max(recent_closes[:-3])
    hi_idx = max(
        (i for i, v in enumerate(recent_closes[:-3]) if v == hi_val),
        default=-1,
    )
    if hi_idx != -1 and recent_closes[-1] >= hi_val * (1 - zone_pct) and recent_rsi[-1] < recent_rsi[hi_idx] - 2:
        return "bearish"

    return None


def _pivot_levels(
    candles: list[dict],
    n: int,
    max_pivots: int = 8,
) -> tuple[list[float], list[float]]:
    supports:    list[float] = []
    resistances: list[float] = []
    start = n
    end   = len(candles) - n
    if end <= start:
        return [], []

    for i in range(start, end):
        left_high  = [candles[j]["high"] for j in range(i - n, i)]
        right_high = [candles[j]["high"] for j in range(i + 1, i + n + 1)]
        if candles[i]["high"] > max(left_high) and candles[i]["high"] > max(right_high):
            resistances.append(candles[i]["high"])

        left_low  = [candles[j]["low"] for j in range(i - n, i)]
        right_low = [candles[j]["low"] for j in range(i + 1, i + n + 1)]
        if candles[i]["low"] < min(left_low) and candles[i]["low"] < min(right_low):
            supports.append(candles[i]["low"])

    return supports[-max_pivots:], resistances[-max_pivots:]


def _sr_context(
    price: float,
    candles_1h: list[dict],
    candles_4h: list[dict] | None,
    zone_pct: float = SR_ZONE_PCT,
) -> str:
    near_support    = False
    near_resistance = False
    dist_sup        = float("inf")
    dist_res        = float("inf")

    if len(candles_1h) >= 2 * SR_PIVOT_BARS_1H + 5:
        sups_1h, ress_1h = _pivot_levels(candles_1h, SR_PIVOT_BARS_1H)
        for lvl in sups_1h:
            d = abs(price - lvl) / price
            if d <= zone_pct and d < dist_sup:
                near_support = True
                dist_sup     = d
        for lvl in ress_1h:
            d = abs(price - lvl) / price
            if d <= zone_pct and d < dist_res:
                near_resistance = True
                dist_res        = d

    if candles_4h and len(candles_4h) >= 2 * SR_PIVOT_BARS_4H + 5:
        zone_4h = zone_pct * 1.5
        sups_4h, ress_4h = _pivot_levels(candles_4h, SR_PIVOT_BARS_4H)
        for lvl in sups_4h:
            d = abs(price - lvl) / price
            if d <= zone_4h and d < dist_sup:
                near_support = True
                dist_sup     = d
        for lvl in ress_4h:
            d = abs(price - lvl) / price
            if d <= zone_4h and d < dist_res:
                near_resistance = True
                dist_res        = d

    if near_support and near_resistance:
        return "support" if dist_sup <= dist_res else "resistance"
    if near_support:
        return "support"
    if near_resistance:
        return "resistance"
    return "none"


def _load_hour_bias_from_csv() -> None:
    global _hour_bias_cache, _hour_bias_cache_ts, _hour_bias_n_trades

    now = time.time()
    if now - _hour_bias_cache_ts < _hour_bias_cache_ttl:
        return

    csv_path = os.getenv("TRADES_CSV", "trades.csv")
    if not os.path.exists(csv_path):
        _hour_bias_cache_ts = now
        _hour_bias_n_trades = 0
        return

    hour_pnl: dict[int, list[float]] = defaultdict(list)
    total = 0
    try:
        with open(csv_path, newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    hour = int(row["date"].split(" ")[1].split(":")[0])
                    pnl  = float(row["pnl_pct"])
                    hour_pnl[hour].append(pnl)
                    total += 1
                except (KeyError, ValueError, IndexError):
                    continue
    except Exception as e:
        log.debug("Error leyendo CSV para sesgo horario: %s", e)
        _hour_bias_cache_ts = now
        _hour_bias_n_trades = 0
        return

    _hour_bias_cache    = {h: sum(v) / len(v) for h, v in hour_pnl.items()}
    _hour_bias_cache_ts = now
    _hour_bias_n_trades = total
    log.debug("Sesgo horario dinámico cargado: %d trades, %d horas", total, len(_hour_bias_cache))


def _hour_bonus(hour_utc: int) -> int:
    """Retorna el bonus horario. Valores negativos indican hora mala."""
    _load_hour_bias_from_csv()

    if _hour_bias_n_trades >= HOUR_BIAS_MIN_TRADES:
        mean_pnl = _hour_bias_cache.get(hour_utc, 0.0)
        if mean_pnl > HOUR_BIAS_GOOD_PCT:
            return 8
        if mean_pnl < HOUR_BIAS_BAD_PCT:
            return -10  # señal de hora mala — evaluate() bloqueará
        return 0

    if hour_utc in HIGH_BIAS_HOURS:
        return 8
    if hour_utc in LOW_BIAS_HOURS:
        return -10  # señal de hora mala — evaluate() bloqueará
    return 0


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
    return "range", adx


def _regime_confirmed(candles_1h: list[dict], n: int = REGIME_CONFIRM_BARS) -> tuple[str, float]:
    """Devuelve el régimen solo si las últimas n velas cerradas coinciden."""
    if len(candles_1h) < 200 + n + 1:
        return "range", 0.0

    regimes = []
    for offset in range(1, n + 1):
        window = candles_1h[:-offset]
        if len(window) < 210:
            return "range", 0.0
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
    if len(candles) < window + 2:
        return True
    vols = [c["volume"] for c in candles[-(window + 1):-1]]
    avg  = sum(vols) / len(vols)
    return candles[-1]["volume"] >= avg * VOLUME_MULT


# ── Función principal ─────────────────────────────────────────────────────

def evaluate(
    candles_15m: list[dict],
    candles_1h:  list[dict],
    candles_4h:  list[dict] | None = None,
    min_score:   int | None = None,
) -> tuple[str | None, int, str]:
    """Evalúa señales.

    Returns:
        (signal, score, regime)
        signal : 'long' | 'short' | None
        score  : 0-100
        regime : 'bull' | 'bear' | 'range'
    """
    effective_min = min_score if min_score is not None else MIN_SCORE

    if len(candles_1h) < 210:
        return None, 0, "range"
    if len(candles_15m) < 60:
        return None, 0, "range"

    closed_15m = candles_15m[:-1]
    closed_1h  = candles_1h[:-1]
    closed_4h  = candles_4h[:-1] if candles_4h and len(candles_4h) > 1 else None

    closes_15m = [c["close"] for c in closed_15m]
    closes_1h  = [c["close"] for c in closed_1h]

    regime, adx_1h = _regime_confirmed(closed_1h)
    if regime == "range":
        log.info("⬛ Régimen lateral o inestable — sin señal")
        return None, 0, "range"

    ema200 = _ema(closes_1h, 200)[-1]
    price  = closes_15m[-1]
    dist   = abs(price - ema200) / ema200
    if dist < EMA200_MIN_DIST:
        log.info("⚠️  Precio cerca del EMA200 (%.3f%%) — sin señal", dist * 100)
        return None, 0, regime

    trend_long  = regime == "bull"
    trend_short = regime == "bear"

    atr        = _atr(closed_15m, 14)
    last_range = closed_15m[-1]["high"] - closed_15m[-1]["low"]
    if atr > 0 and last_range > NO_CHASE_MULT * atr:
        log.info("⚠️  Vela explosiva — sin señal")
        return None, 0, regime

    adx = _adx(closed_15m, 14)
    if adx < ADX_MIN:
        log.info("⚠️  ADX 15m demasiado bajo (%.1f < %d) — mercado sin momentum", adx, ADX_MIN)
        return None, 0, regime

    candle_ts  = closed_15m[-1].get("time", 0)
    hour_utc   = datetime.datetime.utcfromtimestamp(candle_ts / 1000).hour if candle_ts else datetime.datetime.now(timezone.utc).hour
    hour_bonus = _hour_bonus(hour_utc)

    if hour_bonus < 0:
        log.info("⬛ Hora mala (UTC %d, bias=%+d) — sin señal", hour_utc, hour_bonus)
        return None, 0, regime

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
    rsi_bull       = rsi_curr > 50
    rsi_bear       = rsi_curr < 50
    rsi_ext_bull   = rsi_curr > 60
    rsi_ext_bear   = rsi_curr < 40

    divergence_15m = _rsi_divergence(closes_15m, closed_15m, lookback=10, zone_pct=DIV_ZONE_PCT)

    divergence_1h: str | None = None
    if len(closes_1h) >= 35:
        divergence_1h = _rsi_divergence(closes_1h, closed_1h, lookback=15, zone_pct=DIV_ZONE_PCT_1H)

    sr = _sr_context(price, closed_1h, closed_4h)

    hist_15m = _macd_histogram(closes_15m)
    hist_1h  = _macd_histogram(closes_1h)

    macd15_bull_strong = hist_15m[-1] > 0 and hist_15m[-1] > hist_15m[-2]
    macd15_bull_weak   = hist_15m[-1] > 0 and not macd15_bull_strong
    macd15_bear_strong = hist_15m[-1] < 0 and hist_15m[-1] < hist_15m[-2]
    macd15_bear_weak   = hist_15m[-1] < 0 and not macd15_bear_strong

    macd1h_bull         = hist_1h[-1] > 0
    macd1h_bear         = hist_1h[-1] < 0
    macd1h_bull_accel   = macd1h_bull and hist_1h[-1] > hist_1h[-2]
    macd1h_bear_accel   = macd1h_bear and hist_1h[-1] < hist_1h[-2]

    vol_ok = _volume_ok(closed_15m)

    def _macro_pts(macro: bool | None) -> int:
        if macro is None:
            return 0
        return 15 if macro else -10

    def score_long() -> int:
        if not trend_long:
            return 0
        s = 20
        s += _macro_pts(macro_long)
        if adx > 35:        s += 12
        elif adx > 25:      s += 6
        if adx_1h < 18:     s -= 5
        elif adx_1h > 25:   s += 5
        # RSI: cruce +15, extremo sin cruce +8, direccional +4
        if rsi_cross_up:    s += 15
        elif rsi_ext_bull:  s += 8
        elif rsi_bull:      s += 4
        else:               s -= 5
        if macd15_bull_strong:   s += 10
        elif macd15_bull_weak:   s += 5
        else:                    s -= 5
        # MACD 1h: acelerando +10 (es pilar), sin acelerar también +10 pts
        if macd1h_bull:     s += 10
        elif macd1h_bear:   s -= 5
        if vol_ok:          s += 8
        if divergence_15m == "bullish":   s += 8
        if divergence_1h  == "bullish":   s += 12
        if sr == "support":               s += 10
        elif sr == "resistance":          s -= 10
        s += hour_bonus
        return min(max(s, 0), 100)

    def score_short() -> int:
        if not trend_short:
            return 0
        s = 20
        s += _macro_pts(macro_short)
        if adx > 35:         s += 12
        elif adx > 25:       s += 6
        if adx_1h < 18:      s -= 5
        elif adx_1h > 25:    s += 5
        # RSI: cruce +15, extremo sin cruce +8, direccional +4
        if rsi_cross_down:   s += 15
        elif rsi_ext_bear:   s += 8
        elif rsi_bear:       s += 4
        else:                s -= 5
        if macd15_bear_strong:   s += 10
        elif macd15_bear_weak:   s += 5
        else:                    s -= 5
        # MACD 1h: acelerando +10 (es pilar), sin acelerar también +10 pts
        if macd1h_bear:      s += 10
        elif macd1h_bull:    s -= 5
        if vol_ok:           s += 8
        if divergence_15m == "bearish":   s += 8
        if divergence_1h  == "bearish":   s += 12
        if sr == "resistance":            s += 10
        elif sr == "support":             s -= 10
        s += hour_bonus
        return min(max(s, 0), 100)

    sc_long  = score_long()
    sc_short = score_short()

    macro_l_str = "None" if macro_long  is None else str(macro_long)
    macro_s_str = "None" if macro_short is None else str(macro_short)

    log.info(
        "regime=%s(×%d) adx1h=%.1f hour=%dUTC(bias=%+d) | price=%.4f ema200=%.4f dist=%.3f%% "
        "ADX15m=%.1f rsi=%.1f→%.1f vol=%s "
        "div15m=%s div1h=%s sr=%s macro_l=%s macro_s=%s "
        "macd15=%.5f macd1h=%.5f macd1h_accel_bull=%s macd1h_accel_bear=%s "
        "| score_long=%d score_short=%d (min=%d)",
        regime, REGIME_CONFIRM_BARS, adx_1h, hour_utc, hour_bonus,
        price, ema200, dist * 100,
        adx, rsi_prev, rsi_curr, vol_ok,
        divergence_15m, divergence_1h, sr,
        macro_l_str, macro_s_str,
        hist_15m[-1], hist_1h[-1], macd1h_bull_accel, macd1h_bear_accel,
        sc_long, sc_short, effective_min,
    )

    # ── v12: Guard de confluencia mínima (2 de 6 pilares) ────────────────
    # Pilar RSI: cruce de 50 O extremo >60/<40. En tendencia madura el RSI
    # vive sobre 60 sin cruzar → ambas condiciones son pilares válidos.
    # Pilar MACD 1h: solo si está acelerando en la dirección correcta.
    pilares_long = sum([
        bool(rsi_cross_up or rsi_ext_bull),                               # RSI cruce o extremo alcista
        bool(macd15_bull_strong),                                         # MACD 15m acelerando alcista
        bool(macd1h_bull_accel),                                          # MACD 1h acelerando alcista
        bool(divergence_15m == "bullish" or divergence_1h == "bullish"),  # divergencia
        bool(sr == "support"),                                            # soporte
        bool(adx > ADX_STRONG),                                           # ADX fuerte
    ])

    pilares_short = sum([
        bool(rsi_cross_down or rsi_ext_bear),                             # RSI cruce o extremo bajista
        bool(macd15_bear_strong),                                         # MACD 15m acelerando bajista
        bool(macd1h_bear_accel),                                          # MACD 1h acelerando bajista
        bool(divergence_15m == "bearish" or divergence_1h == "bearish"),  # divergencia
        bool(sr == "resistance"),                                         # resistencia
        bool(adx > ADX_STRONG),                                           # ADX fuerte
    ])

    if sc_long >= effective_min and sc_long >= sc_short:
        if pilares_long < MIN_CONFLUENCE:
            log.info(
                "⬛ LONG score=%d pero solo %d pilar(es) fuerte(s) de %d requeridos "
                "[rsi=%s macd15_accel=%s macd1h_accel=%s div=%s sr_sup=%s adx>28=%s] — calidad insuficiente",
                sc_long, pilares_long, MIN_CONFLUENCE,
                rsi_cross_up or rsi_ext_bull,
                macd15_bull_strong,
                macd1h_bull_accel,
                divergence_15m == "bullish" or divergence_1h == "bullish",
                sr == "support",
                adx > ADX_STRONG,
            )
            return None, 0, regime
        log.info(
            "✅ LONG score=%d pilares=%d/6 (rsi=%s macd15=%s macd1h_accel=%s div15m=%s div1h=%s sr=%s bias=%+d)",
            sc_long, pilares_long,
            rsi_cross_up or rsi_ext_bull, macd15_bull_strong, macd1h_bull_accel,
            divergence_15m, divergence_1h, sr, hour_bonus,
        )
        return "long", sc_long, regime

    if sc_short >= effective_min and sc_short > sc_long:
        if pilares_short < MIN_CONFLUENCE:
            log.info(
                "⬛ SHORT score=%d pero solo %d pilar(es) fuerte(s) de %d requeridos "
                "[rsi=%s macd15_accel=%s macd1h_accel=%s div=%s sr_res=%s adx>28=%s] — calidad insuficiente",
                sc_short, pilares_short, MIN_CONFLUENCE,
                rsi_cross_down or rsi_ext_bear,
                macd15_bear_strong,
                macd1h_bear_accel,
                divergence_15m == "bearish" or divergence_1h == "bearish",
                sr == "resistance",
                adx > ADX_STRONG,
            )
            return None, 0, regime
        log.info(
            "✅ SHORT score=%d pilares=%d/6 (rsi=%s macd15=%s macd1h_accel=%s div15m=%s div1h=%s sr=%s bias=%+d)",
            sc_short, pilares_short,
            rsi_cross_down or rsi_ext_bear, macd15_bear_strong, macd1h_bear_accel,
            divergence_15m, divergence_1h, sr, hour_bonus,
        )
        return "short", sc_short, regime

    log.info("⬛ Sin señal (score L=%d S=%d < %d)", sc_long, sc_short, effective_min)
    return None, 0, regime
