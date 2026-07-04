"""signals.py — Sistema de señales v6.

v6 — Techo ~96, MIN_SCORE=70 fijo
  - Máximo teórico ~96 sin divergencia ~86; MIN_SCORE=70 = 73% del techo práctico
  - Régimen simplificado: price vs EMA200 + EMA50 vs EMA200 + proto si ADX>=18
  - Hard-guards macro 4h, structure-range+ADX, bear+ADX<22, bear EMA200_15m
    convertidos en penalizaciones suaves (no matan la señal)
  - Eliminado sesgo horario del scoring
  - Eliminadas penalizaciones de vela diaria del scoring
  - PROTO_SCORE_PENALTY eliminado (la condición de proto ya implica menos alineación)
  - Pesos rebalanceados para techo ~96:
      W_ADX_1H_30=20, W_ADX_1H_25=14, W_ADX_1H_20=5
      W_MACD_1H=14, W_RSI_IDEAL=13, W_MACD_15M=12
      W_VOLUME_HIGH=11, W_STRUCTURE=10, W_DIVERGENCIA=10, W_VELA=6
  - Penalizaciones soft: W_MACRO_CONTRA=-8, W_BEAR_EMA200_15M=-6,
      W_VOLUME_LOW=-4, W_RSI_SOBRE=-8, W_STRUCTURE_CONTRA=-8

Techo real por ruta:
  ADX_1H>=30 (20) + MACD_1H (14) + RSI_IDEAL (13) + MACD_15M (12)
  + VOLUME_HIGH (11) + STRUCTURE (10) + VELA (6) = 86 sin divergencia
  + DIVERGENCIA (10) = 96 con divergencia

Filtros activos:
  1. Vela cerrada        : penúltima vela
  2. Régimen simplificado: price/EMA200 + EMA50/EMA200 + proto si ADX>=18
  3. Macro 4h            : penalización suave si contradice (no hard-guard)
  4. EMA200 1h           : hard-guard dirección (mantenido)
  5. EMA200 15m bear     : penalización suave (no hard-guard)
  6. ATR volátil         : hard-guard >3.5%
  7. ADX 15m             : hard-guard <18
  8. No-chase            : hard-guard rango vela
  9. Pullback EMA20 15m  : hard-guard sobreextensión (dinámico con ATR)
  10. Structure range    : penalización en vez de bloqueo
  11. Bear ADX_1h<22     : penalización en vez de bloqueo
  12. Score mínimo       : LONGs >= MIN_SCORE, SHORTs >= MIN_SCORE+SHORT_EXTRA

REGLA FUNDAMENTAL:
  Bear/proto_bear → SOLO SHORT. Bull/proto_bull → SOLO LONG.

v6.1 — Mejoras de calidad de señal:
  - _rsi_divergence: busca el swing más reciente (último índice del extremo)
    en vez del extremo absoluto para evitar falsas divergencias planas.
    Además verifica que el swing previo sea estructuralmente significativo
    (diferencia mínima del 0.5% respecto al último cierre).
  - PULLBACK_EMA20_DIST dinámico: max(0.015, atr_15m_pct * 1.5) para escalar
    con la volatilidad real del par y no filtrar memecoins de forma excesiva.
  - ATR_HIGH_VOL_BUMP: reducido de 5 a 3 para no penalizar doblemente señales
    técnicamente sólidas en entornos de alta volatilidad (el hard-guard de
    ATR_VOLATILE_PCT=3.5% ya cubre el caso extremo).
  - _price_structure: n=1→2, requiere mínimo 2 HH+HL o 2 LH+LL consecutivos
    para clasificar como bull/bear, evitando falsos positivos estructurales.
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

# v6.1: PULLBACK_EMA20_DIST se calcula dinámicamente en evaluate()
# usando max(PULLBACK_EMA20_DIST_BASE, atr_15m_pct * PULLBACK_ATR_MULT)
PULLBACK_EMA20_DIST_BASE = 0.015
PULLBACK_ATR_MULT        = 1.5

SHORT_MIN_SCORE_EXTRA = 6
ATR_VOLATILE_PCT      = 0.035
ADX_15M_MIN           = 18

# ── Umbrales estructura ───────────────────────────────────────────────────
STRUCTURE_LOOKBACK    = 12
MIN_HOURLY_VOLUME     = 100_000   # USDT — ajustado a liquidez real de Hyperliquid (era 150k)

# ── Umbrales proto-régimen ────────────────────────────────────────────────
PROTO_ADX_MIN         = 18   # era 22 — simplificado

# ── Volatilidad dinámica ──────────────────────────────────────────────────
ATR_HIGH_VOL_PCT      = 0.020
ATR_LOW_VOL_PCT       = 0.005
ATR_HIGH_VOL_BUMP     = 3    # v6.1: reducido de 5 a 3 (no doble penalización)
ATR_LOW_VOL_BUMP      = 4

# ── Pesos del scorer v6 — techo ~96 ──────────────────────────────────────
W_ADX_1H_30        = 20
W_ADX_1H_25        = 14
W_ADX_1H_20        =  5
W_MACD_1H          = 14
W_RSI_IDEAL        = 13
W_MACD_15M         = 12
W_VOLUME_HIGH      = 11
W_STRUCTURE        = 10
W_DIVERGENCIA      = 10
W_VELA             =  6

# Penalizaciones suaves
W_VOLUME_LOW        = -4
W_RSI_SOBRE         = -8
W_STRUCTURE_CONTRA  = -8
W_MACRO_CONTRA      = -8   # macro 4h contradice → penalización (no hard-guard)
W_BEAR_EMA200_15M   = -6   # bear sobreextendido bajo EMA200_15m
W_BEAR_LOW_ADX      = -5   # bear con ADX_1h < 22 (era hard-guard, ahora penalización)


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
    """Detecta divergencia RSI vs precio.

    v6.1: Usa el swing más reciente dentro de la ventana (no el extremo absoluto)
    para evitar falsas divergencias planas. Requiere además que el swing previo
    sea significativamente distinto del último cierre (>= 0.5%) para descartar
    ruido de precios casi idénticos.
    """
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

    last_close = recent_closes[-1]

    # Divergencia alcista: precio hace mínimo más bajo pero RSI hace mínimo más alto
    # Buscamos el último mínimo local (más reciente) en la ventana previa
    try:
        # Mínimo más bajo dentro de la ventana previa
        lo_val = min(recent_closes[:-1])
        # índice más reciente con ese valor mínimo
        lo_idx = max(i for i, v in enumerate(recent_closes[:-1]) if v == lo_val)
        # El último cierre debe estar por debajo del mínimo previo
        # y la diferencia debe ser significativa (>= 0.5%)
        if (last_close < lo_val
                and lo_val > 0
                and abs(last_close - lo_val) / lo_val >= 0.005
                and recent_rsi[-1] > recent_rsi[lo_idx] + 2):
            return "bullish"
    except (ValueError, IndexError):
        pass

    # Divergencia bajista: precio hace máximo más alto pero RSI hace máximo más bajo
    try:
        hi_val = max(recent_closes[:-1])
        hi_idx = max(i for i, v in enumerate(recent_closes[:-1]) if v == hi_val)
        if (last_close > hi_val
                and hi_val > 0
                and abs(last_close - hi_val) / hi_val >= 0.005
                and recent_rsi[-1] < recent_rsi[hi_idx] - 2):
            return "bearish"
    except (ValueError, IndexError):
        pass

    return None


def _market_regime(candles_1h: list[dict]) -> tuple[str, float]:
    """Detecta régimen de mercado — v6 simplificado.

    bull:       price > EMA200 y EMA50 > EMA200
    bear:       price < EMA200 y EMA50 < EMA200
    proto_bull: price > EMA200 y ADX >= PROTO_ADX_MIN (EMA50 aún no ordenada)
    proto_bear: price < EMA200 y ADX >= PROTO_ADX_MIN
    range:      resto
    """
    closes        = [c["close"] for c in candles_1h]
    closes_closed = closes[:-1]
    ema50  = _ema(closes_closed, 50)[-1]
    ema200 = _ema(closes_closed, 200)[-1]
    adx    = _adx(candles_1h[:-1], 14)
    price  = closes[-2] if len(closes) >= 2 else closes[-1]

    if price > ema200 and ema50 > ema200:
        return "bull", adx
    if price < ema200 and ema50 < ema200:
        return "bear", adx
    if price > ema200 and adx >= PROTO_ADX_MIN:
        log.debug(
            "[regime] proto_bull (price=%.6f > EMA200=%.6f adx=%.1f)",
            price, ema200, adx,
        )
        return "proto_bull", adx
    if price < ema200 and adx >= PROTO_ADX_MIN:
        log.debug(
            "[regime] proto_bear (price=%.6f < EMA200=%.6f adx=%.1f)",
            price, ema200, adx,
        )
        return "proto_bear", adx

    return "range", adx


def _find_swing_highs(highs: list[float], wing: int = 2) -> list[int]:
    pivots = []
    for i in range(wing, len(highs) - wing):
        if all(highs[i] > highs[i - j] for j in range(1, wing + 1)) and \
           all(highs[i] > highs[i + j] for j in range(1, wing + 1)):
            pivots.append(i)
    return pivots


def _find_swing_lows(lows: list[float], wing: int = 2) -> list[int]:
    pivots = []
    for i in range(wing, len(lows) - wing):
        if all(lows[i] < lows[i - j] for j in range(1, wing + 1)) and \
           all(lows[i] < lows[i + j] for j in range(1, wing + 1)):
            pivots.append(i)
    return pivots


def _price_structure(candles_1h: list[dict], lookback: int = STRUCTURE_LOOKBACK) -> str:
    """Detecta estructura de precio (bull / bear / range).

    v6.1: n=2 — requiere mínimo 2 HH+HL consecutivos para bull,
    o 2 LH+LL para bear. Evita falsos positivos con un único swing.
    """
    closed = candles_1h[:-1]
    if len(closed) < lookback + 4:
        return "range"

    recent = closed[-(lookback + 4):]
    highs  = [c["high"]  for c in recent]
    lows   = [c["low"]   for c in recent]

    swing_high_idxs = _find_swing_highs(highs, wing=2)
    swing_low_idxs  = _find_swing_lows(lows,  wing=2)

    if len(swing_high_idxs) < 2 or len(swing_low_idxs) < 2:
        return "range"

    swing_high_vals = [highs[i] for i in swing_high_idxs]
    swing_low_vals  = [lows[i]  for i in swing_low_idxs]

    hh_count = sum(
        1 for i in range(1, len(swing_high_vals))
        if swing_high_vals[i] > swing_high_vals[i - 1] * 1.001
    )
    hl_count = sum(
        1 for i in range(1, len(swing_low_vals))
        if swing_low_vals[i] > swing_low_vals[i - 1] * 1.001
    )
    lh_count = sum(
        1 for i in range(1, len(swing_high_vals))
        if swing_high_vals[i] < swing_high_vals[i - 1] * 0.999
    )
    ll_count = sum(
        1 for i in range(1, len(swing_low_vals))
        if swing_low_vals[i] < swing_low_vals[i - 1] * 0.999
    )

    # v6.1: n=2 en vez de n=1 para exigir confirmación estructural mínima
    n = 2
    if hh_count >= n and hl_count >= n:
        return "bull"
    if lh_count >= n and ll_count >= n:
        return "bear"
    return "range"


def _liquidity_ok(candles_1h: list[dict]) -> bool:
    """Verifica que el par tenga volumen USDT suficiente en 1h."""
    recent = candles_1h[-25:-1]
    if not recent:
        return False
    avg_vol = sum(c.get("quote_volume", c.get("volume", 0.0) * c.get("close", 1.0))
                  for c in recent) / len(recent)
    ok = avg_vol >= MIN_HOURLY_VOLUME
    if not ok:
        log.debug(
            "[liquidity] skip — quote_volume_avg_1h=%.0f < umbral %d USDT",
            avg_vol, MIN_HOURLY_VOLUME,
        )
    return ok


def _dynamic_min_score_bump(candles_1h: list[dict], price: float) -> int:
    if price <= 0 or len(candles_1h) < 16:
        return 0
    atr_1h = _atr(candles_1h[:-1], period=14)
    if atr_1h <= 0:
        return 0
    atr_pct = atr_1h / price
    if atr_pct > ATR_HIGH_VOL_PCT:
        log.debug(
            "[vol] ATR_1h=%.4f%% > %.1f%% → min_score +%d (alta volatilidad)",
            atr_pct * 100, ATR_HIGH_VOL_PCT * 100, ATR_HIGH_VOL_BUMP,
        )
        return ATR_HIGH_VOL_BUMP
    if atr_pct < ATR_LOW_VOL_PCT:
        log.debug(
            "[vol] ATR_1h=%.4f%% < %.1f%% → min_score +%d (baja volatilidad)",
            atr_pct * 100, ATR_LOW_VOL_PCT * 100, ATR_LOW_VOL_BUMP,
        )
        return ATR_LOW_VOL_BUMP
    return 0


def evaluate(
    candles_15m: list[dict],
    candles_1h:  list[dict],
    candles_4h:  list[dict] | None = None,
    min_score:   int = MIN_SCORE,
    symbol:      str = "???",
) -> tuple[str | None, int, str | None]:
    """Evalúa si hay señal de entrada. Devuelve (side, score, regime) o (None, score, None)."""

    if len(candles_15m) < 50 or len(candles_1h) < 220:
        log.debug("[%s] skip: candles insuficientes (15m=%d 1h=%d)", symbol, len(candles_15m), len(candles_1h))
        return None, 0, None

    if not _liquidity_ok(candles_1h):
        log.debug("[%s] skip: liquidez insuficiente (avg_vol_1h < %d)", symbol, MIN_HOURLY_VOLUME)
        return None, 0, None

    closed  = candles_15m[-2]
    c_open  = closed["open"]
    c_close = closed["close"]
    c_high  = closed["high"]
    c_low   = closed["low"]
    bullish_candle = c_close > c_open

    # ── Régimen de mercado 1h ────────────────────────────────────────────
    regime, adx_1h = _market_regime(candles_1h)

    is_proto = regime in ("proto_bull", "proto_bear")
    effective_regime = "bull" if regime in ("bull", "proto_bull") else (
        "bear" if regime in ("bear", "proto_bear") else "range"
    )

    if effective_regime == "range":
        log.debug("[%s] skip: régimen=range adx_1h=%.1f", symbol, adx_1h)
        return None, 0, None

    # ── Estructura de precio 1h ──────────────────────────────────────────
    structure = _price_structure(candles_1h)

    # ── Indicadores 15m ──────────────────────────────────────────────────
    closes_15m = [c["close"] for c in candles_15m]
    price      = closes_15m[-2]

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
    vol_ratio  = last_vol / avg_vol if avg_vol else 0.0

    closes_1h        = [c["close"] for c in candles_1h]
    ema200_1h        = _ema(closes_1h[:-1], 200)[-1]
    closes_1h_closed = closes_1h[:-1]
    macd_1h          = _macd_histogram(closes_1h_closed)[-1]
    atr_1h_val       = _atr(candles_1h[:-1], 14)
    atr_1h_pct       = atr_1h_val / price if price > 0 else 0.0

    # v6.1: distancia dinámica a EMA20_15m según volatilidad del par
    atr_15m_pct = atr_15m / price if price > 0 else PULLBACK_EMA20_DIST_BASE
    pullback_dist = max(PULLBACK_EMA20_DIST_BASE, atr_15m_pct * PULLBACK_ATR_MULT)

    log.debug(
        "[%s] régimen=%s structure=%s | ADX_1h=%.1f ADX_15m=%.1f | "
        "RSI=%.1f MACD_15m=%.5f MACD_1h=%.5f | "
        "vol_ratio=%.2f (last=%.0f avg=%.0f) | "
        "ATR_15m=%.4f%% ATR_1h=%.4f%% | "
        "precio=%.6f EMA200_1h=%.6f EMA20_15m=%.6f pullback_dist=%.2f%%",
        symbol, regime, structure,
        adx_1h, adx_15m,
        rsi, macd_hist, macd_1h,
        vol_ratio, last_vol, avg_vol,
        (atr_15m / price * 100) if price > 0 else 0,
        atr_1h_pct * 100,
        price, ema200_1h, ema20_15m, pullback_dist * 100,
    )

    # ── Hard-guards (mínimos no negociables) ─────────────────────────────
    if price > 0 and atr_15m / price > ATR_VOLATILE_PCT:
        log.debug("[%s] skip: ATR_15m=%.4f%% > %.1f%% (demasiado volátil)", symbol, atr_15m / price * 100, ATR_VOLATILE_PCT * 100)
        return None, 0, None

    if adx_15m < ADX_15M_MIN:
        log.debug("[%s] skip: ADX_15m=%.1f < %d (lateral 15m)", symbol, adx_15m, ADX_15M_MIN)
        return None, 0, None

    if effective_regime == "bull" and price < ema200_1h * (1 - EMA200_MIN_DIST):
        log.debug("[%s] skip: bull pero precio=%.6f < EMA200_1h=%.6f", symbol, price, ema200_1h)
        return None, 0, None
    if effective_regime == "bear" and price > ema200_1h * (1 + EMA200_MIN_DIST):
        log.debug("[%s] skip: bear pero precio=%.6f > EMA200_1h=%.6f", symbol, price, ema200_1h)
        return None, 0, None

    candle_range = c_high - c_low
    if candle_range > 0 and atr_15m > 0:
        if candle_range > NO_CHASE_MULT * atr_15m:
            log.debug("[%s] skip: no-chase rango_vela=%.6f > %.1f*ATR=%.6f", symbol, candle_range, NO_CHASE_MULT, atr_15m)
            return None, 0, None

    if price > 0 and ema20_15m > 0:
        dist_ema20 = abs(price - ema20_15m) / price
        if dist_ema20 > pullback_dist:
            if effective_regime == "bull" and price > ema20_15m:
                log.debug("[%s] skip: sobreextendido sobre EMA20_15m dist=%.2f%% (umbral=%.2f%%)", symbol, dist_ema20 * 100, pullback_dist * 100)
                return None, 0, None
            if effective_regime == "bear" and price < ema20_15m:
                log.debug("[%s] skip: sobreextendido bajo EMA20_15m dist=%.2f%% (umbral=%.2f%%)", symbol, dist_ema20 * 100, pullback_dist * 100)
                return None, 0, None

    # ── Score mínimo dinámico por volatilidad ────────────────────────────
    vol_bump        = _dynamic_min_score_bump(candles_1h, price)
    min_required_base = min_score + vol_bump

    # ── Scoring ───────────────────────────────────────────────────────────
    score = 0

    # Macro 4h — penalización suave (no hard-guard)
    if candles_4h and len(candles_4h) >= 55:
        closes_4h = [c["close"] for c in candles_4h[:-1]]
        ema50_4h  = _ema(closes_4h, 50)[-1]
        price_4h  = closes_4h[-1]
        if effective_regime == "bull" and price_4h < ema50_4h:
            score += W_MACRO_CONTRA
            log.debug("[%s] macro 4h bearish en bull → %d (score=%d)", symbol, W_MACRO_CONTRA, score)
        elif effective_regime == "bear" and price_4h > ema50_4h:
            score += W_MACRO_CONTRA
            log.debug("[%s] macro 4h bullish en bear → %d (score=%d)", symbol, W_MACRO_CONTRA, score)

    # Bear sobreextendido bajo EMA200_15m — penalización suave
    if effective_regime == "bear" and price < ema200_15m * (1 - EMA200_MIN_DIST):
        score += W_BEAR_EMA200_15M
        log.debug("[%s] bear sobreextendido bajo EMA200_15m → %d (score=%d)", symbol, W_BEAR_EMA200_15M, score)

    # Bear con ADX_1h bajo — penalización suave (era hard-guard)
    if effective_regime == "bear" and adx_1h < 22:
        score += W_BEAR_LOW_ADX
        log.debug("[%s] bear + ADX_1h=%.1f < 22 → %d (score=%d)", symbol, adx_1h, W_BEAR_LOW_ADX, score)

    # Vela alineada con régimen
    if effective_regime == "bull" and bullish_candle:
        score += W_VELA
        log.debug("[%s] vela alcista en bull → +%d (score=%d)", symbol, W_VELA, score)
    elif effective_regime == "bear" and not bullish_candle:
        score += W_VELA
        log.debug("[%s] vela bajista en bear → +%d (score=%d)", symbol, W_VELA, score)

    # RSI
    if effective_regime == "bull":
        if 40 <= rsi <= 68:
            score += W_RSI_IDEAL
            log.debug("[%s] RSI=%.1f en zona ideal bull → +%d (score=%d)", symbol, rsi, W_RSI_IDEAL, score)
        elif rsi > 70:
            score += W_RSI_SOBRE
            log.debug("[%s] RSI=%.1f sobrecomprado → %d (score=%d)", symbol, rsi, W_RSI_SOBRE, score)
            if rsi > 80:
                log.debug("[%s] skip: RSI=%.1f > 80 (sobrecomprado extremo)", symbol, rsi)
                return None, score, None
        else:
            log.debug("[%s] RSI=%.1f fuera de zona ideal → +0 (score=%d)", symbol, rsi, score)
    else:
        if 32 <= rsi <= 58:
            score += W_RSI_IDEAL
            log.debug("[%s] RSI=%.1f en zona ideal bear → +%d (score=%d)", symbol, rsi, W_RSI_IDEAL, score)
        elif rsi < 30:
            log.debug("[%s] skip: RSI=%.1f < 30 (sobrevendido en bear)", symbol, rsi)
            return None, score, None
        else:
            log.debug("[%s] RSI=%.1f fuera de zona ideal → +0 (score=%d)", symbol, rsi, score)

    # ADX 1h
    if adx_1h >= 30:
        score += W_ADX_1H_30
        log.debug("[%s] ADX_1h=%.1f >= 30 → +%d (score=%d)", symbol, adx_1h, W_ADX_1H_30, score)
    elif adx_1h >= 25:
        score += W_ADX_1H_25
        log.debug("[%s] ADX_1h=%.1f >= 25 → +%d (score=%d)", symbol, adx_1h, W_ADX_1H_25, score)
    elif adx_1h >= 20:
        score += W_ADX_1H_20
        log.debug("[%s] ADX_1h=%.1f >= 20 → +%d (score=%d)", symbol, adx_1h, W_ADX_1H_20, score)
    else:
        log.debug("[%s] ADX_1h=%.1f < 20 → +0 (score=%d)", symbol, adx_1h, score)

    # MACD 15m
    if effective_regime == "bull" and macd_hist > 0:
        score += W_MACD_15M
        log.debug("[%s] MACD_15m=%.5f positivo en bull → +%d (score=%d)", symbol, macd_hist, W_MACD_15M, score)
    elif effective_regime == "bear" and macd_hist < 0:
        score += W_MACD_15M
        log.debug("[%s] MACD_15m=%.5f negativo en bear → +%d (score=%d)", symbol, macd_hist, W_MACD_15M, score)
    else:
        log.debug("[%s] MACD_15m=%.5f contrario al régimen → +0 (score=%d)", symbol, macd_hist, score)

    # MACD 1h
    if effective_regime == "bull" and macd_1h > 0:
        score += W_MACD_1H
        log.debug("[%s] MACD_1h=%.5f positivo en bull → +%d (score=%d)", symbol, macd_1h, W_MACD_1H, score)
    elif effective_regime == "bear" and macd_1h < 0:
        score += W_MACD_1H
        log.debug("[%s] MACD_1h=%.5f negativo en bear → +%d (score=%d)", symbol, macd_1h, W_MACD_1H, score)
    else:
        log.debug("[%s] MACD_1h=%.5f contrario al régimen → +0 (score=%d)", symbol, macd_1h, score)

    # Volumen
    if avg_vol > 0:
        if last_vol >= avg_vol * VOLUME_MULT:
            score += W_VOLUME_HIGH
            log.debug("[%s] vol_ratio=%.2f >= %.1f → +%d (score=%d)", symbol, vol_ratio, VOLUME_MULT, W_VOLUME_HIGH, score)
        elif last_vol < avg_vol * VOLUME_WEAK:
            score += W_VOLUME_LOW
            log.debug("[%s] vol_ratio=%.2f < %.1f → %d (score=%d)", symbol, vol_ratio, VOLUME_WEAK, W_VOLUME_LOW, score)
        else:
            log.debug("[%s] vol_ratio=%.2f normal → +0 (score=%d)", symbol, vol_ratio, score)

    # Divergencia RSI (bonus raro pero fiable)
    div = _rsi_divergence(closes_15m[:-1], candles_15m[:-1])
    if effective_regime == "bull" and div == "bullish":
        score += W_DIVERGENCIA
        log.debug("[%s] divergencia RSI bullish → +%d (score=%d)", symbol, W_DIVERGENCIA, score)
    elif effective_regime == "bear" and div == "bearish":
        score += W_DIVERGENCIA
        log.debug("[%s] divergencia RSI bearish → +%d (score=%d)", symbol, W_DIVERGENCIA, score)

    # Estructura
    if structure == effective_regime:
        score += W_STRUCTURE
        log.debug("[%s] structure=%s == régimen → +%d (score=%d)", symbol, structure, W_STRUCTURE, score)
    elif structure != "range" and structure != effective_regime:
        score += W_STRUCTURE_CONTRA
        log.debug(
            "[%s] structure=%s contradice régimen=%s → %d (score=%d)",
            symbol, structure, regime, W_STRUCTURE_CONTRA, score,
        )
    # structure==range: sin penalización (era hard-guard en v5, ahora neutro)

    # ── Score mínimo ──────────────────────────────────────────────────────
    min_required = min_required_base + (SHORT_MIN_SCORE_EXTRA if effective_regime == "bear" else 0)
    log.info(
        "[%s] SCORE=%d min=%d | régimen=%s adx1h=%.1f adx15m=%.1f rsi=%.1f vol=%.2f",
        symbol, score, min_required, regime, adx_1h, adx_15m, rsi, vol_ratio,
    )

    if score < min_required:
        return None, score, None

    side = "long" if effective_regime == "bull" else "short"
    log.info(
        "✅ SEÑAL %s | score=%d (min=%d) | regime=%s structure=%s "
        "adx1h=%.1f adx15m=%.1f rsi=%.1f macd=%.5f vol=%.2f%s",
        side.upper(), score, min_required, regime, structure,
        adx_1h, adx_15m, rsi, macd_hist, vol_ratio,
        " [PROTO]" if is_proto else "",
    )
    return side, score, regime
