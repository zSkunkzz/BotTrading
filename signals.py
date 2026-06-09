"""signals.py — Lógica de señales con 5 filtros de calidad.

Filtros (TODOS deben cumplirse):
  1. Vela cerrada     : se evalúa solo la penúltima vela (ya cerrada), no la viva
  2. Tendencia 1h     : precio > EMA200 → solo LONG | precio < EMA200 → solo SHORT
                        + distancia mínima 0.3% al EMA200
  3. ADX > 25         : mercado en tendencia, no lateral
  4. RSI 15m          : cruce del nivel 50 en vela cerrada
  5. MACD 15m + 1h    : histograma confirma dirección en ambos timeframes
  6. Filtro no-chase  : la vela de entrada no supera 2×ATR de rango
"""
from __future__ import annotations
import logging

log = logging.getLogger("signals")

EMA200_MIN_DIST = 0.003   # 0.3% distancia mínima al EMA200
ADX_THRESHOLD   = 25      # mercado en tendencia si ADX > este valor
NO_CHASE_MULT   = 2.0     # rango de la vela de entrada ≤ 2×ATR


# ── Indicadores ────────────────────────────────────────────────────────────────

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
    """ADX simplificado (Wilder smoothing)."""
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

    atr_s  = _smooth(tr_list)
    pdm_s  = _smooth(plus_dm)
    mdm_s  = _smooth(minus_dm)

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


# ── Función principal ──────────────────────────────────────────────────────────

def evaluate(candles_15m: list[dict], candles_1h: list[dict]) -> str | None:
    """
    candles_15m / candles_1h : [{open, high, low, close, volume}] más antigua primero.
    IMPORTANTE: se usa la penúltima vela ([-2]) — la última está viva.
    Devuelve 'long', 'short' o None.
    """
    if len(candles_1h) < 210:
        log.warning("Pocas velas 1h (%d/210)", len(candles_1h))
        return None
    if len(candles_15m) < 60:
        log.warning("Pocas velas 15m (%d/60)", len(candles_15m))
        return None

    # Usamos velas cerradas: todo[-2] en vez de [-1]
    closed_15m = candles_15m[:-1]   # excluimos la vela viva
    closed_1h  = candles_1h[:-1]

    closes_15m = [c["close"] for c in closed_15m]
    closes_1h  = [c["close"] for c in closed_1h]

    # ── 1. Vela cerrada ya garantizada (closed_15m[-1] es la última cerrada) ──

    # ── 2. Tendencia 1h: EMA200 + distancia mínima ───────────────────────
    ema200 = _ema(closes_1h, 200)[-1]
    price  = closes_15m[-1]
    dist   = abs(price - ema200) / ema200

    if dist < EMA200_MIN_DIST:
        log.info("⚠️  Precio demasiado cerca del EMA200 (dist=%.4f%%) — sin señal", dist * 100)
        return None

    trend_long  = price > ema200
    trend_short = price < ema200

    # ── 3. ADX > 25 ─────────────────────────────────────────────────────────
    adx = _adx(closed_15m, period=14)
    if adx < ADX_THRESHOLD:
        log.info("⚠️  ADX=%.1f < %d — mercado lateral, sin señal", adx, ADX_THRESHOLD)
        return None

    # ── 4. RSI 15m: cruce del nivel 50 ───────────────────────────────────────
    rsi_series   = _rsi(closes_15m, 14)
    rsi_prev     = rsi_series[-2]
    rsi_curr     = rsi_series[-1]
    rsi_cross_up   = rsi_prev < 50 <= rsi_curr
    rsi_cross_down = rsi_prev > 50 >= rsi_curr

    # ── 5. MACD histograma en 15m Y 1h confirman dirección ──────────────────
    hist_15m = _macd_histogram(closes_15m)
    hist_1h  = _macd_histogram(closes_1h)

    macd15_bull = hist_15m[-1] > 0 and hist_15m[-1] > hist_15m[-2]
    macd15_bear = hist_15m[-1] < 0 and hist_15m[-1] < hist_15m[-2]
    macd1h_bull = hist_1h[-1] > 0
    macd1h_bear = hist_1h[-1] < 0

    # ── 6. Filtro no-chase: rango de la vela ≤ 2×ATR ──────────────────────
    atr       = _atr(closed_15m, 14)
    last_range = closed_15m[-1]["high"] - closed_15m[-1]["low"]
    chasing    = atr > 0 and last_range > NO_CHASE_MULT * atr

    if chasing:
        log.info("⚠️  Vela explosiva (rango=%.4f > 2×ATR=%.4f) — sin señal", last_range, atr)
        return None

    log.info(
        "price=%.4f ema200=%.4f dist=%.2f%% ADX=%.1f | "
        "rsi=%.1f→%.1f | hist15m=%.6f→%.6f | hist1h=%.6f",
        price, ema200, dist * 100, adx,
        rsi_prev, rsi_curr,
        hist_15m[-2], hist_15m[-1], hist_1h[-1],
    )

    # ── Decisión ───────────────────────────────────────────────────────────────
    if trend_long and rsi_cross_up and macd15_bull and macd1h_bull:
        log.info("✅ LONG — todos los filtros OK")
        return "long"

    if trend_short and rsi_cross_down and macd15_bear and macd1h_bear:
        log.info("✅ SHORT — todos los filtros OK")
        return "short"

    log.info("⬛ Sin señal")
    return None
