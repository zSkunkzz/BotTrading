"""signals.py — Lógica de señales.

Filtros (todos deben cumplirse para entrar):
  1. Tendencia 1h : precio > EMA200 → solo LONG | precio < EMA200 → solo SHORT
  2. RSI 15m      : cruce del nivel 50 (sube → LONG, baja → SHORT)
  3. MACD 15m     : histograma positivo y creciendo (LONG) o negativo y cayendo (SHORT)
"""
from __future__ import annotations
import logging

log = logging.getLogger("signals")


# ── Indicadores ────────────────────────────────────────────────────────────────

def _ema(closes: list[float], period: int) -> list[float]:
    k = 2 / (period + 1)
    emas = [closes[0]]
    for c in closes[1:]:
        emas.append(c * k + emas[-1] * (1 - k))
    return emas


def _rsi(closes: list[float], period: int = 14) -> list[float]:
    """Devuelve serie completa de RSI (mismo largo que closes)."""
    rsi = [50.0] * len(closes)
    if len(closes) < period + 1:
        return rsi
    deltas = [closes[i] - closes[i - 1] for i in range(1, len(closes))]
    avg_gain = sum(d for d in deltas[:period] if d > 0) / period
    avg_loss = sum(-d for d in deltas[:period] if d < 0) / period
    for i in range(period, len(closes)):
        delta = deltas[i - 1]
        gain  = max(delta, 0)
        loss  = max(-delta, 0)
        avg_gain = (avg_gain * (period - 1) + gain) / period
        avg_loss = (avg_loss * (period - 1) + loss) / period
        if avg_loss == 0:
            rsi[i] = 100.0
        else:
            rs = avg_gain / avg_loss
            rsi[i] = 100 - (100 / (1 + rs))
    return rsi


def _macd(closes: list[float], fast: int = 12, slow: int = 26, signal: int = 9):
    """Devuelve (macd_line, signal_line, histogram) — listas del mismo largo."""
    ema_fast   = _ema(closes, fast)
    ema_slow   = _ema(closes, slow)
    macd_line  = [f - s for f, s in zip(ema_fast, ema_slow)]
    signal_line = _ema(macd_line, signal)
    histogram  = [m - s for m, s in zip(macd_line, signal_line)]
    return macd_line, signal_line, histogram


# ── Función principal ──────────────────────────────────────────────────────────

def evaluate(candles_15m: list[dict], candles_1h: list[dict]) -> str | None:
    """
    candles_15m / candles_1h : lista de velas [{open, high, low, close, volume}]
                               ordenadas de más antigua a más reciente.
    Devuelve 'long', 'short' o None.
    """
    # Mínimo de velas necesarias
    if len(candles_1h) < 200:
        log.warning("Pocas velas 1h (%d/200)", len(candles_1h))
        return None
    if len(candles_15m) < 35:
        log.warning("Pocas velas 15m (%d/35)", len(candles_15m))
        return None

    closes_1h  = [c["close"] for c in candles_1h]
    closes_15m = [c["close"] for c in candles_15m]

    # ── 1. Tendencia 1h: precio vs EMA200 ─────────────────────────────────────
    ema200     = _ema(closes_1h, 200)[-1]
    price      = closes_15m[-1]
    trend_long  = price > ema200
    trend_short = price < ema200

    # ── 2. RSI 15m: cruce del nivel 50 ────────────────────────────────────────
    rsi_series  = _rsi(closes_15m, 14)
    rsi_prev    = rsi_series[-2]
    rsi_curr    = rsi_series[-1]
    rsi_cross_up   = rsi_prev < 50 and rsi_curr >= 50   # cruzó 50 hacia arriba
    rsi_cross_down = rsi_prev > 50 and rsi_curr <= 50   # cruzó 50 hacia abajo

    # ── 3. MACD 15m: histograma ────────────────────────────────────────────────
    _, _, histogram = _macd(closes_15m)
    hist_prev = histogram[-2]
    hist_curr = histogram[-1]
    macd_bull = hist_curr > 0 and hist_curr > hist_prev   # positivo y creciendo
    macd_bear = hist_curr < 0 and hist_curr < hist_prev   # negativo y cayendo

    log.info(
        "price=%.4f ema200_1h=%.4f | rsi=%.1f (prev=%.1f) | hist=%.6f (prev=%.6f)",
        price, ema200, rsi_curr, rsi_prev, hist_curr, hist_prev,
    )

    # ── Decisión ───────────────────────────────────────────────────────────────
    if trend_long and rsi_cross_up and macd_bull:
        log.info("✅ LONG — tendencia OK, RSI cruzó 50↑, MACD histograma ↑")
        return "long"

    if trend_short and rsi_cross_down and macd_bear:
        log.info("✅ SHORT — tendencia OK, RSI cruzó 50↓, MACD histograma ↓")
        return "short"

    log.info("⬛ Sin señal")
    return None
