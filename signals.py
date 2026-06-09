"""signals.py — Tus indicadores. Devuelve 'long', 'short' o None.

PON AQUÍ TU LÓGICA. El ejemplo usa EMA cruce + RSI como placeholder.
"""
from __future__ import annotations
import logging

log = logging.getLogger("signals")


# ── Helpers de indicadores ────────────────────────────────────────────────────

def _ema(closes: list[float], period: int) -> list[float]:
    k = 2 / (period + 1)
    emas = [closes[0]]
    for c in closes[1:]:
        emas.append(c * k + emas[-1] * (1 - k))
    return emas


def _rsi(closes: list[float], period: int = 14) -> float:
    deltas = [closes[i] - closes[i - 1] for i in range(1, len(closes))]
    gains  = [d for d in deltas if d > 0]
    losses = [-d for d in deltas if d < 0]
    if not gains or not losses:
        return 50.0
    avg_gain = sum(gains[-period:]) / period
    avg_loss = sum(losses[-period:]) / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


# ── Función principal ─────────────────────────────────────────────────────────

def evaluate(candles: list[dict]) -> str | None:
    """
    Recibe lista de velas [{open, high, low, close, volume}].
    Devuelve 'long', 'short' o None.

    REEMPLAZA ESTE CUERPO CON TU LÓGICA.
    """
    if len(candles) < 50:
        log.warning("Pocas velas (%d) para calcular señal", len(candles))
        return None

    closes = [c["close"] for c in candles]

    ema_fast = _ema(closes, 9)
    ema_slow = _ema(closes, 21)
    rsi      = _rsi(closes, 14)

    prev_fast, curr_fast = ema_fast[-2], ema_fast[-1]
    prev_slow, curr_slow = ema_slow[-2], ema_slow[-1]

    cross_up   = prev_fast <= prev_slow and curr_fast > curr_slow
    cross_down = prev_fast >= prev_slow and curr_fast < curr_slow

    log.debug(
        "EMA9=%.4f EMA21=%.4f RSI=%.1f | cross_up=%s cross_down=%s",
        curr_fast, curr_slow, rsi, cross_up, cross_down,
    )

    if cross_up and rsi < 70:
        return "long"
    if cross_down and rsi > 30:
        return "short"

    return None
