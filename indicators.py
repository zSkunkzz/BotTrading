"""indicators.py — Indicadores técnicos compartidos.

Módulo centralizado para evitar duplicación entre signals.py y risk.py.
Cualquier fix en _atr, _ema, etc. se propaga automáticamente a todos
los consumidores.
"""
from __future__ import annotations


def ema(closes: list[float], period: int) -> list[float]:
    """EMA con guard de lista vacía."""
    if not closes:
        return []
    k = 2 / (period + 1)
    emas = [closes[0]]
    for c in closes[1:]:
        emas.append(c * k + emas[-1] * (1 - k))
    return emas


def atr(candles: list[dict], period: int = 14) -> float:
    """Average True Range."""
    trs = []
    for i in range(1, len(candles)):
        h, l, pc = candles[i]["high"], candles[i]["low"], candles[i - 1]["close"]
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    return sum(trs[-period:]) / min(period, len(trs)) if trs else 0.0
