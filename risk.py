"""risk.py — Gestión de riesgo con sizing proporcional al score y trailing stop.

Sizing:
  base_margin = MARGIN_USDT
  score  55-69  →  0.7× base_margin
  score  70-84  →  1.0× base_margin
  score  85-100 →  1.4× base_margin

Trailing stop:
  Se gestiona en main.py — aquí se calcula el step inicial.
  trail_step = 0.5 × ATR  (se mueve el SL cada vez que el precio avanza 0.5 ATR)
"""
import math
import logging
import config

log = logging.getLogger("risk")


def _atr(candles: list[dict], period: int = 14) -> float:
    trs = []
    for i in range(1, len(candles)):
        h, l, pc = candles[i]["high"], candles[i]["low"], candles[i - 1]["close"]
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    return sum(trs[-period:]) / min(period, len(trs)) if trs else 0.0


def _size_multiplier(score: int) -> float:
    if score >= 85:
        return 1.4
    if score >= 70:
        return 1.0
    return 0.7   # score 55-69


def calc(side: str, entry: float, candles: list[dict], score: int = 70) -> dict:
    atr = _atr(candles, period=14)

    if atr <= 0:
        log.warning("ATR=0, usando fallback porcentual")
        atr = entry * (config.SL_PCT / 100) / 1.5

    sl_dist = 1.5 * atr
    tp_dist = 3.0 * atr   # ratio 1:2

    sl = (entry - sl_dist) if side == "long" else (entry + sl_dist)
    tp = (entry + tp_dist) if side == "long" else (entry - tp_dist)

    # Sizing proporcional al score
    mult   = _size_multiplier(score)
    margin = config.MARGIN_USDT * mult
    raw_qty = (margin * config.LEVERAGE) / entry
    qty = math.floor(raw_qty * 1000) / 1000

    # Trail step: SL se mueve cuando precio avanza 0.5 ATR
    trail_step = round(0.5 * atr, 6)

    log.info(
        "[%s] score=%d mult=%.1f margin=%.1f ATR=%.4f SL=%.4f TP=%.4f qty=%.4f trail=%.4f",
        side.upper(), score, mult, margin, atr, sl, tp, qty, trail_step,
    )

    return {
        "qty":        qty,
        "sl":         round(sl, 6),
        "tp":         round(tp, 6),
        "atr":        round(atr, 6),
        "trail_step": trail_step,
        "score":      score,
    }
