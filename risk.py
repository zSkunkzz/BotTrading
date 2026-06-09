"""risk.py — SL/TP con ATR. Margin fijo por trade.

qty = (MARGIN_USDT × LEVERAGE) / precio
SL  = 1.5 × ATR
TP  = 3.0 × ATR  (ratio 1:2)
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


def calc(side: str, entry: float, candles: list[dict]) -> dict:
    atr = _atr(candles, period=14)

    if atr <= 0:
        log.warning("ATR=0, usando fallback porcentual")
        atr = entry * (config.SL_PCT / 100) / 1.5

    sl_dist = 1.5 * atr
    tp_dist = 3.0 * atr

    sl = (entry - sl_dist) if side == "long" else (entry + sl_dist)
    tp = (entry + tp_dist) if side == "long" else (entry - tp_dist)

    # Margin fijo: MARGIN_USDT define el capital expuesto
    raw_qty = (config.MARGIN_USDT * config.LEVERAGE) / entry
    qty = math.floor(raw_qty * 1000) / 1000

    log.info("[%s] ATR=%.4f SL=%.4f TP=%.4f qty=%.4f (margin=%.0f USDT)",
             side.upper(), atr, sl, tp, qty, config.MARGIN_USDT)

    return {"qty": qty, "sl": round(sl, 4), "tp": round(tp, 4), "atr": round(atr, 6)}
