"""risk.py — SL y TP calculados con ATR. SL = 1.5×ATR, TP = 3×ATR (ratio 1:2)."""
import math
import logging
import config

log = logging.getLogger("risk")


def _atr(candles: list[dict], period: int = 14) -> float:
    """ATR simple (media de True Range de las últimas `period` velas)."""
    trs = []
    for i in range(1, len(candles)):
        high  = candles[i]["high"]
        low   = candles[i]["low"]
        prev_close = candles[i - 1]["close"]
        tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
        trs.append(tr)
    if not trs:
        return 0.0
    return sum(trs[-period:]) / min(period, len(trs))


def calc(side: str, entry: float, candles: list[dict]) -> dict:
    """
    Devuelve {qty, sl, tp, atr}.
      side    : 'long' | 'short'
      entry   : precio de entrada
      candles : velas 15m para calcular ATR
    """
    atr = _atr(candles, period=14)

    if atr <= 0:
        # Fallback a porcentaje fijo si ATR falla
        log.warning("ATR=0, usando SL/TP por porcentaje fijo")
        atr = entry * (config.SL_PCT / 100) / 1.5

    sl_dist = 1.5 * atr
    tp_dist = 3.0 * atr

    if side == "long":
        sl = entry - sl_dist
        tp = entry + tp_dist
    else:
        sl = entry + sl_dist
        tp = entry - tp_dist

    # qty = (capital × leverage) / precio
    raw_qty = (config.USDC_SIZE * config.LEVERAGE) / entry
    qty = math.floor(raw_qty * 1000) / 1000

    log.info(
        "ATR=%.4f | SL=%.4f (1.5×ATR) | TP=%.4f (3×ATR) | qty=%.4f",
        atr, sl, tp, qty,
    )

    return {"qty": qty, "sl": round(sl, 4), "tp": round(tp, 4), "atr": round(atr, 6)}
