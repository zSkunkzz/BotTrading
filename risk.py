"""risk.py — Calcula qty, SL y TP fijos a partir del precio de entrada."""
import math
import config


def calc(side: str, entry: float) -> dict:
    """
    Devuelve {qty, sl, tp} dados:
      - side:  'long' | 'short'
      - entry: precio de entrada
    """
    sl_dist = entry * (config.SL_PCT / 100)
    tp_dist = entry * (config.TP_PCT / 100)

    if side == "long":
        sl = entry - sl_dist
        tp = entry + tp_dist
    else:
        sl = entry + sl_dist
        tp = entry - tp_dist

    # qty = (capital * leverage) / precio  → en contratos base
    raw_qty = (config.USDC_SIZE * config.LEVERAGE) / entry
    qty = math.floor(raw_qty * 1000) / 1000  # 3 decimales, redondeado a la baja

    return {"qty": qty, "sl": round(sl, 4), "tp": round(tp, 4)}
