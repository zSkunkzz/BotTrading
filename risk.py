"""risk.py — Gestión de riesgo con sizing proporcional al score y trailing stop.

Sizing:
  base_margin = MARGIN_USDT
  score  55-69  →  0.7× base_margin
  score  70-84  →  1.0× base_margin
  score  85-100 →  1.4× base_margin

El qty resultante se redondea al step-size REAL del contrato usando
exchange.floor_qty() en lugar del hardcode floor(x*1000)/1000 que
asumía siempre 3 decimales y fallaba en altcoins de precio extremo.

Trailing stop:
  trail_step = 0.5 × ATR (el SL se mueve cuando el precio avanza 0.5 ATR)
"""
import logging
import config
import exchange as _exchange

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


def calc(side: str, entry: float, candles: list[dict], score: int = 70,
         symbol: str | None = None) -> dict:
    """
    Calcula SL, TP, qty y trail_step para una entrada.

    Args:
        side    : 'long' | 'short'
        entry   : precio de entrada
        candles : velas 15m (para ATR)
        score   : score de la señal (0-100)
        symbol  : símbolo BingX (e.g. 'BTC-USDT'). Si se pasa, se consulta
                  el step-size real del contrato para redondear qty.
    """
    atr = _atr(candles, period=14)

    if atr <= 0:
        log.warning("ATR=0, usando fallback porcentual")
        atr = entry * (config.SL_PCT / 100) / 1.5

    sl_dist = 1.5 * atr
    tp_dist = 3.0 * atr   # ratio 1:2

    sl = (entry - sl_dist) if side == "long" else (entry + sl_dist)
    tp = (entry + tp_dist) if side == "long" else (entry - tp_dist)

    # Sizing proporcional al score
    mult      = _size_multiplier(score)
    margin    = config.MARGIN_USDT * mult
    raw_qty   = (margin * config.LEVERAGE) / entry

    # Redondear al step-size real del contrato (no hardcode 3 decimales)
    if symbol:
        try:
            info     = _exchange._get_contract_info(symbol)
            step     = info["stepSize"]
            qty      = _exchange.floor_qty(raw_qty, step)
        except Exception as exc:
            log.warning("No se pudo obtener step-size para %s: %s — usando 3 dec", symbol, exc)
            import math
            qty = math.floor(raw_qty * 1000) / 1000
    else:
        # Fallback cuando no se pasa símbolo
        import math
        qty = math.floor(raw_qty * 1000) / 1000

    # Trail step: SL se mueve cuando precio avanza 0.5 ATR
    trail_step = round(0.5 * atr, 8)

    log.info(
        "[%s] score=%d mult=%.1f margin=%.2f ATR=%.6f SL=%.6f TP=%.6f "
        "raw_qty=%.8f qty=%.8f trail=%.8f",
        side.upper(), score, mult, margin, atr, sl, tp,
        raw_qty, qty, trail_step,
    )

    return {
        "qty":        qty,
        "sl":         round(sl, 8),
        "tp":         round(tp, 8),
        "atr":        round(atr, 8),
        "trail_step": trail_step,
        "score":      score,
    }
