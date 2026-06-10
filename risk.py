"""risk.py — Gestión de riesgo con sizing proporcional al score y trailing stop.

Sizing:
  base_margin = MARGIN_USDT
  score  55-69  →  0.7× base_margin
  score  70-84  →  1.0× base_margin
  score  85-100 →  1.4× base_margin

SL / TP:
  El ATR de las velas 15m se convierte a porcentaje del precio de entrada
  y se clampea entre SL_MIN_PCT y SL_MAX_PCT para evitar valores
  desorbitados en coins de bajo precio o alta volatilidad.

  sl_pct  = clamp(ATR/entry × 1.5, SL_MIN_PCT, SL_MAX_PCT)
  tp_pct  = sl_pct × TP_RR           (ratio riesgo:beneficio fijo)

  Valores por defecto:
    SL_MIN_PCT = 0.4%   — mínimo SL (evita stops demasiado ajustados)
    SL_MAX_PCT = 2.5%   — máximo SL (evita stops absurdamente amplios)
    TP_RR      = 2.0    — ratio riesgo:beneficio (TP = 2×SL)

Trailing stop:
  trail_step = 0.3 × sl_dist  (el SL se mueve cuando el precio avanza 0.3×SL)
"""
import logging
import config
import exchange as _exchange

log = logging.getLogger("risk")

# ── Límites porcentuales SL/TP ────────────────────────────────────────────────
SL_MIN_PCT = 0.004   # 0.4%  — stop mínimo
SL_MAX_PCT = 0.025   # 2.5%  — stop máximo
TP_RR      = 2.0     # ratio riesgo:beneficio  (TP = TP_RR × SL)


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

    # ATR como porcentaje del precio de entrada
    if atr > 0 and entry > 0:
        atr_pct = atr / entry      # e.g. 0.0025 = 0.25%
    else:
        log.warning("ATR=0 o entry=0, usando fallback SL_PCT")
        atr_pct = config.SL_PCT / 100 / 1.5   # deshacer el ×1.5 que se aplica abajo

    # SL como porcentaje, clampeado entre mínimo y máximo
    raw_sl_pct = atr_pct * 1.5
    sl_pct     = max(SL_MIN_PCT, min(SL_MAX_PCT, raw_sl_pct))
    tp_pct     = sl_pct * TP_RR

    sl_dist = entry * sl_pct
    tp_dist = entry * tp_pct

    sl = (entry - sl_dist) if side == "long" else (entry + sl_dist)
    tp = (entry + tp_dist) if side == "long" else (entry - tp_dist)

    # Sizing proporcional al score
    mult      = _size_multiplier(score)
    margin    = config.MARGIN_USDT * mult
    raw_qty   = (margin * config.LEVERAGE) / entry

    # Redondear al step-size real del contrato
    if symbol:
        try:
            info = _exchange._get_contract_info(symbol)
            step = info["stepSize"]
            qty  = _exchange.floor_qty(raw_qty, step)
        except Exception as exc:
            log.warning("No se pudo obtener step-size para %s: %s — usando 3 dec", symbol, exc)
            import math
            qty = math.floor(raw_qty * 1000) / 1000
    else:
        import math
        qty = math.floor(raw_qty * 1000) / 1000

    # Trail step: el SL se mueve cuando el precio avanza 0.3 × sl_dist
    trail_step = round(0.3 * sl_dist, 8)

    log.info(
        "[%s] score=%d mult=%.1f margin=%.2f "
        "ATR=%.6f atr_pct=%.3f%% raw_sl=%.3f%% → sl_pct=%.3f%% tp_pct=%.3f%% "
        "SL=%.6f TP=%.6f raw_qty=%.8f qty=%.8f trail=%.8f",
        side.upper(), score, mult, margin,
        atr, atr_pct * 100, raw_sl_pct * 100, sl_pct * 100, tp_pct * 100,
        sl, tp, raw_qty, qty, trail_step,
    )

    return {
        "qty":        qty,
        "sl":         round(sl, 8),
        "tp":         round(tp, 8),
        "atr":        round(atr, 8),
        "trail_step": trail_step,
        "score":      score,
    }
