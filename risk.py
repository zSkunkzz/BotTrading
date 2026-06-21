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
  tp_pct  = sl_pct × _tp_rr(score, regime)

  TP_RR dinámico:
    regime="range"  →  2.0  (margen suficiente para cubrir spread + comisión)
    score ≥ 85      →  2.5  (señal fuerte + tendencia, dejar correr)
    score ≥ 70      →  2.0  (normal)
    score < 70      →  1.8  (señal débil, mínimo viable con comisiones)

  Valores fijos:
    SL_MIN_PCT = 0.006   — mínimo SL (0.6%)
    SL_MAX_PCT = 3.5%    — máximo SL (ampliado: crypto necesita respirar)

Trailing stop:
  trail_step = 0.5 × sl_dist  (era 0.3 — demasiado sensible al ruido)

FIX v5: _atr importado de indicators.py (compartido con signals.py).
FIX: qty=0 guard — si floor_qty devuelve 0, se lanza ValueError.
FIX: step/min_qty inicializados antes del bloque try/except.
FIX: SL/TP redondeados con pricePrecision real del contrato en lugar de 8
     decimales fijos (evita truncado silencioso por BingX).
FIX: SL_MAX_PCT subido a 3.5% — el cap anterior (2.5%) provocaba SL
     artificialmente ajustados que saltaban por ruido normal de mercado.
FIX: trail_step subido a 0.5×sl_dist — el 0.3 anterior activaba el trailing
     en rangos normales convirtiendo posiciones ganadoras en cierres prematuros.
FIX: TP_RR de regime="range" subido a 2.0 — el 1.5 anterior dejaba el TP
     tan cerca que spread + comisión lo anulaban.
FIX: TP_RR mínimo (score<70) subido a 1.8 — el 1.7 no cubría comisiones
     en señales débiles con SL ajustado.
"""
import logging
import math

import config
import exchange as _exchange
import indicators as ind

log = logging.getLogger("risk")

SL_MIN_PCT = 0.006
SL_MAX_PCT = 0.035


def _tp_rr(score: int, regime: str) -> float:
    """TP_RR dinámico: ajusta el ratio riesgo:beneficio según régimen y score."""
    if regime == "range":
        return 2.0
    if score >= 85:
        return 2.5
    if score >= 70:
        return 2.0
    return 1.8


def _size_multiplier(score: int) -> float:
    if score >= 85:
        return 1.4
    if score >= 70:
        return 1.0
    return 0.7


def calc(
    side: str,
    entry: float,
    candles: list[dict],
    score: int = 70,
    symbol: str | None = None,
    regime: str = "bull",
) -> dict:
    """
    Calcula SL, TP, qty y trail_step para una entrada.

    Args:
        side    : 'long' | 'short'
        entry   : precio de entrada
        candles : velas 15m (para ATR)
        score   : score de la señal (0-100)
        symbol  : símbolo BingX (e.g. 'BTC-USDT'). Si se pasa, se consulta
                  el step-size real del contrato para redondear qty.
        regime  : régimen de mercado ('bull' | 'bear' | 'range') para TP_RR
                  dinámico. Ahora recibido desde signals.evaluate().

    Raises:
        ValueError: si qty calculada es 0 o inferior al minQty del contrato.
    """
    # FIX v5: usar indicators.atr (compartido con signals.py)
    atr = ind.atr(candles, period=14)

    if atr > 0 and entry > 0:
        atr_pct = atr / entry
    else:
        log.warning("ATR=0 o entry=0, usando fallback SL_PCT")
        atr_pct = config.SL_PCT / 100 / 1.5

    raw_sl_pct = atr_pct * 1.5
    sl_pct     = max(SL_MIN_PCT, min(SL_MAX_PCT, raw_sl_pct))

    rr     = _tp_rr(score, regime)
    tp_pct = sl_pct * rr

    sl_dist = entry * sl_pct
    tp_dist = entry * tp_pct

    sl = (entry - sl_dist) if side == "long" else (entry + sl_dist)
    tp = (entry + tp_dist) if side == "long" else (entry - tp_dist)

    mult    = _size_multiplier(score)
    margin  = config.MARGIN_USDT * mult
    raw_qty = (margin * config.LEVERAGE) / entry

    step       = 0.001
    min_qty    = 0.001
    price_prec = 8

    if symbol:
        try:
            info       = _exchange._get_contract_info(symbol)
            step       = info["stepSize"]
            min_qty    = info["minQty"]
            price_prec = info["pricePrecision"]
            qty        = _exchange.floor_qty(raw_qty, step)
        except Exception as exc:
            log.warning("No se pudo obtener step-size para %s: %s — usando 3 dec", symbol, exc)
            qty = math.floor(raw_qty * 1000) / 1000
    else:
        qty = math.floor(raw_qty * 1000) / 1000

    if qty <= 0 or qty < min_qty:
        raise ValueError(
            f"[{symbol}] qty calculada ({qty:.8f}) es 0 o inferior al minQty ({min_qty}) "
            f"— margin={margin:.2f} USDT, price={entry:.6f}, step={step:.8f}. "
            "Aumenta MARGIN_USDT o reduce el apalancamiento."
        )

    sl = round(sl, price_prec)
    tp = round(tp, price_prec)
    trail_step = round(0.5 * sl_dist, price_prec)

    log.info(
        "[%s] score=%d regime=%s RR=%.1f mult=%.1f margin=%.2f "
        "ATR=%.6f atr_pct=%.3f%% sl_pct=%.3f%% tp_pct=%.3f%% "
        "SL=%.6f TP=%.6f qty=%.8f trail=%.8f",
        side.upper(), score, regime, rr, mult, margin,
        atr, atr_pct * 100, sl_pct * 100, tp_pct * 100,
        sl, tp, qty, trail_step,
    )

    return {
        "qty":        qty,
        "sl":         sl,
        "tp":         tp,
        "atr":        round(atr, 8),
        "trail_step": trail_step,
        "score":      score,
        "tp_rr":      rr,
    }
