"""risk.py — Gestión de riesgo con sizing proporcional al score y trailing stop.

Sizing:
  base_margin = MARGIN_USDT
  score  70-84  →  0.6× base_margin  (señal válida pero no probada — tamaño reducido)
  score  ≥ 85   →  1.0× base_margin  (señal de alta calidad — tamaño completo)
  (score < 70 nunca llega aquí — bloqueado en signals.py por MIN_SCORE)

SL / TP:
  El ATR de las velas 15m se convierte a porcentaje del precio de entrada
  y se clampea entre SL_MIN_PCT y SL_MAX_PCT.

  sl_pct  = clamp(ATR/entry × 1.5, SL_MIN_PCT, SL_MAX_PCT)
  tp_pct  = sl_pct × _tp_rr(score, regime)

  TP_RR dinámico:
    regime="range"  →  2.0
    score ≥ 85      →  2.5
    score ≥ 70      →  2.0
    score < 70      →  1.8

  Valores fijos:
    SL_MIN_PCT = 0.008   — mínimo SL (0.8%) — evita saltar por ruido puro
    SL_MAX_PCT = 0.020   — máximo SL (2.0%) — cap conservador, protege cuenta

Trailing stop:
  trail_step = 0.5 × sl_dist

CAMBIOS v10:
  - Sizing INVERTIDO: score 70-84 → 0.6× (antes 1.0×), score ≥85 → 1.0× (antes 1.4×).
    El multiplicador 1.4× en señales de score 70 era la causa principal de pérdidas
    grandes — el bot apostaba el máximo en señales que apenas superaban el umbral.
  - SL_MAX_PCT bajado de 3.5% a 2.0%. Un SL de 3.5% con apalancamiento alto
    significa pérdidas enormes antes de salir. 2% es el máximo tolerable.
  - SL_MIN_PCT subido de 0.6% a 0.8%. SL de 0.6% salta por ruido de mercado normal.
"""
import logging
import math

import config
import exchange as _exchange
import indicators as ind

log = logging.getLogger("risk")

SL_MIN_PCT = 0.008   # 0.8% mínimo — evita SL que salta por ruido
SL_MAX_PCT = 0.020   # 2.0% máximo — cap conservador


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
    """Sizing conservador: máximo solo para señales de alta calidad (≥85)."""
    if score >= 85:
        return 1.0   # tamaño completo solo para señales premium
    return 0.6       # señales válidas 70-84 → tamaño reducido


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
                  dinámico. Recibido desde signals.evaluate().

    Raises:
        ValueError: si qty calculada es 0 o inferior al minQty del contrato.
    """
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
