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
    regime="range"  →  1.5  (precio no viaja lejos, TP más conservador)
    score ≥ 85      →  2.5  (señal fuerte + tendencia, dejar correr)
    score ≥ 70      →  2.0  (normal)
    score < 70      →  1.7  (señal débil, asegurar beneficio antes)

  Valores fijos:
    SL_MIN_PCT = 0.4%   — mínimo SL
    SL_MAX_PCT = 2.5%   — máximo SL

Trailing stop:
  trail_step = 0.3 × sl_dist
"""
import logging
import config
import exchange as _exchange

log = logging.getLogger("risk")

# ── Límites porcentuales SL/TP ────────────────────────────────────────────────
SL_MIN_PCT = 0.004   # 0.4%
SL_MAX_PCT = 0.025   # 2.5%


def _tp_rr(score: int, regime: str) -> float:
    """TP_RR dinámico: ajusta el ratio riesgo:beneficio según régimen y score.

    En rango el precio no tiene recorrido largo → TP más conservador.
    Con señal fuerte en tendencia se deja correr más.
    Con señal débil se asegura el beneficio antes.
    """
    if regime == "range":
        return 1.5
    if score >= 85:
        return 2.5
    if score >= 70:
        return 2.0
    return 1.7


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
    return 0.7


def calc(side: str, entry: float, candles: list[dict], score: int = 70,
         symbol: str | None = None, regime: str = "bull") -> dict:
    """
    Calcula SL, TP, qty y trail_step para una entrada.

    Args:
        side    : 'long' | 'short'
        entry   : precio de entrada
        candles : velas 15m (para ATR)
        score   : score de la señal (0-100)
        symbol  : símbolo BingX (e.g. 'BTC-USDT'). Si se pasa, se consulta
                  el step-size real del contrato para redondear qty.
        regime  : régimen de mercado ('bull' | 'bear' | 'range') para TP_RR dinámico.
    """
    atr = _atr(candles, period=14)

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

    trail_step = round(0.3 * sl_dist, 8)

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
        "sl":         round(sl, 8),
        "tp":         round(tp, 8),
        "atr":        round(atr, 8),
        "trail_step": trail_step,
        "score":      score,
        "tp_rr":      rr,
    }
