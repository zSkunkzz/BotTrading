"""risk.py — Gestión de riesgo con SL calibrado en ATR 1h.

Cambio principal v2:
  El SL ya no se calcula sobre el ATR de velas 15m sino sobre el ATR de
  velas 1h. Esto es crítico porque la mayoría de los trades duran horas
  (SOL 364min, OP 877min, SEI 803min en el historial). Un SL calibrado
  sobre ruido de 15m es inevitable que se toque por el paso del tiempo
  aunque la tesis siga siendo correcta.

  ATR 1h captura el rango real de movimiento en el timeframe del trade.

Sizing:
  base_margin = MARGIN_USDT
  score 78-84  →  1.0× base_margin
  score ≥ 85   →  1.4× base_margin

SL / TP:
  sl_pct = clamp(ATR_1h / entry × 1.2, SL_MIN_PCT, SL_MAX_PCT)
  tp_pct = sl_pct × _tp_rr(score, regime)

  Multiplicador 1.2 (antes 1.5 sobre ATR 15m):
    ATR 1h ya es ~3-4× el ATR 15m. Usar 1.2 sobre ATR 1h da un SL
    comparable pero con mucho más resistencia al ruido intracandle.

  SL_MIN_PCT = 0.8%   — igual que antes
  SL_MAX_PCT = 3.0%   — subido de 2.5% a 3.0% para dar espacio en 1h

  TP_RR dinámico:
    score ≥ 85  →  2.5×
    score ≥ 78  →  2.0×

Trailing stop:
  trail_step = max(0.3 × sl_dist, 1 tick)
  Igual que antes, ahora sl_dist es más amplio y el trailing
  también respira más.
"""
import logging
import math
import config
import exchange as _exchange

log = logging.getLogger("risk")

SL_MIN_PCT = 0.008   # 0.8%
SL_MAX_PCT = 0.030   # 3.0% (subido de 2.5% para dar espacio a ATR 1h)


def _tp_rr(score: int, regime: str) -> float:
    if score >= 85:
        return 2.5
    return 2.0


def _atr(candles: list[dict], period: int = 14) -> float:
    trs = []
    for i in range(1, len(candles)):
        h, l, pc = candles[i]["high"], candles[i]["low"], candles[i - 1]["close"]
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    return sum(trs[-period:]) / min(period, len(trs)) if trs else 0.0


def _size_multiplier(score: int) -> float:
    if score >= 85:
        return 1.4
    return 1.0


def calc(side: str, entry: float, candles: list[dict], score: int = 78,
         symbol: str | None = None, regime: str = "bull",
         candles_1h: list[dict] | None = None) -> dict:
    """
    Calcula SL, TP, qty y trail_step.

    candles_1h: si se pasan, el SL se basa en ATR 1h (preferido).
                Si no se pasan, fallback a ATR 15m (compatibilidad).
    """
    # ── ATR: usar 1h si está disponible ──────────────────────────────────────
    if candles_1h and len(candles_1h) >= 16:
        # ATR sobre velas 1h cerradas
        atr  = _atr(candles_1h[:-1], period=14)
        mult = 1.2   # 1.2× ATR 1h da espacio real al trade
        source = "1h"
    else:
        # Fallback: ATR 15m
        atr  = _atr(candles, period=14)
        mult = 1.5
        source = "15m"

    if atr > 0 and entry > 0:
        atr_pct = atr / entry
    else:
        log.warning("ATR=0 o entry=0, usando fallback SL_PCT")
        atr_pct = config.SL_PCT / 100 / mult
        source  = "fallback"

    raw_sl_pct = atr_pct * mult
    sl_pct     = max(SL_MIN_PCT, min(SL_MAX_PCT, raw_sl_pct))

    rr     = _tp_rr(score, regime)
    tp_pct = sl_pct * rr

    sl_dist = entry * sl_pct
    tp_dist = entry * tp_pct

    sl = (entry - sl_dist) if side == "long" else (entry + sl_dist)
    tp = (entry + tp_dist) if side == "long" else (entry - tp_dist)

    mult_size = _size_multiplier(score)
    margin    = config.MARGIN_USDT * mult_size
    raw_qty   = (margin * config.LEVERAGE) / entry

    step = 0.001
    if symbol:
        try:
            info = _exchange._get_contract_info(symbol)
            step = info["stepSize"]
            qty  = _exchange.floor_qty(raw_qty, step)
        except Exception as exc:
            log.warning("No se pudo obtener step-size para %s: %s — usando 3 dec", symbol, exc)
            qty = math.floor(raw_qty * 1000) / 1000
    else:
        qty = math.floor(raw_qty * 1000) / 1000

    raw_trail = 0.3 * sl_dist
    trail_step = round(max(raw_trail, step), 8)

    log.info(
        "[%s] score=%d regime=%s RR=%.1f mult=%.1f margin=%.2f "
        "ATR_%s=%.6f atr_pct=%.3f%% sl_pct=%.3f%% tp_pct=%.3f%% "
        "SL=%.6f TP=%.6f qty=%.8f trail=%.8f",
        side.upper(), score, regime, rr, mult_size, margin,
        source, atr, atr_pct * 100, sl_pct * 100, tp_pct * 100,
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
