"""risk.py — Gestión de riesgo con SL calibrado en ATR 1h y break-even lock.

v3:
  Break-even lock activado cuando precio ≥ 1×ATR_1h a favor.
  SL movido a entry + 0.1×ATR (cubre comisiones).

v2:
  SL sobre ATR 1h (no 15m).

v4 (trail_step):
  trail_step sube de 0.3× a 0.5× sl_dist para evitar que el trailing
  se mueva en cada tick en altcoins baratas (ONDO, PYTH, kPEPE...).
  Un step más grueso reduce cancelaciones/recolocaciones innecesarias
  en Hyperliquid y el spam de notificaciones Telegram.
  Mínimo absoluto: 1 tick del par (tickSize).

Sizing:
  Fijo: MARGIN_USDT para todas las señales.

SL / TP:
  sl_pct = clamp(ATR_1h / entry × 1.2, SL_MIN_PCT, SL_MAX_PCT)
  tp_pct = sl_pct × _tp_rr(score, regime)
  SL_MIN_PCT = 0.8% | SL_MAX_PCT = 3.0%
  TP_RR: score ≥ 85 → 2.5× | resto → 2.0×

Break-even:
  be_trigger = entry ± 1.0 × ATR_1h
  be_sl      = entry ± 0.1 × ATR_1h
"""
import logging
import math
import config
import exchange as _exchange

log = logging.getLogger("risk")

SL_MIN_PCT = 0.008   # 0.8%
SL_MAX_PCT = 0.030   # 3.0%

BE_ATR_MULT    = 1.0
BE_BUFFER_MULT = 0.1

# trail_step: fracción del sl_dist usada como paso mínimo del trailing.
# 0.5 (antes 0.3) reduce el número de actualizaciones en altcoins baratas.
TRAIL_STEP_MULT = 0.5


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


def calc(side: str, entry: float, candles: list[dict], score: int = 78,
         symbol: str | None = None, regime: str = "bull",
         candles_1h: list[dict] | None = None) -> dict:
    """
    Calcula SL, TP, qty, trail_step y be_trigger.

    candles_1h: si se pasan, SL se basa en ATR 1h (preferido).
                Fallback a ATR 15m si no están disponibles.
    """
    if candles_1h and len(candles_1h) >= 16:
        atr    = _atr(candles_1h[:-1], period=14)
        mult   = 1.2
        source = "1h"
    else:
        atr    = _atr(candles, period=14)
        mult   = 1.5
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

    if atr > 0:
        be_trigger = (entry + BE_ATR_MULT * atr) if side == "long" else (entry - BE_ATR_MULT * atr)
        be_sl      = (entry + BE_BUFFER_MULT * atr) if side == "long" else (entry - BE_BUFFER_MULT * atr)
    else:
        be_trigger = None
        be_sl      = None

    margin  = config.MARGIN_USDT
    raw_qty = (margin * config.LEVERAGE) / entry

    if symbol:
        try:
            qty = _exchange.floor_qty(raw_qty, symbol)
        except Exception as exc:
            log.warning("No se pudo calcular floor_qty para %s: %s — truncando a 3 dec", symbol, exc)
            qty = math.floor(raw_qty * 1000) / 1000
    else:
        qty = math.floor(raw_qty * 1000) / 1000

    # v4: trail_step = TRAIL_STEP_MULT × sl_dist (subido de 0.3 a 0.5)
    # Mínimo: 1 tick de precio del par para no enviar órdenes sub-tick.
    if symbol:
        coin = _exchange._hl_symbol(symbol)
        tick_dec = _exchange._get_tick_decimals(coin)
        min_step = 10 ** (-tick_dec)
    else:
        min_step = 1e-6

    raw_trail  = TRAIL_STEP_MULT * sl_dist
    trail_step = round(max(raw_trail, min_step), 8)

    log.info(
        "[%s] score=%d regime=%s RR=%.1f margin=%.2f "
        "ATR_%s=%.6f atr_pct=%.3f%% sl_pct=%.3f%% tp_pct=%.3f%% "
        "SL=%.6f TP=%.6f be_trigger=%s be_sl=%s qty=%.8f trail=%.8f",
        side.upper(), score, regime, rr, margin,
        source, atr, atr_pct * 100, sl_pct * 100, tp_pct * 100,
        sl, tp,
        f"{be_trigger:.6f}" if be_trigger else "N/A",
        f"{be_sl:.6f}" if be_sl else "N/A",
        qty, trail_step,
    )

    return {
        "qty":        qty,
        "sl":         round(sl, 8),
        "tp":         round(tp, 8),
        "atr":        round(atr, 8),
        "trail_step": trail_step,
        "score":      score,
        "tp_rr":      rr,
        "be_trigger": round(be_trigger, 8) if be_trigger else None,
        "be_sl":      round(be_sl, 8) if be_sl else None,
    }


def check_breakeven(symbol: str, pos: dict, current_price: float) -> bool:
    """Evalúa si activar break-even lock. Devuelve True si se activó ahora."""
    if pos.get("be_locked"):
        return False

    be_trigger = pos.get("be_trigger")
    be_sl      = pos.get("be_sl")
    if be_trigger is None or be_sl is None:
        return False

    side = pos["side"]
    triggered = (
        (side == "long"  and current_price >= be_trigger) or
        (side == "short" and current_price <= be_trigger)
    )
    if not triggered:
        return False

    current_sl = pos.get("sl")
    if current_sl is not None:
        if side == "long"  and be_sl <= current_sl:
            return False
        if side == "short" and be_sl >= current_sl:
            return False

    log.info(
        "[%s] 🔒 Break-even lock activado | precio=%.6f trigger=%.6f be_sl=%.6f (antes sl=%.6f)",
        symbol, current_price, be_trigger, be_sl, current_sl or 0,
    )
    pos["sl"]        = round(be_sl, 8)
    pos["be_locked"] = True
    return True
