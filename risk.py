"""risk.py — Gestión de riesgo con SL calibrado en ATR 1h y break-even lock.

Cambios v3:
  Break-even lock:
    Cuando el precio se mueve ≥ 1×ATR_1h a favor, el módulo calcula el
    nivel de break-even (entry + 0.1×ATR para cubrir comisiones).
    main.py llama a check_breakeven() en cada loop y aplica el lock
    UNA sola vez por posición (flag 'be_locked' en el dict de posición).

    Efecto: el trade se vuelve "gratis" una vez que alcanza +1R de beneficio.
    Elimina el mayor destructor de capital: trades que llegaron a +1R
    y acabaron en SL completo.

Cambios v2:
  SL sobre ATR 1h (no 15m). Ver docstring v2 para detalles.

Sizing:
  Fijo: MARGIN_USDT (20 USDT) para todas las señales.
  Sin multiplicador dinámico por score.

SL / TP:
  sl_pct = clamp(ATR_1h / entry × 1.2, SL_MIN_PCT, SL_MAX_PCT)
  tp_pct = sl_pct × _tp_rr(score, regime)

  SL_MIN_PCT = 0.8%
  SL_MAX_PCT = 3.0%

  TP_RR dinámico:
    score ≥ 85  →  2.5×
    score ≥ 78  →  2.0×
    proto_bull / proto_bear → RR × 0.8 (FIX BUG-11)

Break-even lock:
  be_trigger = entry ± 1.0 × ATR_1h   (± según side)
  be_sl      = entry ± 0.1 × ATR_1h   (buffer para comisiones)
  Se activa UNA vez. Después el trailing sigue operando desde be_sl.
"""
import logging
import math
import config
import exchange as _exchange

log = logging.getLogger("risk")

SL_MIN_PCT = 0.008   # 0.8%
SL_MAX_PCT = 0.030   # 3.0%

# Break-even: se activa cuando el precio se mueve ≥ BE_ATR_MULT × ATR a favor
BE_ATR_MULT    = 1.0   # 1× ATR → activar break-even
BE_BUFFER_MULT = 0.1   # buffer 0.1× ATR por encima del entry (cubre comisiones ~0.04%×2)

# FIX BUG-11: regímenes proto reducen el RR para ser más conservadores
_PROTO_REGIMES = {"proto_bull", "proto_bear"}
_PROTO_RR_MULT = 0.8


def _tp_rr(score: int, regime: str) -> float:
    """RR dinámico según score y régimen.

    FIX BUG-11: los regímenes 'proto_bull' y 'proto_bear' aplican un
    multiplicador de 0.8 sobre el RR base, forzando objetivos más cercanos
    y reduciendo el tiempo de exposición en tendencias no confirmadas.
    """
    if score >= 85:
        rr = 2.5
    else:
        rr = 2.0
    if regime in _PROTO_REGIMES:
        rr *= _PROTO_RR_MULT
    return round(rr, 4)


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
    Calcula SL, TP, qty, trail_step y be_trigger (break-even lock level).

    candles_1h: si se pasan, el SL se basa en ATR 1h (preferido).
                Si no se pasan, fallback a ATR 15m (compatibilidad).

    FIX BUG-7: el dict devuelto incluye 'tp_original' con el TP calculado
    en el momento de apertura. main.py debe guardar este valor en el dict de
    posición para que las extensiones de TP calculen la distancia correctamente.
    """
    # ── ATR: usar 1h si está disponible ──────────────────────────────────────
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

    # ── Break-even lock levels ────────────────────────────────────────────
    # be_trigger: precio al que se activa el lock (1× ATR a favor)
    # be_sl: nuevo SL tras lock (entry + buffer de comisiones)
    if atr > 0:
        be_trigger = (entry + BE_ATR_MULT * atr) if side == "long" else (entry - BE_ATR_MULT * atr)
        be_sl      = (entry + BE_BUFFER_MULT * atr) if side == "long" else (entry - BE_BUFFER_MULT * atr)
    else:
        be_trigger = None
        be_sl      = None

    # Sizing fijo: MARGIN_USDT para todas las señales
    margin  = config.MARGIN_USDT
    raw_qty = (margin * config.LEVERAGE) / entry

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

    tp_rounded = round(tp, 8)

    log.info(
        "[%s] score=%d regime=%s RR=%.2f margin=%.2f "
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
        "qty":         qty,
        "sl":          round(sl, 8),
        "tp":          tp_rounded,
        "tp_original": tp_rounded,   # FIX BUG-7: guardar TP inicial para extensiones
        "atr":         round(atr, 8),
        "trail_step":  trail_step,
        "score":       score,
        "tp_rr":       rr,
        "be_trigger":  round(be_trigger, 8) if be_trigger else None,
        "be_sl":       round(be_sl, 8) if be_sl else None,
    }


def check_breakeven(symbol: str, pos: dict, current_price: float) -> bool:
    """Evalúa si el trade debe moverse a break-even. Devuelve True si se activó el lock.

    Debe llamarse desde main.py en cada loop, ANTES de _update_trailing.
    El flag 'be_locked' en el dict de posición evita activarlo más de una vez.

    Parámetros:
        symbol        : símbolo para logs
        pos           : dict de posición local (se modifica in-place si se activa)
        current_price : precio actual del feed

    Devuelve:
        True  → lock activado en este loop (llamador debe actualizar SL en exchange)
        False → no activado (ya estaba activo, o precio no llegó al trigger aún)
    """
    if pos.get("be_locked"):
        return False  # ya activado previamente

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

    # Solo activar si be_sl mejora el SL actual
    current_sl = pos.get("sl")
    if current_sl is not None:
        if side == "long"  and be_sl <= current_sl:
            return False  # el trailing ya movió el SL más lejos de entry
        if side == "short" and be_sl >= current_sl:
            return False

    log.info(
        "[%s] 🔒 Break-even lock activado | precio=%.6f trigger=%.6f be_sl=%.6f (antes sl=%.6f)",
        symbol, current_price, be_trigger, be_sl, current_sl or 0,
    )
    pos["sl"]       = round(be_sl, 8)
    pos["be_locked"] = True
    return True
