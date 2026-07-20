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

v5:
  SL, TP, be_trigger y be_sl se redondean con _round_price(coin, precio)
  en lugar de round(..., 8) fijo.
  _round_price lee el tickSize real del par desde la API de HL (cacheado)
  y usa Decimal.quantize para evitar residuos de punto flotante.
  Esto asegura que precios tanto para monedas <$0.001 (PEPE, SHIB, kPEPE)
  como para monedas >$1000 (BTC, ETH) cumplan el formato que HL exige
  y no sean rechazados con "Order has invalid price" al colocarse.

v6 (fix R:R y BE agresivo):
  - BE_ATR_MULT: 1.0 → 1.5
    El break-even a 1×ATR se activaba al primer movimiento normal del
    precio, sacando trades con casi cero ganancia (avg win real +2.18 USDT).
    Con 1.5× el precio tiene más margen antes de bloquear en BE.
  - _tp_rr: score 70-84 → 1.5× (antes 2.0×)
    El TP de 2.0× nunca se alcanzaba en la práctica (R:R efectivo 0.79).
    Con 1.5× el TP es más realista y el WR necesario baja a ~40%,
    alineado con el 43.9% histórico observado.
    Score >= 85 mantiene 2.5× (señales de alta convicción).

v7 — R:R adaptativo y SL_MIN_PCT ajustado:
  - SL_MIN_PCT: 0.008 → 0.012 (más espacio para el precio, reduce stops por ruido)
  - _tp_rr ahora recibe adx_1h y atr_pct para ajustar RR dinámicamente:
      * En proto-regímenes: RR reducido (1.2–1.8)
      * Si ADX_1h < 22: RR reducido (1.0–1.8)
      * Si volatilidad > 3%: RR reducido
      * Score alto + tendencia fuerte: RR hasta 2.5
  - Se añaden parámetros a calc y se pasa información desde main.
"""
import logging
import math
import config
import exchange as _exchange

log = logging.getLogger("risk")

SL_MIN_PCT = 0.012   # 1.2% (v7: subido desde 0.8%)
SL_MAX_PCT = 0.030   # 3.0%

BE_ATR_MULT    = 1.5
BE_BUFFER_MULT = 0.1

TRAIL_STEP_MULT = 0.5


def _tp_rr(score: int, regime: str, adx_1h: float, atr_pct: float) -> float:
    """R:R adaptativo según calidad de señal, régimen, ADX y volatilidad.

    - Base: 1.5
    - Score >= 85 y ADX >= 30 → 2.5 (tendencia fuerte + alta convicción)
    - Score >= 80 y ADX >= 25 → 2.0
    - Proto-regímenes: RR reducido (máx 1.8, mínimo 1.2)
    - ADX < 22 (sin tendencia): RR reducido (máx 1.8, mínimo 1.0)
    - Volatilidad alta (>3% ATR): RR reducido 0.3
    - Si varias condiciones se combinan, se aplican de forma acumulativa pero nunca por debajo de 1.0.
    """
    base = 1.5

    # Bonos por score + tendencia
    if score >= 85 and adx_1h >= 30:
        base = 2.5
    elif score >= 80 and adx_1h >= 25:
        base = 2.0
    elif score >= 75 and adx_1h >= 22:
        base = 1.8

    # Penalizaciones
    if regime in ("proto_bull", "proto_bear"):
        base = max(1.2, base - 0.4)

    if adx_1h < 22:
        base = max(1.0, base - 0.4)

    if atr_pct > 0.03:
        base = max(1.0, base - 0.3)

    return round(base, 1)


def _atr(candles: list[dict], period: int = 14) -> float:
    trs = []
    for i in range(1, len(candles)):
        h, l, pc = candles[i]["high"], candles[i]["low"], candles[i - 1]["close"]
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    return sum(trs[-period:]) / min(period, len(trs)) if trs else 0.0


def _rp(coin: str | None, price: float) -> float:
    if coin:
        try:
            return _exchange._round_price(coin, price)
        except Exception:
            pass
    return round(price, 8)


def calc(side: str, entry: float, candles: list[dict], score: int = 78,
         symbol: str | None = None, regime: str = "bull",
         candles_1h: list[dict] | None = None,
         adx_1h: float = 0.0, atr_1h_pct: float = 0.0) -> dict:
    """
    Calcula SL, TP, qty, trail_step y be_trigger.

    Ahora recibe adx_1h y atr_1h_pct para ajustar R:R dinámicamente.
    """
    coin = _exchange._hl_symbol(symbol) if symbol else None

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

    # R:R adaptativo usando los parámetros recibidos
    rr = _tp_rr(score, regime, adx_1h, atr_1h_pct if atr_1h_pct > 0 else atr_pct)
    tp_pct = sl_pct * rr

    sl_dist = entry * sl_pct
    tp_dist = entry * tp_pct

    sl_raw = (entry - sl_dist) if side == "long" else (entry + sl_dist)
    tp_raw = (entry + tp_dist) if side == "long" else (entry - tp_dist)

    sl = _rp(coin, sl_raw)
    tp = _rp(coin, tp_raw)

    if atr > 0:
        be_trigger_raw = (entry + BE_ATR_MULT * atr)    if side == "long" else (entry - BE_ATR_MULT * atr)
        be_sl_raw      = (entry + BE_BUFFER_MULT * atr) if side == "long" else (entry - BE_BUFFER_MULT * atr)
        be_trigger = _rp(coin, be_trigger_raw)
        be_sl      = _rp(coin, be_sl_raw)
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

    if coin:
        try:
            tick_dec = _exchange._get_tick_decimals(coin)
            min_step = 10 ** (-tick_dec)
        except Exception:
            min_step = 1e-6
    else:
        min_step = 1e-6

    raw_trail  = TRAIL_STEP_MULT * sl_dist
    trail_step = round(max(raw_trail, min_step), 8)

    log.info(
        "[%s] score=%d regime=%s RR=%.1f margin=%.2f "
        "ATR_%s=%.8f atr_pct=%.4f%% sl_pct=%.3f%% tp_pct=%.3f%% "
        "SL=%.8f TP=%.8f be_trigger=%s be_sl=%s qty=%.8f trail=%.8f",
        side.upper(), score, regime, rr, margin,
        source, atr, atr_pct * 100, sl_pct * 100, tp_pct * 100,
        sl, tp,
        f"{be_trigger:.8f}" if be_trigger else "N/A",
        f"{be_sl:.8f}" if be_sl else "N/A",
        qty, trail_step,
    )

    return {
        "qty":        qty,
        "sl":         sl,
        "tp":         tp,
        "atr":        round(atr, 8),
        "trail_step": trail_step,
        "score":      score,
        "tp_rr":      rr,
        "be_trigger": be_trigger,
        "be_sl":      be_sl,
    }


def check_breakeven(symbol: str, pos: dict, current_price: float) -> bool:
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
        "[%s] \U0001f512 Break-even lock activado | precio=%.8f trigger=%.8f be_sl=%.8f (antes sl=%.8f)",
        symbol, current_price, be_trigger, be_sl, current_sl or 0,
    )
    coin = _exchange._hl_symbol(symbol)
    pos["sl"]        = _rp(coin, be_sl)
    pos["be_locked"] = True
    return True