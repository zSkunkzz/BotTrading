"""
signal_engine.py  —  Estrategia limpia y efectiva

Lógica en 3 capas:
  1. FILTRO DE TENDENCIA (1h)  : precio vs EMA200
  2. SEÑAL DE ENTRADA   (15m)  : RSI cruza nivel 50 + MACD histograma confirma
  3. SL / TP dinámicos         : 1.5× ATR (SL) y 3× ATR (TP)

Retorna un dict con:
  {
    'side':   'LONG' | 'SHORT' | None,
    'entry':  float,
    'sl':     float,
    'tp':     float,
    'reason': str
  }
"""

from bot.indicators import ema, rsi, macd, atr


# ─── Parámetros ───────────────────────────────────────────────────────────────
TREND_EMA_PERIOD   = 200   # EMA de tendencia en HTF (1h)
RSI_PERIOD         = 14
RSI_MID            = 50    # Nivel de cruce
MACD_FAST          = 12
MACD_SLOW          = 26
MACD_SIGNAL        = 9
ATR_PERIOD         = 14
ATR_SL_MULT        = 1.5   # SL = entrada ± 1.5 × ATR
ATR_TP_MULT        = 3.0   # TP = entrada ± 3.0 × ATR  (ratio 1:2)
MIN_BARS_HTF       = 220   # Mínimo de velas 1h para calcular EMA200
MIN_BARS_LTF       = 60    # Mínimo de velas 15m para señal
# ──────────────────────────────────────────────────────────────────────────────


def _no_signal(reason: str) -> dict:
    return {"side": None, "entry": None, "sl": None, "tp": None, "reason": reason}


def generate_signal(
    closes_1h: list,
    closes_15m: list,
    highs_15m:  list,
    lows_15m:   list,
) -> dict:
    """
    Genera una señal de trading.

    Parámetros
    ----------
    closes_1h  : lista de precios de cierre en timeframe 1h  (orden cronológico)
    closes_15m : lista de precios de cierre en timeframe 15m (orden cronológico)
    highs_15m  : lista de máximos en 15m
    lows_15m   : lista de mínimos en 15m

    Returns
    -------
    dict con 'side', 'entry', 'sl', 'tp', 'reason'
    """

    # ── 1. Validación de datos ─────────────────────────────────────────────────
    if len(closes_1h) < MIN_BARS_HTF:
        return _no_signal(f"Insuficientes velas 1h ({len(closes_1h)}/{MIN_BARS_HTF})")

    if len(closes_15m) < MIN_BARS_LTF:
        return _no_signal(f"Insuficientes velas 15m ({len(closes_15m)}/{MIN_BARS_LTF})")

    # ── 2. Filtro de tendencia (1h) ────────────────────────────────────────────
    ema200 = ema(closes_1h, TREND_EMA_PERIOD)
    if not ema200:
        return _no_signal("No se pudo calcular EMA200")

    price_now  = closes_15m[-1]
    ema200_val = ema200[-1]
    trend_bull = price_now > ema200_val   # precio sobre EMA200 → tendencia alcista
    trend_bear = price_now < ema200_val   # precio bajo  EMA200 → tendencia bajista

    # ── 3. RSI en 15m — detectar cruce del nivel 50 ───────────────────────────
    rsi_now  = rsi(closes_15m,       RSI_PERIOD)
    rsi_prev = rsi(closes_15m[:-1],  RSI_PERIOD)

    rsi_cross_up   = rsi_prev < RSI_MID and rsi_now >= RSI_MID   # cruce alcista
    rsi_cross_down = rsi_prev > RSI_MID and rsi_now <= RSI_MID   # cruce bajista

    # ── 4. MACD en 15m — histograma confirma dirección ────────────────────────
    _, _, hist_now  = macd(closes_15m,       MACD_FAST, MACD_SLOW, MACD_SIGNAL)
    _, _, hist_prev = macd(closes_15m[:-1],  MACD_FAST, MACD_SLOW, MACD_SIGNAL)

    macd_bull = hist_now > 0 and hist_now > hist_prev   # histograma positivo y creciendo
    macd_bear = hist_now < 0 and hist_now < hist_prev   # histograma negativo y cayendo

    # ── 5. ATR para SL / TP dinámicos ─────────────────────────────────────────
    atr_val = atr(highs_15m, lows_15m, closes_15m, ATR_PERIOD)
    if atr_val == 0:
        return _no_signal("ATR = 0, datos inválidos")

    sl_dist = ATR_SL_MULT * atr_val
    tp_dist = ATR_TP_MULT * atr_val

    # ── 6. Decisión final ─────────────────────────────────────────────────────
    # LONG: tendencia alcista + RSI cruza 50 hacia arriba + MACD confirma
    if trend_bull and rsi_cross_up and macd_bull:
        entry = price_now
        return {
            "side":   "LONG",
            "entry":  round(entry, 6),
            "sl":     round(entry - sl_dist, 6),
            "tp":     round(entry + tp_dist, 6),
            "reason": f"LONG | precio({price_now:.2f}) > EMA200({ema200_val:.2f}) | RSI {rsi_prev:.1f}→{rsi_now:.1f} cruza 50 | MACD hist {hist_now:.6f}"
        }

    # SHORT: tendencia bajista + RSI cruza 50 hacia abajo + MACD confirma
    if trend_bear and rsi_cross_down and macd_bear:
        entry = price_now
        return {
            "side":   "SHORT",
            "entry":  round(entry, 6),
            "sl":     round(entry + sl_dist, 6),
            "tp":     round(entry - tp_dist, 6),
            "reason": f"SHORT | precio({price_now:.2f}) < EMA200({ema200_val:.2f}) | RSI {rsi_prev:.1f}→{rsi_now:.1f} cruza 50 | MACD hist {hist_now:.6f}"
        }

    return _no_signal(
        f"Sin señal | trend={'BULL' if trend_bull else 'BEAR' if trend_bear else 'NEUTRAL'} "
        f"RSI={rsi_now:.1f} MACD_hist={hist_now:.6f}"
    )
