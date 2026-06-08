import math
import time as _time

# Aperturas de sesión en horas UTC: Asia, London, New York, Post-NY/Asia-pre
_SESSION_OPENS_UTC = [0, 8, 13, 21]


def ema(closes, period):
    if len(closes) < period:
        return []
    k = 2 / (period + 1)
    result = [sum(closes[:period]) / period]
    for price in closes[period:]:
        result.append(price * k + result[-1] * (1 - k))
    return result

def rsi(closes, period=14):
    if len(closes) < period + 1:
        return 50.0
    gains, losses = [], []
    for i in range(1, len(closes)):
        diff = closes[i] - closes[i-1]
        gains.append(max(diff, 0))
        losses.append(max(-diff, 0))
    avg_gain = sum(gains[-period:]) / period
    avg_loss = sum(losses[-period:]) / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return round(100 - (100 / (1 + rs)), 2)

def macd(closes, fast=12, slow=26, signal=9):
    if len(closes) < slow + signal:
        return 0.0, 0.0, 0.0
    ema_fast = ema(closes, fast)
    ema_slow = ema(closes, slow)
    min_len = min(len(ema_fast), len(ema_slow))
    macd_line = [ema_fast[-(min_len-i)] - ema_slow[-(min_len-i)] for i in range(min_len)]
    signal_line = ema(macd_line, signal)
    if not signal_line:
        return 0.0, 0.0, 0.0
    m = round(macd_line[-1], 6)
    s = round(signal_line[-1], 6)
    return m, s, round(m - s, 6)

def atr(highs, lows, closes, period=14):
    trs = []
    for i in range(1, len(closes)):
        tr = max(
            highs[i] - lows[i],
            abs(highs[i] - closes[i-1]),
            abs(lows[i] - closes[i-1])
        )
        trs.append(tr)
    if len(trs) < period:
        return sum(trs) / len(trs) if trs else 0
    atr_val = sum(trs[:period]) / period
    for tr in trs[period:]:
        atr_val = (atr_val * (period - 1) + tr) / period
    return round(atr_val, 6)

def supertrend(highs, lows, closes, period=10, factor=3.0):
    atr_vals = []
    for i in range(1, len(closes)):
        tr = max(highs[i]-lows[i], abs(highs[i]-closes[i-1]), abs(lows[i]-closes[i-1]))
        atr_vals.append(tr)
    if len(atr_vals) < period:
        return 1, closes[-1]
    smoothed = [sum(atr_vals[:period])/period]
    for v in atr_vals[period:]:
        smoothed.append((smoothed[-1]*(period-1)+v)/period)
    direction = 1
    st = closes[-1]
    for i in range(len(smoothed)):
        idx = i + period
        if idx >= len(closes):
            break
        mid = (highs[idx] + lows[idx]) / 2
        upper = mid + factor * smoothed[i]
        lower = mid - factor * smoothed[i]
        if closes[idx] > upper:
            direction = 1
            st = lower
        elif closes[idx] < lower:
            direction = -1
            st = upper
    return direction, round(st, 6)


def _session_start_ms(now_s: float) -> int:
    """
    Devuelve el timestamp en milisegundos del inicio de la sesión activa
    (Asia / London / New York) para el instante now_s (epoch segundos UTC).

    Sesiones (UTC):
      Asia    00:00 – 08:00
      London  08:00 – 13:00
      New York 13:00 – 21:00
      Post-NY 21:00 – 00:00  (ancla a 21:00)
    """
    day_start_s   = now_s - (now_s % 86400)          # 00:00 UTC de hoy en segundos
    hour_utc      = int((now_s % 86400) // 3600)      # hora UTC actual (0–23)
    session_open_h = max(h for h in _SESSION_OPENS_UTC if h <= hour_utc)
    return int((day_start_s + session_open_h * 3600) * 1000)


def vwap(bars: list, reset_daily: bool = True) -> float:
    """
    VWAP anclado a la sesión activa (Asia / London / New York).

    Cuando reset_daily=True (default), acumula SOLO las barras desde
    el inicio de la sesión de mercado activa en ese momento:
      - Asia:     00:00 – 08:00 UTC
      - London:   08:00 – 13:00 UTC
      - New York: 13:00 – 21:00 UTC
      - Post-NY:  21:00 – 00:00 UTC

    Esto produce un VWAP siempre fresco y representativo del contexto
    de liquidez actual, en lugar de acumular hasta 21 horas de datos
    como hacía el reset diario fijo a 00:00 UTC.

    Fallback: si la sesión actual tiene < 4 barras (sesión muy reciente),
    retrocede a la sesión anterior completa en lugar de retroceder 24h.

    Cuando reset_daily=False, acumula todas las barras provistas
    (comportamiento anterior, útil para tests o timeframes > 1h).

    Usa el Typical Price (H+L+C)/3 ponderado por volumen.
    Seguro ante barras con v=0 o v=None (las omite sin excepción).
    Retorna 0.0 si no hay volumen acumulado válido.
    """
    if not reset_daily:
        bars_to_use = bars
    else:
        now_s         = _time.time()
        session_ms    = _session_start_ms(now_s)

        bars_to_use = [
            b for b in bars
            if b is not None and len(b) > 0
            and b[0] is not None and int(b[0]) >= session_ms
        ]

        # Fallback: sesión con < 4 barras → usar sesión anterior
        if len(bars_to_use) < 4:
            # Retroceder hasta la apertura de la sesión anterior
            hour_utc       = int((now_s % 86400) // 3600)
            opens_sorted   = sorted(_SESSION_OPENS_UTC)
            # Índice de la sesión actual
            cur_idx        = max(i for i, h in enumerate(opens_sorted) if h <= hour_utc)
            prev_idx       = (cur_idx - 1) % len(opens_sorted)
            prev_open_h    = opens_sorted[prev_idx]
            day_start_s    = now_s - (now_s % 86400)
            # Si la sesión anterior pertenece al día anterior (ej: Post-NY 21h y son las 00:xx)
            if prev_open_h > hour_utc:
                day_start_s -= 86400
            prev_session_ms = int((day_start_s + prev_open_h * 3600) * 1000)
            bars_to_use = [
                b for b in bars
                if b is not None and len(b) > 0
                and b[0] is not None and int(b[0]) >= prev_session_ms
            ]

    cum_vol = 0.0
    cum_tpv = 0.0
    for b in bars_to_use:
        try:
            h = float(b[2])
            l = float(b[3])
            c = float(b[4])
            v = float(b[5]) if b[5] is not None else 0.0
        except (TypeError, IndexError, ValueError):
            continue
        if v <= 0:
            continue
        tp = (h + l + c) / 3
        cum_tpv += tp * v
        cum_vol  += v
    return round(cum_tpv / cum_vol, 6) if cum_vol > 0 else 0.0


def rsi_divergence(bars: list, rsi_period: int = 14, lookback: int = 30) -> str:
    """Detecta divergencias RSI alcistas y bajistas.

    Compara los dos ultimos minimos de precio (divergencia alcista) o
    los dos ultimos maximos de precio (divergencia bajista) con los
    valores de RSI correspondientes.

    Retorna:
      'BULLISH'  — precio hace minimo mas bajo pero RSI hace minimo mas alto
      'BEARISH'  — precio hace maximo mas alto pero RSI hace maximo mas bajo
      'NONE'     — sin divergencia detectada

    Requiere al menos lookback+rsi_period+2 barras para ser fiable.
    """
    if len(bars) < lookback + rsi_period + 2:
        return "NONE"

    closes = [float(b[4]) for b in bars]
    lows   = [float(b[3]) for b in bars]
    highs  = [float(b[2]) for b in bars]

    # Calcula RSI para cada barra del lookback usando ventana deslizante
    rsi_vals = []
    for i in range(len(closes) - lookback, len(closes)):
        window = closes[max(0, i - rsi_period * 2): i + 1]
        rsi_vals.append(rsi(window, rsi_period))

    price_lows  = lows[-lookback:]
    price_highs = highs[-lookback:]

    # Busca los dos ultimos minimos locales de precio
    def find_local_mins(series, window=3):
        mins = []
        for i in range(window, len(series) - window):
            if series[i] == min(series[i - window: i + window + 1]):
                mins.append(i)
        return mins[-2:] if len(mins) >= 2 else []

    def find_local_maxs(series, window=3):
        maxs = []
        for i in range(window, len(series) - window):
            if series[i] == max(series[i - window: i + window + 1]):
                maxs.append(i)
        return maxs[-2:] if len(maxs) >= 2 else []

    # Divergencia alcista: precio min mas bajo, RSI min mas alto
    lmin_idx = find_local_mins(price_lows)
    if len(lmin_idx) == 2:
        i1, i2 = lmin_idx
        if price_lows[i2] < price_lows[i1] and rsi_vals[i2] > rsi_vals[i1]:
            return "BULLISH"

    # Divergencia bajista: precio max mas alto, RSI max mas bajo
    hmax_idx = find_local_maxs(price_highs)
    if len(hmax_idx) == 2:
        i1, i2 = hmax_idx
        if price_highs[i2] > price_highs[i1] and rsi_vals[i2] < rsi_vals[i1]:
            return "BEARISH"

    return "NONE"
