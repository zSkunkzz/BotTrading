# -*- coding: utf-8 -*-
"""
signal_engine.py — Motor de señales v23.1: Momentum puro con pullback (pulido).

CAMBIOS v23.1 sobre v23:
  Fix 1 — Zona pullback basada en ATR_1h (0.5×) en vez de porcentaje fijo.
           Ahora la tolerancia se adapta a la volatilidad real de cada activo.
  Fix 2 — Validación de retroceso real: el precio debe venir del lado correcto
           del EMA/VWAP en las últimas N velas 1h antes del pullback actual.
  Fix 3 — TP filtrado por estructura 4h: si el objetivo choca con un
           máximo/mínimo relevante, se recorta. Si el RR resultante < MIN_RR
           se descarta la operación.

ESTRATEGIA v23.1 — 3 reglas binarias:
  R1 → EMA21 > EMA50 en 4h con spread ≥ EMA_SPREAD_MIN   → dirección LONG/SHORT
  R2 → Precio retrocede a zona EMA21_1h o VWAP (0.5×ATR) → pullback real verificado
  R3 → Vela 15m en dirección con vol ≥ VOL_CONFIRM_MIN    → confirmación entrada

SL: mínimo/máximo últimas SL_STRUCT_BARS velas 1h con cap SL_STRUCTURE_MAX_DIST_PCT.
    Fallback: entry ± SL_ATR_MULT × ATR_15m.
TP: entry ± TP_RR_MULT × riesgo, recortado por estructura 4h si es necesario.
"""
from __future__ import annotations

import asyncio
import logging
import os
import time
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, Tuple

from bot.indicators import ema, atr as calc_atr, vwap as calc_vwap

log = logging.getLogger(__name__)

# ─── Parámetros configurables por env ────────────────────────────────────────

MIN_RR: float = float(os.getenv("MIN_RR_REQUIRED", "2.0"))

# Tendencia 4h
_EMA_SPREAD_MIN         = float(os.getenv("EMA_SPREAD_MIN",         "0.003"))  # 0.3%

# Pullback zona de valor 1h — FIX 1: basado en ATR_1h
_PULLBACK_ATR_MULT      = float(os.getenv("PULLBACK_ATR_MULT",      "0.5"))    # zona = 0.5 × ATR_1h
_PULLBACK_REVERT_BARS   = int(os.getenv("PULLBACK_REVERT_BARS",     "4"))      # FIX 2: velas 1h a mirar

# Confirmación 15m
_VOL_CONFIRM_MIN        = float(os.getenv("VOL_CONFIRM_MIN",        "1.2"))
_VOL_AVG_WINDOW         = int(os.getenv("VOL_AVG_WINDOW",           "20"))
_VOL_MIN_GLOBAL         = float(os.getenv("VOL_MIN_GLOBAL",          "0.6"))

# SL / TP
_SL_STRUCT_BARS         = int(os.getenv("SL_STRUCT_BARS",            "3"))
_SL_STRUCTURE_MAX_DIST  = float(os.getenv("SL_STRUCTURE_MAX_DIST_PCT", "4.0")) / 100.0
_SL_ATR_MULT            = float(os.getenv("SL_ATR_MULT",             "1.5"))
_TP_RR_MULT             = float(os.getenv("TP_RR_MULT",              "2.5"))

# FIX 3: estructura 4h para el TP
_TP_STRUCT_BARS_4H      = int(os.getenv("TP_STRUCT_BARS_4H",         "20"))    # velas 4h a mirar
_TP_STRUCT_BUFFER       = float(os.getenv("TP_STRUCT_BUFFER",        "0.002")) # 0.2% buffer al nivel

# Funding
_FUNDING_LONG_MAX       = float(os.getenv("FUNDING_LONG_MAX",        "0.0005"))
_FUNDING_SHORT_MIN      = float(os.getenv("FUNDING_SHORT_MIN",       "-0.0005"))

# Apalancamiento
_MAX_LEV                = int(os.getenv("LEVERAGE", "15"))

# Barras necesarias
_BARS_NEEDED            = int(os.getenv("BARS_NEEDED", "100"))

# Cooldowns
_FLIP_COOLDOWN_S        = float(os.getenv("SIGNAL_FLIP_COOLDOWN_S",  "120"))
_MANUAL_CLOSE_CD_S      = int(os.getenv("MANUAL_CLOSE_COOLDOWN_S",   "600"))

# RSI sobreextensión
_RSI_OB                 = float(os.getenv("RSI_OVERBOUGHT",  "72"))
_RSI_OS                 = float(os.getenv("RSI_OVERSOLD",    "28"))


# ─── Resultado de señal ───────────────────────────────────────────────────────

@dataclass
class SignalResult:
    symbol:        str
    signal:        str          # "LONG" | "SHORT" | "NEUTRAL"
    entry_mode:    str          # "NORMAL" | "HOLD"
    score:         int
    max_score:     int
    entry:         float
    sl:            float
    tp1:           float
    tp2:           float        # igual a tp1 (TP único)
    atr:           float
    rr:            float
    suggested_lev: int
    indicators:    Dict
    is_valid:      bool = True
    reason:        str  = ""
    signal_block:  str  = ""
    extra:         Dict = field(default_factory=dict)


# ─── Limpieza de barras ───────────────────────────────────────────────────────

def _clean_bars(bars: list) -> list:
    return [b for b in (bars or []) if b is not None and all(v is not None for v in b)]


# ─── Conversión de símbolo ────────────────────────────────────────────────────

def _to_ccxt_symbol(symbol: str) -> str:
    if "/USDC:USDC" in symbol:
        return symbol
    coin = (
        symbol
        .replace(":USDT", "").replace(":USDC", "")
        .replace("/USDT", "").replace("/USDC", "")
        .replace("/USD",  "").replace("USDT",  "")
        .upper().strip()
    )
    return f"{coin}/USDC:USDC"


# ─── Indicadores ─────────────────────────────────────────────────────────────

def _compute_indicators(bars: list) -> dict:
    if not bars or len(bars) < 10:
        return {}
    closes = [float(b[4]) for b in bars]
    highs  = [float(b[2]) for b in bars]
    lows   = [float(b[3]) for b in bars]
    vols   = [float(b[5]) for b in bars]

    ema21_vals = ema(closes, 21)
    ema50_vals = ema(closes, 50)
    atr14      = calc_atr(highs, lows, closes, 14)
    vwap_v     = calc_vwap(bars)

    vol_window = min(_VOL_AVG_WINDOW, len(vols))
    avg_vol    = sum(vols[-vol_window:]) / vol_window if vol_window > 0 else 1.0
    vol_ratio  = round(vols[-1] / avg_vol, 3) if avg_vol > 0 else 1.0

    ema21  = ema21_vals[-1] if ema21_vals else None
    ema50  = ema50_vals[-1] if ema50_vals else None
    spread = abs(ema21 - ema50) / ema50 if (ema21 and ema50 and ema50 != 0) else 0.0

    return {
        "ema21":      ema21,
        "ema50":      ema50,
        "ema_spread": spread,
        "ema_bull":   bool(ema21 and ema50 and ema21 > ema50),
        "ema_bear":   bool(ema21 and ema50 and ema21 < ema50),
        "atr":        atr14,
        "vol_ratio":  vol_ratio,
        "vwap":       vwap_v,
        "close":      closes[-1],
        "high":       highs[-1],
        "low":        lows[-1],
    }


# ─── RSI simple ──────────────────────────────────────────────────────────────

def _rsi_last(closes: list, period: int = 14) -> Optional[float]:
    if len(closes) < period + 1:
        return None
    gains, losses = [], []
    for i in range(1, period + 1):
        diff = closes[-period - 1 + i] - closes[-period - 2 + i]
        (gains if diff > 0 else losses).append(abs(diff))
    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period
    if avg_loss == 0:
        return 100.0
    return round(100 - 100 / (1 + avg_gain / avg_loss), 2)


# ─── FIX 2: Verificar que el pullback es real ─────────────────────────────────
# El precio debe haber estado del lado correcto del nivel en al menos una
# de las últimas N velas antes de la actual.

def _verify_real_pullback(
    bars_1h: list,
    direction: str,
    level: float,
    n_bars: int = _PULLBACK_REVERT_BARS,
) -> bool:
    """
    LONG : al menos una de las últimas n velas 1h tuvo close > level
           (el precio venía de arriba antes del retroceso actual).
    SHORT: al menos una de las últimas n velas 1h tuvo close < level.
    """
    if len(bars_1h) < n_bars + 2 or level <= 0:
        return True  # sin datos suficientes: no bloquear
    # excluir la última vela (la actual)
    recent_closes = [float(b[4]) for b in bars_1h[-(n_bars + 1):-1]]
    if direction == "LONG":
        return any(c > level for c in recent_closes)
    else:
        return any(c < level for c in recent_closes)


# ─── FIX 3: Estructura 4h para el TP ─────────────────────────────────────────

def _adjust_tp_for_structure(
    bars_4h: list,
    direction: str,
    entry: float,
    tp_raw: float,
    sl: float,
) -> Tuple[float, float]:
    """
    Busca el máximo/mínimo relevante entre entry y tp_raw en las últimas
    TP_STRUCT_BARS_4H velas 4h. Si hay una zona de estructura dentro del
    recorrido, recorta el TP justo antes de ella.

    Devuelve (tp_final, rr_final).
    """
    if len(bars_4h) < 5 or entry <= 0 or tp_raw <= 0:
        risk = abs(entry - sl)
        return tp_raw, round(abs(tp_raw - entry) / risk, 2) if risk > 0 else 0.0

    highs_4h = [float(b[2]) for b in bars_4h[-_TP_STRUCT_BARS_4H:]]
    lows_4h  = [float(b[3]) for b in bars_4h[-_TP_STRUCT_BARS_4H:]]

    risk = abs(entry - sl)
    if risk <= 0:
        return tp_raw, 0.0

    if direction == "LONG":
        # Buscar el mínimo máximo-4h que quede ENTRE entry y tp_raw
        obstacles = [h for h in highs_4h if entry < h < tp_raw]
        if obstacles:
            nearest = min(obstacles)  # la resistencia más cercana al entry
            tp_adj  = round(nearest * (1 - _TP_STRUCT_BUFFER), 6)
            log.debug(
                "[signal_engine] LONG TP recortado por estructura 4h: %.6f → %.6f",
                tp_raw, tp_adj,
            )
            return tp_adj, round(abs(tp_adj - entry) / risk, 2)
    else:
        # Buscar el máximo mínimo-4h entre tp_raw y entry
        obstacles = [l for l in lows_4h if tp_raw < l < entry]
        if obstacles:
            nearest = max(obstacles)  # el soporte más cercano al entry
            tp_adj  = round(nearest * (1 + _TP_STRUCT_BUFFER), 6)
            log.debug(
                "[signal_engine] SHORT TP recortado por estructura 4h: %.6f → %.6f",
                tp_raw, tp_adj,
            )
            return tp_adj, round(abs(tp_adj - entry) / risk, 2)

    return tp_raw, round(abs(tp_raw - entry) / risk, 2)


# ─── Núcleo de la estrategia ──────────────────────────────────────────────────

def _evaluate_signal(
    ind_4h:   dict,
    ind_1h:   dict,
    ind_15m:  dict,
    bars_1h:  list,
    bars_15m: list,
) -> Tuple[str, List[str]]:
    reasons: List[str] = []

    # ── R1: Tendencia 4h ──────────────────────────────────────────────────────
    if not ind_4h:
        return "NEUTRAL", ["Sin datos 4h"]

    spread_4h = ind_4h.get("ema_spread", 0)
    if spread_4h < _EMA_SPREAD_MIN:
        return "NEUTRAL", [f"4h en rango — spread EMA={spread_4h*100:.2f}% < {_EMA_SPREAD_MIN*100:.1f}%"]

    if ind_4h.get("ema_bull"):
        direction = "LONG"
        reasons.append(f"✓ R1: EMA4h alcista (spread={spread_4h*100:.2f}%)")
    elif ind_4h.get("ema_bear"):
        direction = "SHORT"
        reasons.append(f"✓ R1: EMA4h bajista (spread={spread_4h*100:.2f}%)")
    else:
        return "NEUTRAL", ["4h sin dirección clara"]

    # ── R2: Pullback a zona de valor 1h ──────────────────────────────────────
    if not ind_1h:
        return "NEUTRAL", reasons + ["Sin datos 1h"]

    close_1h  = ind_1h.get("close", 0.0)
    ema21_1h  = ind_1h.get("ema21")
    vwap_1h   = ind_1h.get("vwap", 0.0)
    atr_1h    = ind_1h.get("atr") or 0.0

    # FIX 1: zona = 0.5 × ATR_1h (adaptativo) con fallback a 0.5% si ATR no disponible
    zone_dist = (_PULLBACK_ATR_MULT * atr_1h) if atr_1h > 0 else (close_1h * 0.005)

    in_zone  = False
    zone_ref = None
    level_used = None

    if ema21_1h and ema21_1h > 0:
        if abs(close_1h - ema21_1h) <= zone_dist:
            in_zone    = True
            zone_ref   = f"EMA21_1h={ema21_1h:.6f} (zona±{zone_dist:.6f})"
            level_used = ema21_1h

    if not in_zone and vwap_1h and vwap_1h > 0:
        if abs(close_1h - vwap_1h) <= zone_dist:
            # FIX 1 + chequeo direccional mínimo para VWAP
            if direction == "LONG" and close_1h >= vwap_1h * 0.995:
                in_zone    = True
                zone_ref   = f"VWAP_1h={vwap_1h:.6f} (zona±{zone_dist:.6f})"
                level_used = vwap_1h
            elif direction == "SHORT" and close_1h <= vwap_1h * 1.005:
                in_zone    = True
                zone_ref   = f"VWAP_1h={vwap_1h:.6f} (zona±{zone_dist:.6f})"
                level_used = vwap_1h

    if not in_zone:
        ema_info  = f"EMA21={ema21_1h:.6f}" if ema21_1h else "EMA21=N/A"
        vwap_info = f"VWAP={vwap_1h:.6f}"   if vwap_1h  else "VWAP=N/A"
        return "NEUTRAL", reasons + [
            f"✗ R2: Sin pullback a zona ({ema_info} | {vwap_info} | zona±{zone_dist:.6f})"
        ]

    # FIX 2: verificar que el retroceso es real
    if level_used and not _verify_real_pullback(bars_1h, direction, level_used):
        return "NEUTRAL", reasons + [
            f"✗ R2: Precio no vino del lado correcto del nivel ({zone_ref}) en últimas {_PULLBACK_REVERT_BARS} velas 1h"
        ]

    reasons.append(f"✓ R2: Pullback real a {zone_ref}")

    # ── R3: Vela de confirmación 15m ─────────────────────────────────────────
    if not ind_15m or len(bars_15m) < 2:
        return "NEUTRAL", reasons + ["Sin datos 15m"]

    last_bar      = bars_15m[-1]
    open_15m      = float(last_bar[1])
    close_15m     = float(last_bar[4])
    vol_ratio_15m = ind_15m.get("vol_ratio", 1.0)

    candle_bull = close_15m > open_15m
    candle_bear = close_15m < open_15m
    vol_ok      = vol_ratio_15m >= _VOL_CONFIRM_MIN

    confirm_ok = (
        (direction == "LONG"  and candle_bull and vol_ok) or
        (direction == "SHORT" and candle_bear and vol_ok)
    )

    if not confirm_ok:
        dir_candle = "alcista" if candle_bull else "bajista" if candle_bear else "doji"
        return "NEUTRAL", reasons + [
            f"✗ R3: Sin confirmación 15m — vela {dir_candle}, vol={vol_ratio_15m:.2f}x (min {_VOL_CONFIRM_MIN}x)"
        ]

    reasons.append(
        f"✓ R3: Vela {'alcista' if direction == 'LONG' else 'bajista'} 15m vol={vol_ratio_15m:.2f}x"
    )

    return direction, reasons


# ─── SL por estructura ────────────────────────────────────────────────────────

def _structure_sl(
    bars_1h:  list,
    direction: str,
    entry:    float,
    sl_fallback: float,
) -> float:
    if len(bars_1h) < _SL_STRUCT_BARS + 1:
        return sl_fallback
    recent = bars_1h[-(_SL_STRUCT_BARS + 1):-1]
    try:
        if direction == "LONG":
            level     = min(float(b[3]) for b in recent)
            candidate = round(level * 0.9995, 6)
        else:
            level     = max(float(b[2]) for b in recent)
            candidate = round(level * 1.0005, 6)

        if entry > 0:
            dist = abs(entry - candidate) / entry
            if dist > _SL_STRUCTURE_MAX_DIST:
                log.debug(
                    "[signal_engine] SL estructura cap (%.2f%% > %.2f%%) → fallback",
                    dist * 100, _SL_STRUCTURE_MAX_DIST * 100,
                )
                return sl_fallback
        return candidate
    except Exception as e:
        log.debug("[signal_engine] _structure_sl error: %s", e)
        return sl_fallback


# ─── Función principal ────────────────────────────────────────────────────────

async def analyze_pair(
    exch,
    symbol:       str,
    ohlcv_fn:     Optional[Callable] = None,
    funding_rate: float = 0.0,
) -> SignalResult:

    # Fetch OHLCV
    try:
        if ohlcv_fn is not None:
            bars_15m, bars_1h, bars_4h = await asyncio.gather(
                ohlcv_fn("15m"), ohlcv_fn("1h"), ohlcv_fn("4h"),
            )
        else:
            bars_15m, bars_1h, bars_4h = await asyncio.gather(
                _fetch_bars(exch, symbol, "15m", _BARS_NEEDED),
                _fetch_bars(exch, symbol, "1h",  _BARS_NEEDED),
                _fetch_bars(exch, symbol, "4h",  max(60, _TP_STRUCT_BARS_4H + 10)),
            )
    except Exception as e:
        log.error("[signal_engine] OHLCV fetch error %s: %s", symbol, e)
        return _hold_result(symbol, f"OHLCV error: {e}")

    bars_15m = _clean_bars(bars_15m)
    bars_1h  = _clean_bars(bars_1h)
    bars_4h  = _clean_bars(bars_4h)

    if len(bars_15m) < 30:
        return _hold_result(symbol, f"Insuficientes velas 15m ({len(bars_15m)})")
    if len(bars_4h) < 30:
        return _hold_result(symbol, f"Insuficientes velas 4h ({len(bars_4h)})")
    if len(bars_1h) < 20:
        log.warning("[signal_engine] %s 1h incompleto (%d velas)", symbol, len(bars_1h))

    ind_15m = _compute_indicators(bars_15m)
    ind_1h  = _compute_indicators(bars_1h) if len(bars_1h) >= 20 else {}
    ind_4h  = _compute_indicators(bars_4h) if len(bars_4h) >= 30 else {}

    # Filtro volumen global
    vol_ratio = ind_15m.get("vol_ratio", 1.0)
    if vol_ratio < _VOL_MIN_GLOBAL:
        return _hold_result(symbol, f"Mercado dormido vol={vol_ratio:.2f}x (min {_VOL_MIN_GLOBAL}x)")

    # Filtro RSI sobreextensión 15m
    closes_15m = [float(b[4]) for b in bars_15m]
    rsi_15m    = _rsi_last(closes_15m)
    if rsi_15m is not None:
        if rsi_15m > _RSI_OB:
            return _hold_result(symbol, f"RSI15m={rsi_15m:.0f} sobrecompra — esperar pullback")
        if rsi_15m < _RSI_OS:
            return _hold_result(symbol, f"RSI15m={rsi_15m:.0f} sobreventa — esperar rebote")

    # Evaluar las 3 reglas
    direction, reasons = _evaluate_signal(ind_4h, ind_1h, ind_15m, bars_1h, bars_15m)

    if direction == "NEUTRAL":
        return _hold_result(symbol, " | ".join(reasons))

    # Filtro funding
    if direction == "LONG"  and funding_rate > _FUNDING_LONG_MAX:
        return _hold_result(symbol, f"Funding {funding_rate:.4%} > {_FUNDING_LONG_MAX:.4%} → no LONG")
    if direction == "SHORT" and funding_rate < _FUNDING_SHORT_MIN:
        return _hold_result(symbol, f"Funding {funding_rate:.4%} < {_FUNDING_SHORT_MIN:.4%} → no SHORT")

    # Niveles
    entry   = ind_15m.get("close", 0.0)
    atr_val = ind_15m.get("atr") or 0.0

    if entry <= 0 or atr_val <= 0:
        return _hold_result(symbol, "Entry o ATR inválido")

    sl_fallback = (
        round(entry - _SL_ATR_MULT * atr_val, 6) if direction == "LONG"
        else round(entry + _SL_ATR_MULT * atr_val, 6)
    )
    sl = _structure_sl(bars_1h, direction, entry, sl_fallback)

    risk = abs(entry - sl)
    if risk <= 0:
        return _hold_result(symbol, "SL coincide con entry")

    # TP raw
    tp_raw = (
        round(entry + risk * _TP_RR_MULT, 6) if direction == "LONG"
        else round(entry - risk * _TP_RR_MULT, 6)
    )

    # FIX 3: ajustar TP por estructura 4h
    tp, rr = _adjust_tp_for_structure(bars_4h, direction, entry, tp_raw, sl)

    if rr < MIN_RR:
        return _hold_result(
            symbol,
            f"TP recortado por estructura 4h → RR={rr:.2f} < mínimo {MIN_RR} "
            f"(tp_raw={tp_raw:.6f} → tp_adj={tp:.6f})"
        )

    suggested_lev = max(1, int(_MAX_LEV * 0.5))

    log.info(
        "[signal_engine] %s %s score=3/3 RR=%.2f entry=%.6f sl=%.6f tp=%.6f "
        "atr=%.6f lev=%dx funding=%.4f%% | %s",
        symbol, direction, rr, entry, sl, tp, atr_val,
        suggested_lev, funding_rate * 100, " · ".join(reasons),
    )

    return SignalResult(
        symbol=symbol,
        signal=direction,
        entry_mode="NORMAL",
        score=3,
        max_score=3,
        entry=entry,
        sl=sl,
        tp1=tp,
        tp2=tp,
        atr=atr_val,
        rr=rr,
        suggested_lev=suggested_lev,
        indicators={"15m": ind_15m, "1h": ind_1h, "4h": ind_4h},
        is_valid=True,
        reason="",
        extra={
            "setup_type":   "MOMENTUM",
            "sl_atr":       sl_fallback,
            "sl_used":      sl,
            "tp_raw":       tp_raw,
            "tp_adjusted":  tp != tp_raw,
            "funding_rate": funding_rate,
            "rsi_15m":      rsi_15m,
        },
    )


# ─── Fetch interno ────────────────────────────────────────────────────────────

async def _fetch_bars(exch, symbol: str, timeframe: str, limit: int) -> list:
    ccxt_sym = _to_ccxt_symbol(symbol)
    try:
        bars = await exch.fetch_ohlcv(ccxt_sym, timeframe=timeframe, limit=limit)
        return bars or []
    except Exception as e:
        log.warning("[signal_engine] fetch_ohlcv(%s, %s) error: %s", ccxt_sym, timeframe, e)
        return []


# ─── Hold result ──────────────────────────────────────────────────────────────

def _hold_result(symbol: str, reason: str) -> SignalResult:
    return SignalResult(
        symbol=symbol, signal="NEUTRAL", entry_mode="HOLD",
        score=0, max_score=3, entry=0.0, sl=0.0, tp1=0.0, tp2=0.0,
        atr=0.0, rr=0.0, suggested_lev=1, indicators={},
        is_valid=False, reason=reason,
    )


# ─── Formato señal Telegram ───────────────────────────────────────────────────

def format_signal_block(signal) -> str:
    if signal is None:
        return ""
    arrow = "\U0001f7e2 LONG" if signal.signal == "LONG" else "\U0001f534 SHORT" if signal.signal == "SHORT" else "⚪ NEUTRAL"
    lev   = f"{signal.suggested_lev}x" if signal.suggested_lev else "—"
    rr    = f"{signal.rr:.2f}"         if signal.rr            else "—"
    tp_adj = " ⚠️adj" if signal.extra.get("tp_adjusted") else ""
    lines = [
        f"**{signal.symbol}** · {arrow} [MOMENTUM]",
        f"Score: `3/3` · Lev: `{lev}` · R/R: `{rr}`{tp_adj}",
    ]
    if signal.entry:
        lines.append(f"Entry: `{signal.entry}` | SL: `{signal.sl}` | TP: `{signal.tp1}`")
    if signal.reason:
        lines.append(f"_{signal.reason}_")
    return "\n".join(lines)


# ─── Compatibilidad con imports externos ─────────────────────────────────────
MIN_SCORE: int = 1


# ─── Alias público: get_signal → analyze_pair ────────────────────────────────
# main.py importa `get_signal`; este alias evita el ImportError sin cambiar
# la lógica existente de analyze_pair.

get_signal = analyze_pair


# ─── SignalFlipGuard ──────────────────────────────────────────────────────────

class SignalFlipGuard:
    def __init__(self, cooldown_s: float = _FLIP_COOLDOWN_S):
        self._cooldown = cooldown_s
        self._last: Dict[str, Tuple[str, float]] = {}

    def allow(self, symbol: str, signal) -> bool:
        if self._cooldown <= 0 or signal is None:
            return True
        side = getattr(signal, "side", None) or getattr(signal, "signal", None)
        if not side:
            if isinstance(signal, str) and signal.upper() in ("LONG", "SHORT", "BUY", "SELL"):
                side = signal
            else:
                return True
        side_norm = "long" if str(side).upper() in ("LONG", "BUY") else "short"
        last = self._last.get(symbol)
        if last is not None:
            last_side, last_ts = last
            elapsed = time.monotonic() - last_ts
            if last_side != side_norm and elapsed < self._cooldown:
                log.warning("[SignalFlipGuard] %s: señal %s BLOQUEADA (flip en %.1fs)", symbol, side_norm, elapsed)
                return False
        self._last[symbol] = (side_norm, time.monotonic())
        return True

    def reset(self, symbol: str) -> None:
        self._last.pop(symbol, None)

    def update(self, symbol: str, side: str) -> None:
        side_norm = "long" if str(side).upper() in ("LONG", "BUY") else "short"
        self._last[symbol] = (side_norm, time.monotonic())


signal_flip_guard = SignalFlipGuard()


# ─── ManualCloseCooldown ──────────────────────────────────────────────────────

class ManualCloseCooldown:
    def __init__(self, cooldown_s: int = _MANUAL_CLOSE_CD_S):
        self._cooldown = cooldown_s
        self._closed: Dict[str, float] = {}

    def register(self, symbol: str) -> None:
        self._closed[symbol] = time.monotonic()
        log.info("[ManualCloseCooldown] %s: cooldown manual activado (%ds)", symbol, self._cooldown)

    def is_blocked(self, symbol: str) -> bool:
        ts = self._closed.get(symbol)
        if ts is None:
            return False
        elapsed = time.monotonic() - ts
        if elapsed < self._cooldown:
            log.debug("[ManualCloseCooldown] %s: bloqueado — %ds restantes", symbol, int(self._cooldown - elapsed))
            return True
        del self._closed[symbol]
        return False

    def clear(self, symbol: str) -> None:
        self._closed.pop(symbol, None)


manual_close_cooldown = ManualCloseCooldown()
