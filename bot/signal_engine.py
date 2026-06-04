# -*- coding: utf-8 -*-
"""
signal_engine.py — Motor de señales v23: Momentum puro con pullback.

EXPORTA:
  SignalResult, analyze_pair, format_signal_block,
  MIN_RR, SignalFlipGuard, signal_flip_guard,
  ManualCloseCooldown, manual_close_cooldown

ARQUITECTURA:
  signal_engine → lógica técnica pura (OHLCV + indicadores, SIN imports de strategy)
  strategy      → orquesta: llama analyze_pair
  trader        → ejecuta órdenes usando strategy.decide()

ESTRATEGIA v23 — Momentum puro con pullback (3 reglas binarias):

  REGLA 1 — Tendencia 4h no negociable:
    EMA21 > EMA50 en 4h con separación mínima → solo LONG.
    EMA21 < EMA50 en 4h con separación mínima → solo SHORT.
    Sin separación clara → NEUTRAL, no opera.

  REGLA 2 — Pullback a zona de valor en 1h:
    LONG: precio retrocede a zona EMA21_1h o VWAP (tolerancia PULLBACK_ZONE_PCT).
    SHORT: precio sube a zona EMA21_1h o VWAP.
    Sin pullback → NEUTRAL.

  REGLA 3 — Vela de confirmación en 15m:
    Una vela que cierre en dirección de la tendencia con volumen >= VOL_CONFIRM_MIN.
    Sin vela de confirmación → NEUTRAL.

  SL: mínimo/máximo de las últimas SL_STRUCT_BARS velas 1h.
      Cap máximo SL_STRUCTURE_MAX_DIST_PCT (4%) para evitar SLs absurdos.
      Fallback: entry ± SL_ATR_MULT × ATR_15m.

  TP: entry ± TP_RR_MULT × riesgo (R:R fijo sobre el SL real).
      Si RR calculado < MIN_RR → NEUTRAL.

FILTROS EXTRA (se mantienen de versiones anteriores):
  - Funding rate extremo bloquea la dirección correspondiente.
  - Volumen mínimo global (mercado dormido → NEUTRAL).
  - SignalFlipGuard: cooldown entre flips de dirección.
  - ManualCloseCooldown: cooldown tras cierre manual.
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
_EMA_SPREAD_MIN        = float(os.getenv("EMA_SPREAD_MIN",        "0.003"))  # 0.3% separación mínima EMA 4h

# Pullback zona de valor 1h
_PULLBACK_ZONE_PCT     = float(os.getenv("PULLBACK_ZONE_PCT",     "0.008"))  # 0.8% tolerancia al EMA/VWAP

# Confirmación 15m
_VOL_CONFIRM_MIN       = float(os.getenv("VOL_CONFIRM_MIN",       "1.2"))    # vol vela confirmación ≥ 1.2× media
_VOL_AVG_WINDOW        = int(os.getenv("VOL_AVG_WINDOW",          "20"))
_VOL_MIN_GLOBAL        = float(os.getenv("VOL_MIN_GLOBAL",         "0.6"))   # mercado dormido

# SL / TP
_SL_STRUCT_BARS        = int(os.getenv("SL_STRUCT_BARS",           "3"))     # velas 1h para SL estructura
_SL_STRUCTURE_MAX_DIST = float(os.getenv("SL_STRUCTURE_MAX_DIST_PCT", "4.0")) / 100.0
_SL_ATR_MULT           = float(os.getenv("SL_ATR_MULT",            "1.5"))   # fallback SL ATR
_TP_RR_MULT            = float(os.getenv("TP_RR_MULT",             "2.5"))   # TP = entry ± riesgo × 2.5

# Funding
_FUNDING_LONG_MAX      = float(os.getenv("FUNDING_LONG_MAX",       "0.0005"))
_FUNDING_SHORT_MIN     = float(os.getenv("FUNDING_SHORT_MIN",      "-0.0005"))

# Apalancamiento
_MAX_LEV               = int(os.getenv("LEVERAGE", "15"))

# Barras necesarias
_BARS_NEEDED           = int(os.getenv("BARS_NEEDED", "100"))

# Cooldowns
_FLIP_COOLDOWN_S       = float(os.getenv("SIGNAL_FLIP_COOLDOWN_S",  "120"))
_MANUAL_CLOSE_CD_S     = int(os.getenv("MANUAL_CLOSE_COOLDOWN_S",   "600"))

# RSI sobreextensión — filtro duro (evita entrar en zonas extremas)
_RSI_OB               = float(os.getenv("RSI_OVERBOUGHT",  "72"))
_RSI_OS               = float(os.getenv("RSI_OVERSOLD",    "28"))


# ─── Resultado de señal ───────────────────────────────────────────────────────

@dataclass
class SignalResult:
    symbol:        str
    signal:        str          # "LONG" | "SHORT" | "NEUTRAL"
    entry_mode:    str          # "NORMAL" | "HOLD"
    score:         int          # siempre 3 si valid (las 3 reglas)
    max_score:     int          # siempre 3
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
    """Elimina barras con None en cualquier campo OHLCV."""
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

    ema21 = ema21_vals[-1] if ema21_vals else None
    ema50 = ema50_vals[-1] if ema50_vals else None

    spread = abs(ema21 - ema50) / ema50 if (ema21 and ema50 and ema50 != 0) else 0.0

    return {
        "ema21":     ema21,
        "ema50":     ema50,
        "ema_spread": spread,
        "ema_bull":  bool(ema21 and ema50 and ema21 > ema50),
        "ema_bear":  bool(ema21 and ema50 and ema21 < ema50),
        "atr":       atr14,
        "vol_ratio": vol_ratio,
        "vwap":      vwap_v,
        "close":     closes[-1],
        "high":      highs[-1],
        "low":       lows[-1],
    }


# ─── RSI simple (sin importar del módulo para evitar dependencias extra) ──────

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
    rs = avg_gain / avg_loss
    return round(100 - 100 / (1 + rs), 2)


# ─── Núcleo de la estrategia ──────────────────────────────────────────────────

def _evaluate_signal(
    ind_4h: dict,
    ind_1h: dict,
    ind_15m: dict,
    bars_1h: list,
    bars_15m: list,
) -> Tuple[str, List[str]]:
    """
    Evalúa las 3 reglas en orden. Devuelve (direction, reasons).
    direction es "LONG", "SHORT", o "NEUTRAL".
    """
    reasons: List[str] = []

    # ── REGLA 1: Tendencia 4h clara ──────────────────────────────────────────
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

    # ── REGLA 2: Pullback a zona de valor en 1h ──────────────────────────────
    if not ind_1h:
        return "NEUTRAL", reasons + ["Sin datos 1h"]

    close_1h  = ind_1h.get("close", 0.0)
    ema21_1h  = ind_1h.get("ema21")
    vwap_1h   = ind_1h.get("vwap", 0.0)

    in_zone = False
    zone_ref = None

    if ema21_1h and ema21_1h > 0:
        dist_ema = abs(close_1h - ema21_1h) / ema21_1h
        if dist_ema <= _PULLBACK_ZONE_PCT:
            in_zone = True
            zone_ref = f"EMA21_1h={ema21_1h:.4f} (dist={dist_ema*100:.2f}%)"

    if not in_zone and vwap_1h and vwap_1h > 0:
        dist_vwap = abs(close_1h - vwap_1h) / vwap_1h
        if dist_vwap <= _PULLBACK_ZONE_PCT:
            # Además verificar que el pullback es en la dirección correcta:
            # LONG: precio viene de arriba hacia VWAP (no ya por debajo)
            # SHORT: precio viene de abajo hacia VWAP
            if direction == "LONG" and close_1h >= vwap_1h * 0.995:
                in_zone = True
                zone_ref = f"VWAP_1h={vwap_1h:.4f} (dist={dist_vwap*100:.2f}%)"
            elif direction == "SHORT" and close_1h <= vwap_1h * 1.005:
                in_zone = True
                zone_ref = f"VWAP_1h={vwap_1h:.4f} (dist={dist_vwap*100:.2f}%)"

    if not in_zone:
        ema_info = f"EMA21={ema21_1h:.4f}" if ema21_1h else "EMA21=N/A"
        vwap_info = f"VWAP={vwap_1h:.4f}" if vwap_1h else "VWAP=N/A"
        return "NEUTRAL", reasons + [f"✗ R2: Sin pullback a zona ({ema_info} | {vwap_info})"]

    reasons.append(f"✓ R2: Pullback a {zone_ref}")

    # ── REGLA 3: Vela de confirmación 15m ───────────────────────────────────
    if not ind_15m or len(bars_15m) < 2:
        return "NEUTRAL", reasons + ["Sin datos 15m"]

    last_bar  = bars_15m[-1]
    open_15m  = float(last_bar[1])
    close_15m = float(last_bar[4])
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
    bars_1h: list,
    direction: str,
    entry: float,
    sl_fallback: float,
) -> float:
    """SL = min/max de las últimas SL_STRUCT_BARS velas 1h con cap de distancia."""
    if len(bars_1h) < _SL_STRUCT_BARS + 1:
        return sl_fallback
    recent = bars_1h[-(_SL_STRUCT_BARS + 1):-1]
    try:
        if direction == "LONG":
            level = min(float(b[3]) for b in recent)  # mínimos
            candidate = round(level * 0.9995, 6)       # pequeño buffer
        else:
            level = max(float(b[2]) for b in recent)  # máximos
            candidate = round(level * 1.0005, 6)

        if entry > 0:
            dist = abs(entry - candidate) / entry
            if dist > _SL_STRUCTURE_MAX_DIST:
                log.debug("[signal_engine] SL estructura cap (%.2f%% > %.2f%%) → fallback",
                          dist * 100, _SL_STRUCTURE_MAX_DIST * 100)
                return sl_fallback
        return candidate
    except Exception as e:
        log.debug("[signal_engine] _structure_sl error: %s", e)
        return sl_fallback


# ─── Función principal ────────────────────────────────────────────────────────

async def analyze_pair(
    exch,
    symbol: str,
    ohlcv_fn: Optional[Callable] = None,
    funding_rate: float = 0.0,
) -> SignalResult:
    # Fetch OHLCV
    try:
        if ohlcv_fn is not None:
            bars_15m, bars_1h, bars_4h = await asyncio.gather(
                ohlcv_fn("15m"), ohlcv_fn("1h"), ohlcv_fn("4h"),
                return_exceptions=False,
            )
        else:
            bars_15m, bars_1h, bars_4h = await asyncio.gather(
                _fetch_bars(exch, symbol, "15m", _BARS_NEEDED),
                _fetch_bars(exch, symbol, "1h",  _BARS_NEEDED),
                _fetch_bars(exch, symbol, "4h",  max(60, _BARS_NEEDED // 2)),
                return_exceptions=False,
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

    # Filtro volumen global — mercado dormido
    vol_ratio = ind_15m.get("vol_ratio", 1.0)
    if vol_ratio < _VOL_MIN_GLOBAL:
        return _hold_result(symbol, f"Mercado dormido vol={vol_ratio:.2f}x (min {_VOL_MIN_GLOBAL}x)")

    # Filtro RSI sobreextensión 15m
    closes_15m = [float(b[4]) for b in bars_15m]
    rsi_15m = _rsi_last(closes_15m)
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
    if direction == "LONG" and funding_rate > _FUNDING_LONG_MAX:
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

    reward = risk * _TP_RR_MULT
    tp = (
        round(entry + reward, 6) if direction == "LONG"
        else round(entry - reward, 6)
    )
    rr = round(reward / risk, 2)

    if rr < MIN_RR:
        return _hold_result(symbol, f"RR={rr:.2f} < mínimo {MIN_RR}")

    # Apalancamiento — conservador, sin kelly
    suggested_lev = max(1, int(_MAX_LEV * 0.5))  # 50% del máximo siempre

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
            "setup_type": "MOMENTUM",
            "sl_atr": sl_fallback,
            "sl_used": sl,
            "funding_rate": funding_rate,
            "rsi_15m": rsi_15m,
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


# ─── Formato de señal para Telegram ──────────────────────────────────────────

def format_signal_block(signal) -> str:
    if signal is None:
        return ""
    arrow = "\U0001f7e2 LONG" if signal.signal == "LONG" else "\U0001f534 SHORT" if signal.signal == "SHORT" else "⚪ NEUTRAL"
    lev   = f"{signal.suggested_lev}x" if signal.suggested_lev else "—"
    rr    = f"{signal.rr:.2f}" if signal.rr else "—"
    lines = [
        f"**{signal.symbol}** · {arrow} [MOMENTUM]",
        f"Score: `3/3` · Lev: `{lev}` · R/R: `{rr}`",
    ]
    if signal.entry:
        lines.append(f"Entry: `{signal.entry}` | SL: `{signal.sl}` | TP: `{signal.tp1}`")
    if signal.reason:
        lines.append(f"_{signal.reason}_")
    return "\n".join(lines)


# ─── MIN_SCORE — mantenido por compatibilidad con imports externos ─────────────
# En v23 no se usa internamente (lógica binaria), pero strategy.py puede importarlo.
MIN_SCORE: int = 1


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
