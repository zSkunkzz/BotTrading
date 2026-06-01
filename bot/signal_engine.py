# -*- coding: utf-8 -*-
"""
signal_engine.py — Motor de señales de trading.

Exporta:
  - SignalResult          : dataclass con el resultado de analyze_pair()
  - analyze_pair()        : analiza un par y devuelve SignalResult
  - format_signal_block() : formatea SignalResult como bloque Markdown
  - MIN_SCORE             : puntuación mínima para señal válida (env: MIN_SIGNAL_SCORE)
  - MIN_RR                : ratio R/R mínimo (env: MIN_RR_REQUIRED)
  - SignalFlipGuard       : previene flip-flop de señales opuestas (BUG #7 fix)
  - signal_flip_guard     : singleton exportado de SignalFlipGuard

BUG #10 FIX: analyze_pair importaba build_snapshot y compute_indicators que
nunca existieron en market_snapshot.py / indicators.py. Se reemplaza por
bot.strategy.decide() que es la función real que ya usa trader.py.
"""
from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

log = logging.getLogger(__name__)

# ─── Constantes exportadas ───────────────────────────────────────────────────────────────────────────────

MIN_SCORE: int = int(os.getenv("MIN_SIGNAL_SCORE", "6"))
MIN_RR: float = float(os.getenv("MIN_RR_REQUIRED", "1.8"))

# ─── SignalResult ─────────────────────────────────────────────────────────────────────────────────────

@dataclass
class SignalResult:
    """Resultado completo de analyze_pair()."""
    symbol: str
    signal: str                          # "LONG" | "SHORT" | "NEUTRAL"
    entry_mode: str                      # "STRONG" | "NORMAL" | "EARLY" | "HOLD"
    score: int
    max_score: int
    entry: float
    sl: float
    tp1: float
    tp2: float
    atr: float
    rr: float
    suggested_lev: int
    indicators: Dict                     # indicadores por timeframe
    is_valid: bool = True
    reason: str = ""
    # campos opcionales para compatibilidad
    signal_block: str = ""
    extra: Dict = field(default_factory=dict)


# ─── analyze_pair ───────────────────────────────────────────────────────────────────────────────────

async def analyze_pair(exch, symbol: str) -> SignalResult:
    """
    Analiza un par y devuelve un SignalResult.

    BUG #10 FIX: usa bot.strategy.decide() — la misma función que trader.py —
    en vez de build_snapshot/compute_indicators que no existían.
    """
    try:
        from bot.strategy import decide
        from bot.ai_trader import ai_decide
    except ImportError as e:
        log.error("[signal_engine] import error en analyze_pair: %s", e)
        return _hold_result(symbol, f"ImportError: {e}")

    try:
        decision = await decide(
            exch=exch,
            symbol=symbol,
            ai_decide_fn=ai_decide,
            has_open_position=False,
            current_pnl=None,
        )
    except Exception as e:
        log.error("[signal_engine] decide(%s) falló: %s", symbol, e)
        return _hold_result(symbol, f"decide error: {e}")

    action = decision.get("action", "HOLD")
    sig    = decision.get("signal")

    if action not in ("BUY", "SELL") or sig is None:
        return _hold_result(symbol, "HOLD")

    signal_str = "LONG" if action == "BUY" else "SHORT"
    score      = int(getattr(sig, "score", 0))
    max_score  = int(getattr(sig, "max_score", 10))
    entry      = float(getattr(sig, "entry", 0) or 0)
    sl         = float(getattr(sig, "sl", 0) or 0)
    tp1        = float(getattr(sig, "tp1", 0) or 0)
    tp2        = float(getattr(sig, "tp2", 0) or 0)
    atr_val    = float(getattr(sig, "atr", 0) or 0)
    lev        = int(getattr(sig, "suggested_lev", 1) or 1)
    entry_mode = str(getattr(sig, "entry_mode", "") or "NORMAL")

    # R/R estimado
    if entry > 0 and sl > 0 and tp1 > 0:
        risk   = abs(entry - sl)
        reward = abs(tp1 - entry)
        rr     = round(reward / risk, 2) if risk > 0 else 0.0
    else:
        rr = 0.0

    is_valid = score >= MIN_SCORE and rr >= MIN_RR

    return SignalResult(
        symbol=symbol,
        signal=signal_str,
        entry_mode=entry_mode,
        score=score,
        max_score=max_score,
        entry=entry,
        sl=sl,
        tp1=tp1,
        tp2=tp2,
        atr=atr_val,
        rr=rr,
        suggested_lev=lev,
        indicators={},
        is_valid=is_valid,
        reason="" if is_valid else f"score={score} rr={rr:.2f}",
    )


def _hold_result(symbol: str, reason: str) -> SignalResult:
    """Devuelve un SignalResult de HOLD seguro cuando hay error."""
    return SignalResult(
        symbol=symbol,
        signal="NEUTRAL",
        entry_mode="HOLD",
        score=0,
        max_score=10,
        entry=0.0,
        sl=0.0,
        tp1=0.0,
        tp2=0.0,
        atr=0.0,
        rr=0.0,
        suggested_lev=1,
        indicators={},
        is_valid=False,
        reason=reason,
    )


# ─── format_signal_block ─────────────────────────────────────────────────────────────────────────────────

def format_signal_block(signal: Optional[SignalResult]) -> str:
    """Formatea un SignalResult como bloque Markdown para Telegram/logs."""
    if signal is None:
        return ""

    arrow = "🟢 LONG" if signal.signal == "LONG" else "🔴 SHORT" if signal.signal == "SHORT" else "⚪ NEUTRAL"
    lev = f"{signal.suggested_lev}x" if signal.suggested_lev else "—"
    rr = f"{signal.rr:.2f}" if signal.rr else "—"

    lines = [
        f"**{signal.symbol}** · {arrow}",
        f"Score: `{signal.score}/{signal.max_score}` · Mode: `{signal.entry_mode}` · Lev: `{lev}` · R/R: `{rr}`",
    ]

    if signal.entry:
        lines.append(f"Entry: `{signal.entry}` | SL: `{signal.sl}` | TP1: `{signal.tp1}` | TP2: `{signal.tp2}`")

    if signal.reason:
        lines.append(f"_{signal.reason}_")

    return "\n".join(lines)


# ─── SignalFlipGuard (BUG #7 FIX) ────────────────────────────────────────────────────────────────────────

_FLIP_COOLDOWN_S = float(os.getenv("SIGNAL_FLIP_COOLDOWN_S", "120"))


class SignalFlipGuard:
    """
    BUG #7 FIX: Previene flip-flop de señales opuestas en ventana corta.

    Uso:
        guard = SignalFlipGuard()
        signal = decide(...)   # devuelve objeto con .side o None
        if guard.allow(symbol, signal):
            # procesar señal
        else:
            # señal bloqueada por cooldown
    """

    def __init__(self, cooldown_s: float = _FLIP_COOLDOWN_S):
        self._cooldown = cooldown_s
        # symbol -> (side: str, ts: float)
        self._last: Dict[str, Tuple[str, float]] = {}

    def allow(self, symbol: str, signal) -> bool:
        """
        Devuelve True si la señal debe procesarse, False si debe bloquearse.
        """
        if self._cooldown <= 0:
            return True
        if signal is None:
            return True

        side = getattr(signal, "side", None)
        if not side:
            if isinstance(signal, str) and signal in ("long", "short", "buy", "sell"):
                side = signal
            else:
                return True

        side_norm = "long" if side in ("long", "buy") else "short"

        last = self._last.get(symbol)
        if last is not None:
            last_side, last_ts = last
            elapsed = time.monotonic() - last_ts
            if last_side != side_norm and elapsed < self._cooldown:
                log.warning(
                    "[SignalFlipGuard] %s: señal %s BLOQUEADA — inversión de %s a %s "
                    "en %.1fs (cooldown=%.0fs). Evitando flip-flop.",
                    symbol, side_norm, last_side, side_norm,
                    elapsed, self._cooldown,
                )
                return False

        self._last[symbol] = (side_norm, time.monotonic())
        return True

    def reset(self, symbol: str) -> None:
        """Limpiar el registro de un símbolo (llamar tras cierre de posición)."""
        self._last.pop(symbol, None)

    def update(self, symbol: str, side: str) -> None:
        """Actualizar manualmente el último side sin pasar por allow()."""
        side_norm = "long" if side in ("long", "buy") else "short"
        self._last[symbol] = (side_norm, time.monotonic())


# Singleton exportado
signal_flip_guard = SignalFlipGuard()
