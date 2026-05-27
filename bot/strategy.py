#!/usr/bin/env python3
"""
strategy.py — Lógica de decisión de BotTrading

Flujo de decisión:
  1. signal_engine analiza el par técnicamente (multi-TF, scoring /10)
  2. Si score < MIN_SIGNAL_SCORE  → devuelve HOLD sin llamar a la IA
  3. Si score >= MIN_SIGNAL_SCORE → confirma con IA (1 sola llamada)
  4. La IA recibe el contexto completo + niveles de signal_engine

Variables de entorno opcionales:
  MIN_SIGNAL_SCORE  (default: 6)  — score mínimo para activar IA
  MIN_RR_REQUIRED   (default: 1.8) — R/R mínimo para activar IA
"""

import logging
import os
from typing import Optional

from bot.signal_engine import (
    SignalResult,
    analyze_pair,
    format_signal_block,
    MIN_SCORE,
    MIN_RR,
)

log = logging.getLogger(__name__)

MIN_SIGNAL_SCORE = int(os.getenv("MIN_SIGNAL_SCORE", str(MIN_SCORE)))
MIN_RR_REQUIRED  = float(os.getenv("MIN_RR_REQUIRED", str(MIN_RR)))


async def decide(
    exch,
    symbol: str,
    ai_decide_fn,          # función async (symbol, context_dict) -> "BUY"|"SELL"|"HOLD"
    has_open_position: bool = False,
    current_pnl: Optional[float] = None,
) -> dict:
    """
    Punto de entrada principal para la estrategia.

    Retorna un dict con:
        action      : "BUY" | "SELL" | "HOLD"
        signal      : SignalResult (siempre presente)
        ai_used     : bool — si se llamó a la IA
        reason      : str  — motivo legible
        signal_block: str  — bloque Markdown para Telegram
    """

    # ── 1. Si hay posición abierta, no abrimos otra ─────────────────────────
    if has_open_position:
        return _result("HOLD", None, False, "Posición ya abierta — esperando cierre")

    # ── 2. Análisis técnico async ────────────────────────────────────────────
    try:
        signal: SignalResult = await analyze_pair(exch, symbol)
    except Exception as e:
        log.error(f"[strategy] analyze_pair error: {e}")
        return _result("HOLD", None, False, f"Error en análisis técnico: {e}")

    log.info(f"[strategy] {symbol} · score={signal.score}/10 · {signal.signal} · RR={signal.rr}")

    # ── 3. Filtro de score y R/R ─────────────────────────────────────────────
    if signal.score < MIN_SIGNAL_SCORE:
        return _result(
            "HOLD", signal, False,
            f"Score insuficiente ({signal.score}/10 < {MIN_SIGNAL_SCORE}) — sin señal"
        )

    if signal.rr < MIN_RR_REQUIRED:
        return _result(
            "HOLD", signal, False,
            f"R/R insuficiente ({signal.rr:.1f} < {MIN_RR_REQUIRED}) — no merece la pena"
        )

    if signal.signal == "NEUTRAL":
        return _result("HOLD", signal, False, "Señal técnica neutral — sin consenso")

    # ── 4. Llamada a la IA con contexto enriquecido ─────────────────────────
    i15 = signal.indicators.get("15m", {})
    i1h = signal.indicators.get("1h",  {})
    i4h = signal.indicators.get("4h",  {})

    context = {
        "symbol":        symbol,
        "signal":        signal.signal,
        "score":         signal.score,
        "rr":            signal.rr,
        "entry":         signal.entry,
        "sl":            signal.sl,
        "tp1":           signal.tp1,
        "tp2":           signal.tp2,
        "atr":           signal.atr,
        "suggested_lev": signal.suggested_lev,
        "ema_4h":        i4h.get("ema_trend", 0),
        "macd_4h":       i4h.get("macd", 0),
        "ema_1h":        i1h.get("ema_trend", 0),
        "rsi_1h":        i1h.get("rsi_val", 50),
        "supertrend_1h": i1h.get("supertrend", 0),
        "ema_15m":       i15.get("ema_trend", 0),
        "macd_15m":      i15.get("macd", 0),
        "stoch_15m":     i15.get("stoch", 0),
        "volume_15m":    i15.get("volume", 0),
        "vol_ratio":     i15.get("vol_ratio", 1.0),
        "rsi_15m":       i15.get("rsi_val", 50),
    }

    try:
        ai_action = await ai_decide_fn(symbol, context)
    except Exception as e:
        log.warning(f"[strategy] IA falló, usando señal técnica directa: {e}")
        ai_action = "BUY" if signal.signal == "LONG" else "SELL"

    action = ai_action.upper().strip()
    if action not in ("BUY", "SELL", "HOLD"):
        action = "HOLD"

    return _result(action, signal, True, f"IA confirmó {action} · técnico {signal.signal} {signal.score}/10")


def _result(action: str, signal: Optional[SignalResult], ai_used: bool, reason: str) -> dict:
    return {
        "action":       action,
        "signal":       signal,
        "ai_used":      ai_used,
        "reason":       reason,
        "signal_block": format_signal_block(signal) if signal else "",
    }
