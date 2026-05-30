#!/usr/bin/env python3
"""
strategy.py — Lógica de decisión de BotTrading

Flujo y coste IA:
  NONE   (score<5)   → HOLD directo, sin IA
  EARLY  (score 5-6) → HOLD directo, sin IA
  NORMAL (score 7)   → confirma con IA (score suficiente desde AI_CALL_MIN_SCORE=7)
  NORMAL (score >=8) → confirma con IA; si IA dice HOLD y score>=8 → override técnico
  STRONG (score >=8) → entra directo, sin IA (máxima confluencia)

Variables de entorno:
  MIN_SIGNAL_SCORE   (default: 5)    — mínimo para activar cualquier modo
  MIN_RR_REQUIRED    (default: 1.8)
  SKIP_AI_ON_STRONG  (default: true) — omite IA cuando modo=STRONG
  AI_CALL_MIN_SCORE  (default: 7)    — score mínimo para llamar a la IA (era 8)
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

MIN_SIGNAL_SCORE  = int(os.getenv("MIN_SIGNAL_SCORE",  str(MIN_SCORE)))
MIN_RR_REQUIRED   = float(os.getenv("MIN_RR_REQUIRED", str(MIN_RR)))
SKIP_AI_ON_STRONG = os.getenv("SKIP_AI_ON_STRONG", "true").lower() != "false"
# FIX: bajado de 8 a 7 — score 7 ahora pasa a la IA en vez de ir a HOLD directo
AI_CALL_MIN_SCORE = int(os.getenv("AI_CALL_MIN_SCORE", "7"))
# FIX: score mínimo para override técnico si la IA dice HOLD
AI_HOLD_OVERRIDE_SCORE = int(os.getenv("AI_HOLD_OVERRIDE_SCORE", "8"))


async def decide(
    exch,
    symbol: str,
    ai_decide_fn,
    has_open_position: bool = False,
    current_pnl: Optional[float] = None,
) -> dict:
    """
    Retorna:
        action      : "BUY" | "SELL" | "HOLD"
        signal      : SignalResult
        ai_used     : bool
        reason      : str
        signal_block: str (Markdown)
        ai_confidence: int (0 if IA not used)
        ai_reason   : str
    """

    if has_open_position:
        return _result("HOLD", None, False, "Posición ya abierta — esperando cierre")

    try:
        signal: SignalResult = await analyze_pair(exch, symbol)
    except Exception as e:
        log.error(f"[strategy] analyze_pair error: {e}")
        return _result("HOLD", None, False, f"Error en análisis técnico: {e}")

    log.info(
        f"[strategy] {symbol} · score={signal.score}/10 · mode={signal.entry_mode} "
        f"· {signal.signal} · RR={signal.rr} · lev={signal.suggested_lev}x"
    )

    if not signal.is_valid:
        return _result(
            "HOLD", signal, False,
            f"Sin modo de entrada válido (score={signal.score}/10, mode={signal.entry_mode})"
        )

    if signal.rr < MIN_RR_REQUIRED:
        return _result(
            "HOLD", signal, False,
            f"R/R insuficiente ({signal.rr:.1f} < {MIN_RR_REQUIRED})"
        )

    if signal.signal == "NEUTRAL":
        return _result("HOLD", signal, False, "Señal técnica neutral")

    # STRONG: confluencia máxima → entra directo sin IA
    if signal.entry_mode == "STRONG" and SKIP_AI_ON_STRONG:
        action = "BUY" if signal.signal == "LONG" else "SELL"
        return _result(
            action, signal, False,
            f"💥 STRONG entry directo · score={signal.score}/10 · lev={signal.suggested_lev}x"
        )

    # EARLY: score bajo (5-6), no justifica llamada a IA
    if signal.entry_mode == "EARLY":
        return _result(
            "HOLD", signal, False,
            f"⏭️ EARLY score={signal.score}/10 → HOLD sin IA"
        )

    # NORMAL con score bajo: sin IA
    if signal.score < AI_CALL_MIN_SCORE:
        return _result(
            "HOLD", signal, False,
            f"⏭️ NORMAL score={signal.score}/10 < {AI_CALL_MIN_SCORE} → HOLD sin IA"
        )

    # NORMAL con score >= AI_CALL_MIN_SCORE: confirmar con IA
    i15 = signal.indicators.get("15m", {})
    i1h = signal.indicators.get("1h",  {})
    i4h = signal.indicators.get("4h",  {})

    context_override = {
        "symbol":        symbol,
        "signal":        signal.signal,
        "entry_mode":    signal.entry_mode,
        "score":         signal.score,
        "rr":            signal.rr,
        "entry":         signal.entry,
        "sl":            signal.sl,
        "tp1":           signal.tp1,
        "tp2":           signal.tp2,
        "atr":           signal.atr,
        "suggested_lev": signal.suggested_lev,
        "size_ratio":    signal.size_ratio,
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

    log.info(f"[strategy] {symbol} 🤖 Consultando IA (score={signal.score}/10, mode=NORMAL)")

    try:
        # FIX: bars=[] es intencional aquí — ai_decide usa context_override y no accede a bars
        # cuando context_override está presente. El guard en ai_decide protege el path else.
        ai_result = await ai_decide_fn(
            symbol,
            [],          # bars: no disponibles aquí, context_override tiene todo
            None,        # position
            None,        # entry_price
            signal.suggested_lev,   # FIX: usar el leverage real de la señal en vez de 1
            context_override=context_override,
        )
    except Exception as e:
        log.warning(f"[strategy] IA falló, usando señal técnica directa: {e}")
        ai_result = {
            "action":     "BUY" if signal.signal == "LONG" else "SELL",
            "confidence": 7,
            "reason":     "Fallback técnico",
        }

    action     = str(ai_result.get("action", "HOLD")).upper().strip()
    confidence = ai_result.get("confidence", 0)
    ai_reason  = ai_result.get("reason", ai_result.get("reasoning", ""))

    if action not in ("BUY", "SELL", "HOLD", "CLOSE"):
        action = "HOLD"

    # FIX: si la IA dice HOLD pero el score técnico es muy alto (>=AI_HOLD_OVERRIDE_SCORE),
    # la señal técnica es suficientemente fuerte para entrar sin confirmación IA.
    # Esto evita que la IA bloquee setups STRONG/NORMAL de alta calidad.
    if action == "HOLD" and signal.score >= AI_HOLD_OVERRIDE_SCORE:
        override_action = "BUY" if signal.signal == "LONG" else "SELL"
        log.info(
            f"[strategy] {symbol} 🔁 IA→HOLD pero score={signal.score}>={AI_HOLD_OVERRIDE_SCORE} "
            f"→ override técnico {override_action}"
        )
        return _result(
            override_action, signal, True,
            f"🔁 Override técnico (score={signal.score}/{AI_HOLD_OVERRIDE_SCORE}) · IA dudó pero señal fuerte · "
            f"lev {signal.suggested_lev}x | {ai_reason}",
            ai_confidence=confidence,
            ai_reason=ai_reason,
        )

    return _result(
        action, signal, True,
        f"IA confirmó {action} ({confidence}/10) · modo {signal.entry_mode} · "
        f"score {signal.score}/10 · lev {signal.suggested_lev}x | {ai_reason}",
        ai_confidence=confidence,
        ai_reason=ai_reason,
    )


def _result(
    action: str,
    signal: Optional[SignalResult],
    ai_used: bool,
    reason: str,
    ai_confidence: int = 0,
    ai_reason: str = "",
) -> dict:
    return {
        "action":        action,
        "signal":        signal,
        "ai_used":       ai_used,
        "reason":        reason,
        "signal_block":  format_signal_block(signal) if signal else "",
        "ai_confidence": ai_confidence,
        "ai_reason":     ai_reason,
    }
