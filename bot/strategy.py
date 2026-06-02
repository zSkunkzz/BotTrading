#!/usr/bin/env python3
"""
strategy.py — Lógica de decisión de BotTrading

Flujo (sin IA para el caso normal):
  score < MIN_SIGNAL_SCORE  → HOLD directo
  RR < MIN_RR_REQUIRED      → HOLD directo
  NEUTRAL                   → HOLD directo
  STRONG (score >=9)        → pasa por enriched_filter (CAMBIO v2: ya no salta filtros)
  EARLY/NORMAL (score >= AI_CALL_MIN_SCORE):
    1. Aplica enriched_filter (F&G, funding, OI, RSI, vol) — sin IA, instantáneo
    2. Si el filtro bloquea → HOLD (motivo detallado)
    3. Si hay >= NEWS_AI_THRESHOLD noticias relevantes → consulta IA solo para noticias
    4. Si la IA de noticias dice HOLD con alta confianza → respeta
    5. Si no hay noticias relevantes o la IA confirma → entra

FIX 5: Fallback seguro cuando fetch_enriched_context falla:
  - enriched=None y score < MIN_SCORE+2 → HOLD (no entrar a ciegas en señal marginal)
  - enriched=None y score >= MIN_SCORE+2 → entra con WARNING explícito en el log

FIX 6: Propagar ef_penalty al decision_engine (v4.4):
  - _result() ahora acepta ef_penalty (default 0)
  - El dict devuelto incluye siempre 'ef_penalty'
  - Paso 4 (entrada tras enriched_filter OK) pasa ef_result.penalty
  - Todos los demás caminos (HOLD, fallback, sin enriched) propagan 0
  - decision_engine.evaluate() ya lee decision.get('ef_penalty', 0)
    y aplica sizing reducido según calidad de señal

FIX 7: market_regime gate en RANGING:
  - Si market_regime global está en RANGING, se eleva MIN_SIGNAL_SCORE a
    RANGING_MIN_SCORE (default: 9) y se bloquean entradas EARLY.
  - Si market_regime está en VOLATILE, se eleva a VOLATILE_MIN_SCORE (default: 8).
  - Si MARKET_REGIME_GATE=false en Railway, este bloque se salta completamente.

Variables de entorno:
  MIN_SIGNAL_SCORE             (default: 6)
  MIN_RR_REQUIRED              (default: 1.8)
  SKIP_AI_ON_STRONG            (default: false)
  AI_CALL_MIN_SCORE            (default: 6)
  AI_HOLD_OVERRIDE_SCORE       (default: 8)
  AI_HIGH_CONFIDENCE_THRESHOLD (default: 8)
  USE_AI_FOR_NEWS              (default: true)
  ENRICHED_FALLBACK_MIN_SCORE  (default: 8)  — score mínimo para entrar sin datos externos
  RANGING_MIN_SCORE            (default: 9)  — score mínimo en régimen RANGING
  VOLATILE_MIN_SCORE           (default: 8)  — score mínimo en régimen VOLATILE
  RANGING_BLOCK_EARLY          (default: true) — bloquear modo EARLY en RANGING
"""

import logging
import os
import time
from typing import Callable, Optional

from bot.signal_engine import (
    SignalResult,
    analyze_pair,
    format_signal_block,
    MIN_SCORE,
    MIN_RR,
)

log = logging.getLogger(__name__)

MIN_SIGNAL_SCORE             = int(os.getenv("MIN_SIGNAL_SCORE",             "6"))
MIN_RR_REQUIRED              = float(os.getenv("MIN_RR_REQUIRED",            "1.8"))
SKIP_AI_ON_STRONG            = os.getenv("SKIP_AI_ON_STRONG",                "false").lower() != "false"
AI_CALL_MIN_SCORE            = int(os.getenv("AI_CALL_MIN_SCORE",            "6"))
AI_HOLD_OVERRIDE_SCORE       = int(os.getenv("AI_HOLD_OVERRIDE_SCORE",       "8"))
AI_HIGH_CONFIDENCE_THRESHOLD = int(os.getenv("AI_HIGH_CONFIDENCE_THRESHOLD", "8"))
USE_AI_FOR_NEWS              = os.getenv("USE_AI_FOR_NEWS", "true").lower() != "false"

# FIX 5: score mínimo para entrar cuando fetch_enriched_context ha fallado
_ENRICHED_FALLBACK_MIN_SCORE = int(os.getenv("ENRICHED_FALLBACK_MIN_SCORE", "8"))

# FIX 7: market_regime gate
_RANGING_MIN_SCORE    = int(os.getenv("RANGING_MIN_SCORE",   "9"))
_VOLATILE_MIN_SCORE   = int(os.getenv("VOLATILE_MIN_SCORE",  "8"))
_RANGING_BLOCK_EARLY  = os.getenv("RANGING_BLOCK_EARLY", "true").lower() != "false"

_AI_NEWS_COOLDOWN_S = int(os.getenv("AI_NEWS_COOLDOWN_S", "300"))  # 5 min
_last_ai_news_call: dict[str, float] = {}


def _apply_regime_gate(signal: SignalResult, symbol: str) -> Optional[dict]:
    """
    FIX 7: Comprueba el régimen de mercado global (BTC) y eleva el umbral
    de score o bloquea la entrada según el régimen.

    Returns None si la señal pasa el gate, o un dict HOLD si queda bloqueada.
    """
    try:
        from bot.market_regime import market_regime, MARKET_REGIME_GATE
        if not MARKET_REGIME_GATE:
            return None

        regime_raw = market_regime.regime_raw()  # TRENDING / RANGING / VOLATILE / UNKNOWN

        if regime_raw == "RANGING":
            # Bloquear EARLY directamente en RANGING
            if _RANGING_BLOCK_EARLY and signal.entry_mode == "EARLY":
                return _result(
                    "HOLD", signal, False,
                    f"🔴 market_regime=RANGING → modo EARLY bloqueado (false breakout risk)",
                )
            # Exigir score mínimo más alto
            if signal.score < _RANGING_MIN_SCORE:
                return _result(
                    "HOLD", signal, False,
                    f"🔴 market_regime=RANGING → score={signal.score} < {_RANGING_MIN_SCORE} requerido",
                )
            log.info(
                "[strategy] %s market_regime=RANGING pero score=%d >= %d — permitiendo",
                symbol, signal.score, _RANGING_MIN_SCORE,
            )

        elif regime_raw == "VOLATILE":
            if signal.score < _VOLATILE_MIN_SCORE:
                return _result(
                    "HOLD", signal, False,
                    f"🟡 market_regime=VOLATILE → score={signal.score} < {_VOLATILE_MIN_SCORE} requerido",
                )

    except Exception as e:
        log.debug("[strategy] _apply_regime_gate error (ignorado): %s", e)

    return None  # gate pasado


async def decide(
    exch,
    symbol: str,
    ai_decide_fn,
    has_open_position: bool = False,
    current_pnl: Optional[float] = None,
    ohlcv_fn: Optional[Callable] = None,
) -> dict:
    """
    Retorna:
        action       : "BUY" | "SELL" | "HOLD"
        signal       : SignalResult
        ai_used      : bool
        reason       : str
        signal_block : str (Markdown)
        ai_confidence: int (0 if IA not used)
        ai_reason    : str
        ef_penalty   : int (0-3, penalización de enriched_filter para sizing)
    """

    if has_open_position:
        return _result("HOLD", None, False, "Posición ya abierta — esperando cierre")

    try:
        signal: SignalResult = await analyze_pair(exch, symbol, ohlcv_fn=ohlcv_fn)
    except Exception as e:
        log.error(f"[strategy] analyze_pair error: {e}")
        return _result("HOLD", None, False, f"Error en análisis técnico: {e}")

    log.info(
        f"[strategy] {symbol} · score={signal.score}/{signal.max_score} · mode={signal.entry_mode} "
        f"· {signal.signal} · RR={signal.rr} · lev={signal.suggested_lev}x"
    )

    if not signal.is_valid:
        return _result(
            "HOLD", signal, False,
            f"Sin modo de entrada válido (score={signal.score}/{signal.max_score}, mode={signal.entry_mode})"
        )

    if signal.rr < MIN_RR_REQUIRED:
        return _result(
            "HOLD", signal, False,
            f"R/R insuficiente ({signal.rr:.1f} < {MIN_RR_REQUIRED})"
        )

    if signal.signal == "NEUTRAL":
        return _result("HOLD", signal, False, "Señal técnica neutral")

    # FIX 7: gate de régimen de mercado (RANGING/VOLATILE)
    regime_block = _apply_regime_gate(signal, symbol)
    if regime_block is not None:
        return regime_block

    if signal.entry_mode == "STRONG" and SKIP_AI_ON_STRONG:
        action = "BUY" if signal.signal == "LONG" else "SELL"
        return _result(
            action, signal, False,
            f"💥 STRONG entry directo · score={signal.score}/{signal.max_score} · lev={signal.suggested_lev}x"
        )

    if signal.score < AI_CALL_MIN_SCORE:
        return _result(
            "HOLD", signal, False,
            f"⏭️ score={signal.score}/{signal.max_score} < {AI_CALL_MIN_SCORE} → HOLD"
        )

    # ── Paso 1: enriquecer contexto externo ──────────────────────────────────
    from bot.data_enricher import fetch_enriched_context
    from bot.enriched_filter import apply as ef_apply

    enriched = None
    enriched_failed = False
    try:
        enriched = await fetch_enriched_context(symbol)
    except Exception as e:
        log.warning(f"[strategy] fetch_enriched_context error: {e} — activando fallback seguro")
        enriched_failed = True

    # ── Paso 2: filtro determinista ───────────────────────────────────────────
    action_if_pass = "BUY" if signal.signal == "LONG" else "SELL"

    if enriched is not None:
        i15       = signal.indicators.get("15m", {})
        rsi_val   = i15.get("rsi_val")
        vol_ratio = i15.get("vol_ratio")

        price_dir = None
        try:
            closes = signal.indicators.get("_closes_15m") or []
            if len(closes) >= 2:
                price_dir = "rising" if closes[-1] > closes[-2] else "falling"
        except Exception:
            pass

        ef_result = ef_apply(
            signal=signal.signal,
            enriched=enriched,
            rsi=rsi_val,
            vol_ratio=vol_ratio,
            price_direction=price_dir,
        )

        if not ef_result.allowed:
            return _result(
                "HOLD", signal, False,
                f"🚫 EnrichedFilter bloqueó: {ef_result.reason}"
            )

        base_confidence = max(5, signal.score - ef_result.penalty)

        # ── Paso 3: IA solo si hay noticias relevantes ────────────────────────
        if USE_AI_FOR_NEWS and ef_result.news_ai_needed:
            now = time.monotonic()
            cooldown_remaining = _AI_NEWS_COOLDOWN_S - (now - _last_ai_news_call.get(symbol, 0))
            if cooldown_remaining > 0:
                log.debug(
                    f"[strategy] {symbol} IA news cooldown activo ({cooldown_remaining:.0f}s restantes)"
                )
            else:
                log.info(
                    f"[strategy] {symbol} 📰 Consultando IA solo para análisis de noticias "
                    f"(score={signal.score}, {len(enriched.news)} noticias relevantes)"
                )
                _last_ai_news_call[symbol] = now

                news_context = {
                    "symbol":     symbol,
                    "signal":     signal.signal,
                    "score":      signal.score,
                    "max_score":  signal.max_score,
                    "rr":         signal.rr,
                    "entry":      signal.entry,
                    "sl":         signal.sl,
                    "tp1":        signal.tp1,
                    "tp2":        signal.tp2,
                    "atr":        signal.atr,
                    "suggested_lev": signal.suggested_lev,
                    "task":       "news_sentiment_only",
                    "external":   _format_news_only(enriched),
                }

                try:
                    ai_result = await ai_decide_fn(
                        symbol, [], None, None, signal.suggested_lev,
                        context_override=news_context,
                    )
                    ai_action     = str(ai_result.get("action", "HOLD")).upper().strip()
                    ai_confidence = ai_result.get("confidence", 0)
                    ai_reason     = ai_result.get("reason", ai_result.get("reasoning", ""))

                    if ai_action not in ("BUY", "SELL"):
                        ai_action = "HOLD"

                    if ai_action == "HOLD" and ai_confidence >= AI_HIGH_CONFIDENCE_THRESHOLD:
                        return _result(
                            "HOLD", signal, True,
                            f"📰 IA news→HOLD ({ai_confidence}/10) bloqueó entrada | {ai_reason}",
                            ai_confidence=ai_confidence,
                            ai_reason=ai_reason,
                        )

                    if ai_action == "HOLD" and signal.score >= AI_HOLD_OVERRIDE_SCORE:
                        log.info(
                            f"[strategy] {symbol} 🔁 IA news→HOLD (conf={ai_confidence}) "
                            f"pero score={signal.score}>={AI_HOLD_OVERRIDE_SCORE} → override"
                        )
                        return _result(
                            action_if_pass, signal, True,
                            f"🔁 Override técnico (score={signal.score}) · IA news dudó (conf={ai_confidence}/10) | {ai_reason}",
                            ai_confidence=ai_confidence,
                            ai_reason=ai_reason,
                            ef_penalty=ef_result.penalty,
                        )

                except Exception as e:
                    log.warning(f"[strategy] IA noticias falló: {e} — ignorando")

        # ── Paso 4: entrada (filtro determinista pasado) ──────────────────────
        return _result(
            action_if_pass, signal, False,
            f"✅ EnrichedFilter OK · {ef_result.reason} · "
            f"score={signal.score}/{signal.max_score} · lev={signal.suggested_lev}x",
            ef_penalty=ef_result.penalty,
        )

    # FIX 5: Fallback seguro — sin datos externos
    if enriched_failed:
        if signal.score < _ENRICHED_FALLBACK_MIN_SCORE:
            return _result(
                "HOLD", signal, False,
                f"⚠️ Sin datos externos (error) + score={signal.score} < {_ENRICHED_FALLBACK_MIN_SCORE} → HOLD seguro"
            )
        log.warning(
            f"[strategy] {symbol} ⚠️ ENTRANDO SIN DATOS EXTERNOS (enriquecimiento falló) "
            f"— score={signal.score}/{signal.max_score} supera umbral {_ENRICHED_FALLBACK_MIN_SCORE}"
        )
        return _result(
            action_if_pass, signal, False,
            f"⚡ Técnico fuerte SIN validación externa (enriched falló) · "
            f"score={signal.score}/{signal.max_score} · lev={signal.suggested_lev}x"
        )

    log.info(f"[strategy] {symbol} Sin datos externos — decisión técnica pura")
    return _result(
        action_if_pass, signal, False,
        f"⚡ Técnico (sin datos externos) · score={signal.score}/{signal.max_score} · lev={signal.suggested_lev}x"
    )


def _format_news_only(enriched) -> str:
    lines = []
    if enriched.news:
        lines.append("Recent news (your only job is to evaluate these):")
        for item in enriched.news:
            icon = "📈" if item.sentiment == "bullish" else "📉" if item.sentiment == "bearish" else "📰"
            lines.append(f"  {icon} [{item.sentiment}] {item.title}")
    else:
        lines.append("Recent news: none")
    return "\n".join(lines)


def _result(
    action: str,
    signal,
    ai_used: bool,
    reason: str,
    ai_confidence: int = 0,
    ai_reason: str = "",
    ef_penalty: int = 0,
) -> dict:
    return {
        "action":        action,
        "signal":        signal,
        "ai_used":       ai_used,
        "reason":        reason,
        "signal_block":  format_signal_block(signal) if signal else "",
        "ai_confidence": ai_confidence,
        "ai_reason":     ai_reason,
        "ef_penalty":    ef_penalty,
    }
