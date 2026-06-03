#!/usr/bin/env python3
"""
strategy.py — Lógica de decisión de BotTrading

Flujo (sin IA para el caso normal):
  score < MIN_SIGNAL_SCORE  → HOLD directo
  RR < MIN_RR_REQUIRED      → HOLD directo
  NEUTRAL                   → HOLD directo
  regime_gate               → bloquea por tipo de setup + score
  score < AI_CALL_MIN_SCORE → HOLD (evita fetch_enriched innecesario)
  STRONG (score >=9)        → pasa por enriched_filter
  EARLY/NORMAL (score >= AI_CALL_MIN_SCORE):
    1. Aplica enriched_filter (F&G, funding, OI, RSI, vol) — sin IA, instantáneo
    2. Si el filtro bloquea → HOLD (motivo detallado)
    3. Si hay >= NEWS_AI_THRESHOLD noticias relevantes → consulta IA solo para noticias
    4. Si la IA de noticias dice HOLD con alta confianza → respeta
    5. Si no hay noticias relevantes o la IA confirma → entra

FIX 5 (REVISADO): Fallback cuando fetch_enriched_context falla:
  - enriched=None por error → SIEMPRE HOLD. No se entra sin datos externos.
  - Esto evita entradas en señales técnicas débiles sin validación externa.

FIX 6: Propagar ef_penalty al decision_engine (v4.4)

FIX 7 (MEJORADO): market_regime gate por tipo de setup:
  - RANGING: bloquea TENDENCIA y BREAKOUT. Solo REVERSAL permitido.
    Ratio de fakeout en breakout/tendencia durante RANGING es muy alto.
  - TRENDING: bloquea REVERSAL (buscar reversal contra tendencia fuerte falla).
  - VOLATILE: eleva MIN_SIGNAL_SCORE (ya estaba, mantenido).
  - EARLY siempre bloqueado en RANGING (RANGING_BLOCK_EARLY=true).
  Config Railway:
    REGIME_BLOCK_TREND_ON_RANGING    → default true
    REGIME_BLOCK_REVERSAL_ON_TRENDING → default true
    RANGING_MIN_SCORE   → default 9
    VOLATILE_MIN_SCORE  → default 8
    RANGING_BLOCK_EARLY → default true

FIX 8: price_direction robusto + momentum guard en fallback

FIX 9 (AI filter orden): fetch_enriched_context se mueve tras el check
  score >= AI_CALL_MIN_SCORE. Señales que no superan ese umbral ya no
  consumen llamadas HTTP al enricher (F&G, funding, OI, etc.).

Variables de entorno:
  MIN_SIGNAL_SCORE             (default: 8)
  MIN_RR_REQUIRED              (default: 1.8)
  SKIP_AI_ON_STRONG            (default: false)
  AI_CALL_MIN_SCORE            (default: 8)
  AI_HOLD_OVERRIDE_SCORE       (default: 8)
  AI_HIGH_CONFIDENCE_THRESHOLD (default: 8)
  USE_AI_FOR_NEWS              (default: true)
  RANGING_MIN_SCORE            (default: 9)
  VOLATILE_MIN_SCORE           (default: 8)
  RANGING_BLOCK_EARLY          (default: true)
  REGIME_BLOCK_TREND_ON_RANGING    (default: true)
  REGIME_BLOCK_REVERSAL_ON_TRENDING (default: true)
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

MIN_SIGNAL_SCORE             = int(os.getenv("MIN_SIGNAL_SCORE",             "8"))
MIN_RR_REQUIRED              = float(os.getenv("MIN_RR_REQUIRED",            "1.8"))
SKIP_AI_ON_STRONG            = os.getenv("SKIP_AI_ON_STRONG",                "false").lower() != "false"
AI_CALL_MIN_SCORE            = int(os.getenv("AI_CALL_MIN_SCORE",            "8"))
AI_HOLD_OVERRIDE_SCORE       = int(os.getenv("AI_HOLD_OVERRIDE_SCORE",       "8"))
AI_HIGH_CONFIDENCE_THRESHOLD = int(os.getenv("AI_HIGH_CONFIDENCE_THRESHOLD", "8"))
USE_AI_FOR_NEWS              = os.getenv("USE_AI_FOR_NEWS", "true").lower() != "false"

# FIX 7: market_regime gate
_RANGING_MIN_SCORE    = int(os.getenv("RANGING_MIN_SCORE",   "9"))
_VOLATILE_MIN_SCORE   = int(os.getenv("VOLATILE_MIN_SCORE",  "8"))
_RANGING_BLOCK_EARLY  = os.getenv("RANGING_BLOCK_EARLY", "true").lower() != "false"
_REGIME_BLOCK_TREND_ON_RANGING     = os.getenv("REGIME_BLOCK_TREND_ON_RANGING",     "true").lower() != "false"
_REGIME_BLOCK_REVERSAL_ON_TRENDING = os.getenv("REGIME_BLOCK_REVERSAL_ON_TRENDING", "true").lower() != "false"

# FIX 8: momentum guard
_RSI_MOMENTUM_BLOCK = float(os.getenv("EF_RSI_MOMENTUM_BLOCK", "50"))

_AI_NEWS_COOLDOWN_S = int(os.getenv("AI_NEWS_COOLDOWN_S", "300"))
_last_ai_news_call: dict[str, float] = {}


def _compute_price_direction(
    signal: "SignalResult",
    ohlcv_fn: Optional[Callable] = None,
) -> Optional[str]:
    try:
        closes = signal.indicators.get("_closes_15m") or []
        if len(closes) >= 2:
            return "rising" if closes[-1] > closes[-2] else "falling"
    except Exception:
        pass

    if ohlcv_fn is not None:
        try:
            import inspect
            if not inspect.iscoroutinefunction(ohlcv_fn):
                candles = ohlcv_fn()
                if candles and len(candles) >= 2:
                    return "rising" if candles[-1][4] > candles[-2][4] else "falling"
        except Exception:
            pass

    return None


def _conservative_price_dir(signal_direction: str, price_dir: Optional[str]) -> str:
    if price_dir is not None:
        return price_dir
    return "falling" if signal_direction.upper() == "LONG" else "rising"


def _momentum_guard_fallback(signal: "SignalResult", price_dir: Optional[str]) -> Optional[dict]:
    effective_dir = _conservative_price_dir(signal.signal, price_dir)
    i15     = signal.indicators.get("15m", {})
    rsi_val = i15.get("rsi_val")

    is_long  = signal.signal.upper() == "LONG"
    is_short = signal.signal.upper() == "SHORT"

    if is_long and effective_dir == "falling":
        if rsi_val is not None and rsi_val < _RSI_MOMENTUM_BLOCK:
            return _result(
                "HOLD", signal, False,
                f"🚫 [fallback momentum] precio cayendo + RSI={rsi_val:.1f} < {_RSI_MOMENTUM_BLOCK} "
                f"— LONG bloqueado (anti caída libre, sin enriched)"
            )
        if signal.score < MIN_SIGNAL_SCORE + 1:
            return _result(
                "HOLD", signal, False,
                f"🚫 [fallback momentum] precio cayendo + score={signal.score} insuficiente para LONG sin datos externos"
            )

    if is_short and effective_dir == "rising":
        if rsi_val is not None and rsi_val > (100 - _RSI_MOMENTUM_BLOCK):
            return _result(
                "HOLD", signal, False,
                f"🚫 [fallback momentum] precio subiendo + RSI={rsi_val:.1f} > {100 - _RSI_MOMENTUM_BLOCK} "
                f"— SHORT bloqueado (anti pump, sin enriched)"
            )
        if signal.score < MIN_SIGNAL_SCORE + 1:
            return _result(
                "HOLD", signal, False,
                f"🚫 [fallback momentum] precio subiendo + score={signal.score} insuficiente para SHORT sin datos externos"
            )

    return None


def _apply_regime_gate(signal: SignalResult, symbol: str) -> Optional[dict]:
    """
    FIX 7 (MEJORADO): Gate de régimen de mercado con bloqueo por tipo de setup.

    RANGING:
      - TENDENCIA y BREAKOUT bloqueados (fakeout rate muy alto en rango)
      - REVERSAL permitido (busca agotamiento de movimientos en rango)
      - EARLY siempre bloqueado (RANGING_BLOCK_EARLY=true)
      - Score mínimo elevado a RANGING_MIN_SCORE

    TRENDING:
      - REVERSAL bloqueado (buscar reversión contra tendencia fuerte tiene
        win rate muy bajo; el setup correcto en TRENDING es TENDENCIA)
      - TENDENCIA y BREAKOUT permitidos

    VOLATILE:
      - Solo eleva score mínimo a VOLATILE_MIN_SCORE
      - Ningún setup bloqueado por tipo (volatilidad es oportunidad)
    """
    try:
        from bot.market_regime import market_regime, MARKET_REGIME_GATE
        if not MARKET_REGIME_GATE:
            return None

        regime_raw = market_regime.regime_raw()
        setup_type = signal.extra.get("setup_type", "")

        # ── RANGING ─────────────────────────────────────────────────
        if regime_raw == "RANGING":
            if _RANGING_BLOCK_EARLY and signal.entry_mode == "EARLY":
                return _result(
                    "HOLD", signal, False,
                    f"🔴 market_regime=RANGING → modo EARLY bloqueado (false breakout risk)",
                )
            if _REGIME_BLOCK_TREND_ON_RANGING and setup_type in ("TENDENCIA", "BREAKOUT"):
                return _result(
                    "HOLD", signal, False,
                    f"🔴 market_regime=RANGING → {setup_type} bloqueado "
                    f"(fakeout rate alto en mercado lateral — usa REVERSAL)",
                )
            if signal.score < _RANGING_MIN_SCORE:
                return _result(
                    "HOLD", signal, False,
                    f"🔴 market_regime=RANGING → score={signal.score} < {_RANGING_MIN_SCORE} requerido",
                )
            log.info(
                "[strategy] %s market_regime=RANGING, setup=%s, score=%d >= %d — permitiendo",
                symbol, setup_type, signal.score, _RANGING_MIN_SCORE,
            )

        # ── TRENDING ─────────────────────────────────────────────────
        elif regime_raw == "TRENDING":
            if _REGIME_BLOCK_REVERSAL_ON_TRENDING and setup_type == "REVERSAL":
                return _result(
                    "HOLD", signal, False,
                    f"🟢 market_regime=TRENDING → REVERSAL bloqueado "
                    f"(win rate bajo reversando contra tendencia fuerte — usa TENDENCIA)",
                )

        # ── VOLATILE ─────────────────────────────────────────────────
        elif regime_raw == "VOLATILE":
            if signal.score < _VOLATILE_MIN_SCORE:
                return _result(
                    "HOLD", signal, False,
                    f"🟡 market_regime=VOLATILE → score={signal.score} < {_VOLATILE_MIN_SCORE} requerido",
                )

    except Exception as e:
        log.debug("[strategy] _apply_regime_gate error (ignorado): %s", e)

    return None


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

    # FIX 7 (MEJORADO): gate de régimen por tipo de setup
    regime_block = _apply_regime_gate(signal, symbol)
    if regime_block is not None:
        return regime_block

    if signal.entry_mode == "STRONG" and SKIP_AI_ON_STRONG:
        action = "BUY" if signal.signal == "LONG" else "SELL"
        return _result(
            action, signal, False,
            f"💥 STRONG entry directo · score={signal.score}/{signal.max_score} · lev={signal.suggested_lev}x"
        )

    # FIX 9: score check ANTES de fetch_enriched_context
    # Señales que no pasan este umbral no llegan al enricher (sin HTTP innecesarios)
    if signal.score < AI_CALL_MIN_SCORE:
        return _result(
            "HOLD", signal, False,
            f"⏭️ score={signal.score}/{signal.max_score} < {AI_CALL_MIN_SCORE} → HOLD"
        )

    # FIX 8: calcular price_direction una sola vez, con fallback robusto
    price_dir = _compute_price_direction(signal, ohlcv_fn)
    effective_price_dir = _conservative_price_dir(signal.signal, price_dir)
    if price_dir is None:
        log.debug(
            "[strategy] %s price_direction desconocido → usando conservador '%s' para %s",
            symbol, effective_price_dir, signal.signal,
        )

    # ── Paso 1: enriquecer contexto externo ──────────────────────────────────────
    # Solo llega aquí si score >= AI_CALL_MIN_SCORE (FIX 9)
    from bot.data_enricher import fetch_enriched_context
    from bot.enriched_filter import apply as ef_apply

    enriched = None
    enriched_failed = False
    try:
        enriched = await fetch_enriched_context(symbol)
    except Exception as e:
        log.warning(f"[strategy] fetch_enriched_context error: {e} — bloqueando entrada (FIX: no entrar sin datos externos)")
        enriched_failed = True

    if enriched_failed:
        log.warning(
            f"[strategy] {symbol} ⛔ HOLD — datos externos no disponibles (error de red). "
            f"No se abre posición sin validación de F&G/funding/OI."
        )
        return _result(
            "HOLD", signal, False,
            f"⛔ Sin datos externos (error de red) — entrada bloqueada para protección"
        )

    # ── Paso 2: filtro determinista ──────────────────────────────────────────────
    action_if_pass = "BUY" if signal.signal == "LONG" else "SELL"

    if enriched is not None:
        i15       = signal.indicators.get("15m", {})
        rsi_val   = i15.get("rsi_val")
        vol_ratio = i15.get("vol_ratio")

        ef_result = ef_apply(
            signal=signal.signal,
            enriched=enriched,
            rsi=rsi_val,
            vol_ratio=vol_ratio,
            price_direction=effective_price_dir,
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

    # Path técnico puro (enriched=None sin error)
    momentum_block = _momentum_guard_fallback(signal, price_dir)
    if momentum_block is not None:
        return momentum_block

    log.info(f"[strategy] {symbol} Sin datos externos (None limpio) — decisión técnica pura con momentum guard")
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
