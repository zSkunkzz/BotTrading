#!/usr/bin/env python3
"""
strategy.py — Lógica de decisión de BotTrading

Flujo (sin IA para el caso normal):
  score < MIN_SIGNAL_SCORE  → HOLD directo
  RR < MIN_RR_REQUIRED      → HOLD directo
  NEUTRAL                   → HOLD directo
  session_gate              → bloquea TENDENCIA/BREAKOUT fuera de 07:00-18:00 UTC
  correlation_gate          → bloquea si hay demasiadas posiciones en la misma dirección
  regime_gate               → bloquea por tipo de setup + score
  score < AI_CALL_MIN_SCORE → HOLD (evita fetch_enriched innecesario)
  STRONG (score >=9)        → pasa por enriched_filter
  EARLY/NORMAL (score >= AI_CALL_MIN_SCORE):
    1. Aplica enriched_filter (F&G, funding, OI, RSI, vol) — sin IA, instantáneo
    2. Si el filtro bloquea → HOLD (motivo detallado)
    3. Si hay >= NEWS_AI_THRESHOLD noticias relevantes → consulta IA solo para noticias
       El score_delta de la IA se cachea (NEWS_SCORE_TTL_MINUTES, default 30 min).
       En ciclos siguientes dentro del TTL se reutiliza el score más alto
       entre el fresco y el cacheado — evita que una noticia negativa/positiva se olvide.
    4. Si la IA de noticias dice HOLD con alta confianza → respeta
    5. Si no hay noticias relevantes o la IA confirma → entra

FIX 5 (REVISADO): Fallback cuando fetch_enriched_context falla:
  - enriched=None por error → SIEMPRE HOLD. No se entra sin datos externos.

FIX 6: Propagar ef_penalty al decision_engine (v4.4)

FIX 7 (MEJORADO): market_regime gate por tipo de setup.

FIX 8: price_direction robusto + momentum guard en fallback

FIX 9 (AI filter orden): fetch_enriched_context se mueve tras el check
  score >= AI_CALL_MIN_SCORE.

v17: RANGING_BLOCK_EARLY excluye REVERSAL.

v21-P4: cache TTL para score_delta de noticias.
  NEWS_SCORE_TTL_MINUTES (default 30): tiempo en minutos que se retiene
  el score_delta de noticias.

v23 — A+B+C:
  A — regime se obtiene en decide() y se pasa a analyze_pair().
      Activa MTF bias filter y R/R dinámico por régimen en signal_engine.
  B — _regime también se pasa a enriched_filter.apply().
      RANGING: funding umbral más estricto, OI+caída sin umbral.
      VOLATILE: penalty +1 extra si momentum contrario.
      TRENDING: RSI_OVERBOUGHT sube a 72.
  C — cache noticias bidireccional: _get_news_score() no sobreescribe
      el cache con fresh_score=0.0 cuando la IA no fue consultada.
      Preserva scores tanto negativos (malas noticias) como positivos
      (buenas noticias) entre ciclos.

v24 — session_gate + correlation_gate:
  session_filter.check_session(setup_type): bloquea TENDENCIA/BREAKOUT
  fuera de 07:00–18:00 UTC. REVERSAL permitido 24h.
  correlation_guard.check_correlation(direction, open_positions): bloquea
  si hay ≥MAX_SAME_DIR posiciones en la misma dirección o ≥MAX_OPEN total.
  Ambos gates se ejecutan antes del regime_gate.

Variables de entorno:
  MIN_SIGNAL_SCORE             (default: 8)
  MIN_RR_REQUIRED              (default: 1.8)
  SKIP_AI_ON_STRONG            (default: false)
  AI_CALL_MIN_SCORE            (default: 8)
  AI_HOLD_OVERRIDE_SCORE       (default: 8)
  AI_HIGH_CONFIDENCE_THRESHOLD (default: 8)
  USE_AI_FOR_NEWS              (default: true)
  NEWS_SCORE_TTL_MINUTES       (default: 30)
  RANGING_MIN_SCORE            (default: 9)
  VOLATILE_MIN_SCORE           (default: 8)
  RANGING_BLOCK_EARLY          (default: true)
  REGIME_BLOCK_TREND_ON_RANGING    (default: true)
  REGIME_BLOCK_REVERSAL_ON_TRENDING (default: true)
  SESSION_FILTER_ENABLED       (default: true)
  SESSION_START_UTC            (default: 7)
  SESSION_END_UTC              (default: 18)
  SESSION_ALLOW_REVERSAL       (default: true)
  CORR_ENABLED                 (default: true)
  CORR_MAX_SAME_DIR            (default: 3)
  CORR_MAX_OPEN                (default: 5)
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

# v21-P4: cache TTL para score_delta de noticias
# v23-C: bidireccional — preserva scores positivos Y negativos entre ciclos
# Estructura: symbol → (score_delta: float, expires_ts: float)
_news_score_cache: dict[str, tuple[float, float]] = {}
_NEWS_SCORE_TTL = float(os.getenv("NEWS_SCORE_TTL_MINUTES", "30")) * 60  # segundos


def _get_news_score(symbol: str, fresh_score: float) -> float:
    """Devuelve el score_delta de noticias más representativo.

    v23-C (bidireccional):
    - Si fresh_score == 0.0 (IA no consultada este ciclo) Y hay cache válido,
      se devuelve el cacheado SIN sobreescribirlo. Esto preserva tanto
      noticias negativas como positivas entre ciclos donde la IA no se llama.
    - Si fresh_score != 0.0, se compara con el cacheado por valor absoluto:
      el de mayor peso manda y se almacena.
    - Si el cache expiró, se usa fresh_score directamente.

    El resultado es que una noticia muy positiva/negativa recibida en ciclo N
    sigue influenciando entradas en ciclos N+1, N+2… hasta que el TTL expire,
    aunque en esos ciclos la IA no haya sido consultada.
    """
    now = time.time()
    cached_score, expires = _news_score_cache.get(symbol, (0.0, 0.0))

    # v23-C: si la IA no fue consultada (fresh=0.0), devolver cache si válido
    if fresh_score == 0.0:
        if now < expires and cached_score != 0.0:
            log.debug(
                "[strategy] %s news_cache devuelto (IA no consultada): %.3f (expira en %.0fs)",
                symbol, cached_score, expires - now,
            )
            return cached_score
        return 0.0

    if now < expires:
        # Cache válido: elegir el score de mayor peso absoluto
        if abs(fresh_score) >= abs(cached_score):
            best = fresh_score
            _news_score_cache[symbol] = (fresh_score, now + _NEWS_SCORE_TTL)
            log.debug(
                "[strategy] %s news_cache actualizado: %.3f → %.3f (TTL %.0fs)",
                symbol, cached_score, fresh_score, _NEWS_SCORE_TTL,
            )
        else:
            best = cached_score
            log.debug(
                "[strategy] %s news_cache reutilizado: %.3f (fresh=%.3f, expira en %.0fs)",
                symbol, cached_score, fresh_score, expires - now,
            )
    else:
        # Cache expirado o vacío: usar fresh y almacenar
        best = fresh_score
        _news_score_cache[symbol] = (fresh_score, now + _NEWS_SCORE_TTL)
        log.debug(
            "[strategy] %s news_cache nuevo: %.3f (TTL %.0fs)",
            symbol, fresh_score, _NEWS_SCORE_TTL,
        )

    return best


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
    FIX 7 (MEJORADO) + v17:

    RANGING:
      - TENDENCIA y BREAKOUT bloqueados (fakeout rate muy alto en rango)
      - REVERSAL permitido
      - EARLY bloqueado en RANGING EXCEPTO para REVERSAL (v17)
      - Score mínimo elevado a RANGING_MIN_SCORE

    TRENDING:
      - REVERSAL bloqueado
      - TENDENCIA y BREAKOUT permitidos

    VOLATILE:
      - Solo eleva score mínimo a VOLATILE_MIN_SCORE
    """
    try:
        from bot.market_regime import market_regime, MARKET_REGIME_GATE
        if not MARKET_REGIME_GATE:
            return None

        regime_raw = market_regime.regime_raw()
        setup_type = signal.extra.get("setup_type", "")

        if regime_raw == "RANGING":
            if _RANGING_BLOCK_EARLY and signal.entry_mode == "EARLY" and setup_type != "REVERSAL":
                return _result(
                    "HOLD", signal, False,
                    f"🔴 market_regime=RANGING → modo EARLY bloqueado para {setup_type} (false breakout risk)",
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

        elif regime_raw == "TRENDING":
            if _REGIME_BLOCK_REVERSAL_ON_TRENDING and setup_type == "REVERSAL":
                return _result(
                    "HOLD", signal, False,
                    f"🟢 market_regime=TRENDING → REVERSAL bloqueado "
                    f"(win rate bajo reversando contra tendencia fuerte — usa TENDENCIA)",
                )

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
    open_positions: Optional[dict] = None,
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

    # v23-A: obtener régimen UNA vez y reutilizarlo en todo el flujo
    _regime: Optional[str] = None
    try:
        from bot.market_regime import market_regime, MARKET_REGIME_GATE
        if MARKET_REGIME_GATE:
            _regime = market_regime.regime_raw()
    except Exception as e:
        log.debug("[strategy] regime lookup error (ignorado): %s", e)

    try:
        # v23-A: pasar regime → activa MTF bias filter y R/R dinámico en signal_engine
        signal: SignalResult = await analyze_pair(
            exch, symbol, ohlcv_fn=ohlcv_fn, regime=_regime
        )
    except Exception as e:
        log.error(f"[strategy] analyze_pair error: {e}")
        return _result("HOLD", None, False, f"Error en análisis técnico: {e}")

    log.info(
        f"[strategy] {symbol} · score={signal.score}/{signal.max_score} · mode={signal.entry_mode} "
        f"· {signal.signal} · RR={signal.rr} · lev={signal.suggested_lev}x · regime={_regime or 'none'}"
    )

    if signal.signal == "NEUTRAL" or not signal.is_valid:
        return _result("HOLD", signal, False, signal.reason or "Señal no válida")

    if signal.score < MIN_SIGNAL_SCORE:
        return _result(
            "HOLD", signal, False,
            f"Score {signal.score} < mínimo {MIN_SIGNAL_SCORE}"
        )

    if signal.rr < MIN_RR_REQUIRED:
        return _result(
            "HOLD", signal, False,
            f"RR {signal.rr:.2f} < mínimo {MIN_RR_REQUIRED}"
        )

    # ── v24: Gate de sesión ─────────────────────────────────────────────────
    # Bloquea TENDENCIA/BREAKOUT fuera de 07:00–18:00 UTC (baja liquidez).
    # REVERSAL se permite 24h (SESSION_ALLOW_REVERSAL=true por defecto).
    try:
        from bot.session_filter import check_session
        setup_type = signal.extra.get("setup_type") if signal.extra else None
        session_block = check_session(setup_type)
        if session_block:
            return _result("HOLD", signal, False, session_block)
    except Exception as _se:
        log.debug("[strategy] session_filter error (ignorado): %s", _se)

    # ── v24: Gate de correlación ─────────────────────────────────────────────
    # Bloquea si hay ≥MAX_SAME_DIR posiciones en la misma dirección
    # o ≥MAX_OPEN posiciones abiertas en total.
    try:
        from bot.correlation_guard import check_correlation
        _positions = open_positions or {}
        corr_ok, corr_reason = check_correlation(
            proposed_direction=signal.signal,  # "LONG" / "SHORT"
            open_positions=_positions,
        )
        if not corr_ok:
            return _result("HOLD", signal, False, f"🔒 correlation_guard: {corr_reason}")
    except Exception as _ce:
        log.debug("[strategy] correlation_guard error (ignorado): %s", _ce)

    gate_result = _apply_regime_gate(signal, symbol)
    if gate_result is not None:
        return gate_result

    if signal.score < AI_CALL_MIN_SCORE:
        return _result(
            "HOLD", signal, False,
            f"Score {signal.score} < AI_CALL_MIN_SCORE {AI_CALL_MIN_SCORE} — señal insuficiente"
        )

    # ── Enriched context ──────────────────────────────────────────────────────
    enriched = None
    try:
        from bot.data_enricher import fetch_enriched_context
        enriched = await fetch_enriched_context(symbol)
    except Exception as e:
        log.warning(f"[strategy] fetch_enriched_context error: {e}")

    if enriched is None:
        fallback = _momentum_guard_fallback(signal, _compute_price_direction(signal, ohlcv_fn))
        if fallback:
            return fallback
        return _result(
            "HOLD", signal, False,
            "enriched=None — HOLD por política de seguridad (sin datos externos)"
        )

    # ── Enriched filter (v23-B: pasa regime) ─────────────────────────────────
    from bot.enriched_filter import apply as ef_apply
    price_dir = _compute_price_direction(signal, ohlcv_fn)
    i15 = signal.indicators.get("15m", {})
    ef_result = ef_apply(
        signal=signal.signal,
        enriched=enriched,
        rsi=i15.get("rsi_val"),
        vol_ratio=i15.get("vol_ratio"),
        price_direction=price_dir,
        regime=_regime,   # v23-B
    )
    ef_penalty = ef_result.penalty

    if not ef_result.allowed:
        return _result(
            "HOLD", signal, False,
            f"🚫 enriched_filter: {ef_result.reason}",
            ef_penalty=ef_penalty,
        )

    # ── IA de noticias (v23-C: _get_news_score corregido) ────────────────────
    ai_confidence = 0
    ai_reason     = ""
    ai_used       = False
    score_delta   = 0.0

    if USE_AI_FOR_NEWS and ef_result.news_ai_needed:
        now = time.time()
        last_call = _last_ai_news_call.get(symbol, 0.0)
        if now - last_call >= _AI_NEWS_COOLDOWN_S:
            try:
                ai_result = await ai_decide_fn(
                    symbol=symbol,
                    signal=signal.signal,
                    enriched=enriched,
                )
                _last_ai_news_call[symbol] = now
                ai_used       = True
                ai_confidence = ai_result.get("confidence", 0)
                ai_reason     = ai_result.get("reason", "")
                score_delta   = float(ai_result.get("score_delta", 0.0))

                log.info(
                    "[strategy] %s IA noticias → confidence=%d score_delta=%.2f reason=%s",
                    symbol, ai_confidence, score_delta, ai_reason[:80],
                )
            except Exception as e:
                log.warning("[strategy] ai_decide_fn error: %s", e)
        else:
            log.debug(
                "[strategy] %s AI noticias en cooldown (%.0fs restantes)",
                symbol, _AI_NEWS_COOLDOWN_S - (now - last_call),
            )

    # v23-C: _get_news_score ya no sobreescribe cache con 0.0 cuando IA no se consultó
    effective_score_delta = _get_news_score(symbol, score_delta)

    adjusted_score = signal.score + effective_score_delta
    if ai_used and ai_confidence >= AI_HIGH_CONFIDENCE_THRESHOLD:
        if ai_result.get("action", "").upper() == "HOLD":
            return _result(
                "HOLD", signal, ai_used,
                f"🤖 IA noticias HOLD (conf={ai_confidence}): {ai_reason}",
                ai_confidence=ai_confidence,
                ai_reason=ai_reason,
                ef_penalty=ef_penalty,
            )

    if adjusted_score < AI_HOLD_OVERRIDE_SCORE:
        return _result(
            "HOLD", signal, ai_used,
            f"Score ajustado {adjusted_score:.1f} < {AI_HOLD_OVERRIDE_SCORE} (delta noticias={effective_score_delta:+.1f})",
            ai_confidence=ai_confidence,
            ai_reason=ai_reason,
            ef_penalty=ef_penalty,
        )

    action = "BUY" if signal.signal == "LONG" else "SELL"
    return _result(
        action, signal, ai_used,
        ef_result.reason,
        ai_confidence=ai_confidence,
        ai_reason=ai_reason,
        ef_penalty=ef_penalty,
    )


def _result(
    action: str,
    signal: Optional[SignalResult],
    ai_used: bool,
    reason: str,
    ai_confidence: int = 0,
    ai_reason: str = "",
    ef_penalty: int = 0,
) -> dict:
    sb = format_signal_block(signal) if signal else ""
    return {
        "action":        action,
        "signal":        signal,
        "ai_used":       ai_used,
        "reason":        reason,
        "signal_block":  sb,
        "ai_confidence": ai_confidence,
        "ai_reason":     ai_reason,
        "ef_penalty":    ef_penalty,
    }
