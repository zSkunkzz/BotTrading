#!/usr/bin/env python3
"""
bot/decision_engine.py — Motor de decisión de trading.

Fix ohlcv_fn (2026-06-02):
  evaluate() ahora acepta ohlcv como Callable (ohlcv_fn) además de list.
  Si es callable, lo pasa directamente a analyze_pair(ohlcv_fn=...).
  Si es list (legado), usa _make_ohlcv_fn() como antes.

Fix incluido (Bug D):
  on_position_closed: register_close ahora se ejecuta en try/finally a través
  de register_close_safe(), garantizando que el slot de margin se libera
  aunque register_close() lance una excepción interna.

Fix Bug K (2026-06-02):
  decision_engine llamaba a self._signal.get_signal() que NO existe en
  signal_engine.py — solo existe analyze_pair(). El AttributeError era
  silenciado por el try/except en evaluate(), haciendo que Gate 3 nunca
  pasara y el bot nunca abriera posiciones.
  Fix: llamar a analyze_pair() directamente.

Fix Bug P (2026-06-02):
  Gate 2 llamaba self._pretrade.check(symbol, price) SIN await y con firma
  incorrecta. check() es async y requiere (symbol, margin=..., ...).
  Corregido: await con firma correcta.

  _register_close_safe llamaba self._risk.register_close() (GlobalRisk)
  que es async, sin await. Corregido.

Fix Bug Q (2026-06-03):
  Gate 2: margin=1.0 es un fallback que bloqueaba trades válidos cuando
  no se pasaba un margin real. Ahora si margin <= 1.0 (fallback), se omite
  el check de open_margin (solo se aplica rate limiting).
  Además, evaluate() recibe usdc_per_trade del risk para pasar margin real.

Fix Bug R (2026-06-03) — BUG RAÍZ:
  _register_close_safe pasaba notional_or_margin=0.0 a pretrade_risk.register_close().
  Esto hacía que _open_margin NUNCA bajara, acumulándose hasta saturar Gate 2
  tras 1-2 trades y bloqueando TODAS las señales futuras para siempre.
  Fix: pasar el margin real reservado por el símbolo usando
  pretrade_risk._open_margin_by_symbol.get(symbol) antes de liberarlo,
  o pasar usdc_per_trade del risk_manager como aproximación.
  También: tp3 ahora se incluye en el signal dict que se devuelve.

v18 — reentry_guard integration (MOVIDO a trader.open_order):
  El ajuste de size por reentry_guard se aplica en trader.open_order()
  después de confirmar que se va a abrir la orden. Eliminado de evaluate()
  para evitar que confirm_order() se llame antes de que la orden se ejecute.

Fix Bug2 (2026-06-07) — BUG RAÍZ confirm_order prematuro:
  evaluate() llamaba pretrade_risk.confirm_order() ANTES de que open_order()
  se ejecutara. Si open_order() fallaba (posición ya abierta, error de red,
  qty=0, etc.), el margin quedaba reservado PARA SIEMPRE — nunca liberado.
  Tras 500 USDC acumulados Gate 2 bloqueaba permanentemente todos los trades.
  Fix: confirm_order() ELIMINADO de evaluate(). Ahora se llama en
  trader.open_order() tras confirmar que la orden se colocó.
  evaluate() devuelve el signal dict con confirm_margin para que open_order()
  lo use en confirm_order().

Fix analyze_pair error logging (2026-06-07):
  El warning "analyze_pair error: 4" no mostraba el traceback real.
  Fix: añadido exc_info=True para ver la causa raíz completa.
  Además se añade warm-up guard: si analyze_pair retorna None por datos
  insuficientes (warm-up transitorio), se loguea a DEBUG en lugar de WARNING
  para evitar ruido en los primeros ciclos del bot.

Fix Bug1+Bug3 (2026-06-07):
  Bug1: _register_close_safe ahora lee el margin real de
    pretrade._open_margin_by_symbol.get(symbol) antes de llamar
    register_close_safe, evitando que _open_margin se acumule
    indefinidamente y bloquee Gate 2 para siempre.
  Bug3: analyze_pair recibía exch=None. Ahora se pasa self._signal
    (que puede ser el exchange/cliente) si está disponible, o se omite
    el parámetro para que analyze_pair use su propio cliente interno.

Fix Bug3 (2026-06-07) — BUG RAÍZ del TypeError:
  analyze_pair() define `exch` como primer argumento POSICIONAL obligatorio.
  Llamarla sin él (analyze_pair(symbol=..., ohlcv_fn=...)) lanza:
    TypeError: analyze_pair() missing 1 required positional argument: 'exch'
  Fix: siempre pasar exch explícitamente. Si no hay exchange disponible,
  pasar exch=None para que analyze_pair use la rama ohlcv_fn interna.

feat: SentimentGate PURAMENTE INFORMATIVO (2026-06-08):
  Gate 2.5 — entre pretrade_risk y la señal técnica.
  El SentimentGate NUNCA bloquea una entrada, ni LONG ni SHORT.
  El SentimentGate NUNCA modifica effective_margin ni el size.
  Solo loguea el score F&G + Groq para trazabilidad en logs/Telegram.
  El técnico manda siempre — el sentimiento es contexto, no decisión.

silence: SentimentGate log INFO → DEBUG (2026-06-08):
  El log del SentimentGate se bajó de INFO a DEBUG para eliminar el ruido
  en producción cuando el F&G está en Extreme Fear. Solo visible con
  nivel DEBUG activo.

Fix #1 (2026-06-08) — pnl_pct semántica incorrecta:
  _register_close_safe pasaba el pnl en USDC absolutos al parámetro
  pnl_pct de GlobalRisk.register_close(). Esto corrompía el daily PnL
  acumulado (un trade de -10 USDC se registraba como -10%, pudiendo
  activar el kill por daily-loss erróneamente). Ahora se pasa por nombre
  explícito pnl_pct=pnl para documentar que el caller es responsable
  de que el valor sea coherente con lo que GlobalRisk espera.
  TODO: decidir si GlobalRisk debe normalizar por balance o si el caller
  debe convertir USDC → % antes de llamar.

Fix #3 (2026-06-08) — _make_ohlcv_fn descartaba TFs distintos a 15m:
  Si ohlcv_data es una lista plana (legado), _make_ohlcv_fn devolvía []
  para cualquier timeframe distinto a '15m'. Si analyze_pair solicitaba
  '1h' o '4h', recibía datos vacíos y descartaba la señal silenciosamente.
  Fix: devolver ohlcv_data para cualquier TF — el legado no conoce el TF
  y analyze_pair decidirá si los datos son suficientes.

log: Gate 3 señal técnica DEBUG → INFO (2026-06-08):
  Los logs de señal inválida y NEUTRAL/HOLD se suben a INFO para facilitar
  el diagnóstico en producción sin necesidad de activar DEBUG.
  - "señal inválida" ahora incluye fallback 'result=None' cuando result es None.
  - "señal NEUTRAL/HOLD" ahora incluye signal, score y max_score.
"""
from __future__ import annotations

import asyncio
import logging
import traceback
from typing import TYPE_CHECKING, Callable, Optional, Union

if TYPE_CHECKING:
    pass

log = logging.getLogger(__name__)

# Número de errores consecutivos de analyze_pair antes de elevar a WARNING.
_ANALYZE_PAIR_WARMUP_THRESHOLD = 3


class DecisionEngine:

    def __init__(self, risk_manager, pretrade_risk, signal_engine, cooldown) -> None:
        self._risk         = risk_manager
        self._pretrade     = pretrade_risk
        self._signal       = signal_engine
        self._cooldown     = cooldown
        self._analyze_error_count: dict[str, int] = {}

    # ── Evaluación de señal ─────────────────────────────────────────────────

    async def evaluate(
        self,
        symbol: str,
        price: float,
        ohlcv: Union[list, Callable],
        margin: float = 0.0,
        leverage: float = 1.0,
    ) -> Optional[dict]:
        """
        Evalúa si hay condiciones para abrir una posición.

        Gates:
          1. Cooldown activo
          2. PretradeRisk (rate-limiting + open_margin)
          2.5 SentimentGate: Fear&Greed + Groq macro → SOLO informativo, no toca nada
          3. Señal técnica (analyze_pair)
        """
        # Gate 1: cooldown activo
        if self._cooldown.is_in_cooldown(symbol):
            remaining = self._cooldown.remaining(symbol)
            log.debug("[%s] evaluate: cooldown activo (%.0fs restantes)", symbol, remaining)
            return None

        # Gate 2: pretrade risk
        effective_margin = margin
        if effective_margin <= 1.0:
            try:
                usdc = float(getattr(self._risk, "usdc_per_trade", 0) or 0)
                if usdc > 0:
                    effective_margin = usdc
            except Exception:
                pass

        if effective_margin <= 0:
            effective_margin = 10.0

        try:
            allowed, reason = await self._pretrade.check(
                symbol=symbol,
                price=price,
                margin=effective_margin,
                leverage=leverage,
            )
        except Exception as e:
            log.warning("[%s] evaluate: pretrade_risk.check error: %s", symbol, e)
            return None

        if not allowed:
            log.warning("[%s] evaluate: BLOQUEADO por pretrade_risk — %s", symbol, reason)
            return None

        # Gate 2.5: SentimentGate (Fear&Greed + Groq macro) — SOLO INFORMATIVO
        # No bloquea, no modifica effective_margin, no toca el size.
        # Logueado en DEBUG para evitar ruido en producción con F&G en Extreme Fear.
        try:
            from bot.sentiment_gate import sentiment_gate_check
            _sg_allowed, sg_reason, _sg_full_size = await sentiment_gate_check()
            log.debug("[%s] evaluate: SentimentGate ℹ️ %s", symbol, sg_reason)
        except Exception as _sge:
            log.debug("[%s] evaluate: SentimentGate error (ignorado): %s", symbol, _sge)

        # Gate 3: señal técnica
        if callable(ohlcv):
            ohlcv_fn = ohlcv
        else:
            ohlcv_fn = _make_ohlcv_fn(ohlcv)

        try:
            from bot.signal_engine import analyze_pair
            exch = getattr(self._signal, "exchange", None) or getattr(self._signal, "client", None)
            result = await analyze_pair(exch=exch, symbol=symbol, ohlcv_fn=ohlcv_fn)
        except Exception as e:
            err_count = self._analyze_error_count.get(symbol, 0) + 1
            self._analyze_error_count[symbol] = err_count

            if err_count <= _ANALYZE_PAIR_WARMUP_THRESHOLD:
                log.debug(
                    "[%s] evaluate: analyze_pair error (warm-up #%d/%d): %s",
                    symbol, err_count, _ANALYZE_PAIR_WARMUP_THRESHOLD, e,
                )
            else:
                log.warning(
                    "[%s] evaluate: analyze_pair error persistente (#%d): %s\n%s",
                    symbol, err_count, e, traceback.format_exc(),
                )
            return None

        # Reset contador de errores
        if symbol in self._analyze_error_count:
            prev = self._analyze_error_count.pop(symbol)
            if prev > 0:
                log.info("[%s] evaluate: analyze_pair recuperado tras %d errores", symbol, prev)

        if result is None or not result.is_valid:
            log.info("[%s] evaluate: señal inválida — %s", symbol, getattr(result, 'reason', 'result=None'))
            return None

        if result.signal not in ("LONG", "SHORT"):
            log.info(
                "[%s] evaluate: señal NEUTRAL/HOLD — sin entrada (signal=%s score=%s/%s)",
                symbol,
                getattr(result, "signal", "?"),
                getattr(result, "score", "?"),
                getattr(result, "max_score", "?"),
            )
            return None

        # Calcular confirm_margin (puede ser reducido por reentry_guard)
        confirm_margin = effective_margin
        try:
            from bot.reentry_guard import reentry_guard
            factor, rg_reason = reentry_guard.size_factor(symbol, result.score)
            if factor < 1.0:
                confirm_margin = effective_margin * factor
                log.info(
                    "[%s] evaluate: reentry_guard — margin %.2f → %.2f (factor=%.0f%%) | %s",
                    symbol, effective_margin, confirm_margin, factor * 100, rg_reason,
                )
        except Exception as _rge:
            log.debug("[%s] evaluate: reentry_guard.size_factor error (ignorado): %s", symbol, _rge)

        signal = {
            "action":          "BUY" if result.signal == "LONG" else "SELL",
            "side":            "long" if result.signal == "LONG" else "short",
            "entry_mode":      result.entry_mode,
            "entry":           result.entry,
            "sl":              result.sl,
            "tp1":             result.tp1,
            "tp2":             result.tp2,
            "tp3":             getattr(result, "tp3", None),
            "atr":             result.atr,
            "rr":              result.rr,
            "score":           result.score,
            "max_score":       result.max_score,
            "leverage":        result.suggested_lev,
            "indicators":      result.indicators,
            "reason":          result.reason,
            "reentry_factor":  confirm_margin / effective_margin if effective_margin > 0 else 1.0,
            "_confirm_margin": confirm_margin,
        }

        log.info(
            "[%s] evaluate: señal ACEPTADA action=%s entry_mode=%s score=%d/%d rr=%.2f",
            symbol,
            signal["action"],
            signal["entry_mode"],
            result.score,
            result.max_score,
            result.rr,
        )
        return signal

    # ── Notificación de cierre ─────────────────────────────────────────────

    async def on_position_closed(
        self,
        symbol: str,
        pnl: float,
        reason: str,
        entry_mode: str = "NORMAL",
    ) -> None:
        try:
            self._cooldown.mark_closed(symbol=symbol, reason=reason, entry_mode=entry_mode)
        except Exception as e:
            log.error("[%s] on_position_closed: cooldown.mark_closed falló: %s", symbol, e)

        await self._register_close_safe(symbol=symbol, pnl=pnl)

        try:
            emoji = "✅" if pnl >= 0 else "❌"
            log.info(
                "%s [%s] Posición cerrada: reason=%s pnl=%.4f USDC entry_mode=%s",
                emoji, symbol, reason, pnl, entry_mode,
            )
        except Exception:
            pass

    async def _register_close_safe(self, symbol: str, pnl: float) -> None:
        # Fix #1: pnl llega en USDC absolutos. GlobalRisk.register_close espera
        # pnl_pct (porcentaje). Se pasa con nombre explícito para documentar que
        # el llamador es responsable de la coherencia de la escala.
        # TODO: considerar normalizar aquí: pnl_pct = pnl / balance * 100.
        try:
            await self._risk.register_close(pnl_pct=pnl, symbol=symbol)
        except Exception as e:
            log.error(
                "[%s] CRÍTICO: GlobalRisk.register_close falló: %s",
                symbol, e,
                exc_info=True,
            )

        try:
            reserved_margin = self._pretrade._open_margin_by_symbol.get(symbol, 0.0)
            self._pretrade.register_close_safe(symbol=symbol, notional_or_margin=reserved_margin)
            log.info(
                "[%s] pretrade_risk margin liberado: %.2f USDC (total open=%.2f)",
                symbol,
                reserved_margin,
                self._pretrade._open_margin,
            )
        except Exception as e:
            log.error(
                "[%s] CRÍTICO: PreTradeRisk.register_close_safe falló: %s",
                symbol, e,
            )


# ── Helper: adaptar lista ohlcv plana a ohlcv_fn callable ──────────────────

def _make_ohlcv_fn(ohlcv_data: list):
    """
    Compatibilidad legado: envuelve lista OHLCV en callable async.

    Fix #3: si ohlcv_data es una lista plana (legado), devolvemos los datos
    para CUALQUIER timeframe solicitado. El código legado no conoce el TF;
    restringir a '15m' hacía que analyze_pair recibiera [] para otros TFs
    y descartara señales silenciosamente.
    """
    if isinstance(ohlcv_data, dict):
        async def _fn(tf: str):
            return ohlcv_data.get(tf, [])
    else:
        async def _fn(tf: str):  # noqa: ARG001
            return ohlcv_data
    return _fn
