#!/usr/bin/env python3
"""
bot/decision_engine.py — Cálculo de tamaño de posición, sizing y orquestación de señales.

v12 — Fix CRÍTICO: NEUTRAL ya no llega al trader.
  - evaluate() descarta explícitamente señales con signal/side == NEUTRAL/HOLD
    antes de devolver el dict, evitando que trader.py abra un SHORT silencioso.
  - analyze_pair añadido como cuarto método soportado en signal_engine
    (fix para error 'no tiene evaluate/evaluate_signal/get_signal').

v11 — Fix CRÍTICO: SignalResult (dataclass) ya no se descarta silenciosamente.
  - evaluate() convierte dataclass → dict via dataclasses.asdict() antes de
    rechazar el resultado. También normaliza el campo 'side' desde 'signal'
    si falta, para que trader.py reciba siempre side=long/short.

v10 — Kelly sizing:
  - compute_kelly_fraction(win_rate, avg_win, avg_loss) calcula la fracción
    óptima de Kelly y la limita en [KELLY_MIN_FRACTION, KELLY_MAX_FRACTION].
  - calc_position_size() acepta kelly_stats opcional. Si se provee y
    KELLY_ENABLED=true, multiplica el tamaño base por la fracción de Kelly.
  - Config: KELLY_ENABLED (default false), KELLY_MIN_FRACTION (default 0.05),
            KELLY_MAX_FRACTION (default 0.25).

Clase DecisionEngine:
  Fachada que orquesta señales (signal_engine.analyze_pair), pretrade_risk
  y sizing. Inyectada por TradingLoop._build_decision_engine() con:
    DecisionEngine(
        risk_manager  = risk,       # RiskManager
        pretrade_risk = ...,        # PretradeRisk singleton
        signal_engine = ...,        # módulo signal_engine
        cooldown      = ...,        # SignalCooldown
    )
"""

import dataclasses
import inspect
import logging
import os
from typing import Optional

log = logging.getLogger(__name__)

# ─ Config general ───────────────────────────────────────────────────────────────
_CAPITAL               = float(os.getenv("CAPITAL",                "100.0"))
_MAX_RISK_PCT          = float(os.getenv("MAX_RISK_PCT",           "0.01"))
_MAX_LEVERAGE          = int(os.getenv("MAX_LEVERAGE",             "10"))
_EF_PENALTY_REDUCTION  = float(os.getenv("EF_PENALTY_REDUCTION",   "0.10"))

# ─ Kelly ────────────────────────────────────────────────────────────────────────
_KELLY_ENABLED      = os.getenv("KELLY_ENABLED",     "false").lower() not in ("false", "0", "no")
_KELLY_MIN_FRACTION = float(os.getenv("KELLY_MIN_FRACTION", "0.05"))
_KELLY_MAX_FRACTION = float(os.getenv("KELLY_MAX_FRACTION", "0.25"))

# Valores de señal que significan "sin entrada" — nunca deben llegar al trader
_NEUTRAL_SIGNALS = frozenset({"neutral", "hold", "none", "", "0"})


def compute_kelly_fraction(
    win_rate: float,
    avg_win:  float,
    avg_loss: float,
) -> float:
    """
    Fracción de Kelly = (p * b - q) / b
    Limitada a [KELLY_MIN_FRACTION, KELLY_MAX_FRACTION].
    """
    if avg_loss <= 0 or win_rate <= 0 or win_rate >= 1:
        log.debug("[kelly] inputs inválidos: win_rate=%.3f avg_win=%.4f avg_loss=%.4f",
                  win_rate, avg_win, avg_loss)
        return _KELLY_MIN_FRACTION

    b = avg_win / avg_loss
    q = 1.0 - win_rate
    kelly = (win_rate * b - q) / b

    if kelly <= 0:
        log.debug("[kelly] EV negativo (kelly=%.4f) — usando mínimo %.3f", kelly, _KELLY_MIN_FRACTION)
        return _KELLY_MIN_FRACTION

    clipped = max(_KELLY_MIN_FRACTION, min(_KELLY_MAX_FRACTION, kelly))
    log.debug("[kelly] raw=%.4f → clipped=%.4f", kelly, clipped)
    return clipped


def calc_position_size(
    entry:       float,
    sl:          float,
    leverage:    int,
    capital:     Optional[float] = None,
    ef_penalty:  int             = 0,
    kelly_stats: Optional[dict]  = None,
) -> float:
    """Calcula el tamaño de posición (en contratos/monedas)."""
    _cap = capital if capital is not None else _CAPITAL
    if entry <= 0 or sl <= 0 or entry == sl:
        log.warning("[decision_engine] entry/sl inválidos: entry=%.6f sl=%.6f", entry, sl)
        return 0.0

    risk_per_unit = abs(entry - sl)
    base_risk     = _cap * _MAX_RISK_PCT

    kelly_fraction = 1.0
    if _KELLY_ENABLED and kelly_stats:
        kelly_fraction = compute_kelly_fraction(
            win_rate = float(kelly_stats.get("win_rate",  0.5)),
            avg_win  = float(kelly_stats.get("avg_win",   1.0)),
            avg_loss = float(kelly_stats.get("avg_loss",  1.0)),
        )
        log.info(
            "[decision_engine] Kelly fraction=%.4f (win_rate=%.3f avg_win=%.4f avg_loss=%.4f)",
            kelly_fraction,
            kelly_stats.get("win_rate", 0.5),
            kelly_stats.get("avg_win",  1.0),
            kelly_stats.get("avg_loss", 1.0),
        )

    effective_risk = base_risk * kelly_fraction

    if ef_penalty > 0:
        factor = max(0.0, 1.0 - ef_penalty * _EF_PENALTY_REDUCTION)
        effective_risk *= factor
        log.info(
            "[decision_engine] ef_penalty=%d → riesgo reducido x%.2f (%.4f → %.4f)",
            ef_penalty, factor, base_risk, effective_risk,
        )

    lev = min(leverage, _MAX_LEVERAGE)
    qty = effective_risk / (risk_per_unit * lev)

    log.debug(
        "[decision_engine] entry=%.6f sl=%.6f lev=%d kelly=%.4f risk=%.4f qty=%.6f",
        entry, sl, lev, kelly_fraction, effective_risk, qty,
    )
    return qty


def _signal_result_to_dict(result) -> Optional[dict]:
    """
    v11 FIX: convierte un SignalResult (dataclass) a dict.
    Si ya es dict, lo devuelve tal cual.
    Si es None, devuelve None.
    Si es dataclass, usa dataclasses.asdict().
    Normaliza el campo 'side' desde 'signal' (LONG/SHORT → long/short)
    para que trader.py siempre reciba side= correcto.
    """
    if result is None:
        return None
    if isinstance(result, dict):
        d = result
    elif dataclasses.is_dataclass(result) and not isinstance(result, type):
        try:
            d = dataclasses.asdict(result)
        except Exception as e:
            log.warning("[DecisionEngine] asdict() falló (%s) — intentando __dict__", e)
            try:
                d = dict(vars(result))
            except Exception:
                return None
    else:
        # Último intento: __dict__
        try:
            d = dict(vars(result))
        except Exception:
            return None

    # Normalizar 'side' desde 'signal' si falta
    # SignalResult tiene .signal = "LONG" | "SHORT"
    if not d.get("side") and d.get("signal"):
        raw = str(d["signal"]).lower()
        if "long" in raw or raw == "buy":
            d["side"] = "long"
        elif "short" in raw or raw == "sell":
            d["side"] = "short"
        log.debug(
            "[DecisionEngine] normalizado side=%s desde signal=%s",
            d.get("side"), d.get("signal"),
        )

    return d


class DecisionEngine:
    """
    Orquestador de señales para TradingLoop.

    Inyectado desde TradingLoop._build_decision_engine() con:
        DecisionEngine(
            risk_manager  = risk,
            pretrade_risk = pretrade_risk_singleton,
            signal_engine = signal_engine_module,
            cooldown      = signal_cooldown,
        )

    Métodos públicos:
        await evaluate(symbol, price, ohlcv_fn)  → dict | None
        await on_position_closed(symbol, pnl, reason, entry_mode)
        calc_position_size(...)                  → float  (delegado)
        compute_kelly_fraction(...)              → float  (delegado)
    """

    def __init__(
        self,
        risk_manager=None,
        pretrade_risk=None,
        signal_engine=None,
        cooldown=None,
    ):
        self._risk_manager  = risk_manager
        self._pretrade_risk = pretrade_risk
        self._signal_engine = signal_engine
        self._cooldown      = cooldown

    # ── Interface principal ───────────────────────────────────────────────────

    async def evaluate(self, symbol: str, price: float, ohlcv_fn) -> Optional[dict]:
        """
        Evalúa la señal para el símbolo delegando en signal_engine.
        Retorna dict de señal o None si no hay entrada.

        Métodos soportados en signal_engine (por orden de prioridad):
          evaluate(), evaluate_signal(), get_signal(), analyze_pair()
        Tanto síncronos como async.

        v12 FIX: descarta explícitamente señales NEUTRAL/HOLD antes de
          devolver al trader, evitando la apertura de SHORT silenciosos.
        v11 FIX: si el resultado es un dataclass (SignalResult), se convierte
          a dict via _signal_result_to_dict() en lugar de descartarlo.
        """
        if self._signal_engine is None:
            log.warning("[DecisionEngine] signal_engine no inyectado para %s", symbol)
            return None

        try:
            se = self._signal_engine

            # Determinar qué método usar (v12: añade analyze_pair)
            if hasattr(se, "evaluate"):
                fn = se.evaluate
                fn_name = "evaluate"
            elif hasattr(se, "evaluate_signal"):
                fn = se.evaluate_signal
                fn_name = "evaluate_signal"
            elif hasattr(se, "get_signal"):
                fn = se.get_signal
                fn_name = "get_signal"
            elif hasattr(se, "analyze_pair"):
                fn = se.analyze_pair
                fn_name = "analyze_pair"
            else:
                log.error(
                    "[DecisionEngine] signal_engine no tiene "
                    "evaluate/evaluate_signal/get_signal/analyze_pair"
                )
                return None

            log.debug("[DecisionEngine] llamando %s(%s, price=%.6f)", fn_name, symbol, price)

            # Llamar la función — puede ser sync o async
            result = fn(symbol, price, ohlcv_fn)

            # Si devuelve una coroutine, esperarla
            if inspect.isawaitable(result):
                result = await result

            if result is None:
                log.debug("[DecisionEngine] %s → None (sin señal)", symbol)
                return None

            # v11 FIX: convertir dataclass → dict antes de rechazar por tipo
            if not isinstance(result, dict):
                converted = _signal_result_to_dict(result)
                if converted is None:
                    log.warning(
                        "[DecisionEngine] %s → tipo inesperado %s y no convertible: %r",
                        symbol, type(result).__name__, result,
                    )
                    return None
                log.debug(
                    "[DecisionEngine] %s → convertido %s → dict OK",
                    symbol, type(result).__name__,
                )
                result = converted

            # v12 FIX: descartar señales NEUTRAL/HOLD — nunca deben llegar al trader
            sig_val = str(
                result.get("signal") or result.get("side") or ""
            ).strip().lower()
            if sig_val in _NEUTRAL_SIGNALS:
                log.debug(
                    "[DecisionEngine] %s → signal=%r (NEUTRAL/HOLD) — descartado, sin entrada",
                    symbol, result.get("signal") or result.get("side"),
                )
                return None

            # Normalizar side también si el result ya era dict pero le falta side
            if not result.get("side") and result.get("signal"):
                raw = str(result["signal"]).lower()
                if "long" in raw or raw == "buy":
                    result["side"] = "long"
                elif "short" in raw or raw == "sell":
                    result["side"] = "short"

            log.debug("[DecisionEngine] %s → señal OK: %s", symbol, result)
            return result

        except Exception as exc:
            log.error("[DecisionEngine] evaluate(%s) EXCEPCIÓN: %s", symbol, exc, exc_info=True)
            return None

    async def on_position_closed(
        self,
        symbol:     str,
        pnl:        float,
        reason:     str = "",
        entry_mode: str = "",
    ) -> None:
        """
        Notifica el cierre de posición a pretrade_risk (libera slot)
        y al cooldown si corresponde.
        """
        if self._pretrade_risk is not None:
            try:
                fn = getattr(self._pretrade_risk, "on_position_closed", None)
                if callable(fn):
                    res = fn(symbol=symbol, pnl=pnl, reason=reason, entry_mode=entry_mode)
                    if inspect.isawaitable(res):
                        await res
            except Exception as exc:
                log.warning(
                    "[DecisionEngine] pretrade_risk.on_position_closed error: %s", exc
                )

        if self._cooldown is not None:
            try:
                fn = getattr(self._cooldown, "register_close", None)
                if callable(fn):
                    res = fn(symbol)
                    if inspect.isawaitable(res):
                        await res
            except Exception as exc:
                log.debug("[DecisionEngine] cooldown.register_close error: %s", exc)

    # ── Delegados de sizing ───────────────────────────────────────────────────

    def calc_position_size(
        self,
        entry:       float,
        sl:          float,
        leverage:    int,
        capital:     Optional[float] = None,
        ef_penalty:  int             = 0,
        kelly_stats: Optional[dict]  = None,
    ) -> float:
        return calc_position_size(
            entry=entry, sl=sl, leverage=leverage,
            capital=capital, ef_penalty=ef_penalty, kelly_stats=kelly_stats,
        )

    def compute_kelly_fraction(
        self,
        win_rate: float,
        avg_win:  float,
        avg_loss: float,
    ) -> float:
        return compute_kelly_fraction(
            win_rate=win_rate, avg_win=avg_win, avg_loss=avg_loss,
        )
