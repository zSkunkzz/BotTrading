"""
shadow_mode.py — Modo sombra: estrategia en paralelo sin órdenes reales.

Registra trades teóricos al mismo tiempo que los reales,
permitiendo comparar señal teórica vs ejecución real y detectar drift.

#8 Feedback win-rate → sizing:
  - ShadowTrade incluye entry_mode (STRONG/NORMAL/EARLY).
  - win_rate_by_mode()     → dict {modo: win_rate}
  - sizing_multiplier(mode) → factor de ajuste de tamaño según win-rate.
    Formula: clamp(win_rate / WIN_RATE_BASELINE, MIN_MULT, MAX_MULT)
    Ejemplo: STRONG 70% WR → 70/55 = 1.27×; EARLY 30% WR → 30/55 = 0.54× → clamped 0.6×

Variables de entorno:
  SHADOW_MODE_ENABLED        Activar shadow mode (1/0)  (default 0)
  SHADOW_SIZING_ENABLED      Activar sizing dinámico    (default 0)
  SHADOW_WIN_RATE_BASELINE   Win-rate base (default 0.55)
  SHADOW_MIN_MULT            Mínimo multiplicador       (default 0.6)
  SHADOW_MAX_MULT            Máximo multiplicador        (default 1.5)
  SHADOW_MIN_SAMPLES         Trades mínimos para ajustar sizing (default 20)

Uso:
  from bot.shadow_mode import shadow_mode
  shadow_mode.record_signal(symbol, side, price, sl, tp3, entry_mode="STRONG")
  shadow_mode.record_real_open(symbol, side, fill_price)
  shadow_mode.record_close(symbol, exit_price, real_pnl_pct)
  mult = shadow_mode.sizing_multiplier("STRONG")   # 1.0 si no hay datos
  report = shadow_mode.get_drift_report(symbol)
  stats  = shadow_mode.get_mode_stats()            # {modo: {trades, win_rate}}
"""
from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass, field
from typing import Optional

logger = logging.getLogger("ShadowMode")


@dataclass
class ShadowTrade:
    symbol:         str
    side:           str
    signal_price:   float
    sl:             Optional[float]
    tp:             Optional[float]
    entry_mode:     str             = ""     # #8 STRONG / NORMAL / EARLY
    real_fill:      Optional[float] = None
    theory_pnl:     float           = 0.0
    real_pnl:       float           = 0.0
    drift:          float           = 0.0
    closed:         bool            = False
    ts_open:        float           = field(default_factory=time.monotonic)
    ts_close:       float           = 0.0


class ShadowMode:
    def __init__(self) -> None:
        self.enabled         = bool(int(os.getenv("SHADOW_MODE_ENABLED",   "0")))
        self.sizing_enabled  = bool(int(os.getenv("SHADOW_SIZING_ENABLED", "0")))
        self._win_rate_base  = float(os.getenv("SHADOW_WIN_RATE_BASELINE", "0.55"))
        self._min_mult       = float(os.getenv("SHADOW_MIN_MULT",          "0.6"))
        self._max_mult       = float(os.getenv("SHADOW_MAX_MULT",          "1.5"))
        self._min_samples    = int(os.getenv("SHADOW_MIN_SAMPLES",         "20"))
        # {symbol: ShadowTrade abierto}
        self._open:    dict[str, ShadowTrade] = {}
        # historial cerrado
        self._history: list[ShadowTrade]      = []

    # ── API pública ────────────────────────────────────────────────────

    def record_signal(
        self,
        symbol:     str,
        side:       str,
        price:      float,
        sl:         float | None = None,
        tp:         float | None = None,
        entry_mode: str          = "",   # #8
    ) -> None:
        """Registrar una señal (al mismo tiempo que se ejecuta la orden real)."""
        if not self.enabled:
            return
        sym = symbol.replace("/", "").replace(":USDT", "")
        self._open[sym] = ShadowTrade(
            symbol=sym, side=side, signal_price=price,
            sl=sl, tp=tp, entry_mode=(entry_mode or "").upper(),
        )
        logger.debug(f"[Shadow:{sym}] Señal registrada: {side} @ {price} [modo={entry_mode}]")

    def record_real_open(self, symbol: str, fill_price: float) -> None:
        """Registrar el fill real para calcular slippage teórico."""
        if not self.enabled:
            return
        sym = symbol.replace("/", "").replace(":USDT", "")
        t = self._open.get(sym)
        if t:
            t.real_fill = fill_price
            logger.debug(f"[Shadow:{sym}] Fill real: {fill_price} (señal: {t.signal_price})")

    def record_close(
        self,
        symbol:       str,
        exit_price:   float,
        real_pnl_pct: float,
    ) -> None:
        """Cerrar la posición sombra y calcular drift."""
        if not self.enabled:
            return
        sym = symbol.replace("/", "").replace(":USDT", "")
        t = self._open.pop(sym, None)
        if not t:
            return

        entry = t.signal_price
        if entry > 0:
            if t.side in ("buy", "long"):
                t.theory_pnl = (exit_price - entry) / entry * 100
            else:
                t.theory_pnl = (entry - exit_price) / entry * 100

        t.real_pnl = real_pnl_pct
        t.drift    = real_pnl_pct - t.theory_pnl
        t.closed   = True
        t.ts_close = time.monotonic()
        self._history.append(t)

        logger.info(
            f"[Shadow:{sym}] Cerrado [modo={t.entry_mode}] | "
            f"theory={t.theory_pnl:+.2f}% real={t.real_pnl:+.2f}% drift={t.drift:+.2f}%"
        )

    # ── #8 Win-rate y sizing ──────────────────────────────────────────────

    def win_rate_by_mode(self) -> dict[str, float]:
        """
        Calcula el win-rate (trades con real_pnl > 0) por entry_mode.
        Solo incluye modos con al menos _min_samples trades.
        Devuelve {} si no hay suficientes datos.
        """
        buckets: dict[str, list[bool]] = {}
        for t in self._history:
            mode = t.entry_mode or "UNKNOWN"
            buckets.setdefault(mode, []).append(t.real_pnl > 0)

        result: dict[str, float] = {}
        for mode, outcomes in buckets.items():
            if len(outcomes) >= self._min_samples:
                result[mode] = sum(outcomes) / len(outcomes)
        return result

    def sizing_multiplier(self, entry_mode: str) -> float:
        """
        Devuelve el multiplicador de tamaño para el entry_mode dado.

        Formula:
          mult = win_rate / WIN_RATE_BASELINE
          mult = clamp(mult, MIN_MULT, MAX_MULT)

        Ejemplos con baseline=0.55:
          STRONG 70% WR → 70/55 = 1.27×
          NORMAL 55% WR → 55/55 = 1.00×
          EARLY  35% WR → 35/55 = 0.64×

        Devuelve 1.0 si:
          - sizing_enabled está desactivado
          - no hay suficientes trades para el modo
          - el modo no existe en el historial
        """
        if not self.sizing_enabled:
            return 1.0
        mode = (entry_mode or "").upper()
        wr = self.win_rate_by_mode().get(mode)
        if wr is None:
            return 1.0
        mult = wr / self._win_rate_base
        return max(self._min_mult, min(self._max_mult, mult))

    def get_mode_stats(self) -> dict[str, dict]:
        """
        Resumen completo por modo para logs/Telegram.
        {
          "STRONG": {"trades": 42, "wins": 30, "win_rate": 0.71, "mult": 1.29},
          ...
        }
        """
        buckets: dict[str, list[float]] = {}
        for t in self._history:
            mode = t.entry_mode or "UNKNOWN"
            buckets.setdefault(mode, []).append(t.real_pnl)

        result = {}
        for mode, pnls in buckets.items():
            wins     = sum(1 for p in pnls if p > 0)
            wr       = wins / len(pnls) if pnls else 0.0
            mult     = self.sizing_multiplier(mode)
            result[mode] = {
                "trades":   len(pnls),
                "wins":     wins,
                "win_rate": round(wr, 3),
                "mult":     round(mult, 3),
            }
        return result

    # ── Drift report (compatibilidad) ──────────────────────────────────────

    def get_drift_report(self, symbol: str) -> dict:
        """Historial de drift para un símbolo."""
        sym     = symbol.replace("/", "").replace(":USDT", "")
        trades  = [t for t in self._history if t.symbol == sym]
        if not trades:
            return {"symbol": sym, "trades": 0}
        avg_drift = sum(t.drift for t in trades) / len(trades)
        return {
            "symbol":    sym,
            "trades":    len(trades),
            "avg_drift": round(avg_drift, 4),
            "details":   [
                {
                    "side":       t.side,
                    "mode":       t.entry_mode,
                    "signal_px":  t.signal_price,
                    "fill_px":    t.real_fill,
                    "theory_pnl": round(t.theory_pnl, 3),
                    "real_pnl":   round(t.real_pnl, 3),
                    "drift":      round(t.drift, 3),
                }
                for t in trades[-20:]
            ],
        }


# Instancia singleton
shadow_mode = ShadowMode()
