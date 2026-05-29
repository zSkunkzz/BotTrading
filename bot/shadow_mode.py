"""
shadow_mode.py — Modo sombra: estrategia en paralelo sin órdenes reales.

Registra trades teóricos al mismo tiempo que los reales,
permitiendo comparar señal teórica vs ejecución real y detectar drift.

Variables de entorno:
  SHADOW_MODE_ENABLED    Activar shadow mode (1/0)  (default 0)

Uso:
  from bot.shadow_mode import shadow_mode
  shadow_mode.record_signal(symbol, side, price, sl, tp3)
  shadow_mode.record_real_open(symbol, side, fill_price)
  shadow_mode.record_close(symbol, exit_price, real_pnl_pct)
  report = shadow_mode.get_drift_report(symbol)
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
    signal_price:   float           # precio en el momento de la señal
    sl:             Optional[float]
    tp:             Optional[float]
    real_fill:      Optional[float] = None   # precio de fill real
    theory_pnl:     float           = 0.0   # PnL teórico (sin slippage)
    real_pnl:       float           = 0.0   # PnL real comunicado por trader
    drift:          float           = 0.0   # real - theory
    closed:         bool            = False
    ts_open:        float           = field(default_factory=time.monotonic)
    ts_close:       float           = 0.0


class ShadowMode:
    def __init__(self) -> None:
        self.enabled = bool(int(os.getenv("SHADOW_MODE_ENABLED", "0")))
        # {symbol: ShadowTrade abierto}
        self._open: dict[str, ShadowTrade] = {}
        # historial cerrado
        self._history: list[ShadowTrade] = []

    # ── API pública ──────────────────────────────────────────────────────────

    def record_signal(
        self,
        symbol: str,
        side:   str,
        price:  float,
        sl:     float | None = None,
        tp:     float | None = None,
    ) -> None:
        """Registrar una señal (al mismo tiempo que se ejecuta la orden real)."""
        if not self.enabled:
            return
        sym = symbol.replace("/", "").replace(":USDT", "")
        self._open[sym] = ShadowTrade(
            symbol=sym, side=side, signal_price=price, sl=sl, tp=tp
        )
        logger.debug(f"[Shadow:{sym}] Señal registrada: {side} @ {price}")

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
        symbol:        str,
        exit_price:    float,
        real_pnl_pct:  float,
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

        t.real_pnl  = real_pnl_pct
        t.drift     = real_pnl_pct - t.theory_pnl
        t.closed    = True
        t.ts_close  = time.monotonic()
        self._history.append(t)

        logger.info(
            f"[Shadow:{sym}] Cerrado | theory={t.theory_pnl:+.2f}% "
            f"real={t.real