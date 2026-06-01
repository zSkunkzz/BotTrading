"""
bot/pretrade_risk.py  –  Pre-trade risk checks (margin-based, not gross notional).

v3 — BUG #5 FIX: margin bloqueado si cierre falla a mitad
  - _open_margin_by_symbol: dict[symbol -> float] para tracking exacto
  - register_close() libera EXACTAMENTE el margen reservado por symbol,
    no el valor aproximado que pase el caller
  - register_close_safe() wrapper que siempre libera aunque haya excepción
  - confirm_order() registra por symbol para poder liberar correctamente
"""
from __future__ import annotations

import asyncio
import logging
import os
import time
from collections import deque
from typing import Deque, Dict, Optional, Tuple

log = logging.getLogger(__name__)


class PreTradeRisk:

    def __init__(
        self,
        max_open_margin: float = 500.0,
        max_orders_per_window: int = 20,
        window_seconds: float = 60.0,
    ) -> None:
        self._max_open_margin       = max_open_margin
        self._max_orders_per_window = max_orders_per_window
        self._window_seconds        = window_seconds

        self._lock = asyncio.Lock()
        self._open_margin: float = 0.0
        # BUG #5 FIX: tracking por símbolo para liberar exactamente lo reservado
        self._open_margin_by_symbol: Dict[str, float] = {}
        self._order_timestamps: Deque[float] = deque()

    # ── public API ───────────────────────────────────────────────────────

    async def check(
        self,
        symbol: str,
        side: Optional[str] = None,
        margin: float = 0.0,
        notional: float = 0.0,
        leverage: float = 1.0,
        balance: Optional[float] = None,
        price: float = 0.0,
        qty: float = 0.0,
        sl: Optional[float] = None,
    ) -> Tuple[bool, str]:
        if margin <= 0 and notional > 0:
            lev = leverage if leverage and leverage > 0 else 1.0
            margin = notional / lev

        async with self._lock:
            now = time.monotonic()
            cutoff = now - self._window_seconds
            while self._order_timestamps and self._order_timestamps[0] < cutoff:
                self._order_timestamps.popleft()

            if len(self._order_timestamps) >= self._max_orders_per_window:
                reason = (
                    f"Rate limit: {len(self._order_timestamps)} orders en los últimos "
                    f"{self._window_seconds:.0f}s (max {self._max_orders_per_window})"
                )
                log.warning("[PreTradeRisk] %s – %s", symbol, reason)
                return False, reason

            if margin <= 0:
                reason = f"Margin inválido: {margin}"
                log.warning("[PreTradeRisk] %s – %s", symbol, reason)
                return False, reason

            projected = self._open_margin + margin
            if projected > self._max_open_margin:
                reason = (
                    f"Open-margin limit: alcanzaría {projected:.2f} USDC "
                    f"(límite {self._max_open_margin:.2f})"
                )
                log.warning("[PreTradeRisk] %s – %s", symbol, reason)
                return False, reason

            return True, ""

    def confirm_order(self, symbol: str, notional_or_margin: float) -> None:
        """
        BUG #5 FIX: registra margin por symbol para poder liberarlo exactamente.
        """
        margin = notional_or_margin
        # Acumular al margen existente del símbolo (puede haber parciales)
        prev = self._open_margin_by_symbol.get(symbol, 0.0)
        self._open_margin_by_symbol[symbol] = prev + margin
        self._open_margin += margin
        self._order_timestamps.append(time.monotonic())
        log.debug(
            "[PreTradeRisk] confirmed %s +%.2f → total open=%.2f (sym=%.2f)",
            symbol, margin, self._open_margin,
            self._open_margin_by_symbol[symbol],
        )

    def register_close(self, symbol: str, notional_or_margin: float) -> None:
        """
        BUG #5 FIX: libera EXACTAMENTE el margen reservado para este symbol.
        Si el caller pasa un valor diferente al reservado, usamos el reservado.
        Esto evita que un fallo parcial en el cierre deje el margen bloqueado.
        """
        reserved = self._open_margin_by_symbol.pop(symbol, None)
        if reserved is not None:
            # Liberar exactamente lo que se reservó
            release = reserved
        else:
            # Fallback: usar el valor que pasa el caller (compatibilidad)
            release = notional_or_margin

        self._open_margin = max(0.0, self._open_margin - release)
        log.debug(
            "[PreTradeRisk] closed %s -%.2f → total open=%.2f",
            symbol, release, self._open_margin,
        )

    def register_close_safe(self, symbol: str, notional_or_margin: float = 0.0) -> None:
        """
        BUG #5 FIX: wrapper seguro que SIEMPRE libera el margen.
        Llamar en finally-blocks o cuando el cierre puede haber fallado.
        """
        try:
            self.register_close(symbol, notional_or_margin)
        except Exception as e:
            log.error(
                "[PreTradeRisk] register_close_safe(%s) error: %s — forzando liberación.",
                symbol, e,
            )
            # Forzar liberación aunque falle
            self._open_margin_by_symbol.pop(symbol, None)
            self._open_margin = max(0.0, self._open_margin - notional_or_margin)

    async def get_open_margin(self) -> float:
        async with self._lock:
            return self._open_margin

    async def get_open_margin_by_symbol(self) -> Dict[str, float]:
        async with self._lock:
            return dict(self._open_margin_by_symbol)

    async def get_order_rate(self) -> int:
        async with self._lock:
            now = time.monotonic()
            cutoff = now - self._window_seconds
            while self._order_timestamps and self._order_timestamps[0] < cutoff:
                self._order_timestamps.popleft()
            return len(self._order_timestamps)


# ── singleton ─────────────────────────────────────────────────────────────
pretrade_risk = PreTradeRisk(
    max_open_margin=float(os.getenv("MAX_OPEN_MARGIN_USDC", "500")),
    max_orders_per_window=int(os.getenv("MAX_ORDERS_PER_WINDOW", "20")),
    window_seconds=float(os.getenv("RATE_LIMIT_WINDOW_SECONDS", "60")),
)
