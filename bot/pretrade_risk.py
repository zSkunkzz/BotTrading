"""
bot/pretrade_risk.py  –  Pre-trade risk checks (margin-based, not gross notional).

Fixes:
  - All limits now denominated in MARGIN (usdc_per_trade * leverage), not raw notional
  - confirm_order() / register_close() track open-margin correctly
  - Sliding-window rate limiter (deque) replaces broken counter
  - check() returns (bool, str) – callers must unpack
"""
from __future__ import annotations

import asyncio
import logging
import time
from collections import deque
from typing import Deque, Optional, Tuple

log = logging.getLogger(__name__)


class PreTradeRisk:
    """
    Parameters
    ----------
    max_open_margin : float
        Maximum total margin (USDC) that may be open simultaneously.
    max_orders_per_window : int
        Hard cap on orders placed within `window_seconds`.
    window_seconds : float
        Sliding window length for the rate limiter (default 60 s).
    """

    def __init__(
        self,
        max_open_margin: float = 500.0,
        max_orders_per_window: int = 20,
        window_seconds: float = 60.0,
    ) -> None:
        self._max_open_margin = max_open_margin
        self._max_orders_per_window = max_orders_per_window
        self._window_seconds = window_seconds

        self._lock = asyncio.Lock()
        self._open_margin: float = 0.0          # sum of margin of live positions
        self._order_timestamps: Deque[float] = deque()  # sliding window

    # ── public helpers ───────────────────────────────────────────────────────

    async def check(
        self,
        symbol: str,
        side: Optional[str],
        margin: float,          # USDC margin for THIS trade (size / leverage)
        *,
        price: float = 0.0,     # accepted but not used (forward-compat)
        qty: float = 0.0,
    ) -> Tuple[bool, str]:
        """
        Returns (True, "") if the trade is allowed,
                (False, <reason>) otherwise.
        """
        async with self._lock:
            # 1. Rate-limit check (sliding window)
            now = time.monotonic()
            cutoff = now - self._window_seconds
            while self._order_timestamps and self._order_timestamps[0] < cutoff:
                self._order_timestamps.popleft()

            if len(self._order_timestamps) >= self._max_orders_per_window:
                reason = (
                    f"Rate limit: {len(self._order_timestamps)} orders in the last "
                    f"{self._window_seconds:.0f}s (max {self._max_orders_per_window})"
                )
                log.warning("[PreTradeRisk] %s – %s", symbol, reason)
                return False, reason

            # 2. Margin headroom check
            if margin <= 0:
                reason = f"Invalid margin value: {margin}"
                log.warning("[PreTradeRisk] %s – %s", symbol, reason)
                return False, reason

            projected = self._open_margin + margin
            if projected > self._max_open_margin:
                reason = (
                    f"Open-margin limit: would reach {projected:.2f} USDC "
                    f"(limit {self._max_open_margin:.2f})"
                )
                log.warning("[PreTradeRisk] %s – %s", symbol, reason)
                return False, reason

            return True, ""

    async def confirm_order(
        self,
        symbol: str,
        margin: float,
    ) -> None:
        """
        Call AFTER an order is accepted by the exchange.
        Registers the margin and stamps the rate-limiter window.
        """
        async with self._lock:
            self._open_margin += margin
            self._order_timestamps.append(time.monotonic())
            log.debug(
                "[PreTradeRisk] confirmed %s +%.2f USDC margin → total %.2f",
                symbol,
                margin,
                self._open_margin,
            )

    async def register_close(
        self,
        symbol: str,
        margin: float,
    ) -> None:
        """
        Call when a position is fully closed.
        Releases the reserved margin.
        """
        async with self._lock:
            self._open_margin = max(0.0, self._open_margin - margin)
            log.debug(
                "[PreTradeRisk] closed %s -%.2f USDC margin → total %.2f",
                symbol,
                margin,
                self._open_margin,
            )

    async def get_open_margin(self) -> float:
        async with self._lock:
            return self._open_margin

    async def get_order_rate(self) -> int:
        """Return number of orders placed in the current sliding window."""
        async with self._lock:
            now = time.monotonic()
            cutoff = now - self._window_seconds
            while self._order_timestamps and self._order_timestamps[0] < cutoff:
                self._order_timestamps.popleft()
            return len(self._order_timestamps)
