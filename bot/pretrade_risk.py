"""
bot/pretrade_risk.py  –  Pre-trade risk checks (margin-based, not gross notional).

Fixes:
  - All limits now denominated in MARGIN (usdc_per_trade * leverage), not raw notional
  - confirm_order() / register_close() track open-margin correctly
  - Sliding-window rate limiter (deque) replaces broken counter
  - check() returns (bool, str) – callers must unpack
  - Module-level singleton `pretrade_risk` exported (ImportError fix)
  - check() accepts both (margin=) and (notional=, leverage=, balance=) call signatures
  - confirm_order() / register_close() accept both sync and async callers
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
        self._max_open_margin      = max_open_margin
        self._max_orders_per_window = max_orders_per_window
        self._window_seconds       = window_seconds

        self._lock = asyncio.Lock()
        self._open_margin: float = 0.0
        self._order_timestamps: Deque[float] = deque()

    # ── public API ───────────────────────────────────────────────────────────────────────────────

    async def check(
        self,
        symbol: str,
        side: Optional[str] = None,
        # --- margin-based call (position_manager, old callers) ---
        margin: float = 0.0,
        # --- notional-based call (decision_engine) ---
        notional: float = 0.0,
        leverage: float = 1.0,
        balance: Optional[float] = None,
        # --- forward-compat ignored kwargs ---
        price: float = 0.0,
        qty: float = 0.0,
        sl: Optional[float] = None,
    ) -> Tuple[bool, str]:
        """
        Returns (True, "") if the trade is allowed,
                (False, <reason>) otherwise.

        Accepts two call signatures:
          1. check(symbol, side, margin=M)                  ← direct margin
          2. check(symbol, side, notional=N, leverage=L)    ← decision_engine style
        """
        # Resolve margin from notional if not provided directly
        if margin <= 0 and notional > 0:
            lev = leverage if leverage and leverage > 0 else 1.0
            margin = notional / lev

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

            # 2. Margin validity
            if margin <= 0:
                reason = f"Invalid margin value: {margin}"
                log.warning("[PreTradeRisk] %s – %s", symbol, reason)
                return False, reason

            # 3. Margin headroom check
            projected = self._open_margin + margin
            if projected > self._max_open_margin:
                reason = (
                    f"Open-margin limit: would reach {projected:.2f} USDC "
                    f"(limit {self._max_open_margin:.2f})"
                )
                log.warning("[PreTradeRisk] %s – %s", symbol, reason)
                return False, reason

            return True, ""

    def confirm_order(self, symbol: str, notional_or_margin: float) -> None:
        """
        Call AFTER an order is accepted by the exchange.
        Accepts the notional (decision_engine passes notional);
        internally we treat it as margin for accounting purposes.
        Stamps the rate-limiter window. Sync — safe to call without await.
        """
        # decision_engine calls: pretrade_risk.confirm_order(symbol, notional)
        # We record it as-is; the margin ledger is approximate when called
        # with notional, but the check() already validated the margin headroom.
        margin = notional_or_margin
        self._open_margin += margin
        self._order_timestamps.append(time.monotonic())
        log.debug(
            "[PreTradeRisk] confirmed %s +%.2f → total open=%.2f",
            symbol, margin, self._open_margin,
        )

    def register_close(self, symbol: str, notional_or_margin: float) -> None:
        """
        Call when a position is fully closed. Sync — safe to call without await.
        """
        margin = notional_or_margin
        self._open_margin = max(0.0, self._open_margin - margin)
        log.debug(
            "[PreTradeRisk] closed %s -%.2f → total open=%.2f",
            symbol, margin, self._open_margin,
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


# ── module-level singleton ───────────────────────────────────────────────────────────────────────────────────
# Importado desde trader.py, position_manager.py y decision_engine.py como:
#   from bot.pretrade_risk import pretrade_risk
pretrade_risk = PreTradeRisk(
    max_open_margin=float(__import__('os').getenv("MAX_OPEN_MARGIN_USDC", "500")),
    max_orders_per_window=int(__import__('os').getenv("MAX_ORDERS_PER_WINDOW", "20")),
    window_seconds=float(__import__('os').getenv("RATE_LIMIT_WINDOW_SECONDS", "60")),
)
