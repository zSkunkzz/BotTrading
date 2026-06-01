"""
bot/decision_engine.py  –  High-level trade decision with pre-trade risk gate.

Fixes:
  - pretrade_risk.check() called with correct keyword args
  - Return value (ok, reason) properly unpacked
  - on_order_confirmed() / on_position_closed() wired to pretrade_risk
"""
from __future__ import annotations

import logging
from typing import Any, Dict, Optional, Tuple

log = logging.getLogger(__name__)


class DecisionEngine:
    """
    Wraps signal evaluation + pre-trade risk in a single coroutine.

    Parameters
    ----------
    pretrade_risk : PreTradeRisk
        An initialised PreTradeRisk instance.
    signal_engine : SignalEngine | None
        Optional signal engine; if None the caller is expected to pass
        pre-built signal dicts to ``evaluate()``.
    usdc_per_trade : float
        Margin allocated per trade (USDC).
    leverage : int
        Leverage multiplier (used only for logging; margin is pre-computed).
    """

    def __init__(
        self,
        pretrade_risk,
        signal_engine=None,
        usdc_per_trade: float = 50.0,
        leverage: int = 10,
    ) -> None:
        self._risk = pretrade_risk
        self._signals = signal_engine
        self._usdc_per_trade = usdc_per_trade
        self._leverage = leverage

    # ── main entry point ────────────────────────────────────────────────────

    async def evaluate(
        self,
        symbol: str,
        signal: Dict[str, Any],
        price: float,
    ) -> Tuple[bool, str, Optional[Dict[str, Any]]]:
        """
        Returns
        -------
        (approved, reason, enriched_signal)
            approved        – True if the trade should proceed
            reason          – human-readable rejection reason ("" if approved)
            enriched_signal – signal dict with added risk fields, or None
        """
        side: str = signal.get("side", "")
        if not side:
            return False, "Signal missing 'side'", None

        margin = self._usdc_per_trade  # margin = size in USDC (already post-leverage)

        ok, reason = await self._risk.check(
            symbol=symbol,
            side=side,
            margin=margin,
            price=price,
        )

        if not ok:
            log.info("[DecisionEngine] %s rejected: %s", symbol, reason)
            return False, reason, None

        enriched = {
            **signal,
            "_margin": margin,
            "_leverage": self._leverage,
            "_price_at_decision": price,
        }
        return True, "", enriched

    # ── lifecycle callbacks ──────────────────────────────────────────────────

    async def on_order_confirmed(self, symbol: str, margin: Optional[float] = None) -> None:
        """
        Call after the exchange confirms order placement.
        Registers the margin in the risk ledger and stamps the rate-limiter.
        """
        m = margin if margin is not None else self._usdc_per_trade
        await self._risk.confirm_order(symbol=symbol, margin=m)
        log.debug("[DecisionEngine] order confirmed for %s (margin %.2f)", symbol, m)

    async def on_position_closed(self, symbol: str, margin: Optional[float] = None) -> None:
        """
        Call when a position is fully closed.
        Releases the margin from the risk ledger.
        """
        m = margin if margin is not None else self._usdc_per_trade
        await self._risk.register_close(symbol=symbol, margin=m)
        log.debug("[DecisionEngine] position closed for %s (margin %.2f released)", symbol, m)
