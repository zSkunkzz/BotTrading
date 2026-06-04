"""
Risk package — single entry point for all risk management.
Usage: from bot.risk import RiskManager
"""

from bot.risk.global_risk import GlobalRisk
from bot.risk.pretrade import PreTradeRisk
from bot.risk.drawdown import DailyDrawdown
from bot.risk.kelly import KellySizer
from bot.risk.correlation import CorrelationGuard


class RiskManager:
    """Facade that wires all risk sub-modules together."""

    def __init__(self, config: dict):
        self.global_risk   = GlobalRisk(config)
        self.pretrade      = PreTradeRisk(config)
        self.drawdown      = DailyDrawdown(config)
        self.kelly         = KellySizer(config)
        self.correlation   = CorrelationGuard(config)

    # ── Pretrade gate ──────────────────────────────────────────────
    def check_pretrade(self, symbol: str, signal: dict) -> str | None:
        """Returns a block reason string, or None if trade is allowed."""
        return self.pretrade.check(symbol, signal)

    # ── Position sizing ────────────────────────────────────────────
    def get_size_multiplier(self, symbol: str, rr: float) -> float:
        return self.kelly.get_multiplier(symbol, rr)

    # ── Post-trade accounting ──────────────────────────────────────
    def on_trade_result(self, symbol: str, pnl_pct: float) -> None:
        self.drawdown.record(pnl_pct)
        self.kelly.record(symbol, pnl_pct)
        self.global_risk.on_trade_result(symbol, pnl_pct)

    # ── Daily reset ───────────────────────────────────────────────
    def reset_daily(self) -> None:
        self.drawdown.reset()
        self.global_risk.reset_daily()

    # ── Correlation filter ────────────────────────────────────────
    def is_correlated(self, symbol: str, open_symbols: list[str]) -> bool:
        return self.correlation.is_blocked(symbol, open_symbols)
