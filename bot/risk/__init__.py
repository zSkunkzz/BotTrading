"""
Risk package — punto de entrada único para gestión de riesgo.
Usage: from bot.risk import RiskManager
"""

from bot.risk.risk_manager import RiskManager, GlobalRiskManager, PreTradeRiskChecker
from bot.risk.kelly import kelly_multiplier
from bot.risk.pretrade import PreTradeRisk
from bot.risk.drawdown import DailyDrawdown
from bot.risk.global_risk import GlobalRisk
from bot.risk.correlation import CorrelationGuard

__all__ = [
    "RiskManager",
    "GlobalRiskManager",
    "PreTradeRiskChecker",
    "kelly_multiplier",
    "PreTradeRisk",
    "DailyDrawdown",
    "GlobalRisk",
    "CorrelationGuard",
]
