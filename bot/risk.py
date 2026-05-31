"""
bot/risk.py — Shim de compatibilidad.

Este módulo existía antes de que la lógica de riesgo se moviera al
package bot/risk/. Se mantiene como re-exportación para no romper
cualquier import legacy que apunte a 'from bot.risk import RiskManager'.

No añadir lógica aquí. Todo vive en bot/risk/risk_manager.py.
"""
from bot.risk.risk_manager import RiskManager  # noqa: F401

__all__ = ["RiskManager"]
