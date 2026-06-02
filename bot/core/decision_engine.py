"""
bot/core/decision_engine.py — STUB DE COMPATIBILIDAD

Este fichero existía como duplicado de bot/decision_engine.py y causaba
el BUG #4: si cualquier módulo importaba desde bot.core.decision_engine
se creaba una segunda instancia de DecisionEngine con estado propio
(_open_margin, posiciones) completamente desincronizado del singleton
principal en bot.decision_engine.

FIX (2026-06-02): este módulo ahora es un redirect puro hacia
bot.decision_engine. Cualquier import existente sigue funcionando
sin crear estado duplicado.

NO añadir lógica aquí. Toda la implementación vive en bot/decision_engine.py.
"""
from bot.decision_engine import DecisionEngine  # noqa: F401  — re-export
from bot.decision_engine import DecisionEngine as _DecisionEngine  # noqa: F401

__all__ = ["DecisionEngine"]
