"""
bot/execution_engine.py — SHIM de compatibilidad.

El motor real vive en bot/execution/execution_engine.py.
Este archivo solo re-exporta para no romper imports legacy.
"""
from bot.execution.execution_engine import execution_engine, ExecutionEngine

__all__ = ["execution_engine", "ExecutionEngine"]
