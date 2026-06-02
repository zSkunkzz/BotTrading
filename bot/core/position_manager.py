"""
bot/core/position_manager.py — STUB DE COMPATIBILIDAD

Este fichero existía como duplicado de bot/position_manager.py y causaba
el BUG #5: si cualquier módulo importaba desde bot.core.position_manager
se creaba un segundo singleton con su propio estado de posiciones,
independiente del singleton principal, provocando desincronización.

FIX (2026-06-02): este módulo ahora es un redirect puro hacia
bot.position_manager. Cualquier import existente sigue funcionando
sin crear estado duplicado.

NO añadir lógica aquí. Toda la implementación vive en bot/position_manager.py.
"""
try:
    from bot.position_manager import PositionManager  # noqa: F401  — re-export
    from bot.position_manager import PositionManager as _PositionManager  # noqa: F401
    __all__ = ["PositionManager"]
except ImportError:
    # Si bot/position_manager.py tampoco existe, levantar error claro
    raise ImportError(
        "bot/position_manager.py no encontrado. "
        "bot/core/position_manager.py es solo un stub de compatibilidad."
    )
