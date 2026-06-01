"""
signal_cooldown.py — Bloqueo de reapertura tras cierre de posición.

Cuando el bot cierra una posición (por SL, TP o trailing), la señal
actual queda "consumida". No se permite reabrir hasta que cierre la
siguiente vela 15m — momento en que el análisis técnico es fresco.

Uso:
  # Al cerrar una posición (position_manager.py):
  from bot.signal_cooldown import signal_cooldown
  signal_cooldown.mark_closed(symbol)

  # Al evaluar si abrir (decision_engine.py):
  from bot.signal_cooldown import signal_cooldown
  if signal_cooldown.is_blocked(symbol):
      return
"""
from __future__ import annotations

import logging
import time
import os

logger = logging.getLogger("SignalCooldown")

# Duración del bloqueo en segundos. Default: 1 vela 15m = 900s.
# Configurable con REENTRY_COOLDOWN_SECS=0 para desactivar.
COOLDOWN_SECS: float = float(os.getenv("REENTRY_COOLDOWN_SECS", "900"))


class SignalCooldown:
    """
    Mantiene un dict {symbol: unblock_at_monotonic}.
    Thread-safe para lectura; escritura desde el event loop principal.
    """

    def __init__(self) -> None:
        self._unblock_at: dict[str, float] = {}

    def mark_closed(self, symbol: str, reason: str = "") -> None:
        """
        Marca el símbolo como bloqueado durante COOLDOWN_SECS.
        Llamar inmediatamente después de cerrar la posición.
        """
        if COOLDOWN_SECS <= 0:
            return
        sym = symbol.upper()
        unblock_at = time.monotonic() + COOLDOWN_SECS
        self._unblock_at[sym] = unblock_at
        logger.info(
            "[Cooldown] %s bloqueado %ds tras cierre%s — reapertura disponible en %.0fs",
            sym, int(COOLDOWN_SECS), f" ({reason})" if reason else "", COOLDOWN_SECS,
        )

    def is_blocked(self, symbol: str) -> bool:
        """
        Devuelve True si el símbolo está en cooldown.
        Limpia la entrada automáticamente cuando expira.
        """
        if COOLDOWN_SECS <= 0:
            return False
        sym = symbol.upper()
        unblock_at = self._unblock_at.get(sym)
        if unblock_at is None:
            return False
        if time.monotonic() >= unblock_at:
            del self._unblock_at[sym]
            return False
        return True

    def remaining(self, symbol: str) -> float:
        """Segundos restantes de cooldown (0 si no está bloqueado)."""
        sym = symbol.upper()
        unblock_at = self._unblock_at.get(sym)
        if unblock_at is None:
            return 0.0
        remaining = unblock_at - time.monotonic()
        return max(0.0, remaining)

    def clear(self, symbol: str) -> None:
        """Fuerza el fin del cooldown (uso manual / tests)."""
        self._unblock_at.pop(symbol.upper(), None)


# Instancia singleton
signal_cooldown = SignalCooldown()
