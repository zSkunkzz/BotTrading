"""
signal_cooldown.py — Bloqueo de reapertura tras cierre de posición.

Cuando el bot cierra una posición (por SL, TP o trailing), la señal
actual queda "consumida". No se permite reabrir hasta que pase el
cooldown correspondiente al entry_mode de la señal cerrada.

#4 Cooldown diferenciado por entry_mode:
  STRONG  → REENTRY_COOLDOWN_STRONG_SECS  (default 600  — 2 velas 15m)
  NORMAL  → REENTRY_COOLDOWN_NORMAL_SECS  (default 900  — 1 vela 15m)
  EARLY   → REENTRY_COOLDOWN_EARLY_SECS   (default 1800 — 2 velas 15m + margen)
  (sin modo / legacy) → REENTRY_COOLDOWN_SECS (default 900)

Uso:
  # Al cerrar una posición (position_manager.py / decision_engine.py):
  from bot.signal_cooldown import signal_cooldown
  signal_cooldown.mark_closed(symbol, entry_mode="STRONG", reason="SL")

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

# Cooldown base (legacy / sin entry_mode)
COOLDOWN_SECS: float = float(os.getenv("REENTRY_COOLDOWN_SECS", "900"))

# #4 Cooldowns diferenciados por entry_mode
_COOLDOWN_BY_MODE: dict[str, float] = {
    "STRONG": float(os.getenv("REENTRY_COOLDOWN_STRONG_SECS", "600")),
    "NORMAL": float(os.getenv("REENTRY_COOLDOWN_NORMAL_SECS", "900")),
    "EARLY":  float(os.getenv("REENTRY_COOLDOWN_EARLY_SECS",  "1800")),
}


class SignalCooldown:
    """
    Mantiene un dict {symbol: unblock_at_monotonic}.
    Thread-safe para lectura; escritura desde el event loop principal.
    """

    def __init__(self) -> None:
        self._unblock_at: dict[str, float] = {}

    def mark_closed(
        self,
        symbol: str,
        entry_mode: str = "",
        reason: str = "",
    ) -> None:
        """
        Marca el símbolo como bloqueado.
        entry_mode (STRONG/NORMAL/EARLY) determina la duración del cooldown.
        Llamar inmediatamente después de cerrar la posición.
        """
        mode = (entry_mode or "").upper()
        secs = _COOLDOWN_BY_MODE.get(mode, COOLDOWN_SECS)
        if secs <= 0:
            return
        sym = symbol.upper()
        unblock_at = time.monotonic() + secs
        self._unblock_at[sym] = unblock_at
        logger.info(
            "[Cooldown] %s bloqueado %ds (modo=%s)%s — reapertura en %.0fs",
            sym, int(secs),
            mode or "legacy",
            f" ({reason})" if reason else "",
            secs,
        )

    def is_blocked(self, symbol: str) -> bool:
        """
        Devuelve True si el símbolo está en cooldown.
        Limpia la entrada automáticamente cuando expira.
        """
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
        return max(0.0, unblock_at - time.monotonic())

    def clear(self, symbol: str) -> None:
        """Fuerza el fin del cooldown (uso manual / tests)."""
        self._unblock_at.pop(symbol.upper(), None)


# Instancia singleton
signal_cooldown = SignalCooldown()
