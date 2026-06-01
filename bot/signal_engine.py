# -*- coding: utf-8 -*-
"""
signal_engine.py — Motor de señales de trading.

v2 — BUG #7 FIX: sin cooldown por dirección → flip-flop de posiciones

  El bug original permitía generar señales opuestas en <2 velas cuando
  el mercado hacía zigzag, causando cierres y reaperturas repetidos con
  doble comisión en cada flip.

  Fix:
    - _last_signal_by_symbol: dict[symbol, (side, ts)] almacena la última
      señal generada por símbolo.
    - Si se genera señal opuesta a la anterior en menos de
      SIGNAL_FLIP_COOLDOWN_S (default 120s = 2 velas 1m / 0.1 velas 15m),
      la señal se descarta con WARNING.
    - El cooldown solo aplica a inversiones de dirección (long→short o
      short→long), no a señales en la misma dirección.
    - Se puede desactivar con SIGNAL_FLIP_COOLDOWN_S=0.
"""
from __future__ import annotations

import logging
import os
import time
from typing import Dict, Optional, Tuple

log = logging.getLogger(__name__)

_FLIP_COOLDOWN_S = float(os.getenv("SIGNAL_FLIP_COOLDOWN_S", "120"))

# Tipo de señal devuelto por decide() / ai_decide()
# Se asume que es un objeto con atributo .side (str: 'long'/'short') o None


class SignalFlipGuard:
    """
    BUG #7 FIX: Previene flip-flop de señales opuestas en ventana corta.

    Uso:
        guard = SignalFlipGuard()
        signal = decide(...)   # devuelve objeto con .side o None
        if guard.allow(symbol, signal):
            # procesar señal
        else:
            # señal bloqueada por cooldown
    """

    def __init__(self, cooldown_s: float = _FLIP_COOLDOWN_S):
        self._cooldown = cooldown_s
        # symbol -> (side: str, ts: float)
        self._last: Dict[str, Tuple[str, float]] = {}

    def allow(self, symbol: str, signal) -> bool:
        """
        Devuelve True si la señal debe procesarse, False si debe bloquearse.

        signal puede ser:
          - None o sin atributo .side → se permite (señal nula)
          - objeto con .side in ('long', 'short', 'buy', 'sell')
        """
        if self._cooldown <= 0:
            return True
        if signal is None:
            return True

        # Obtener side de la señal
        side = getattr(signal, "side", None)
        if not side:
            # Si la señal tiene dirección como string directamente
            if isinstance(signal, str) and signal in ("long", "short", "buy", "sell"):
                side = signal
            else:
                return True  # señal sin side definido → dejar pasar

        # Normalizar
        side_norm = "long" if side in ("long", "buy") else "short"

        last = self._last.get(symbol)
        if last is not None:
            last_side, last_ts = last
            elapsed = time.monotonic() - last_ts
            if last_side != side_norm and elapsed < self._cooldown:
                log.warning(
                    "[SignalFlipGuard] %s: señal %s BLOQUEADA — inversión de %s a %s "
                    "en %.1fs (cooldown=%.0fs). Evitando flip-flop.",
                    symbol, side_norm, last_side, side_norm,
                    elapsed, self._cooldown,
                )
                return False

        # Actualizar registro y permitir
        self._last[symbol] = (side_norm, time.monotonic())
        return True

    def reset(self, symbol: str) -> None:
        """Limpiar el registro de un símbolo (llamar tras cierre de posición)."""
        self._last.pop(symbol, None)

    def update(self, symbol: str, side: str) -> None:
        """Actualizar manualmente el último side sin pasar por allow()."""
        side_norm = "long" if side in ("long", "buy") else "short"
        self._last[symbol] = (side_norm, time.monotonic())


# Singleton exportado — importar desde trader.py:
#   from bot.signal_engine import signal_flip_guard
#   if not signal_flip_guard.allow(symbol, signal): continue
signal_flip_guard = SignalFlipGuard()
