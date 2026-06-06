"""
signal_memory.py — Persistencia de señales entre ciclos.

Problema que resuelve:
  Cada ciclo de signal_engine evalúa desde cero, sin considerar si el par
  ha estado generando el mismo setup en ciclos anteriores. Un par que lleva
  3 ciclos consecutivos con score 9/13 en el mismo setup es más confiable
  que uno que lo hace por primera vez.

Mecanismo:
  - Por cada par se guarda un deque de los últimos N scores (con dirección).
  - persistence_bonus() compara cuántos ciclos consecutivos el par ha
    superado MIN_SCORE en la misma dirección:
      · 0-1 ciclos → +0 (señal nueva, sin bonus)
      · 2   ciclos → +1 (señal estable, confirma momentum)
      · 3+  ciclos → +2 (señal institucional persistente)
  - El bonus se suma al score ANTES de evaluar MIN_SCORE_RATIO.
  - La memoria se limpia automáticamente si el par cambia de dirección
    o si el score cae bajo MIN_SCORE (reset de ciclo).

Variables de entorno:
  SMEM_ENABLED    true|false  Activar/desactivar (default true)
  SMEM_WINDOW     int         Ciclos máximos en memoria por par (default 5)
  SMEM_MIN_CYCLES int         Ciclos mínimos para bonus +1 (default 2)
  SMEM_SCORE_FLOOR int        Score mínimo para contar como ciclo válido (default 8)

Uso:
  from bot.signal_memory import signal_memory

  # Al final de analyze_pair, antes de is_valid:
  bonus = signal_memory.persistence_bonus(symbol, direction="LONG", score=9)
  signal_memory.record(symbol, score=9, direction="LONG")
  final_score = score + bonus
"""
from __future__ import annotations

import logging
import os
import threading
from collections import deque
from dataclasses import dataclass
from typing import Deque, Dict, Optional

log = logging.getLogger(__name__)

_ENABLED    = os.getenv("SMEM_ENABLED",    "true").lower() != "false"
_WINDOW     = int(os.getenv("SMEM_WINDOW",     "5"))
_MIN_CYCLES = int(os.getenv("SMEM_MIN_CYCLES", "2"))
_SCORE_FLOOR = int(os.getenv("SMEM_SCORE_FLOOR", "8"))


@dataclass
class _CycleEntry:
    score:     int
    direction: str   # "LONG" | "SHORT" | "NEUTRAL"


class SignalMemory:
    """
    Registro ligero de scores recientes por par.
    Thread-safe mediante Lock.
    """

    def __init__(self) -> None:
        self._lock: threading.Lock = threading.Lock()
        self._store: Dict[str, Deque[_CycleEntry]] = {}

    # ── API pública ──────────────────────────────────────────────────────────

    def record(
        self,
        symbol:    str,
        score:     int,
        direction: str,
    ) -> None:
        """
        Registra un ciclo de análisis para el par.

        Si el score cae bajo _SCORE_FLOOR O la dirección cambia,
        el historial se resetea (el mercado cambió de setup).
        """
        if not _ENABLED:
            return

        dir_norm = direction.upper().strip()
        with self._lock:
            dq = self._store.setdefault(symbol, deque(maxlen=_WINDOW))
            # Reset si cambia dirección o score inválido
            if dq and (dq[-1].direction != dir_norm or score < _SCORE_FLOOR):
                dq.clear()
            if score >= _SCORE_FLOOR:
                dq.append(_CycleEntry(score=score, direction=dir_norm))

    def persistence_bonus(
        self,
        symbol:    str,
        direction: str,
        score:     int,
    ) -> int:
        """
        Calcula el bonus de persistencia ANTES de registrar el ciclo actual.

        Returns:
            0  → señal nueva o cambiada
            +1 → 2 ciclos previos consecutivos en misma dirección
            +2 → 3+ ciclos previos consecutivos en misma dirección
        """
        if not _ENABLED or score < _SCORE_FLOOR:
            return 0

        dir_norm = direction.upper().strip()
        with self._lock:
            dq = self._store.get(symbol)
            if not dq:
                return 0

            # Contar ciclos consecutivos recientes con la misma dirección
            consecutive = 0
            for entry in reversed(dq):
                if entry.direction == dir_norm and entry.score >= _SCORE_FLOOR:
                    consecutive += 1
                else:
                    break

        if consecutive >= 3:
            bonus = 2
        elif consecutive >= _MIN_CYCLES:
            bonus = 1
        else:
            bonus = 0

        if bonus > 0:
            log.info(
                "[signal_memory] %s %s consecutive=%d → persistence_bonus=+%d",
                symbol, dir_norm, consecutive, bonus,
            )
        return bonus

    def get_history(
        self,
        symbol: str,
    ) -> list[_CycleEntry]:
        """Devuelve una copia del historial de ciclos del par (para debug)."""
        with self._lock:
            dq = self._store.get(symbol)
            return list(dq) if dq else []

    def clear(self, symbol: Optional[str] = None) -> None:
        """Limpia el historial de un par o de todos."""
        with self._lock:
            if symbol:
                self._store.pop(symbol, None)
            else:
                self._store.clear()


# Singleton global — importar en signal_engine y strategy
signal_memory: SignalMemory = SignalMemory()
