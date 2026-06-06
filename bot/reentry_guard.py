"""
reentry_guard.py — Re-entry con size reducido tras Stop Loss.

Problema que resuelve:
  Cuando un par sufre SL, el bot ignora ese par durante el cooldown del
  signal_flip_guard, pero cuando vuelve a generar señal NO diferencia entre
  un setup completamente nuevo o el mismo setup que acaba de fallar. Un
  re-entry inmediato al mismo size aumenta el riesgo secuencial.

Mecanismo:
  1. Cuando un trader registra un SL en un par, llama:
       reentry_guard.register_sl(symbol)
  2. En strategy.py, antes de calcular el size definitivo:
       factor, reason = reentry_guard.size_factor(symbol, score)
       usdt_trade = usdt_base * factor
  3. El factor es 1.0 (normal) o REENTRY_SIZE_PCT (reducido) si el par
     está dentro de la ventana de cooldown y el score no supera
     REENTRY_MIN_SCORE.
  4. Si el re-entry alcanza TP1, llamar:
       reentry_guard.register_tp(symbol)  # limpia el estado

Variables de entorno:
  REENTRY_ENABLED      true|false  Activar/desactivar (default true)
  REENTRY_WINDOW_S     float       Segundos de ventana tras SL (default 900 = 15min)
  REENTRY_SIZE_PCT     float       Factor de size en re-entry, 0.0-1.0 (default 0.50)
  REENTRY_MIN_SCORE    int         Score mínimo para re-entry con size normal (default MIN_SCORE+1=9)

Uso:
  from bot.reentry_guard import reentry_guard

  # En trader.py al detectar SL:
  reentry_guard.register_sl(symbol)

  # En strategy.py al calcular size:
  factor, reason = reentry_guard.size_factor(symbol, score=result.score)
  usdt_trade = usdt_base * factor
  if factor < 1.0:
      log.info("[reentry] %s size reducido x%.0f%% (%s)", symbol, factor*100, reason)
"""
from __future__ import annotations

import logging
import os
import threading
import time
from dataclasses import dataclass, field
from typing import Dict, Optional, Tuple

log = logging.getLogger(__name__)

_ENABLED      = os.getenv("REENTRY_ENABLED", "true").lower() != "false"
_WINDOW_S     = float(os.getenv("REENTRY_WINDOW_S",  "900"))    # 15 min
_SIZE_PCT     = float(os.getenv("REENTRY_SIZE_PCT",   "0.50"))   # 50% size
_MIN_SCORE    = int(os.getenv("REENTRY_MIN_SCORE",    "9"))      # MIN_SCORE+1


@dataclass
class _SlRecord:
    symbol:   str
    sl_at:    float          # time.monotonic()
    cleared:  bool = False   # True si TP1 alcanzado


class ReentryGuard:
    """
    Registra SLs recientes y reduce el size del siguiente trade en el par.
    Thread-safe mediante Lock.
    """

    def __init__(self) -> None:
        self._lock: threading.Lock = threading.Lock()
        self._records: Dict[str, _SlRecord] = {}

    # ── API pública ─────────────────────────────────────────────────────────

    def register_sl(self, symbol: str) -> None:
        """
        Llamar cuando un par cierra en SL.
        Inicia o reinicia la ventana de re-entry.
        """
        if not _ENABLED:
            return
        with self._lock:
            self._records[symbol] = _SlRecord(symbol=symbol, sl_at=time.monotonic())
        log.info("[reentry_guard] %s SL registrado — ventana re-entry %.0fs", symbol, _WINDOW_S)

    def register_tp(self, symbol: str) -> None:
        """
        Llamar cuando un re-entry en el par alcanza TP1.
        Limpia el registro para que el próximo trade sea al size normal.
        """
        with self._lock:
            rec = self._records.get(symbol)
            if rec:
                rec.cleared = True
        log.info("[reentry_guard] %s TP1 — re-entry guard limpiado", symbol)

    def size_factor(self, symbol: str, score: int) -> Tuple[float, str]:
        """
        Devuelve el factor de size para el próximo trade en el par.

        Returns:
            (factor, reason)
            factor = 1.0  si no hay SL reciente o ya fue limpiado
            factor = _SIZE_PCT si está dentro de ventana y score < _MIN_SCORE
            factor = 1.0  si score >= _MIN_SCORE (señal muy fuerte → size normal)
        """
        if not _ENABLED:
            return 1.0, ""

        with self._lock:
            rec = self._records.get(symbol)

        if rec is None or rec.cleared:
            return 1.0, ""

        elapsed = time.monotonic() - rec.sl_at
        if elapsed >= _WINDOW_S:
            # Ventana expirada: limpiar y size normal
            with self._lock:
                self._records.pop(symbol, None)
            return 1.0, ""

        # Dentro de ventana
        if score >= _MIN_SCORE:
            reason = (
                f"re-entry {symbol} score={score}>={_MIN_SCORE} — "
                f"señal fuerte, size normal"
            )
            log.info("[reentry_guard] %s", reason)
            return 1.0, reason

        remaining = int(_WINDOW_S - elapsed)
        reason = (
            f"re-entry {symbol} tras SL (hace {int(elapsed)}s, quedan {remaining}s) — "
            f"size reducido {int(_SIZE_PCT*100)}%"
        )
        return _SIZE_PCT, reason

    def is_in_reentry_window(self, symbol: str) -> bool:
        """True si el par está dentro de la ventana de re-entry."""
        with self._lock:
            rec = self._records.get(symbol)
        if rec is None or rec.cleared:
            return False
        return (time.monotonic() - rec.sl_at) < _WINDOW_S


# Singleton global
reentry_guard: ReentryGuard = ReentryGuard()
