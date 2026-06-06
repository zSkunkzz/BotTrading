#!/usr/bin/env python3
"""
signal_persistence.py — Memoria de scoring entre ciclos de evaluación.

PROBLEMA (gap #3):
  signal_engine evalúa cada ciclo desde cero. Un par que puntúa 9/13 durante
  tres ciclos consecutivos debería ser una señal más fuerte que uno que puntúa
  9/13 por primera vez. Sin memoria, ambos se tratan igual.

SOLUCIÓN:
  SignalPersistence rastrea el historial reciente de scores/setup por símbolo
  y expone un bonus de persistencia (+0/+1/+2) que signal_engine puede sumar
  al score final antes de comparar con MIN_SCORE.

CRITERIO DE BONUS:
  - 0 puntos  → primer ciclo con señal válida (score >= MIN_SCORE)
  - +1 punto  → señal válida en los últimos PERSIST_CYCLES_NEEDED ciclos
               con el mismo tipo de setup
  - +2 puntos → señal válida en los últimos PERSIST_CYCLES_STRONG ciclos
               consecutivos con el mismo tipo de setup y misma dirección

Config Railway:
  PERSIST_CYCLES_NEEDED   → ciclos para bonus +1  (default 3)
  PERSIST_CYCLES_STRONG   → ciclos para bonus +2  (default 5)
  PERSIST_WINDOW_S        → ventana temporal máxima (default 3600 = 1h)
  PERSIST_SCORE_THRESHOLD → score mínimo para contar un ciclo (default 7)
  PERSIST_PERSIST_PATH    → ruta JSON de persistencia (default /tmp/sig_persist.json)
"""
from __future__ import annotations

import json
import logging
import os
import time
from collections import deque
from dataclasses import dataclass, asdict
from typing import Deque, Dict, Optional

log = logging.getLogger(__name__)

_CYCLES_NEEDED    = int(float(os.getenv("PERSIST_CYCLES_NEEDED",    "3")))
_CYCLES_STRONG    = int(float(os.getenv("PERSIST_CYCLES_STRONG",    "5")))
_WINDOW_S         = float(os.getenv("PERSIST_WINDOW_S",            "3600"))
_SCORE_THRESHOLD  = int(float(os.getenv("PERSIST_SCORE_THRESHOLD",  "7")))
_PERSIST_PATH     = os.getenv("PERSIST_PERSIST_PATH", "/tmp/sig_persist.json")
_MAX_HISTORY      = max(_CYCLES_STRONG + 2, 10)


@dataclass
class _Cycle:
    ts:        float   # timestamp del ciclo
    score:     int
    max_score: int
    setup:     str     # TENDENCIA | BREAKOUT | REVERSAL
    direction: str     # LONG | SHORT


class SignalPersistence:
    """
    Rastrea historial de señales por símbolo y calcula un bonus de persistencia.

    Uso en signal_engine:
        from bot.signal_persistence import signal_persistence
        bonus = signal_persistence.record_and_get_bonus(
            symbol="BTCUSDT", score=9, max_score=13,
            setup="TENDENCIA", direction="LONG"
        )
        final_score = score + bonus
    """

    def __init__(self) -> None:
        # {symbol: deque de _Cycle}
        self._history: Dict[str, Deque[_Cycle]] = {}
        self._load()

    # ── Interfaz pública ──────────────────────────────────────────────────────

    def record_and_get_bonus(
        self,
        symbol:    str,
        score:     int,
        max_score: int,
        setup:     str,
        direction: str,
    ) -> int:
        """
        Registra el ciclo actual y devuelve el bonus de persistencia (0, 1 ó 2).
        Sólo registra ciclos cuyo score >= PERSIST_SCORE_THRESHOLD.

        Returns:
            0  → sin historial suficiente
            1  → señal persistente (>= PERSIST_CYCLES_NEEDED ciclos)
            2  → señal muy persistente (>= PERSIST_CYCLES_STRONG ciclos consecutivos,
                 mismo setup y dirección)
        """
        if score >= _SCORE_THRESHOLD:
            self._push(symbol, score, max_score, setup, direction)

        return self._compute_bonus(symbol, setup, direction)

    def get_bonus_only(self, symbol: str, setup: str, direction: str) -> int:
        """Consulta el bonus sin registrar nada (solo lectura)."""
        return self._compute_bonus(symbol, setup, direction)

    def reset(self, symbol: str) -> None:
        """Limpia el historial de un símbolo (usar tras cierre por SL)."""
        self._history.pop(symbol, None)
        self._save()
        log.debug("[persist] %s: historial reseteado", symbol)

    def consecutive_count(self, symbol: str, setup: str, direction: str) -> int:
        """
        Devuelve cuántos ciclos consecutivos recientes tiene el símbolo
        con el mismo setup y dirección dentro de la ventana temporal.
        """
        hist = self._pruned(symbol)
        if not hist:
            return 0
        count = 0
        for c in reversed(list(hist)):
            if c.setup == setup and c.direction == direction:
                count += 1
            else:
                break
        return count

    # ── Internos ──────────────────────────────────────────────────────────────

    def _push(
        self,
        symbol:    str,
        score:     int,
        max_score: int,
        setup:     str,
        direction: str,
    ) -> None:
        if symbol not in self._history:
            self._history[symbol] = deque(maxlen=_MAX_HISTORY)
        self._history[symbol].append(
            _Cycle(ts=time.time(), score=score, max_score=max_score,
                   setup=setup, direction=direction)
        )
        self._save()

    def _pruned(self, symbol: str) -> Deque[_Cycle]:
        """Devuelve sólo los ciclos dentro de la ventana temporal."""
        if symbol not in self._history:
            return deque()
        cutoff = time.time() - _WINDOW_S
        valid  = deque(c for c in self._history[symbol] if c.ts >= cutoff)
        return valid

    def _compute_bonus(self, symbol: str, setup: str, direction: str) -> int:
        hist = self._pruned(symbol)
        if not hist:
            return 0

        # Contar ciclos consecutivos recientes con mismo setup + dirección
        consecutive = 0
        for c in reversed(list(hist)):
            if c.setup == setup and c.direction == direction:
                consecutive += 1
            else:
                break

        if consecutive >= _CYCLES_STRONG:
            log.debug(
                "[persist] %s %s %s: %d ciclos consecutivos → bonus +2",
                symbol, setup, direction, consecutive,
            )
            return 2

        if consecutive >= _CYCLES_NEEDED:
            log.debug(
                "[persist] %s %s %s: %d ciclos → bonus +1",
                symbol, setup, direction, consecutive,
            )
            return 1

        return 0

    # ── Persistencia JSON ─────────────────────────────────────────────────────

    def _save(self) -> None:
        try:
            data: dict = {}
            for sym, dq in self._history.items():
                data[sym] = [asdict(c) for c in dq]
            with open(_PERSIST_PATH, "w") as f:
                json.dump({"history": data, "saved_at": time.time()}, f)
        except Exception as e:
            log.debug("[persist] save error: %s", e)

    def _load(self) -> None:
        try:
            with open(_PERSIST_PATH) as f:
                raw = json.load(f)
            cutoff = time.time() - _WINDOW_S
            for sym, cycles in raw.get("history", {}).items():
                dq: Deque[_Cycle] = deque(maxlen=_MAX_HISTORY)
                for c in cycles:
                    if c.get("ts", 0) >= cutoff:
                        dq.append(_Cycle(**c))
                if dq:
                    self._history[sym] = dq
            total = sum(len(v) for v in self._history.values())
            if total:
                log.info("[persist] Historial restaurado: %d registros en %d símbolos",
                         total, len(self._history))
        except FileNotFoundError:
            pass
        except Exception as e:
            log.warning("[persist] load error: %s", e)


# Singleton global
signal_persistence = SignalPersistence()
