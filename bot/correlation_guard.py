#!/usr/bin/env python3
"""
bot/correlation_guard.py — Control de correlación entre posiciones abiertas.

Evita acumular demasiada exposición en la misma dirección (todos LONG o todos
SHORT) o superar el límite de posiciones abiertas totales.

Variables de entorno:
  CORR_ENABLED      (default: true)
  CORR_MAX_SAME_DIR (default: 3)  — máx. posiciones en la misma dirección
  CORR_MAX_OPEN     (default: 5)  — máx. posiciones abiertas totales

Uso desde strategy.decide():
  ok, reason = check_correlation(proposed_direction="LONG", open_positions={...})
  # open_positions: dict symbol -> {"direction": "LONG" | "SHORT"}
"""

import logging
import os
from typing import Tuple

log = logging.getLogger(__name__)

_ENABLED      = os.getenv("CORR_ENABLED",      "true").lower() not in ("false", "0", "no")
_MAX_SAME_DIR = int(os.getenv("CORR_MAX_SAME_DIR", "3"))
_MAX_OPEN     = int(os.getenv("CORR_MAX_OPEN",     "5"))


def check_correlation(
    proposed_direction: str,
    open_positions: dict,
) -> Tuple[bool, str]:
    """
    Retorna:
      (True,  "")       → entrada permitida.
      (False, motivo)   → entrada bloqueada.

    Argumentos:
      proposed_direction  : "LONG" o "SHORT" (la dirección de la nueva entrada).
      open_positions      : dict { symbol: {"direction": "LONG" | "SHORT"} }
                            Puede ser {} si no hay posiciones abiertas.
    """
    if not _ENABLED:
        return True, ""

    direction = proposed_direction.upper()
    positions = list(open_positions.values()) if open_positions else []
    total     = len(positions)

    if total >= _MAX_OPEN:
        msg = (
            f"🔒 límite de posiciones abiertas alcanzado ({total}/{_MAX_OPEN}). "
            f"Cierra alguna posición antes de abrir otra."
        )
        log.info("[correlation_guard] %s", msg)
        return False, msg

    same_dir = sum(
        1 for p in positions
        if (p.get("direction") or "").upper() == direction
    )

    if same_dir >= _MAX_SAME_DIR:
        msg = (
            f"🔒 demasiadas posiciones {direction} ({same_dir}/{_MAX_SAME_DIR}). "
            f"Diversifica antes de añadir otra posición en la misma dirección."
        )
        log.info("[correlation_guard] %s", msg)
        return False, msg

    log.debug(
        "[correlation_guard] %s OK: total=%d same_dir=%d (max_open=%d max_same=%d)",
        direction, total, same_dir, _MAX_OPEN, _MAX_SAME_DIR,
    )
    return True, ""
