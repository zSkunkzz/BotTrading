#!/usr/bin/env python3
"""
bot/session_filter.py — Filtro de sesión de trading.

Bloquea la apertura de nuevas posiciones fuera del horario de trading activo
según el tipo de setup:

  - TENDENCIA / BREAKOUT: solo entre SESSION_START_UTC y SESSION_END_UTC (UTC).
    Fuera de ese horario la liquidez es baja y los fakeouts son frecuentes.
  - REVERSAL: permitido 24h si SESSION_ALLOW_REVERSAL=true (default).
  - Si SESSION_FILTER_ENABLED=false, check_session() siempre retorna None (sin bloqueo).

Variables de entorno:
  SESSION_FILTER_ENABLED  (default: true)
  SESSION_START_UTC       (default: 7)   — hora de inicio en UTC (int)
  SESSION_END_UTC         (default: 18)  — hora de fin en UTC (int)
  SESSION_ALLOW_REVERSAL  (default: true)
"""

import logging
import os
from datetime import datetime, timezone
from typing import Optional

log = logging.getLogger(__name__)

_ENABLED         = os.getenv("SESSION_FILTER_ENABLED", "true").lower() not in ("false", "0", "no")
_START_UTC       = int(os.getenv("SESSION_START_UTC",  "7"))
_END_UTC         = int(os.getenv("SESSION_END_UTC",    "18"))
_ALLOW_REVERSAL  = os.getenv("SESSION_ALLOW_REVERSAL", "true").lower() not in ("false", "0", "no")


def check_session(setup_type: Optional[str]) -> Optional[str]:
    """
    Devuelve:
      None   → sesión OK, la entrada está permitida.
      str    → motivo de bloqueo (se usa como reason en _result("HOLD", ...)).

    Lógica:
      - Filtro desactivado → None.
      - setup_type == 'REVERSAL' y SESSION_ALLOW_REVERSAL=true → None.
      - Dentro del horario [START_UTC, END_UTC) → None.
      - Fuera del horario → string de bloqueo.
    """
    if not _ENABLED:
        return None

    st = (setup_type or "").upper()

    if st == "REVERSAL" and _ALLOW_REVERSAL:
        log.debug("[session_filter] REVERSAL — permitido 24h")
        return None

    now_utc = datetime.now(tz=timezone.utc)
    hour    = now_utc.hour

    if _START_UTC <= hour < _END_UTC:
        log.debug(
            "[session_filter] %s — hora UTC=%d dentro de [%d, %d) — OK",
            st or "(sin tipo)", hour, _START_UTC, _END_UTC,
        )
        return None

    reason = (
        f"⏰ session_filter: {st or 'setup'} bloqueado fuera de sesión activa "
        f"(UTC {hour:02d}:xx está fuera de [{_START_UTC:02d}:00–{_END_UTC:02d}:00]). "
        f"Espera la apertura europea/americana para TENDENCIA/BREAKOUT."
    )
    log.info("[session_filter] %s", reason)
    return reason
