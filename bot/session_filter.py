#!/usr/bin/env python3
"""
session_filter.py — Filtro de sesión de trading.

Bloquea entradas de tipo TENDENCIA y BREAKOUT fuera del horario de mayor
liquidez (London open + NY overlap). Los setups REVERSAL se permiten las
24h porque buscan agotamiento de tendencia, que ocurre en cualquier sesión.

Horario activo por defecto: 07:00–18:00 UTC
  - London open:  07:00–09:00 UTC
  - NY open:      13:00–15:00 UTC
  - Overlap:      13:00–17:00 UTC

Fuera de este horario (Asia/noche europea), los fakeouts en breakout y
tendencia son significativamente más frecuentes por baja liquidez.

Config Railway:
  SESSION_FILTER_ENABLED   → default true  (false = desactiva el filtro)
  SESSION_START_UTC        → default 7     (hora UTC de inicio)
  SESSION_END_UTC          → default 18    (hora UTC de fin)
  SESSION_ALLOW_REVERSAL   → default true  (REVERSAL siempre permitido)
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Optional

log = logging.getLogger(__name__)

SESSION_FILTER_ENABLED = os.getenv("SESSION_FILTER_ENABLED", "true").lower() != "false"
SESSION_START_UTC      = int(os.getenv("SESSION_START_UTC",   "7"))
SESSION_END_UTC        = int(os.getenv("SESSION_END_UTC",     "18"))
SESSION_ALLOW_REVERSAL = os.getenv("SESSION_ALLOW_REVERSAL",  "true").lower() != "false"


def is_trading_session(setup_type: Optional[str] = None) -> tuple[bool, str]:
    """
    Determina si la hora actual es adecuada para operar el setup dado.

    Args:
        setup_type: 'TENDENCIA' | 'BREAKOUT' | 'REVERSAL' | None

    Returns:
        (True, "")           si se permite la entrada
        (False, reason_str)  si se bloquea
    """
    if not SESSION_FILTER_ENABLED:
        return True, ""

    # REVERSAL permitido 24h (busca agotamiento, no liquidez)
    if SESSION_ALLOW_REVERSAL and setup_type == "REVERSAL":
        return True, ""

    now_utc  = datetime.now(timezone.utc)
    hour_utc = now_utc.hour + now_utc.minute / 60.0

    in_session = SESSION_START_UTC <= hour_utc < SESSION_END_UTC

    if in_session:
        return True, ""

    reason = (
        f"⏰ Fuera de sesión ({now_utc.strftime('%H:%M')} UTC, "
        f"activa {SESSION_START_UTC:02d}:00–{SESSION_END_UTC:02d}:00 UTC) "
        f"— {setup_type or 'setup'} bloqueado (baja liquidez)"
    )
    log.info("[session_filter] %s", reason)
    return False, reason


def check_session(setup_type: Optional[str] = None) -> Optional[str]:
    """
    Convenience wrapper: devuelve el motivo de bloqueo o None si OK.
    Úsalo en signal_engine o strategy para obtener el motivo directamente.
    """
    allowed, reason = is_trading_session(setup_type)
    return None if allowed else reason
