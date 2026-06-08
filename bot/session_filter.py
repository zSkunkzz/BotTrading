#!/usr/bin/env python3
"""
bot/session_filter.py — Filtro de sesión de trading.

Bloquea la apertura de nuevas posiciones fuera del horario de trading activo
según el tipo de setup:

  - TENDENCIA / BREAKOUT: solo entre SESSION_START_UTC y SESSION_END_UTC (UTC).
    Fuera de ese horario la liquidez es baja y los fakeouts son frecuentes.
  - REVERSAL: permitido 24h si SESSION_ALLOW_REVERSAL=true (default).
  - Si SESSION_FILTER_ENABLED=false, check_session() siempre retorna None (sin bloqueo).

Mejora #5 — Filtro London/NY open:
  Los primeros 30 minutos de la apertura de Londres (08:00 UTC) y de Nueva York
  (13:30 UTC) concentran spread alto y volatilidad de ruido. Se bloquean las
  entradas en esas ventanas para evitar falsas señales en el primer impulso.

Variables de entorno:
  SESSION_FILTER_ENABLED    (default: true)
  SESSION_START_UTC         (default: 7)   — hora de inicio en UTC (int)
  SESSION_END_UTC           (default: 18)  — hora de fin en UTC (int)
  SESSION_ALLOW_REVERSAL    (default: true)

  FILTER_LONDON_OPEN        (default: true)
      Bloquea entradas durante los primeros LONDON_OPEN_BLACKOUT_MINS minutos
      después de la apertura de Londres (08:00 UTC).

  LONDON_OPEN_BLACKOUT_MINS (default: 30)
      Duración en minutos de la ventana de bloqueo London open.

  FILTER_NY_OPEN            (default: true)
      Bloquea entradas durante los primeros NY_OPEN_BLACKOUT_MINS minutos
      después de la apertura de Nueva York (13:30 UTC).

  NY_OPEN_BLACKOUT_MINS     (default: 30)
      Duración en minutos de la ventana de bloqueo NY open.
"""

import logging
import os
from datetime import datetime, timezone
from typing import Optional

log = logging.getLogger(__name__)

_ENABLED        = os.getenv("SESSION_FILTER_ENABLED",  "true").lower() not in ("false", "0", "no")
_START_UTC      = int(os.getenv("SESSION_START_UTC",   "7"))
_END_UTC        = int(os.getenv("SESSION_END_UTC",     "18"))
_ALLOW_REVERSAL = os.getenv("SESSION_ALLOW_REVERSAL",  "true").lower() not in ("false", "0", "no")

# —— London open (08:00 UTC) ———————————————————————————————————————
_FILTER_LONDON       = os.getenv("FILTER_LONDON_OPEN",        "true").lower() not in ("false", "0", "no")
_LONDON_OPEN_H       = 8    # London open → 08:00 UTC
_LONDON_OPEN_M       = 0
_LONDON_BLACKOUT_MIN = int(os.getenv("LONDON_OPEN_BLACKOUT_MINS", "30"))

# —— NY open (13:30 UTC) —————————————————————————————————————————
_FILTER_NY           = os.getenv("FILTER_NY_OPEN",             "true").lower() not in ("false", "0", "no")
_NY_OPEN_H           = 13   # NY open → 13:30 UTC
_NY_OPEN_M           = 30
_NY_BLACKOUT_MIN     = int(os.getenv("NY_OPEN_BLACKOUT_MINS",    "30"))


def _minutes_since(now_utc: datetime, open_h: int, open_m: int) -> int:
    """
    Devuelve los minutos transcurridos desde la apertura de sesión del día.
    Devuelve -1 si todavía no ha llegado la apertura hoy.
    """
    today_open_min = open_h * 60 + open_m
    now_min        = now_utc.hour * 60 + now_utc.minute
    elapsed        = now_min - today_open_min
    return elapsed  # puede ser negativo (antes de apertura)


def _in_blackout(now_utc: datetime, open_h: int, open_m: int, blackout_mins: int) -> bool:
    """
    Devuelve True si el momento actual está dentro de la ventana de blackout
    [open_h:open_m, open_h:open_m + blackout_mins) UTC.
    """
    elapsed = _minutes_since(now_utc, open_h, open_m)
    return 0 <= elapsed < blackout_mins


def check_session(setup_type: Optional[str]) -> Optional[str]:
    """
    Devuelve:
      None   → sesión OK, la entrada está permitida.
      str    → motivo de bloqueo (se usa como reason en _result("HOLD", ...)).

    Prioridad de checks:
      1. Filtro desactivado globalmente → None.
      2. setup_type == 'REVERSAL' y SESSION_ALLOW_REVERSAL=true → None
         (REVERSAL permitido siempre, pero AÚN aplican los blackouts
          si FILTER_LONDON_OPEN / FILTER_NY_OPEN están activos).
      3. Blackout London open → bloqueo.
      4. Blackout NY open → bloqueo.
      5. Fuera del horario [SESSION_START_UTC, SESSION_END_UTC) → bloqueo.
    
    Nota: REVERSAL esquiva el chequeo de horario (paso 5) pero NO esquiva
    los blackouts de apertura (pasos 3-4) para evitar entrar en el primer
    spike de volatilidad tras la apertura europea/americana.
    """
    if not _ENABLED:
        return None

    st      = (setup_type or "").upper()
    now_utc = datetime.now(tz=timezone.utc)
    hour    = now_utc.hour

    # —— Blackout London open (aplica a TODOS los setup types) ——————————
    if _FILTER_LONDON and _in_blackout(now_utc, _LONDON_OPEN_H, _LONDON_OPEN_M, _LONDON_BLACKOUT_MIN):
        elapsed = _minutes_since(now_utc, _LONDON_OPEN_H, _LONDON_OPEN_M)
        reason = (
            f"⏰ session_filter: entrada bloqueada — London open blackout "
            f"(+{elapsed}min / {_LONDON_BLACKOUT_MIN}min desde 08:00 UTC). "
            f"Spread y volatilidad de ruido en los primeros minutos de Londres."
        )
        log.info("[session_filter] %s", reason)
        return reason

    # —— Blackout NY open (aplica a TODOS los setup types) ————————————
    if _FILTER_NY and _in_blackout(now_utc, _NY_OPEN_H, _NY_OPEN_M, _NY_BLACKOUT_MIN):
        elapsed = _minutes_since(now_utc, _NY_OPEN_H, _NY_OPEN_M)
        reason = (
            f"⏰ session_filter: entrada bloqueada — NY open blackout "
            f"(+{elapsed}min / {_NY_BLACKOUT_MIN}min desde 13:30 UTC). "
            f"Spread y volatilidad de ruido en los primeros minutos de Nueva York."
        )
        log.info("[session_filter] %s", reason)
        return reason

    # —— REVERSAL: esquiva el filtro de horario pero no los blackouts ——
    if st == "REVERSAL" and _ALLOW_REVERSAL:
        log.debug("[session_filter] REVERSAL — permitido 24h (fuera de blackout)")
        return None

    # —— Horario general [START_UTC, END_UTC) —————————————————————
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
