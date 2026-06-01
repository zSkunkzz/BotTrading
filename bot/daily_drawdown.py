#!/usr/bin/env python3
"""
daily_drawdown.py — Límite de drawdown diario

v2 — BUG #8 FIX: reset a hora configurada en zona horaria correcta

  El bug original comparaba solo el 'day' del calendario UTC.
  Esto hacía que el reset se produjera a las 00:00 UTC aunque el usuario
  operara en CET (UTC+2), permitiendo efectivamente DD*2 en la ventana
  nocturna 00:00–02:00 hora local.

  Fix: reset se dispara al cruzar DRAWDOWN_RESET_HOUR_UTC en UTC (default 0),
  comparando (día, hora) en lugar de solo (día).
  Env var DRAWDOWN_TZ permite especificar zona horaria local (e.g. Europe/Madrid)
  para que el reset sea a medianoche local en lugar de UTC.

Config Railway:
  MAX_DAILY_DRAWDOWN_PCT  → default 5.0
  DRAWDOWN_RESET_HOUR_UTC → default 0  (hora UTC del reset, 0-23)
  DRAWDOWN_TZ             → default '' (vacío = UTC). Ejemplo: 'Europe/Madrid'
"""
from __future__ import annotations

import logging
import os
import time
from datetime import datetime, timezone
from typing import Optional

log = logging.getLogger(__name__)

MAX_DD_PCT     = float(os.getenv("MAX_DAILY_DRAWDOWN_PCT", "5.0"))
RESET_HOUR_UTC = int(os.getenv("DRAWDOWN_RESET_HOUR_UTC", "0"))
_TZ_NAME       = os.getenv("DRAWDOWN_TZ", "").strip()

# Resolver zona horaria
def _get_tz():
    if not _TZ_NAME:
        return timezone.utc
    try:
        import zoneinfo
        return zoneinfo.ZoneInfo(_TZ_NAME)
    except Exception:
        try:
            from dateutil import tz as _tz
            z = _tz.gettz(_TZ_NAME)
            if z:
                return z
        except Exception:
            pass
        log.warning(
            "[drawdown] No se pudo resolver DRAWDOWN_TZ=%r — usando UTC.", _TZ_NAME
        )
        return timezone.utc

_TZ = _get_tz()


class DailyDrawdown:
    """
    Registra P&L realizado del día y bloquea nuevas entradas si se supera el límite.

    BUG #8 FIX: _check_reset compara (día, hora) en la zona horaria configurada,
    no solo (día) en UTC. Esto evita la ventana de doble DD al cruzar medianoche
    en zonas horarias UTC+N.
    """

    def __init__(self) -> None:
        self._day_pnl: float     = 0.0
        self._balance_ref: float = 0.0
        # Guardamos (día, hora_reset) del último reset para comparación correcta
        self._last_reset_key: Optional[tuple] = None
        self._blocked: bool = False

    def set_balance_ref(self, balance: float) -> None:
        self._balance_ref = balance
        log.info("[drawdown] Balance ref = $%.2f", balance)

    def record_trade(self, pnl_usd: float) -> None:
        self._check_reset()
        self._day_pnl += pnl_usd
        pct = self._drawdown_pct()
        log.info(
            "[drawdown] P&L hoy: $%.2f (%.2f%%) · Límite: %.1f%%",
            self._day_pnl, pct, MAX_DD_PCT,
        )
        if pct <= -MAX_DD_PCT and not self._blocked:
            self._blocked = True
            log.warning(
                "[drawdown] 🛑 Drawdown diario %.2f%% supera límite %.1f%% — "
                "bloqueando nuevas entradas hasta el próximo reset (%s h%02d)",
                pct, MAX_DD_PCT,
                _TZ_NAME or "UTC", RESET_HOUR_UTC,
            )

    def is_blocked(self) -> bool:
        self._check_reset()
        return self._blocked

    def day_pnl(self) -> float:
        return self._day_pnl

    def drawdown_pct(self) -> float:
        return self._drawdown_pct()

    def summary(self) -> str:
        icon = "\ud83d\uded1" if self._blocked else (
            "\ud83d\udfe1" if self._drawdown_pct() < -MAX_DD_PCT * 0.7 else "\u2705"
        )
        return (
            f"{icon} Drawdown hoy: ${self._day_pnl:+.2f} "
            f"({self._drawdown_pct():+.2f}%) · Límite: {MAX_DD_PCT:.1f}%"
        )

    def _drawdown_pct(self) -> float:
        if self._balance_ref <= 0:
            return 0.0
        return (self._day_pnl / self._balance_ref) * 100.0

    def _current_reset_key(self) -> tuple:
        """
        BUG #8 FIX: devuelve (año, mes, dia, hora_reset_slot) en la TZ configurada.
        El 'hora_reset_slot' es 0 si estamos antes de RESET_HOUR_UTC, 1 si después,
        de modo que al cruzar RESET_HOUR_UTC se genera una nueva clave y se resetea.

        Ejemplo con RESET_HOUR_UTC=0, TZ=Europe/Madrid:
          23:59 CET del 1 jun → key = (2026, 6, 1, 0) [antes del reset de hoy]
          00:01 CET del 2 jun → key = (2026, 6, 2, 0) [nuevo día → reset]

        Ejemplo con RESET_HOUR_UTC=8, TZ=UTC:
          07:59 UTC → key = (2026, 6, 1, 0) [antes del reset de hoy]
          08:01 UTC → key = (2026, 6, 1, 1) [cruzamos hora 8 → reset]
        """
        now_local = datetime.now(_TZ)
        # Si la hora local ya superó el RESET_HOUR_UTC, estamos en la ventana post-reset
        past_reset = 1 if now_local.hour >= RESET_HOUR_UTC else 0
        return (now_local.year, now_local.month, now_local.day, past_reset)

    def _check_reset(self) -> None:
        key = self._current_reset_key()
        if key != self._last_reset_key:
            prev_pnl = self._day_pnl
            self._day_pnl       = 0.0
            self._blocked       = False
            self._last_reset_key = key
            if prev_pnl != 0:
                log.info("[drawdown] Reset diario — P&L sesión anterior: $%.2f", prev_pnl)


# Singleton
daily_drawdown = DailyDrawdown()
