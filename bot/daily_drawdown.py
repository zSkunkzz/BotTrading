#!/usr/bin/env python3
"""
daily_drawdown.py — Límite de drawdown diario

v2 — BUG #8 FIX: reset a hora configurada en zona horaria correcta
  El bug original comparaba solo el 'day' del calendario UTC.
  Fix: reset se dispara al cruzar DRAWDOWN_RESET_HOUR_UTC en UTC (default 0),
  comparando (día, hora) en lugar de solo (día).
  Env var DRAWDOWN_TZ permite especificar zona horaria local (e.g. Europe/Madrid)
  para que el reset sea a medianoche local en lugar de UTC.

v3 — FIX #11: Persistencia del P&L diario en bot_state.json
  Al reiniciar Railway (deploy, crash, OOM) el contador de P&L diario se perdía,
  permitiendo efectivamente el doble del drawdown permitido en ese mismo día.
  Fix: _day_pnl y _last_reset_key se persisten via state.py y se restauran
  en __init__. Se exponen persist() y restore() para que main.py pueda llamarlos
  en el arranque sin depender de import circular.

v3 — FIX #14: _balance_ref se actualiza tras trades ganadores
  El balance de referencia inicial se fijaba al arranque y nunca se actualizaba.
  Si el balance crecía, el cálculo de DD% se desviaba sistemáticamente.
  Fix: record_trade() acepta current_balance opcional para actualizar _balance_ref
  solo cuando el trade es ganador (no queremos reducir el ref ante pérdidas para
  evitar que el límite de DD se vuelva más permisivo cuando el balance cae).

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

# Claves usadas para persistir en bot_state.json via state.py
_STATE_KEY_PNL       = "_dd_day_pnl"
_STATE_KEY_RESET_KEY = "_dd_last_reset_key"
_STATE_KEY_BALANCE   = "_dd_balance_ref"


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

    FIX #11: _day_pnl y _last_reset_key se persisten en bot_state.json para
    sobrevivir reinicios de Railway sin perder el tracking del drawdown diario.

    FIX #14: _balance_ref se actualiza con el balance actual cuando hay trades
    ganadores, manteniendo el cálculo de DD% preciso a medida que el balance crece.
    """

    def __init__(self) -> None:
        self._day_pnl: float     = 0.0
        self._balance_ref: float = 0.0
        self._last_reset_key: Optional[tuple] = None
        self._blocked: bool = False
        # FIX #11: intentar restaurar desde state.py al arrancar
        self._restore_from_state()

    def _restore_from_state(self) -> None:
        """
        FIX #11: recupera _day_pnl y _last_reset_key desde bot_state.json.
        Si el reset_key guardado corresponde al período actual, restaura el P&L.
        Si el período ha cambiado (nuevo día/hora de reset), empieza desde 0.
        """
        try:
            # Importación diferida para evitar circulares al nivel de módulo
            from bot.state import bot_state
            # Usar la API sync ya que __init__ no es async
            state_data = bot_state._positions.get("__drawdown__", {})
            if not state_data:
                return

            saved_pnl       = state_data.get(_STATE_KEY_PNL, 0.0)
            saved_reset_key = state_data.get(_STATE_KEY_RESET_KEY)
            saved_balance   = state_data.get(_STATE_KEY_BALANCE, 0.0)

            # Convertir la lista guardada en JSON a tuple
            if isinstance(saved_reset_key, list):
                saved_reset_key = tuple(saved_reset_key)

            current_key = self._current_reset_key()
            if saved_reset_key == current_key:
                # Mismo período — restaurar P&L y balance ref
                self._day_pnl        = float(saved_pnl)
                self._last_reset_key = saved_reset_key
                if saved_balance > 0:
                    self._balance_ref = float(saved_balance)
                # Recalcular si está bloqueado
                if self._drawdown_pct() <= -MAX_DD_PCT:
                    self._blocked = True
                log.info(
                    "[drawdown] ♻️ P&L restaurado desde state: $%.2f (%.2f%%) — período actual",
                    self._day_pnl, self._drawdown_pct(),
                )
            else:
                # Período diferente — empezar limpio (no restaurar P&L)
                log.info(
                    "[drawdown] Nuevo período detectado al arrancar — P&L reseteado a $0."
                )
        except Exception as e:
            log.debug("[drawdown] _restore_from_state: %s (no crítico)", e)

    def _persist_to_state(self) -> None:
        """
        FIX #11: guarda _day_pnl, _last_reset_key y _balance_ref en bot_state.json
        usando la API sync de BotState (compatible con contexto no-async).
        """
        try:
            from bot.state import bot_state
            payload = {
                _STATE_KEY_PNL:       self._day_pnl,
                _STATE_KEY_RESET_KEY: list(self._last_reset_key) if self._last_reset_key else None,
                _STATE_KEY_BALANCE:   self._balance_ref,
            }
            bot_state._positions["__drawdown__"] = payload
            bot_state._save_sync()
        except Exception as e:
            log.debug("[drawdown] _persist_to_state: %s (no crítico)", e)

    def set_balance_ref(self, balance: float) -> None:
        if balance > 0:
            self._balance_ref = balance
            log.info("[drawdown] Balance ref = $%.2f", balance)
            self._persist_to_state()

    def record_trade(self, pnl_usd: float, current_balance: float = 0.0) -> None:
        """
        Registra el resultado de un trade.

        FIX #14: si current_balance > _balance_ref (balance ha crecido), actualiza
        el balance de referencia. Esto mantiene el cálculo de DD% preciso a medida
        que el bot genera ganancias. No se reduce el ref ante pérdidas para no hacer
        el límite de DD más permisivo cuando el balance cae.
        """
        self._check_reset()
        self._day_pnl += pnl_usd

        # FIX #14: actualizar balance ref si el balance ha crecido
        if current_balance > 0 and pnl_usd > 0 and current_balance > self._balance_ref:
            old_ref = self._balance_ref
            self._balance_ref = current_balance
            log.info(
                "[drawdown] Balance ref actualizado: $%.2f → $%.2f (trade ganador)",
                old_ref, self._balance_ref,
            )

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
        # FIX #11: persistir tras cada trade
        self._persist_to_state()

    def is_blocked(self) -> bool:
        self._check_reset()
        return self._blocked

    def day_pnl(self) -> float:
        return self._day_pnl

    def drawdown_pct(self) -> float:
        return self._drawdown_pct()

    def summary(self) -> str:
        # FIX #18: usar literales Unicode en lugar de surrogates
        icon = "\U0001f6d1" if self._blocked else (
            "\U0001f7e1" if self._drawdown_pct() < -MAX_DD_PCT * 0.7 else "\u2705"
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
        """
        now_local = datetime.now(_TZ)
        past_reset = 1 if now_local.hour >= RESET_HOUR_UTC else 0
        return (now_local.year, now_local.month, now_local.day, past_reset)

    def _check_reset(self) -> None:
        key = self._current_reset_key()
        if key != self._last_reset_key:
            prev_pnl = self._day_pnl
            self._day_pnl        = 0.0
            self._blocked        = False
            self._last_reset_key = key
            if prev_pnl != 0:
                log.info("[drawdown] Reset diario — P&L sesión anterior: $%.2f", prev_pnl)
            # FIX #11: persistir el estado reseteado
            self._persist_to_state()


# Singleton
daily_drawdown = DailyDrawdown()
