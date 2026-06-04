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

v3 — BUG #11 FIX: persistencia del P&L diario entre reinicios de Railway

  El drawdown acumulado se perdía en cada restart/redeploy del contenedor.
  Si el bot había acumulado -3% de drawdown antes del reinicio, al arrancar
  el contador volvía a 0 permitiendo un 5% adicional ese mismo día.

  Fix: _day_pnl y _last_reset_key se persisten en bot_state.json a través
  de state.py. En _load_from_state() se restauran al arrancar si la clave
  de reset corresponde al mismo período (mismo día/slot), descartándolos
  si el estado guardado pertenece a un período anterior.

v3 — BUG #14 FIX: balance_ref dinámico

  _balance_ref se inicializaba una sola vez al arrancar y nunca se actualizaba.
  Si el balance crecía significativamente, el % de drawdown calculado divergía
  del drawdown real. Ahora set_balance_ref() puede llamarse periódicamente
  (recomendado: una vez por hora o tras cada trade ganador) y actualiza el
  balance de referencia solo si el nuevo valor es mayor que el anterior
  (no se reduce la referencia intradiaria para no relajar el límite de DD).

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

# Clave usada para guardar/restaurar estado en bot_state.json
_STATE_KEY = "daily_drawdown"


class DailyDrawdown:
    """
    Registra P&L realizado del día y bloquea nuevas entradas si se supera el límite.

    BUG #8 FIX: _check_reset compara (día, hora) en la zona horaria configurada.
    BUG #11 FIX: _day_pnl y _last_reset_key se persisten en state.py para
                 sobrevivir reinicios de Railway sin perder el DD acumulado.
    BUG #14 FIX: set_balance_ref() actualiza el balance de referencia dinámicamente
                 pero solo al alza — no reduce la ref intradiaria para no relajar
                 el límite de DD durante la sesión.
    """

    def __init__(self) -> None:
        self._day_pnl: float     = 0.0
        self._balance_ref: float = 0.0
        self._last_reset_key: Optional[tuple] = None
        self._blocked: bool = False
        # BUG #11 FIX: intentar restaurar estado persistido
        self._load_from_state()

    # ── BUG #11 FIX: persistencia via state.py ───────────────────────────────

    def _load_from_state(self) -> None:
        """
        Restaura _day_pnl y _last_reset_key desde bot_state.json.
        Solo restaura si la clave de reset guardada corresponde al período actual
        (mismo día/slot de reset), descartando datos de períodos anteriores.
        """
        try:
            from bot.state import bot_state
            data = bot_state._positions.get(_STATE_KEY)
            if not isinstance(data, dict):
                return
            saved_key_raw = data.get("reset_key")
            saved_pnl     = data.get("day_pnl", 0.0)
            saved_blocked = data.get("blocked", False)
            if saved_key_raw is None:
                return
            # Reconstruir la tupla (JSON la serializa como lista)
            saved_key = tuple(saved_key_raw)
            current_key = self._current_reset_key()
            if saved_key == current_key:
                self._day_pnl         = float(saved_pnl)
                self._last_reset_key  = saved_key
                self._blocked         = bool(saved_blocked)
                log.info(
                    "[drawdown] Estado restaurado tras reinicio: P&L=%.2f blocked=%s",
                    self._day_pnl, self._blocked,
                )
            else:
                log.info(
                    "[drawdown] Estado guardado es de período anterior — descartando "
                    "(guardado=%s, actual=%s).",
                    saved_key, current_key,
                )
        except Exception as e:
            log.debug("[drawdown] _load_from_state error (ignorado): %s", e)

    def _save_to_state(self) -> None:
        """
        Persiste _day_pnl, _last_reset_key y _blocked en bot_state.json.
        Usa el sync helper de BotState para no requerir await.
        """
        try:
            from bot.state import bot_state
            bot_state._save_position_sync(_STATE_KEY, {
                "reset_key": list(self._last_reset_key) if self._last_reset_key else None,
                "day_pnl":   self._day_pnl,
                "blocked":   self._blocked,
            })
        except Exception as e:
            log.debug("[drawdown] _save_to_state error (ignorado): %s", e)

    # ── BUG #14 FIX: balance_ref dinámico solo al alza ───────────────────────

    def set_balance_ref(self, balance: float) -> None:
        """
        BUG #14 FIX: actualiza _balance_ref solo si el nuevo valor es mayor
        que el actual. Esto permite que el límite de DD se recalcule cuando
        el balance crece (mayores ganancias → mayor base de cálculo), pero
        no reduce la base intradiaria si el balance baja (eso sería
        relajar el límite durante una racha perdedora).

        Llamar periódicamente: una vez al arrancar y una vez por hora.
        """
        if balance > self._balance_ref:
            old = self._balance_ref
            self._balance_ref = balance
            if old > 0:
                log.info(
                    "[drawdown] Balance ref actualizado: $%.2f → $%.2f",
                    old, balance,
                )
            else:
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
        # BUG #11 FIX: persistir tras cada trade
        self._save_to_state()

    def is_blocked(self) -> bool:
        self._check_reset()
        return self._blocked

    def day_pnl(self) -> float:
        return self._day_pnl

    def drawdown_pct(self) -> float:
        return self._drawdown_pct()

    def summary(self) -> str:
        # BUG #18 FIX: emoji como literales Unicode en lugar de surrogates UTF-16
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
        El 'hora_reset_slot' es 0 si estamos antes de RESET_HOUR_UTC, 1 si después,
        de modo que al cruzar RESET_HOUR_UTC se genera una nueva clave y se resetea.
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
            # BUG #11 FIX: persistir el reset
            self._save_to_state()


# Singleton
daily_drawdown = DailyDrawdown()
