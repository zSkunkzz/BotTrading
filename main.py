"""main.py — Loop principal con trailing stop, break-even lock, TP dinámico y cooldown inteligente.

v4: Cooldown diferenciado por calidad de señal.
  - SL en los primeros SMART_COOLDOWN_FAST_WINDOW segundos con score >= SMART_COOLDOWN_HIGH_SCORE
    → cooldown reducido a COOLDOWN_SL_FAST (probablemente ruido puntual, no tendencia)
  - Resto de SLs → COOLDOWN_SL normal (60min)
  - TPs → COOLDOWN_TP (30min, sin cambios)

fix trailing SL:
  trail_high/trail_low se actualizan SIEMPRE que el precio supera el pico,
  independientemente de si new_sl mejora el SL actual.
  Antes el trailing quedaba congelado después de un break-even lock si el precio
  seguía subiendo pero new_sl <= pos['sl'] (ya movido por el lock).

v4.1:
  - evaluate() devuelve 4 valores: (side, score, regime, metrics)
  - Se loguea ADX_1h, ADX_15m y vol_ratio en nivel INFO con cada señal evaluada.
  - Nivel de log configurable via env LOG_LEVEL (default INFO).

BUG-5 fix:
  _update_trailing() llamaba telegram.notify() en cada tick que movía el SL.
  Con LOOP_SLEEP=20s y un rally fuerte podía enviar 100+ mensajes/hora.
  Fix: _trailing_notified dict + TRAILING_NOTIFY_COOLDOWN (300s) por símbolo.
  El log INFO sigue escribiéndose en cada tick; solo el Telegram está rate-limitado.

BUG-3 fix:
  _exit_price_for usaba umbral de 0.5% (0.995/1.005) para detectar TP/SL hit.
  Con TP a 2×RR ese margen producía falsos positivos — el bot registraba un TP
  cuando el precio aún estaba 0.4% lejos del nivel real. Reducido a 0.1% (0.001).

BUG-4 fix:
  open_ts se fijaba con time.time() del loop, varios segundos después de la
  ejecución real de la orden. Ahora se llama _get_position_open_ts() tras
  open_order() para sincronizar el timestamp real desde el historial BingX.
  Fallback a time.time() si la llamada falla (comportamiento anterior).
"""
import logging
import os
import sys
import time
from datetime import datetime, timezone

import bot_state
import config
import exchange
import risk
import signals
import telegram
import tg_commands
import trade_logger
from ws_feed import KlineFeed

_log_level = getattr(logging, os.environ.get("LOG_LEVEL", "INFO").upper(), logging.INFO)
logging.basicConfig(
    level=_log_level,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("main")

_cooldown: dict[str, float] = {}
_cooldown_reason: dict[str, str] = {}   # 'tp' | 'sl' | 'sl_fast'
_manual_alert_cooldown: dict[str, float] = {}

# BUG-5 FIX: rate-limit de notificaciones Telegram para trailing SL.
_trailing_notified: dict[str, float] = {}   # symbol → timestamp última notificación
TRAILING_NOTIFY_COOLDOWN = 5 * 60          # 5 minutos entre notificaciones por símbolo

COOLDOWN_SL           = 60 * 60       # 60 min — SL normal
COOLDOWN_SL_FAST      = 15 * 60       # 15 min — SL rápido (score alto + cierre temprano)
COOLDOWN_TP           = 30 * 60       # 30 min — TP
MANUAL_ALERT_COOLDOWN  = 60 * 60
MAX_TP_EXTENSIONS      = 3
TP_EXTEND_RR           = 1.5
TP_EXTEND_THRESH       = 0.015
MIN_HOLD_SECS          = 90

# v4: Smart cooldown
SMART_COOLDOWN_FAST_WINDOW     = 15 * 60   # 15 min desde apertura
SMART_COOLDOWN_HIGH_SCORE      = 85        # score mínimo para activar smart cooldown

READY_TIMEOUT = 120
READY_MIN_PCT = 0.80

WEEKDAY_MIN_SCORE = int(getattr(config, "WEEKDAY_MIN_SCORE", 70))
WEEKEND_MIN_SCORE = int(getattr(config, "WEEKEND_MIN_SCORE", 90))

VALID_SIDES = {"long", "short"}

CLOSE_CONFIRM_LOOPS = 2
_missing_count: dict[str, int] = {}

_weekend_notified_day: int = -1

MAX_SAME_SIDE = int(getattr(config, "MAX_SAME_SIDE", 4))


def _is_weekend() -> bool:
    return datetime.now(timezone.utc).weekday() >= 5


def _cooldown_for(symbol: str) -> int:
    reason = _cooldown_reason.get(symbol, "sl")
    if reason == "tp":
        return COOLDOWN_TP
    if reason == "sl_fast":
        return COOLDOWN_SL_FAST
    return COOLDOWN_SL


def _corr_group_for(symbol: str) -> int | None:
    for idx, group in enumerate(config.CORR_GROUPS):
        if symbol in group:
            return idx
    return None


def _check_directional_guard(signal: str, positions: dict, symbol: str) -> bool:
    same_side_count = sum(1 for p in positions.values() if p["side"] == signal)
    if same_side_count >= MAX_SAME_SIDE:
        log.debug(
            "[%s] Guard MAX_SAME_SIDE: ya hay %d posiciones %s (máx %d) — skip",
            symbol, same_side_count, signal.upper(), MAX_SAME_SIDE,
        )
        return False

    grp_idx = _corr_group_for(symbol)
    if grp_idx is not None:
        grp_count = sum(
            1 for sym, p in positions.items()
            if _corr_group_for(sym) == grp_idx
        )
        max_corr = getattr(config, "MAX_CORR_PER_GROUP", 2)
        if grp_count >= max_corr:
            log.debug(
                "[%s] Guard CORR_GROUP[%d]: ya hay %d posiciones en el grupo (máx %d) — skip",
                symbol, grp_idx, grp_count, max_corr,
            )
            return False

    return True


def _sync_entry_from_exchange(symbol: str, local_price: float, side: str) -> float:
    try:
        pos_live = exchange.get_position(symbol)
        if pos_live and pos_live.get("side") == side:
            real_entry = float(pos_live.get("entry") or 0.0)
            if real_entry > 0:
                drift_pct = abs(real_entry - local_price) / local_price * 100
                if drift_pct > 0.1:
                    log.info(
                        "[%s] Entry sincronizado desde exchange: %.6f → %.6f (drift %.3f%%)",
                        symbol, local_price, real_entry, drift_pct,
                    )
                return real_entry
    except Exception as exc:
        log.warning(
            "[%s] No se pudo sincronizar entry real tras apertura: %s — usando precio feed",
            symbol, exc,
        )
    return local_price


def _apply_breakeven(symbol: str, pos: dict, current_price: float) -> None:
    if time.time() - pos.get("open_ts", 0) < MIN_HOLD_SECS:
        return

    activated = risk.check_breakeven(symbol, pos, current_price)
    if not activated:
        return

    new_sl = pos["sl"]
    side   = pos["side"]
    try:
        exchange.cancel_all_orders(symbol)
        exchange.place_stop_order(symbol, side, pos["qty"], new_sl)
        exchange.place_tp_order(symbol, side, pos["qty"], pos["tp"])
        telegram.notify(
            f"\U0001f512 <b>Break-even activado</b>\n"
            f"{symbol} {side.upper()}\n"
            f"SL movido a entry+buffer: <code>{new_sl:.6f}</code>\n"
            f"Trade gratuito desde aquí."
        )
    except Exception as e:
        log.warning("[%s] Error actualizando SL break-even en exchange: %s", symbol, e)
        pos["be_locked"] = False
        pos["sl"]        = pos.get("be_trigger", new_sl)


def _update_trailing(symbol: str, pos: dict, current_price: float) -> None:
    if time.time() - pos.get("open_ts", 0) < MIN_HOLD_SECS:
        return

    side       = pos["side"]
    trail_step = pos.get("trail_step", 0)
    if trail_step <= 0:
        return
    if pos.get("sl") is None or pos.get("tp") is None:
        retur