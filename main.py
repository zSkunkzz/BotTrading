"""main.py — Loop principal con trailing stop, break-even lock, TP dinámico y cooldown inteligente.

v4: Cooldown diferenciado por calidad de señal.
v5: BE silencioso notificado, score sync=0, clasificación SL/TP por order_type, _extending leak fix.
v6: _check_tp_extension usa WEEKDAY_MIN_SCORE, señales manuales consumidas, contabilidad medianoche.
v7: SL/TP restaurados tras reinicio si Hyperliquid no los devuelve.
v8:
  - _update_trailing: _round_price en new_sl, debounce 5 min por par, notificación enriquecida
    con PnL latente y distancia al TP.
  - _check_tp_extension: fórmula lineal (entry + tp_orig_dist × (n+1)) en lugar de
    multiplicador acumulativo que escalaba demasiado. _round_price en new_tp.
  - _open_position: regime pasado a notify_open (antes siempre llegaba vacío).
v9:
  - _open_position: guard de exchange en tiempo real antes de enviar open_order.
    Si ya existe posición para el símbolo en Hyperliquid (race condition tras reinicio),
    se aborta la apertura y se registra localmente en lugar de doblar el tamaño.
v10:
  - _open_position: lock en memoria (_opening set) que bloquea aperturas dobles
    dentro del mismo proceso aunque el guard de exchange no haya visto aún la orden
    (latencia exchange ~100-300 ms). El guard v9 se mantiene como segunda línea.
  - Causa real: señal manual + señal automática evaluadas casi simultáneamente
    para el mismo símbolo, o dos loops muy juntos cuando LOOP_SLEEP es bajo.
v11:
  - _cycle_lock (threading.Lock): impide que el siguiente loop empiece antes
    de que el anterior haya terminado. Si el loop tarda más que LOOP_SLEEP
    los ciclos ya no se solapan — el nuevo se descarta con un WARNING.
    Causa real: LOOP_SLEEP bajo + evaluación de ~50 pares en paralelo →
    múltiples ciclos activos simultáneamente → señales duplicadas.
  - _restore_sl_tp retry: si una posición sincronizada (reinicio/race) sigue
    con sl=None o tp=None en loops posteriores (feed no listo en el momento
    del sync), se reintenta _restore_sl_tp_on_sync cada iteración hasta que
    el feed esté listo. Fix directo para HYPE (y cualquier par) abierto sin
    SL/TP tras reinicio del bot.
v13:
  - _apply_breakeven, _update_trailing y _check_tp_extension usan
    exchange.modify_sltp_orders() en lugar de cancel_all_orders→place_stop→place_tp.
    Esto elimina la ventana de desprotección entre el cancel y el re-place:
    batchModify modifica las órdenes in-place; si el exchange las rechaza,
    las antiguas siguen activas. Fallback a cancel+place solo si batchModify
    falla completamente (gestionado dentro de modify_sltp_orders).
"""
import logging
import os
import sys
import threading
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

_LOG_LEVEL_STR = os.environ.get("LOG_LEVEL", "INFO").upper()
_LOG_LEVEL = getattr(logging, _LOG_LEVEL_STR, logging.INFO)

logging.basicConfig(
    level=_LOG_LEVEL,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("main")
log.info("Log level activo: %s", _LOG_LEVEL_STR)

for _noisy_logger in ("httpcore", "httpx", "websockets", "asyncio"):
    logging.getLogger(_noisy_logger).setLevel(logging.WARNING)

_cooldown: dict[str, float] = {}
_cooldown_reason: dict[str, str] = {}
_manual_alert_cooldown: dict[str, float] = {}
# v8: debounce para notificaciones de trailing (ts del último envío por símbolo)
_trailing_notify_ts: dict[str, float] = {}
TRAILING_NOTIFY_DEBOUNCE = 5 * 60  # 5 min entre notificaciones trailing del mismo par

# v10: mutex en memoria — impide que dos llamadas a _open_position para el mismo
# símbolo se solapen antes de que la primera haya registrado la posición.
_opening: set[str] = set()

# v11: lock de ciclo — impide que el siguiente loop empiece antes de que el
# anterior haya terminado. Si LOOP_SLEEP < duración real del loop, el ciclo
# nuevo se descarta silenciosamente en lugar de ejecutarse en paralelo.
_cycle_lock = threading.Lock()

COOLDOWN_SL           = 60 * 60
COOLDOWN_SL_FAST      = 15 * 60
COOLDOWN_TP           = 30 * 60
MANUAL_ALERT_COOLDOWN  = 60 * 60
MAX_TP_EXTENSIONS      = 3
TP_EXTEND_RR           = 1.5
TP_EXTEND_THRESH       = 0.015
MIN_HOLD_SECS          = 90

SMART_COOLDOWN_FAST_WINDOW     = 15 * 60
SMART_COOLDOWN_HIGH_SCORE      = 85

_SYNC_SCORE_UNKNOWN = 0

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
        exchange.modify_sltp_orders(symbol, side, pos["qty"], new_sl, pos["tp"])
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
        telegram.notify(
            f"\u26a0\ufe0f <b>BE fallido en exchange</b>\n"
            f"{symbol} {side.upper()}\n"
            f"SL de break-even NO aplicado: <code>{new_sl:.6f}</code>\n"
            f"Error: {e}\n"
            f"Revisar posición manualmente."
        )


def _update_trailing(symbol: str, pos: dict, current_price: float) -> None:
    """v8: _round_price en new_sl + debounce de notificaciones (5 min/par)."""
    if time.time() - pos.get("open_ts", 0) < MIN_HOLD_SECS:
        return

    side       = pos["side"]
    trail_step = pos.get("trail_step", 0)
    if trail_step <= 0:
        return
    if pos.get("sl") is None or pos.get("tp") is None:
        return

    if side == "long":
        peak = pos.get("trail_high", pos["entry"])
        if current_price > peak + trail_step:
            pos["trail_high"] = current_price
            coin   = exchange._hl_symbol(symbol)
            new_sl = exchange._round_price(coin, current_price - 1.5 * trail_step)
            if new_sl > pos["sl"]:
                log.info("[%s] Trailing SL: %.6f → %.6f", symbol, pos["sl"], new_sl)
                pos["sl"] = new_sl
                try:
                    exchange.modify_sltp_orders(symbol, "long", pos["qty"], new_sl, pos["tp"])
                except Exception as e:
                    log.warning("[%s] Error actualizando trailing SL: %s", symbol, e)
                    return
                now = time.time()
                if now - _trailing_notify_ts.get(symbol, 0) >= TRAILING_NOTIFY_DEBOUNCE:
                    _trailing_notify_ts[symbol] = now
                    telegram.notify_trailing(
                        symbol=symbol, side="long",
                        entry=pos["entry"], current_price=current_price,
                        new_sl=new_sl, tp=pos["tp"],
                    )
    else:
        trough = pos.get("trail_low", pos["entry"])
        if current_price < trough - trail_step:
            pos["trail_low"] = current_price
            coin   = exchange._hl_symbol(symbol)
            new_sl = exchange._round_price(coin, current_price + 1.5 * trail_step)
            if new_sl < pos["sl"]:
                log.info("[%s] Trailing SL: %.6f → %.6f", symbol, pos["sl"], new_sl)
                pos["sl"] = new_sl
                try:
                    exchange.modify_sltp_orders(symbol, "short", pos["qty"], new_sl, pos["tp"])
                except Exception as e:
                    log.warning("[%s] Error actualizando trailing SL: %s", symbol, e)
                    return
                now = time.time()
                if now - _trailing_notify_ts.get(symbol, 0) >= TRAILING_NOTIFY_DEBOUNCE:
                    _trailing_notify_ts[symbol] = now
                    telegram.notify_trailing(
                        symbol=symbol, side="short",
                        entry=pos["entry"], current_price=current_price,
                        new_sl=new_sl, tp=pos["tp"],
                    )


def _check_tp_extension(
    symbol: str,
    pos: dict,
    current_price: float,
    feed,
    effective_min_score: int,
) -> None:
    """v8: fórmula de new_tp lineal (entry + tp_orig_dist × (n+1)) y _round_price."""
    if time.time() - pos.get("open_ts", 0) < MIN_HOLD_SECS:
        return

    extensions = pos.get("tp_extensions", 0)
    if extensions >= MAX_TP_EXTENSIONS:
        return

    tp   = pos.get("tp")
    side = pos["side"]
    if tp is None:
        return

    if side == "long":
        near_tp = current_price >= tp * (1 - TP_EXTEND_THRESH)
    else:
        near_tp = current_price <= tp * (1 + TP_EXTEND_THRESH)

    if not near_tp:
        return
    if pos.get("_extending"):
        return

    pos["_extending"] = True
    try:
        dist_pct = abs(current_price - tp) / tp
        log.info("[%s] Precio a %.2f%% del TP — evaluando extensión", symbol, dist_pct * 100)

        try:
            candles_15m = feed.get(symbol, "15m")
            candles_1h  = feed.get(symbol, "1h")
            candles_4h  = feed.get(symbol, "4h") if feed.has_tf(symbol, "4h") else None
            signal, score, _ = signals.evaluate(
                candles_15m, candles_1h, candles_4h,
                min_score=WEEKDAY_MIN_SCORE,
                symbol=symbol,
            )
        except Exception as e:
            log.warning("[%s] Error evaluando señal para extend_tp: %s", symbol, e)
            return

        if not signal or signal != side:
            log.info(
                "[%s] Señal no válida para extend_tp (signal=%s) — dejando TP actual",
                symbol, signal,
            )
            return

        try:
            pos_live = exchange.get_position(symbol)
        except Exception as e:
            log.warning("[%s] No se pudo verificar posición antes de extend_tp: %s", symbol, e)
            return

        if pos_live is None:
            log.info(
                "[%s] Posición ya cerrada — extend_tp cancelado (TP original ejecutado)",
                symbol,
            )
            return

        if pos_live.get("side") != side:
            log.warning(
                "[%s] Side en exchange (%s) difiere del local (%s) — extend_tp cancelado",
                symbol, pos_live.get("side"), side,
            )
            return

        entry        = pos["entry"]
        tp_orig      = pos.get("tp_original", tp)
        tp_orig_dist = abs(tp_orig - entry)

        n = extensions + 2
        coin = exchange._hl_symbol(symbol)
        if side == "long":
            new_tp = exchange._round_price(coin, entry + tp_orig_dist * n)
        else:
            new_tp = exchange._round_price(coin, entry - tp_orig_dist * n)

        max_tp_dist = tp_orig_dist * (MAX_TP_EXTENSIONS + 2) * 1.1
        if abs(new_tp - entry) > max_tp_dist:
            log.warning(
                "[%s] extend_tp cancelado — new_tp %.6f excede límite razonable desde entry %.6f",
                symbol, new_tp, entry,
            )
            return

        if side == "long" and new_tp <= current_price * 1.001:
            log.warning(
                "[%s] extend_tp cancelado — new_tp %.6f <= precio_actual %.6f",
                symbol, new_tp, current_price,
            )
            return
        if side == "short" and new_tp >= current_price * 0.999:
            log.warning(
                "[%s] extend_tp cancelado — new_tp %.6f >= precio_actual %.6f",
                symbol, new_tp, current_price,
            )
            return

        current_sl = pos["sl"]

        try:
            exchange.modify_sltp_orders(symbol, side, pos["qty"], current_sl, new_tp)
        except Exception as e:
            log.warning("[%s] Error colocando órdenes en extend_tp: %s", symbol, e)
            return

        old_tp = pos["tp"]
        pos["tp"]            = new_tp
        pos["tp_extensions"] = extensions + 1
        pos["trail_high"]    = current_price
        pos["trail_low"]     = current_price

        log.info(
            "[%s] TP extendido #%d | old_tp=%.6f → new_tp=%.6f | SL=%.6f | score=%d",
            symbol, extensions + 1, old_tp, new_tp, current_sl, score,
        )
        telegram.notify(
            f"\U0001f4c8 TP Extendido #{extensions + 1}\n"
            f"{symbol} {side.upper()}\n"
            f"TP anterior: <code>{old_tp:.6f}</code>\n"
            f"Nuevo TP: <code>{new_tp:.6f}</code> (+{abs(new_tp - entry)/entry*100:.2f}% desde entry)\n"
            f"SL sin cambios: <code>{current_sl:.6f}</code>\n"
            f"Score: {score}"
        )

    finally:
        pos["_extending"] = False
