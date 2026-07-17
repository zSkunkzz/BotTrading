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
  - _restore_sl_tp_on_sync: usa _place_sl_tp_bulk en lugar de place_stop_order +
    place_tp_order separados, igual que open_order. Evita el fallo silencioso donde
    place_stop_order tenía éxito pero place_tp_order fallaba y quedaba sin TP.
  - _restore_sl_tp_on_sync: antes de recalcular, consulta get_open_trigger_orders
    para ver si HL ya tiene SL y/o TP activos. Si ambos existen, los sincroniza
    localmente en lugar de cancelar y volver a colocar innecesariamente.
    Esto cubre el caso de reinicio donde el bot no tenía estado local pero HL
    sí tenía las trigger orders en pie (posición abierta con TPSL desde sesión anterior).
v12:
  - _update_trailing, _apply_breakeven, _check_tp_extension: cambiado cancel_all_orders
    por cancel_trigger_orders. cancel_all_orders borraba TODAS las órdenes del par,
    incluyendo SL/TP manuales colocados desde la UI de HL. cancel_trigger_orders solo
    cancela las trigger orders (SL/TP) del bot, dejando intactas las órdenes manuales.
  - _restore_sl_tp_on_sync: ídem, cancel_all_orders → cancel_trigger_orders.
v13:
  - get_all_positions: capturado con try/except propio dentro del loop.
    Un 429 o error puntual ya no propaga la excepción al run() ni crashea el proceso;
    se loguea como warning, se duerme 10s adicionales y se hace continue.
  - open_order (price snapshot): una sola llamada a _market_price usada tanto para
    el check de min_notional como para el precio de la orden, eliminando la race
    condition de precio entre las dos llamadas anteriores.
v14 (calidad de señal + fixes):
  - Pasar `coin` a signals.evaluate en bucle automático y _check_tp_extension.
  - _check_tp_extension usa effective_min_score en lugar de WEEKDAY_MIN_SCORE.
  - _check_tp_extension añade condiciones de calidad: score+5, no proto, contexto>=0.
  - Clasificación de TP/SL en _get_real_exit_price movida a exchange.py.
"""
import logging
import os
import sys
import time
from datetime import datetime, timezone

import bot_state
import config
import exchange
import market_context
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

# v13: sleep extra tras error en get_all_positions (ej. 429)
_GET_POSITIONS_ERROR_SLEEP = 10


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
        # v12: cancel_trigger_orders en lugar de cancel_all_orders
        # para no borrar SL/TP manuales colocados desde la UI de HL
        exchange.cancel_trigger_orders(symbol)
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
                    # v12: cancel_trigger_orders en lugar de cancel_all_orders
                    exchange.cancel_trigger_orders(symbol)
                    exchange.place_stop_order(symbol, "long", pos["qty"], new_sl)
                    exchange.place_tp_order(symbol, "long", pos["qty"], pos["tp"])
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
                    # v12: cancel_trigger_orders en lugar de cancel_all_orders
                    exchange.cancel_trigger_orders(symbol)
                    exchange.place_stop_order(symbol, "short", pos["qty"], new_sl)
                    exchange.place_tp_order(symbol, "short", pos["qty"], pos["tp"])
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


def _price_change_1h(candles_1h: list[dict]) -> float:
    """Cambio porcentual del precio en la última hora (copia de signals)."""
    closes = [c["close"] for c in candles_1h]
    if len(closes) < 3:
        return 0.0
    prev = closes[-3]
    curr = closes[-2]
    if prev <= 0:
        return 0.0
    return (curr - prev) / prev


def _check_tp_extension(
    symbol: str,
    pos: dict,
    current_price: float,
    feed,
    effective_min_score: int,
) -> None:
    """v8: fórmula lineal + _round_price. v14: condiciones de calidad."""
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
            coin = exchange._hl_symbol(symbol)
            signal, score, regime = signals.evaluate(
                candles_15m, candles_1h, candles_4h,
                min_score=effective_min_score,  # usa el mismo umbral que para abrir
                symbol=symbol,
                coin=coin,
            )
        except Exception as e:
            log.warning("[%s] Error evaluando señal para extend_tp: %s", symbol, e)
            return

        # ── CONDICIONES DE CALIDAD PARA EXTENDER (v14) ──────────────────────
        # 1. Score mínimo más exigente: +5 puntos
        min_extend_score = effective_min_score + 5
        if score < min_extend_score:
            log.info(
                "[%s] TP extension: score %d < %d (mínimo extend) — no extender",
                symbol, score, min_extend_score
            )
            return

        # 2. No extender en régimen proto (solo si está confirmado)
        if regime and "proto" in regime:
            log.info(
                "[%s] TP extension: régimen %s es proto — no extender (esperar confirmación)",
                symbol, regime
            )
            return

        # 3. El contexto de mercado (funding + OI) debe ser positivo para la extensión
        if coin is not None:
            price_chg_1h = _price_change_1h(candles_1h)
            ctx_mod = market_context.score_context(coin, side, price_chg_1h)
            if ctx_mod < 0:
                log.info(
                    "[%s] TP extension: contexto de mercado negativo (%d) — no extender",
                    symbol, ctx_mod
                )
                return
        # ────────────────────────────────────────────────────────────────────────

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
            # v12: cancel_trigger_orders en lugar de cancel_all_orders
            exchange.cancel_trigger_orders(symbol)
            exchange.place_stop_order(symbol, side, pos["qty"], current_sl)
            exchange.place_tp_order(symbol, side, pos["qty"], new_tp)
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


def _wait_feed_ready(feed: KlineFeed) -> None:
    total    = len(config.SYMBOLS)
    needed   = max(1, int(total * READY_MIN_PCT))
    deadline = time.time() + READY_TIMEOUT
    last_log = 0
    while time.time() < deadline:
        ready = feed.ready_count()
        now   = time.time()
        if now - last_log >= 10:
            log.info("Feed: %d/%d pares listos (mínimo %d)", ready, total, needed)
            last_log = now
        if ready >= needed:
            log.info("Feed listo — %d/%d pares con datos suficientes", ready, total)
            return
        time.sleep(2)
    log.warning("Timeout feed (%ds) — arrancando con %d/%d pares listos",
                READY_TIMEOUT, feed.ready_count(), total)


def _exit_price_for(pos: dict, current_price: float) -> tuple[float, str]:
    side = pos["side"]
    tp   = pos.get("tp")
    sl   = pos.get("sl")

    if tp is not None and sl is not None:
        if side == "long":
            if current_price >= tp * 0.995:
                return tp, "TP"
            if current_price <= sl * 1.005:
                return sl, "SL"
        else:
            if current_price <= tp * 1.005:
                return tp, "TP"
            if current_price >= sl * 0.995:
                return sl, "SL"
    elif tp is not None:
        hit_tp = (
            (side == "long"  and current_price >= tp * 0.995) or
            (side == "short" and current_price <= tp * 1.005)
        )
        if hit_tp:
            return tp, "TP"
    elif sl is not None:
        hit_sl = (
            (side == "long"  and current_price <= sl * 1.005) or
            (side == "short" and current_price >= sl * 0.995)
        )
        if hit_sl:
            return sl, "SL"

    entry = pos.get("entry", 0)
    if entry > 0:
        move_pct = (
            (current_price - entry) / entry if side == "long"
            else (entry - current_price) / entry
        )
        if move_pct > 0.015:
            return current_price, "TP"
        if move_pct < -0.008:
            return current_price, "SL"

    return current_price, "MANUAL"


def _get_real_exit_price(
    symbol: str,
    pos: dict,
    fallback: float,
    fallback_reason: str,
) -> tuple[float, str]:
    """Delega la clasificación a exchange.get_real_exit_classification."""
    return exchange.get_real_exit_classification(symbol, pos, fallback, fallback_reason)


def _calc_pnl(side: str, entry: float, exit_price: float, qty: float) -> tuple[float, float]:
    if side == "long":
        price_move = (exit_price - entry) / entry
    else:
        price_move = (entry - exit_price) / entry

    pnl_pct  = price_move * config.LEVERAGE * 100
    pnl_usdt = price_move * qty * entry
    return pnl_pct, pnl_usdt


def _declare_closed(symbol: str, p: dict, positions: dict) -> None:
    current_price          = exchange.get_price(symbol)
    exit_price_est, reason_est = _exit_price_for(p, current_price)
    exit_price, reason     = _get_real_exit_price(symbol, p, exit_price_est, reason_est)

    pnl_pct, pnl_usdt = _calc_pnl(p["side"], p["entry"], exit_price, p["qty"])

    log.info(
        "[%s] Cierre detectado | side=%s entry=%.6f exit=%.6f reason=%s "
        "pnl_pct=%+.2f%% pnl_usdt=%+.4f USDT",
        symbol, p["side"], p["entry"], exit_price, reason, pnl_pct, pnl_usdt,
    )

    if reason == "SL":
        hold_secs   = time.time() - p.get("open_ts", 0)
        trade_score = p.get("score", 0)
        if hold_secs <= SMART_COOLDOWN_FAST_WINDOW and trade_score >= SMART_COOLDOWN_HIGH_SCORE:
            _cooldown_reason[symbol] = "sl_fast"
            log.info(
                "[%s] Smart cooldown: SL rápido (hold=%.0fs score=%d) → cooldown %dmin",
                symbol, hold_secs, trade_score, COOLDOWN_SL_FAST // 60,
            )
        else:
            _cooldown_reason[symbol] = "sl"
    else:
        _cooldown_reason[symbol] = "tp"

    _cooldown[symbol] = time.time()
    cd_mins = _cooldown_for(symbol) // 60
    log.info("[%s] Cooldown %dm activado tras %s", symbol, cd_mins, reason)

    _missing_count.pop(symbol, None)
    _trailing_notify_ts.pop(symbol, None)

    limit_hit = bot_state.record_trade(pnl_usdt)
    daily_pnl = bot_state.get_daily_pnl()
    capital   = config.MARGIN_USDT * config.MAX_POSITIONS
    daily_pct = (daily_pnl / capital * 100) if capital else 0.0
    daily_max = float(getattr(config, "DAILY_MAX_LOSS_PCT", -3.0))

    log.info(
        "[drawdown] PnL acum. hoy: %+.2f USDT (%+.2f%% de ~%.0f USDT capital)",
        daily_pnl, daily_pct, capital,
    )

    if limit_hit:
        msg = (
            f"\U0001f6d1 <b>L\u00edmite de p\u00e9rdidas diario alcanzado</b>\n"
            f"PnL hoy: <code>{daily_pnl:+.2f} USDT</code> ({daily_pct:+.2f}%)\n"
            f"Umbral: {daily_max}% — bot pausado hasta las 00:00 UTC.\n"
            f"Las posiciones abiertas siguen gestionándose (trailing/TP)."
        )
        log.warning("[drawdown] %s", msg.replace("\n", " "))
        telegram.notify(msg)

    trade_logger.record(
        symbol     = symbol,
        side       = p["side"],
        entry      = p["entry"],
        exit_price = exit_price,
        pnl_pct    = pnl_pct,
        pnl_usdt   = pnl_usdt,
        score      = p.get("score", 0),
        reason     = reason,
        open_ts    = p.get("open_ts", time.time()),
    )

    telegram.notify_close(
        symbol    = symbol,
        side      = p["side"],
        entry     = p["entry"],
        exit_p    = exit_price,
        pnl_pct   = pnl_pct,
        pnl_usdt  = pnl_usdt,
        reason    = reason,
        open_ts   = p.get("open_ts", 0.0),
        daily_pnl = daily_pnl,
    )


def _calc_trail_step_from_atr(symbol: str, feed, sl: float | None, entry: float) -> float:
    try:
        candles_15m = feed.get(symbol, "15m")
        if candles_15m and len(candles_15m) >= 15:
            trs = []
            for i in range(1, len(candles_15m)):
                h  = candles_15m[i]["high"]
                l  = candles_15m[i]["low"]
                pc = candles_15m[i - 1]["close"]
                trs.append(max(h - l, abs(h - pc), abs(l - pc)))
            atr  = sum(trs[-14:]) / min(14, len(trs))
            step = round(max(0.3 * atr, atr * 0.05), 8)
            log.info("[%s] trail_step recalculado desde ATR tras sync: %.8f", symbol, step)
            return step
    except Exception as exc:
        log.debug("[%s] No se pudo recalcular trail_step desde ATR: %s", symbol, exc)

    if sl is not None and entry > 0:
        sl_dist = abs(entry - sl)
        if sl_dist > 0:
            step = round(0.3 * sl_dist, 8)
            log.info("[%s] trail_step recalculado desde SL-dist tras sync: %.8f", symbol, step)
            return step

    return 0.0


def _calc_be_levels_from_atr(
    symbol: str, feed, side: str, entry: float
) -> tuple[float | None, float | None]:
    try:
        candles_15m = feed.get(symbol, "15m")
        if candles_15m and len(candles_15m) >= 15:
            trs = []
            for i in range(1, len(candles_15m)):
                h  = candles_15m[i]["high"]
                l  = candles_15m[i]["low"]
                pc = candles_15m[i - 1]["close"]
                trs.append(max(h - l, abs(h - pc), abs(l - pc)))
            atr = sum(trs[-14:]) / min(14, len(trs))
            if atr > 0 and entry > 0:
                be_trigger = (entry + risk.BE_ATR_MULT * atr) if side == "long" else (entry - risk.BE_ATR_MULT * atr)
                be_sl      = (entry + risk.BE_BUFFER_MULT * atr) if side == "long" else (entry - risk.BE_BUFFER_MULT * atr)
                return round(be_trigger, 8), round(be_sl, 8)
    except Exception as exc:
        log.debug("[%s] No se pudo calcular be_levels tras sync: %s", symbol, exc)
    return None, None


def _get_position_open_ts(symbol: str, pos_ex: dict) -> float:
    try:
        side         = pos_ex.get("side", "long")
        hl_open_dir  = "Open Long" if side == "long" else "Open Short"
        closed = exchange.get_closed_orders(symbol, limit=20)
        for order in closed:
            if order.get("dir") != hl_open_dir:
                continue
            ts_ms = int(order.get("time") or 0)
            if ts_ms > 0:
                ts = ts_ms / 1000.0
                log.info("[%s] open_ts real recuperado desde historial: %.0f", symbol, ts)
                return ts
    except Exception as exc:
        log.debug("[%s] No se pudo recuperar open_ts real: %s — usando time.time()", symbol, exc)
    return time.time()


def _restore_sl_tp_on_sync(
    symbol: str,
    pos: dict,
    feed,
) -> None:
    """v11: Restaura SL/TP en HL tras reinicio del bot.

    Pasos:
    1. Consultar get_open_trigger_orders para ver si HL ya tiene SL y/o TP activos.
       - Si ambos existen: sincronizar localmente los valores y NO tocar el exchange.
       - Si falta uno o los dos: recalcular desde ATR y colocar los que falten.
    2. Usar _place_sl_tp_bulk para colocar ambos de una vez (más fiable que separados).
    """
    local_sl = pos.get("sl")
    local_tp = pos.get("tp")
    side     = pos["side"]
    entry    = pos.get("entry", 0.0)
    qty      = pos.get("qty", 0.0)

    if entry <= 0 or qty <= 0:
        log.warning(
            "[%s] _restore_sl_tp: entry=%.6f qty=%.8f inválidos — no se pueden recalcular",
            symbol, entry, qty,
        )
        return

    # ── Paso 1: consultar triggers reales en HL ──────────────────────────
    try:
        triggers = exchange.get_open_trigger_orders(symbol)
    except Exception as exc:
        log.warning("[%s] _restore_sl_tp: no se pudo consultar triggers en HL: %s", symbol, exc)
        triggers = []

    hl_sl_px = None
    hl_tp_px = None
    for t in triggers:
        ot = str(t.get("orderType", "")).lower()
        px = float(t.get("triggerPx") or 0)
        if px <= 0:
            continue
        if "stop" in ot:
            hl_sl_px = px
        elif "take profit" in ot or "tp" in ot:
            hl_tp_px = px

    # Si HL ya tiene ambos activos, solo sincronizar en local y terminar
    if hl_sl_px is not None and hl_tp_px is not None:
        if local_sl != hl_sl_px or local_tp != hl_tp_px:
            log.info(
                "[%s] _restore_sl_tp: HL ya tiene SL=%.6f TP=%.6f — sincronizando local sin tocar exchange",
                symbol, hl_sl_px, hl_tp_px,
            )
            pos["sl"]          = hl_sl_px
            pos["tp"]          = hl_tp_px
            pos["tp_original"] = pos.get("tp_original") or hl_tp_px
        else:
            log.debug("[%s] _restore_sl_tp: HL tiene SL+TP coincidentes con local — nada que hacer", symbol)
        return

    # Si falta alguno, necesitamos el feed para recalcular
    try:
        candles_15m = feed.get(symbol, "15m")
        candles_1h  = feed.get(symbol, "1h")
        if not candles_15m or not candles_1h:
            log.warning("[%s] _restore_sl_tp: feed no listo aún — reintentando en próximo loop", symbol)
            return
    except Exception as exc:
        log.warning("[%s] _restore_sl_tp: feed no disponible: %s", symbol, exc)
        return

    # Determinar qué falta: usar valores de HL si existen, recalcular los que faltan
    if hl_sl_px is not None or hl_tp_px is not None:
        # Parcialmente cubierto: recalcular solo para tener ambos valores
        log.info(
            "[%s] _restore_sl_tp: HL tiene SL=%s TP=%s (parcial) — recalculando ambos para consistencia",
            symbol,
            f"{hl_sl_px:.6f}" if hl_sl_px else "None",
            f"{hl_tp_px:.6f}" if hl_tp_px else "None",
        )

    try:
        params = risk.calc(
            side, entry, candles_15m,
            score=_SYNC_SCORE_UNKNOWN,
            symbol=symbol,
            regime=side,
            candles_1h=candles_1h,
        )
        new_sl = hl_sl_px if hl_sl_px is not None else params["sl"]
        new_tp = hl_tp_px if hl_tp_px is not None else params["tp"]

        # ── Paso 2: cancelar solo triggers del bot y colocar SL+TP en bulk ──
        # v12: cancel_trigger_orders en lugar de cancel_all_orders
        exchange.cancel_trigger_orders(symbol)
        exchange._place_sl_tp_bulk(symbol, side, qty, new_sl, new_tp)

        pos["sl"]          = new_sl
        pos["tp"]          = new_tp
        pos["tp_original"] = pos.get("tp_original") or new_tp
        if pos.get("be_trigger") is None:
            pos["be_trigger"] = params.get("be_trigger")
            pos["be_sl"]      = params.get("be_sl")
        if not pos.get("trail_step"):
            pos["trail_step"] = params.get("trail_step", 0.0)

        log.warning(
            "[%s] SL/TP restaurados tras sync | side=%s entry=%.6f sl=%.6f tp=%.6f",
            symbol, side, entry, new_sl, new_tp,
        )
        telegram.notify(
            f"\U0001f527 <b>SL/TP restaurados</b> (reinicio bot)\n"
            f"{symbol} {side.upper()}\n"
            f"Entry: <code>{entry:.6f}</code>\n"
            f"SL: <code>{new_sl:.6f}</code>\n"
            f"TP: <code>{new_tp:.6f}</code>"
        )
    except Exception as exc:
        log.error("[%s] _restore_sl_tp: fallo al recalcular/colocar SL/TP: %s", symbol, exc)
        telegram.notify(
            f"\u26a0\ufe0f <b>SL/TP NO restaurados</b>\n"
            f"{symbol} {side.upper()} — revisar manualmente.\n"
            f"Error: {exc}"
        )


def _open_position(
    symbol: str,
    signal: str,
    score: int,
    regime: str,
    price: float,
    candles_15m: list,
    candles_1h: list,
    positions: dict,
) -> None:
    # v10: lock en memoria — primera línea de defensa contra duplicados.
    # Impide que dos rutas (manual + automática, o dos evaluaciones solapadas)
    # lancen open_order para el mismo símbolo antes de que la primera
    # termine de registrar la posición en `positions`.
    if symbol in _opening:
        log.warning(
            "[%s] _open_position: ya hay una apertura en curso para este símbolo — abortando",
            symbol,
        )
        return
    _opening.add(symbol)

    try:
        # v9: guard de exchange — segunda línea de defensa (cubre reinicios).
        # El lock de arriba cubre el caso intra-proceso; este cubre el caso
        # en que el bot crasheó justo tras enviar la orden pero antes de
        # registrarla en `positions`.
        try:
            pos_already = exchange.get_position(symbol)
            if pos_already and pos_already.get("side") in VALID_SIDES:
                log.warning(
                    "[%s] Guard exchange: ya existe posición %s @ %.6f — abortando open_order "
                    "(race condition reinicio). Se registrará en el siguiente sync.",
                    symbol, pos_already["side"], pos_already.get("entry", 0),
                )
                telegram.notify(
                    f"\u26a0\ufe0f <b>Posición duplicada bloqueada</b>\n"
                    f"{symbol} — ya existe {pos_already['side'].upper()} en exchange.\n"
                    f"La apertura fue ignorada. Se sincronizará en el siguiente loop."
                )
                return
        except Exception as guard_exc:
            log.warning(
                "[%s] Guard exchange: no se pudo verificar posición previa (%s) — continuando apertura",
                symbol, guard_exc,
            )

        params = risk.calc(
            signal, price, candles_15m,
            score=score, symbol=symbol, regime=regime,
            candles_1h=candles_1h,
        )

        log.info(
            "[%s] SEÑAL %s | regime=%s RR=%.1f | "
            "entry=%.6f sl=%.6f tp=%.6f be_trigger=%s qty=%.8f score=%d",
            symbol, signal.upper(), regime, params["tp_rr"],
            price, params["sl"], params["tp"],
            f"{params['be_trigger']:.6f}" if params.get("be_trigger") else "N/A",
            params["qty"], score,
        )

        try:
            exchange.open_order(
                side   = signal,
                qty    = params["qty"],
                sl     = params["sl"],
                tp     = params["tp"],
                symbol = symbol,
            )
        except Exception as open_err:
            log.error(
                "[%s] open_order falló — posición NO registrada: %s",
                symbol, open_err,
            )
            try:
                pos_live = exchange.get_position(symbol)
                if pos_live:
                    exchange.close_position(
                        side   = pos_live["side"],
                        qty    = pos_live["size"],
                        symbol = symbol,
                    )
                    log.warning("[%s] Rollback OK: posición parcial cerrada", symbol)
                else:
                    log.info("[%s] Rollback innecesario — no hay posición en exchange", symbol)
            except Exception as close_err:
                log.error(
                    "[%s] Rollback fallido — revisar posición manualmente: %s",
                    symbol, close_err,
                )
            return

        real_entry = _sync_entry_from_exchange(symbol, price, signal)

        positions[symbol] = {
            "side":          signal,
            "entry":         real_entry,
            "qty":           params["qty"],
            "sl":            params["sl"],
            "tp":            params["tp"],
            "tp_original":   params["tp"],
            "tp_extensions": 0,
            "_extending":    False,
            "trail_step":    params["trail_step"],
            "trail_high":    real_entry,
            "trail_low":     real_entry,
            "score":         score,
            "open_ts":       time.time(),
            "be_trigger":    params.get("be_trigger"),
            "be_sl":         params.get("be_sl"),
            "be_locked":     False,
        }

        telegram.notify_open(
            symbol = symbol,
            price  = real_entry,
            side   = signal,
            qty    = params["qty"],
            sl     = params["sl"],
            tp     = params["tp"],
            score  = score,
            tp_rr  = params["tp_rr"],
            regime = regime,
        )

    finally:
        # Siempre liberar el lock, tanto si la apertura tuvo éxito como si no.
        _opening.discard(symbol)


def run() -> None:
    global _weekend_notified_day

    log.info(
        "Bot iniciado | %d pares | lev=%dx | margin=%s USDT | max=%d posiciones | max_same_side=%d",
        len(config.SYMBOLS), config.LEVERAGE, config.MARGIN_USDT,
        config.MAX_POSITIONS, MAX_SAME_SIDE,
    )

    for symbol in config.SYMBOLS:
        try:
            exchange.set_leverage(symbol)
        except Exception as e:
            log.warning("No se pudo setear leverage en %s: %s", symbol, e)

    telegram.notify(
        f"\U0001f916 Bot iniciado — {len(config.SYMBOLS)} pares | "
        f"{config.LEVERAGE}x | max {config.MAX_POSITIONS} posiciones"
    )

    feed = KlineFeed(config.SYMBOLS)
    feed.start()
    _wait_feed_ready(feed)

    positions: dict = {}
    tg_commands.start(get_positions_fn=lambda: positions, feed=feed)
    trade_logger.start_scheduler()

    loop_count = 0

    while True:
        try:
            loop_count += 1

            if bot_state.reset_daily_if_new_day():
                log.info("[drawdown] Nuevo día UTC — bot reactivado")
                telegram.notify("\U0001f305 Nuevo día UTC — límite de pérdidas reseteado. Bot activo.")

            weekend = _is_weekend()
            effective_min_score = WEEKEND_MIN_SCORE if weekend else WEEKDAY_MIN_SCORE

            # v13: get_all_positions con su propio try/except.
            # Un 429 o error HTTP puntual ya NO propaga la excepción al run()
            # ni crashea el proceso; se loguea como warning, se espera
            # _GET_POSITIONS_ERROR_SLEEP segundos adicionales y se hace
            # continue para reintentar en el siguiente loop.
            try:
                all_ex_positions = exchange.get_all_positions()
            except Exception as pos_err:
                log.warning(
                    "get_all_positions falló (loop #%d): %s — reintentando en %ds",
                    loop_count, pos_err, _GET_POSITIONS_ERROR_SLEEP,
                )
                time.sleep(_GET_POSITIONS_ERROR_SLEEP)
                continue

            for symbol in list(positions.keys()):
                pos_ex = all_ex_positions.get(symbol)
                if pos_ex:
                    _missing_count.pop(symbol, None)
                    continue

                _missing_count[symbol] = _missing_count.get(symbol, 0) + 1
                absent = _missing_count[symbol]

                if absent < CLOSE_CONFIRM_LOOPS:
                    log.debug(
                        "[%s] No vista en exchange (intento %d/%d) — esperando confirmación",
                        symbol, absent, CLOSE_CONFIRM_LOOPS,
                    )
                    continue

                p = positions.pop(symbol)
                _declare_closed(symbol, p, positions)

            expired = [
                sym for sym, ts in _cooldown.items()
                if time.time() - ts >= _cooldown_for(sym)
            ]
            for sym in expired:
                _cooldown.pop(sym, None)
                _cooldown_reason.pop(sym, None)
                log.info("[%s] Cooldown expirado — símbolo disponible", sym)

            expired_alerts = [
                sym for sym, ts in _manual_alert_cooldown.items()
                if time.time() - ts >= MANUAL_ALERT_COOLDOWN
            ]
            for sym in expired_alerts:
                _manual_alert_cooldown.pop(sym, None)

            for symbol, pos_ex in all_ex_positions.items():
                if symbol not in positions:
                    ex_side = pos_ex.get("side")
                    if ex_side not in VALID_SIDES:
                        log.warning(
                            "[%s] Posición ignorada en sync — side inválido del exchange: %r",
                            symbol, ex_side,
                        )
                        continue

                    synced_sl    = pos_ex.get("sl")
                    synced_entry = pos_ex["entry"]
                    trail_step   = _calc_trail_step_from_atr(symbol, feed, synced_sl, synced_entry)
                    real_open_ts = _get_position_open_ts(symbol, pos_ex)
                    be_trigger, be_sl = _calc_be_levels_from_atr(symbol, feed, ex_side, synced_entry)

                    positions[symbol] = {
                        "side":          ex_side,
                        "entry":         synced_entry,
                        "qty":           pos_ex["size"],
                        "sl":            synced_sl,
                        "tp":            pos_ex.get("tp"),
                        "tp_original":   pos_ex.get("tp"),
                        "tp_extensions": 0,
                        "_extending":    False,
                        "trail_step":    trail_step,
                        "trail_high":    synced_entry,
                        "trail_low":     synced_entry,
                        "score":         _SYNC_SCORE_UNKNOWN,
                        "open_ts":       real_open_ts,
                        "be_trigger":    be_trigger,
                        "be_sl":         be_sl,
                        "be_locked":     False,
                    }
                    log.info(
                        "[%s] Sincronizada: %s @ %.6f (sl=%s tp=%s trail=%.8f be_trigger=%s open_ts=%.0f score=sync)",
                        symbol, ex_side, synced_entry,
                        synced_sl, pos_ex.get("tp"), trail_step,
                        f"{be_trigger:.6f}" if be_trigger else "N/A",
                        real_open_ts,
                    )

                    _restore_sl_tp_on_sync(symbol, positions[symbol], feed)

            open_count = len(positions)

            if loop_count % 10 == 1:
                daily_pnl  = bot_state.get_daily_pnl()
                paused_str = (
                    "PAUSADO(drawdown)" if bot_state.is_daily_limit_hit()
                    else ("PAUSADO" if bot_state.is_paused() else "activo")
                )
                long_count  = sum(1 for p in positions.values() if p["side"] == "long")
                short_count = sum(1 for p in positions.values() if p["side"] == "short")
                log.info(
                    "[loop #%d] Posiciones: %d/%d (L:%d S:%d) | Feed: %d/%d | "
                    "Cooldowns: %d | Estado: %s | PnL hoy: %+.2f USDT",
                    loop_count, open_count, config.MAX_POSITIONS,
                    long_count, short_count,
                    feed.ready_count(), len(config.SYMBOLS), len(_cooldown),
                    paused_str, daily_pnl,
                )

            for symbol, pos in list(positions.items()):
                try:
                    price = exchange.get_price(symbol)
                    _apply_breakeven(symbol, pos, price)
                    _update_trailing(symbol, pos, price)
                    _check_tp_extension(symbol, pos, price, feed, effective_min_score)
                except Exception as e:
                    log.warning("[%s] Error gestión posición: %s", symbol, e)

            if weekend:
                today = datetime.now(timezone.utc).weekday()
                if today != _weekend_notified_day:
                    _weekend_notified_day = today
                    day_name = "Sábado" if today == 5 else "Domingo"
                    log.info("Modo fin de semana activo (%s UTC) — score mínimo %d",
                             day_name, WEEKEND_MIN_SCORE)
                    telegram.notify(
                        f"\U0001f6ab Modo fin de semana ({day_name})\n"
                        f"No se abrirán posiciones nuevas salvo score \u2265 {WEEKEND_MIN_SCORE}.\n"
                        f"Posiciones actuales siguen gestionándose con normalidad."
                    )

            if bot_state.is_paused() or bot_state.is_daily_limit_hit():
                if bot_state.is_daily_limit_hit():
                    log.debug("Bot pausado por drawdown diario — saltando búsqueda de señales")
                else:
                    log.debug("Bot pausado manualmente — saltando búsqueda de señales")
            else:
                if loop_count % 10 == 1:
                    regime_summary = []
                    for sym in config.SYMBOLS:
                        if not feed.ready(sym):
                            continue
                        try:
                            c1h = feed.get(sym, "1h")
                            reg, adx = signals._market_regime(c1h)
                            regime_summary.append(f"{sym.split('-')[0]}:{reg[0].upper()}{adx:.0f}")
                        except Exception:
                            pass
                    if regime_summary:
                        log.info("[regímenes] %s", "\t".join(regime_summary))

                for symbol in list(config.SYMBOLS):
                    manual_side = tg_commands.pop_manual_signal(symbol)
                    if not manual_side:
                        continue
                    if symbol in positions:
                        telegram.notify(
                            f"\u26a0\ufe0f Señal manual {symbol} {manual_side.upper()} ignorada "
                            f"— ya hay posición abierta."
                        )
                        continue
                    if symbol in _cooldown:
                        telegram.notify(
                            f"\u26a0\ufe0f Señal manual {symbol} {manual_side.upper()} ignorada "
                            f"— símbolo en cooldown."
                        )
                        continue
                    if len(positions) >= config.MAX_POSITIONS:
                        telegram.notify(
                            f"\u26a0\ufe0f Señal manual {symbol} {manual_side.upper()} ignorada "
                            f"— posiciones máximas alcanzadas ({config.MAX_POSITIONS})."
                        )
                        continue
                    if not feed.ready(symbol):
                        telegram.notify(
                            f"\u26a0\ufe0f Señal manual {symbol} {manual_side.upper()} ignorada "
                            f"— feed no listo para este par."
                        )
                        continue
                    if not _check_directional_guard(manual_side, positions, symbol):
                        telegram.notify(
                            f"\u26a0\ufe0f Señal manual {symbol} {manual_side.upper()} ignorada "
                            f"— MAX_SAME_SIDE o grupo correlación alcanzado."
                        )
                        continue
                    try:
                        candles_15m = feed.get(symbol, "15m")
                        candles_1h  = feed.get(symbol, "1h")
                        price       = exchange.get_price(symbol)
                        regime = "bull" if manual_side == "long" else "bear"
                        log.info(
                            "[%s] SEÑAL MANUAL %s vía Telegram — ejecutando",
                            symbol, manual_side.upper(),
                        )
                        _open_position(
                            symbol, manual_side, score=100, regime=regime,
                            price=price, candles_15m=candles_15m,
                            candles_1h=candles_1h, positions=positions,
                        )
                    except Exception as e:
                        log.error("[%s] Error ejecutando señal manual: %s", symbol, e)
                        telegram.notify(
                            f"\u274c Error ejecutando señal manual {symbol} {manual_side.upper()}: {e}"
                        )

                for symbol in config.SYMBOLS:
                    if symbol in positions:
                        continue
                    if not feed.ready(symbol):
                        continue
                    if symbol in _cooldown:
                        continue

                    is_manual = symbol in config.MANUAL_ALERT_SYMBOLS

                    if is_manual and symbol in _manual_alert_cooldown:
                        continue

                    if not is_manual and len(positions) >= config.MAX_POSITIONS:
                        break

                    try:
                        candles_15m = feed.get(symbol, "15m")
                        candles_1h  = feed.get(symbol, "1h")
                        candles_4h  = feed.get(symbol, "4h") if feed.has_tf(symbol, "4h") else None

                        # v14: pasar coin para que el contexto de mercado se aplique
                        coin = exchange._hl_symbol(symbol)
                        signal, score, regime = signals.evaluate(
                            candles_15m, candles_1h, candles_4h,
                            min_score=effective_min_score,
                            symbol=symbol,
                            coin=coin,
                        )

                        if not signal or signal not in VALID_SIDES:
                            if signal is not None:
                                log.warning(
                                    "[%s] Señal ignorada — side inválido: %r (score=%d)",
                                    symbol, signal, score,
                                )
                            continue

                        if not regime:
                            regime = "bull" if signal == "long" else "bear"

                        if not is_manual and not _check_directional_guard(signal, positions, symbol):
                            continue

                        price  = exchange.get_price(symbol)

                        if is_manual:
                            _manual_alert_cooldown[symbol] = time.time()
                            params = risk.calc(
                                signal, price, candles_15m,
                                score=score, symbol=symbol, regime=regime,
                                candles_1h=candles_1h,
                            )
                            side_icon = "\U0001f7e1" if signal == "long" else "\U0001f534"
                            telegram.notify(
                                f"\U0001f6a8 <b>Alerta manual — {symbol}</b>\n\n"
                                f"{side_icon} Dirección: <b>{signal.upper()}</b>\n"
                                f"Precio actual: <code>{price:.6f}</code>\n"
                                f"SL sugerido:   <code>{params['sl']:.6f}</code>\n"
                                f"TP sugerido:   <code>{params['tp']:.6f}</code>\n"
                                f"Score: <b>{score}</b>\n\n"
                                f"\u26a0\ufe0f <i>Operación NO abierta automáticamente.</i>"
                            )
                            log.info("[%s] ALERTA MANUAL enviada | %s score=%d", symbol, signal.upper(), score)
                            continue

                        _open_position(
                            symbol, signal, score=score, regime=regime,
                            price=price, candles_15m=candles_15m,
                            candles_1h=candles_1h, positions=positions,
                        )

                    except Exception as e:
                        log.error("[%s] Error: %s", symbol, e, exc_info=True)

        except Exception as e:
            log.error("Error en loop: %s", e, exc_info=True)
            telegram.notify(f"\u26a0\ufe0f Error en bot: {e}")

        time.sleep(config.LOOP_SLEEP)


if __name__ == "__main__":
    run()