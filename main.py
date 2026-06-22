"""main.py — Loop principal con trailing stop y TP dinámico (extend_tp)."""
import logging
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

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("main")

_cooldown: dict[str, float] = {}
_manual_alert_cooldown: dict[str, float] = {}

COOLDOWN               = 60 * 60
MANUAL_ALERT_COOLDOWN  = 60 * 60
MAX_TP_EXTENSIONS      = 3
TP_EXTEND_RR           = 1.5
TP_EXTEND_THRESH       = 0.015

# FIX 3: guard temporal — ni extend_tp ni trailing actúan en los primeros 5 min
MIN_HOLD_SECS = 300

READY_TIMEOUT = 120
READY_MIN_PCT = 0.80

WEEKDAY_MIN_SCORE = 70
WEEKEND_MIN_SCORE = 90

_weekend_notified_day: int = -1


def _is_weekend() -> bool:
    return datetime.now(timezone.utc).weekday() >= 5


def _update_trailing(symbol: str, pos: dict, current_price: float) -> None:
    # FIX 3: no trailing en los primeros MIN_HOLD_SECS
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
            new_sl = round(current_price - 1.5 * trail_step, 6)
            if new_sl > pos["sl"]:
                log.info("[%s] Trailing SL: %.4f → %.4f", symbol, pos["sl"], new_sl)
                pos["trail_high"] = current_price
                pos["sl"]         = new_sl
                try:
                    exchange.cancel_all_orders(symbol)
                    exchange.place_stop_order(symbol, "long", pos["qty"], new_sl)
                    exchange.place_tp_order(symbol, "long", pos["qty"], pos["tp"])
                    telegram.notify(f"🔼 Trailing SL movido\n{symbol} LONG\nNuevo SL: <code>{new_sl:.4f}</code>")
                except Exception as e:
                    log.warning("[%s] Error actualizando trailing SL: %s", symbol, e)
    else:
        trough = pos.get("trail_low", pos["entry"])
        if current_price < trough - trail_step:
            new_sl = round(current_price + 1.5 * trail_step, 6)
            if new_sl < pos["sl"]:
                log.info("[%s] Trailing SL: %.4f → %.4f", symbol, pos["sl"], new_sl)
                pos["trail_low"] = current_price
                pos["sl"]        = new_sl
                try:
                    exchange.cancel_all_orders(symbol)
                    exchange.place_stop_order(symbol, "short", pos["qty"], new_sl)
                    exchange.place_tp_order(symbol, "short", pos["qty"], pos["tp"])
                    telegram.notify(f"🔽 Trailing SL movido\n{symbol} SHORT\nNuevo SL: <code>{new_sl:.4f}</code>")
                except Exception as e:
                    log.warning("[%s] Error actualizando trailing SL: %s", symbol, e)


def _check_tp_extension(
    symbol: str,
    pos: dict,
    current_price: float,
    feed,
    effective_min_score: int,
) -> None:
    # FIX 3: no extend_tp en los primeros MIN_HOLD_SECS
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

    dist_pct = abs(current_price - tp) / tp
    log.info("[%s] Precio a %.2f%% del TP — evaluando extensión", symbol, dist_pct * 100)

    try:
        candles_15m = feed.get(symbol, "15m")
        candles_1h  = feed.get(symbol, "1h")
        candles_4h  = feed.get(symbol, "4h") if feed.has_tf(symbol, "4h") else None
        signal, score = signals.evaluate(
            candles_15m, candles_1h, candles_4h,
            min_score=effective_min_score,
        )
    except Exception as e:
        log.warning("[%s] Error evaluando señal para extend_tp: %s", symbol, e)
        pos["_extending"] = False
        return

    if not signal or signal != side:
        log.info("[%s] Señal no válida para extend_tp (signal=%s) — dejando TP actual", symbol, signal)
        pos["_extending"] = False
        return

    entry        = pos["entry"]
    tp_orig_dist = abs(pos.get("tp_original", tp) - entry)

    if side == "long":
        new_tp = round(entry + tp_orig_dist * TP_EXTEND_RR * (extensions + 2), 6)
    else:
        new_tp = round(entry - tp_orig_dist * TP_EXTEND_RR * (extensions + 2), 6)

    # FIX 1: extend_tp NO mueve el SL. El SL actual se mantiene intacto.
    # El SL solo lo mueve _update_trailing cuando el precio lo justifica.
    current_sl = pos["sl"]

    try:
        exchange.cancel_all_orders(symbol)
        exchange.place_stop_order(symbol, side, pos["qty"], current_sl)
        exchange.place_tp_order(symbol, side, pos["qty"], new_tp)
    except Exception as e:
        log.warning("[%s] Error colocando órdenes en extend_tp: %s", symbol, e)
        pos["_extending"] = False
        return

    old_tp = pos["tp"]
    pos["tp"]            = new_tp
    pos["tp_extensions"] = extensions + 1
    pos["trail_high"]    = current_price
    pos["trail_low"]     = current_price
    pos["_extending"]    = False

    log.info(
        "[%s] TP extendido #%d | old_tp=%.6f → new_tp=%.6f | SL sin cambios=%.6f | score=%d",
        symbol, extensions + 1, old_tp, new_tp, current_sl, score,
    )
    telegram.notify(
        f"📈 TP Extendido #{extensions + 1}\n"
        f"{symbol} {side.upper()}\n"
        f"TP anterior: <code>{old_tp:.6f}</code>\n"
        f"Nuevo TP: <code>{new_tp:.6f}</code>\n"
        f"SL sin cambios: <code>{current_sl:.6f}</code>\n"
        f"Score: {score}"
    )


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
    # FIX 2: lógica unificada sin bloque duplicado
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

    # Cierre externo (manual o liquidación) — usamos precio actual, razón desconocida
    return current_price, "MANUAL"


def run() -> None:
    global _weekend_notified_day

    log.info(
        "Bot iniciado | %d pares | lev=%dx | margin=%s USDT | max=%d posiciones",
        len(config.SYMBOLS), config.LEVERAGE, config.MARGIN_USDT, config.MAX_POSITIONS,
    )

    for symbol in config.SYMBOLS:
        try:
            exchange.set_leverage(symbol)
        except Exception as e:
            log.warning("No se pudo setear leverage en %s: %s", symbol, e)

    telegram.notify(
        f"🤖 Bot iniciado — {len(config.SYMBOLS)} pares | "
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

            weekend = _is_weekend()
            effective_min_score = WEEKEND_MIN_SCORE if weekend else WEEKDAY_MIN_SCORE

            all_ex_positions = exchange.get_all_positions()

            # ── Sync posiciones abiertas ────────────────────────────────────────
            for symbol in list(positions.keys()):
                pos_ex = all_ex_positions.get(symbol)
                if not pos_ex:
                    p             = positions.pop(symbol)
                    current_price = exchange.get_price(symbol)
                    exit_price, reason = _exit_price_for(p, current_price)

                    pnl_pct = (
                        (exit_price - p["entry"]) / p["entry"]
                        if p["side"] == "long"
                        else (p["entry"] - exit_price) / p["entry"]
                    ) * config.LEVERAGE * 100
                    pnl_usdt = (pnl_pct / 100) * (p["qty"] * p["entry"] / config.LEVERAGE)

                    _cooldown[symbol] = time.time()
                    log.info("[%s] Cooldown activado (%dm) tras %s", symbol, COOLDOWN // 60, reason)

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
                        symbol   = symbol,
                        side     = p["side"],
                        entry    = p["entry"],
                        exit_p   = exit_price,
                        pnl_pct  = pnl_pct,
                        pnl_usdt = pnl_usdt,
                        reason   = reason,
                        open_ts  = p.get("open_ts", 0.0),
                    )
                    log.info("[%s] Cerrada | %s | PnL=%+.2f%% (%+.4f USDT) | ext=%d",
                             symbol, reason, pnl_pct, pnl_usdt, p.get("tp_extensions", 0))

            # ── Purgar cooldowns expirados ──────────────────────────────────────
            expired = [sym for sym, ts in _cooldown.items() if time.time() - ts >= COOLDOWN]
            for sym in expired:
                _cooldown.pop(sym, None)
                log.info("[%s] Cooldown expirado — símbolo disponible", sym)

            expired_alerts = [sym for sym, ts in _manual_alert_cooldown.items()
                              if time.time() - ts >= MANUAL_ALERT_COOLDOWN]
            for sym in expired_alerts:
                _manual_alert_cooldown.pop(sym, None)

            for symbol, pos_ex in all_ex_positions.items():
                if symbol not in positions:
                    positions[symbol] = {
                        "side":          pos_ex["side"],
                        "entry":         pos_ex["entry"],
                        "qty":           pos_ex["size"],
                        "sl":            pos_ex["sl"],
                        "tp":            pos_ex["tp"],
                        "tp_original":   pos_ex["tp"],
                        "tp_extensions": 0,
                        "_extending":    False,
                        "trail_step":    0,
                        "score":         70,
                        "open_ts":       time.time(),
                    }
                    log.info("[%s] Sincronizada: %s @ %.4f (sl=%s tp=%s)",
                             symbol, pos_ex["side"], pos_ex["entry"],
                             pos_ex["sl"], pos_ex["tp"])

            open_count = len(positions)

            if loop_count % 10 == 1:
                log.info("[loop #%d] Posiciones: %d/%d | Feed: %d/%d pares listos | Cooldowns: %d | Pausado: %s",
                         loop_count, open_count, config.MAX_POSITIONS,
                         feed.ready_count(), len(config.SYMBOLS), len(_cooldown),
                         bot_state.is_paused())

            # ── Trailing stop + extend TP proactivo ────────────────────────────
            for symbol, pos in list(positions.items()):
                try:
                    price = exchange.get_price(symbol)
                    _update_trailing(symbol, pos, price)
                    _check_tp_extension(symbol, pos, price, feed, effective_min_score)
                except Exception as e:
                    log.warning("[%s] Error gestión posición: %s", symbol, e)

            # ── Filtro fin de semana ────────────────────────────────────────────
            if weekend:
                today = datetime.now(timezone.utc).weekday()
                if today != _weekend_notified_day:
                    _weekend_notified_day = today
                    day_name = "Sábado" if today == 5 else "Domingo"
                    log.info("Modo fin de semana activo (%s UTC) — score mínimo %d para nuevas entradas",
                             day_name, WEEKEND_MIN_SCORE)
                    telegram.notify(
                        f"🚫 Modo fin de semana ({day_name})\n"
                        f"No se abrirán posiciones nuevas salvo score ≥ {WEEKEND_MIN_SCORE}.\n"
                        f"Posiciones actuales siguen gestionándose con normalidad."
                    )

            # ── Buscar señales ──────────────────────────────────────────────────
            if bot_state.is_paused():
                log.debug("Bot pausado — saltando búsqueda de señales")
            else:
                # FIX 4: log de diagnóstico de régimen cada 10 loops
                if loop_count % 10 == 1:
                    regime_summary = []
                    for sym in config.SYMBOLS:
                        if not feed.ready(sym):
                            continue
                        try:
                            c1h = feed.get(sym, "1h")
                            reg, adx = signals._regime_confirmed(c1h)
                            regime_summary.append(f"{sym.split('-')[0]}:{reg[0].upper()}{adx:.0f}")
                        except Exception:
                            pass
                    if regime_summary:
                        log.info("[regímenes] %s", "  ".join(regime_summary))

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

                    if not is_manual and open_count >= config.MAX_POSITIONS:
                        break

                    try:
                        candles_15m = feed.get(symbol, "15m")
                        candles_1h  = feed.get(symbol, "1h")
                        candles_4h  = feed.get(symbol, "4h") if feed.has_tf(symbol, "4h") else None

                        signal, score = signals.evaluate(
                            candles_15m, candles_1h, candles_4h,
                            min_score=effective_min_score,
                        )
                        if not signal:
                            continue

                        try:
                            regime, _ = signals._market_regime(candles_1h)
                        except Exception:
                            regime = "bull"

                        price  = exchange.get_price(symbol)
                        params = risk.calc(
                            signal, price, candles_15m,
                            score=score, symbol=symbol, regime=regime,
                        )

                        # ── Modo alerta manual ──────────────────────────────
                        if is_manual:
                            _manual_alert_cooldown[symbol] = time.time()
                            side_icon = "🟡" if signal == "long" else "🔴"
                            telegram.notify(
                                f"🚨 <b>Alerta manual — {symbol}</b>\n\n"
                                f"{side_icon} Dirección: <b>{signal.upper()}</b>\n"
                                f"Precio actual: <code>{price:.2f}</code>\n"
                                f"SL sugerido:   <code>{params['sl']:.2f}</code>\n"
                                f"TP sugerido:   <code>{params['tp']:.2f}</code>\n"
                                f"Score: <b>{score}</b>\n\n"
                                f"⚠️ <i>Operación NO abierta automáticamente. Ábrela tú si lo consideras.</i>"
                            )
                            log.info("[%s] ALERTA MANUAL enviada | %s score=%d", symbol, signal.upper(), score)
                            continue

                        # ── Modo automático ───────────────────────────────────
                        log.info(
                            "[%s] SEÑAL %s | regime=%s RR=%.1f | "
                            "entry=%.6f sl=%.6f tp=%.6f qty=%.8f score=%d",
                            symbol, signal.upper(), regime, params["tp_rr"],
                            price, params["sl"], params["tp"], params["qty"], score,
                        )

                        exchange.open_order(
                            side   = signal,
                            qty    = params["qty"],
                            sl     = params["sl"],
                            tp     = params["tp"],
                            symbol = symbol,
                        )

                        positions[symbol] = {
                            "side":          signal,
                            "entry":         price,
                            "qty":           params["qty"],
                            "sl":            params["sl"],
                            "tp":            params["tp"],
                            "tp_original":   params["tp"],
                            "tp_extensions": 0,
                            "_extending":    False,
                            "trail_step":    params["trail_step"],
                            "trail_high":    price,
                            "trail_low":     price,
                            "score":         score,
                            "open_ts":       time.time(),
                        }
                        open_count += 1

                        telegram.notify_open(
                            symbol = symbol,
                            price  = price,
                            side   = signal,
                            qty    = params["qty"],
                            sl     = params["sl"],
                            tp     = params["tp"],
                            score  = score,
                            tp_rr  = params["tp_rr"],
                        )

                    except Exception as e:
                        log.error("[%s] Error: %s", symbol, e, exc_info=True)

        except Exception as e:
            log.error("Error en loop: %s", e, exc_info=True)
            telegram.notify(f"⚠️ Error en bot: {e}")

        time.sleep(config.LOOP_SLEEP)


if __name__ == "__main__":
    run()
