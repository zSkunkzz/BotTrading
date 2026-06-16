"""main.py — Loop principal con trailing stop y TP dinámico (extend_tp)."""
import logging
import sys
import time
from datetime import datetime, timezone

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

# _cooldown guarda el timestamp del último cierre por SL por símbolo.
# Tras un TP con señal inválida también se activa.
_cooldown: dict[str, float] = {}

COOLDOWN          = 60 * 60   # 1 hora tras cierre por SL o TP sin señal
MAX_TP_EXTENSIONS = 3         # máximo de veces que se puede estirar el TP
TP_EXTEND_RR      = 1.5       # multiplicador de distancia entry→TP para cada extensión

READY_TIMEOUT = 120
READY_MIN_PCT = 0.80

# Score mínimo según sesión
WEEKDAY_MIN_SCORE = 70   # lunes–viernes
WEEKEND_MIN_SCORE = 90   # sábado y domingo

# Aviso de fin de semana: sólo notifica 1 vez por jornada
_weekend_notified_day: int = -1


def _is_weekend() -> bool:
    """Devuelve True si es sábado (5) o domingo (6) en UTC."""
    return datetime.now(timezone.utc).weekday() >= 5


def _update_trailing(symbol: str, pos: dict, current_price: float) -> None:
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


def _try_extend_tp(
    symbol: str,
    pos: dict,
    current_price: float,
    feed,
    effective_min_score: int,
) -> bool:
    """Intenta estirar el TP en lugar de cerrar la posición.

    Devuelve True si se extendió el TP (la posición NO debe cerrarse).
    Devuelve False si no aplica extension (cerrar normalmente).
    """
    extensions = pos.get("tp_extensions", 0)
    if extensions >= MAX_TP_EXTENSIONS:
        log.info("[%s] Máx extensiones TP alcanzadas (%d) — cerrando", symbol, MAX_TP_EXTENSIONS)
        return False

    # Evaluar si la señal sigue activa
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
        return False

    # La señal debe coincidir con el lado de la posición abierta
    if not signal or signal != pos["side"]:
        log.info("[%s] Señal no válida para extend_tp (signal=%s, pos=%s) — cerrando",
                 symbol, signal, pos["side"])
        return False

    # Calcular nuevo TP: entrada + (distancia original) * multiplicador
    entry       = pos["entry"]
    tp_orig_dist = abs(pos.get("tp_original", pos["tp"]) - entry)
    side        = pos["side"]

    if side == "long":
        new_tp = round(entry + tp_orig_dist * TP_EXTEND_RR * (extensions + 2), 6)
        # SL sube a breakeven (entrada) para proteger ganancia
        new_sl = round(entry * 1.0005, 6)   # +0.05% sobre entrada (cubre comisión)
    else:
        new_tp = round(entry - tp_orig_dist * TP_EXTEND_RR * (extensions + 2), 6)
        new_sl = round(entry * 0.9995, 6)

    try:
        exchange.cancel_all_orders(symbol)
        exchange.place_stop_order(symbol, side, pos["qty"], new_sl)
        exchange.place_tp_order(symbol, side, pos["qty"], new_tp)
    except Exception as e:
        log.warning("[%s] Error colocando órdenes en extend_tp: %s", symbol, e)
        return False

    old_tp = pos["tp"]
    pos["tp"]           = new_tp
    pos["sl"]           = new_sl
    pos["tp_extensions"] = extensions + 1
    pos["trail_high"]   = current_price
    pos["trail_low"]    = current_price

    log.info(
        "[%s] TP extendido #%d | old_tp=%.6f → new_tp=%.6f | SL→BE=%.6f | score=%d",
        symbol, extensions + 1, old_tp, new_tp, new_sl, score,
    )
    telegram.notify(
        f"📈 TP Extendido #{extensions + 1}\n"
        f"{symbol} {side.upper()}\n"
        f"TP anterior: <code>{old_tp:.6f}</code>\n"
        f"Nuevo TP: <code>{new_tp:.6f}</code>\n"
        f"SL → Breakeven: <code>{new_sl:.6f}</code>\n"
        f"Score: {score}"
    )
    return True


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

    hit_tp = (
        (side == "long"  and tp is not None and current_price >= tp * 0.995) or
        (side == "short" and tp is not None and current_price <= tp * 1.005)
    )
    return current_price, "TP" if hit_tp else "SL"


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

            # ── BATCH: 1 sola llamada para todas las posiciones del exchange ──────
            all_ex_positions = exchange.get_all_positions()

            # ── Sync posiciones abiertas ────────────────────────────────
            for symbol in list(positions.keys()):
                pos_ex = all_ex_positions.get(symbol)
                if not pos_ex:
                    p             = positions[symbol]
                    current_price = exchange.get_price(symbol)

                    exit_price, reason = _exit_price_for(p, current_price)

                    # ── Intentar extend_tp antes de cerrar ─────────────
                    if reason == "TP":
                        extended = _try_extend_tp(
                            symbol, p, current_price, feed, effective_min_score
                        )
                        if extended:
                            # Posición sigue abierta con nuevo TP — no cerrar
                            continue

                    # Cerrar definitivamente
                    positions.pop(symbol)

                    pnl_pct = (
                        (exit_price - p["entry"]) / p["entry"]
                        if p["side"] == "long"
                        else (p["entry"] - exit_price) / p["entry"]
                    ) * config.LEVERAGE * 100
                    pnl_usdt = (pnl_pct / 100) * (p["qty"] * p["entry"] / config.LEVERAGE)

                    hit_tp = reason == "TP"

                    # Cooldown solo si cierra por SL, o por TP sin señal válida
                    _cooldown[symbol] = time.time()
                    log.info(
                        "[%s] Cooldown activado (%dm) tras %s",
                        symbol, COOLDOWN // 60, reason,
                    )

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
                        symbol  = symbol,
                        side    = p["side"],
                        entry   = p["entry"],
                        exit_p  = exit_price,
                        pnl_pct = pnl_pct,
                        reason  = reason + (" ✅" if hit_tp else " ❌"),
                    )
                    log.info("[%s] Cerrada | %s | PnL=%+.2f%% (%+.4f USDT) | ext=%d",
                             symbol, reason, pnl_pct, pnl_usdt,
                             p.get("tp_extensions", 0))

            # ── Purgar cooldowns expirados ──────────────────────────────
            expired = [
                sym for sym, ts in _cooldown.items()
                if time.time() - ts >= COOLDOWN
            ]
            for sym in expired:
                _cooldown.pop(sym, None)
                log.info("[%s] Cooldown expirado — símbolo disponible", sym)

            # Recuperar posiciones sincronizadas del exchange
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
                        "trail_step":    0,
                        "score":         70,
                        "open_ts":       time.time(),
                    }
                    log.info("[%s] Sincronizada: %s @ %.4f (sl=%s tp=%s)",
                             symbol, pos_ex["side"], pos_ex["entry"],
                             pos_ex["sl"], pos_ex["tp"])

            open_count = len(positions)

            if loop_count % 10 == 1:
                log.info("[loop #%d] Posiciones: %d/%d | Feed: %d/%d pares listos | Cooldowns: %d",
                         loop_count, open_count, config.MAX_POSITIONS,
                         feed.ready_count(), len(config.SYMBOLS), len(_cooldown))

            # ── Trailing stop ──────────────────────────────────────────────
            for symbol, pos in list(positions.items()):
                try:
                    _update_trailing(symbol, pos, exchange.get_price(symbol))
                except Exception as e:
                    log.warning("[%s] Error trailing: %s", symbol, e)

            # ── Filtro fin de semana ────────────────────────────────────────
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

            # ── Buscar señales nuevas ──────────────────────────────────────────
            if open_count < config.MAX_POSITIONS:
                for symbol in config.SYMBOLS:
                    if symbol in positions:
                        continue
                    if len(positions) >= config.MAX_POSITIONS:
                        break
                    if not feed.ready(symbol):
                        continue

                    if symbol in _cooldown:
                        remaining = int(COOLDOWN - (time.time() - _cooldown[symbol]))
                        log.debug("[%s] En cooldown (%ds restantes)", symbol, remaining)
                        continue

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
                            "trail_step":    params["trail_step"],
                            "trail_high":    price,
                            "trail_low":     price,
                            "score":         score,
                            "open_ts":       time.time(),
                        }

                        telegram.notify_open(
                            symbol = symbol,
                            price  = price,
                            side   = signal,
                            qty    = params["qty"],
                            sl     = params["sl"],
                            tp     = params["tp"],
                        )

                    except Exception as e:
                        log.error("[%s] Error: %s", symbol, e, exc_info=True)

        except Exception as e:
            log.error("Error en loop: %s", e, exc_info=True)
            telegram.notify(f"⚠️ Error en bot: {e}")

        time.sleep(config.LOOP_SLEEP)


if __name__ == "__main__":
    run()
