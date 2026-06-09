"""main.py — Loop principal con trailing stop y re-entrada inteligente."""
import logging
import sys
import time

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

_last_closed: dict = {}
REENTRY_WINDOW      = 4 * 60
REENTRY_SCORE_BOOST = 10
REENTRY_SIZE_MULT   = 0.6

READY_TIMEOUT = 120
READY_MIN_PCT = 0.80


def _update_trailing(symbol: str, pos: dict, current_price: float) -> None:
    side       = pos["side"]
    trail_step = pos.get("trail_step", 0)
    if trail_step <= 0:
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


def run() -> None:
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

    # ── Arrancar listener de comandos Telegram ──
    tg_commands.start(get_positions_fn=lambda: positions, feed=feed)

    loop_count = 0

    while True:
        try:
            loop_count += 1

            # ── Sync posiciones ──────────────────────────────────────────────
            for symbol in list(positions.keys()):
                pos_ex = exchange.get_position(symbol)
                if not pos_ex:
                    p          = positions.pop(symbol)
                    exit_price = exchange.get_price(symbol)
                    pnl_pct = (
                        (exit_price - p["entry"]) / p["entry"]
                        if p["side"] == "long"
                        else (p["entry"] - exit_price) / p["entry"]
                    ) * config.LEVERAGE * 100
                    pnl_usdt = (pnl_pct / 100) * (p["qty"] * p["entry"] / config.LEVERAGE)

                    hit_tp = (
                        (p["side"] == "long"  and exit_price >= p["tp"] * 0.995) or
                        (p["side"] == "short" and exit_price <= p["tp"] * 1.005)
                    )
                    reason = "TP" if hit_tp else "SL"

                    if hit_tp:
                        _last_closed[symbol] = {
                            "side":  p["side"],
                            "ts":    time.time(),
                            "score": p.get("score", 70),
                        }

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
                    log.info("[%s] Cerrada | %s | PnL=%+.2f%% (%+.4f USDT)",
                             symbol, reason, pnl_pct, pnl_usdt)

            # Recuperar posiciones abiertas no registradas
            for symbol in config.SYMBOLS:
                if symbol not in positions:
                    pos_ex = exchange.get_position(symbol)
                    if pos_ex:
                        positions[symbol] = {
                            "side":       pos_ex["side"],
                            "entry":      pos_ex["entry"],
                            "qty":        pos_ex["size"],
                            "sl":         pos_ex["sl"],
                            "tp":         pos_ex["tp"],
                            "trail_step": 0,
                            "score":      70,
                            "open_ts":    time.time(),
                        }
                        log.info("[%s] Sincronizada: %s @ %.4f",
                                 symbol, pos_ex["side"], pos_ex["entry"])

            open_count = len(positions)

            if loop_count % 10 == 1:
                log.info("[loop #%d] Posiciones: %d/%d | Feed: %d/%d pares listos",
                         loop_count, open_count, config.MAX_POSITIONS,
                         feed.ready_count(), len(config.SYMBOLS))

            # ── Trailing stop ────────────────────────────────────────────────
            for symbol, pos in list(positions.items()):
                try:
                    current_price = exchange.get_price(symbol)
                    _update_trailing(symbol, pos, current_price)
                except Exception as e:
                    log.warning("[%s] Error trailing: %s", symbol, e)

            # ── Buscar señales nuevas ────────────────────────────────────────
            if open_count < config.MAX_POSITIONS:
                for symbol in config.SYMBOLS:
                    if symbol in positions:
                        continue
                    if len(positions) >= config.MAX_POSITIONS:
                        break
                    if not feed.ready(symbol):
                        continue

                    try:
                        candles_15m = feed.get(symbol, "15m")
                        candles_1h  = feed.get(symbol, "1h")
                        candles_4h  = feed.get(symbol, "4h") if feed.has_tf(symbol, "4h") else None

                        signal, score = signals.evaluate(candles_15m, candles_1h, candles_4h)

                        if signal is None and symbol in _last_closed:
                            last = _last_closed[symbol]
                            if time.time() - last["ts"] < REENTRY_WINDOW:
                                sig_re, sc_re = signals.evaluate(candles_15m, candles_1h, candles_4h)
                                boosted = (sc_re or 0) + REENTRY_SCORE_BOOST
                                if boosted >= signals.MIN_SCORE and last["side"] == (sig_re or last["side"]):
                                    signal = last["side"]
                                    score  = boosted
                                    log.info("[%s] 🔄 Re-entrada | side=%s score=%d", symbol, signal, score)
                                else:
                                    _last_closed.pop(symbol, None)

                        if not signal:
                            continue

                        price  = exchange.get_price(symbol)
                        # Pasamos symbol para que risk.calc() use el step-size
                        # real del contrato en vez del hardcode de 3 decimales
                        params = risk.calc(signal, price, candles_15m, score, symbol=symbol)

                        qty = params["qty"]
                        if symbol in _last_closed:
                            qty = exchange.floor_qty(
                                params["qty"] * REENTRY_SIZE_MULT,
                                exchange._get_contract_info(symbol)["stepSize"],
                            )
                            _last_closed.pop(symbol, None)

                        log.info("[%s] SEÑAL %s | entry=%.6f sl=%.6f tp=%.6f qty=%.8f score=%d",
                                 symbol, signal.upper(), price,
                                 params["sl"], params["tp"], qty, score)

                        exchange.open_order(
                            side   = signal,
                            qty    = qty,
                            sl     = params["sl"],
                            tp     = params["tp"],
                            symbol = symbol,
                        )

                        positions[symbol] = {
                            "side":       signal,
                            "entry":      price,
                            "qty":        qty,
                            "sl":         params["sl"],
                            "tp":         params["tp"],
                            "trail_step": params["trail_step"],
                            "trail_high": price,
                            "trail_low":  price,
                            "score":      score,
                            "open_ts":    time.time(),
                        }

                        telegram.notify_open(
                            symbol = symbol,
                            price  = price,
                            side   = signal,
                            qty    = qty,
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
