"""main.py — Loop principal con trailing stop y re-entrada inteligente."""
import logging
import sys
import time

import config
import exchange
import risk
import signals
import telegram
from ws_feed import KlineFeed

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("main")

# ── Re-entrada: recuerda el último símbolo/dirección cerrado con TP ───────────
_last_closed: dict = {}   # {symbol: {"side": str, "ts": float, "score": int}}
REENTRY_WINDOW  = 4 * 60   # segundos — ventana para re-entrada tras TP
REENTRY_SCORE_BOOST = 10    # bonus de score en re-entrada (tendencia sigue viva)
REENTRY_SIZE_MULT   = 0.6   # tamaño reducido en re-entrada


def _update_trailing(symbol: str, pos: dict, current_price: float) -> None:
    """
    Mueve el SL cuando el precio avanza ≥ trail_step desde el último high/low.
    Solo sube el SL (nunca lo baja).
    Actualiza pos['sl'] y pos['trail_high'/'trail_low'] in-place.
    """
    side       = pos["side"]
    trail_step = pos.get("trail_step", 0)
    if trail_step <= 0:
        return

    if side == "long":
        peak = pos.get("trail_high", pos["entry"])
        if current_price > peak + trail_step:
            new_peak = current_price
            new_sl   = round(new_peak - 1.5 * trail_step, 6)
            if new_sl > pos["sl"]:
                log.info("[%s] Trailing SL: %.4f → %.4f (peak=%.4f)",
                         symbol, pos["sl"], new_sl, new_peak)
                pos["trail_high"] = new_peak
                pos["sl"]         = new_sl
                # Reemplaza la stop-order en el exchange
                try:
                    exchange.cancel_all_orders(symbol)
                    exchange.place_stop_order(symbol, "long", pos["qty"], new_sl)
                    exchange.place_tp_order(symbol, "long", pos["qty"], pos["tp"])
                except Exception as e:
                    log.warning("[%s] Error actualizando trailing SL: %s", symbol, e)

    else:  # short
        trough = pos.get("trail_low", pos["entry"])
        if current_price < trough - trail_step:
            new_trough = current_price
            new_sl     = round(new_trough + 1.5 * trail_step, 6)
            if new_sl < pos["sl"]:
                log.info("[%s] Trailing SL: %.4f → %.4f (trough=%.4f)",
                         symbol, pos["sl"], new_sl, new_trough)
                pos["trail_low"] = new_trough
                pos["sl"]        = new_sl
                try:
                    exchange.cancel_all_orders(symbol)
                    exchange.place_stop_order(symbol, "short", pos["qty"], new_sl)
                    exchange.place_tp_order(symbol, "short", pos["qty"], pos["tp"])
                except Exception as e:
                    log.warning("[%s] Error actualizando trailing SL: %s", symbol, e)


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

    log.info("Esperando datos del feed...")
    while not all(feed.ready(s) for s in config.SYMBOLS):
        time.sleep(2)
    log.info("Feed listo — iniciando loop de señales")

    positions: dict = {}

    while True:
        try:
            # ── Sync posiciones ───────────────────────────────────────────────
            for symbol in list(positions.keys()):
                pos_ex = exchange.get_position(symbol)
                if not pos_ex:
                    p = positions.pop(symbol)
                    exit_price = exchange.get_price(symbol)
                    pnl = (
                        (exit_price - p["entry"]) / p["entry"]
                        if p["side"] == "long"
                        else (p["entry"] - exit_price) / p["entry"]
                    ) * config.LEVERAGE * 100

                    # Detectar si fue TP (precio llegó al target) para re-entrada
                    hit_tp = (
                        (p["side"] == "long"  and exit_price >= p["tp"] * 0.995) or
                        (p["side"] == "short" and exit_price <= p["tp"] * 1.005)
                    )
                    reason = "TP ✅" if hit_tp else "SL/cierre externo"

                    if hit_tp:
                        _last_closed[symbol] = {
                            "side":  p["side"],
                            "ts":    time.time(),
                            "score": p.get("score", 70),
                        }
                        log.info("[%s] Guardada para re-entrada (TP hit)", symbol)

                    telegram.notify_close(
                        symbol  = symbol,
                        side    = p["side"],
                        entry   = p["entry"],
                        exit_p  = exit_price,
                        pnl_pct = pnl,
                        reason  = reason,
                    )
                    log.info("[%s] Cerrada | %s | PnL=%+.2f%%", symbol, reason, pnl)

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
                        }
                        log.info("[%s] Sincronizada: %s @ %.4f",
                                 symbol, pos_ex["side"], pos_ex["entry"])

            open_count = len(positions)
            log.info("Posiciones abiertas: %d/%d", open_count, config.MAX_POSITIONS)

            # ── Trailing stop en posiciones abiertas ──────────────────────────
            for symbol, pos in positions.items():
                try:
                    current_price = exchange.get_price(symbol)
                    _update_trailing(symbol, pos, current_price)
                except Exception as e:
                    log.warning("[%s] Error trailing: %s", symbol, e)

            # ── Buscar señales nuevas ─────────────────────────────────────────
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

                        # ── Re-entrada inteligente ────────────────────────────
                        if signal is None and symbol in _last_closed:
                            last = _last_closed[symbol]
                            elapsed = time.time() - last["ts"]
                            if elapsed < REENTRY_WINDOW:
                                # Comprobar si la tendencia sigue en la misma dirección
                                sig_re, sc_re = signals.evaluate(candles_15m, candles_1h, candles_4h)
                                # Usar score boosted aunque la señal no alcance MIN_SCORE
                                boosted = sc_re + REENTRY_SCORE_BOOST if sig_re is None else sc_re
                                if boosted >= signals.MIN_SCORE and last["side"] == (sig_re or last["side"]):
                                    signal = last["side"]
                                    score  = boosted
                                    log.info("[%s] 🔄 Re-entrada | side=%s score=%d",
                                             symbol, signal, score)
                                else:
                                    _last_closed.pop(symbol, None)   # tendencia rota

                        if not signal:
                            continue

                        price  = exchange.get_price(symbol)
                        params = risk.calc(signal, price, candles_15m, score)

                        # Re-entrada usa size reducido
                        qty = params["qty"]
                        if symbol in _last_closed:
                            qty = round(params["qty"] * REENTRY_SIZE_MULT, 4)
                            _last_closed.pop(symbol, None)

                        log.info("[%s] %s | entry=%.4f sl=%.4f tp=%.4f qty=%.4f score=%d",
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
                        }

                        telegram.notify_open(
                            symbol = symbol,
                            side   = signal,
                            price  = price,
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
