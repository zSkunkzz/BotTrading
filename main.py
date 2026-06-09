"""main.py — Loop principal. Usa WebSocket feed para las velas."""
import logging
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
)
log = logging.getLogger("main")


def run() -> None:
    log.info(
        "Bot iniciado | %d pares | lev=%dx | margin=%s USDT | max=%d posiciones",
        len(config.SYMBOLS), config.LEVERAGE, config.MARGIN_USDT, config.MAX_POSITIONS,
    )

    # Setear apalancamiento en todos los pares
    for symbol in config.SYMBOLS:
        try:
            exchange.set_leverage(symbol)
        except Exception as e:
            log.warning("No se pudo setear leverage en %s: %s", symbol, e)

    telegram.notify(
        f"🤖 Bot iniciado — {len(config.SYMBOLS)} pares | "
        f"{config.LEVERAGE}x | max {config.MAX_POSITIONS} posiciones"
    )

    # Arrancar WebSocket feed (precarga REST + suscripción WS)
    feed = KlineFeed(config.SYMBOLS)
    feed.start()

    # Esperar a que el feed tenga velas suficientes
    log.info("Esperando datos del feed...")
    while not all(feed.ready(s) for s in config.SYMBOLS):
        time.sleep(2)
    log.info("Feed listo — iniciando loop de señales")

    positions: dict = {}  # {symbol: {side, entry, qty, sl, tp}}

    while True:
        try:
            # ── Sync posiciones con el exchange ──────────────────────────────
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
                    telegram.notify_close(
                        symbol  = symbol,
                        side    = p["side"],
                        entry   = p["entry"],
                        exit_p  = exit_price,
                        pnl_pct = pnl,
                        reason  = "SL/TP o cierre externo",
                    )
                    log.info("[%s] Cerrada | PnL=%+.2f%%", symbol, pnl)

            # Recuperar posiciones abiertas en exchange no registradas
            for symbol in config.SYMBOLS:
                if symbol not in positions:
                    pos_ex = exchange.get_position(symbol)
                    if pos_ex:
                        positions[symbol] = {
                            "side":  pos_ex["side"],
                            "entry": pos_ex["entry"],
                            "qty":   pos_ex["size"],
                            "sl":    pos_ex["sl"],
                            "tp":    pos_ex["tp"],
                        }
                        log.info("[%s] Sincronizada: %s @ %.4f",
                                 symbol, pos_ex["side"], pos_ex["entry"])

            open_count = len(positions)
            log.info("Posiciones abiertas: %d/%d", open_count, config.MAX_POSITIONS)

            # ── Buscar señales ────────────────────────────────────────────────
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
                        signal      = signals.evaluate(candles_15m, candles_1h)

                        if not signal:
                            continue

                        price  = exchange.get_price(symbol)
                        params = risk.calc(signal, price, candles_15m)

                        log.info("[%s] %s | entry=%.4f sl=%.4f tp=%.4f qty=%.4f",
                                 symbol, signal.upper(), price,
                                 params["sl"], params["tp"], params["qty"])

                        exchange.open_order(
                            side   = signal,
                            qty    = params["qty"],
                            sl     = params["sl"],
                            tp     = params["tp"],
                            symbol = symbol,
                        )

                        positions[symbol] = {
                            "side":  signal,
                            "entry": price,
                            "qty":   params["qty"],
                            "sl":    params["sl"],
                            "tp":    params["tp"],
                        }

                        telegram.notify_open(
                            symbol = symbol,
                            side   = signal,
                            price  = price,
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
