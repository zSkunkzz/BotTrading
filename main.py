"""main.py — Loop principal del bot."""
import logging
import time

import config
import exchange
import risk
import signals
import telegram

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
log = logging.getLogger("main")


def run() -> None:
    log.info("Bot iniciado | %s | lev=%dx | size=%s USDT | SL=1.5×ATR TP=3×ATR",
             config.SYMBOL, config.LEVERAGE, config.USDC_SIZE)

    exchange.set_leverage()
    telegram.notify(f"🤖 Bot iniciado — {config.SYMBOL} {config.LEVERAGE}x")

    position = None  # {side, entry, qty, sl, tp}

    while True:
        try:
            # ── Sync posición con el exchange ──────────────────────────────────
            pos_exchange = exchange.get_position()

            if position and not pos_exchange:
                # Cerrada externamente (SL/TP ejecutado en exchange)
                exit_price = exchange.get_price()
                pnl = ((exit_price - position["entry"]) / position["entry"]
                       if position["side"] == "long"
                       else (position["entry"] - exit_price) / position["entry"])
                pnl *= config.LEVERAGE * 100
                telegram.notify_close(
                    symbol  = config.SYMBOL,
                    side    = position["side"],
                    entry   = position["entry"],
                    exit_p  = exit_price,
                    pnl_pct = pnl,
                    reason  = "SL/TP o cierre externo",
                )
                log.info("Posición cerrada externamente | PnL=%+.2f%%", pnl)
                position = None

            elif pos_exchange and not position:
                # Posición en exchange que no tenemos en memoria → sincronizar
                position = {
                    "side":  pos_exchange["side"],
                    "entry": pos_exchange["entry"],
                    "qty":   pos_exchange["size"],
                    "sl":    pos_exchange["sl"],
                    "tp":    pos_exchange["tp"],
                }
                log.info("Posición sincronizada desde exchange: %s @ %.4f",
                         position["side"], position["entry"])

            # ── Sin posición → buscar señal ────────────────────────────────────
            if not position:
                candles_15m = exchange.get_ohlcv(interval="15m", limit=100)
                candles_1h  = exchange.get_ohlcv(interval="1h",  limit=210)

                signal = signals.evaluate(candles_15m, candles_1h)

                if signal:
                    price  = exchange.get_price()
                    params = risk.calc(signal, price, candles_15m)

                    log.info("Señal: %s | entry=%.4f sl=%.4f tp=%.4f qty=%.4f",
                             signal.upper(), price,
                             params["sl"], params["tp"], params["qty"])

                    exchange.open_order(
                        side = signal,
                        qty  = params["qty"],
                        sl   = params["sl"],
                        tp   = params["tp"],
                    )

                    position = {
                        "side":  signal,
                        "entry": price,
                        "qty":   params["qty"],
                        "sl":    params["sl"],
                        "tp":    params["tp"],
                    }

                    telegram.notify_open(
                        symbol = config.SYMBOL,
                        side   = signal,
                        price  = price,
                        qty    = params["qty"],
                        sl     = params["sl"],
                        tp     = params["tp"],
                    )

        except Exception as e:
            log.error("Error en loop: %s", e, exc_info=True)
            telegram.notify(f"⚠️ Error en bot: {e}")

        time.sleep(config.LOOP_SLEEP)


if __name__ == "__main__":
    run()
