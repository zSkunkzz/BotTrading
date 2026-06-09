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
    log.info("Bot iniciado | %s | lev=%dx | size=%s USDT | SL=%.1f%% TP=%.1f%%",
             config.SYMBOL, config.LEVERAGE, config.USDC_SIZE,
             config.SL_PCT, config.TP_PCT)

    exchange.set_leverage()
    telegram.notify(f"🤖 Bot iniciado — {config.SYMBOL} {config.LEVERAGE}x")

    position = None  # {side, entry, qty, sl, tp}

    while True:
        try:
            # ── Sync posición con el exchange ─────────────────────────────────
            pos_exchange = exchange.get_position()

            if position and not pos_exchange:
                # Posición cerrada externamente (SL/TP ejecutado)
                exit_price = exchange.get_price()
                pnl = ((exit_price - position["entry"]) / position["entry"] * 100
                       if position["side"] == "long"
                       else (position["entry"] - exit_price) / position["entry"] * 100)
                pnl *= config.LEVERAGE
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
                # Posición detectada en exchange que no tenemos en memoria
                position = {
                    "side":  pos_exchange["side"],
                    "entry": pos_exchange["entry"],
                    "qty":   pos_exchange["size"],
                    "sl":    pos_exchange["sl"],
                    "tp":    pos_exchange["tp"],
                }
                log.info("Posición sincronizada desde exchange: %s @ %.4f",
                         position["side"], position["entry"])

            # ── Sin posición → buscar señal ───────────────────────────────────
            if not position:
                candles = exchange.get_ohlcv()
                signal  = signals.evaluate(candles)

                if signal:
                    price  = exchange.get_price()
                    params = risk.calc(signal, price)

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
                else:
                    log.info("Sin señal")

        except Exception as e:
            log.error("Error en loop: %s", e, exc_info=True)
            telegram.notify(f"⚠️ Error en bot: {e}")

        time.sleep(config.LOOP_SLEEP)


if __name__ == "__main__":
    run()
