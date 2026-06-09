"""main.py — Loop multi-par. Máximo MAX_POSITIONS abiertas simultáneamente."""
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
    log.info(
        "Bot iniciado | %d pares | lev=%dx | margin=%s USDT | max=%d posiciones",
        len(config.SYMBOLS), config.LEVERAGE, config.MARGIN_USDT, config.MAX_POSITIONS,
    )

    # Setear apalancamiento en todos los pares al arrancar
    for symbol in config.SYMBOLS:
        try:
            exchange.set_leverage(symbol)
        except Exception as e:
            log.warning("No se pudo setear leverage en %s: %s", symbol, e)

    telegram.notify(
        f"🤖 Bot iniciado — {len(config.SYMBOLS)} pares | "
        f"{config.LEVERAGE}x | max {config.MAX_POSITIONS} posiciones"
    )

    # Estado en memoria: {symbol: {side, entry, qty, sl, tp}}
    positions: dict = {}

    while True:
        try:
            # ── Sync posiciones con el exchange ──────────────────────────────
            for symbol in list(positions.keys()):
                pos_ex = exchange.get_position(symbol)
                if not pos_ex:
                    # Cerrada externamente (SL/TP)
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
                    log.info("[%s] Cerrada externamente | PnL=%+.2f%%", symbol, pnl)

            # Recuperar posiciones abiertas en exchange que no están en memoria
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
                        log.info("[%s] Sincronizada desde exchange: %s @ %.4f",
                                 symbol, pos_ex["side"], pos_ex["entry"])

            open_count = len(positions)
            log.info("Posiciones abiertas: %d/%d", open_count, config.MAX_POSITIONS)

            # ── Buscar señales en pares sin posición ─────────────────────────
            if open_count < config.MAX_POSITIONS:
                for symbol in config.SYMBOLS:
                    if symbol in positions:
                        continue
                    if len(positions) >= config.MAX_POSITIONS:
                        break

                    try:
                        candles_15m = exchange.get_ohlcv(symbol, interval="15m", limit=120)
                        candles_1h  = exchange.get_ohlcv(symbol, interval="1h",  limit=220)
                        signal      = signals.evaluate(candles_15m, candles_1h)

                        if not signal:
                            continue

                        price  = exchange.get_price(symbol)
                        params = risk.calc(signal, price, candles_15m)

                        log.info("[%s] Señal %s | entry=%.4f sl=%.4f tp=%.4f qty=%.4f",
                                 symbol, signal.upper(), price,
                                 params["sl"], params["tp"], params["qty"])

                        exchange.open_order(
                            side    = signal,
                            qty     = params["qty"],
                            sl      = params["sl"],
                            tp      = params["tp"],
                            symbol  = symbol,
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
                        log.error("[%s] Error escaneando: %s", symbol, e, exc_info=True)

        except Exception as e:
            log.error("Error en loop principal: %s", e, exc_info=True)
            telegram.notify(f"⚠️ Error en bot: {e}")

        time.sleep(config.LOOP_SLEEP)


if __name__ == "__main__":
    run()
