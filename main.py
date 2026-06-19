"""main.py — Loop principal con trailing stop, breakeven automático y TP dinámico.

NOVEDADES:
  - Breakeven automático: mueve SL a entrada cuando precio alcanza +1 RR.
  - Persistencia de posiciones: guarda/restaura positions.json al arrancar.
    Sobrevive reinicios y deploys de Railway preservando open_ts, score,
    tp_original y tp_extensions.
"""
import json
import logging
import os
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

_cooldown:              dict[str, float] = {}
_cooldown_sl:           dict[str, float] = {}
_manual_alert_cooldown: dict[str, float] = {}

COOLDOWN               = 60 * 60
COOLDOWN_SL            = 2 * 60 * 60
MANUAL_ALERT_COOLDOWN  = 60 * 60
MAX_TP_EXTENSIONS      = 3
TP_EXTEND_RR           = 1.5
TP_EXTEND_THRESH       = 0.015

# Breakeven: se activa cuando el precio alcanza entry + BE_RR × distancia_sl
BE_RR          = 1.0   # 1 RR de ganancia → SL a breakeven
BE_BUFFER      = 0.0003  # buffer mínimo sobre entry (0.03%) para cubrir fees

READY_TIMEOUT = 120
READY_MIN_PCT = 0.80

WEEKDAY_MIN_SCORE = config.MIN_SCORE
WEEKEND_MIN_SCORE = config.WEEKEND_MIN_SCORE

POSITIONS_FILE = os.getenv("POSITIONS_FILE", "positions.json")

_weekend_notified_day: int = -1


def _is_weekend() -> bool:
    return datetime.now(timezone.utc).weekday() >= 5


def _corr_group_count(symbol: str, positions: dict) -> int:
    for group in config.CORR_GROUPS:
        if symbol in group:
            return sum(1 for s in positions if s in group)
    return 0


# ── Persistencia de posiciones ────────────────────────────────────────────────

def _save_positions(positions: dict) -> None:
    """Serializa positions a JSON para sobrevivir reinicios."""
    try:
        data = {}
        for sym, pos in positions.items():
            # Excluye claves internas que no son serializables o son transitorias
            data[sym] = {k: v for k, v in pos.items() if k != "_extending"}
        with open(POSITIONS_FILE, "w") as f:
            json.dump(data, f)
    except Exception as e:
        log.warning("Error guardando positions.json: %s", e)


def _load_positions() -> dict:
    """Restaura positions desde JSON si existe.

    Recupera open_ts, score, tp_original, tp_extensions y tp/sl reales
    de la sesión anterior. Si el archivo no existe o está corrupto,
    arranca con dict vacío (sincronización normal desde exchange).
    """
    if not os.path.exists(POSITIONS_FILE):
        log.info("positions.json no encontrado — sincronización normal desde exchange")
        return {}
    try:
        with open(POSITIONS_FILE) as f:
            data = json.load(f)
        # Asegura que todos los campos opcionales existen
        for sym, pos in data.items():
            pos.setdefault("tp_extensions", 0)
            pos.setdefault("_extending", False)
            pos.setdefault("trail_step", 0)
            pos.setdefault("trail_high", pos.get("entry", 0))
            pos.setdefault("trail_low",  pos.get("entry", 0))
            pos.setdefault("score", 70)
            pos.setdefault("open_ts", time.time())
            pos.setdefault("breakeven_set", False)
        log.info("Posiciones restauradas desde JSON: %s", list(data.keys()))
        return data
    except Exception as e:
        log.warning("Error leyendo positions.json: %s — arrancando vacío", e)
        return {}


# ── Breakeven automático ──────────────────────────────────────────────────────

def _check_breakeven(symbol: str, pos: dict, current_price: float) -> None:
    """Mueve SL a entry+buffer cuando el precio alcanza entry + BE_RR × riesgo.

    Solo actúa una vez por posición (pos['breakeven_set'] = True).
    No interfiere con el trailing ni con extend_tp — son capas independientes.
    """
    if pos.get("breakeven_set"):
        return
    if pos.get("sl") is None or pos.get("entry") is None:
        return

    side  = pos["side"]
    entry = pos["entry"]
    sl    = pos["sl"]
    risk_dist = abs(entry - sl)  # distancia entry→SL = 1 unidad de riesgo

    if risk_dist == 0:
        return

    if side == "long":
        target = entry + BE_RR * risk_dist
        if current_price < target:
            return
        new_sl = round(entry * (1 + BE_BUFFER), 6)
        if new_sl <= sl:  # ya está mejor que entry — no mover hacia atrás
            pos["breakeven_set"] = True
            return
    else:
        target = entry - BE_RR * risk_dist
        if current_price > target:
            return
        new_sl = round(entry * (1 - BE_BUFFER), 6)
        if new_sl >= sl:
            pos["breakeven_set"] = True
            return

    log.info(
        "[%s] Breakeven activado | entry=%.4f SL: %.4f → %.4f (precio=%.4f target=%.4f)",
        symbol, entry, sl, new_sl, current_price, target,
    )

    try:
        exchange.cancel_all_orders(symbol)
        exchange.place_stop_order(symbol, side, pos["qty"], new_sl)
        if pos.get("tp") is not None:
            exchange.place_tp_order(symbol, side, pos["qty"], pos["tp"])
    except Exception as e:
        log.warning("[%s] Error colocando órdenes en breakeven: %s", symbol, e)
        return

    pos["sl"]             = new_sl
    pos["breakeven_set"]  = True

    telegram.notify(
        f"🔒 Breakeven activado\n"
        f"{symbol} {side.upper()}\n"
        f"SL movido a entrada: <code>{new_sl:.4f}</code>\n"
        f"(precio: <code>{current_price:.4f}</code> | +{BE_RR:.0f}R alcanzado)"
    )


# ── Trailing stop ─────────────────────────────────────────────────────────────

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


# ── TP extension ──────────────────────────────────────────────────────────────

def _check_tp_extension(
    symbol: str,
    pos: dict,
    current_price: float,
    feed,
    effective_min_score: int,
) -> None:
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
        log.info("[%s] Señal no válida para extend_tp — dejando TP actual", symbol)
        pos["_extending"] = False
        return

    entry        = pos["entry"]
    tp_orig_dist = abs(pos.get("tp_original", tp) - entry)

    if side == "long":
        new_tp = round(entry + tp_orig_dist * TP_EXTEND_RR * (extensions + 2), 6)
        new_sl = round(entry * 1.0005, 6)
    else:
        new_tp = round(entry - tp_orig_dist * TP_EXTEND_RR * (extensions + 2), 6)
        new_sl = round(entry * 0.9995, 6)

    try:
        exchange.cancel_all_orders(symbol)
        exchange.place_stop_order(symbol, side, pos["qty"], new_sl)
        exchange.place_tp_order(symbol, side, pos["qty"], new_tp)
    except Exception as e:
        log.warning("[%s] Error colocando órdenes en extend_tp: %s", symbol, e)
        pos["_extending"] = False
        return

    old_tp = pos["tp"]
    pos["tp"]            = new_tp
    pos["sl"]            = new_sl
    pos["tp_extensions"] = extensions + 1
    pos["trail_high"]    = current_price
    pos["trail_low"]     = current_price
    pos["_extending"]    = False
    # Al extender TP el SL ya va a breakeven — marcar para no mover de nuevo
    pos["breakeven_set"] = True

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


def _resolve_close(pos: dict, symbol: str) -> tuple[float, str]:
    reason, exit_price = exchange.get_closed_reason(symbol)
    if reason and exit_price:
        return exit_price, reason
    current_price = exchange.get_price(symbol)
    side = pos["side"]
    tp   = pos.get("tp")
    sl   = pos.get("sl")
    if tp is not None and sl is not None:
        dist_tp = abs(current_price - tp)
        dist_sl = abs(current_price - sl)
        return (tp, "TP") if dist_tp < dist_sl else (sl, "SL")
    pnl_raw = (
        (current_price - pos["entry"]) / pos["entry"]
        if side == "long"
        else (pos["entry"] - current_price) / pos["entry"]
    )
    return current_price, "TP" if pnl_raw > 0 else "SL"


def run() -> None:
    global _weekend_notified_day

    log.info(
        "Bot iniciado | %d pares | lev=%dx | margin=%s USDT | max=%d posiciones | "
        "min_score=%d | weekend_min_score=%d | max_daily_loss=%.0f USDT | "
        "max_corr=%d | sl_min=0.6%% | max_spread=%.2f%%",
        len(config.SYMBOLS), config.LEVERAGE, config.MARGIN_USDT, config.MAX_POSITIONS,
        WEEKDAY_MIN_SCORE, WEEKEND_MIN_SCORE, config.MAX_DAILY_LOSS_USDT,
        config.MAX_CORR_PER_GROUP, config.MAX_SPREAD_PCT,
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

    # Restaurar posiciones desde JSON antes de sincronizar con exchange
    positions: dict = _load_positions()
    tg_commands.start(get_positions_fn=lambda: positions, feed=feed)
    trade_logger.start_scheduler()   # también restaura daily loss desde CSV

    loop_count = 0

    while True:
        try:
            loop_count += 1

            weekend = _is_weekend()
            effective_min_score = WEEKEND_MIN_SCORE if weekend else WEEKDAY_MIN_SCORE

            all_ex_positions = exchange.get_all_positions()

            for symbol in list(positions.keys()):
                pos_ex = all_ex_positions.get(symbol)
                if not pos_ex:
                    p = positions.pop(symbol)
                    _save_positions(positions)  # persistir tras cierre

                    exit_price, reason = _resolve_close(p, symbol)
                    pnl_pct = (
                        (exit_price - p["entry"]) / p["entry"]
                        if p["side"] == "long"
                        else (p["entry"] - exit_price) / p["entry"]
                    ) * config.LEVERAGE * 100
                    pnl_usdt = (pnl_pct / 100) * (p["qty"] * p["entry"] / config.LEVERAGE)

                    if reason == "SL":
                        _cooldown_sl[symbol] = time.time()
                        log.info("[%s] Cooldown SL activado (%dh)", symbol, COOLDOWN_SL // 3600)
                    else:
                        _cooldown[symbol] = time.time()
                        log.info("[%s] Cooldown activado (%dm) tras %s", symbol, COOLDOWN // 60, reason)

                    trade_logger.record(
                        symbol=symbol, side=p["side"], entry=p["entry"],
                        exit_price=exit_price, pnl_pct=pnl_pct, pnl_usdt=pnl_usdt,
                        score=p.get("score", 0), reason=reason, open_ts=p.get("open_ts", time.time()),
                    )
                    telegram.notify_close(
                        symbol=symbol, side=p["side"], entry=p["entry"],
                        exit_p=exit_price, pnl_pct=pnl_pct, pnl_usdt=pnl_usdt,
                        reason=reason, open_ts=p.get("open_ts", 0.0),
                    )
                    log.info("[%s] Cerrada | %s | PnL=%+.2f%% (%+.4f USDT) | ext=%d | be=%s",
                             symbol, reason, pnl_pct, pnl_usdt,
                             p.get("tp_extensions", 0), p.get("breakeven_set", False))

            expired = [sym for sym, ts in _cooldown.items() if time.time() - ts >= COOLDOWN]
            for sym in expired:
                _cooldown.pop(sym, None)
                log.info("[%s] Cooldown TP expirado", sym)

            expired_sl = [sym for sym, ts in _cooldown_sl.items() if time.time() - ts >= COOLDOWN_SL]
            for sym in expired_sl:
                _cooldown_sl.pop(sym, None)
                log.info("[%s] Cooldown SL expirado", sym)

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
                        "breakeven_set": False,
                    }
                    log.info("[%s] Sincronizada desde exchange: %s @ %.4f", symbol, pos_ex["side"], pos_ex["entry"])

            open_count = len(positions)

            if loop_count % 10 == 1:
                log.info(
                    "[loop #%d] Posiciones: %d/%d | Feed: %d/%d | "
                    "Cooldowns: %d TP + %d SL | Pausado: %s | DailyLimit: %s",
                    loop_count, open_count, config.MAX_POSITIONS,
                    feed.ready_count(), len(config.SYMBOLS),
                    len(_cooldown), len(_cooldown_sl),
                    bot_state.is_paused(), trade_logger.is_daily_limit_hit(),
                )

            for symbol, pos in list(positions.items()):
                try:
                    price = exchange.get_price(symbol)
                    _check_breakeven(symbol, pos, price)   # 1º: breakeven
                    _update_trailing(symbol, pos, price)   # 2º: trailing
                    _check_tp_extension(symbol, pos, price, feed, effective_min_score)  # 3º: extend
                except Exception as e:
                    log.warning("[%s] Error gestión posición: %s", symbol, e)

            if weekend:
                today = datetime.now(timezone.utc).weekday()
                if today != _weekend_notified_day:
                    _weekend_notified_day = today
                    day_name = "Sábado" if today == 5 else "Domingo"
                    log.info("Modo fin de semana activo (%s UTC) — score mínimo %d", day_name, WEEKEND_MIN_SCORE)
                    telegram.notify(
                        f"🚫 Modo fin de semana ({day_name})\n"
                        f"Score mínimo ≥ {WEEKEND_MIN_SCORE} para nuevas entradas."
                    )

            if trade_logger.is_daily_limit_hit():
                log.debug("Daily drawdown límite activo — sin nuevas entradas")
                time.sleep(config.LOOP_SLEEP)
                continue

            if bot_state.is_paused():
                log.debug("Bot pausado — saltando búsqueda de señales")
            else:
                for symbol in config.SYMBOLS:
                    if symbol in positions:
                        continue
                    if not feed.ready(symbol):
                        continue
                    if symbol in _cooldown or symbol in _cooldown_sl:
                        continue

                    is_manual = symbol in config.MANUAL_ALERT_SYMBOLS

                    if is_manual and symbol in _manual_alert_cooldown:
                        continue
                    if not is_manual and open_count >= config.MAX_POSITIONS:
                        break

                    if not is_manual:
                        corr_count = _corr_group_count(symbol, positions)
                        if corr_count >= config.MAX_CORR_PER_GROUP:
                            log.debug("[%s] Correlación: %d/%d — saltando",
                                      symbol, corr_count, config.MAX_CORR_PER_GROUP)
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

                        if not is_manual:
                            spread = exchange.get_spread_pct(symbol)
                            if spread > config.MAX_SPREAD_PCT:
                                log.info(
                                    "[%s] Spread demasiado alto (%.3f%% > %.3f%%) — saltando",
                                    symbol, spread, config.MAX_SPREAD_PCT,
                                )
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
                                f"⚠️ <i>Operación NO abierta automáticamente.</i>"
                            )
                            log.info("[%s] ALERTA MANUAL enviada | %s score=%d", symbol, signal.upper(), score)
                            continue

                        log.info(
                            "[%s] SEÑAL %s | regime=%s RR=%.1f spread=%.3f%% | "
                            "entry=%.6f sl=%.6f tp=%.6f qty=%.8f score=%d",
                            symbol, signal.upper(), regime, params["tp_rr"], spread,
                            price, params["sl"], params["tp"], params["qty"], score,
                        )

                        exchange.open_order(
                            side=signal, qty=params["qty"],
                            sl=params["sl"], tp=params["tp"], symbol=symbol,
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
                            "breakeven_set": False,
                        }
                        open_count += 1
                        _save_positions(positions)  # persistir tras apertura

                        telegram.notify_open(
                            symbol=symbol, price=price, side=signal,
                            qty=params["qty"], sl=params["sl"], tp=params["tp"],
                            score=score, tp_rr=params["tp_rr"],
                        )

                    except Exception as e:
                        log.error("[%s] Error: %s", symbol, e, exc_info=True)

        except Exception as e:
            log.error("Error en loop: %s", e, exc_info=True)
            telegram.notify(f"⚠️ Error en bot: {e}")

        time.sleep(config.LOOP_SLEEP)


if __name__ == "__main__":
    run()
