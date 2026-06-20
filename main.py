"""main.py — Loop principal con trailing stop, breakeven automático y TP dinámico.

NOVEDADES:
  - Breakeven automático: mueve SL a entrada cuando precio alcanza +1 RR.
  - Persistencia de posiciones: guarda/restaura positions.json al arrancar.
    Sobrevive reinicios y deploys de Railway preservando open_ts, score,
    tp_original y tp_extensions.

FIXES:
  - spread inicializada a 0.0 antes del bloque is_manual.
  - positions.json se borra cuando positions queda vacío.
  - _extending excluido del JSON persistido; guard anti doble-extensión.
  - set_leverage(symbol, config.LEVERAGE) — segundo argumento faltaba.
  - _check_tp_extension valida new_sl antes de cancelar órdenes.
  - len(positions) en check MAX_POSITIONS (era open_count, podía desincronizarse).
  - _check_breakeven devuelve bool; si activó, se salta _update_trailing.
  - _resolve_close acepta hint_price snapshot antes del pop().
  - FIX CRÍTICO: risk.calc() llamada con argumentos en orden correcto:
    risk.calc(side=signal, entry=price, candles=candles_15m, score=score, symbol=symbol)
    Antes: risk.calc(price, signal, candles_15m) → side='long'/'short' como float,
    entry=precio como string → SL/TP completamente erróneos.
  - FIX CRÍTICO: _sync_sl_tp_from_orders filtro de positionSide permisivo:
    acepta órdenes con positionSide correcto O positionSide vacío (BingX
    a veces lo omite en órdenes condicionales).
  - FIX CALIDAD: get_closed_reason() se llama con side.
  - FIX CALIDAD: fill_entry sin sleep extra (open_order ya espera 0.5s).
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

BE_RR     = 1.0
BE_BUFFER = 0.0003

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


# ── Persistencia ─────────────────────────────────────────────────────────

def _save_positions(positions: dict) -> None:
    if not positions:
        try:
            if os.path.exists(POSITIONS_FILE):
                os.remove(POSITIONS_FILE)
        except Exception as e:
            log.warning("Error borrando positions.json: %s", e)
        return
    try:
        data = {}
        for sym, pos in positions.items():
            data[sym] = {k: v for k, v in pos.items() if k != "_extending"}
        with open(POSITIONS_FILE, "w") as f:
            json.dump(data, f)
    except Exception as e:
        log.warning("Error guardando positions.json: %s", e)


def _load_positions() -> dict:
    if not os.path.exists(POSITIONS_FILE):
        log.info("positions.json no encontrado — sincronización normal desde exchange")
        return {}
    try:
        with open(POSITIONS_FILE) as f:
            data = json.load(f)
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


# ── Sync SL/TP desde órdenes abiertas ───────────────────────────────────

def _sync_sl_tp_from_orders(symbol: str, pos: dict) -> None:
    """Rellena sl y tp desde las órdenes condicionales abiertas del exchange.

    BingX devuelve stopLossPrice/takeProfitPrice como None en get_all_positions()
    cuando el SL y TP son órdenes independientes (el caso habitual).

    FIX: el filtro de positionSide acepta también positionSide vacío (''),
    ya que BingX a veces lo omite en órdenes condicionales.
    """
    try:
        orders = exchange._get_open_orders(symbol)
    except Exception as e:
        log.warning("[%s] _sync_sl_tp_from_orders: error obteniendo órdenes: %s", symbol, e)
        return

    side          = pos.get("side", "")
    position_side = "LONG" if side == "long" else "SHORT"

    sl_price: float | None = None
    tp_price: float | None = None

    for order in orders:
        ps = order.get("positionSide", "").upper()
        # FIX: aceptar positionSide correcto O vacío (BingX a veces lo omite)
        if ps and ps != position_side:
            continue
        order_type = order.get("type", "")
        stop_price = float(order.get("stopPrice") or 0)
        if stop_price <= 0:
            continue
        if order_type == "STOP_MARKET" and sl_price is None:
            sl_price = stop_price
        elif order_type == "TAKE_PROFIT_MARKET" and tp_price is None:
            tp_price = stop_price

    updated = []
    if pos.get("sl") is None and sl_price is not None:
        pos["sl"] = sl_price
        updated.append(f"sl={sl_price:.6f}")
    if pos.get("tp") is None and tp_price is not None:
        pos["tp"] = tp_price
        updated.append(f"tp={tp_price:.6f}")

    if updated:
        log.info("[%s] SL/TP recuperados desde órdenes abiertas: %s", symbol, ", ".join(updated))
    else:
        log.warning(
            "[%s] _sync_sl_tp_from_orders: sin órdenes SL/TP "
            "(sl=%s tp=%s) — breakeven/trailing desactivados",
            symbol, pos.get("sl"), pos.get("tp"),
        )


# ── Breakeven ────────────────────────────────────────────────────────────

def _check_breakeven(symbol: str, pos: dict, current_price: float) -> bool:
    if pos.get("breakeven_set"):
        return False
    if pos.get("sl") is None or pos.get("entry") is None:
        return False

    side      = pos["side"]
    entry     = pos["entry"]
    sl        = pos["sl"]
    risk_dist = abs(entry - sl)

    if risk_dist == 0:
        return False

    if side == "long":
        target = entry + BE_RR * risk_dist
        if current_price < target:
            return False
        new_sl = round(entry * (1 + BE_BUFFER), 6)
        if new_sl <= sl:
            pos["breakeven_set"] = True
            return False
    else:
        target = entry - BE_RR * risk_dist
        if current_price > target:
            return False
        new_sl = round(entry * (1 - BE_BUFFER), 6)
        if new_sl >= sl:
            pos["breakeven_set"] = True
            return False

    log.info(
        "[%s] Breakeven activado | entry=%.4f SL: %.4f → %.4f (precio=%.4f)",
        symbol, entry, sl, new_sl, current_price,
    )

    try:
        exchange.cancel_all_orders(symbol)
        exchange.place_stop_order(symbol, side, pos["qty"], new_sl)
        if pos.get("tp") is not None:
            exchange.place_tp_order(symbol, side, pos["qty"], pos["tp"])
    except Exception as e:
        log.warning("[%s] Error colocando órdenes en breakeven: %s", symbol, e)
        return False

    pos["sl"]            = new_sl
    pos["breakeven_set"] = True

    telegram.notify(
        f"🔒 Breakeven activado\n"
        f"{symbol} {side.upper()}\n"
        f"SL movido a entrada: <code>{new_sl:.4f}</code>\n"
        f"(precio: <code>{current_price:.4f}</code> | +{BE_RR:.0f}R alcanzado)"
    )
    return True


# ── Trailing stop ─────────────────────────────────────────────────────────

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


# ── TP extension ──────────────────────────────────────────────────────────

def _sl_price_valid(side: str, sl_price: float, current_price: float) -> bool:
    if side == "short":
        return sl_price > current_price
    else:
        return sl_price < current_price


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

    entry        = pos["entry"]
    tp_orig_dist = abs(pos.get("tp_original", tp) - entry)
    if tp_orig_dist > 0:
        expected_next_tp_dist = tp_orig_dist * TP_EXTEND_RR * (extensions + 2)
        current_tp_dist       = abs(tp - entry)
        if current_tp_dist >= expected_next_tp_dist * 0.95:
            log.debug("[%s] TP ya extendido al nivel correcto — saltando", symbol)
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

    if side == "long":
        new_tp = round(entry + tp_orig_dist * TP_EXTEND_RR * (extensions + 2), 6)
        new_sl = round(entry * 1.0005, 6)
    else:
        new_tp = round(entry - tp_orig_dist * TP_EXTEND_RR * (extensions + 2), 6)
        new_sl = round(entry * 0.9995, 6)

    sl_valid = _sl_price_valid(side, new_sl, current_price)
    if not sl_valid:
        log.warning(
            "[%s] extend_tp: new_sl=%.6f no válido con precio=%.6f (%s) — "
            "extendiendo solo TP sin mover SL",
            symbol, new_sl, current_price, side,
        )
        try:
            exchange.cancel_all_orders(symbol)
            if pos.get("sl") is not None:
                exchange.place_stop_order(symbol, side, pos["qty"], pos["sl"])
            exchange.place_tp_order(symbol, side, pos["qty"], new_tp)
        except Exception as e:
            log.warning("[%s] Error extendiendo TP sin SL: %s", symbol, e)
            pos["_extending"] = False
            return
        old_tp = pos["tp"]
        pos["tp"]            = new_tp
        pos["tp_extensions"] = extensions + 1
        pos["trail_high"]    = current_price
        pos["trail_low"]     = current_price
        pos["_extending"]    = False
        pos["breakeven_set"] = True
        log.info("[%s] TP extendido #%d (sin mover SL) | %.6f → %.6f | score=%d",
                 symbol, extensions + 1, old_tp, new_tp, score)
        telegram.notify(
            f"📈 TP Extendido #{extensions + 1}\n"
            f"{symbol} {side.upper()}\n"
            f"TP anterior: <code>{old_tp:.6f}</code>\n"
            f"Nuevo TP: <code>{new_tp:.6f}</code>\n"
            f"⚠️ SL no movido (precio cerca del nivel BE)\n"
            f"Score: {score}"
        )
        return

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
    pos["breakeven_set"] = True

    log.info("[%s] TP extendido #%d | %.6f → %.6f | SL→BE=%.6f | score=%d",
             symbol, extensions + 1, old_tp, new_tp, new_sl, score)
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


def _resolve_close(pos: dict, symbol: str, hint_price: float | None = None) -> tuple[float, str]:
    """Determina precio y motivo de cierre. Pasa side para filtrar en Hedge mode."""
    reason, exit_price = exchange.get_closed_reason(symbol, side=pos.get("side"))
    if reason and exit_price:
        return exit_price, reason
    current_price = hint_price if hint_price is not None else exchange.get_price(symbol)
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
            exchange.set_leverage(symbol, config.LEVERAGE)
        except Exception as e:
            log.warning("No se pudo setear leverage en %s: %s", symbol, e)

    telegram.notify(
        f"🤖 Bot iniciado — {len(config.SYMBOLS)} pares | "
        f"{config.LEVERAGE}x | max {config.MAX_POSITIONS} posiciones"
    )

    feed = KlineFeed(config.SYMBOLS)
    feed.start()
    _wait_feed_ready(feed)

    positions: dict = _load_positions()
    tg_commands.start(get_positions_fn=lambda: positions, feed=feed)
    trade_logger.start_scheduler()

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
                    pos        = positions[symbol]
                    hint_price = exchange.get_price(symbol)
                    exit_price, reason = _resolve_close(pos, symbol, hint_price=hint_price)
                    pnl_pct = (
                        (exit_price - pos["entry"]) / pos["entry"]
                        if pos["side"] == "long"
                        else (pos["entry"] - exit_price) / pos["entry"]
                    ) * 100
                    duration_min = (time.time() - pos.get("open_ts", time.time())) / 60
                    pnl_usdt = pnl_pct / 100 * float(config.MARGIN_USDT) * config.LEVERAGE

                    log.info(
                        "[%s] Posición cerrada | %s | precio=%.4f | PnL=%.2f%% (%.2f USDT) | dur=%.0fmin",
                        symbol, reason, exit_price, pnl_pct, pnl_usdt, duration_min,
                    )
                    telegram.notify(
                        f"{'✅' if pnl_pct > 0 else '❌'} Posición cerrada\n"
                        f"{symbol} {pos['side'].upper()} ({reason})\n"
                        f"PnL: <code>{pnl_pct:+.2f}%</code> ({pnl_usdt:+.2f} USDT)\n"
                        f"Duración: {duration_min:.0f}min"
                    )
                    trade_logger.log_trade(
                        symbol=symbol,
                        side=pos["side"],
                        entry=pos["entry"],
                        exit_price=exit_price,
                        reason=reason,
                        score=pos.get("score", 0),
                        duration_min=duration_min,
                    )
                    bot_state.record_trade(pnl_usdt)
                    if reason == "SL":
                        _cooldown_sl[symbol] = time.time()
                    positions.pop(symbol)
                    _save_positions(positions)
                    continue

                # Posición activa — actualizar qty desde exchange
                pos = positions[symbol]
                pos["qty"] = pos_ex["size"]

                # Sincronizar entry con avgPrice real (corrige entrada guardada incorrectamente)
                if pos_ex.get("entry") and pos_ex["entry"] > 0:
                    old_entry = pos.get("entry", 0)
                    if old_entry and abs(old_entry - pos_ex["entry"]) / pos_ex["entry"] > 0.001:
                        log.info(
                            "[%s] Corrigiendo entry: %.6f → %.6f (diferencia >0.1%%)",
                            symbol, old_entry, pos_ex["entry"],
                        )
                    pos["entry"] = pos_ex["entry"]

                # Si sl o tp son None, intentar recuperarlos desde órdenes abiertas
                if pos.get("sl") is None or pos.get("tp") is None:
                    _sync_sl_tp_from_orders(symbol, pos)

                try:
                    current_price = exchange.get_price(symbol)
                except Exception as e:
                    log.warning("[%s] Error obteniendo precio: %s", symbol, e)
                    continue

                be_activated = _check_breakeven(symbol, pos, current_price)
                if not be_activated:
                    _update_trailing(symbol, pos, current_price)
                _check_tp_extension(symbol, pos, current_price, feed, effective_min_score)
                _save_positions(positions)

            # ── Señales nuevas ──────────────────────────────────────────────
            if _is_weekend():
                today = datetime.now(timezone.utc).weekday()
                if today != _weekend_notified_day:
                    _weekend_notified_day = today
                    log.info("Fin de semana — umbrales elevados (min_score=%d)", WEEKEND_MIN_SCORE)

            if bot_state.is_daily_loss_exceeded():
                log.warning("Límite de pérdida diaria alcanzado — sin nuevas señales")
                time.sleep(60)
                continue

            for symbol in config.SYMBOLS:
                if symbol in positions:
                    continue
                if len(positions) >= config.MAX_POSITIONS:
                    break

                now = time.time()
                if now - _cooldown.get(symbol, 0) < COOLDOWN:
                    continue
                if now - _cooldown_sl.get(symbol, 0) < COOLDOWN_SL:
                    continue
                if _corr_group_count(symbol, positions) >= config.MAX_CORR_PER_GROUP:
                    continue

                try:
                    candles_15m = feed.get(symbol, "15m")
                    candles_1h  = feed.get(symbol, "1h")
                    candles_4h  = feed.get(symbol, "4h") if feed.has_tf(symbol, "4h") else None
                except Exception:
                    continue

                is_manual = False
                manual_sig = tg_commands.pop_manual_signal(symbol)
                spread = 0.0

                if manual_sig:
                    signal, score = manual_sig, 80
                    is_manual = True
                    log.info("[%s] Señal MANUAL: %s", symbol, signal)
                else:
                    try:
                        signal, score = signals.evaluate(
                            candles_15m, candles_1h, candles_4h,
                            min_score=effective_min_score,
                        )
                    except Exception as e:
                        log.warning("[%s] Error evaluando señales: %s", symbol, e)
                        continue

                    if not signal:
                        continue

                    try:
                        spread = exchange.get_spread_pct(symbol)
                    except Exception:
                        spread = 0.0
                    if spread > config.MAX_SPREAD_PCT:
                        log.info("[%s] Spread alto (%.3f%%) — saltando", symbol, spread)
                        continue

                try:
                    price = exchange.get_price(symbol)
                except Exception as e:
                    log.warning("[%s] Error obteniendo precio para señal: %s", symbol, e)
                    continue

                # FIX CRÍTICO: firma correcta de risk.calc()
                # Antes: risk.calc(price, signal, candles_15m)  ← INCORRECTO
                # Ahora: risk.calc(side=signal, entry=price, ...)  ← CORRECTO
                try:
                    params = risk.calc(
                        side=signal,
                        entry=price,
                        candles=candles_15m,
                        score=score,
                        symbol=symbol,
                    )
                except ValueError as e:
                    log.info("[%s] risk.calc: %s — saltando", symbol, e)
                    continue
                except Exception as e:
                    log.warning("[%s] Error en risk.calc: %s", symbol, e)
                    continue

                log.info(
                    "[%s] SEÑAL %s | score=%d | spread=%.3f%% | "
                    "entry=%.4f sl=%.4f tp=%.4f qty=%.4f",
                    symbol, signal.upper(), score, spread,
                    price, params["sl"], params["tp"], params["qty"],
                )

                try:
                    exchange.open_order(
                        side=signal,
                        qty=params["qty"],
                        sl=params["sl"],
                        tp=params["tp"],
                        symbol=symbol,
                    )
                except Exception as e:
                    log.error("[%s] Error abriendo orden: %s", symbol, e)
                    continue

                # Obtener precio de llenado real (open_order ya esperó 0.5s internamente)
                fill_entry = price
                try:
                    pos_ex_new = exchange.get_position(symbol)
                    if pos_ex_new and pos_ex_new.get("entry") and pos_ex_new["entry"] > 0:
                        fill_entry = pos_ex_new["entry"]
                        log.info("[%s] Precio de llenado real: %.6f (señal: %.6f)",
                                 symbol, fill_entry, price)
                except Exception as e:
                    log.warning("[%s] No se pudo obtener fill price: %s — usando precio señal", symbol, e)

                trail_step = params.get("trail_step", 0)
                positions[symbol] = {
                    "side":          signal,
                    "entry":         fill_entry,
                    "qty":           params["qty"],
                    "sl":            params["sl"],
                    "tp":            params["tp"],
                    "tp_original":   params["tp"],
                    "tp_extensions": 0,
                    "trail_step":    trail_step,
                    "trail_high":    fill_entry,
                    "trail_low":     fill_entry,
                    "score":         score,
                    "open_ts":       time.time(),
                    "breakeven_set": False,
                    "_extending":    False,
                }
                _save_positions(positions)
                _cooldown[symbol] = time.time()

                telegram.notify(
                    f"🚀 Nueva posición\n"
                    f"{symbol} {signal.upper()} | Score: {score}\n"
                    f"Entry: <code>{fill_entry:.4f}</code>\n"
                    f"SL: <code>{params['sl']:.4f}</code>\n"
                    f"TP: <code>{params['tp']:.4f}</code>\n"
                    f"Qty: {params['qty']}"
                    + (f"\nSpread: {spread:.3f}%" if not is_manual else "")
                )

        except Exception as e:
            log.error("Error en loop principal: %s", e, exc_info=True)

        time.sleep(15)


if __name__ == "__main__":
    run()
