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

LOGLEVEL_STR = os.environ.get("LOGLEVEL", "INFO").upper()
LOGLEVEL = getattr(logging, LOGLEVEL_STR, logging.INFO)
logging.basicConfig(
    level=LOGLEVEL,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("main")

for noisy_logger in ("httpcore", "httpx", "websockets", "asyncio"):
    logging.getLogger(noisy_logger).setLevel(logging.WARNING)

_cooldown: dict[str, float] = {}
_cooldown_reason: dict[str, str] = {}
_manual_alert_cooldown: dict[str, float] = {}
_missing_count: dict[str, int] = {}
_trailing_notify_ts: dict[str, float] = {}
_opening: set[str] = set()

TRAILING_NOTIFY_DEBOUNCE = 5 * 60
COOLDOWN_SL = 60 * 60
COOLDOWN_SL_FAST = 15 * 60
COOLDOWN_TP = 30 * 60
MANUAL_ALERT_COOLDOWN = 60 * 60
MAX_TP_EXTENSIONS = 1
TP_EXTEND_THRESH = 0.010
MIN_HOLD_SECS = 90
SMART_COOLDOWN_FAST_WINDOW = 15 * 60
SMART_COOLDOWN_HIGH_SCORE = 85
SYNC_SCORE_UNKNOWN = 0
READY_TIMEOUT = 120
READY_MIN_PCT = 0.80
WEEKDAY_MIN_SCORE = int(getattr(config, "WEEKDAY_MIN_SCORE", 70))
WEEKEND_MIN_SCORE = int(getattr(config, "WEEKEND_MIN_SCORE", 90))
VALID_SIDES = {"long", "short"}
CLOSE_CONFIRM_LOOPS = 2
MAX_SAME_SIDE = int(getattr(config, "MAX_SAME_SIDE", 4))
GET_POSITIONS_ERROR_SLEEP = 10
weekend_notified_day = -1


def _is_weekend() -> bool:
    return datetime.now(timezone.utc).weekday() >= 5


def _cooldown_for(symbol: str) -> int:
    reason = _cooldown_reason.get(symbol, "sl")
    if reason == "tp":
        return COOLDOWN_TP
    if reason == "sl_fast":
        return COOLDOWN_SL_FAST
    return COOLDOWN_SL


def _corr_group_for(symbol: str) -> int | None:
    for idx, group in enumerate(config.CORR_GROUPS):
        if symbol in group:
            return idx
    return None


def _check_directional_guard(signal: str, positions: dict, symbol: str) -> bool:
    same_side_count = sum(1 for p in positions.values() if p["side"] == signal)
    if same_side_count >= MAX_SAME_SIDE:
        return False
    grp_idx = _corr_group_for(symbol)
    if grp_idx is not None:
        grp_count = sum(1 for sym, p in positions.items() if _corr_group_for(sym) == grp_idx)
        max_corr = getattr(config, "MAX_CORR_PER_GROUP", 2)
        if grp_count >= max_corr:
            return False
    return True


def _sync_entry_from_exchange(symbol: str, local_price: float, side: str) -> float:
    try:
        pos_live = exchange.get_position(symbol)
        if pos_live and pos_live.get("side") == side:
            real_entry = float(pos_live.get("entry") or 0.0)
            if real_entry > 0:
                return real_entry
    except Exception:
        pass
    return local_price


def _apply_breakeven(symbol: str, pos: dict, current_price: float) -> None:
    if time.time() - pos.get("open_ts", 0) < MIN_HOLD_SECS:
        return
    activated = risk.check_breakeven(symbol, pos, current_price)
    if not activated:
        return
    new_sl = pos["sl"]
    side = pos["side"]
    try:
        exchange.cancel_trigger_orders(symbol)
        exchange.place_stop_order(symbol, side, pos["qty"], new_sl)
        exchange.place_tp_order(symbol, side, pos["qty"], pos["tp"])
        telegram.notify(
            f"🔒 Break-even activado\n{symbol} {side.upper()}\nSL movido a {new_sl:.6f}"
        )
    except Exception as e:
        log.warning("[%s] Error actualizando SL break-even en exchange: %s", symbol, e)
        pos["be_locked"] = False


def _update_trailing(symbol: str, pos: dict, current_price: float) -> None:
    if time.time() - pos.get("open_ts", 0) < MIN_HOLD_SECS:
        return
    side = pos["side"]
    trail_step = pos.get("trail_step", 0)
    if trail_step <= 0 or pos.get("sl") is None or pos.get("tp") is None:
        return

    score = pos.get("score", 0)
    trailing_mult = 1.8 if score >= 85 else 1.5

    if side == "long":
        peak = pos.get("trail_high", pos["entry"])
        if current_price <= peak + trail_step:
            return
        pos["trail_high"] = current_price
        coin = exchange._hl_symbol(symbol)
        new_sl = exchange._round_price(coin, current_price - trailing_mult * trail_step)
        if new_sl <= pos["sl"]:
            return
    else:
        trough = pos.get("trail_low", pos["entry"])
        if current_price >= trough - trail_step:
            return
        pos["trail_low"] = current_price
        coin = exchange._hl_symbol(symbol)
        new_sl = exchange._round_price(coin, current_price + trailing_mult * trail_step)
        if new_sl >= pos["sl"]:
            return

    pos["sl"] = new_sl
    try:
        exchange.cancel_trigger_orders(symbol)
        exchange.place_stop_order(symbol, side, pos["qty"], new_sl)
        exchange.place_tp_order(symbol, side, pos["qty"], pos["tp"])
    except Exception as e:
        log.warning("[%s] Error actualizando trailing SL: %s", symbol, e)
        return

    now = time.time()
    if now - _trailing_notify_ts.get(symbol, 0) >= TRAILING_NOTIFY_DEBOUNCE:
        _trailing_notify_ts[symbol] = now
        telegram.notify_trailing(
            symbol=symbol,
            side=side,
            entry=pos["entry"],
            current_price=current_price,
            new_sl=new_sl,
            tp=pos["tp"],
        )


def _check_tp_extension(symbol: str, pos: dict, current_price: float, feed, effective_min_score: int) -> None:
    if time.time() - pos.get("open_ts", 0) < MIN_HOLD_SECS:
        return
    extensions = pos.get("tp_extensions", 0)
    if extensions >= MAX_TP_EXTENSIONS:
        return
    if pos.get("_extending"):
        return

    tp = pos.get("tp")
    side = pos["side"]
    score_open = pos.get("score", 0)
    if tp is None or score_open < 85:
        return

    near_tp = current_price >= tp * (1 - TP_EXTEND_THRESH) if side == "long" else current_price <= tp * (1 + TP_EXTEND_THRESH)
    if not near_tp:
        return

    pos["_extending"] = True
    try:
        candles_15m = feed.get(symbol, "15m")
        candles_1h = feed.get(symbol, "1h")
        candles_4h = feed.get(symbol, "4h") if feed.has_tf(symbol, "4h") else None
        signal, score_now, regime = signals.evaluate(
            candles_15m,
            candles_1h,
            candles_4h,
            min_score=effective_min_score,
            symbol=symbol,
        )
        if not signal or signal != side or score_now < max(effective_min_score + 10, 85):
            return
        if regime in ("proto_bear", "proto_bull"):
            return

        pos_live = exchange.get_position(symbol)
        if pos_live is None or pos_live.get("side") != side:
            return

        entry = pos["entry"]
        tp_orig = pos.get("tp_original", tp)
        tp_orig_dist = abs(tp_orig - entry)
        coin = exchange._hl_symbol(symbol)
        extension_factor = 1.5
        if side == "long":
            new_tp = exchange._round_price(coin, tp + tp_orig_dist * (extension_factor - 1.0))
            if new_tp <= current_price * 1.003:
                return
        else:
            new_tp = exchange._round_price(coin, tp - tp_orig_dist * (extension_factor - 1.0))
            if new_tp >= current_price * 0.997:
                return

        current_sl = pos["sl"]
        exchange.cancel_trigger_orders(symbol)
        exchange.place_stop_order(symbol, side, pos["qty"], current_sl)
        exchange.place_tp_order(symbol, side, pos["qty"], new_tp)
        pos["tp"] = new_tp
        pos["tp_extensions"] = extensions + 1
        pos["trail_high"] = current_price
        pos["trail_low"] = current_price
        telegram.notify(
            f"📈 TP extendido #{extensions + 1}\n{symbol} {side.upper()}\nNuevo TP: {new_tp:.6f}\nScore actual: {score_now}"
        )
    except Exception as e:
        log.warning("[%s] Error en extend_tp: %s", symbol, e)
    finally:
        pos["_extending"] = False


def _wait_feed_ready(feed: KlineFeed) -> None:
    total = len(config.SYMBOLS)
    needed = max(1, int(total * READY_MIN_PCT))
    deadline = time.time() + READY_TIMEOUT
    while time.time() < deadline:
        ready = feed.ready_count()
        if ready >= needed:
            return
        time.sleep(2)


def _exit_price_for(pos: dict, current_price: float) -> tuple[float, str]:
    side = pos["side"]
    tp = pos.get("tp")
    sl = pos.get("sl")
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
    return current_price, "MANUAL"


def _get_real_exit_price(symbol: str, pos: dict, fallback: float, fallback_reason: str) -> tuple[float, str]:
    side = pos.get("side", "long")
    entry = pos.get("entry", 0.0)
    open_ts = pos.get("open_ts", 0.0)
    open_ms = int(open_ts * 1000)
    hl_close_dir = "Close Long" if side == "long" else "Close Short"
    try:
        closed_orders = exchange.get_closed_orders(symbol, limit=20)
    except Exception:
        return fallback, fallback_reason
    for order in closed_orders:
        if order.get("dir") != hl_close_dir:
            continue
        order_time = int(order.get("time") or 0)
        if order_time > 0 and order_time < open_ms:
            continue
        real_price = float(order.get("px") or order.get("price") or 0)
        if real_price <= 0:
            continue
        if entry > 0 and abs(real_price - entry) / entry > 0.30:
            continue
        order_type = str(order.get("order_type") or order.get("type") or "").upper()
        if "TAKE_PROFIT" in order_type or "TP" in order_type:
            return real_price, "TP"
        if "STOP" in order_type or "SL" in order_type:
            return real_price, "SL"
        closed_pnl = float(order.get("closedPnl") or 0)
        return real_price, "TP" if closed_pnl >= 0 else "SL"
    return fallback, fallback_reason


def _calc_pnl(side: str, entry: float, exit_price: float, qty: float) -> tuple[float, float]:
    price_move = (exit_price - entry) / entry if side == "long" else (entry - exit_price) / entry
    pnl_pct = price_move * config.LEVERAGE * 100
    pnl_usdt = price_move * qty * entry
    return pnl_pct, pnl_usdt


def _declare_closed(symbol: str, p: dict, positions: dict) -> None:
    current_price = exchange.get_price(symbol)
    exit_price_est, reason_est = _exit_price_for(p, current_price)
    exit_price, reason = _get_real_exit_price(symbol, p, exit_price_est, reason_est)
    pnl_pct, pnl_usdt = _calc_pnl(p["side"], p["entry"], exit_price, p["qty"])

    if reason == "SL":
        hold_secs = time.time() - p.get("open_ts", 0)
        trade_score = p.get("score", 0)
        _cooldown_reason[symbol] = "sl_fast" if hold_secs <= SMART_COOLDOWN_FAST_WINDOW and trade_score >= SMART_COOLDOWN_HIGH_SCORE else "sl"
    else:
        _cooldown_reason[symbol] = "tp"
    _cooldown[symbol] = time.time()
    _missing_count.pop(symbol, None)
    _trailing_notify_ts.pop(symbol, None)

    limit_hit = bot_state.record_trade(pnl_usdt)
    daily_pnl = bot_state.get_daily_pnl()
    trade_logger.record(
        symbol=symbol,
        side=p["side"],
        entry=p["entry"],
        exit_price=exit_price,
        pnl_pct=pnl_pct,
        pnl_usdt=pnl_usdt,
        score=p.get("score", 0),
        reason=reason,
        open_ts=p.get("open_ts", time.time()),
    )
    telegram.notify_close(
        symbol=symbol,
        side=p["side"],
        entry=p["entry"],
        exit_p=exit_price,
        pnl_pct=pnl_pct,
        pnl_usdt=pnl_usdt,
        reason=reason,
        open_ts=p.get("open_ts", 0.0),
        daily_pnl=daily_pnl,
    )
    if limit_hit:
        telegram.notify("🛑 Límite de pérdidas diario alcanzado. Bot pausado hasta 00:00 UTC.")


def _calc_trail_step_from_atr(symbol: str, feed, sl: float | None, entry: float, score: int = 0) -> float:
    mult = risk.HIGH_SCORE_TRAIL_STEP_MULT if score >= 85 else risk.TRAIL_STEP_MULT
    try:
        candles_15m = feed.get(symbol, "15m")
        if candles_15m and len(candles_15m) >= 15:
            trs = []
            for i in range(1, len(candles_15m)):
                h = candles_15m[i]["high"]
                l = candles_15m[i]["low"]
                pc = candles_15m[i - 1]["close"]
                trs.append(max(h - l, abs(h - pc), abs(l - pc)))
            atr = sum(trs[-14:]) / min(14, len(trs))
            return round(max(mult * atr, atr * 0.05), 8)
    except Exception:
        pass
    if sl is not None and entry > 0:
        sl_dist = abs(entry - sl)
        if sl_dist > 0:
            return round(mult * sl_dist, 8)
    return 0.0


def _calc_be_levels_from_atr(symbol: str, feed, side: str, entry: float, score: int = 0) -> tuple[float | None, float | None]:
    be_buffer_mult = risk.HIGH_SCORE_BE_BUFFER_MULT if score >= 85 else risk.BE_BUFFER_MULT
    try:
        candles_15m = feed.get(symbol, "15m")
        if candles_15m and len(candles_15m) >= 15:
            trs = []
            for i in range(1, len(candles_15m)):
                h = candles_15m[i]["high"]
                l = candles_15m[i]["low"]
                pc = candles_15m[i - 1]["close"]
                trs.append(max(h - l, abs(h - pc), abs(l - pc)))
            atr = sum(trs[-14:]) / min(14, len(trs))
            if atr > 0 and entry > 0:
                be_trigger = (entry + risk.BE_ATR_MULT * atr) if side == "long" else (entry - risk.BE_ATR_MULT * atr)
                be_sl = (entry + be_buffer_mult * atr) if side == "long" else (entry - be_buffer_mult * atr)
                return round(be_trigger, 8), round(be_sl, 8)
    except Exception:
        pass
    return None, None


def _get_position_open_ts(symbol: str, pos_ex: dict) -> float:
    try:
        side = pos_ex.get("side", "long")
        hl_open_dir = "Open Long" if side == "long" else "Open Short"
        closed = exchange.get_closed_orders(symbol, limit=20)
        for order in closed:
            if order.get("dir") != hl_open_dir:
                continue
            ts_ms = int(order.get("time") or 0)
            if ts_ms > 0:
                return ts_ms / 1000.0
    except Exception:
        pass
    return time.time()


def _restore_sl_tp_on_sync(symbol: str, pos: dict, feed) -> None:
    side = pos["side"]
    entry = pos.get("entry", 0.0)
    qty = pos.get("qty", 0.0)
    if entry <= 0 or qty <= 0:
        return
    try:
        triggers = exchange.get_open_trigger_orders(symbol)
    except Exception:
        triggers = []
    hl_sl_px = None
    hl_tp_px = None
    for t in triggers:
        ot = str(t.get("orderType", "")).lower()
        px = float(t.get("triggerPx") or 0)
        if px <= 0:
            continue
        if "stop" in ot:
            hl_sl_px = px
        elif "take profit" in ot or "tp" in ot:
            hl_tp_px = px
    if hl_sl_px is not None and hl_tp_px is not None:
        pos["sl"] = hl_sl_px
        pos["tp"] = hl_tp_px
        pos["tp_original"] = pos.get("tp_original") or hl_tp_px
        return
    try:
        candles_15m = feed.get(symbol, "15m")
        candles_1h = feed.get(symbol, "1h")
        if not candles_15m or not candles_1h:
            return
        params = risk.calc(side, entry, candles_15m, score=SYNC_SCORE_UNKNOWN, symbol=symbol, regime=side, candles_1h=candles_1h)
        new_sl = hl_sl_px if hl_sl_px is not None else params["sl"]
        new_tp = hl_tp_px if hl_tp_px is not None else params["tp"]
        exchange.cancel_trigger_orders(symbol)
        exchange._place_sl_tp_bulk(symbol, side, qty, new_sl, new_tp)
        pos["sl"] = new_sl
        pos["tp"] = new_tp
        pos["tp_original"] = pos.get("tp_original") or new_tp
        if pos.get("be_trigger") is None:
            pos["be_trigger"] = params.get("be_trigger")
            pos["be_sl"] = params.get("be_sl")
        if not pos.get("trail_step"):
            pos["trail_step"] = params.get("trail_step", 0.0)
    except Exception as exc:
        log.warning("[%s] restore SL/TP falló: %s", symbol, exc)


def _open_position(symbol: str, signal: str, score: int, regime: str, price: float, candles_15m: list, candles_1h: list, positions: dict) -> None:
    if symbol in _opening:
        return
    _opening.add(symbol)
    try:
        try:
            pos_already = exchange.get_position(symbol)
            if pos_already and pos_already.get("side") in VALID_SIDES:
                return
        except Exception:
            pass

        params = risk.calc(signal, price, candles_15m, score=score, symbol=symbol, regime=regime, candles_1h=candles_1h)
        exchange.open_order(side=signal, qty=params["qty"], sl=params["sl"], tp=params["tp"], symbol=symbol)
        real_entry = _sync_entry_from_exchange(symbol, price, signal)
        positions[symbol] = {
            "side": signal,
            "entry": real_entry,
            "qty": params["qty"],
            "sl": params["sl"],
            "tp": params["tp"],
            "tp_original": params["tp"],
            "tp_extensions": 0,
            "_extending": False,
            "trail_step": params["trail_step"],
            "trail_high": real_entry,
            "trail_low": real_entry,
            "score": score,
            "open_ts": time.time(),
            "be_trigger": params.get("be_trigger"),
            "be_sl": params.get("be_sl"),
            "be_locked": False,
        }
        telegram.notify_open(symbol=symbol, price=real_entry, side=signal, qty=params["qty"], sl=params["sl"], tp=params["tp"], score=score, tp_rr=params["tp_rr"], regime=regime)
    except Exception as e:
        log.error("[%s] Error abriendo posición: %s", symbol, e)
    finally:
        _opening.discard(symbol)


def run() -> None:
    global weekend_notified_day
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
            if bot_state.reset_daily_if_new_day():
                telegram.notify("🌅 Nuevo día UTC. Límite diario reseteado.")

            weekend = _is_weekend()
            effective_min_score = WEEKEND_MIN_SCORE if weekend else WEEKDAY_MIN_SCORE

            try:
                all_ex_positions = exchange.get_all_positions()
            except Exception as pos_err:
                log.warning("get_all_positions falló: %s", pos_err)
                time.sleep(GET_POSITIONS_ERROR_SLEEP)
                continue

            for symbol in list(positions.keys()):
                pos_ex = all_ex_positions.get(symbol)
                if pos_ex:
                    _missing_count.pop(symbol, None)
                    continue
                _missing_count[symbol] = _missing_count.get(symbol, 0) + 1
                if _missing_count[symbol] < CLOSE_CONFIRM_LOOPS:
                    continue
                p = positions.pop(symbol)
                _declare_closed(symbol, p, positions)

            expired = [sym for sym, ts in _cooldown.items() if time.time() - ts >= _cooldown_for(sym)]
            for sym in expired:
                _cooldown.pop(sym, None)
                _cooldown_reason.pop(sym, None)

            expired_alerts = [sym for sym, ts in _manual_alert_cooldown.items() if time.time() - ts >= MANUAL_ALERT_COOLDOWN]
            for sym in expired_alerts:
                _manual_alert_cooldown.pop(sym, None)

            for symbol, pos_ex in all_ex_positions.items():
                if symbol in positions:
                    continue
                ex_side = pos_ex.get("side")
                if ex_side not in VALID_SIDES:
                    continue
                synced_sl = pos_ex.get("sl")
                synced_entry = pos_ex["entry"]
                real_open_ts = _get_position_open_ts(symbol, pos_ex)
                be_trigger, be_sl = _calc_be_levels_from_atr(symbol, feed, ex_side, synced_entry)
                positions[symbol] = {
                    "side": ex_side,
                    "entry": synced_entry,
                    "qty": pos_ex["size"],
                    "sl": synced_sl,
                    "tp": pos_ex.get("tp"),
                    "tp_original": pos_ex.get("tp"),
                    "tp_extensions": 0,
                    "_extending": False,
                    "trail_step": _calc_trail_step_from_atr(symbol, feed, synced_sl, synced_entry),
                    "trail_high": synced_entry,
                    "trail_low": synced_entry,
                    "score": SYNC_SCORE_UNKNOWN,
                    "open_ts": real_open_ts,
                    "be_trigger": be_trigger,
                    "be_sl": be_sl,
                    "be_locked": False,
                }
                _restore_sl_tp_on_sync(symbol, positions[symbol], feed)

            for symbol, pos in list(positions.items()):
                try:
                    price = exchange.get_price(symbol)
                    _apply_breakeven(symbol, pos, price)
                    _update_trailing(symbol, pos, price)
                    _check_tp_extension(symbol, pos, price, feed, effective_min_score)
                except Exception as e:
                    log.warning("[%s] Error gestión posición: %s", symbol, e)

            if not bot_state.is_paused() and not bot_state.is_daily_limit_hit():
                for symbol in list(config.SYMBOLS):
                    manual_side = tg_commands.pop_manual_signal(symbol)
                    if not manual_side:
                        continue
                    if symbol in positions or symbol in _cooldown or not feed.ready(symbol):
                        continue
                    if not _check_directional_guard(manual_side, positions, symbol):
                        continue
                    candles_15m = feed.get(symbol, "15m")
                    candles_1h = feed.get(symbol, "1h")
                    price = exchange.get_price(symbol)
                    regime = "bull" if manual_side == "long" else "bear"
                    _open_position(symbol, manual_side, 100, regime, price, candles_15m, candles_1h, positions)

                for symbol in config.SYMBOLS:
                    if symbol in positions or symbol in _cooldown or not feed.ready(symbol):
                        continue
                    is_manual = symbol in config.MANUAL_ALERT_SYMBOLS
                    if is_manual and symbol in _manual_alert_cooldown:
                        continue
                    if not is_manual and len(positions) >= config.MAX_POSITIONS:
                        break
                    try:
                        candles_15m = feed.get(symbol, "15m")
                        candles_1h = feed.get(symbol, "1h")
                        candles_4h = feed.get(symbol, "4h") if feed.has_tf(symbol, "4h") else None
                        signal, score, regime = signals.evaluate(candles_15m, candles_1h, candles_4h, min_score=effective_min_score, symbol=symbol)
                        if not signal or signal not in VALID_SIDES:
                            continue
                        if not regime:
                            regime = "bull" if signal == "long" else "bear"
                        if not is_manual and not _check_directional_guard(signal, positions, symbol):
                            continue
                        price = exchange.get_price(symbol)
                        if is_manual:
                            _manual_alert_cooldown[symbol] = time.time()
                            params = risk.calc(signal, price, candles_15m, score=score, symbol=symbol, regime=regime, candles_1h=candles_1h)
                            telegram.notify(
                                f"🚨 Alerta manual {symbol}\nDirección: {signal.upper()}\nPrecio: {price:.6f}\nSL: {params['sl']:.6f}\nTP: {params['tp']:.6f}\nScore: {score}"
                            )
                            continue
                        _open_position(symbol, signal, score, regime, price, candles_15m, candles_1h, positions)
                    except Exception as e:
                        log.error("[%s] Error evaluando señal: %s", symbol, e)

            if weekend:
                today = datetime.now(timezone.utc).weekday()
                if today != weekend_notified_day:
                    weekend_notified_day = today
                    telegram.notify(f"🪫 Modo fin de semana activo. Score mínimo: {WEEKEND_MIN_SCORE}.")
        except Exception as e:
            log.error("Error en loop principal: %s", e, exc_info=True)
            telegram.notify(f"⚠️ Error en bot: {e}")
        time.sleep(config.LOOP_SLEEP)


if __name__ == "__main__":
    run()
