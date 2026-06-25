"""main.py — Loop principal con trailing stop y TP dinámico (extend_tp)."""
import logging
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

_cooldown: dict[str, float] = {}
_cooldown_reason: dict[str, str] = {}   # 'tp' | 'sl'
_manual_alert_cooldown: dict[str, float] = {}

COOLDOWN_SL           = 60 * 60
COOLDOWN_TP           = 15 * 60
MANUAL_ALERT_COOLDOWN  = 60 * 60
MAX_TP_EXTENSIONS      = 3
TP_EXTEND_RR           = 1.5
TP_EXTEND_THRESH       = 0.015
MIN_HOLD_SECS          = 300

READY_TIMEOUT = 120
READY_MIN_PCT = 0.80

WEEKDAY_MIN_SCORE = 70
WEEKEND_MIN_SCORE = 90

VALID_SIDES = {"long", "short"}

CLOSE_CONFIRM_LOOPS = 2
_missing_count: dict[str, int] = {}

_weekend_notified_day: int = -1


def _is_weekend() -> bool:
    return datetime.now(timezone.utc).weekday() >= 5


def _cooldown_for(symbol: str) -> int:
    reason = _cooldown_reason.get(symbol, "sl")
    return COOLDOWN_TP if reason == "tp" else COOLDOWN_SL


def _update_trailing(symbol: str, pos: dict, current_price: float) -> None:
    if time.time() - pos.get("open_ts", 0) < MIN_HOLD_SECS:
        return

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
                    telegram.notify(f"\U0001f53c Trailing SL movido\n{symbol} LONG\nNuevo SL: <code>{new_sl:.6f}</code>")
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
                    telegram.notify(f"\U0001f53d Trailing SL movido\n{symbol} SHORT\nNuevo SL: <code>{new_sl:.6f}</code>")
                except Exception as e:
                    log.warning("[%s] Error actualizando trailing SL: %s", symbol, e)


def _check_tp_extension(
    symbol: str,
    pos: dict,
    current_price: float,
    feed,
    effective_min_score: int,
) -> None:
    if time.time() - pos.get("open_ts", 0) < MIN_HOLD_SECS:
        return

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
        # ── 1. Evaluar señal ────────────────────────────────────────────
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
            return

        if not signal or signal != side:
            log.info(
                "[%s] Señal no válida para extend_tp (signal=%s) — dejando TP actual",
                symbol, signal,
            )
            return

        # ── 2. Verificar que la posición sigue abierta en BingX ────────
        # Race condition: el precio puede haber tocado el TP_original justo
        # entre que detectamos near_tp y ahora. Si la posición ya cerró,
        # cancel_all_orders + place_* fallarían silenciosamente dejando el
        # estado del bot desincronizado.
        try:
            pos_live = exchange.get_position(symbol)
        except Exception as e:
            log.warning("[%s] No se pudo verificar posición antes de extend_tp: %s", symbol, e)
            return

        if pos_live is None:
            log.info(
                "[%s] Posición ya cerrada en BingX — extend_tp cancelado (TP original ejecutado)",
                symbol,
            )
            return

        # Confirmar que el side sigue siendo el mismo (protección extra)
        if pos_live.get("side") != side:
            log.warning(
                "[%s] Side en exchange (%s) difiere del local (%s) — extend_tp cancelado",
                symbol, pos_live.get("side"), side,
            )
            return

        # ── 3. Calcular nuevo TP ────────────────────────────────────────
        entry        = pos["entry"]
        tp_orig_dist = abs(pos.get("tp_original", tp) - entry)

        if side == "long":
            new_tp = round(entry + tp_orig_dist * TP_EXTEND_RR * (extensions + 2), 6)
        else:
            new_tp = round(entry - tp_orig_dist * TP_EXTEND_RR * (extensions + 2), 6)

        current_sl = pos["sl"]

        # ── 4. Recolocar órdenes ────────────────────────────────────────
        try:
            exchange.cancel_all_orders(symbol)
            exchange.place_stop_order(symbol, side, pos["qty"], current_sl)
            exchange.place_tp_order(symbol, side, pos["qty"], new_tp)
        except Exception as e:
            log.warning("[%s] Error colocando órdenes en extend_tp: %s", symbol, e)
            return

        # ── 5. Actualizar estado local ──────────────────────────────────
        old_tp = pos["tp"]
        pos["tp"]            = new_tp
        pos["tp_extensions"] = extensions + 1
        pos["trail_high"]    = current_price
        pos["trail_low"]     = current_price

        log.info(
            "[%s] TP extendido #%d | old_tp=%.6f → new_tp=%.6f | SL=%.6f | score=%d",
            symbol, extensions + 1, old_tp, new_tp, current_sl, score,
        )
        telegram.notify(
            f"\U0001f4c8 TP Extendido #{extensions + 1}\n"
            f"{symbol} {side.upper()}\n"
            f"TP anterior: <code>{old_tp:.6f}</code>\n"
            f"Nuevo TP: <code>{new_tp:.6f}</code>\n"
            f"SL sin cambios: <code>{current_sl:.6f}</code>\n"
            f"Score: {score}"
        )

    finally:
        # Siempre liberar el lock, incluso si hubo return anticipado o excepción
        pos["_extending"] = False


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
    """Estima precio y razón de cierre a partir del precio actual y los niveles guardados."""
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
    elif tp is not None:
        hit_tp = (
            (side == "long"  and current_price >= tp * 0.995) or
            (side == "short" and current_price <= tp * 1.005)
        )
        if hit_tp:
            return tp, "TP"
    elif sl is not None:
        hit_sl = (
            (side == "long"  and current_price <= sl * 1.005) or
            (side == "short" and current_price >= sl * 0.995)
        )
        if hit_sl:
            return sl, "SL"

    entry = pos.get("entry", 0)
    if entry > 0:
        move_pct = (
            (current_price - entry) / entry if side == "long"
            else (entry - current_price) / entry
        )
        if move_pct > 0.005:
            return current_price, "TP"
        if move_pct < -0.003:
            return current_price, "SL"

    return current_price, "MANUAL"


def _get_real_exit_price(
    symbol: str,
    pos: dict,
    fallback: float,
    fallback_reason: str,
) -> tuple[float, str]:
    """Busca el precio real de cierre en el historial de órdenes del exchange.

    FIX: filtra órdenes por:
      - Lado correcto (SELL para LONG, BUY para SHORT) — evita coger la apertura
      - Tipo de cierre (STOP_MARKET o TAKE_PROFIT_MARKET) — evita órdenes MARKET
      - Tiempo posterior a la apertura de la posición — evita órdenes de trades anteriores
      - Precio plausible (dentro del ±30% de entry) — filtro de cordura final
    """
    side     = pos.get("side", "long")
    entry    = pos.get("entry", 0.0)
    open_ts  = pos.get("open_ts", 0.0)        # segundos UNIX
    open_ms  = int(open_ts * 1000)             # BingX usa ms

    # La orden de cierre va en dirección contraria a la posición
    close_bx_side = "SELL" if side == "long" else "BUY"

    # Tipos que BingX usa para SL y TP
    CLOSE_TYPES = {"STOP_MARKET", "TAKE_PROFIT_MARKET"}

    try:
        closed_orders = exchange.get_closed_orders(symbol, limit=20)
    except Exception as exc:
        log.debug("[%s] get_closed_orders falló: %s", symbol, exc)
        return fallback, fallback_reason

    for order in closed_orders:
        # 1. Filtro por lado
        if str(order.get("side", "")).upper() != close_bx_side:
            continue

        # 2. Filtro por tipo (solo órdenes de cierre)
        order_type = str(order.get("type", "")).upper()
        if order_type not in CLOSE_TYPES:
            continue

        # 3. Filtro por tiempo (debe ser posterior a la apertura)
        order_time = int(order.get("time") or order.get("updateTime") or 0)
        if order_time > 0 and order_time < open_ms:
            continue

        # 4. Precio plausible (±30% de entrada — cortafuegos de datos corruptos)
        real_price = float(order.get("avgPrice") or order.get("price") or 0)
        if real_price <= 0:
            continue
        if entry > 0 and abs(real_price - entry) / entry > 0.30:
            log.warning(
                "[%s] Orden descartada — precio %.6f fuera de rango razonable (entry=%.6f)",
                symbol, real_price, entry,
            )
            continue

        # Orden válida encontrada
        if "TAKE_PROFIT" in order_type:
            return real_price, "TP"
        if "STOP" in order_type:
            return real_price, "SL"

    log.debug("[%s] Sin orden de cierre válida en historial — usando fallback %.6f", symbol, fallback)
    return fallback, fallback_reason


def _calc_pnl(side: str, entry: float, exit_price: float, qty: float) -> tuple[float, float]:
    """Calcula PnL en % (con leverage) y en USDT (absoluto).

    FIX: la fórmula anterior calculaba pnl_usdt dividiendo por leverage dos veces.
    Correcto:
      pnl_pct  = movimiento_precio% * leverage
      pnl_usdt = movimiento_precio * qty   (qty ya es en contratos/monedas base)

    Ejemplo SHORT: entry=0.182, exit=0.184 (SL), qty=1089, leverage=5
      movimiento = (0.182 - 0.184) / 0.182 = -1.099%
      pnl_pct    = -1.099 * 5 = -5.49%
      pnl_usdt   = (0.182 - 0.184) * 1089 = -2.178 USDT
    """
    if side == "long":
        price_move = (exit_price - entry) / entry
    else:
        price_move = (entry - exit_price) / entry

    pnl_pct  = price_move * config.LEVERAGE * 100
    pnl_usdt = price_move * qty * entry          # qty * entry = valor nocional sin apalancamiento
    return pnl_pct, pnl_usdt


def _declare_closed(symbol: str, p: dict, positions: dict) -> None:
    """Registra el cierre de una posición: PnL, cooldown, logger y Telegram."""
    current_price          = exchange.get_price(symbol)
    exit_price_est, reason_est = _exit_price_for(p, current_price)
    exit_price, reason     = _get_real_exit_price(symbol, p, exit_price_est, reason_est)

    pnl_pct, pnl_usdt = _calc_pnl(p["side"], p["entry"], exit_price, p["qty"])

    log.info(
        "[%s] Cierre detectado | side=%s entry=%.6f exit=%.6f reason=%s "
        "pnl_pct=%+.2f%% pnl_usdt=%+.4f USDT",
        symbol, p["side"], p["entry"], exit_price, reason, pnl_pct, pnl_usdt,
    )

    # Cooldown
    _cooldown[symbol]        = time.time()
    _cooldown_reason[symbol] = "tp" if reason == "TP" else "sl"
    cd_mins = (COOLDOWN_TP if reason == "TP" else COOLDOWN_SL) // 60
    log.info("[%s] Cooldown %dm activado tras %s", symbol, cd_mins, reason)

    _missing_count.pop(symbol, None)

    # Registrar PnL en bot_state (fuente única de verdad)
    limit_hit = bot_state.record_trade(pnl_usdt)
    daily_pnl = bot_state.get_daily_pnl()
    capital   = config.MARGIN_USDT * config.MAX_POSITIONS
    daily_pct = (daily_pnl / capital * 100) if capital else 0.0
    daily_max = float(getattr(config, "DAILY_MAX_LOSS_PCT", -3.0))

    log.info(
        "[drawdown] PnL acum. hoy: %+.2f USDT (%+.2f%% de ~%.0f USDT capital)",
        daily_pnl, daily_pct, capital,
    )

    if limit_hit:
        msg = (
            f"\U0001f6d1 <b>L\u00edmite de p\u00e9rdidas diario alcanzado</b>\n"
            f"PnL hoy: <code>{daily_pnl:+.2f} USDT</code> ({daily_pct:+.2f}%)\n"
            f"Umbral: {daily_max}% — bot pausado hasta las 00:00 UTC.\n"
            f"Las posiciones abiertas siguen gestion\u00e1ndose (trailing/TP)."
        )
        log.warning("[drawdown] %s", msg.replace("\n", " "))
        telegram.notify(msg)

    # Persistir en CSV y _cache
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

    # Notificación Telegram del cierre
    telegram.notify_close(
        symbol    = symbol,
        side      = p["side"],
        entry     = p["entry"],
        exit_p    = exit_price,
        pnl_pct   = pnl_pct,
        pnl_usdt  = pnl_usdt,
        reason    = reason,
        open_ts   = p.get("open_ts", 0.0),
        daily_pnl = daily_pnl,
    )


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
        f"\U0001f916 Bot iniciado — {len(config.SYMBOLS)} pares | "
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

            # Nuevo día UTC → resetear drawdown y reactivar bot
            if bot_state.reset_daily_if_new_day():
                log.info("[drawdown] Nuevo día UTC — bot reactivado")
                telegram.notify("\U0001f305 Nuevo día UTC — límite de pérdidas reseteado. Bot activo.")

            weekend = _is_weekend()
            effective_min_score = WEEKEND_MIN_SCORE if weekend else WEEKDAY_MIN_SCORE

            all_ex_positions = exchange.get_all_positions()

            # Sync posiciones con tolerancia de cierre
            for symbol in list(positions.keys()):
                pos_ex = all_ex_positions.get(symbol)
                if pos_ex:
                    _missing_count.pop(symbol, None)
                    continue

                _missing_count[symbol] = _missing_count.get(symbol, 0) + 1
                absent = _missing_count[symbol]

                if absent < CLOSE_CONFIRM_LOOPS:
                    log.debug(
                        "[%s] No vista en exchange (intento %d/%d) — esperando confirmación",
                        symbol, absent, CLOSE_CONFIRM_LOOPS,
                    )
                    continue

                p = positions.pop(symbol)
                _declare_closed(symbol, p, positions)

            # Purgar cooldowns expirados
            expired = [
                sym for sym, ts in _cooldown.items()
                if time.time() - ts >= _cooldown_for(sym)
            ]
            for sym in expired:
                _cooldown.pop(sym, None)
                _cooldown_reason.pop(sym, None)
                log.info("[%s] Cooldown expirado — símbolo disponible", sym)

            expired_alerts = [
                sym for sym, ts in _manual_alert_cooldown.items()
                if time.time() - ts >= MANUAL_ALERT_COOLDOWN
            ]
            for sym in expired_alerts:
                _manual_alert_cooldown.pop(sym, None)

            # Sincronizar posiciones externas al dict local
            for symbol, pos_ex in all_ex_positions.items():
                if symbol not in positions:
                    ex_side = pos_ex.get("side")
                    if ex_side not in VALID_SIDES:
                        log.warning(
                            "[%s] Posición ignorada en sync — side inválido del exchange: %r",
                            symbol, ex_side,
                        )
                        continue
                    positions[symbol] = {
                        "side":          ex_side,
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
                    }
                    log.info("[%s] Sincronizada: %s @ %.6f (sl=%s tp=%s)",
                             symbol, ex_side, pos_ex["entry"],
                             pos_ex["sl"], pos_ex["tp"])

            open_count = len(positions)

            if loop_count % 10 == 1:
                daily_pnl  = bot_state.get_daily_pnl()
                paused_str = (
                    "PAUSADO(drawdown)" if bot_state.is_daily_limit_hit()
                    else ("PAUSADO" if bot_state.is_paused() else "activo")
                )
                log.info(
                    "[loop #%d] Posiciones: %d/%d | Feed: %d/%d | Cooldowns: %d | Estado: %s | PnL hoy: %+.2f USDT",
                    loop_count, open_count, config.MAX_POSITIONS,
                    feed.ready_count(), len(config.SYMBOLS), len(_cooldown),
                    paused_str, daily_pnl,
                )

            # Trailing stop + extend TP
            for symbol, pos in list(positions.items()):
                try:
                    price = exchange.get_price(symbol)
                    _update_trailing(symbol, pos, price)
                    _check_tp_extension(symbol, pos, price, feed, effective_min_score)
                except Exception as e:
                    log.warning("[%s] Error gestión posición: %s", symbol, e)

            # Filtro fin de semana
            if weekend:
                today = datetime.now(timezone.utc).weekday()
                if today != _weekend_notified_day:
                    _weekend_notified_day = today
                    day_name = "Sábado" if today == 5 else "Domingo"
                    log.info("Modo fin de semana activo (%s UTC) — score mínimo %d",
                             day_name, WEEKEND_MIN_SCORE)
                    telegram.notify(
                        f"\U0001f6ab Modo fin de semana ({day_name})\n"
                        f"No se abrirán posiciones nuevas salvo score \u2265 {WEEKEND_MIN_SCORE}.\n"
                        f"Posiciones actuales siguen gestionándose con normalidad."
                    )

            # Buscar señales
            if bot_state.is_paused() or bot_state.is_daily_limit_hit():
                if bot_state.is_daily_limit_hit():
                    log.debug("Bot pausado por drawdown diario — saltando búsqueda de señales")
                else:
                    log.debug("Bot pausado manualmente — saltando búsqueda de señales")
            else:
                if loop_count % 10 == 1:
                    regime_summary = []
                    for sym in config.SYMBOLS:
                        if not feed.ready(sym):
                            continue
                        try:
                            c1h = feed.get(sym, "1h")
                            reg, adx = signals._regime_confirmed(c1h)
                            regime_summary.append(f"{sym.split('-')[0]}:{reg[0].upper()}{adx:.0f}")
                        except Exception:
                            pass
                    if regime_summary:
                        log.info("[regímenes] %s", "  ".join(regime_summary))

                for symbol in config.SYMBOLS:
                    if symbol in positions:
                        continue
                    if not feed.ready(symbol):
                        continue
                    if symbol in _cooldown:
                        continue

                    is_manual = symbol in config.MANUAL_ALERT_SYMBOLS

                    if is_manual and symbol in _manual_alert_cooldown:
                        continue

                    if not is_manual and open_count >= config.MAX_POSITIONS:
                        break

                    try:
                        candles_15m = feed.get(symbol, "15m")
                        candles_1h  = feed.get(symbol, "1h")
                        candles_4h  = feed.get(symbol, "4h") if feed.has_tf(symbol, "4h") else None

                        signal, score = signals.evaluate(
                            candles_15m, candles_1h, candles_4h,
                            min_score=effective_min_score,
                        )

                        if not signal or signal not in VALID_SIDES:
                            if signal is not None:
                                log.warning(
                                    "[%s] Señal ignorada — side inválido: %r (score=%d)",
                                    symbol, signal, score,
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

                        # Modo alerta manual
                        if is_manual:
                            _manual_alert_cooldown[symbol] = time.time()
                            side_icon = "\U0001f7e1" if signal == "long" else "\U0001f534"
                            telegram.notify(
                                f"\U0001f6a8 <b>Alerta manual — {symbol}</b>\n\n"
                                f"{side_icon} Dirección: <b>{signal.upper()}</b>\n"
                                f"Precio actual: <code>{price:.6f}</code>\n"
                                f"SL sugerido:   <code>{params['sl']:.6f}</code>\n"
                                f"TP sugerido:   <code>{params['tp']:.6f}</code>\n"
                                f"Score: <b>{score}</b>\n\n"
                                f"\u26a0\ufe0f <i>Operación NO abierta automáticamente.</i>"
                            )
                            log.info("[%s] ALERTA MANUAL enviada | %s score=%d", symbol, signal.upper(), score)
                            continue

                        # Modo automático
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
                            "_extending":    False,
                            "trail_step":    params["trail_step"],
                            "trail_high":    price,
                            "trail_low":     price,
                            "score":         score,
                            "open_ts":       time.time(),
                        }
                        open_count += 1

                        telegram.notify_open(
                            symbol = symbol,
                            price  = price,
                            side   = signal,
                            qty    = params["qty"],
                            sl     = params["sl"],
                            tp     = params["tp"],
                            score  = score,
                            tp_rr  = params["tp_rr"],
                        )

                    except Exception as e:
                        log.error("[%s] Error: %s", symbol, e, exc_info=True)

        except Exception as e:
            log.error("Error en loop: %s", e, exc_info=True)
            telegram.notify(f"\u26a0\ufe0f Error en bot: {e}")

        time.sleep(config.LOOP_SLEEP)


if __name__ == "__main__":
    run()
