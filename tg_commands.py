"""tg_commands.py — Listener de comandos Telegram (long polling).

Comandos:
    /start      — Lista de comandos
    /historial  — Histórico completo de todos los trades (lee CSV local)
    /stats      — Win rate y PnL del histórico completo
    /posiciones — Posiciones abiertas ahora
    /status     — Estado del bot
    /stop       — Pausa la búsqueda de señales nuevas
    /resume     — Reanuda el bot
    /trades     — Envía el archivo trades.csv como documento descargable
    /long <SYM> — Señal manual LONG para el símbolo
    /short <SYM>— Señal manual SHORT para el símbolo
    /sltp <SYM> <SL> <TP> — Modifica SL y TP de una posición abierta

FIX: _fetch_trade_history usa trade_logger.get_cache_snapshot() en lugar de
     acceder a trade_logger._cache directamente, respetando el lock del módulo.
     /historial limita la visualización a los últimos 100 trades para no
     bloquear el thread de polling en CSVs grandes.
FIX: _cmd_posiciones formatea sl/tp con fallback 'N/A' si son None (posición
     abierta sin SL/TP colocado por error en exchange).
FIX: _poll strip del sufijo @botname en comandos (e.g. /status@MiBot).
FIX: pop_manual_signal añadida — referenciada en main.py pero no existía.
FIX #6: _cmd_status muestra PnL del día y estado de pausa por drawdown diario.
"""
import csv
import logging
import os
import threading
import time
from datetime import datetime, timezone

import httpx

import bot_state
import config
import exchange
import trade_logger

log = logging.getLogger("tg_commands")

_API      = f"https://api.telegram.org/bot{config.TG_TOKEN}"
_CHAT_ID  = config.TG_CHAT_ID
_offset   = 0
_start_ts = time.time()

_get_positions = None
_feed          = None

# Señales manuales pendientes: symbol → side ('long' | 'short')
_manual_signals: dict[str, str] = {}
_manual_signals_lock = threading.Lock()


def pop_manual_signal(symbol: str) -> str | None:
    """Extrae y devuelve la señal manual pendiente para el símbolo, o None.

    Llamada desde el loop principal de main.py. Thread-safe.
    """
    with _manual_signals_lock:
        return _manual_signals.pop(symbol, None)


def _send(text: str, chat_id: str = None) -> None:
    if not config.TG_TOKEN:
        return
    try:
        httpx.post(f"{_API}/sendMessage", json={
            "chat_id":    chat_id or _CHAT_ID,
            "text":       text,
            "parse_mode": "HTML",
        }, timeout=5)
    except Exception as e:
        log.warning("tg_commands send error: %s", e)


def _send_document(file_path: str, caption: str, chat_id: str = None) -> None:
    """Envía un archivo como documento al chat de Telegram."""
    if not config.TG_TOKEN:
        return
    try:
        with open(file_path, "rb") as f:
            httpx.post(
                f"{_API}/sendDocument",
                data={
                    "chat_id":    chat_id or _CHAT_ID,
                    "caption":    caption,
                    "parse_mode": "HTML",
                },
                files={"document": (os.path.basename(file_path), f, "text/csv")},
                timeout=15,
            )
    except Exception as e:
        log.warning("tg_commands send_document error: %s", e)


def _fetch_trade_history() -> list[dict]:
    """Devuelve el histórico combinando CSV + cache en memoria."""
    trades = []
    csv_path = trade_logger.LOG_FILE

    if os.path.exists(csv_path) and os.path.getsize(csv_path) > 0:
        try:
            with open(csv_path, newline="") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    try:
                        trades.append({
                            "date":     row["date"],
                            "symbol":   row["symbol"],
                            "side":     row["side"],
                            "entry":    float(row["entry"]),
                            "exit":     float(row["exit"]),
                            "pnl_pct":  float(row["pnl_pct"]),
                            "pnl_usdt": float(row["pnl_usdt"]),
                            "score":    int(row["score"]),
                            "reason":   row["reason"],
                            "duration": float(row.get("duration_min", 0)),
                        })
                    except (ValueError, KeyError):
                        continue
        except Exception as e:
            log.warning("Error leyendo CSV de trades: %s", e)

    seen = {(t["date"], t["symbol"]) for t in trades}
    for t in trade_logger.get_cache_snapshot():
        key = (t["date"], t["symbol"])
        if key not in seen:
            trades.append(t)
            seen.add(key)

    trades.sort(key=lambda t: t["date"])
    return trades


def _cmd_historial() -> str:
    trades = _fetch_trade_history()
    if not trades:
        return "📜 <b>Histórico completo</b>\nSin trades registrados todavía."

    total = len(trades)
    display = trades[-100:]
    omitted = total - len(display)

    lines = [f"📜 <b>Histórico ({total} trades{', últimos 100' if omitted else ''})</b>\n"]
    for t in display:
        icon = "✅" if t["pnl_pct"] >= 0 else "❌"
        lines.append(
            f"{icon} <b>{t['symbol']}</b> {t['side'].upper()} "
            f"<code>{t['pnl_pct']:+.2f}%</code> | "
            f"score {t['score']} | {t['date']}"
        )
    if omitted:
        lines.append(f"\n<i>... y {omitted} trades anteriores en CSV.</i>")

    msg = "\n".join(lines)
    if len(msg) > 3800:
        msg = msg[:3800] + f"\n\n<i>... mensaje truncado ({total} trades totales).</i>"
    return msg


def _cmd_stats() -> str:
    trades = _fetch_trade_history()
    if not trades:
        return "📊 <b>Stats</b>\nSin trades registrados todavía."

    total     = len(trades)
    wins      = [t for t in trades if t["pnl_pct"] >= 0]
    losses    = [t for t in trades if t["pnl_pct"] < 0]
    total_pnl = sum(t["pnl_usdt"] for t in trades)
    win_rate  = len(wins) / total * 100
    best      = max(trades, key=lambda t: t["pnl_pct"])
    worst     = min(trades, key=lambda t: t["pnl_pct"])
    avg_score = sum(t["score"] for t in trades) / total

    return (
        f"📊 <b>Estadísticas históricas</b>\n\n"
        f"Trades totales: <code>{total}</code> ({len(wins)}W / {len(losses)}L)\n"
        f"Win rate:       <code>{win_rate:.1f}%</code>\n"
        f"PnL total:      <code>{total_pnl:+.4f} USDT</code>\n"
        f"Score medio:    <code>{avg_score:.0f}</code>\n\n"
        f"🏆 Mejor:  {best['symbol']} <code>{best['pnl_pct']:+.2f}%</code>\n"
        f"💩 Peor:   {worst['symbol']} <code>{worst['pnl_pct']:+.2f}%</code>"
    )


def _cmd_posiciones() -> str:
    if _get_positions is None:
        return "⚠️ No disponible."
    positions = dict(_get_positions())
    if not positions:
        return "📊 <b>Posiciones abiertas</b>\nNinguna en este momento."

    lines = [f"📊 <b>Posiciones abiertas ({len(positions)}/{config.MAX_POSITIONS})</b>\n"]
    for symbol, p in positions.items():
        side_icon = "🟢" if p["side"] == "long" else "🔴"
        dur = round((time.time() - p.get("open_ts", time.time())) / 60, 0)
        ext = p.get("tp_extensions", 0)
        ext_str = f" | Ext: <code>{ext}</code>" if ext > 0 else ""
        sl_str = f"{p['sl']:.4f}" if p.get('sl') is not None else "N/A"
        tp_str = f"{p['tp']:.4f}" if p.get('tp') is not None else "N/A"
        lines.append(
            f"{side_icon} <b>{symbol}</b> {p['side'].upper()}\n"
            f"   Entry: <code>{p['entry']:.4f}</code> | "
            f"SL: <code>{sl_str}</code> | "
            f"TP: <code>{tp_str}</code>\n"
            f"   Score: <code>{p.get('score','?')}</code> | "
            f"Dur: <code>{dur:.0f} min</code>{ext_str}"
        )
    return "\n".join(lines)


def _cmd_status() -> str:
    """fix #6: muestra PnL del día y si el bot está pausado por drawdown diario.

    Antes solo mostraba pausa manual. Si el bot se pausaba automáticamente por
    pérdidas (is_daily_limit_hit), /status no lo reflejaba — el usuario no
    sabía por qué el bot no abría posiciones.
    """
    uptime_s   = int(time.time() - _start_ts)
    h, rem     = divmod(uptime_s, 3600)
    m, s       = divmod(rem, 60)
    positions  = dict(_get_positions()) if _get_positions else {}
    feed_info  = ""
    if _feed:
        feed_info = f"Feed:        <code>{_feed.ready_count()}/{len(config.SYMBOLS)} pares</code>\n"
    csv_trades = len(_fetch_trade_history())
    daily_pnl  = bot_state.get_daily_pnl()
    capital    = float(getattr(config, "MARGIN_USDT", 0)) * int(getattr(config, "MAX_POSITIONS", 1))
    daily_pct  = (daily_pnl / capital * 100) if capital else 0.0

    # fix #6: distinguir pausa manual de pausa por drawdown
    if bot_state.is_daily_limit_hit():
        paused_str = "🛑 <b>PAUSADO por drawdown diario</b> — no se abrirán posiciones nuevas\n"
    elif bot_state.is_paused():
        paused_str = "🛑 <b>PAUSADO manualmente</b> — no se abrirán posiciones nuevas\n"
    else:
        paused_str = ""

    return (
        f"🤖 <b>Estado del bot</b>\n\n"
        f"{paused_str}"
        f"Uptime:      <code>{h}h {m}m {s}s</code>\n"
        f"{feed_info}"
        f"Posiciones:  <code>{len(positions)}/{config.MAX_POSITIONS}</code>\n"
        f"Trades total: <code>{csv_trades}</code>\n"
        f"PnL hoy:     <code>{daily_pnl:+.2f} USDT</code> (<code>{daily_pct:+.2f}%</code>)\n"
        f"Hora:        <code>{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}</code>"
    )


def _cmd_stop() -> str:
    if bot_state.is_paused():
        return "🛑 El bot ya está pausado. Usa /resume para reanudarlo."
    bot_state.pause()
    log.info("Bot PAUSADO por comando Telegram")
    return (
        "🛑 <b>Bot pausado</b>\n\n"
        "No se abrirán posiciones nuevas.\n"
        "Las posiciones ya abiertas siguen gestionándose con normalidad "
        "(trailing, TP dinámico, SL).\n\n"
        "Usa /resume para reanudar."
    )


def _cmd_resume() -> str:
    if not bot_state.is_paused():
        return "▶️ El bot ya está activo."
    bot_state.resume()
    log.info("Bot REANUDADO por comando Telegram")
    return "▶️ <b>Bot reanudado</b>\nVolverá a buscar señales en el próximo ciclo."


def _handle_sltp(parts: list[str], chat_id: str) -> None:
    """Comando /sltp <SYM> <SL> <TP>

    Modifica el Stop Loss y el Take Profit de una posición abierta sin
    cerrarla. Usa exchange.modify_sltp_orders() (batchModify in-place).

    Uso:
        /sltp BTC-USDT 60000 70000
        /sltp ETH-USDT 2800.5 3400

    Reglas de validación:
      - El símbolo debe tener una posición abierta en el bot.
      - SL y TP deben ser números positivos.
      - Long:  SL < entry y TP > entry
      - Short: SL > entry y TP < entry
    """
    if len(parts) < 4:
        _send(
            "⚠️ Uso: <code>/sltp SYMBOL SL TP</code>\n"
            "Ejemplo: <code>/sltp BTC-USDT 60000 70000</code>",
            chat_id,
        )
        return

    symbol = parts[1].upper()
    try:
        new_sl = float(parts[2])
        new_tp = float(parts[3])
    except ValueError:
        _send("⚠️ SL y TP deben ser números. Ejemplo: <code>/sltp BTC-USDT 60000 70000</code>", chat_id)
        return

    if new_sl <= 0 or new_tp <= 0:
        _send("⚠️ SL y TP deben ser mayores que 0.", chat_id)
        return

    if _get_positions is None:
        _send("⚠️ No hay acceso a posiciones en este momento.", chat_id)
        return

    positions = dict(_get_positions())
    if symbol not in positions:
        open_syms = ", ".join(positions.keys()) if positions else "ninguna"
        _send(
            f"⚠️ No hay posición abierta para <b>{symbol}</b>.\n"
            f"Posiciones actuales: <code>{open_syms}</code>",
            chat_id,
        )
        return

    pos   = positions[symbol]
    side  = pos["side"]
    entry = pos.get("entry", 0.0)
    qty   = pos.get("qty", 0.0)

    # Validación direccional
    if side == "long":
        if new_sl >= entry:
            _send(
                f"⚠️ Para LONG el SL debe ser menor que entry.\n"
                f"Entry: <code>{entry:.6f}</code> | SL propuesto: <code>{new_sl}</code>",
                chat_id,
            )
            return
        if new_tp <= entry:
            _send(
                f"⚠️ Para LONG el TP debe ser mayor que entry.\n"
                f"Entry: <code>{entry:.6f}</code> | TP propuesto: <code>{new_tp}</code>",
                chat_id,
            )
            return
    else:  # short
        if new_sl <= entry:
            _send(
                f"⚠️ Para SHORT el SL debe ser mayor que entry.\n"
                f"Entry: <code>{entry:.6f}</code> | SL propuesto: <code>{new_sl}</code>",
                chat_id,
            )
            return
        if new_tp >= entry:
            _send(
                f"⚠️ Para SHORT el TP debe ser menor que entry.\n"
                f"Entry: <code>{entry:.6f}</code> | TP propuesto: <code>{new_tp}</code>",
                chat_id,
            )
            return

    old_sl = pos.get("sl")
    old_tp = pos.get("tp")

    try:
        exchange.modify_sltp_orders(symbol, side, qty, new_sl, new_tp)
    except Exception as exc:
        log.error("[%s] /sltp: error en modify_sltp_orders: %s", symbol, exc)
        _send(
            f"❌ <b>Error al modificar SL/TP</b>\n"
            f"{symbol} {side.upper()}\n"
            f"Error: <code>{exc}</code>\n"
            f"Las órdenes anteriores siguen activas.",
            chat_id,
        )
        return

    # Actualizar el dict local en memoria para que main.py vea los nuevos valores
    pos["sl"] = new_sl
    pos["tp"] = new_tp

    old_sl_str = f"{old_sl:.6f}" if old_sl is not None else "N/A"
    old_tp_str = f"{old_tp:.6f}" if old_tp is not None else "N/A"

    log.info(
        "[%s] /sltp aplicado | side=%s entry=%.6f | SL %s→%.6f | TP %s→%.6f",
        symbol, side, entry, old_sl_str, new_sl, old_tp_str, new_tp,
    )
    _send(
        f"✅ <b>SL/TP modificado</b>\n"
        f"{symbol} {side.upper()}\n\n"
        f"Entry:  <code>{entry:.6f}</code>\n"
        f"SL: <code>{old_sl_str}</code> → <code>{new_sl:.6f}</code>\n"
        f"TP: <code>{old_tp_str}</code> → <code>{new_tp:.6f}</code>",
        chat_id,
    )


COMMANDS = {
    "/historial":  _cmd_historial,
    "/stats":      _cmd_stats,
    "/posiciones": _cmd_posiciones,
    "/status":     _cmd_status,
    "/stop":       _cmd_stop,
    "/resume":     _cmd_resume,
    "/start": lambda: (
        "🤖 <b>Bot de trading activo</b>\n\n"
        "Comandos disponibles:\n"
        "/historial — Histórico completo de todos los trades\n"
        "/stats — Win rate y PnL históricos\n"
        "/posiciones — Posiciones abiertas ahora\n"
        "/status — Estado general del bot\n"
        "/trades — Descargar trades.csv\n"
        "/stop — Pausar búsqueda de señales nuevas\n"
        "/resume — Reanudar el bot\n"
        "/long BTC-USDT — Señal manual LONG\n"
        "/short BTC-USDT — Señal manual SHORT\n"
        "/sltp BTC-USDT 60000 70000 — Modificar SL y TP de una posición"
    ),
}


def _poll() -> None:
    global _offset
    log.info("Telegram command listener iniciado")
    while True:
        try:
            resp = httpx.get(
                f"{_API}/getUpdates",
                params={"offset": _offset, "timeout": 30},
                timeout=35,
            )
            for update in resp.json().get("result", []):
                _offset = update["update_id"] + 1
                msg  = update.get("message") or update.get("edited_message")
                if not msg:
                    continue
                parts   = msg.get("text", "").strip().split()
                if not parts:
                    continue
                raw_cmd = parts[0].lower()
                text    = raw_cmd.split("@")[0]
                chat_id = str(msg["chat"]["id"])
                if chat_id != str(_CHAT_ID):
                    continue

                # Comando /trades — envía el CSV como documento
                if text == "/trades":
                    csv_path = trade_logger.LOG_FILE
                    if not os.path.exists(csv_path) or os.path.getsize(csv_path) == 0:
                        _send("📂 <b>trades.csv</b>\nEl archivo está vacío o no existe todavía.", chat_id)
                    else:
                        trades = _fetch_trade_history()
                        size_kb = os.path.getsize(csv_path) / 1024
                        caption = (
                            f"📂 <b>trades.csv</b>\n"
                            f"{len(trades)} trades | {size_kb:.1f} KB"
                        )
                        log.info("Enviando trades.csv (%d trades, %.1f KB)", len(trades), size_kb)
                        _send_document(csv_path, caption, chat_id)
                    continue

                # Comando /sltp <SYM> <SL> <TP>
                if text == "/sltp":
                    _handle_sltp(parts, chat_id)
                    continue

                # Comandos manuales /long <SYM> y /short <SYM>
                if text in ("/long", "/short") and len(parts) >= 2:
                    side   = text.lstrip("/")
                    symbol = parts[1].upper()
                    if symbol not in config.SYMBOLS:
                        _send(f"⚠️ Símbolo no reconocido: <code>{symbol}</code>\n"
                              f"Símbolos válidos: {', '.join(config.SYMBOLS[:10])}{'...' if len(config.SYMBOLS) > 10 else ''}",
                              chat_id)
                    else:
                        with _manual_signals_lock:
                            _manual_signals[symbol] = side
                        log.info("Señal manual encolada: %s %s", side.upper(), symbol)
                        _send(f"✅ Señal manual encolada: <b>{symbol} {side.upper()}</b>\n"
                              f"Se ejecutará en el próximo ciclo del bot.", chat_id)
                    continue

                if text in COMMANDS:
                    log.info("Comando: %s", text)
                    _send(COMMANDS[text](), chat_id)
                else:
                    _send(
                        f"Comando no reconocido: <code>{text}</code>\n"
                        "Escribe /start para ver los disponibles.",
                        chat_id,
                    )
        except Exception as e:
            log.warning("Poll error: %s — reintentando en 5s", e)
            time.sleep(5)


def start(get_positions_fn, feed=None) -> None:
    if not config.TG_TOKEN or not config.TG_CHAT_ID:
        log.warning("Telegram no configurado — comandos desactivados")
        return
    global _get_positions, _feed
    _get_positions = get_positions_fn
    _feed          = feed
    threading.Thread(target=_poll, daemon=True, name="tg-commands").start()
