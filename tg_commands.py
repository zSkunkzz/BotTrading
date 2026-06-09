"""tg_commands.py — Escucha comandos Telegram en un hilo separado (long polling).

Comandos disponibles:
    /historial  — Historial completo de trades de la sesión
    /stats      — Win rate, PnL total, mejor y peor trade
    /posiciones — Posiciones abiertas actualmente
    /status     — Estado del bot (uptime, feed, capital estimado)

Uso:
    import tg_commands
    tg_commands.start(get_positions_fn, feed)  # llamar desde main.py
"""
import logging
import threading
import time
from datetime import datetime, timezone

import httpx

import config
import trade_logger

log = logging.getLogger("tg_commands")

_API      = f"https://api.telegram.org/bot{config.TG_TOKEN}"
_CHAT_ID  = config.TG_CHAT_ID
_offset   = 0
_start_ts = time.time()

# Referencia a las posiciones abiertas y al feed (se inyectan desde main.py)
_get_positions = None
_feed          = None


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


def _cmd_historial() -> str:
    trades = trade_logger._cache
    if not trades:
        return "📜 <b>Historial</b>\nSin trades en esta sesión todavía."

    lines = [f"📜 <b>Historial de trades ({len(trades)})</b>\n"]
    for t in trades:
        icon = "✅" if t["pnl_pct"] >= 0 else "❌"
        lines.append(
            f"{icon} <b>{t['symbol']}</b> {t['side'].upper()} "
            f"<code>{t['pnl_pct']:+.2f}%</code> | "
            f"score {t['score']} | {t['date']}"
        )
    return "\n".join(lines)


def _cmd_stats() -> str:
    trades = trade_logger._cache
    if not trades:
        return "📊 <b>Stats</b>\nSin trades todavía."

    total     = len(trades)
    wins      = [t for t in trades if t["pnl_pct"] >= 0]
    losses    = [t for t in trades if t["pnl_pct"] < 0]
    total_pnl = sum(t["pnl_usdt"] for t in trades)
    win_rate  = len(wins) / total * 100
    best      = max(trades, key=lambda t: t["pnl_pct"])
    worst     = min(trades, key=lambda t: t["pnl_pct"])
    avg_dur   = sum(t["duration"] for t in trades) / total
    avg_score = sum(t["score"] for t in trades) / total

    return (
        f"📊 <b>Estadísticas de sesión</b>\n\n"
        f"Trades:      <code>{total}</code> ({len(wins)}W / {len(losses)}L)\n"
        f"Win rate:    <code>{win_rate:.1f}%</code>\n"
        f"PnL total:   <code>{total_pnl:+.4f} USDT</code>\n"
        f"Score medio: <code>{avg_score:.0f}</code>\n"
        f"Dur. media:  <code>{avg_dur:.0f} min</code>\n\n"
        f"🏆 Mejor:  {best['symbol']} <code>{best['pnl_pct']:+.2f}%</code>\n"
        f"💩 Peor:   {worst['symbol']} <code>{worst['pnl_pct']:+.2f}%</code>"
    )


def _cmd_posiciones() -> str:
    if _get_positions is None:
        return "⚠️ No disponible."
    positions = _get_positions()
    if not positions:
        return "📊 <b>Posiciones abiertas</b>\nNinguna en este momento."

    lines = [f"📊 <b>Posiciones abiertas ({len(positions)}/{config.MAX_POSITIONS})</b>\n"]
    for symbol, p in positions.items():
        side_icon = "🟢" if p["side"] == "long" else "🔴"
        dur = round((time.time() - p.get("open_ts", time.time())) / 60, 0)
        lines.append(
            f"{side_icon} <b>{symbol}</b> {p['side'].upper()}\n"
            f"   Entry: <code>{p['entry']:.4f}</code> | "
            f"SL: <code>{p['sl']:.4f}</code> | "
            f"TP: <code>{p['tp']:.4f}</code>\n"
            f"   Score: <code>{p.get('score', '?')}</code> | "
            f"Dur: <code>{dur:.0f} min</code>"
        )
    return "\n".join(lines)


def _cmd_status() -> str:
    uptime_s  = int(time.time() - _start_ts)
    h, rem    = divmod(uptime_s, 3600)
    m, s      = divmod(rem, 60)
    uptime_str = f"{h}h {m}m {s}s"

    feed_info = ""
    if _feed is not None:
        ready = _feed.ready_count()
        total = len(config.SYMBOLS)
        feed_info = f"Feed:        <code>{ready}/{total} pares listos</code>\n"

    positions = _get_positions() if _get_positions else {}
    now_utc   = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    return (
        f"🤖 <b>Estado del bot</b>\n\n"
        f"Uptime:      <code>{uptime_str}</code>\n"
        f"{feed_info}"
        f"Posiciones:  <code>{len(positions)}/{config.MAX_POSITIONS}</code>\n"
        f"Trades hoy:  <code>{len(trade_logger._cache)}</code>\n"
        f"Hora:        <code>{now_utc}</code>"
    )


COMMANDS = {
    "/historial": _cmd_historial,
    "/stats":     _cmd_stats,
    "/posiciones": _cmd_posiciones,
    "/status":    _cmd_status,
    "/start":     lambda: (
        "🤖 <b>Bot de trading activo</b>\n\n"
        "Comandos disponibles:\n"
        "/historial — Historial completo de trades\n"
        "/stats — Win rate y PnL de la sesión\n"
        "/posiciones — Posiciones abiertas ahora\n"
        "/status — Estado general del bot"
    ),
}


def _poll() -> None:
    global _offset
    log.info("Telegram command listener iniciado")
    while True:
        try:
            resp = httpx.get(f"{_API}/getUpdates",
                             params={"offset": _offset, "timeout": 30},
                             timeout=35)
            data = resp.json()
            for update in data.get("result", []):
                _offset = update["update_id"] + 1
                msg = update.get("message") or update.get("edited_message")
                if not msg:
                    continue
                text    = msg.get("text", "").strip().split()[0].lower()
                chat_id = str(msg["chat"]["id"])

                # Solo responder al chat autorizado
                if chat_id != str(_CHAT_ID):
                    continue

                if text in COMMANDS:
                    log.info("Comando recibido: %s", text)
                    _send(COMMANDS[text](), chat_id)
                else:
                    _send(
                        f"Comando no reconocido: <code>{text}</code>\n"
                        "Escribe /start para ver los comandos disponibles.",
                        chat_id
                    )
        except Exception as e:
            log.warning("Poll error: %s — reintentando en 5s", e)
            time.sleep(5)


def start(get_positions_fn, feed=None) -> None:
    """Arranca el listener en un hilo daemon."""
    if not config.TG_TOKEN or not config.TG_CHAT_ID:
        log.warning("Telegram no configurado — comandos desactivados")
        return

    global _get_positions, _feed
    _get_positions = get_positions_fn
    _feed          = feed

    t = threading.Thread(target=_poll, daemon=True, name="tg-commands")
    t.start()
