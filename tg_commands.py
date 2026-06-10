"""tg_commands.py — Listener de comandos Telegram (long polling).

Comandos:
    /start      — Lista de comandos
    /historial  — Historial completo de todos los trades (lee CSV local)
    /stats      — Win rate y PnL del histórico completo
    /posiciones — Posiciones abiertas ahora
    /status     — Estado del bot
"""
import csv
import logging
import os
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


def _fetch_trade_history() -> list[dict]:
    """
    Lee el historial de trades desde el CSV local (trades.csv).
    Complementa con el caché en memoria para trades de la sesión actual
    que aún no se hayan flusheado al disco.
    """
    trades = []
    csv_path = trade_logger.LOG_FILE

    # 1. Leer CSV persistente
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

    # 2. Añadir trades en memoria que no estén ya en el CSV
    #    (el CSV se escribe en cada record(), así que normalmente ya están)
    seen = {(t["date"], t["symbol"]) for t in trades}
    for t in trade_logger._cache:
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

    lines = [f"📜 <b>Histórico completo ({len(trades)} trades)</b>\n"]
    for t in trades:
        icon = "✅" if t["pnl_pct"] >= 0 else "❌"
        lines.append(
            f"{icon} <b>{t['symbol']}</b> {t['side'].upper()} "
            f"<code>{t['pnl_pct']:+.2f}%</code> | "
            f"score {t['score']} | {t['date']}"
        )

    msg = "\n".join(lines)
    if len(msg) > 3800:
        msg = msg[:3800] + f"\n\n<i>... y {len(trades)} trades en total.</i>"
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
            f"   Score: <code>{p.get('score','?')}</code> | "
            f"Dur: <code>{dur:.0f} min</code>"
        )
    return "\n".join(lines)


def _cmd_status() -> str:
    uptime_s   = int(time.time() - _start_ts)
    h, rem     = divmod(uptime_s, 3600)
    m, s       = divmod(rem, 60)
    positions  = _get_positions() if _get_positions else {}
    feed_info  = ""
    if _feed:
        feed_info = f"Feed:        <code>{_feed.ready_count()}/{len(config.SYMBOLS)} pares</code>\n"
    csv_trades = len(_fetch_trade_history())
    return (
        f"🤖 <b>Estado del bot</b>\n\n"
        f"Uptime:      <code>{h}h {m}m {s}s</code>\n"
        f"{feed_info}"
        f"Posiciones:  <code>{len(positions)}/{config.MAX_POSITIONS}</code>\n"
        f"Trades total: <code>{csv_trades}</code>\n"
        f"Hora:        <code>{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}</code>"
    )


COMMANDS = {
    "/historial":  _cmd_historial,
    "/stats":      _cmd_stats,
    "/posiciones": _cmd_posiciones,
    "/status":     _cmd_status,
    "/start": lambda: (
        "🤖 <b>Bot de trading activo</b>\n\n"
        "Comandos disponibles:\n"
        "/historial — Histórico completo de todos los trades\n"
        "/stats — Win rate y PnL históricos\n"
        "/posiciones — Posiciones abiertas ahora\n"
        "/status — Estado general del bot"
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
                text    = msg.get("text", "").strip().split()[0].lower()
                chat_id = str(msg["chat"]["id"])
                if chat_id != str(_CHAT_ID):
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
