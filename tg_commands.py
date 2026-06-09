"""tg_commands.py — Listener de comandos Telegram (long polling).

Comandos:
    /start      — Lista de comandos
    /historial  — Historial completo de todos los trades (lee mensajes TG)
    /stats      — Win rate y PnL del histórico completo
    /posiciones — Posiciones abiertas ahora
    /status     — Estado del bot
"""
import logging
import re
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

LOG_CHAT_ID = config.TG_CHAT_ID  # puede sobreescribirse con TG_LOG_CHAT_ID


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
    Recupera todos los mensajes del chat que sean registros de trades
    (identificados por el emoji 📌 al inicio).
    Usa getUpdates con offset=0 para leer el histórico completo.
    Limitado a los últimos 100 updates de Telegram (límite de la API).
    """
    trades = []
    try:
        # Exportar historial via getChatHistory no existe en Bot API;
        # usamos forwardMessages trick: leemos updates con offset -1 (los últimos)
        # y combinamos con el caché en memoria
        resp = httpx.get(
            f"{_API}/getUpdates",
            params={"offset": -1, "limit": 1},
            timeout=10,
        )
        last_update_id = 0
        result = resp.json().get("result", [])
        if result:
            last_update_id = result[-1]["update_id"]

        # Leer desde el inicio en bloques de 100
        offset = 0
        all_messages = []
        while True:
            r = httpx.get(
                f"{_API}/getUpdates",
                params={"offset": offset, "limit": 100, "timeout": 0},
                timeout=15,
            )
            updates = r.json().get("result", [])
            if not updates:
                break
            for u in updates:
                msg = u.get("message") or u.get("channel_post")
                if msg and str(msg.get("chat", {}).get("id")) == str(_CHAT_ID):
                    all_messages.append(msg)
            offset = updates[-1]["update_id"] + 1
            if updates[-1]["update_id"] >= last_update_id:
                break

        # Filtrar mensajes que sean registros de trades (📌)
        for msg in all_messages:
            text = msg.get("text", "")
            if not text.startswith("📌"):
                continue
            trade = _parse_trade_msg(text)
            if trade:
                trades.append(trade)

    except Exception as e:
        log.warning("Error leyendo historial TG: %s", e)

    # Combinar con caché en memoria (trades de esta sesión ya parseados)
    # Evitar duplicados por fecha+symbol
    seen = {(t["date"], t["symbol"]) for t in trades}
    for t in trade_logger._cache:
        key = (t["date"], t["symbol"])
        if key not in seen:
            trades.append(t)
            seen.add(key)

    trades.sort(key=lambda t: t["date"])
    return trades


def _parse_trade_msg(text: str) -> dict | None:
    """Extrae datos de un mensaje de trade con formato 📌."""
    try:
        # Ejemplo:
        # 📌 BTC-USDT | 🟢 LONG | TP ✅
        # Entry:    67420.0000
        # Exit:     68850.0000
        # PnL:      +4.23% | +8.46 USDT
        # Score:    78 | Dur: 187 min
        # 2026-06-09 09:14 UTC
        lines = text.strip().splitlines()
        header = lines[0]  # 📌 BTC-USDT | 🟢 LONG | TP ✅
        parts  = [p.strip() for p in header.replace("📌", "").split("|")]
        symbol = parts[0].strip()
        side   = "long" if "🟢" in parts[1] or "LONG" in parts[1] else "short"
        reason = parts[2].replace("✅", "").replace("❌", "").strip()

        pnl_pct  = 0.0
        pnl_usdt = 0.0
        score    = 0
        date_str = ""

        for line in lines[1:]:
            line = line.strip()
            if line.startswith("PnL:"):
                m = re.search(r"([+-]?\d+\.\d+)%.*?([+-]?\d+\.\d+)\s*USDT", line)
                if m:
                    pnl_pct  = float(m.group(1))
                    pnl_usdt = float(m.group(2))
                    if "-" in line.split("%")[0]:
                        pnl_pct = -abs(pnl_pct)
                    if line.split("USDT")[0].strip().endswith("-") or "-" in line.split("|")[1]:
                        pnl_usdt = -abs(pnl_usdt)
            elif line.startswith("Score:"):
                m = re.search(r"(\d+)", line)
                if m:
                    score = int(m.group(1))
            elif re.match(r"\d{4}-\d{2}-\d{2}", line):
                date_str = line.replace("UTC", "").strip()

        if not symbol or not date_str:
            return None

        return {
            "date":     date_str,
            "symbol":   symbol,
            "side":     side,
            "pnl_pct":  pnl_pct,
            "pnl_usdt": pnl_usdt,
            "score":    score,
            "reason":   reason,
            "duration": 0,
        }
    except Exception:
        return None


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

    # Telegram tiene límite de 4096 chars por mensaje
    msg = "\n".join(lines)
    if len(msg) > 3800:
        # Truncar y avisar
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
    return (
        f"🤖 <b>Estado del bot</b>\n\n"
        f"Uptime:      <code>{h}h {m}m {s}s</code>\n"
        f"{feed_info}"
        f"Posiciones:  <code>{len(positions)}/{config.MAX_POSITIONS}</code>\n"
        f"Trades sesión: <code>{len(trade_logger._cache)}</code>\n"
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
