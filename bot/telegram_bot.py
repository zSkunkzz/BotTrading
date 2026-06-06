import asyncio
import logging
import os
import signal
from telegram import Bot, Update
from telegram.error import TelegramError, NetworkError
from telegram.request import HTTPXRequest

logger = logging.getLogger("TelegramBot")

_bot: Bot | None = None

# Long-poll timeout enviado a Telegram (segundos que el servidor espera antes de
# responder con lista vacía si no hay updates). El read_timeout del cliente HTTP
# DEBE ser mayor para que httpx no corte la conexión antes de que Telegram responda.
_POLL_TIMEOUT    = int(os.getenv("TG_POLL_TIMEOUT",    "30"))
_HTTP_READ_TIMEOUT = int(os.getenv("TG_HTTP_READ_TIMEOUT", "45"))  # > _POLL_TIMEOUT

# Flag pendiente de confirmación para /stop
_STOP_PENDING: bool = False


def _get_bot() -> Bot | None:
    global _bot
    if _bot is None:
        token = os.getenv("TELEGRAM_TOKEN", "")
        if token:
            _bot = Bot(
                token=token,
                request=HTTPXRequest(
                    read_timeout=_HTTP_READ_TIMEOUT,
                    write_timeout=10,
                    connect_timeout=10,
                    pool_timeout=5,
                ),
            )
    return _bot


def _chat() -> str:
    return os.getenv("TELEGRAM_CHAT_ID", "")


def _esc(text: str) -> str:
    return str(text).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


async def _send(text: str):
    bot  = _get_bot()
    chat = _chat()
    if not bot or not chat:
        return
    try:
        await bot.send_message(chat_id=chat, text=text, parse_mode="HTML")
    except TelegramError as e:
        logger.warning(f"[Telegram] {e}")


async def send_message(text: str):
    await _send(text)


async def notify_startup(pairs: list, dry_run: bool, top_n: int):
    mode = "\U0001f9ea DRY RUN" if dry_run else "\U0001f4b0 REAL MONEY"
    pairs_str = _esc(", ".join(pairs[:10])) + (" ..." if len(pairs) > 10 else "")
    await _send(
        f"\U0001f916 <b>TradingBot arrancado</b> \u2014 {mode}\n"
        f"Pares activos ({top_n}): <code>{pairs_str}</code>\n"
        f"Comandos: /stop | /pause | /resume | /ksstatus | /resetks"
    )


async def notify_open(
    symbol,
    side,
    price=None,
    entry=None,
    leverage=None,
    usdt=None,
    usdc=None,
    size_usdc=None,
    notional=None,
    sl=None,
    tp1=None,
    tp2=None,
    tp3=None,
    dry_run=False,
    signal_block=None,
    ai_used=False,
    ai_confidence=0,
    entry_mode=None,
    **kwargs,
):
    actual_price = price if price is not None else entry
    actual_size  = size_usdc if size_usdc is not None else (
                   usdc if usdc is not None else (
                   usdt if usdt is not None else notional))

    emoji = "\U0001f4c8" if side == "long" else "\U0001f4c9"
    mode  = " [DRY]" if dry_run else ""
    lines = [
        f"{emoji} <b>OPEN {_esc(side.upper())}</b>{mode} <code>{_esc(symbol)}</code>",
        f"Entry: <code>{_esc(actual_price)}</code> | Lev: <code>{_esc(leverage)}x</code>"
        + (f" | Size: <code>{_esc(actual_size)}$</code>" if actual_size is not None else ""),
    ]
    if sl:
        lines.append(f"SL: <code>{_esc(sl)}</code>")
    tps = [tp for tp in (tp1, tp2, tp3) if tp is not None]
    if tps:
        lines.append("TP: " + " / ".join(f"<code>{_esc(t)}</code>" for t in tps))
    if entry_mode:
        lines.append(f"Modo: <code>{_esc(entry_mode)}</code>")
    if ai_used and ai_confidence:
        lines.append(f"IA: <code>{_esc(ai_confidence)}/10</code>")
    await _send("\n".join(lines))


async def notify_close(symbol, side, exit_p, pnl, entry=None, reason="", dry_run=False):
    emoji = "\u2705" if pnl > 0 else "\u274c"
    mode  = " [DRY]" if dry_run else ""
    entry_str = f"Entry: <code>{_esc(entry)}</code> \u2192 " if entry is not None else ""
    await _send(
        f"{emoji} <b>CLOSE {_esc(side.upper())}</b>{mode} <code>{_esc(symbol)}</code>\n"
        f"{entry_str}Exit: <code>{_esc(exit_p)}</code>\n"
        f"PnL: <code>{pnl:+.2f}%</code> | {_esc(reason)}"
    )


async def notify_close_failed(symbol, reason, error):
    await _send(
        f"\u26a0\ufe0f <b>\u274c CIERRE FALLIDO</b> <code>{_esc(symbol)}</code>\n"
        f"Raz\u00f3n: {_esc(reason)}\n"
        f"Error: <code>{_esc(error[:200])}</code>\n"
        f"<b>\u26a0\ufe0f POSICI\u00d3N SIGUE ABIERTA \u2014 revisar manualmente</b>"
    )


async def notify_tp_partial(symbol, side, price, tp_level: int = 2, ratio: float = 0.5, dry_run: bool = False):
    mode = " [DRY]" if dry_run else ""
    await _send(
        f"\u2702\ufe0f <b>TP{tp_level} PARCIAL</b>{mode} <code>{_esc(symbol)}</code>\n"
        f"Cerrado <code>{ratio*100:.0f}%</code> de la posici\u00f3n {_esc(side.upper())} @ <code>{_esc(price)}</code>\n"
        f"SL movido a <b>break-even</b>"
    )


async def notify_ai_decision(symbol, action, score, reason):
    emojis = {"BUY": "\U0001f7e2", "SELL": "\U0001f534", "CLOSE": "\U0001f512", "HOLD": "\u26aa"}
    emoji  = emojis.get(action, "\u2753")
    await _send(
        f"{emoji} <b>IA \u2192 {_esc(action)}</b> <code>{_esc(symbol)}</code> Score: <code>{_esc(score)}/10</code>\n"
        f"{_esc(str(reason)[:300])}"
    )


async def notify_risk_block(symbol, reason):
    await _send(f"\u26d4 <b>RISK BLOCK</b> <code>{_esc(symbol)}</code>\n{_esc(reason)}")


async def notify_scanner_update(added: set, removed: set, total: int):
    parts = [f"\U0001f50d <b>Scanner update</b> \u2014 {total} pares activos"]
    if added:   parts.append(f"\u2795 A\u00f1adidos: <code>{_esc(', '.join(added))}</code>")
    if removed: parts.append(f"\u2796 Retirados: <code>{_esc(', '.join(removed))}</code>")
    await _send("\n".join(parts))


async def notify_kill_switch(level: int, trigger: str):
    level_labels = {
        1: "\u26a0\ufe0f L1 \u2014 Nuevas entradas pausadas",
        2: "\U0001f6d1 L2 \u2014 S\u00edmbolo/estrategia halted",
        3: "\U0001f6a8 L3 \u2014 \u00d3rdenes bloqueadas",
        4: "\U0001f480 L4 \u2014 HARD KILL",
    }
    label = level_labels.get(level, f"L{level}")
    await _send(
        f"\U0001f6d1 <b>KILL SWITCH ACTIVADO</b>\n"
        f"Nivel: <b>{label}</b>\n"
        f"Trigger: <code>{_esc(trigger[:300])}</code>\n"
        f"Usa /resetks para re-armar el bot."
    )


_ALLOWED_CHAT: str = ""


async def _handle_update(update: Update) -> None:
    msg = update.message
    if not msg or not msg.text:
        return
    if _ALLOWED_CHAT and str(msg.chat_id) != _ALLOWED_CHAT:
        logger.warning("[Telegram] Mensaje ignorado de chat no autorizado: %s", msg.chat_id)
        return
    text = msg.text.strip()
    if text.startswith("/resetks"):
        parts = text.split()
        await _cmd_resetks(msg.chat_id, parts[1:])
    elif text.startswith("/ksstatus"):
        await _cmd_ksstatus(msg.chat_id)
    elif text.startswith("/stop"):
        parts = text.split()
        await _cmd_stop(msg.chat_id, parts[1:])
    elif text.startswith("/pause"):
        await _cmd_pause(msg.chat_id)
    elif text.startswith("/resume"):
        await _cmd_resume(msg.chat_id)


async def _cmd_ksstatus(chat_id: int | str) -> None:
    """Muestra el estado actual del Kill Switch sin hacer nada."""
    from bot.kill_switch import kill_switch
    bot = _get_bot()
    if not bot:
        return

    async def reply(text: str):
        try:
            await bot.send_message(chat_id=chat_id, text=text, parse_mode="HTML")
        except TelegramError as e:
            logger.warning("[Telegram cmd_ksstatus] %s", e)

    lvl     = kill_switch.level()
    trigger = kill_switch._trigger or "\u2014"
    halted  = ", ".join(kill_switch._halted_symbols) or "ninguno"
    hard    = "S\u00cd \U0001f480" if kill_switch.is_hard_killed() else "no"
    daily   = kill_switch._daily_pnl
    consec  = kill_switch._consec_losses
    await reply(
        f"\U0001f6d1 <b>Kill Switch \u2014 Estado actual</b>\n"
        f"Nivel: <b>L{lvl}</b> {'\u2705 OK' if lvl == 0 else '\u26a0\ufe0f ACTIVO'}\n"
        f"Trigger: <code>{_esc(trigger[:200])}</code>\n"
        f"Hard killed: {hard}\n"
        f"S\u00edmbolos pausados: <code>{_esc(halted)}</code>\n"
        f"PnL diario acum: <code>{daily:+.2f}%</code>\n"
        f"P\u00e9rdidas consecutivas: <code>{consec}</code>"
    )


async def _cmd_resetks(chat_id: int | str, args: list[str]) -> None:
    """
    /resetks          — re-arma el Kill Switch completamente
    /resetks daily    — resetea solo contadores diarios
    """
    from bot.kill_switch import kill_switch
    bot = _get_bot()
    if not bot:
        return

    async def reply(text: str):
        try:
            await bot.send_message(chat_id=chat_id, text=text, parse_mode="HTML")
        except TelegramError as e:
            logger.warning("[Telegram cmd_resetks] %s", e)

    if args and args[0].lower() == "daily":
        await kill_switch.reset_daily_pnl()
        await reply(
            "\u2705 <b>Contadores diarios reseteados</b>\n"
            "PnL diario, p\u00e9rdidas consecutivas y reconexiones API \u2192 0\n"
            "El <b>nivel del KS no ha cambiado</b>."
        )
        return

    await kill_switch.manual_reset()
    await reply(
        "\u2705 <b>Kill Switch RE-ARMADO</b>\n"
        "Nivel: <b>L0 \u2014 OK</b>\n"
        "Todos los contadores y flags reseteados.\n"
        "El bot puede volver a abrir posiciones."
    )
    await _send(
        "\U0001f513 <b>Kill Switch re-armado manualmente</b> v\u00eda Telegram.\n"
        "Bot vuelve a estar operativo."
    )


async def _cmd_stop(chat_id: int | str, args: list[str]) -> None:
    """
    /stop          — Pide confirmación antes de parar el proceso.
    /stop confirm  — Envía SIGTERM al proceso (graceful shutdown).
    """
    global _STOP_PENDING
    bot = _get_bot()
    if not bot:
        return

    async def reply(text: str):
        try:
            await bot.send_message(chat_id=chat_id, text=text, parse_mode="HTML")
        except TelegramError as e:
            logger.warning("[Telegram cmd_stop] %s", e)

    if not args or args[0].lower() != "confirm":
        _STOP_PENDING = True
        await reply(
            "\U0001f6d1 <b>\u00bfSeguro que quieres parar el bot?</b>\n\n"
            "Esto enviar\u00e1 SIGTERM al proceso \u2014 shutdown limpio.\n"
            "Las posiciones abiertas <b>quedan vivas en el exchange</b> "
            "(SL/TP siguen activos en el exchange).\n\n"
            "Confirma con: <code>/stop confirm</code>\n"
            "Para cancelar, ignora este mensaje."
        )
        return

    if not _STOP_PENDING:
        await reply(
            "\u26a0\ufe0f Usa primero <code>/stop</code> para iniciar la confirmaci\u00f3n."
        )
        return

    _STOP_PENDING = False
    logger.warning("[Telegram] /stop confirm recibido \u2014 enviando SIGTERM al proceso.")
    await reply(
        "\U0001f6d1 <b>Parando bot...</b>\n"
        "Shutdown limpio iniciado. Las posiciones abiertas quedan con SL/TP activos."
    )
    await asyncio.sleep(1)
    os.kill(os.getpid(), signal.SIGTERM)


async def _cmd_pause(chat_id: int | str) -> None:
    """
    /pause — Activa KS L4 via hard_kill(): el bot deja de abrir y gestionar
    órdenes pero el proceso sigue corriendo. Para reanudar: /resume
    """
    from bot.kill_switch import kill_switch
    bot = _get_bot()
    if not bot:
        return

    async def reply(text: str):
        try:
            await bot.send_message(chat_id=chat_id, text=text, parse_mode="HTML")
        except TelegramError as e:
            logger.warning("[Telegram cmd_pause] %s", e)

    await kill_switch.hard_kill("/pause manual v\u00eda Telegram")
    await reply(
        "\u23f8\ufe0f <b>Bot PAUSADO</b>\n"
        "Kill Switch L4 (Hard Kill) activado.\n"
        "No se abrir\u00e1n nuevas \u00f3rdenes ni se gestionar\u00e1n posiciones.\n"
        "Las posiciones existentes en el exchange siguen con SL/TP activos.\n\n"
        "Para reanudar: <code>/resume</code>"
    )


async def _cmd_resume(chat_id: int | str) -> None:
    """
    /resume — Re-arma el Kill Switch (alias de /resetks).
    El bot vuelve a operar normalmente.
    """
    from bot.kill_switch import kill_switch
    bot = _get_bot()
    if not bot:
        return

    async def reply(text: str):
        try:
            await bot.send_message(chat_id=chat_id, text=text, parse_mode="HTML")
        except TelegramError as e:
            logger.warning("[Telegram cmd_resume] %s", e)

    await kill_switch.manual_reset()
    await reply(
        "\u25b6\ufe0f <b>Bot REANUDADO</b>\n"
        "Kill Switch re-armado \u2014 L0 OK.\n"
        "El bot puede volver a abrir posiciones."
    )
    await _send(
        "\U0001f513 <b>Bot reanudado manualmente</b> v\u00eda /resume.\n"
        "Bot vuelve a estar operativo."
    )


async def _polling_loop() -> None:
    bot = _get_bot()
    if not bot:
        logger.info("[Telegram polling] Sin token \u2014 comandos desactivados.")
        return
    offset: int | None = None
    logger.info(
        "[Telegram polling] Iniciado \u2014 escuchando /stop /pause /resume /resetks /ksstatus "
        "(poll_timeout=%ds read_timeout=%ds)",
        _POLL_TIMEOUT, _HTTP_READ_TIMEOUT,
    )
    while True:
        try:
            updates = await bot.get_updates(
                offset=offset,
                timeout=_POLL_TIMEOUT,
                allowed_updates=["message"],
            )
            for upd in updates:
                offset = upd.update_id + 1
                try:
                    await _handle_update(upd)
                except Exception as e:
                    logger.warning("[Telegram polling] Error en handler: %s", e)
        except asyncio.CancelledError:
            logger.info("[Telegram polling] Cancelado.")
            break
        except NetworkError as e:
            logger.debug("[Telegram polling] red transitoria: %s \u2014 reintentando en 10 s", e)
            await asyncio.sleep(10)
        except TelegramError as e:
            logger.warning("[Telegram polling] %s \u2014 reintentando en 10 s", e)
            await asyncio.sleep(10)
        except Exception as e:
            logger.error("[Telegram polling] Error inesperado: %s", e)
            await asyncio.sleep(10)


def setup_telegram_commands() -> asyncio.Task | None:
    global _ALLOWED_CHAT
    _ALLOWED_CHAT = os.getenv("TELEGRAM_CHAT_ID", "")
    token = os.getenv("TELEGRAM_TOKEN", "")
    if not token:
        logger.info("[Telegram] Sin TELEGRAM_TOKEN \u2014 comandos no disponibles.")
        return None
    return asyncio.create_task(_polling_loop(), name="telegram_polling")
