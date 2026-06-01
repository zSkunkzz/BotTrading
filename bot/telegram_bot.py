import asyncio
import logging
import os
from telegram import Bot, Update
from telegram.error import TelegramError

logger = logging.getLogger("TelegramBot")

_bot: Bot | None = None


def _get_bot() -> Bot | None:
    global _bot
    if _bot is None:
        token = os.getenv("TELEGRAM_TOKEN", "")
        if token:
            _bot = Bot(token=token)
    return _bot


def _chat() -> str:
    return os.getenv("TELEGRAM_CHAT_ID", "")


def _esc(text: str) -> str:
    """Escapa caracteres especiales HTML para mensajes de Telegram."""
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
    """Alias publico para enviar mensajes directos."""
    await _send(text)


async def notify_startup(pairs: list, dry_run: bool, top_n: int):
    mode = "\U0001f9ea DRY RUN" if dry_run else "\U0001f4b0 REAL MONEY"
    pairs_str = _esc(", ".join(pairs[:10])) + (" ..." if len(pairs) > 10 else "")
    text = (
        f"\U0001f916 <b>HyperliquidBot arrancado</b> — {mode}\n"
        f"Pares activos ({top_n}): <code>{pairs_str}</code>"
    )
    await _send(text)


async def notify_open(
    symbol,
    side,
    price,
    leverage,
    usdt=None,
    sl=None,
    tp1=None,
    tp2=None,
    tp3=None,
    dry_run=False,
):
    emoji = "\U0001f4c8" if side == "long" else "\U0001f4c9"
    mode  = " [DRY]" if dry_run else ""
    lines = [
        f"{emoji} <b>OPEN {_esc(side.upper())}</b>{mode} <code>{_esc(symbol)}</code>",
        f"Entry: <code>{_esc(price)}</code> | Lev: <code>{_esc(leverage)}x</code>"
        + (f" | Size: <code>{_esc(usdt)}$</code>" if usdt is not None else ""),
    ]
    if sl:
        lines.append(f"SL: <code>{_esc(sl)}</code>")
    tps = [tp for tp in (tp1, tp2, tp3) if tp is not None]
    if tps:
        lines.append("TP: " + " / ".join(f"<code>{_esc(t)}</code>" for t in tps))
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
    """Alerta urgente cuando un cierre falla en Hyperliquid — posición sigue abierta."""
    await _send(
        f"\u26a0\ufe0f <b>\u274c CIERRE FALLIDO</b> <code>{_esc(symbol)}</code>\n"
        f"Razón: {_esc(reason)}\n"
        f"Error: <code>{_esc(error[:200])}</code>\n"
        f"<b>\u26a0\ufe0f POSICIÓN SIGUE ABIERTA — revisar manualmente</b>"
    )


async def notify_tp_partial(symbol, side, price, tp_level: int = 2, ratio: float = 0.5, dry_run: bool = False):
    """Notifica un cierre parcial de TP."""
    mode = " [DRY]" if dry_run else ""
    await _send(
        f"\u2702\ufe0f <b>TP{tp_level} PARCIAL</b>{mode} <code>{_esc(symbol)}</code>\n"
        f"Cerrado <code>{ratio*100:.0f}%</code> de la posición {_esc(side.upper())} @ <code>{_esc(price)}</code>\n"
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
    """Alerta de kill switch activado."""
    level_labels = {
        1: "\u26a0\ufe0f L1 — Nuevas entradas pausadas",
        2: "\U0001f6d1 L2 — Símbolo/estrategia halted",
        3: "\U0001f6a8 L3 — Órdenes bloqueadas (re-arm manual)",
        4: "\U0001f480 L4 — HARD KILL (re-arm manual)",
    }
    label = level_labels.get(level, f"L{level}")
    await _send(
        f"\U0001f6d1 <b>KILL SWITCH ACTIVADO</b>\n"
        f"Nivel: <b>{label}</b>\n"
        f"Trigger: <code>{_esc(trigger[:300])}</code>\n"
        f"{'⚠️ RE-ARM MANUAL requerido' if level >= 3 else '✅ Se puede resetear automáticamente'}"
    )


# ── Comandos Telegram (polling ligero, sin Application) ───────────────────────

_ALLOWED_CHAT: str = ""   # se rellena en setup_telegram_commands()


async def _handle_update(update: Update) -> None:
    """Despacha un update entrante al handler correcto."""
    msg = update.message
    if not msg or not msg.text:
        return

    # Seguridad: solo responder al chat autorizado
    if _ALLOWED_CHAT and str(msg.chat_id) != _ALLOWED_CHAT:
        logger.warning("[Telegram] Mensaje ignorado de chat no autorizado: %s", msg.chat_id)
        return

    text = msg.text.strip()
    if text.startswith("/resetks"):
        parts = text.split()
        args  = parts[1:]
        await _cmd_resetks(msg.chat_id, args)
    elif text.startswith("/ksstatus"):
        await _cmd_resetks(msg.chat_id, ["status"])


async def _cmd_resetks(chat_id: int | str, args: list[str]) -> None:
    """
    Lógica del comando /resetks.

    /resetks status           → estado actual del KS
    /resetks <clave>          → re-arm completo (cualquier nivel)
    /resetks <clave> daily    → solo resetear contadores diarios
    """
    from bot.kill_switch import kill_switch  # diferida para evitar circular

    bot = _get_bot()
    if not bot:
        return

    async def reply(text: str):
        try:
            await bot.send_message(chat_id=chat_id, text=text, parse_mode="HTML")
        except TelegramError as e:
            logger.warning("[Telegram cmd_resetks] %s", e)

    # ── /resetks  o  /resetks status ────────────────────────────────
    if not args or args[0].lower() == "status":
        lvl     = kill_switch.level()
        trigger = kill_switch._trigger or "—"
        halted  = ", ".join(kill_switch._halted_symbols) or "ninguno"
        hard    = "SÍ 💀" if kill_switch.is_hard_killed() else "no"
        daily   = kill_switch._daily_pnl
        consec  = kill_switch._consec_losses
        await reply(
            f"🛑 <b>Kill Switch — Estado actual</b>\n"
            f"Nivel: <b>L{lvl}</b> {'✅ OK' if lvl == 0 else '⚠️ ACTIVO'}\n"
            f"Trigger: <code>{_esc(trigger[:200])}</code>\n"
            f"Hard killed: {hard}\n"
            f"Símbolos pausados: <code>{_esc(halted)}</code>\n"
            f"PnL diario acum: <code>{daily:+.2f}%</code>\n"
            f"Pérdidas consecutivas: <code>{consec}</code>"
        )
        return

    key        = args[0]
    daily_only = len(args) >= 2 and args[1].lower() == "daily"

    # ── /resetks <clave> daily ───────────────────────────────────────
    if daily_only:
        if key != os.getenv("KILL_SWITCH_REARM_KEY", "REARM-BOTTRADING"):
            await reply("🚫 <b>Clave incorrecta.</b> Kill switch sin cambios.")
            return
        kill_switch.reset_daily_pnl()
        await reply(
            "✅ <b>Contadores diarios reseteados</b>\n"
            "PnL diario, pérdidas consecutivas y reconexiones API → 0\n"
            "El <b>nivel del KS no ha cambiado</b>."
        )
        return

    # ── /resetks <clave>  →  re-arm completo ────────────────────────
    ok = await kill_switch.manual_reset(key)
    if ok:
        await reply(
            "✅ <b>Kill Switch RE-ARMADO</b>\n"
            "Nivel: <b>L0 — OK</b>\n"
            "Todos los contadores y flags reseteados.\n"
            "El bot puede volver a abrir posiciones."
        )
        # Notificar también en el canal de alertas
        await _send(
            "🔓 <b>Kill Switch re-armado manualmente</b> vía Telegram.\n"
            "Bot vuelve a estar operativo."
        )
    else:
        await reply(
            "🚫 <b>Clave incorrecta.</b>\n"
            "El kill switch sigue activo."
        )


async def _polling_loop() -> None:
    """
    Loop de long-polling ligero usando Bot.get_updates directamente.
    Corre como asyncio.Task; se cancela con el resto del programa.
    Compatible con el Bot directo ya existente — no necesita Application.
    """
    bot = _get_bot()
    if not bot:
        logger.info("[Telegram polling] Sin token — comandos desactivados.")
        return

    offset: int | None = None
    logger.info("[Telegram polling] Iniciado — escuchando /resetks y /ksstatus")

    while True:
        try:
            updates = await bot.get_updates(
                offset=offset,
                timeout=30,
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
        except TelegramError as e:
            logger.warning("[Telegram polling] %s — reintentando en 10 s", e)
            await asyncio.sleep(10)
        except Exception as e:
            logger.error("[Telegram polling] Error inesperado: %s", e)
            await asyncio.sleep(10)


def setup_telegram_commands() -> asyncio.Task | None:
    """
    Lanza el loop de polling como asyncio.Task.
    Llamar desde main() DESPUÉS de que el event loop esté corriendo.

    Retorna la Task (para cancelarla al shutdown) o None si no hay token.
    """
    global _ALLOWED_CHAT
    _ALLOWED_CHAT = os.getenv("TELEGRAM_CHAT_ID", "")

    token = os.getenv("TELEGRAM_TOKEN", "")
    if not token:
        logger.info("[Telegram] Sin TELEGRAM_TOKEN — comandos no disponibles.")
        return None

    return asyncio.create_task(_polling_loop(), name="telegram_polling")
