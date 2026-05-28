import logging
import os
from telegram import Bot
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


async def notify_startup(pairs: list, dry_run: bool, top_n: int):
    mode = "\U0001f9ea DRY RUN" if dry_run else "\U0001f4b0 REAL MONEY"
    pairs_str = _esc(", ".join(pairs[:10])) + (" ..." if len(pairs) > 10 else "")
    text = (
        f"\U0001f916 <b>BitgetProBot arrancado</b> — {mode}\n"
        f"Pares activos ({top_n}): <code>{pairs_str}</code>"
    )
    await _send(text)


async def notify_open(symbol, side, price, leverage, usdt, dry_run):
    emoji = "\U0001f4c8" if side == "long" else "\U0001f4c9"
    mode  = " [DRY]" if dry_run else ""
    await _send(
        f"{emoji} <b>OPEN {_esc(side.upper())}</b>{mode} <code>{_esc(symbol)}</code>\n"
        f"Entry: <code>{_esc(price)}</code> | Lev: <code>{_esc(leverage)}x</code> | Size: <code>{_esc(usdt)}$</code>"
    )


async def notify_close(symbol, side, entry, exit_p, pnl, reason, dry_run):
    emoji = "\u2705" if pnl > 0 else "\u274c"
    mode  = " [DRY]" if dry_run else ""
    await _send(
        f"{emoji} <b>CLOSE {_esc(side.upper())}</b>{mode} <code>{_esc(symbol)}</code>\n"
        f"Entry: <code>{_esc(entry)}</code> \u2192 Exit: <code>{_esc(exit_p)}</code>\n"
        f"PnL: <code>{pnl:+.2f}%</code> | {_esc(reason)}"
    )


async def notify_close_failed(symbol, reason, error):
    """Alerta urgente cuando un cierre falla en Bitget — posición sigue abierta."""
    await _send(
        f"\u26a0\ufe0f <b>\u274c CIERRE FALLIDO</b> <code>{_esc(symbol)}</code>\n"
        f"Razón: {_esc(reason)}\n"
        f"Error: <code>{_esc(error[:200])}</code>\n"
        f"<b>\u26a0\ufe0f POSICIÓN SIGUE ABIERTA — revisar manualmente</b>"
    )


async def notify_tp_partial(symbol, side, price, tp_level: int, ratio: float):
    """Notifica un cierre parcial de TP."""
    await _send(
        f"\u2702\ufe0f <b>TP{tp_level} PARCIAL</b> <code>{_esc(symbol)}</code>\n"
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
