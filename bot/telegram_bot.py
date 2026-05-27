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


async def _send(text: str):
    bot  = _get_bot()
    chat = _chat()
    if not bot or not chat:
        return
    try:
        await bot.send_message(chat_id=chat, text=text, parse_mode="Markdown")
    except TelegramError as e:
        logger.warning(f"[Telegram] {e}")


async def notify_startup(pairs: list, dry_run: bool, top_n: int):
    mode = "🧪 DRY RUN" if dry_run else "💰 REAL MONEY"
    text = (
        f"🤖 *BitgetProBot arrancado* — {mode}\n"
        f"Pares activos ({top_n}): {', '.join(pairs[:10])}"
        + (" ..." if len(pairs) > 10 else "")
    )
    await _send(text)


async def notify_open(symbol, side, price, leverage, usdt, dry_run):
    emoji = "📈" if side == "long" else "📉"
    mode  = " \[DRY]" if dry_run else ""
    await _send(
        f"{emoji} *OPEN {side.upper()}*{mode} `{symbol}`\n"
        f"Entry: `{price}` | Lev: `{leverage}x` | Size: `{usdt}$`"
    )


async def notify_close(symbol, side, entry, exit_p, pnl, reason, dry_run):
    emoji = "✅" if pnl > 0 else "❌"
    mode  = " \[DRY]" if dry_run else ""
    await _send(
        f"{emoji} *CLOSE {side.upper()}*{mode} `{symbol}`\n"
        f"Entry: `{entry}` → Exit: `{exit_p}`\n"
        f"PnL: `{pnl:+.2f}%` | {reason}"
    )


async def notify_tp_partial(symbol, side, price, tp_level: int, ratio: float):
    """Notifica un cierre parcial de TP."""
    await _send(
        f"✂️ *TP{tp_level} PARCIAL* `{symbol}`\n"
        f"Cerrado `{ratio*100:.0f}%` de la posición {side.upper()} @ `{price}`\n"
        f"SL movido a *break-even*"
    )


async def notify_ai_decision(symbol, action, score, reason):
    emojis = {"BUY": "🟢", "SELL": "🔴", "CLOSE": "🔒", "HOLD": "⚪"}
    emoji  = emojis.get(action, "❓")
    await _send(
        f"{emoji} *IA → {action}* `{symbol}` Score: `{score}/10`\n"
        f"{reason[:300]}"
    )


async def notify_risk_block(symbol, reason):
    await _send(f"⛔ *RISK BLOCK* `{symbol}`\n{reason}")


async def notify_scanner_update(added: set, removed: set, total: int):
    parts = [f"🔍 *Scanner update* — {total} pares activos"]
    if added:   parts.append(f"➕ Añadidos: {', '.join(added)}")
    if removed: parts.append(f"➖ Retirados: {', '.join(removed)}")
    await _send("\n".join(parts))
