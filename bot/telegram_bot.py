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
        f"{'\\u26a0\\ufe0f RE-ARM MANUAL requerido' if level >= 3 else '\\u2705 Se puede resetear automáticamente'}"
    )
