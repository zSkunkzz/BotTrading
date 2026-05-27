import logging
import os
import aiohttp

logger = logging.getLogger("Telegram")

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
BOT_URL = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}"


async def send(text: str, parse_mode="HTML"):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        async with aiohttp.ClientSession() as s:
            await s.post(
                f"https://api.telegram.org/bot{os.getenv('TELEGRAM_TOKEN','')}/sendMessage",
                json={"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": parse_mode},
                timeout=aiohttp.ClientTimeout(total=8),
            )
    except Exception as e:
        logger.warning(f"Telegram send fall\u00f3: {e}")


async def notify_open(symbol, side, price, leverage, usdt, dry=False):
    emoji = "\ud83d\udcc8" if side == "long" else "\ud83d\udcc9"
    mode = "\U0001f9ea DRY" if dry else "\U0001f4b0 REAL"
    await send(
        f"{emoji} <b>{'LONG' if side == 'long' else 'SHORT'} ABIERTO</b> {mode}\n"
        f"Par: <code>{symbol}</code>\n"
        f"Precio: <b>{price}</b>\n"
        f"Capital: ${usdt} USDT | x{leverage}"
    )


async def notify_close(symbol, side, entry, exit_price, pnl_pct, reason, dry=False):
    emoji = "\u2705" if pnl_pct > 0 else "\u274c"
    mode = "\U0001f9ea DRY" if dry else "\U0001f4b0 REAL"
    await send(
        f"{emoji} <b>{'LONG' if side == 'long' else 'SHORT'} CERRADO</b> {mode}\n"
        f"Par: <code>{symbol}</code>\n"
        f"Entrada: {entry} \u2192 Salida: {exit_price}\n"
        f"PnL: <b>{pnl_pct:+.2f}%</b>\n"
        f"Motivo: {reason}"
    )


async def notify_ai_decision(symbol, action, confidence, reasoning):
    if action == "HOLD":
        return
    emoji = {"BUY": "\U0001f7e2", "SELL": "\U0001f534", "CLOSE": "\U0001f512"}.get(action, "\U0001f916")
    await send(
        f"{emoji} <b>IA: {action}</b> en <code>{symbol}</code>\n"
        f"Confianza: {confidence}/10\n"
        f"\U0001f4dd {reasoning}"
    )


async def notify_scanner_update(added, removed, active_count):
    if not added and not removed:
        return
    lines = [f"\U0001f50d <b>Scanner actualizado</b> \u2014 {active_count} pares activos"]
    if added:
        lines.append(f"\u2795 Nuevos: {', '.join(added)}")
    if removed:
        lines.append(f"\u2796 Eliminados: {', '.join(removed)}")
    await send("\n".join(lines))


async def notify_risk_block(symbol, reason):
    await send(f"\u26d4 <b>Operaci\u00f3n bloqueada</b> en <code>{symbol}</code>\n{reason}")


async def notify_startup(pairs, dry_run, top_n):
    mode = "\U0001f9ea DRY RUN" if dry_run else "\U0001f4b0 REAL MONEY"
    await send(
        f"\U0001f680 <b>BitgetProBot v5.1 iniciado</b>\n"
        f"Modo: {mode}\n"
        f"Pares activos ({top_n}): {', '.join(pairs[:10])}{'...' if len(pairs) > 10 else ''}"
    )


async def notify_daily_loss_limit(symbol, daily_pnl):
    await send(
        f"\U0001f6a8 <b>L\u00cdMITE DE P\u00c9RDIDA DIARIA</b>\n"
        f"PnL del d\u00eda: <b>{daily_pnl:+.2f}%</b>\n"
        f"Bot pausado hasta ma\u00f1ana."
    )
