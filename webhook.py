"""
webhook.py — Servidor webhook para eventos en tiempo real de Bitget
=======================================================================
Recibe notificaciones HTTP de Bitget (fills de órdenes, liquidaciones)
y las inyecta al trader activo correspondiente sin esperar el polling.

Cómo funciona:
  1. Railway expone el puerto $PORT (variable automática)
  2. En Bitget → API Management → Webhooks: apunta a
     https://<tu-railway-url>/webhook/bitget
  3. El webhook verifica la firma HMAC-SHA256 del payload
  4. Si es un fill de orden, llama a trader.close_position() o
     actualiza el estado según el evento

Variables de entorno necesarias:
  WEBHOOK_SECRET   — secret que pones en Bitget al crear el webhook
  WEBHOOK_PORT     — puerto (Railway lo inyecta como PORT automáticamente)

NOTA: Bitget no soporta webhooks push nativos para futuros en todas
las regiones. Este servidor también acepta alertas de TradingView
en el endpoint /webhook/tradingview con formato JSON libre.
"""

import asyncio
import hashlib
import hmac
import json
import logging
import os
from aiohttp import web
from bot.logger import setup_logger

logger = logging.getLogger("Webhook")

# Referencia global a los traders activos (inyectada desde main.py)
_active_traders: dict = {}


def register_traders(traders: dict):
    """Llamar desde main.py para dar acceso al webhook a los traders activos."""
    global _active_traders
    _active_traders = traders


# ─────────────────────────────────────────────────────────────────────────────
# Verificación de firma Bitget
# ─────────────────────────────────────────────────────────────────────────────

def _verify_bitget_signature(body: bytes, timestamp: str, signature: str) -> bool:
    secret = os.getenv("WEBHOOK_SECRET", "")
    if not secret:
        logger.warning("WEBHOOK_SECRET no configurado — verificación omitida")
        return True
    message = timestamp + body.decode("utf-8")
    expected = hmac.new(
        secret.encode("utf-8"),
        message.encode("utf-8"),
        hashlib.sha256
    ).hexdigest()
    return hmac.compare_digest(expected, signature)


# ─────────────────────────────────────────────────────────────────────────────
# Handler: Bitget order fills / liquidations
# ─────────────────────────────────────────────────────────────────────────────

async def handle_bitget(request: web.Request) -> web.Response:
    body = await request.read()
    timestamp = request.headers.get("X-Bitget-Timestamp", "")
    signature = request.headers.get("X-Bitget-Sign", "")

    if not _verify_bitget_signature(body, timestamp, signature):
        logger.warning("Webhook Bitget: firma inválida")
        return web.Response(status=401, text="Unauthorized")

    try:
        payload = json.loads(body)
    except json.JSONDecodeError:
        return web.Response(status=400, text="Invalid JSON")

    logger.info(f"Webhook Bitget recibido: {json.dumps(payload)[:200]}")

    # Estructura típica de Bitget: lista de eventos
    events = payload if isinstance(payload, list) else [payload]
    for event in events:
        await _process_bitget_event(event)

    return web.Response(status=200, text="OK")


async def _process_bitget_event(event: dict):
    """
    Procesa un evento de Bitget.
    Eventos relevantes:
      - action: "order" con status "filled" → orden ejecutada
      - action: "position" → cambio de posición (liquidación, cierre forzado)
    """
    action = event.get("action", "")
    data   = event.get("data", [{}])
    if isinstance(data, list):
        data = data[0] if data else {}

    inst_id = data.get("instId", "")          # ej. "BTCUSDT"
    order_type = data.get("ordType", "")
    state      = data.get("state", "")
    side       = data.get("side", "")          # "buy" | "sell"
    reduce_only = data.get("reduceOnly", False)

    if action == "orders" and state == "filled" and reduce_only:
        # Una orden reduceOnly filled = cierre de posición confirmado en exchange
        symbol_ccxt = inst_id  # ya en formato BTCUSDT (buscar en traders)
        trader = _find_trader(symbol_ccxt)
        if trader:
            logger.info(f"[Webhook] Cierre confirmado en exchange para {symbol_ccxt}")
            # Si el trader aún cree que tiene posición, sincronizar estado
            if trader.position:
                fill_price = float(data.get("avgPx", 0) or data.get("fillPx", 0) or 0)
                reason = f"Bitget fill confirmado @ {fill_price}"
                # Notificar y limpiar estado sin ejecutar nueva orden
                await trader._sync_closed_from_exchange(fill_price, reason)

    elif action == "positions":
        # Cambio de posición: puede ser liquidación
        pos_side   = data.get("posSide", "")
        margin_mode = data.get("marginMode", "")
        size       = float(data.get("pos", 0) or 0)
        symbol_ccxt = inst_id
        if size == 0:
            trader = _find_trader(symbol_ccxt)
            if trader and trader.position:
                logger.warning(f"[Webhook] Posición {symbol_ccxt} cerrada externamente (liquidación?)")
                await trader._sync_closed_from_exchange(0, "Cierre externo / liquidación")


def _find_trader(symbol: str):
    """Busca el trader por símbolo. Normaliza BTCUSDT → BTC/USDT:USDT."""
    # Intento directo
    if symbol in _active_traders:
        return getattr(_active_traders[symbol], '_trader_ref', None)
    # Intento normalizado: BTCUSDT → BTC/USDT:USDT
    for key, task in _active_traders.items():
        normalized = key.replace("/", "").replace(":", "").replace("USDT", "")
        incoming   = symbol.replace("USDT", "")
        if normalized == incoming:
            return getattr(task, '_trader_ref', None)
    return None


# ─────────────────────────────────────────────────────────────────────────────
# Handler: TradingView alerts (formato libre JSON)
# ─────────────────────────────────────────────────────────────────────────────

async def handle_tradingview(request: web.Request) -> web.Response:
    """
    Acepta alertas de TradingView con formato:
    {"symbol": "BTCUSDT", "action": "BUY" | "SELL" | "CLOSE", "secret": "..."}
    """
    secret = os.getenv("WEBHOOK_SECRET", "")
    try:
        payload = await request.json()
    except Exception:
        return web.Response(status=400, text="Invalid JSON")

    if secret and payload.get("secret") != secret:
        return web.Response(status=401, text="Unauthorized")

    symbol = payload.get("symbol", "")
    action = payload.get("action", "").upper()
    logger.info(f"[Webhook TradingView] {symbol} → {action}")

    trader = _find_trader(symbol)
    if not trader:
        logger.warning(f"[Webhook] Trader no encontrado para {symbol}")
        return web.Response(status=404, text=f"No trader for {symbol}")

    if action == "CLOSE" and trader.position:
        asyncio.create_task(trader.close_position("TradingView alert"))
    elif action == "BUY" and not trader.position:
        asyncio.create_task(trader.open_long(trader.usdt_amount))
    elif action == "SELL" and not trader.position:
        asyncio.create_task(trader.open_short(trader.usdt_amount))

    return web.Response(status=200, text="OK")


# ─────────────────────────────────────────────────────────────────────────────
# Health check
# ─────────────────────────────────────────────────────────────────────────────

async def handle_health(request: web.Request) -> web.Response:
    active = len(_active_traders)
    return web.Response(
        content_type="application/json",
        text=json.dumps({"status": "ok", "traders_active": active})
    )


# ─────────────────────────────────────────────────────────────────────────────
# Servidor
# ─────────────────────────────────────────────────────────────────────────────

async def start_webhook_server():
    port = int(os.getenv("PORT", os.getenv("WEBHOOK_PORT", "8080")))
    app  = web.Application()
    app.router.add_post("/webhook/bitget",      handle_bitget)
    app.router.add_post("/webhook/tradingview",  handle_tradingview)
    app.router.add_get("/health",               handle_health)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    logger.info(f"🌐 Webhook server escuchando en puerto {port}")
    logger.info(f"   POST /webhook/bitget")
    logger.info(f"   POST /webhook/tradingview")
    logger.info(f"   GET  /health")
    return runner
