"""
webhook.py — Servidor webhook para alertas externas al bot de Hyperliquid.
=======================================================================
Expone tres endpoints:

  POST /webhook/hyperliquid  — Eventos push de Hyperliquid (fills, liquidaciones)
                               Verificación HMAC-SHA256 opcional vía WEBHOOK_SECRET

  POST /webhook/tradingview  — Alertas de TradingView con JSON libre
                               {"symbol":"BTC","action":"BUY|SELL|CLOSE","secret":"..."}

  GET  /health               — Health check con estado de traders activos

Variables de entorno:
  WEBHOOK_SECRET   — secret compartido para verificar la firma del payload
  PORT             — inyectado automáticamente por Railway

Nota: Hyperliquid no tiene webhooks HTTP nativos para fills en perpetuos
(usa WebSocket exclusivamente). Este servidor sirve principalmente para
alertas de TradingView y eventos manuales/personalizados.
"""

import asyncio
import hashlib
import hmac
import json
import logging
import os
import time
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
# Utils
# ─────────────────────────────────────────────────────────────────────────────

def _norm_coin(symbol: str) -> str:
    """BTCUSDT / BTC/USDT:USDT / BTC → BTC"""
    s = symbol.replace("/", "").replace(":USDT", "").upper()
    if s.endswith("USDTUSDT"):
        s = s[:-4]
    if s.endswith("USDT"):
        s = s[:-4]
    return s


def _verify_signature(body: bytes, timestamp: str, signature: str) -> bool:
    """Verifica firma HMAC-SHA256 genérica (timestamp + body)."""
    secret = os.getenv("WEBHOOK_SECRET", "")
    if not secret:
        logger.debug("WEBHOOK_SECRET no configurado — verificación omitida")
        return True
    message  = timestamp + body.decode("utf-8")
    expected = hmac.new(
        secret.encode("utf-8"),
        message.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    return hmac.compare_digest(expected, signature)


def _find_trader(symbol: str):
    """
    Busca el trader por coin normalizado.
    Acepta 'BTC', 'BTCUSDT', 'BTC/USDT:USDT', etc.
    Devuelve el objeto FuturesTrader o None.
    """
    coin = _norm_coin(symbol)
    for key, obj in _active_traders.items():
        if _norm_coin(key) == coin:
            # main.py puede guardar directamente el trader o en '_trader_ref'
            return getattr(obj, "_trader_ref", obj)
    return None


# ─────────────────────────────────────────────────────────────────────────────
# Handler: Hyperliquid push events
# ─────────────────────────────────────────────────────────────────────────────

async def handle_hyperliquid(request: web.Request) -> web.Response:
    """
    Recibe eventos de Hyperliquid reenviados por un relay externo o
    por integraciones propias. Formato esperado:
      {
        "channel": "orderFills" | "liquidations" | "userFills",
        "data": { ... }
      }
    También acepta el formato WebSocket nativo de Hyperliquid.
    """
    body      = await request.read()
    timestamp = request.headers.get("X-Timestamp", str(int(time.time())))
    signature = request.headers.get("X-Signature", "")

    if signature and not _verify_signature(body, timestamp, signature):
        logger.warning("[Webhook HL] Firma inválida")
        return web.Response(status=401, text="Unauthorized")

    try:
        payload = json.loads(body)
    except json.JSONDecodeError:
        return web.Response(status=400, text="Invalid JSON")

    logger.info("[Webhook HL] Recibido: %s", json.dumps(payload)[:300])

    channel = payload.get("channel", "")
    data    = payload.get("data", payload)  # soporte para payload plano

    if channel in ("userFills", "orderFills"):
        await _process_hl_fills(data)
    elif channel in ("liquidations", "userNonFundingLedgerUpdates"):
        await _process_hl_liquidation(data)
    else:
        logger.debug("[Webhook HL] Canal desconocido: %s", channel)

    return web.Response(status=200, text="OK")


async def _process_hl_fills(data):
    """
    Procesa fills de Hyperliquid.
    Estructura userFills:
      {
        "fills": [
          {"coin": "BTC", "side": "A", "sz": "0.001", "px": "67000",
           "oid": 123, "closedPnl": "5.2", "dir": "Open Long" | "Close Long" | ...}
        ]
      }
    side: "A" = Ask (sell), "B" = Bid (buy)
    """
    fills = data.get("fills", [data] if isinstance(data, dict) else [])
    for fill in fills:
        coin      = fill.get("coin", "")
        direction = fill.get("dir", "")    # "Open Long", "Close Long", "Open Short", "Close Short"
        fill_px   = float(fill.get("px", 0) or 0)
        closed_pnl = float(fill.get("closedPnl", 0) or 0)

        if not coin:
            continue

        is_close = direction.startswith("Close")
        trader   = _find_trader(coin)

        if trader and is_close and trader.position:
            logger.info("[Webhook HL] Fill de cierre detectado: %s @ %.4f pnl=%.4f",
                        coin, fill_px, closed_pnl)
            # Sincronizar estado si el exchange ya cerró la posición
            if hasattr(trader, "_check_external_close"):
                asyncio.create_task(trader._check_external_close())


async def _process_hl_liquidation(data):
    """
    Procesa eventos de liquidación o cambios de margin.
    Estructura: {"coin": "BTC", "liqPx": "65000", ...}
    """
    coin = data.get("coin", "")
    if not coin:
        return
    trader = _find_trader(coin)
    if trader and trader.position:
        logger.warning("[Webhook HL] ⚠️ Evento de liquidación para %s", coin)
        asyncio.create_task(trader._check_external_close())


# ─────────────────────────────────────────────────────────────────────────────
# Handler: TradingView alerts
# ─────────────────────────────────────────────────────────────────────────────

async def handle_tradingview(request: web.Request) -> web.Response:
    """
    Acepta alertas de TradingView.
    Formato JSON:
      {"symbol": "BTC", "action": "BUY" | "SELL" | "CLOSE", "secret": "..."}
    symbol puede ser BTC, BTCUSDT, BTC/USDT:USDT — se normaliza automáticamente.
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
    logger.info("[Webhook TV] %s → %s", symbol, action)

    trader = _find_trader(symbol)
    if not trader:
        logger.warning("[Webhook TV] Trader no encontrado para %s", symbol)
        return web.Response(status=404, text=f"No trader for {symbol}")

    if action == "CLOSE" and trader.position:
        asyncio.create_task(trader.close_position("TradingView"))
    elif action == "BUY" and not trader.position:
        usdc = getattr(trader, "_open_notional", None) or float(os.getenv("USDC_PER_TRADE", "50"))
        asyncio.create_task(trader.open_long(usdc))
    elif action == "SELL" and not trader.position:
        usdc = getattr(trader, "_open_notional", None) or float(os.getenv("USDC_PER_TRADE", "50"))
        asyncio.create_task(trader.open_short(usdc))
    else:
        logger.info("[Webhook TV] Acción '%s' ignorada (posición actual: %s)", action, trader.position)

    return web.Response(status=200, text="OK")


# ─────────────────────────────────────────────────────────────────────────────
# Health check
# ─────────────────────────────────────────────────────────────────────────────

async def handle_health(request: web.Request) -> web.Response:
    positions = {
        k: getattr(getattr(v, "_trader_ref", v), "position", None)
        for k, v in _active_traders.items()
    }
    return web.Response(
        content_type="application/json",
        text=json.dumps({
            "status":          "ok",
            "exchange":        "hyperliquid",
            "traders_active":  len(_active_traders),
            "positions":       positions,
        }),
    )


# ─────────────────────────────────────────────────────────────────────────────
# Servidor
# ─────────────────────────────────────────────────────────────────────────────

async def start_webhook_server():
    port = int(os.getenv("PORT", os.getenv("WEBHOOK_PORT", "8080")))
    app  = web.Application()
    app.router.add_post("/webhook/hyperliquid", handle_hyperliquid)
    app.router.add_post("/webhook/tradingview",  handle_tradingview)
    app.router.add_get("/health",               handle_health)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    logger.info("🌐 Webhook server escuchando en puerto %d", port)
    logger.info("   POST /webhook/hyperliquid")
    logger.info("   POST /webhook/tradingview")
    logger.info("   GET  /health")
    return runner
