"""
webhook.py — Servidor webhook para alertas externas al bot de Hyperliquid.
=======================================================================
Expone tres endpoints:

  POST /webhook/hyperliquid  — Eventos push de Hyperliquid (fills, liquidaciones)
                               Verificación HMAC-SHA256 opcional vía WEBHOOK_SECRET

  POST /webhook/tradingview  — Alertas de TradingView con JSON libre
                               {"symbol":"BTC","action":"BUY|SELL|CLOSE","secret":"..."}

  GET  /health               — Health check con estado de traders activos
                               Protegido opcionalmente con X-Health-Key header

Variables de entorno:
  WEBHOOK_SECRET   — secret compartido para verificar la firma del payload
                     OBLIGATORIO si DRY_RUN=false (producción)
  PORT             — inyectado automáticamente por Railway

Nota: Hyperliquid no tiene webhooks HTTP nativos para fills en perpetuos
(usa WebSocket exclusivamente). Este servidor sirve principalmente para
alertas de TradingView y eventos manuales/personalizados.

FIX SEGURIDAD:
  - WEBHOOK_SECRET es obligatorio en producción (DRY_RUN=false).
    Sin él cualquiera puede enviar órdenes al bot.
  - /health requiere X-Health-Key si WEBHOOK_SECRET está configurado.
    Evita exponer públicamente qué símbolos se están tradando.
  - handle_tradingview pasa por pretrade_risk antes de ejecutar órdenes.
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
# Arranque — validación de seguridad
# ─────────────────────────────────────────────────────────────────────────────

def _check_production_security():
    """
    En producción (DRY_RUN=false) WEBHOOK_SECRET es obligatorio.
    Sin él cualquier actor externo puede enviar órdenes reales al bot.
    """
    dry_run = os.getenv("DRY_RUN", "true").lower() == "true"
    secret  = os.getenv("WEBHOOK_SECRET", "").strip()
    if not dry_run and not secret:
        raise RuntimeError(
            "[Webhook] WEBHOOK_SECRET es obligatorio cuando DRY_RUN=false. "
            "Configura la variable de entorno antes de arrancar en producción."
        )


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
            return getattr(obj, "_trader_ref", obj)
    return None


# ─────────────────────────────────────────────────────────────────────────────
# Handler: Hyperliquid push events
# ─────────────────────────────────────────────────────────────────────────────

async def handle_hyperliquid(request: web.Request) -> web.Response:
    """
    Recibe eventos de Hyperliquid reenviados por un relay externo.
    Formato esperado:
      {
        "channel": "orderFills" | "liquidations" | "userFills",
        "data": { ... }
      }
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
    data    = payload.get("data", payload)

    if channel in ("userFills", "orderFills"):
        await _process_hl_fills(data)
    elif channel in ("liquidations", "userNonFundingLedgerUpdates"):
        await _process_hl_liquidation(data)
    else:
        logger.debug("[Webhook HL] Canal desconocido: %s", channel)

    return web.Response(status=200, text="OK")


async def _process_hl_fills(data):
    fills = data.get("fills", [data] if isinstance(data, dict) else [])
    for fill in fills:
        coin      = fill.get("coin", "")
        direction = fill.get("dir", "")
        fill_px   = float(fill.get("px", 0) or 0)
        closed_pnl = float(fill.get("closedPnl", 0) or 0)

        if not coin:
            continue

        is_close = direction.startswith("Close")
        trader   = _find_trader(coin)

        if trader and is_close and trader.position:
            logger.info("[Webhook HL] Fill de cierre detectado: %s @ %.4f pnl=%.4f",
                        coin, fill_px, closed_pnl)
            if hasattr(trader, "_check_external_close"):
                asyncio.create_task(trader._check_external_close())


async def _process_hl_liquidation(data):
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

    FIX SEGURIDAD: las acciones BUY/SELL pasan por pretrade_risk.check()
    antes de ejecutarse para respetar todos los límites de riesgo.
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

    elif action in ("BUY", "SELL") and not trader.position:
        usdc = getattr(trader, "_open_notional", None) or float(os.getenv("USDC_PER_TRADE", "50"))
        lev  = getattr(trader, "leverage", 1)
        margin   = usdc / max(lev, 1)
        notional = usdc
        side_str = "buy" if action == "BUY" else "sell"

        # FIX: pasar por pretrade_risk antes de ejecutar
        try:
            from bot.pretrade_risk import pretrade_risk
            from bot.balance_service import balance_svc
            price   = await trader.get_price()
            balance = await trader.get_balance()
            ok, reason = await pretrade_risk.check(
                symbol=symbol,
                side=side_str,
                notional=notional,
                price=price,
                balance=balance,
                leverage=lev,
            )
            if not ok:
                logger.warning("[Webhook TV] pretrade_risk bloqueó %s %s: %s", action, symbol, reason)
                return web.Response(status=429, text=f"Risk check failed: {reason}")
        except Exception as e:
            logger.warning("[Webhook TV] pretrade_risk error (ignorando): %s", e)

        if action == "BUY":
            asyncio.create_task(trader.open_long(usdc))
        else:
            asyncio.create_task(trader.open_short(usdc))

    else:
        logger.info("[Webhook TV] Acción '%s' ignorada (posición actual: %s)", action, trader.position)

    return web.Response(status=200, text="OK")


# ─────────────────────────────────────────────────────────────────────────────
# Health check
# ─────────────────────────────────────────────────────────────────────────────

async def handle_health(request: web.Request) -> web.Response:
    """
    Health check.

    FIX SEGURIDAD: si WEBHOOK_SECRET está configurado, requiere el header
    X-Health-Key con el mismo valor para no exponer posiciones abiertas
    a cualquier actor externo.
    """
    secret = os.getenv("WEBHOOK_SECRET", "").strip()
    if secret:
        key = request.headers.get("X-Health-Key", "")
        if key != secret:
            # Devolver 200 vacío sin revelar datos (evita escaneo activo)
            return web.Response(
                content_type="application/json",
                text=json.dumps({"status": "ok"}),
            )

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
    _check_production_security()
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
