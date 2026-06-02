#!/usr/bin/env python3
"""
bot/trader.py — FuturesTrader: punto de entrada público para main.py.

Historia:
  Durante la refactorización a bot/core/, este fichero quedó como un
  placeholder que importaba 'AiTrader' desde ai_trader.py. Dicha clase
  nunca existió (ai_trader.py solo tiene funciones de módulo), causando
  un ImportError fatal en cada arranque.

  Esta versión restaura FuturesTrader como una clase concreta que:
    - Acepta la firma completa que main.py espera
    - Delega el loop principal a TradingLoop (bot/core/trading_loop.py)
    - Expone los atributos que main.py necesita: _stopped_event, position,
      cleanup(), run(risk, global_risk=...)
"""
from __future__ import annotations

import asyncio
import logging
import os
from typing import Optional

from bot.core.hl_client import HLClient, _norm_coin
from bot.core.trading_loop import TradingLoop

logger = logging.getLogger("FuturesTrader")

_USE_TESTNET = os.getenv("HL_TESTNET", "").lower() in ("true", "1", "yes")
_API_URL = (
    "https://api.hyperliquid-testnet.xyz"
    if _USE_TESTNET
    else "https://api.hyperliquid.xyz"
)


class FuturesTrader:
    """
    Orquestador principal de un par de trading en Hyperliquid.

    Parámetros
    ----------
    api_key      : dirección del wallet principal (HL_API_WALLET_ADDRESS)
    api_secret   : private key del agente o del wallet principal
    passphrase   : no usado (compatibilidad con firma ccxt-like)
    symbol       : e.g. "BTC", "ETH", "SOL"
    leverage     : apalancamiento deseado (ej. 5)
    margin_mode  : "isolated" | "cross"  (informativo — HL siempre cross perp)
    dry_run      : si True, las órdenes se loggean pero no se envían
    """

    def __init__(
        self,
        api_key: Optional[str],
        api_secret: str,
        passphrase: Optional[str],
        symbol: str,
        leverage: int = 5,
        margin_mode: str = "isolated",
        dry_run: bool = True,
    ) -> None:
        self.symbol      = symbol
        self.coin        = _norm_coin(symbol)
        self.leverage    = leverage
        self.margin_mode = margin_mode
        self.dry_run     = dry_run

        # Estado de posición — gestionado por TradingLoop / PositionManager
        self.position:        Optional[str]   = None   # "long" | "short" | None
        self.entry_price:     Optional[float] = None
        self.sl:              Optional[float] = None
        self.tp1:             Optional[float] = None
        self.tp2:             Optional[float] = None
        self.tp3:             Optional[float] = None
        self.tp2_hit:         bool            = False
        self._open_notional:  float           = 0.0
        self._open_leverage:  int             = leverage
        self._protection_ok:  bool            = False
        self._tp1_be_done:    bool            = False

        # Credenciales (usadas por _get_ccxt y _set_leverage)
        self._api_key    = api_key or ""
        self._api_secret = api_secret or ""

        # Cliente HL sincrónico (órdenes, posiciones, metadatos)
        self._hl_client = HLClient(symbol)

        # Alias de atributos que trading_loop.py y otros módulos esperan
        self._master_addr = self._hl_client._account_addr
        self._agent_mode  = self._hl_client._agent_mode

        # Event para parada limpia — main.py espera _stopped_event.wait()
        self._stopped_event = asyncio.Event()

        # Loop delegado
        self._trading_loop = TradingLoop(symbol)

        # ccxt exchange (lazy-init en _get_ccxt)
        self._ccxt_exchange = None

    # ── Interfaz pública requerida por main.py ─────────────────────

    async def run(self, risk, *, global_risk=None) -> None:
        """Arranca el loop principal. Se cancela desde fuera vía task.cancel()."""
        try:
            await self._trading_loop.run(self, risk, global_risk=global_risk)
        except asyncio.CancelledError:
            logger.info("[%s] FuturesTrader cancelado.", self.symbol)
        finally:
            self._stopped_event.set()

    async def cleanup(self) -> None:
        """Limpieza post-stop (cerrar sesiones HTTP, etc.)."""
        try:
            from bot.ai_trader import close_sessions
            await close_sessions()
        except Exception as e:
            logger.debug("[%s] cleanup ai_trader sessions: %s", self.symbol, e)
        self._stopped_event.set()

    # ── Métodos que TradingLoop llama sobre el objeto trader ────────

    async def _get_ccxt(self) -> None:
        """
        Inicialización lazy del cliente ccxt / SDK.
        TradingLoop llama esto en _init() antes del loop.
        En esta arquitectura el SDK ya está listo en HLClient,
        así que solo nos aseguramos de que el cliente HL esté inicializado.
        """
        # _HLCore ya está inicializado al crear HLClient — noop aquí.
        pass

    async def get_price(self) -> float:
        """Obtiene el precio mid actual del par."""
        import aiohttp
        import json as _json

        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{_API_URL}/info",
                json={"type": "allMids"},
                headers={"Content-Type": "application/json"},
                timeout=aiohttp.ClientTimeout(total=8),
            ) as resp:
                data = _json.loads(await resp.text())

        price = data.get(self.coin)
        if price is None:
            raise ValueError(f"[{self.symbol}] Precio no encontrado en allMids")
        return float(price)

    async def _get_positions(self) -> list[dict]:
        """Devuelve lista de posiciones abiertas en el exchange para este coin."""
        import aiohttp
        import json as _json

        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{_API_URL}/info",
                json={"type": "clearinghouseState", "user": self._master_addr},
                headers={"Content-Type": "application/json"},
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                data = _json.loads(await resp.text())

        result = []
        for p in data.get("assetPositions", []):
            pos = p.get("position", {})
            if pos.get("coin", "").upper() != self.coin.upper():
                continue
            try:
                szi = float(pos.get("szi", 0) or 0)
            except (TypeError, ValueError):
                continue
            if abs(szi) == 0:
                continue
            result.append({
                "side":    "long" if szi > 0 else "short",
                "size":    abs(szi),
                "entryPx": float(pos.get("entryPx") or 0),
                "coin":    pos.get("coin", ""),
            })
        return result

    async def _set_leverage(self, leverage: int) -> None:
        """Configura el apalancamiento en el exchange."""
        if self.dry_run:
            logger.info("[%s] DRY_RUN: _set_leverage(%d) omitido.", self.symbol, leverage)
            return
        try:
            result = self._hl_client._exchange.update_leverage(
                leverage, self.coin, is_cross=False
            )
            logger.info("[%s] Leverage configurado a %dx: %s", self.symbol, leverage, result)
        except Exception as e:
            logger.warning("[%s] No se pudo configurar leverage: %s", self.symbol, e)

    def _info_post(self, payload: dict) -> dict:
        """Wrapper síncrono para llamadas /info (balance_svc lo usa)."""
        return self._hl_client._info._session.post(
            f"{_API_URL}/info", json=payload
        ).json()


__all__ = ["FuturesTrader"]
