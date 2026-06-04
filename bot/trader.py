#!/usr/bin/env python3
"""
bot/trader.py — FuturesTrader: thin orchestrator para un par de trading en Hyperliquid.

Responsabilidades:
  - Ciclo de vida (run / cleanup) delegado a TradingLoop.
  - Bootstrap del SDK exchange-specific (_get_ccxt, _require_hl).
  - Primitivas de exchange que el executor y el position manager necesitan vía interfaz:
      get_price(), _fetch_all_mids(), _place_tpsl(), _round_qty(), _set_leverage().
  - Estado de posición inicializado aquí (position, entry_price, sl, tp*, …).
  - _get_positions(): consulta posiciones abiertas al exchange y normaliza el resultado.

Lo que NO vive aquí (ya extraído a módulos propios):
  - OHLCV            → bot/ohlcv.py  (o OHLCVFetcher)
  - Sincronización de posición → bot/position_manager.py  (PositionSync)
  - Staleness / rescale de entrada → bot/trader_helpers.py
  - Sizing Kelly     → bot/kelly_sizer.py
  - Lógica fill-confirm + SL/TP al abrir → bot/order_executor.py

Historial de fixes relevantes: véase CHANGELOG.md.
"""
from __future__ import annotations

import asyncio
import json as _json
import logging
import os
from typing import Optional

import aiohttp

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
    Thin orchestrator de un par de trading en Hyperliquid.

    Recibe todas sus dependencias por inyección en __init__ y expone
    solo las primitivas de exchange que los módulos satélite necesitan.
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

        self._api_key    = api_key or ""
        self._api_secret = api_secret or ""

        # FIX FREEZE: NO crear HLClient aquí (bloquea el event loop).
        # Se inicializa de forma lazy en _get_ccxt() desde dentro del loop.
        self._hl_client: Optional[HLClient] = None
        self._master_addr: str = ""
        self._agent_mode:  bool = False

        # Caché del último precio válido (resiliencia ante fallos de allMids)
        self._last_price: float = 0.0

        # ── Estado de posición ────────────────────────────────────────────────
        # Estos atributos son escritos por TradingLoop._init() (restauración
        # desde disco) y por TradingLoop._iteration() (sincronización con el
        # exchange). Se declaran aquí para que existan antes de cualquier lectura
        # y para dejar claro cuál es el estado inicial de un trader recién creado.
        self.position:       Optional[str]   = None   # "long" | "short" | None
        self.entry_price:    Optional[float] = None
        self.sl:             Optional[float] = None
        self.tp1:            Optional[float] = None
        self.tp2:            Optional[float] = None
        self.tp3:            Optional[float] = None
        self.tp2_hit:        bool            = False
        self._open_notional: float           = 0.0
        self._open_leverage: int             = leverage
        self._open_qty:      float           = 0.0
        self._protection_ok: bool            = False
        self._tp1_be_done:   bool            = False

        self._stopped_event = asyncio.Event()
        self._trading_loop  = TradingLoop(symbol)
        self._ccxt_exchange  = None

    # ── Ciclo de vida ─────────────────────────────────────────────

    async def run(self, risk, *, global_risk=None) -> None:
        try:
            await self._trading_loop.run(self, risk, global_risk=global_risk)
        except asyncio.CancelledError:
            logger.info("[%s] FuturesTrader cancelado.", self.symbol)
        finally:
            self._stopped_event.set()

    async def cleanup(self) -> None:
        try:
            from bot.ai_trader import close_sessions
            await close_sessions()
        except Exception as e:
            logger.debug("[%s] cleanup ai_trader sessions: %s", self.symbol, e)
        self._stopped_event.set()

    # ── Bootstrap del SDK (exchange-specific) ────────────────────

    async def _get_ccxt(self) -> None:
        """Inicializa HLClient de forma lazy y async-safe (primera llamada desde el loop)."""
        if self._hl_client is not None:
            return
        try:
            logger.info("[%s] _get_ccxt: inicializando HLClient...", self.symbol)
            self._hl_client = await HLClient.create(self.symbol)
            self._master_addr = self._hl_client._account_addr
            self._agent_mode  = self._hl_client._agent_mode
            logger.info(
                "[%s] _get_ccxt: HLClient listo | addr=%s | agente=%s",
                self.symbol,
                self._master_addr[:10] + "..." if self._master_addr else "N/A",
                self._agent_mode,
            )
        except Exception as e:
            logger.error("[%s] _get_ccxt: error inicializando HLClient: %s", self.symbol, e)
            raise

    def _require_hl(self) -> Optional[HLClient]:
        """Devuelve _hl_client o loguea error si aún no se inicializó."""
        if self._hl_client is None:
            logger.error(
                "[%s] _hl_client no inicializado. ¿Se llamó _get_ccxt() antes?",
                self.symbol,
            )
            return None
        return self._hl_client

    # ── Posiciones ────────────────────────────────────────────────

    async def _get_positions(self) -> list:
        """
        Obtiene las posiciones abiertas de self.coin desde el exchange y
        normaliza el resultado al formato que TradingLoop espera:
            [{"side": "long"|"short", "entryPx": float, "size": float}, ...]

        HLClient.get_positions() devuelve los objetos crudos de HL:
            [{"position": {"coin": ..., "szi": ..., "entryPx": ..., ...}, ...}, ...]

        Filtra la posición activa (szi != 0) y convierte al formato interno.
        Devuelve [] si no hay posición, el cliente no está listo, o falla la llamada.
        """
        hl = self._require_hl()
        if hl is None:
            return []
        try:
            raw: list = await asyncio.to_thread(hl.get_positions)
        except Exception as e:
            logger.warning("[%s] _get_positions error: %s", self.symbol, e)
            return []

        result = []
        for entry in raw:
            pos = entry.get("position", {})
            szi_str = str(pos.get("szi", "0"))
            try:
                szi = float(szi_str)
            except (ValueError, TypeError):
                continue
            if szi == 0:
                continue
            side = "long" if szi > 0 else "short"
            try:
                entry_px = float(pos.get("entryPx") or 0)
            except (ValueError, TypeError):
                entry_px = 0.0
            result.append({
                "side":    side,
                "entryPx": entry_px,
                "size":    abs(szi),
            })
        return result

    # ── Precio live ───────────────────────────────────────────────

    async def _fetch_all_mids(self, session: aiohttp.ClientSession) -> Optional[dict]:
        """Llama al endpoint allMids y devuelve el dict o None si falla."""
        try:
            async with session.post(
                f"{_API_URL}/info",
                json={"type": "allMids"},
                headers={"Content-Type": "application/json"},
                timeout=aiohttp.ClientTimeout(total=8),
            ) as resp:
                text = await resp.text()
            data = _json.loads(text)
            if isinstance(data, dict):
                return data
            logger.warning(
                "[%s] allMids devolvió tipo inesperado (%s): %s",
                self.symbol, type(data).__name__, text[:120],
            )
            return None
        except Exception as e:
            logger.warning("[%s] allMids error: %s", self.symbol, e)
            return None

    async def get_price(self) -> float:
        """
        Obtiene el precio mid de self.coin vía allMids.

        Estrategia de resiliencia:
          1. Intento inicial.
          2. Si data es None → espera 0.4 s y reintenta 1 vez.
          3. Si sigue fallando → usa self._last_price (caché del último precio válido).
             Solo propaga excepción si no hay caché (primer arranque).
          4. Cada precio válido actualiza self._last_price.
        """
        async with aiohttp.ClientSession() as session:
            data = await self._fetch_all_mids(session)

            if data is None:
                await asyncio.sleep(0.4)
                data = await self._fetch_all_mids(session)

        if data is None:
            if self._last_price > 0:
                logger.warning(
                    "[%s] allMids falló tras retry — usando último precio válido en caché: %.4f",
                    self.symbol, self._last_price,
                )
                return self._last_price
            raise ValueError(
                f"[{self.symbol}] allMids devolvió tipo inesperado (NoneType): null"
            )

        price = data.get(self.coin)
        if price is None:
            if self._last_price > 0:
                logger.warning(
                    "[%s] Precio no encontrado en allMids — usando caché: %.4f",
                    self.symbol, self._last_price,
                )
                return self._last_price
            raise ValueError(f"[{self.symbol}] Precio no encontrado en allMids")

        result = float(price)
        self._last_price = result
        return result

    # ── Primitivas de exchange ────────────────────────────────────

    async def _place_tpsl(
        self,
        qty: float,
        sl_price: Optional[float],
        tp_price: Optional[float],
        is_long: bool,
        reduce_only: bool = True,
    ) -> None:
        """Coloca SL y/o TP en el exchange. Usado por PositionManager y OrderExecutor."""
        hl = self._require_hl()
        if hl is None:
            return

        if self.dry_run:
            logger.info(
                "[%s] DRY_RUN: _place_tpsl sl=%.4f tp=%.4f omitido.",
                self.symbol, sl_price or 0, tp_price or 0,
            )
            return

        if sl_price and sl_price > 0:
            try:
                result = await asyncio.to_thread(
                    hl.place_sl,
                    is_buy=not is_long,
                    sz=qty,
                    trigger_px=sl_price,
                    entry_px=sl_price,
                )
                logger.info("[%s] _place_tpsl SL=%.4f: %s", self.symbol, sl_price, result)
            except Exception as e:
                logger.error("[%s] _place_tpsl SL error: %s", self.symbol, e)

        if tp_price and tp_price > 0:
            try:
                result = await asyncio.to_thread(
                    hl.place_tp,
                    is_buy=not is_long,
                    sz=qty,
                    trigger_px=tp_price,
                    entry_px=tp_price,
                )
                logger.info("[%s] _place_tpsl TP=%.4f: %s", self.symbol, tp_price, result)
            except Exception as e:
                logger.error("[%s] _place_tpsl TP error: %s", self.symbol, e)

    def _round_qty(self, qty: float) -> float:
        """Redondea qty al tick size del símbolo vía el SDK."""
        hl = self._require_hl()
        if hl is None:
            return qty
        return hl.round_sz(qty)

    async def _set_leverage(self, leverage: int) -> None:
        """Configura el leverage en el exchange con timeout de seguridad."""
        hl = self._require_hl()
        if hl is None:
            return

        if self.dry_run:
            logger.info("[%s] DRY_RUN: _set_leverage(%d) omitido.", self.symbol, leverage)
            return
        try:
            result = await asyncio.wait_for(
                asyncio.to_thread(
                    hl._exchange.update_leverage,
                    leverage, self.coin, False,
                ),
                timeout=15.0,
            )
            logger.info("[%s] Leverage configurado a %dx: %s", self.symbol, leverage, result)
        except asyncio.TimeoutError:
            logger.warning(
                "[%s] _set_leverage timeout (15s) — continuando sin confirmar leverage.",
                self.symbol,
            )
        except Exception as e:
            logger.warning("[%s] No se pudo configurar leverage: %s", self.symbol, e)


__all__ = ["FuturesTrader"]
