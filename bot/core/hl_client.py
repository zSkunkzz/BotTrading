"""
hl_client.py — Cliente Hyperliquid basado en el SDK oficial.

Reemplaza el signing manual de http_client.py por el SDK oficial:
  pip install hyperliquid-python-sdk>=0.1.9

Ventajas vs signing manual:
  - El SDK mantiene el formato EIP-712 actualizado automáticamente.
  - Soporte nativo para trigger orders (TP/SL) sin construir payloads a mano.
  - Menos superficie de error en nonce, vault_address y hash.

Autenticación soportada:
  Opción A (recomendada): API Wallet
    HL_API_PRIVATE_KEY     — private key del agente aprobado en app.hyperliquid.xyz
    HL_API_WALLET_ADDRESS  — dirección del wallet PRINCIPAL (el que tiene fondos)

  Opción B: Private key directa
    HL_PRIVATE_KEY         — private key del wallet principal
    HL_ACCOUNT_ADDR        — dirección pública (opcional, se deriva automáticamente)

Opcionales:
  HL_TESTNET             — "true" para testnet
"""
from __future__ import annotations

import logging
import os
from typing import Optional

from eth_account import Account
from hyperliquid.exchange import Exchange
from hyperliquid.info import Info

logger = logging.getLogger("HLClient")

_USE_TESTNET = os.getenv("HL_TESTNET", "").lower() in ("true", "1", "yes")
_BASE_URL = (
    "https://api.hyperliquid-testnet.xyz"
    if _USE_TESTNET
    else "https://api.hyperliquid.xyz"
)

# Slippage permitido para órdenes de mercado (0.5 % por defecto).
# El SDK requiere un limit_px concreto incluso para isMarket=True;
# usamos precio * (1 ± slippage) para garantizar fill sin rechazos.
_MARKET_SLIPPAGE = float(os.getenv("HL_MARKET_SLIPPAGE", "0.005"))


def _norm_coin(symbol: str) -> str:
    """Normaliza símbolo a nombre de coin HL (e.g. 'BTC/USDT:USDT' → 'BTC')."""
    s = symbol.replace("/", "").replace(":USDT", "").upper()
    for suffix in ("USDTUSDT", "USDT"):
        if s.endswith(suffix):
            s = s[: -len(suffix)]
    return s


class HLClient:
    """
    Wrapper del SDK oficial de Hyperliquid.

    Uso:
        client = HLClient(symbol="BTC/USDT:USDT")
        await client.place_limit(is_buy=True, sz=0.01, price=40000)
        await client.place_tp(is_buy=False, sz=0.01, trigger_px=45000, limit_px=45000)
        await client.place_sl(is_buy=False, sz=0.01, trigger_px=35000)
    """

    def __init__(self, symbol: str):
        self.symbol = symbol
        self.coin   = _norm_coin(symbol)

        api_pk     = os.getenv("HL_API_PRIVATE_KEY", "").strip()
        api_wallet = os.getenv("HL_API_WALLET_ADDRESS", "").strip()

        if api_pk:
            if not api_wallet:
                raise ValueError(
                    "HL_API_WALLET_ADDRESS es obligatoria en modo agente. "
                    "Debe ser la dirección del wallet PRINCIPAL (el que tiene fondos "
                    "y aprobó al agente en app.hyperliquid.xyz → Settings → API)."
                )
            wallet               = Account.from_key(api_pk)
            self._account_addr   = api_wallet          # master wallet (fondos)
            self._agent_addr     = wallet.address      # agent wallet (firma)
            self._agent_mode     = True
            self._exchange       = Exchange(
                wallet=wallet,
                base_url=_BASE_URL,
                account_address=api_wallet,            # <── CLAVE: master addr para el SDK
            )
            logger.info(
                "[%s] HLClient (SDK) • modo agente | master=%s | agente=%s",
                symbol,
                api_wallet[:10] + "...",
                wallet.address[:10] + "...",
            )
        else:
            pk = os.getenv("HL_PRIVATE_KEY", "").strip()
            if not pk:
                raise ValueError(
                    "Sin clave configurada. Define HL_API_PRIVATE_KEY (modo agente) "
                    "o HL_PRIVATE_KEY (modo directo)."
                )
            wallet               = Account.from_key(pk)
            addr                 = os.getenv("HL_ACCOUNT_ADDR", "").strip() or wallet.address
            self._account_addr   = addr
            self._agent_addr     = ""
            self._agent_mode     = False
            self._exchange       = Exchange(wallet=wallet, base_url=_BASE_URL)
            logger.info("[%s] HLClient (SDK) • modo directo | addr=%s", symbol, addr[:10] + "...")

        self._info = Info(base_url=_BASE_URL, skip_ws=True)

    # ──────────────────────────────────────────────────────────────────────────
    # ÓRDENES BÁSICAS
    # ──────────────────────────────────────────────────────────────────────────

    def place_limit(
        self,
        is_buy: bool,
        sz: float,
        price: float,
        reduce_only: bool = False,
        tif: str = "Gtc",
    ) -> dict:
        """Orden límite estándar."""
        return self._exchange.order(
            name=self.coin,
            is_buy=is_buy,
            sz=sz,
            limit_px=price,
            order_type={"limit": {"tif": tif}},
            reduce_only=reduce_only,
        )

    def place_market(
        self,
        is_buy: bool,
        sz: float,
        ref_px: float,
        reduce_only: bool = False,
    ) -> dict:
        """
        Orden de mercado.

        Args:
            ref_px: Precio de referencia actual (mid o last).
                    Se usa para calcular el slippage limit_px que el SDK requiere:
                    compra  → ref_px * (1 + _MARKET_SLIPPAGE)
                    venta   → ref_px * (1 - _MARKET_SLIPPAGE)
                    El exchange ejecuta al mejor precio disponible; el limit_px
                    solo actúa como techo/suelo de protección.
        """
        slippage_px = round(
            ref_px * (1 + _MARKET_SLIPPAGE) if is_buy else ref_px * (1 - _MARKET_SLIPPAGE),
            6,
        )
        return self._exchange.order(
            name=self.coin,
            is_buy=is_buy,
            sz=sz,
            limit_px=slippage_px,
            order_type={"limit": {"tif": "Ioc"}},  # IOC actúa como market en HL
            reduce_only=reduce_only,
        )

    # ──────────────────────────────────────────────────────────────────────────
    # TRIGGER ORDERS — TP / SL REALES EN EL EXCHANGE
    # ──────────────────────────────────────────────────────────────────────────

    def place_tp(
        self,
        is_buy: bool,
        sz: float,
        trigger_px: float,
        limit_px: Optional[float] = None,
    ) -> dict:
        """
        Coloca un Take Profit real en el exchange como trigger order.

        Args:
            is_buy:     False para cerrar long (vender), True para cerrar short (comprar).
            sz:         Cantidad a cerrar.
            trigger_px: Precio que activa la orden.
            limit_px:   Si se especifica → orden límite al activarse.
                        Si es None → orden de mercado al activarse
                        (limit_px se calcula con slippage automático).
        """
        is_market = limit_px is None
        if is_market:
            # TP de mercado: slippage en dirección favorable
            limit_px = round(
                trigger_px * (1 - _MARKET_SLIPPAGE) if is_buy else trigger_px * (1 + _MARKET_SLIPPAGE),
                6,
            )
        return self._exchange.order(
            name=self.coin,
            is_buy=is_buy,
            sz=sz,
            limit_px=limit_px,
            order_type={
                "trigger": {
                    "triggerPx": trigger_px,
                    "isMarket":  is_market,
                    "tpsl":      "tp",
                }
            },
            reduce_only=True,
        )

    def place_sl(
        self,
        is_buy: bool,
        sz: float,
        trigger_px: float,
    ) -> dict:
        """
        Coloca un Stop Loss real en el exchange como trigger order de mercado.

        Args:
            is_buy:     False para cerrar long, True para cerrar short.
            sz:         Cantidad a cerrar.
            trigger_px: Precio que activa el SL (se ejecuta como mercado).
        """
        # SL de mercado: slippage en dirección desfavorable (peor caso)
        slippage_px = round(
            trigger_px * (1 - _MARKET_SLIPPAGE) if is_buy else trigger_px * (1 + _MARKET_SLIPPAGE),
            6,
        )
        return self._exchange.order(
            name=self.coin,
            is_buy=is_buy,
            sz=sz,
            limit_px=slippage_px,
            order_type={
                "trigger": {
                    "triggerPx": trigger_px,
                    "isMarket":  True,
                    "tpsl":      "sl",
                }
            },
            reduce_only=True,
        )

    def place_bulk(
        self,
        orders: list[dict],
    ) -> dict:
        """
        Envía múltiples órdenes en una sola llamada a la API.
        Cada dict tiene los mismos campos que exchange.order().
        """
        return self._exchange.bulk_orders(orders)

    # ──────────────────────────────────────────────────────────────────────────
    # CONSULTAS INFO
    # ──────────────────────────────────────────────────────────────────────────

    def get_user_state(self) -> dict:
        """Estado de la cuenta (balance, posiciones, margin)."""
        return self._info.user_state(self._account_addr)

    def get_open_orders(self) -> list:
        """Lista de órdenes abiertas (incluye trigger orders activos)."""
        return self._info.open_orders(self._account_addr)

    def get_positions(self) -> list:
        """Posiciones abiertas filtradas por coin."""
        state = self.get_user_state()
        return [
            p for p in state.get("assetPositions", [])
            if p.get("position", {}).get("coin") == self.coin
               and float(p.get("position", {}).get("szi", 0)) != 0
        ]

    def get_balance_usdc(self) -> float:
        """Balance disponible en USDC."""
        state = self.get_user_state()
        return float(state.get("crossMarginSummary", {}).get("accountValue", 0.0))

    def cancel_order(self, order_id: int) -> dict:
        """Cancela una orden por su ID."""
        return self._exchange.cancel(self.coin, order_id)

    def cancel_all_open_tpsl(self) -> list[dict]:
        """
        Cancela todos los trigger orders (TP/SL) abiertos para este coin.
        Útil antes de colocar nuevos TP/SL o al cerrar posición manualmente.
        """
        orders  = self.get_open_orders()
        results = []
        for o in orders:
            if o.get("coin") != self.coin:
                continue
            ot = o.get("orderType", "")
            if "Trigger" in ot or "Stop" in ot or "Take Profit" in ot:
                oid = o.get("oid")
                if oid:
                    r = self.cancel_order(oid)
                    results.append(r)
                    logger.info("[%s] Trigger order cancelada: oid=%s type=%s", self.coin, oid, ot)
        return results
