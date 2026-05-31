"""
hl_client.py — Cliente Hyperliquid basado en el SDK oficial.

Reemplaza el signing manual de http_client.py por el SDK oficial:
  pip install hyperliquid-python-sdk==0.9.4

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
_MARKET_SLIPPAGE = float(os.getenv("HL_MARKET_SLIPPAGE", "0.005"))

# Strings que devuelve HL en el campo orderType para trigger orders.
_TRIGGER_ORDER_KEYWORDS = ("stop market", "stop limit", "take profit", "trigger")


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
        client.place_limit(is_buy=True, sz=0.01, price=40000)
        client.place_tp(is_buy=False, sz=0.01, trigger_px=45000)
        client.place_sl(is_buy=False, sz=0.01, trigger_px=35000)
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
            self._account_addr   = api_wallet
            self._agent_addr     = wallet.address
            self._agent_mode     = True
            self._exchange       = Exchange(
                wallet=wallet,
                base_url=_BASE_URL,
                account_address=api_wallet,
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

        # FIX: Info se inicializa lazy para evitar 15 llamadas HTTP síncronas
        # al arrancar todos los traders (causaban 429 en masa).
        self._info: Optional[Info] = None

    @property
    def info(self) -> Info:
        """Lazy-init: sólo crea la conexión Info cuando se necesita."""
        if self._info is None:
            self._info = Info(base_url=_BASE_URL, skip_ws=True)
        return self._info

    # ─────────────────────────────────────────────────────────────────────────
    # ÓRDENES BÁSICAS
    # ─────────────────────────────────────────────────────────────────────────

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
            order_type={"limit": {"tif": "Ioc"}},
            reduce_only=reduce_only,
        )

    # ─────────────────────────────────────────────────────────────────────────
    # TRIGGER ORDERS — TP / SL REALES EN EL EXCHANGE
    # ─────────────────────────────────────────────────────────────────────────

    def place_tp(
        self,
        is_buy: bool,
        sz: float,
        trigger_px: float,
        limit_px: Optional[float] = None,
    ) -> dict:
        """
        Coloca un Take Profit real en el exchange como trigger order.
        """
        is_market = limit_px is None
        if is_market:
            limit_px = round(
                trigger_px * (1 - _MARKET_SLIPPAGE) if not is_buy else trigger_px * (1 + _MARKET_SLIPPAGE),
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
        """
        slippage_px = round(
            trigger_px * (1 - _MARKET_SLIPPAGE) if not is_buy else trigger_px * (1 + _MARKET_SLIPPAGE),
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
        """Envía múltiples órdenes en una sola llamada a la API."""
        return self._exchange.bulk_orders(orders)

    # ─────────────────────────────────────────────────────────────────────────
    # CONSULTAS INFO
    # ─────────────────────────────────────────────────────────────────────────

    def get_user_state(self) -> dict:
        """Estado de la cuenta (balance, posiciones, margin)."""
        return self.info.user_state(self._account_addr)

    def get_open_orders(self) -> list:
        """Lista de órdenes abiertas (incluye trigger orders activos)."""
        return self.info.open_orders(self._account_addr)

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
        """
        orders  = self.get_open_orders()
        results = []
        for o in orders:
            if o.get("coin") != self.coin:
                continue
            ot = o.get("orderType", "").lower()
            if any(kw in ot for kw in _TRIGGER_ORDER_KEYWORDS):
                oid = o.get("oid")
                if oid:
                    r = self.cancel_order(oid)
                    results.append(r)
                    logger.info(
                        "[%s] Trigger order cancelada: oid=%s type=%s",
                        self.coin, oid, o.get("orderType", "")
                    )
        return results
