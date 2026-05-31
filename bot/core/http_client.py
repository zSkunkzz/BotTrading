"""
http_client.py — Cliente HTTP/signing para Hyperliquid.

Extraído de trader.py. Contiene:
  - Rate limiter global para /info
  - Nonce único global
  - _info_post / _exchange_post
  - _get_coin_index
  - Helpers de normalización
"""
from __future__ import annotations

import asyncio
import logging
import os
import time
import json as _json

import aiohttp
from eth_account import Account
from eth_utils import to_hex
from hyperliquid.utils.signing import sign_l1_action, float_to_wire

logger = logging.getLogger("HTTPClient")

_USE_TESTNET = os.getenv("HL_TESTNET", "").lower() in ("true", "1", "yes")
_API_URL     = "https://api.hyperliquid-testnet.xyz" if _USE_TESTNET else "https://api.hyperliquid.xyz"

# ── Rate limiter global para /info ────────────────────────────────────────────
_HL_REST_LOCK    = asyncio.Lock()
_HL_LAST_CALL    = 0.0
_HL_MIN_INTERVAL = 0.6

async def _hl_throttle():
    global _HL_LAST_CALL
    async with _HL_REST_LOCK:
        now  = time.monotonic()
        wait = _HL_MIN_INTERVAL - (now - _HL_LAST_CALL)
        if wait > 0:
            await asyncio.sleep(wait)
        _HL_LAST_CALL = time.monotonic()

# ── Nonce único global ────────────────────────────────────────────────────────
_NONCE_LOCK = asyncio.Lock()
_NONCE_LAST = 0

async def _unique_nonce() -> int:
    global _NONCE_LAST
    async with _NONCE_LOCK:
        n = int(time.time() * 1000)
        if n <= _NONCE_LAST:
            n = _NONCE_LAST + 1
        _NONCE_LAST = n
        return n


def _norm_coin(symbol: str) -> str:
    s = symbol.replace("/", "").replace(":USDT", "").upper()
    if s.endswith("USDTUSDT"):
        s = s[:-4]
    if s.endswith("USDT"):
        s = s[:-4]
    return s


class HyperliquidHTTPClient:
    """
    Encapsula toda la comunicación HTTP con la API de Hyperliquid.
    Maneja autenticación (modo agente vs directo), signing EIP-712,
    throttling y coin index cache.
    """

    _coin_index_cache: dict[str, int] = {}

    def __init__(self, symbol: str):
        self.symbol = symbol
        self.coin   = _norm_coin(symbol)

        api_pk     = os.getenv("HL_API_PRIVATE_KEY", "").strip()
        api_wallet = os.getenv("HL_API_WALLET_ADDRESS", "").strip()

        if api_pk:
            if not api_wallet:
                raise ValueError(
                    f"[{symbol}] HL_API_WALLET_ADDRESS es OBLIGATORIA en modo agente."
                )
            self._private_key   = api_pk
            self._agent_mode    = True
            agent_acct          = Account.from_key(api_pk)
            self._agent_addr    = agent_acct.address
            self._master_addr   = api_wallet
            self._account_addr  = self._master_addr
            self._vault_address = None
            logger.info(
                "[%s] Auth: modo agente | master=%s | agente=%s",
                symbol,
                self._master_addr[:10] + "...",
                self._agent_addr[:10] + "...",
            )
        else:
            pk = os.getenv("HL_PRIVATE_KEY", "").strip()
            if not pk:
                raise ValueError(
                    f"[{symbol}] Sin clave configurada (HL_API_PRIVATE_KEY o HL_PRIVATE_KEY)."
                )
            self._private_key   = pk
            self._agent_mode    = False
            self._agent_addr    = ""
            self._vault_address = None
            addr = os.getenv("HL_ACCOUNT_ADDR", "").strip()
            if not addr:
                addr = Account.from_key(pk).address
            self._account_addr = addr
            self._master_addr  = addr
            logger.info("[%s] Auth: modo directo | addr=%s", symbol, addr[:10] + "...")

    # ── Coin index ────────────────────────────────────────────────────────────

    async def get_coin_index(self) -> int:
        if self.coin in self._coin_index_cache:
            return self._coin_index_cache[self.coin]
        data = await self.info_post({"type": "meta"})
        for i, uni in enumerate(data.get("universe", [])):
            self._coin_index_cache[uni.get("name", "")] = i
        return self._coin_index_cache.get(self.coin, 0)

    # ── HTTP ──────────────────────────────────────────────────────────────────

    async def info_post(self, payload: dict) -> dict:
        await _hl_throttle()
        async with aiohttp.ClientSession() as s:
            async with s.post(
                f"{_API_URL}/info", json=payload,
                headers={"Content-Type": "application/json"},
                timeout=aiohttp.ClientTimeout(total=10),
            ) as r:
                if r.status == 429:
                    logger.warning("[%s] 429 en /info, esperando 5s...", self.symbol)
                    await asyncio.sleep(5.0)
                    await _hl_throttle()
                    async with aiohttp.ClientSession() as s2:
                        async with s2.post(
                            f"{_API_URL}/info", json=payload,
                            headers={"Content-Type": "application/json"},
                            timeout=aiohttp.ClientTimeout(total=10),
                        ) as r2:
                            return _json.loads(await r2.text())
                return _json.loads(await r.text())

    async def exchange_post(self, action: dict) -> dict:
        if not self._private_key:
            raise ValueError("Sin clave configurada.")

        nonce     = await _unique_nonce()
        wallet    = Account.from_key(self._private_key)
        signature = sign_l1_action(
            wallet, action, self._vault_address, nonce, not _USE_TESTNET
        )

        payload: dict = {
            "action":       action,
            "nonce":        nonce,
            "signature":    {"r": to_hex(signature["r"]), "s": to_hex(signature["s"]), "v": signature["v"]},
            "vaultAddress": self._vault_address,
        }

        async with aiohttp.ClientSession() as s:
            async with s.post(
                f"{_API_URL}/exchange", json=payload,
                headers={"Content-Type": "application/json"},
                timeout=aiohttp.ClientTimeout(total=10),
            ) as r:
                text = await r.text()
                try:
                    result = _json.loads(text)
                except Exception:
                    result = {"status": "error", "response": text}

        if result.get("status") != "ok":
            err_str = str(result.get("response", ""))
            if "does not exist" in err_str:
                logger.error(
                    "[%s] HL rechazó con 'does not exist'.\n"
                    "  agente=%s | vault=%s | respuesta HL: %s\n"
                    "  → Verifica en app.hyperliquid.xyz → Settings → API\n"
                    "    que %s esté aprobado para master=%s",
                    self.symbol, self._agent_addr or "N/A",
                    self._vault_address or "None", err_str,
                    self._agent_addr or "N/A", self._master_addr or "NO CONFIGURADA",
                )
        return result
