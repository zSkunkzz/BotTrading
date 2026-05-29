"""
bot/balance_service.py - Servicio de balance usando ccxt
Uso: balance_svc.init(api_key, api_secret, passphrase) y luego await balance_svc.get()
"""

import asyncio
import logging
import os
import time
import ccxt.async_support as ccxt

logger = logging.getLogger("BalanceSvc")

_TTL = int(os.getenv("BALANCE_CACHE_TTL", "60"))  # segundos entre fetches reales


class BalanceService:
    def __init__(self):
        self._exchange = None
        self._value = None
        self._ts = 0.0
        self._lock = None
        self._ready = False

    def init(self, api_key, api_secret, passphrase):
        if self._ready:
            return
        self._exchange = ccxt.bitget({
            "apiKey": api_key,
            "secret": api_secret,
            "password": passphrase,
            "options": {"defaultType": "swap"},
            "enableRateLimit": True,
        })
        self._ready = True
        logger.info("[BalanceSvc] ✅ Inicializado con ccxt (Unified Account)")

    async def get(self):
        if not self._ready:
            return None
        if self._value is not None and time.monotonic() - self._ts < _TTL:
            return self._value
        if self._lock is None:
            self._lock = asyncio.Lock()
        async with self._lock:
            if self._value is not None and time.monotonic() - self._ts < _TTL:
                return self._value
            fresh = await self._fetch_balance()
            if fresh is not None:
                self._value = fresh
                self._ts = time.monotonic()
                logger.info(f"[BalanceSvc] ✅ Balance actualizado: {fresh:.2f} USDT")
            elif self._value is not None:
                logger.warning(f"[BalanceSvc] ⚠️ Usando caché: {self._value:.2f} USDT")
            else:
                logger.error("[BalanceSvc] ❌ No se pudo obtener balance")
            return self._value

    async def _fetch_balance(self):
        try:
            # Obtener balance de la cuenta de futuros (swap)
            balance = await self._exchange.fetch_balance()
            # En ccxt, para Bitget swap, el balance está en 'free' o 'total' de USDT
            usdt_balance = balance.get('USDT', {}).get('free', 0.0)
            if usdt_balance > 0:
                return float(usdt_balance)
            # Si no hay USDT en futuros, probar en spot (por si acaso)
            # Pero normalmente el balance de futuros está separado.
            # También se puede intentar con self._exchange.fetch_free_balance()
            return None
        except Exception as e:
            logger.error(f"[BalanceSvc] Error en fetch_balance: {e}")
            return None

    def invalidate(self):
        self._ts = 0.0


balance_svc = BalanceService()