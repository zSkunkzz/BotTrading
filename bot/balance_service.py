"""
bot/balance_service.py — Servicio de consulta de balance con caché.

v6 — BingX:
  Usa la API REST de BingX para futuros perpetuos:
    GET /openApi/swap/v2/user/balance
  Campo USDT disponible: data.balance.availableMargin
  Fallback: balance.balance (equity total)
"""
from __future__ import annotations

import asyncio
import hashlib
import hmac
import logging
import time
import urllib.parse
from typing import Callable, Optional

import requests

log = logging.getLogger(__name__)

DEFAULT_TTL = float(__import__('os').getenv("BALANCE_CACHE_TTL_S", "8"))


class BalanceService:

    def __init__(self, ttl_s: float = DEFAULT_TTL):
        self._ttl            = ttl_s
        self._cached_value:  Optional[float] = None
        self._cached_at:     float = 0.0
        self._lock           = asyncio.Lock()
        self._fetch_fn:      Optional[Callable] = None  # async () -> float
        self._ready          = False

    # ── BingX ─────────────────────────────────────────────────────
    def init_bingx(self, api_key: str, api_secret: str, testnet: bool = False) -> None:
        """
        Inicializa el servicio con las credenciales de BingX.
        La llamada real se hace en asyncio.to_thread para no bloquear el loop.
        """
        base = (
            "https://open-api-vst.bingx.com"
            if testnet
            else "https://open-api.bingx.com"
        )

        def _sync_fetch() -> float:
            ts     = str(int(time.time() * 1000))
            params = {"timestamp": ts}
            qs     = urllib.parse.urlencode(sorted(params.items()))
            sign   = hmac.new(
                api_secret.encode(), qs.encode(), hashlib.sha256
            ).hexdigest()
            params["sign"] = sign

            resp = requests.get(
                f"{base}/openApi/swap/v2/user/balance",
                params=params,
                headers={"X-BX-APIKEY": api_key},
                timeout=10,
            ).json()

            bal = (resp.get("data") or {}).get("balance") or {}

            avail = float(bal.get("availableMargin") or 0)
            if avail > 0:
                log.debug("[BalanceSvc] availableMargin=%.2f USDT", avail)
                return avail

            equity = float(bal.get("balance") or 0)
            if equity > 0:
                log.debug("[BalanceSvc] availableMargin=0, usando equity=%.2f", equity)
                return equity

            log.debug("[BalanceSvc] balance response: %s", resp)
            return 0.0

        async def _fetch() -> float:
            return await asyncio.to_thread(_sync_fetch)

        self._fetch_fn = _fetch
        self._ready    = True

    # Alias de compatibilidad por si algún módulo aún llama init_okx
    def init_okx(self, *args, **kwargs) -> None:  # type: ignore[override]
        log.warning(
            "[BalanceSvc] init_okx() llamado pero el bot está en modo BingX. "
            "Usa init_bingx() en su lugar."
        )

    # ────────────────────────────────────────────────────────────

    def is_ready(self) -> bool:
        return self._ready

    def invalidate(self, reason: str = "") -> None:
        self._cached_value = None
        self._cached_at    = 0.0
        if reason:
            log.debug("[BalanceSvc] Cache invalidado: %s", reason)

    def invalidate_on_sl_detected(self, symbol: str) -> None:
        self.invalidate(reason=f"SL/TP externo detectado para {symbol}")
        log.info(
            "[BalanceSvc] Balance invalidado por SL/TP externo en %s — "
            "próxima consulta refrescará desde BingX.",
            symbol,
        )

    async def get(self) -> Optional[float]:
        async with self._lock:
            now = time.monotonic()
            if (
                self._cached_value is not None
                and (now - self._cached_at) < self._ttl
            ):
                return self._cached_value

            if not self._ready or self._fetch_fn is None:
                return None

            try:
                usdt = await self._fetch_fn()
                self._cached_value = usdt
                self._cached_at    = time.monotonic()
                log.debug("[BalanceSvc] Balance actualizado: %.2f USDT", usdt)
                return usdt
            except Exception as e:
                log.warning("[BalanceSvc] Error al obtener balance: %s", e)
                return self._cached_value

    async def get_fresh(self) -> Optional[float]:
        """Fuerza una recarga del balance ignorando el caché."""
        self.invalidate(reason="get_fresh() llamado")
        return await self.get()


balance_svc = BalanceService()
