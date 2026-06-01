"""
bot/balance_service.py — Servicio de consulta de balance con caché.

v3 — BUG #8 FIX: balance devolvía 0.00 con fondos reales en la cuenta

  accountValue (crossMarginSummary) incluye PnL no realizado y en modo
  isolated puede devolver 0 aunque haya fondos disponibles.

  El campo correcto para saber cuánto USDC tienes disponible para abrir
  nuevas posiciones es `withdrawable` dentro de crossMarginSummary.

  Si withdrawable no está o es 0, se hace fallback a:
    1. marginSummary.accountValue  (total incluyendo isolated margins)
    2. crossMarginSummary.accountValue  (último recurso)

  Fix:
    - get() usa withdrawable como valor primario
    - fallback encadenado para robustez
    - log de debug muestra los tres valores para diagnóstico
"""
from __future__ import annotations

import asyncio
import logging
import time
from typing import Callable, Optional

log = logging.getLogger(__name__)

DEFAULT_TTL = float(__import__('os').getenv("BALANCE_CACHE_TTL_S", "8"))


class BalanceService:

    def __init__(self, ttl_s: float = DEFAULT_TTL):
        self._ttl           = ttl_s
        self._cached_value: Optional[float] = None
        self._cached_at:    float = 0.0
        self._lock          = asyncio.Lock()
        self._hl_addr:      Optional[str]      = None
        self._info_post_fn: Optional[Callable] = None
        self._ready         = False

    def init_hl(self, address: str, info_post_fn: Callable) -> None:
        self._hl_addr      = address
        self._info_post_fn = info_post_fn
        self._ready        = True

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
            "próxima consulta refrescará desde exchange.",
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

            if not self._ready or not self._hl_addr or not self._info_post_fn:
                return None

            try:
                data = await self._info_post_fn(
                    {"type": "clearinghouseState", "user": self._hl_addr}
                )

                cross  = data.get("crossMarginSummary", {})
                margin = data.get("marginSummary", {})

                # BUG #8 FIX: usar withdrawable como fuente primaria.
                # withdrawable = USDC libre para abrir posiciones nuevas.
                # accountValue incluye PnL no realizado y puede ser 0 en isolated.
                withdrawable    = float(cross.get("withdrawable", 0) or 0)
                margin_total    = float(margin.get("accountValue", 0) or 0)
                cross_acct_val  = float(cross.get("accountValue", 0) or 0)

                log.debug(
                    "[BalanceSvc] withdrawable=%.2f | marginSummary.accountValue=%.2f"
                    " | crossMarginSummary.accountValue=%.2f",
                    withdrawable, margin_total, cross_acct_val,
                )

                # Elegir el mejor valor disponible
                if withdrawable > 0:
                    usdc = withdrawable
                elif margin_total > 0:
                    usdc = margin_total
                else:
                    usdc = cross_acct_val

                self._cached_value = usdc
                self._cached_at    = time.monotonic()
                log.debug("[BalanceSvc] Balance actualizado: %.2f USDC", usdc)
                return usdc

            except Exception as e:
                log.warning("[BalanceSvc] Error al obtener balance: %s", e)
                return self._cached_value

    async def get_fresh(self) -> Optional[float]:
        """Fuerza una recarga del balance ignorando el caché."""
        self.invalidate(reason="get_fresh() llamado")
        return await self.get()


balance_svc = BalanceService()
