"""
bot/balance_service.py — Servicio de consulta de balance con caché.

v2 — BUG #6 FIX: caché inflado tras SL externo al bot

  El TTL original era 30s. Si un SL es ejecutado directamente en el
  exchange (sin pasar por _place_order del bot), el balance en caché
  queda inflado hasta 30s. El siguiente pretrade check usa ese balance
  para calcular el margen permitido y puede oversizear.

  Fix:
    - DEFAULT_TTL reducido de 30s a 8s
    - invalidate() puede ser llamado desde PositionManager tras detectar
      cierre externo (SL hit en exchange)
    - invalidate_on_sl_detected() método explícito para usarlo como
      hook desde _ensure_tpsl_on_exchange cuando detecta que la posición
      ya no existe en el exchange
"""
from __future__ import annotations

import asyncio
import logging
import time
from typing import Callable, Optional

log = logging.getLogger(__name__)

DEFAULT_TTL = float(__import__('os').getenv("BALANCE_CACHE_TTL_S", "8"))  # BUG #6 FIX: 30s → 8s


class BalanceService:
    """
    Caché de balance con TTL configurable.

    BUG #6 FIX:
      - TTL por defecto reducido a 8s (env BALANCE_CACHE_TTL_S)
      - invalidate_on_sl_detected() hook explícito
      - invalidate() acepta reason= para loguear causa
    """

    def __init__(self, ttl_s: float = DEFAULT_TTL):
        self._ttl          = ttl_s
        self._cached_value: Optional[float] = None
        self._cached_at:    float = 0.0
        self._lock         = asyncio.Lock()
        self._fetch_fn:    Optional[Callable] = None
        self._hl_addr:     Optional[str]      = None
        self._info_post_fn: Optional[Callable] = None
        self._ready        = False

    def init_hl(self, address: str, info_post_fn: Callable) -> None:
        self._hl_addr      = address
        self._info_post_fn = info_post_fn
        self._ready        = True

    def is_ready(self) -> bool:
        return self._ready

    def invalidate(self, reason: str = "") -> None:
        """Invalida el caché inmediatamente."""
        self._cached_value = None
        self._cached_at    = 0.0
        if reason:
            log.debug("[BalanceSvc] Cache invalidado: %s", reason)

    def invalidate_on_sl_detected(self, symbol: str) -> None:
        """
        BUG #6 FIX: hook explícito para llamar cuando _ensure_tpsl_on_exchange
        detecta que la posición ya no existe en el exchange (SL/TP ejecutado
        externamente). Fuerza una recarga del balance en la próxima consulta.
        """
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
                usdc = float(
                    data.get("crossMarginSummary", {})
                    .get("accountValue", 0)
                )
                self._cached_value = usdc
                self._cached_at    = time.monotonic()
                return usdc
            except Exception as e:
                log.warning("[BalanceSvc] Error al obtener balance: %s", e)
                return self._cached_value  # devolver valor anterior si hay error

    async def get_fresh(self) -> Optional[float]:
        """Fuerza una recarga del balance ignorando el caché."""
        self.invalidate(reason="get_fresh() llamado")
        return await self.get()


balance_svc = BalanceService()
