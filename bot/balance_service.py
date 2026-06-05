"""
bot/balance_service.py — Servicio de consulta de balance con caché.

v4 — OKX migration:
  Sustituye init_hl() por init_okx() usando okx.Account REST.
  El campo correcto para USDC disponible en futuros OKX es:
    GET /api/v5/account/balance → details[ccy=="USDT"].availBal
  Fallback: totalEq si availBal no está disponible.
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
        self._ttl            = ttl_s
        self._cached_value:  Optional[float] = None
        self._cached_at:     float = 0.0
        self._lock           = asyncio.Lock()
        self._fetch_fn:      Optional[Callable] = None  # async () -> float
        self._ready          = False

    # ── OKX ──────────────────────────────────────────────────────
    def init_okx(self, account_api) -> None:
        """
        Inicializa el servicio con una instancia de okx.Account.AccountAPI.
        La llamada real se hace en asyncio.to_thread para no bloquear el loop.
        """
        import asyncio as _asyncio

        async def _fetch() -> float:
            import asyncio as _a
            data = await _a.to_thread(account_api.get_account_balance)
            details = (
                data.get("data", [{}])[0]
                    .get("details", [])
            )
            usdt_detail = next(
                (d for d in details if d.get("ccy") in ("USDT", "USDC")),
                None,
            )
            if usdt_detail:
                avail = float(usdt_detail.get("availBal") or 0)
                if avail > 0:
                    log.debug("[BalanceSvc] availBal=%.2f USDT", avail)
                    return avail
                total = float(usdt_detail.get("eq") or 0)
                if total > 0:
                    log.debug("[BalanceSvc] availBal=0, usando eq=%.2f", total)
                    return total
            # último recurso: totalEq de la cuenta
            total_eq = float(
                data.get("data", [{}])[0].get("totalEq") or 0
            )
            log.debug("[BalanceSvc] fallback totalEq=%.2f", total_eq)
            return total_eq

        self._fetch_fn = _fetch
        self._ready    = True

    # ── legado HL (mantenido por compatibilidad) ──────────────────
    def init_hl(self, address: str, info_post_fn: Callable) -> None:
        """Deprecated — usar init_okx()."""
        import asyncio as _asyncio

        async def _fetch() -> float:
            data = await info_post_fn(
                {"type": "clearinghouseState", "user": address}
            )
            cross  = data.get("crossMarginSummary", {})
            margin = data.get("marginSummary", {})
            withdrawable   = float(cross.get("withdrawable", 0) or 0)
            margin_total   = float(margin.get("accountValue", 0) or 0)
            cross_acct_val = float(cross.get("accountValue", 0) or 0)
            if withdrawable > 0:
                return withdrawable
            if margin_total > 0:
                return margin_total
            return cross_acct_val

        self._fetch_fn = _fetch
        self._ready    = True

    # ─────────────────────────────────────────────────────────────

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

            if not self._ready or self._fetch_fn is None:
                return None

            try:
                usdc = await self._fetch_fn()
                self._cached_value = usdc
                self._cached_at    = time.monotonic()
                log.debug("[BalanceSvc] Balance actualizado: %.2f USDT", usdc)
                return usdc
            except Exception as e:
                log.warning("[BalanceSvc] Error al obtener balance: %s", e)
                return self._cached_value

    async def get_fresh(self) -> Optional[float]:
        """Fuerza una recarga del balance ignorando el caché."""
        self.invalidate(reason="get_fresh() llamado")
        return await self.get()


balance_svc = BalanceService()
