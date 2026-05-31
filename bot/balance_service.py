"""
balance_service.py — Singleton de balance USDT para Hyperliquid.

Endpoint:
  POST /info  {type: "clearinghouseState", user: <address>}
  → data.marginSummary.accountValue  (equity total en USDT)

No requiere firma — es un endpoint público de lectura.
"""
import asyncio
import logging
import time
import json as _json
import aiohttp

logger = logging.getLogger("BalanceSvc")

_CACHE_TTL  = 30    # segundos entre refreshes
_USE_TESTNET = False  # se actualiza desde init_hl()


class _BalanceService:
    def __init__(self):
        self._addr:   str   = ""
        self._cache:  float | None = None
        self._ts:     float = 0.0
        self._lock    = asyncio.Lock()
        self._ready   = False
        self._api_url = "https://api.hyperliquid.xyz"
        # Callback opcional al info_post del trader (evita duplicar lógica HTTP)
        self._info_post_fn = None

    def is_ready(self) -> bool:
        return self._ready

    # ── Compatibilidad con la firma de Bitget (ignoramos key/secret/pass) ──
    def init(self, key: str = "", secret: str = "", passphrase: str = ""):
        """Stub de compatibilidad — usa init_hl() para Hyperliquid."""
        if self._ready:
            return
        logger.warning("[BalanceSvc] init() ignorado — usa init_hl(addr, info_post_fn)")

    def init_hl(self, addr: str, info_post_fn=None, testnet: bool = False):
        """Inicializa con la dirección pública del wallet."""
        if self._ready:
            return
        self._addr        = addr
        self._info_post_fn = info_post_fn
        self._api_url     = (
            "https://api.hyperliquid-testnet.xyz" if testnet
            else "https://api.hyperliquid.xyz"
        )
        self._ready = True
        logger.info("[BalanceSvc] Inicializado (Hyperliquid) addr=%s", addr[:10] + "...")

    def invalidate(self):
        """Fuerza refresco en la próxima llamada a get()."""
        self._ts = 0.0

    # ── HTTP ──────────────────────────────────────────────────────────────────

    async def _fetch_via_callback(self) -> float | None:
        """Usa el info_post del trader si está disponible (reutiliza sesión)."""
        if not self._info_post_fn:
            return None
        try:
            data = await self._info_post_fn({
                "type": "clearinghouseState",
                "user": self._addr,
            })
            return self._extract(data)
        except Exception as e:
            logger.debug("[BalanceSvc] callback error: %s", e)
            return None

    async def _fetch_direct(self) -> float | None:
        """Fallback: llama directamente al endpoint /info sin firma."""
        if not self._addr:
            return None
        payload = {"type": "clearinghouseState", "user": self._addr}
        try:
            async with aiohttp.ClientSession() as s:
                async with s.post(
                    f"{self._api_url}/info",
                    json=payload,
                    headers={"Content-Type": "application/json"},
                    timeout=aiohttp.ClientTimeout(total=8),
                ) as r:
                    text = await r.text()
                    data = _json.loads(text)
                    return self._extract(data)
        except Exception as e:
            logger.debug("[BalanceSvc] _fetch_direct error: %s", e)
            return None

    def _extract(self, data: dict) -> float | None:
        """
        Extrae el equity disponible de la respuesta clearinghouseState.
        Campos candidatos (en orden de preferencia):
          marginSummary.accountValue   ← equity total
          marginSummary.withdrawable   ← disponible para retirar
          crossMaintenanceMarginUsed   ← fallback
        """
        if not isinstance(data, dict):
            return None

        ms = data.get("marginSummary", {})
        for field in ("accountValue", "withdrawable", "totalNtlPos"):
            v = ms.get(field)
            if v is not None:
                try:
                    val = float(v)
                    if val >= 0:
                        logger.debug("[BalanceSvc] Balance=%.2f USDT (campo=%s)", val, field)
                        return val
                except (ValueError, TypeError):
                    pass

        # Fallback: cross account value
        for field in ("crossMaintenanceMarginUsed", "totalRawUsd"):
            v = data.get(field)
            if v is not None:
                try:
                    return float(v)
                except (ValueError, TypeError):
                    pass

        logger.debug("[BalanceSvc] No se encontró campo de balance en: %s", list(data.keys()))
        return None

    # ── API pública ───────────────────────────────────────────────────────────

    async def get(self) -> float | None:
        """Devuelve balance cacheado o refresca si ha caducado."""
        if not self._ready:
            logger.warning("[BalanceSvc] get() llamado antes de init_hl()")
            return None

        async with self._lock:
            if time.time() - self._ts < _CACHE_TTL and self._cache is not None:
                return self._cache

            val = await self._fetch_via_callback()
            if val is None:
                val = await self._fetch_direct()

            if val is not None:
                self._cache = val
                self._ts    = time.time()
            else:
                logger.warning("[BalanceSvc] ⚠️ No se pudo obtener balance")
            return self._cache


balance_svc = _BalanceService()
