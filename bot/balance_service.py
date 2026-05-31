"""
balance_service.py — Singleton de balance USDC para Hyperliquid.

Endpoint:
  POST /info  {type: "clearinghouseState", user: <address>}
  → data.marginSummary.accountValue  (equity total en USDC)

No requiere firma — es un endpoint público de lectura.

IMPORTANTE: siempre debe consultarse la dirección del wallet MASTER
(el que tiene fondos y aprobó el agente), nunca la del agente.
El agente solo firma órdenes; el margen vive en la cuenta master.
"""
import asyncio
import logging
import time
import json as _json
import aiohttp

logger = logging.getLogger("BalanceSvc")

_CACHE_TTL   = 30    # segundos entre refreshes


class _BalanceService:
    def __init__(self):
        self._addr:        str   = ""
        self._cache:       float | None = None
        self._ts:          float = 0.0
        self._lock         = asyncio.Lock()
        self._ready        = False
        self._api_url      = "https://api.hyperliquid.xyz"
        self._info_post_fn = None

    def is_ready(self) -> bool:
        return self._ready

    def init(self, key: str = "", secret: str = "", passphrase: str = ""):
        """Stub de compatibilidad — usa init_hl() para Hyperliquid."""
        logger.warning("[BalanceSvc] init() ignorado — usa init_hl(addr, info_post_fn)")

    def init_hl(self, addr: str, info_post_fn=None, testnet: bool = False):
        """
        Inicializa con la dirección pública del wallet MASTER.

        Puede llamarse múltiples veces (un trader por símbolo).
        La primera llamada con una dirección válida gana; las siguientes
        se ignoran a menos que la dirección cambie (no debería ocurrir).
        """
        if not addr:
            logger.warning("[BalanceSvc] init_hl() llamado con addr vacía — ignorado")
            return

        if self._ready:
            # Verificar que todos los traders apuntan al mismo master
            if self._addr.lower() != addr.lower():
                logger.error(
                    "[BalanceSvc] CONFLICTO de dirección: ya inicializado con %s, "
                    "nueva llamada con %s — se mantiene la original.",
                    self._addr[:10] + "...", addr[:10] + "...",
                )
            return

        self._addr         = addr
        self._info_post_fn = info_post_fn
        self._api_url      = (
            "https://api.hyperliquid-testnet.xyz" if testnet
            else "https://api.hyperliquid.xyz"
        )
        self._ready = True
        logger.info(
            "[BalanceSvc] Inicializado — consultando balance de MASTER addr=%s",
            addr,  # log completo para poder verificar en Railway
        )

    def invalidate(self):
        """Fuerza refresco en la próxima llamada a get()."""
        self._ts = 0.0
        self._cache = None

    # ── HTTP ────────────────────────────────────────────────────────────────

    async def _fetch_via_callback(self) -> float | None:
        """Usa el info_post del trader si está disponible."""
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
                    val  = self._extract(data)
                    if val is None:
                        # Log completo del raw para poder diagnosticar desde Railway
                        logger.warning(
                            "[BalanceSvc] _fetch_direct addr=%s → balance no encontrado.\n"
                            "  HTTP status : %s\n"
                            "  Keys top-level: %s\n"
                            "  marginSummary : %s\n"
                            "  crossMarginSummary: %s\n"
                            "  Raw (500c) : %s",
                            self._addr,
                            r.status,
                            list(data.keys()) if isinstance(data, dict) else type(data).__name__,
                            data.get("marginSummary") if isinstance(data, dict) else "N/A",
                            data.get("crossMarginSummary") if isinstance(data, dict) else "N/A",
                            text[:500],
                        )
                    return val
        except Exception as e:
            logger.debug("[BalanceSvc] _fetch_direct error: %s", e)
            return None

    def _extract(self, data: dict) -> float | None:
        """
        Extrae el equity disponible de la respuesta clearinghouseState.

        Hyperliquid puede devolver el balance en distintas ubicaciones
        dependiendo del modo de margen (cross vs isolated) y versión de API:

          marginSummary.accountValue        ← equity cross (más común)
          crossMarginSummary.accountValue   ← alias en algunas versiones
          marginSummary.withdrawable        ← disponible para retirar
          withdrawable                      ← a veces en el root
        """
        if not isinstance(data, dict):
            return None

        # 1. Intentar marginSummary (campo estándar)
        ms = data.get("marginSummary", {})
        if isinstance(ms, dict):
            for field in ("accountValue", "withdrawable"):
                v = ms.get(field)
                if v is not None:
                    try:
                        val = float(v)
                        if val >= 0:
                            logger.debug("[BalanceSvc] Balance=%.2f USDC (marginSummary.%s)", val, field)
                            return val
                    except (ValueError, TypeError):
                        pass

        # 2. Intentar crossMarginSummary (alias en cuentas cross)
        cms = data.get("crossMarginSummary", {})
        if isinstance(cms, dict):
            for field in ("accountValue", "withdrawable"):
                v = cms.get(field)
                if v is not None:
                    try:
                        val = float(v)
                        if val >= 0:
                            logger.debug("[BalanceSvc] Balance=%.2f USDC (crossMarginSummary.%s)", val, field)
                            return val
                    except (ValueError, TypeError):
                        pass

        # 3. Campos en el root del objeto
        for field in ("withdrawable", "totalRawUsd", "crossAccountValue"):
            v = data.get(field)
            if v is not None:
                try:
                    val = float(v)
                    if val >= 0:
                        logger.debug("[BalanceSvc] Balance=%.2f USDC (root.%s)", val, field)
                        return val
                except (ValueError, TypeError):
                    pass

        return None

    # ── API pública ────────────────────────────────────────────────────────────────

    async def get(self) -> float | None:
        """
        Devuelve balance cacheado o refresca si ha caducado.

        IMPORTANTE: si el fetch falla, NO se cachea None.
        El caché solo guarda valores positivos confirmados.
        Si el balance real es 0.0 (cuenta vacía), sí se cachea.
        """
        if not self._ready:
            logger.warning("[BalanceSvc] get() llamado antes de init_hl()")
            return None

        async with self._lock:
            # Solo usar caché si tenemos un valor válido y no ha caducado
            if (
                self._cache is not None
                and time.time() - self._ts < _CACHE_TTL
            ):
                return self._cache

            # Intentar fetch con retry (2 intentos)
            val = None
            for attempt in range(2):
                val = await self._fetch_via_callback()
                if val is None:
                    val = await self._fetch_direct()
                if val is not None:
                    break
                if attempt == 0:
                    await asyncio.sleep(1.0)  # breve pausa antes de retry

            if val is not None:
                self._cache = val
                self._ts    = time.time()
                logger.info("[BalanceSvc] Balance actualizado: %.2f USDC (addr=%s)",
                            val, self._addr)
            else:
                logger.warning(
                    "[BalanceSvc] ⚠️ No se pudo obtener balance USDC "
                    "(addr=%s — verifica que sea el wallet MASTER con fondos en perpetuals)",
                    self._addr,
                )

            return self._cache  # puede ser None o el último valor conocido


balance_svc = _BalanceService()
