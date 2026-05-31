"""
balance_service.py — Singleton de balance USDC para Hyperliquid.

Endpoints consultados (en orden):
  1. POST /info  {type: "clearinghouseState", user: <address>}
     → data.marginSummary.accountValue  (equity perpetuals)

  2. Si el valor anterior es 0, fallback a:
     POST /info  {type: "spotClearinghouseState", user: <address>}
     → balances[coin=="USDC"].total  (saldo Spot en Cuenta Unificada)

Con Cuenta Unificada de Hyperliquid el USDC depositado en Spot se usa
directamente como colateral de perpetuals, pero clearinghouseState devuelve
0 hasta que se realiza el primer trade perp. spotClearinghouseState refleja
el saldo real disponible.

No requiere firma — son endpoints públicos de lectura.

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
_MAX_RETRIES = 4     # intentos máximos en _fetch_direct (con backoff)


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
            # 1. clearinghouseState → perpetuals equity
            data = await self._info_post_fn({
                "type": "clearinghouseState",
                "user": self._addr,
            })
            val = self._extract(data)

            # 2. Si es 0, intentar spotClearinghouseState (Cuenta Unificada)
            if val is not None and val == 0.0:
                spot_data = await self._info_post_fn({
                    "type": "spotClearinghouseState",
                    "user": self._addr,
                })
                spot_val = self._extract_spot(spot_data)
                if spot_val is not None and spot_val > 0:
                    logger.info(
                        "[BalanceSvc] Cuenta Unificada detectada — usando saldo Spot: %.2f USDC",
                        spot_val,
                    )
                    return spot_val

            return val
        except Exception as e:
            logger.debug("[BalanceSvc] callback error: %s", e)
            return None

    async def _fetch_direct(self) -> float | None:
        """
        Fallback: llama directamente al endpoint /info sin firma.
        Reintenta hasta _MAX_RETRIES veces con backoff exponencial
        ante 429 o errores de red.
        """
        if not self._addr:
            return None

        for attempt in range(_MAX_RETRIES):
            try:
                async with aiohttp.ClientSession() as s:
                    # ── 1. clearinghouseState ────────────────────────────
                    payload = {"type": "clearinghouseState", "user": self._addr}
                    async with s.post(
                        f"{self._api_url}/info",
                        json=payload,
                        headers={"Content-Type": "application/json"},
                        timeout=aiohttp.ClientTimeout(total=10),
                    ) as r:
                        if r.status == 429:
                            wait = 2.0 * (attempt + 1)
                            logger.warning(
                                "[BalanceSvc] 429 rate-limit en /info (intento %d/%d) — "
                                "esperando %.0fs antes de reintentar...",
                                attempt + 1, _MAX_RETRIES, wait,
                            )
                            await asyncio.sleep(wait)
                            continue

                        if r.status >= 500:
                            logger.warning(
                                "[BalanceSvc] HTTP %s en /info (intento %d/%d)",
                                r.status, attempt + 1, _MAX_RETRIES,
                            )
                            await asyncio.sleep(1.5)
                            continue

                        text = await r.text()
                        try:
                            data = _json.loads(text)
                        except Exception:
                            logger.warning("[BalanceSvc] Respuesta no-JSON: %s", text[:200])
                            await asyncio.sleep(1.0)
                            continue

                        val = self._extract(data)

                        # ── 2. Si es 0, probar spotClearinghouseState ────
                        if val is not None and val == 0.0:
                            spot_payload = {"type": "spotClearinghouseState", "user": self._addr}
                            async with s.post(
                                f"{self._api_url}/info",
                                json=spot_payload,
                                headers={"Content-Type": "application/json"},
                                timeout=aiohttp.ClientTimeout(total=10),
                            ) as spot_r:
                                if spot_r.status == 200:
                                    spot_text = await spot_r.text()
                                    try:
                                        spot_data = _json.loads(spot_text)
                                        spot_val = self._extract_spot(spot_data)
                                        if spot_val is not None and spot_val > 0:
                                            logger.info(
                                                "[BalanceSvc] Cuenta Unificada — "
                                                "saldo Spot: %.2f USDC",
                                                spot_val,
                                            )
                                            return spot_val
                                    except Exception:
                                        pass

                        if val is None:
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
                            return None

                        return val

            except aiohttp.ClientError as e:
                logger.debug("[BalanceSvc] _fetch_direct red error (intento %d): %s", attempt + 1, e)
                await asyncio.sleep(1.0)
            except Exception as e:
                logger.debug("[BalanceSvc] _fetch_direct error (intento %d): %s", attempt + 1, e)
                await asyncio.sleep(1.0)

        logger.warning(
            "[BalanceSvc] _fetch_direct falló tras %d intentos (addr=%s)",
            _MAX_RETRIES, self._addr,
        )
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

    def _extract_spot(self, data: dict) -> float | None:
        """
        Extrae el saldo USDC de la respuesta spotClearinghouseState.

        Estructura esperada:
          { "balances": [ { "coin": "USDC", "token": 0, "total": "289.63", "hold": "0.0" }, ... ] }
        """
        if not isinstance(data, dict):
            return None
        balances = data.get("balances", [])
        if not isinstance(balances, list):
            return None
        for entry in balances:
            if not isinstance(entry, dict):
                continue
            # USDC puede aparecer como "USDC" o token id 0
            coin = entry.get("coin", "")
            token = entry.get("token")
            if coin == "USDC" or token == 0:
                for field in ("total", "withdrawable", "hold"):
                    v = entry.get(field)
                    if v is not None:
                        try:
                            val = float(v)
                            if val > 0:
                                logger.debug(
                                    "[BalanceSvc] Spot balance=%.2f USDC (balances[USDC].%s)",
                                    val, field,
                                )
                                return val
                        except (ValueError, TypeError):
                            pass
        return None

    # ── API pública ────────────────────────────────────────────────────────────────

    async def get(self) -> float | None:
        """
        Devuelve balance cacheado o refresca si ha caducado.

        REGLAS DE CACHÉ:
        - Si el fetch tiene éxito → actualiza caché y timestamp.
        - Si el fetch falla (429, red, etc.) → conserva el último valor conocido
          en lugar de devolver 0 o None. Esto evita que un rate-limit
          momentáneo pause todos los traders.
        - Si nunca hubo un fetch exitoso y falla → devuelve None.
        """
        if not self._ready:
            logger.warning("[BalanceSvc] get() llamado antes de init_hl()")
            return None

        async with self._lock:
            now = time.time()

            # Caché válida — devolverla directamente
            if (
                self._cache is not None
                and now - self._ts < _CACHE_TTL
            ):
                return self._cache

            # Intentar actualizar
            val = await self._fetch_via_callback()
            if val is None:
                val = await self._fetch_direct()

            if val is not None:
                # Fetch exitoso → actualizar caché
                self._cache = val
                self._ts    = now
                logger.info(
                    "[BalanceSvc] Balance actualizado: %.2f USDC (addr=%s)",
                    val, self._addr,
                )
            else:
                # Fetch fallido → mantener caché anterior si existe
                if self._cache is not None:
                    logger.warning(
                        "[BalanceSvc] ⚠️ Fetch fallido — usando balance cacheado: %.2f USDC",
                        self._cache,
                    )
                    # No actualizar self._ts para que reintente pronto
                else:
                    logger.warning(
                        "[BalanceSvc] ⚠️ No se pudo obtener balance USDC "
                        "(addr=%s — verifica que sea el wallet MASTER con fondos)",
                        self._addr,
                    )

            return self._cache  # puede ser None si nunca tuvo éxito


balance_svc = _BalanceService()
