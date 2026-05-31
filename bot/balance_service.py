"""
balance_service.py — Singleton de balance USDC para Hyperliquid.

Estrategia para Cuenta Unificada:
  Hyperliquid Cuenta Unificada permite usar USDC de Spot como colateral de perpetuals.
  clearinghouseState.marginSummary.accountValue = 0 hasta el primer trade perp.
  spotClearinghouseState.balances[USDC].total   = saldo real disponible.

  Por tanto, siempre consultamos AMBOS endpoints en paralelo y devolvemos
  el mayor de los dos valores (o el único disponible).

Endpoints:
  POST /info {type: "clearinghouseState", user: <addr>}    → perps equity
  POST /info {type: "spotClearinghouseState", user: <addr>} → spot USDC

No requieren firma — son endpoints públicos de lectura.

IMPORTANTE: usar siempre la dirección del wallet MASTER (el que tiene fondos),
nunca la del agente.
"""
import asyncio
import logging
import time
import json as _json
import aiohttp

logger = logging.getLogger("BalanceSvc")

_CACHE_TTL   = 30
_MAX_RETRIES = 3


class _BalanceService:
    def __init__(self):
        self._addr:   str   = ""
        self._cache:  float | None = None
        self._ts:     float = 0.0
        self._lock    = asyncio.Lock()
        self._ready   = False
        self._api_url = "https://api.hyperliquid.xyz"

    def is_ready(self) -> bool:
        return self._ready

    def init(self, key: str = "", secret: str = "", passphrase: str = ""):
        logger.warning("[BalanceSvc] init() ignorado — usa init_hl()")

    def init_hl(self, addr: str, info_post_fn=None, testnet: bool = False):
        if not addr:
            return
        if self._ready:
            if self._addr.lower() != addr.lower():
                logger.error("[BalanceSvc] CONFLICTO addr: %s vs %s", self._addr[:10], addr[:10])
            return
        self._addr    = addr
        self._api_url = (
            "https://api.hyperliquid-testnet.xyz" if testnet
            else "https://api.hyperliquid.xyz"
        )
        self._ready = True
        logger.info("[BalanceSvc] Inicializado — consultando balance de MASTER addr=%s", addr)

    def invalidate(self):
        self._ts    = 0.0
        self._cache = None

    # ── HTTP directo (sin depender del callback del trader) ──────────────

    async def _post(self, session: aiohttp.ClientSession, payload: dict) -> dict | None:
        """POST a /info, devuelve dict o None si falla."""
        try:
            async with session.post(
                f"{self._api_url}/info",
                json=payload,
                headers={"Content-Type": "application/json"},
                timeout=aiohttp.ClientTimeout(total=10),
            ) as r:
                if r.status == 429:
                    logger.warning("[BalanceSvc] 429 rate-limit en /info")
                    return None
                if r.status >= 400:
                    logger.warning("[BalanceSvc] HTTP %s en /info", r.status)
                    return None
                text = await r.text()
                return _json.loads(text)
        except Exception as e:
            logger.debug("[BalanceSvc] _post error: %s", e)
            return None

    async def _fetch(self) -> float | None:
        """
        Consulta clearinghouseState Y spotClearinghouseState en paralelo.
        Devuelve el mayor de los dos valores (Cuenta Unificada: el USDC está
        en Spot pero se usa como colateral de perps).
        """
        for attempt in range(_MAX_RETRIES):
            try:
                async with aiohttp.ClientSession() as s:
                    # Consultar ambos endpoints en paralelo
                    perp_task = asyncio.create_task(
                        self._post(s, {"type": "clearinghouseState", "user": self._addr})
                    )
                    spot_task = asyncio.create_task(
                        self._post(s, {"type": "spotClearinghouseState", "user": self._addr})
                    )
                    perp_data, spot_data = await asyncio.gather(perp_task, spot_task)

                    perp_val = self._extract_perp(perp_data)
                    spot_val = self._extract_spot(spot_data)

                    logger.debug(
                        "[BalanceSvc] perp=%.2f  spot=%.2f",
                        perp_val or 0.0, spot_val or 0.0,
                    )

                    # Si ambos fallaron → reintentar
                    if perp_val is None and spot_val is None:
                        logger.warning(
                            "[BalanceSvc] Ambos endpoints fallaron (intento %d/%d) — "
                            "addr=%s  perp_raw=%s  spot_raw=%s",
                            attempt + 1, _MAX_RETRIES,
                            self._addr,
                            perp_data,
                            spot_data,
                        )
                        await asyncio.sleep(1.5 * (attempt + 1))
                        continue

                    # Cuenta Unificada: tomar el mayor
                    candidates = [v for v in (perp_val, spot_val) if v is not None]
                    result = max(candidates)

                    if spot_val and spot_val > 0 and (perp_val or 0) == 0:
                        logger.info(
                            "[BalanceSvc] Cuenta Unificada — saldo Spot: %.2f USDC",
                            spot_val,
                        )

                    return result

            except Exception as e:
                logger.debug("[BalanceSvc] _fetch error (intento %d): %s", attempt + 1, e)
                await asyncio.sleep(1.0)

        logger.warning(
            "[BalanceSvc] _fetch falló tras %d intentos (addr=%s)",
            _MAX_RETRIES, self._addr,
        )
        return None

    def _extract_perp(self, data: dict | None) -> float | None:
        """Extrae accountValue de clearinghouseState."""
        if not isinstance(data, dict):
            return None
        for key in ("marginSummary", "crossMarginSummary"):
            ms = data.get(key, {})
            if isinstance(ms, dict):
                for field in ("accountValue", "withdrawable"):
                    v = ms.get(field)
                    if v is not None:
                        try:
                            val = float(v)
                            if val >= 0:
                                return val
                        except (ValueError, TypeError):
                            pass
        for field in ("withdrawable", "totalRawUsd", "crossAccountValue"):
            v = data.get(field)
            if v is not None:
                try:
                    val = float(v)
                    if val >= 0:
                        return val
                except (ValueError, TypeError):
                    pass
        return None

    def _extract_spot(self, data: dict | None) -> float | None:
        """Extrae saldo USDC de spotClearinghouseState."""
        if not isinstance(data, dict):
            return None
        balances = data.get("balances", [])
        if not isinstance(balances, list):
            return None
        for entry in balances:
            if not isinstance(entry, dict):
                continue
            coin  = entry.get("coin", "")
            token = entry.get("token")
            if coin == "USDC" or token == 0:
                v = entry.get("total")
                if v is None:
                    v = entry.get("withdrawable")
                if v is not None:
                    try:
                        val = float(v)
                        if val >= 0:
                            return val
                    except (ValueError, TypeError):
                        pass
        return None

    # ── API pública ──────────────────────────────────────────────────────

    async def get(self) -> float | None:
        if not self._ready:
            logger.warning("[BalanceSvc] get() llamado antes de init_hl()")
            return None

        async with self._lock:
            now = time.time()
            if self._cache is not None and now - self._ts < _CACHE_TTL:
                return self._cache

            val = await self._fetch()

            if val is not None:
                self._cache = val
                self._ts    = now
                logger.info(
                    "[BalanceSvc] Balance actualizado: %.2f USDC (addr=%s)",
                    val, self._addr,
                )
            else:
                if self._cache is not None:
                    logger.warning(
                        "[BalanceSvc] Fetch fallido — usando cache: %.2f USDC",
                        self._cache,
                    )
                else:
                    logger.warning(
                        "[BalanceSvc] No se pudo obtener balance (addr=%s)",
                        self._addr,
                    )

            return self._cache


balance_svc = _BalanceService()
