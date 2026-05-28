import asyncio
import base64
import logging
import os
import hmac
import hashlib
import time
import json as _json
import aiohttp
import ccxt.async_support as ccxt
from bot.strategy import decide
from bot.ai_trader import ai_decide
from bot.telegram_bot import notify_open, notify_close
from bot.state import (
    save_position, load_position, clear_position, mark_tp2_hit
)
from bot.telegram_bot import notify_tp_partial

logger = logging.getLogger("Trader")

# ─────────────────────────────────────────────────────────────
# CONSTANTES GLOBALES
# ─────────────────────────────────────────────────────────────

TP2_PARTIAL_RATIO = float(os.getenv("TP2_PARTIAL_RATIO", "0.5"))

# Mínimos de qty conocidos por símbolo (fallback si la API no responde)
_MIN_QTY_FALLBACK = {
    "BTCUSDT":   0.001,
    "ETHUSDT":   0.01,
    "SOLUSDT":   0.1,
    "XRPUSDT":   1.0,
    "SUIUSDT":   1.0,
    "NEARUSDT":  0.1,
    "XLMUSDT":   1.0,
    "XAUUSDT":   0.01,
    "XAUTUSDT":  0.001,
    "XAGUSDT":   0.1,
    "HYPEUSDT":  0.1,
    "FILOUSDT":  0.1,
    "FILUSDT":   0.1,
    "SOXLUSDT":  0.1,
    "ZECUSDT":   0.01,
    "WLDUSDT":   0.1,
    "BEATUSDT":  1.0,
    "BZUSDT":    1.0,
    "TAOUSDT":   0.001,
    "ADAUSDT":   1.0,
    "DOGEUSDTUSDT": 1.0,
}

# Cache de min_qty leídos desde API (sym → float)
_min_qty_cache: dict = {}

# ─────────────────────────────────────────────────────────────
# CACHÉ GLOBAL DE BALANCE (compartido por todos los traders)
# TTL solo se actualiza cuando la llamada HTTP TIENE ÉXITO.
# Si falla, se reintenta en el próximo ciclo sin bloquear.
# ─────────────────────────────────────────────────────────────
_BALANCE_CACHE_TTL  = int(os.getenv("BALANCE_CACHE_TTL", "30"))   # segundos
_balance_cache_value: float | None = None   # None = nunca obtenido con éxito
_balance_cache_ts:    float = 0.0           # solo se actualiza en éxito
_balance_fetch_lock:  asyncio.Lock = None   # lazy-init


def _get_balance_lock() -> asyncio.Lock:
    global _balance_fetch_lock
    if _balance_fetch_lock is None:
        _balance_fetch_lock = asyncio.Lock()
    return _balance_fetch_lock


async def _safe_json(response) -> dict:
    """
    Parsea r.json() de forma segura.
    - Fuerza content_type=None para evitar error cuando Bitget devuelve
      text/html o text/plain en errores HTTP.
    - Si el resultado no es un dict, lanza ValueError con el contenido.
    """
    data = await response.json(content_type=None)
    if not isinstance(data, dict):
        raise ValueError(f"Respuesta no-JSON (tipo {type(data).__name__}): {str(data)[:300]}")
    return data


async def _fetch_balance_once(api_key, api_secret, passphrase) -> float | None:
    """
    Llama a la API de Bitget y devuelve el balance USDT disponible.
    Actualiza el caché global SOLO si la llamada tiene éxito.

    Retorna:
      - float  : balance real obtenido de la API
      - None   : los 3 endpoints fallaron — el caller debe tratar como 'desconocido'

    NUNCA devuelve 0.0 para indicar fallo (0.0 significa cuenta vacía real).
    """
    global _balance_cache_value, _balance_cache_ts

    def _sign(ts, method, path_with_qs, body=""):
        msg = ts + method.upper() + path_with_qs + body
        return base64.b64encode(
            hmac.new(api_secret.encode(), msg.encode(), hashlib.sha256).digest()
        ).decode()

    def _headers(method, path_with_qs, body=""):
        ts = str(int(time.time() * 1000))
        return {
            "ACCESS-KEY":        api_key,
            "ACCESS-SIGN":       _sign(ts, method, path_with_qs, body),
            "ACCESS-TIMESTAMP":  ts,
            "ACCESS-PASSPHRASE": passphrase,
            "Content-Type":      "application/json",
            "locale":            "en-US",
        }

    # ─────────────────────────────────────────────────────────────
    # ENDPOINT 1: v3/account/assets — Unified Account
    # BUG FIX: "available" puede ser "0" (string) en UA aunque haya fondos.
    # Probamos en orden: available → crossMaxAvailable → usdtEquity
    # Usamos conversión segura: _to_float() evita que "0" truthy-falsy engañe.
    # ─────────────────────────────────────────────────────────────
    def _to_float(val) -> float | None:
        """Convierte val a float; devuelve None si es None/vacío, float si es número."""
        if val is None:
            return None
        try:
            return float(val)
        except (ValueError, TypeError):
            return None

    try:
        path = "/api/v3/account/assets"
        qs   = "?coin=USDT"
        url  = "https://api.bitget.com" + path + qs
        async with aiohttp.ClientSession() as s:
            async with s.get(url, headers=_headers("GET", path + qs),
                             timeout=aiohttp.ClientTimeout(total=10)) as r:
                data = await _safe_json(r)
        logger.debug(f"[BalanceCache] v3 raw response: {data}")
        if data.get("code") == "00000":
            for item in (data.get("data") or []):
                if item.get("coin") == "USDT":
                    # Probar campos en orden de prioridad para Unified Account
                    bal = None
                    for field in ("available", "crossMaxAvailable", "usdtEquity",
                                  "isolatedMaxAvailable", "equity"):
                        v = _to_float(item.get(field))
                        if v is not None and v > 0:
                            bal = v
                            logger.debug(f"[BalanceCache] v3 campo usado: {field}={v}")
                            break
                    # Si todos son 0.0 explícitamente → cuenta vacía real
                    if bal is None:
                        bal = _to_float(item.get("available")) or 0.0
                    _balance_cache_value = bal
                    _balance_cache_ts    = time.monotonic()
                    logger.info(f"[BalanceCache] ✅ Balance USDT (v3): {bal:.2f}")
                    return bal
            logger.warning("[BalanceCache] ⚠️ v3 OK pero sin ítem USDT en data")
        else:
            logger.warning(
                f"[BalanceCache] ⚠️ v3 code={data.get('code')} msg={data.get('msg')}"
            )
    except ValueError as e:
        logger.warning(f"[BalanceCache] ⚠️ v3 respuesta inesperada: {e}")
    except Exception as e:
        logger.warning(f"[BalanceCache] ❌ v3 excepción: {e}")

    # ─────────────────────────────────────────────────────────────
    # ENDPOINT 2: v2/mix/account/account (singular) — UA + Classic
    # Este endpoint devuelve el balance real de la cuenta de futuros USDT.
    # Funciona en Unified Account cuando v3/assets devuelve available=0.
    # ─────────────────────────────────────────────────────────────
    try:
        path = "/api/v2/mix/account/account"
        qs   = "?symbol=USDTUSDT&productType=USDT-FUTURES&marginCoin=USDT"
        url  = "https://api.bitget.com" + path + qs
        async with aiohttp.ClientSession() as s:
            async with s.get(url, headers=_headers("GET", path + qs),
                             timeout=aiohttp.ClientTimeout(total=10)) as r:
                data = await _safe_json(r)
        logger.debug(f"[BalanceCache] v2-single raw response: {data}")
        if data.get("code") == "00000":
            d = data.get("data") or {}
            for field in ("available", "crossMaxAvailable", "usdtEquity", "equity"):
                v = _to_float(d.get(field))
                if v is not None and v > 0:
                    _balance_cache_value = v
                    _balance_cache_ts    = time.monotonic()
                    logger.info(f"[BalanceCache] ✅ Balance USDT (v2-single/{field}): {v:.2f}")
                    return v
            # Todos los campos son 0 → cuenta de futuros vacía real
            bal = _to_float(d.get("available")) or 0.0
            _balance_cache_value = bal
            _balance_cache_ts    = time.monotonic()
            logger.info(f"[BalanceCache] ✅ Balance USDT (v2-single vacía): {bal:.2f}")
            return bal
        else:
            code = data.get("code")
            msg  = data.get("msg")
            if code == "40085":
                logger.debug("[BalanceCache] v2-single: 40085 (esperado en algunas cuentas)")
            else:
                logger.warning(f"[BalanceCache] ⚠️ v2-single code={code} msg={msg}")
    except ValueError as e:
        logger.warning(f"[BalanceCache] ⚠️ v2-single respuesta inesperada: {e}")
    except Exception as e:
        logger.warning(f"[BalanceCache] ❌ v2-single excepción: {e}")

    # ─────────────────────────────────────────────────────────────
    # ENDPOINT 3: v2/mix/account/accounts (plural) — Classic fallback
    # ─────────────────────────────────────────────────────────────
    try:
        path = "/api/v2/mix/account/accounts"
        qs   = "?productType=USDT-FUTURES"
        url  = "https://api.bitget.com" + path + qs
        async with aiohttp.ClientSession() as s:
            async with s.get(url, headers=_headers("GET", path + qs),
                             timeout=aiohttp.ClientTimeout(total=10)) as r:
                data = await _safe_json(r)
        logger.debug(f"[BalanceCache] v2-multi raw response: {data}")
        if data.get("code") == "00000":
            items = data.get("data") or []
            if items:
                d = items[0]
                for field in ("available", "crossMaxAvailable", "usdtEquity"):
                    v = _to_float(d.get(field))
                    if v is not None and v > 0:
                        _balance_cache_value = v
                        _balance_cache_ts    = time.monotonic()
                        logger.info(f"[BalanceCache] ✅ Balance USDT (v2-multi/{field}): {v:.2f}")
                        return v
                bal = _to_float(d.get("available")) or 0.0
                _balance_cache_value = bal
                _balance_cache_ts    = time.monotonic()
                logger.info(f"[BalanceCache] ✅ Balance USDT (v2-multi vacía): {bal:.2f}")
                return bal
        else:
            code = data.get("code")
            msg  = data.get("msg")
            if code == "40085":
                logger.debug("[BalanceCache] v2-multi: 40085 (Unified Account, esperado)")
            else:
                logger.warning(f"[BalanceCache] ⚠️ v2-multi code={code} msg={msg}")
    except ValueError as e:
        logger.warning(f"[BalanceCache] ⚠️ v2-multi respuesta inesperada: {e}")
    except Exception as e:
        logger.warning(f"[BalanceCache] ❌ v2-multi excepción: {e}")

    # ── Los 3 endpoints fallaron ──
    logger.error(
        "[BalanceCache] 🚨 Los 3 endpoints fallaron — balance NO actualizado. "
        f"Valor en caché: {_balance_cache_value}. "
        "Se reintentará en el próximo ciclo."
    )
    return None


async def get_cached_balance(api_key, api_secret, passphrase) -> float | None:
    """
    Devuelve el balance USDT desde caché global.
    Solo llama a la API si han pasado más de BALANCE_CACHE_TTL segundos
    desde la ÚLTIMA LLAMADA EXITOSA.

    Retorna:
      - float  : balance conocido (puede ser 0.0 si cuenta vacía)
      - None   : API nunca respondió con éxito o caché expiró y fallo nuevo

    El caller (trader.py) debe tratar None como 'balance desconocido',
    NO como 'balance = 0'.
    """
    global _balance_cache_value, _balance_cache_ts
    lock = _get_balance_lock()
    now  = time.monotonic()

    if _balance_cache_value is not None and now - _balance_cache_ts < _BALANCE_CACHE_TTL:
        return _balance_cache_value

    async with lock:
        now = time.monotonic()
        if _balance_cache_value is not None and now - _balance_cache_ts < _BALANCE_CACHE_TTL:
            return _balance_cache_value
        result = await _fetch_balance_once(api_key, api_secret, passphrase)
        if result is None:
            # API falló — devolver caché anterior si existe, o None si nunca hubo éxito
            if _balance_cache_value is not None:
                logger.warning(
                    f"[BalanceCache] ⚠️ API falló, usando caché anterior: "
                    f"{_balance_cache_value:.2f} USDT"
                )
                return _balance_cache_value
            return None
        return result

