"""
pair_scanner.py — Escáner de pares para Hyperliquid perpetuos.

Hyperliquid solo tiene perpetuos USDT (todos contra USD sintético).
La API /info con type="metaAndAssetCtxs" devuelve todos los mercados
y sus métricas en tiempo real (volumen, funding, precio).

No se requieren credenciales — es un endpoint público de lectura.

Formato de símbolo devuelto: nombre corto de coin ("BTC", "ETH", "SOL")
Compatible con ws_feed.py y trader.py que normalizan internamente.
"""
import logging
import asyncio
import os
import aiohttp
import json as _json

logger = logging.getLogger("PairScanner")

# Activos no-crypto o con comportamiento distinto
NON_CRYPTO_BASES = {
    "AAPL", "TSLA", "NVDA", "AMZN", "GOOGL", "META", "MSFT", "NFLX",
    "AMD", "INTC", "MU", "QCOM", "AVGO", "CRM", "ORCL",
    "CL", "GC", "SI", "NG", "HG",
    "XAU", "XAG", "XAUT",
    "SPX", "NDX", "DJI", "VIX",
    "COIN", "MSTR", "MARA", "RIOT",
}

_USE_TESTNET = os.getenv("HL_TESTNET", "").lower() in ("true", "1", "yes")
_API_URL     = "https://api.hyperliquid-testnet.xyz" if _USE_TESTNET else "https://api.hyperliquid.xyz"


async def _info_post(payload: dict) -> dict:
    async with aiohttp.ClientSession() as s:
        async with s.post(
            f"{_API_URL}/info",
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=aiohttp.ClientTimeout(total=15),
        ) as r:
            return _json.loads(await r.text())


class PairScanner:
    """
    Escanea pares USDT perpetuos de Hyperliquid.
    Filtra por volumen 24h y volatilidad (% cambio).
    Devuelve nombres cortos de coin: "BTC", "ETH", "SOL"...
    """

    def __init__(
        self,
        api_key=None, api_secret=None, passphrase=None,   # ignorados, compatibilidad
        min_volume_usdt=5_000_000,
        min_price_change_pct=1.5,
        top_n=15,
        refresh_interval_min=30,
    ):
        self.min_volume_usdt      = min_volume_usdt
        self.min_price_change_pct = min_price_change_pct
        self.top_n                = top_n
        self.refresh_interval     = refresh_interval_min * 60
        self.active_pairs: list   = []

        extra = os.getenv("SYMBOL_BLACKLIST", "")
        self.blacklist = NON_CRYPTO_BASES | {
            s.strip().upper() for s in extra.split(",") if s.strip()
        }

        # Stub: exchange attr para compatibilidad con main.py (fetch_ticker)
        self.exchange = _HLExchangeStub()

    def _is_valid(self, coin: str) -> bool:
        if coin.upper() in self.blacklist:
            return False
        if len(coin) < 2 or len(coin) > 12:
            return False
        return True

    async def scan(self) -> list:
        """Devuelve lista de coins (ej: ["BTC", "ETH", "SOL"]) ordenada por score."""
        try:
            data = await _info_post({"type": "metaAndAssetCtxs"})
        except Exception as e:
            logger.error("[PairScanner] Error fetching metaAndAssetCtxs: %s", e)
            return []

        universe = data[0].get("universe", []) if isinstance(data, list) and data else []
        ctxs     = data[1] if isinstance(data, list) and len(data) > 1 else []

        scored = []
        for i, meta in enumerate(universe):
            coin = meta.get("name", "")
            if not self._is_valid(coin):
                continue
            ctx = ctxs[i] if i < len(ctxs) else {}

            try:
                day_volume   = float(ctx.get("dayNtlVlm",    0) or 0)   # volumen 24h en USDT
                mark_px      = float(ctx.get("markPx",       0) or 0)
                prev_day_px  = float(ctx.get("prevDayPx",    0) or mark_px)
                funding      = float(ctx.get("funding",      0) or 0)
                open_interest= float(ctx.get("openInterest", 0) or 0)
            except (ValueError, TypeError):
                continue

            if day_volume < self.min_volume_usdt or mark_px <= 0:
                continue

            change_pct = abs((mark_px - prev_day_px) / prev_day_px * 100) if prev_day_px > 0 else 0.0
            if change_pct < self.min_price_change_pct:
                continue

            score = (day_volume / 1_000_000) * 0.6 + change_pct * 0.4
            scored.append({
                "symbol":      coin,
                "volume_usdt": round(day_volume / 1_000_000, 2),
                "change_pct":  round(change_pct, 2),
                "last_price":  mark_px,
                "funding":     round(funding * 100, 5),
                "oi_usdt":     round(open_interest * mark_px / 1_000_000, 2),
                "score":       round(score, 3),
            })

        scored.sort(key=lambda x: x["score"], reverse=True)
        top = scored[:self.top_n]

        logger.info("🏆 Top %d pares Hyperliquid seleccionados:", len(top))
        for p in top[:5]:
            logger.info(
                "  %-12s Vol: $%sM | Cambio: %s%% | Score: %s",
                p["symbol"], p["volume_usdt"], p["change_pct"], p["score"],
            )

        self.active_pairs = [p["symbol"] for p in top]
        return self.active_pairs

    def normalize(self, symbol: str) -> str:
        """Compatibilidad con main.py — Hyperliquid ya devuelve nombres cortos."""
        return symbol.replace("/", "").replace(":USDT", "").replace("USDT", "").upper()

    async def run_scanner_loop(self, on_update_callback):
        while True:
            try:
                logger.info("🔍 Re-escaneando mercado Hyperliquid...")
                new_pairs = await self.scan()
                added     = set(new_pairs) - set(self.active_pairs)
                removed   = set(self.active_pairs) - set(new_pairs)
                if added:
                    logger.info("➕ Nuevos pares: %s", ", ".join(added))
                if removed:
                    logger.info("➖ Pares eliminados: %s", ", ".join(removed))
                self.active_pairs = new_pairs
                await on_update_callback(new_pairs)
            except Exception as e:
                logger.error("PairScanner error: %s", e)
            await asyncio.sleep(self.refresh_interval)

    async def close(self):
        pass


class _HLExchangeStub:
    """Stub mínimo para que main.py pueda llamar fetch_ticker sin crashear."""
    async def fetch_ticker(self, symbol: str) -> dict:
        """Consulta precio actual via /info allMids."""
        try:
            data = await _info_post({"type": "allMids"})
            coin = symbol.replace("/USDT:USDT", "").replace("/USDT", "").replace("USDT", "")
            mid  = data.get(coin, 0)
            return {"last": float(mid), "quoteVolume": 0, "percentage": 0}
        except Exception:
            return {"last": 0, "quoteVolume": 0, "percentage": 0}

    async def close(self):
        pass
