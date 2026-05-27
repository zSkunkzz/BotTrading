import logging
import asyncio
import os
import ccxt.async_support as ccxt

logger = logging.getLogger("PairScanner")

# Activos NO-crypto: acciones tokenizadas, ETFs, commodities, índices
# Bitget los lista como swaps pero su comportamiento es distinto
NON_CRYPTO_BASES = {
    # Acciones tech
    "AAPL", "TSLA", "NVDA", "AMZN", "GOOGL", "META", "MSFT", "NFLX",
    "AMD", "INTC", "MU", "SNDK", "QCOM", "AVGO", "CRM", "ORCL",
    # Commodities
    "CL", "GC", "SI", "NG", "HG", "ZC", "ZW", "ZS",
    # Índices
    "SPX", "NDX", "DJI", "VIX",
    # Acciones variadas
    "COIN", "MSTR", "MARA", "RIOT", "BSB", "GME", "AMC",
    "ABNB", "UBER", "LYFT", "SNAP", "PINS", "TWTR",
}


class PairScanner:
    """
    Escanea en tiempo real todos los pares USDT de futuros perpetuos en Bitget.
    Filtra por volumen, volatilidad y tendencia para elegir los mejores.
    Solo opera pares crypto puros — excluye acciones tokenizadas y commodities.
    Se refresca cada X minutos para detectar pares nuevos automáticamente.
    """

    def __init__(self, api_key, api_secret, passphrase,
                 min_volume_usdt=20_000_000,
                 min_price_change_pct=1.5,
                 top_n=15,
                 refresh_interval_min=30):
        self.exchange = ccxt.bitget({
            "apiKey": api_key,
            "secret": api_secret,
            "password": passphrase,
            "options": {"defaultType": "swap"},
        })
        self.min_volume_usdt = min_volume_usdt
        self.min_price_change_pct = min_price_change_pct
        self.top_n = top_n
        self.refresh_interval = refresh_interval_min * 60
        self.active_pairs: list = []
        # Blacklist adicional configurable via .env
        # Ej: SYMBOL_BLACKLIST=ZEC,BSB,MU
        extra = os.getenv("SYMBOL_BLACKLIST", "")
        self.blacklist = NON_CRYPTO_BASES | {
            s.strip().upper() for s in extra.split(",") if s.strip()
        }

    def _is_crypto_pair(self, symbol: str, market: dict) -> bool:
        """Devuelve True solo si el par es crypto pura, no accion/commodity"""
        base = market.get("base", "").upper()
        # Excluir si el base está en la lista negra
        if base in self.blacklist:
            return False
        # Los pares de acciones tokenizadas suelen tener / seguido de USDT:USDT
        # pero el base tiene solo 2-5 letras que coinciden con tickers de bolsa.
        # Heurística adicional: si el base tiene menos de 2 o más de 10 chars
        # y no es crypto conocida, excluir.
        if len(base) < 2 or len(base) > 10:
            return False
        return True

    async def get_all_usdt_perp_pairs(self) -> list:
        markets = await self.exchange.load_markets(reload=True)
        pairs = [
            s for s, m in markets.items()
            if m.get("quote") == "USDT"
            and m.get("type") == "swap"
            and m.get("active", True)
            and not m.get("expiry")
            and self._is_crypto_pair(s, m)
        ]
        logger.info(f"📋 Pares USDT perp crypto disponibles: {len(pairs)}")
        return pairs

    async def score_pair(self, symbol: str) -> dict | None:
        try:
            ticker = await self.exchange.fetch_ticker(symbol)
            volume_usdt = float(ticker.get("quoteVolume") or 0)
            change_pct = abs(float(ticker.get("percentage") or 0))
            last = float(ticker.get("last") or 0)
            if volume_usdt < self.min_volume_usdt:
                return None
            if change_pct < self.min_price_change_pct:
                return None
            if last <= 0:
                return None
            score = (volume_usdt / 1_000_000) * 0.6 + change_pct * 0.4
            return {
                "symbol": symbol,
                "volume_usdt": round(volume_usdt / 1_000_000, 2),
                "change_pct": round(change_pct, 2),
                "last_price": last,
                "score": round(score, 3),
            }
        except Exception:
            return None

    async def scan(self) -> list:
        all_pairs = await self.get_all_usdt_perp_pairs()
        scored = []
        batch_size = 20
        for i in range(0, len(all_pairs), batch_size):
            batch = all_pairs[i:i + batch_size]
            results = await asyncio.gather(*[self.score_pair(s) for s in batch])
            scored.extend([r for r in results if r])
            await asyncio.sleep(0.5)
        scored.sort(key=lambda x: x["score"], reverse=True)
        top = scored[:self.top_n]
        logger.info(f"🏆 Top {len(top)} pares crypto seleccionados:")
        for p in top[:5]:
            logger.info(
                f"  {p['symbol']:<20} Vol: ${p['volume_usdt']}M | "
                f"Cambio: {p['change_pct']}% | Score: {p['score']}"
            )
        return [p["symbol"] for p in top]

    async def run_scanner_loop(self, on_update_callback):
        while True:
            try:
                logger.info("🔍 Re-escaneando mercado...")
                new_pairs = await self.scan()
                added   = set(new_pairs) - set(self.active_pairs)
                removed = set(self.active_pairs) - set(new_pairs)
                if added:
                    logger.info(f"➕ Nuevos pares: {', '.join(added)}")
                if removed:
                    logger.info(f"➖ Pares eliminados: {', '.join(removed)}")
                self.active_pairs = new_pairs
                await on_update_callback(new_pairs)
            except Exception as e:
                logger.error(f"PairScanner error: {e}")
            await asyncio.sleep(self.refresh_interval)

    async def close(self):
        await self.exchange.close()
