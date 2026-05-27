import logging
import asyncio
import ccxt.async_support as ccxt

logger = logging.getLogger("PairScanner")


class PairScanner:
    """
    Escanea en tiempo real todos los pares USDT de futuros perpetuos en Bitget.
    Filtra por volumen, volatilidad y tendencia para elegir los mejores.
    Se refresca cada X minutos para detectar pares nuevos automáticamente.
    """

    def __init__(self, api_key, api_secret, passphrase,
                 min_volume_usdt=5_000_000,
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

    async def get_all_usdt_perp_pairs(self) -> list:
        markets = await self.exchange.load_markets(reload=True)
        pairs = [
            s for s, m in markets.items()
            if m.get("quote") == "USDT"
            and m.get("type") == "swap"
            and m.get("active", True)
            and not m.get("expiry")
        ]
        logger.info(f"📋 Total pares USDT perp disponibles: {len(pairs)}")
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
        logger.info(f"🏆 Top {len(top)} pares seleccionados:")
        for p in top[:5]:
            logger.info(
                f"  {p['symbol']:<15} Vol: ${p['volume_usdt']}M | "
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
