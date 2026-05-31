"""
pair_scanner.py — Escáner de pares para Hyperliquid perpetuos.

Hyperliquid solo tiene perpetuos USDT (todos contra USD sintético).
La API /info con type="metaAndAssetCtxs" devuelve todos los mercados
y sus métricas en tiempo real (volumen, funding, precio).

No se requieren credenciales — es un endpoint público de lectura.

Formato de símbolo devuelto: nombre corto de coin ("BTC", "ETH", "SOL")
Compatible con ws_feed.py y trader.py que normalizan internamente.

Método extra: inject_snapshot(raw_text) — inyecta datos de una tabla de
mercados pegada manualmente (sin llamadas a IA) usando market_snapshot.py.
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

    _last_scored: lista completa de dicts con datos enriquecidos del último scan.
    Utilizada por main.py para pasar datos reales a ai_rank_pairs sin depender
    del stub fetch_ticker que siempre devolvía quoteVolume=0.

    FIX: si prevDayPx es 0 o null (frecuente en el arranque o tras reset de
    mercado en Hyperliquid), el change_pct se marca como None y el filtro
    de cambio mínimo se OMITE para ese par — no se penaliza por dato faltante.

    inject_snapshot(): acepta texto pegado de la UI del exchange y lo parsea
    con market_snapshot.py, sin depender de ninguna IA externa.
    """

    def __init__(
        self,
        api_key=None, api_secret=None, passphrase=None,   # ignorados, compatibilidad
        min_volume_usdt=1_000_000,
        min_price_change_pct=0.5,
        top_n=15,
        refresh_interval_min=30,
    ):
        self.min_volume_usdt      = min_volume_usdt
        self.min_price_change_pct = min_price_change_pct
        self.top_n                = top_n
        self.refresh_interval     = refresh_interval_min * 60
        self.active_pairs: list   = []
        self._last_scored: list   = []   # ← expuesto para main.py

        extra = os.getenv("SYMBOL_BLACKLIST", "")
        self.blacklist = NON_CRYPTO_BASES | {
            s.strip().upper() for s in extra.split(",") if s.strip()
        }

        # Stub: exchange attr para compatibilidad con código legado
        self.exchange = _HLExchangeStub()

    def _is_valid(self, coin: str) -> bool:
        if coin.upper() in self.blacklist:
            return False
        if len(coin) < 2 or len(coin) > 12:
            return False
        return True

    # ------------------------------------------------------------------
    # inject_snapshot — entrada manual sin IA
    # ------------------------------------------------------------------

    def inject_snapshot(self, raw_text: str) -> list[str]:
        """
        Parsea texto pegado de la UI del exchange y sobreescribe _last_scored.

        No llama a ninguna IA. Usa market_snapshot.parse_snapshot() y
        snapshot_to_scanner_format() para producir la misma estructura
        que devuelve scan().

        Parámetros de filtro reutilizados de la instancia:
          - min_volume_usdt
          - min_price_change_pct (como |change_pct|)
          - top_n

        Devuelve: lista de símbolos (igual que scan()).
        Colaterales USDE/USDH/USDT se excluyen por defecto para no
        duplicar pares (el bot opera en USDC).
        """
        from bot.market_snapshot import parse_snapshot, snapshot_to_scanner_format

        rows = parse_snapshot(raw_text)
        scored = snapshot_to_scanner_format(
            rows,
            min_volume_usdt=self.min_volume_usdt,
            min_change_pct=self.min_price_change_pct,
            top_n=self.top_n,
            exclude_quotes={"USDE", "USDH", "USDT"},  # solo operar en USDC
            exclude_collateral=set(),
        )

        self._last_scored = scored
        self.active_pairs = [s["symbol"] for s in scored]

        logger.info(
            "[PairScanner] inject_snapshot: %d mercados activos → top %d seleccionados",
            sum(1 for r in rows if r.active), len(scored),
        )
        for p in scored[:5]:
            logger.info(
                "  %-12s Vol: $%sM | Cambio: %.2f%% | Funding: %.4f%% | Score: %s",
                p["symbol"], p["volume_usdt"], p["change_pct"], p["funding"], p["score"],
            )

        return self.active_pairs

    async def scan(self) -> list:
        """Devuelve lista de coins (ej: ["BTC", "ETH", "SOL"]) ordenada por score."""
        try:
            data = await _info_post({"type": "metaAndAssetCtxs"})
        except Exception as e:
            logger.error("[PairScanner] Error fetching metaAndAssetCtxs: %s", e)
            return []

        universe = data[0].get("universe", []) if isinstance(data, list) and data else []
        ctxs     = data[1] if isinstance(data, list) and len(data) > 1 else []

        total_seen = 0
        skipped_blacklist = 0
        skipped_volume = 0
        skipped_change = 0

        scored = []
        for i, meta in enumerate(universe):
            coin = meta.get("name", "")
            if not self._is_valid(coin):
                skipped_blacklist += 1
                continue
            total_seen += 1
            ctx = ctxs[i] if i < len(ctxs) else {}

            try:
                day_volume    = float(ctx.get("dayNtlVlm",    0) or 0)
                mark_px       = float(ctx.get("markPx",       0) or 0)
                prev_day_px_r = ctx.get("prevDayPx")
                prev_day_px   = float(prev_day_px_r) if prev_day_px_r not in (None, "", "0", 0) else 0.0
                funding       = float(ctx.get("funding",      0) or 0)
                open_interest = float(ctx.get("openInterest", 0) or 0)
            except (ValueError, TypeError):
                continue

            if day_volume < self.min_volume_usdt or mark_px <= 0:
                skipped_volume += 1
                continue

            # FIX: si prevDayPx no está disponible (0/null), change_pct=None
            # → omitir el filtro de cambio mínimo (no penalizar por dato faltante).
            # Esto ocurre frecuentemente en el primer scan tras arranque del bot.
            if prev_day_px > 0:
                change_pct: float | None = abs((mark_px - prev_day_px) / prev_day_px * 100)
                if change_pct < self.min_price_change_pct:
                    skipped_change += 1
                    continue
            else:
                change_pct = None  # desconocido — no filtrar

            score_change = change_pct if change_pct is not None else 0.0
            score = (day_volume / 1_000_000) * 0.6 + score_change * 0.4
            scored.append({
                "symbol":      coin,
                "volume_usdt": round(day_volume / 1_000_000, 2),
                "change_pct":  round(change_pct, 2) if change_pct is not None else None,
                "last_price":  mark_px,
                "funding":     round(funding * 100, 5),
                "oi_usdt":     round(open_interest * mark_px / 1_000_000, 2),
                "score":       round(score, 3),
            })

        logger.debug(
            "[PairScanner] scan: total=%d | blacklist=%d | vol_filter=%d | change_filter=%d | passed=%d",
            total_seen, skipped_blacklist, skipped_volume, skipped_change, len(scored),
        )

        scored.sort(key=lambda x: x["score"], reverse=True)
        top = scored[:self.top_n]

        # Guardar lista completa para que main.py la use con ai_rank_pairs
        self._last_scored = top

        logger.info("🏆 Top %d pares Hyperliquid seleccionados:", len(top))
        for p in top[:5]:
            change_str = f"{p['change_pct']}%" if p["change_pct"] is not None else "N/A"
            logger.info(
                "  %-12s Vol: $%sM | Cambio: %s | Score: %s",
                p["symbol"], p["volume_usdt"], change_str, p["score"],
            )

        # NOTA: NO actualizamos self.active_pairs aquí — eso lo hace
        # run_scanner_loop después de calcular diff, para no destruir
        # la referencia que usa on_pairs_updated para detectar cambios.
        return [p["symbol"] for p in top]

    def normalize(self, symbol: str) -> str:
        """Compatibilidad con main.py — Hyperliquid ya devuelve nombres cortos."""
        return symbol.replace("/", "").replace(":USDT", "").replace("USDT", "").upper()

    async def run_scanner_loop(self, on_update_callback):
        while True:
            await asyncio.sleep(self.refresh_interval)
            try:
                logger.info("🔍 Re-escaneando mercado Hyperliquid...")
                new_pairs = await self.scan()
                if not new_pairs:
                    logger.warning("⚠️ Scanner devolvió 0 pares — manteniendo pares actuales")
                    continue
                added   = set(new_pairs) - set(self.active_pairs)
                removed = set(self.active_pairs) - set(new_pairs)
                # Actualizar active_pairs DESPUÉS de calcular el diff
                self.active_pairs = new_pairs
                if added:
                    logger.info("➕ Nuevos pares: %s", ", ".join(added))
                if removed:
                    logger.info("➖ Pares eliminados: %s", ", ".join(removed))
                if added or removed:
                    await on_update_callback(new_pairs)
                else:
                    logger.info("✅ Pares sin cambios — no se reinician traders")
            except Exception as e:
                logger.error("PairScanner error: %s", e)

    async def close(self):
        pass


class _HLExchangeStub:
    """Stub mínimo para compatibilidad con código legado."""
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
