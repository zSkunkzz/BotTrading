"""
pair_scanner.py — Escaner de pares para BingX perpetuos USDT-margined.

v3 — BingX migration (2026-06-06)
  Sustituye todos los endpoints de OKX/Hyperliquid por la API REST
  pública de BingX (sin firma, sin API key):

    GET /openApi/swap/v2/quote/contracts
        → lista de contratos activos + maxLeverage
    GET /openApi/swap/v2/quote/ticker
        → volumen 24h, precio, openInterest (tradeAmount)
    GET /openApi/swap/v2/quote/premiumIndex
        → funding rate real (lastFundingRate) por símbolo

  Formato símbolo BingX: "BTC-USDT" (no "BTC-USDT-SWAP" ni "BTC").

  Filtros mantenidos:
    SCANNER_TOP_N, SCANNER_REFRESH_MIN, SCANNER_MIN_VOLUME,
    SCANNER_MIN_CHANGE, SCANNER_EXCLUDE_LOW_LEV, SYMBOL_BLACKLIST
    FUNDING_TREND_WINDOW, AI_NEWS_FILTER

  Score: vol 40% + cambio% 30% + funding_abs 20% + OI 10%

  Funding: enriquecido con premiumIndex real (no 0 hardcodeado).
    FUNDING_ENRICH_BATCH (default 50): máx pares a enriquecer por ciclo.
"""
import logging
import asyncio
import os
import aiohttp
import json as _json
from collections import defaultdict

logger = logging.getLogger("PairScanner")

NON_CRYPTO_BASES = {
    "AAPL", "TSLA", "NVDA", "AMZN", "GOOGL", "META", "MSFT", "NFLX",
    "AMD", "INTC", "MU", "QCOM", "AVGO", "CRM", "ORCL",
    "CL", "GC", "SI", "NG", "HG",
    "XAU", "XAG", "XAUT",
    "SPX", "NDX", "DJI", "VIX",
    "COIN", "MSTR", "MARA", "RIOT",
}

# BingX REST pública (no requiere API key ni firma)
_BINGX_BASE = "https://open-api.bingx.com"

_TRADER_STOP_TIMEOUT_S = float(os.getenv("TRADER_STOP_TIMEOUT_S", "15"))
_AI_NEWS_FILTER: bool  = os.getenv("AI_NEWS_FILTER", "false").lower() in ("true", "1", "yes")

_TOP_N               = int(float(os.getenv("SCANNER_TOP_N",           "25")))
_REFRESH_MIN         = int(float(os.getenv("SCANNER_REFRESH_MIN",     "15")))
_MIN_VOLUME          = float(os.getenv("SCANNER_MIN_VOLUME",          "500000"))
_MIN_CHANGE_PCT      = float(os.getenv("SCANNER_MIN_CHANGE",          "0.3"))
_MIN_LEV             = int(float(os.getenv("SCANNER_EXCLUDE_LOW_LEV", "3")))
_FUNDING_ENRICH_BATCH = int(os.getenv("FUNDING_ENRICH_BATCH",         "50"))

_FUNDING_TREND_N: int = int(os.getenv("FUNDING_TREND_WINDOW", "3"))
_funding_history: dict[str, list[float]] = defaultdict(list)


def _funding_trend(symbol: str, current: float) -> str:
    hist = _funding_history[symbol]
    hist.append(current)
    if len(hist) > _FUNDING_TREND_N:
        hist.pop(0)
    if len(hist) < 2:
        return "NEUTRAL"
    return "RISING" if hist[-1] > hist[0] else "FALLING"


async def _bingx_get(path: str, params: dict | None = None) -> dict | list:
    """
    GET a la API pública de BingX.
    Los endpoints de market data no requieren firma.
    Devuelve body["data"] o body completo según el endpoint.
    """
    async with aiohttp.ClientSession() as s:
        async with s.get(
            f"{_BINGX_BASE}{path}",
            params=params,
            headers={"Content-Type": "application/json"},
            timeout=aiohttp.ClientTimeout(total=15),
        ) as r:
            body = _json.loads(await r.text())
            # BingX: {"code": 0, "msg": "", "data": {...}}
            code = body.get("code", -1)
            if code != 0:
                raise RuntimeError(
                    f"BingX API error {code}: {body.get('msg', 'unknown')}"
                )
            return body.get("data", {})


class PairScanner:
    def __init__(
        self,
        api_key=None, api_secret=None, passphrase=None,
        min_volume_usdt=None,
        min_price_change_pct=None,
        top_n=None,
        refresh_interval_min=None,
        on_pairs_updated=None,
    ):
        self.min_volume_usdt      = min_volume_usdt      if min_volume_usdt      is not None else _MIN_VOLUME
        self.min_price_change_pct = min_price_change_pct if min_price_change_pct is not None else _MIN_CHANGE_PCT
        self.top_n                = top_n                if top_n                is not None else _TOP_N
        self.refresh_interval     = (refresh_interval_min if refresh_interval_min is not None else _REFRESH_MIN) * 60
        self.active_pairs: list   = []
        self._last_scored: list   = []
        self.on_pairs_updated     = on_pairs_updated

        extra = os.getenv("SYMBOL_BLACKLIST", "")
        self.blacklist = NON_CRYPTO_BASES | {
            s.strip().upper() for s in extra.split(",") if s.strip()
        }
        # Stub de compatibilidad (main.py puede acceder a self.exchange)
        self.exchange = _BingXExchangeStub()

        logger.info(
            "[PairScanner] Config: top_n=%d | refresh=%dmin | "
            "min_vol=$%s | min_change=%.1f%% | min_lev=%dx | funding_trend_n=%d | funding_batch=%d",
            self.top_n, self.refresh_interval // 60,
            f"{self.min_volume_usdt:,.0f}", self.min_price_change_pct,
            _MIN_LEV, _FUNDING_TREND_N, _FUNDING_ENRICH_BATCH,
        )

    def _is_valid(self, coin: str) -> bool:
        """Valida que el coin sea cripto (no acción/materia prima) y tenga longitud razonable."""
        # BingX símbolo: BTC-USDT → coin = BTC
        base = coin.replace("-USDT", "").upper()
        if base in self.blacklist:
            return False
        if len(base) < 2 or len(base) > 12:
            return False
        return True

    def inject_snapshot(self, raw_text: str) -> list[str]:
        from bot.market_snapshot import parse_snapshot, snapshot_to_scanner_format
        rows = parse_snapshot(raw_text)
        scored = snapshot_to_scanner_format(
            rows,
            min_volume_usdt=self.min_volume_usdt,
            min_change_pct=self.min_price_change_pct,
            top_n=self.top_n,
            exclude_quotes={"USDE", "USDH", "USDT"},
            exclude_collateral=set(),
        )
        for p in scored:
            raw_funding = p.get("funding", 0.0) / 100.0
            p["funding_trend"] = _funding_trend(p["symbol"], raw_funding)
        self._last_scored = scored
        self.active_pairs = [s["symbol"] for s in scored]
        logger.info(
            "[PairScanner] inject_snapshot: %d activos → top %d seleccionados",
            sum(1 for r in rows if r.active), len(scored),
        )
        return self.active_pairs

    async def _fetch_funding_rates(self, symbols: list[str]) -> dict[str, float]:
        """
        Obtiene funding rates reales de BingX para una lista de símbolos.
        Endpoint: GET /openApi/swap/v2/quote/premiumIndex?symbol=BTC-USDT
        Devuelve {symbol: lastFundingRate}.
        Limita a _FUNDING_ENRICH_BATCH pares para no sobrecargar la API.
        """
        result: dict[str, float] = {}
        batch = symbols[:_FUNDING_ENRICH_BATCH]

        async def _fetch_one(sym: str) -> tuple[str, float]:
            try:
                data = await _bingx_get(
                    "/openApi/swap/v2/quote/premiumIndex",
                    {"symbol": sym},
                )
                # BingX devuelve un objeto con campo lastFundingRate (string)
                rate = float(data.get("lastFundingRate", 0) or 0)
                return sym, rate
            except Exception as e:
                logger.debug("[PairScanner] funding %s error: %s", sym, e)
                return sym, 0.0

        results = await asyncio.gather(*[_fetch_one(s) for s in batch])
        for sym, rate in results:
            result[sym] = rate
        return result

    async def scan(self) -> list:
        """
        Obtiene la lista de contratos perpetuos USDT-margined de BingX
        y selecciona los mejores pares por score.

        Endpoints usados:
          1. GET /openApi/swap/v2/quote/contracts
             → contrato activo, maxLeverage
          2. GET /openApi/swap/v2/quote/ticker
             → volumen 24h (volume), precio (lastPrice / openPrice),
               open interest (tradeAmount)
          3. GET /openApi/swap/v2/quote/premiumIndex (por símbolo)
             → funding rate real (lastFundingRate)
        """
        try:
            contracts_raw, tickers_raw = await asyncio.gather(
                _bingx_get("/openApi/swap/v2/quote/contracts"),
                _bingx_get("/openApi/swap/v2/quote/ticker"),
            )
        except Exception as e:
            logger.error("[PairScanner] Error fetcheando datos BingX: %s", e)
            return []

        # BingX /contracts devuelve lista directamente o dentro de "contracts"
        if isinstance(contracts_raw, dict):
            contracts_list = contracts_raw.get("contracts", [])
        else:
            contracts_list = contracts_raw or []

        # BingX /ticker devuelve lista directamente o dentro de "tickers"
        if isinstance(tickers_raw, dict):
            tickers_list = tickers_raw.get("tickers", [])
        else:
            tickers_list = tickers_raw or []

        # ─ Mapa de contratos: symbol → {maxLeverage} ─
        # Campo: {"symbol": "BTC-USDT", "maxLeverage": 150, ...}
        contracts_map: dict[str, dict] = {}
        for c in contracts_list:
            sym = c.get("symbol", "")
            if not sym.endswith("-USDT"):
                continue
            try:
                max_lev = int(float(c.get("maxLeverage", 0) or 0))
            except (ValueError, TypeError):
                max_lev = 0
            contracts_map[sym] = {"max_lev": max_lev}

        # ─ Primera pasada: filtrar por volumen / cambio ─
        pre_scored = []
        total_seen = skipped_bl = skipped_lev = skipped_vol = skipped_chg = 0

        for t in tickers_list:
            # BingX ticker campos:
            #   symbol, lastPrice, openPrice, highPrice, lowPrice,
            #   volume (en moneda base), quoteVolume (en USDT),
            #   tradeAmount (OI en USDT), priceChangePercent
            sym = t.get("symbol", "")
            if not sym.endswith("-USDT"):
                continue

            if not self._is_valid(sym):
                skipped_bl += 1
                continue
            total_seen += 1

            inst_info = contracts_map.get(sym)
            max_lev   = inst_info["max_lev"] if inst_info else 0

            if max_lev < _MIN_LEV:
                skipped_lev += 1
                continue

            try:
                last_px    = float(t.get("lastPrice",  0) or 0)
                open_px    = float(t.get("openPrice",  0) or 0)
                # quoteVolume = volumen 24h en USDT (campo principal BingX)
                vol24h_usdt = float(t.get("quoteVolume", 0) or 0)
                # Fallback: volume (en base) × lastPrice
                if vol24h_usdt == 0:
                    vol_base    = float(t.get("volume", 0) or 0)
                    vol24h_usdt = vol_base * last_px
                # OI en USDT (tradeAmount)
                oi_usdt = float(t.get("tradeAmount", 0) or 0)
            except (ValueError, TypeError):
                continue

            if vol24h_usdt < self.min_volume_usdt or last_px <= 0:
                skipped_vol += 1
                continue

            if open_px > 0:
                change_pct = abs((last_px - open_px) / open_px * 100)
                if change_pct < self.min_price_change_pct:
                    skipped_chg += 1
                    continue
            else:
                change_pct = None

            pre_scored.append({
                "symbol":       sym,
                "vol24h_usdt":  vol24h_usdt,
                "change_pct":   change_pct,
                "last_price":   last_px,
                "oi_usdt":      oi_usdt,
                "max_leverage": max_lev,
                "funding":      0.0,   # se enriquece abajo
            })

        logger.debug(
            "[PairScanner] scan pre-filter: total=%d | bl=%d | lev=%d | vol=%d | chg=%d | passed=%d",
            total_seen, skipped_bl, skipped_lev, skipped_vol, skipped_chg, len(pre_scored),
        )

        # ─ Enriquecer con funding rates reales ─
        if pre_scored:
            syms_to_enrich = [p["symbol"] for p in pre_scored]
            funding_map = await self._fetch_funding_rates(syms_to_enrich)
            for p in pre_scored:
                p["funding"] = funding_map.get(p["symbol"], 0.0)

        # ─ Calcular score y construir lista final ─
        scored = []
        for p in pre_scored:
            funding      = p["funding"]
            change_pct   = p["change_pct"]
            vol24h_usdt  = p["vol24h_usdt"]
            oi_usdt      = p["oi_usdt"]

            score_change = change_pct if change_pct is not None else 0.0
            funding_abs  = abs(funding) * 10_000
            oi_m         = oi_usdt / 1_000_000
            vol_m        = vol24h_usdt / 1_000_000

            score = (
                vol_m        * 0.4 +
                score_change * 0.3 +
                funding_abs  * 0.2 +
                oi_m         * 0.1
            )

            trend = _funding_trend(p["symbol"], funding)

            scored.append({
                "symbol":        p["symbol"],
                "volume_usdt":   round(vol_m, 2),
                "change_pct":    round(change_pct, 2) if change_pct is not None else None,
                "last_price":    p["last_price"],
                "funding":       round(funding * 100, 5),
                "funding_trend": trend,
                "oi_usdt":       round(oi_m, 2),
                "score":         round(score, 3),
                "max_leverage":  p["max_leverage"],
                "ai_delta":      0.0,
            })

        # ─ Filtro IA ─
        if _AI_NEWS_FILTER and scored:
            try:
                from bot.ai_filter import news_score_adjustment

                async def _fetch_delta(pair: dict) -> float:
                    try:
                        return await news_score_adjustment(pair["symbol"])
                    except Exception:
                        return 0.0

                deltas = await asyncio.gather(*[_fetch_delta(p) for p in scored])
                for pair, delta in zip(scored, deltas):
                    if delta != 0.0:
                        pair["score"]    = round(pair["score"] + delta, 3)
                        pair["ai_delta"] = round(delta, 2)
                logger.info(
                    "[PairScanner] IA aplicada — %d pares con delta != 0",
                    sum(1 for d in deltas if d != 0.0),
                )
            except Exception as e:
                logger.warning("[PairScanner] Error filtro IA: %s", e)

        scored.sort(key=lambda x: x["score"], reverse=True)
        top = scored[:self.top_n]
        self._last_scored = top

        logger.info("\U0001f3c6 Top %d pares seleccionados (BingX):", len(top))
        for p in top[:10]:
            change_str = f"{p['change_pct']}%" if p["change_pct"] is not None else "N/A"
            ai_str     = f" | IA: {p['ai_delta']:+.1f}" if p.get("ai_delta") else ""
            logger.info(
                "  %-14s Vol:$%sM Cambio:%s OI:$%sM Fund:%.4f%%(%s) Lev:%dx Score:%.2f%s",
                p["symbol"], p["volume_usdt"], change_str,
                p["oi_usdt"], p["funding"], p.get("funding_trend", "?"),
                p.get("max_leverage", 0), p["score"], ai_str,
            )

        return [p["symbol"] for p in top]

    def normalize(self, symbol: str) -> str:
        """Normaliza un símbolo a formato BingX (BTC-USDT)."""
        s = symbol.replace("/", "-").replace(":USDT", "").upper()
        if not s.endswith("-USDT"):
            s = s.replace("USDT", "") + "-USDT"
        return s

    async def run(self) -> None:
        """Alias de run_scanner_loop() — usa self.on_pairs_updated como callback."""
        await self.run_scanner_loop()

    async def run_scanner_loop(self, on_update_callback=None):
        import inspect
        # Usar el callback del __init__ si no se pasa uno explícitamente
        callback = on_update_callback or self.on_pairs_updated
        if callback is None:
            logger.error("[PairScanner] run_scanner_loop: no se proporcionó callback — abortando loop")
            return

        cb_params = len(inspect.signature(callback).parameters)

        while True:
            try:
                logger.info(
                    "\U0001f50d Re-escaneando (top_n=%d | refresh=%dmin)...",
                    self.top_n, self.refresh_interval // 60,
                )
                new_pairs = await self.scan()
                if not new_pairs:
                    logger.warning("\u26a0\ufe0f Scanner devolvio 0 pares — manteniendo pares actuales")
                else:
                    added   = set(new_pairs) - set(self.active_pairs)
                    removed = set(self.active_pairs) - set(new_pairs)

                    try:
                        import main as _main
                        _main._update_leverage_map(self._last_scored)
                    except Exception:
                        pass

                    self.active_pairs = new_pairs

                    if added:
                        logger.info("\u2795 Nuevos pares (%d): %s", len(added), ", ".join(sorted(added)))
                    if removed:
                        logger.info("\u2796 Pares eliminados (%d): %s", len(removed), ", ".join(sorted(removed)))

                    if added or removed:
                        if cb_params >= 3:
                            await callback(new_pairs, added, removed)
                        else:
                            logger.warning(
                                "[PairScanner] Callback con firma antigua — "
                                "traders salientes no esperaran al ciclo siguiente"
                            )
                            await callback(new_pairs)
                    else:
                        logger.info("\u2705 Sin cambios en pares activos (%d pares)", len(self.active_pairs))

            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error("[PairScanner] Error en run_scanner_loop: %s", e, exc_info=True)

            await asyncio.sleep(self.refresh_interval)


class _BingXExchangeStub:
    """Stub de compatibilidad (main.py puede acceder a scanner.exchange)."""
    pass
