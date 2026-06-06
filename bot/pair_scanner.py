"""
pair_scanner.py — Escaner de pares para BingX perpetuos USDT-margined.

v4 — Fix bugs API real BingX (2026-06-06)
  Corrige 4 bugs descubiertos comparando el código contra las respuestas
  reales de la API BingX (verificadas con curl):

  BUG #1 — /contracts no tiene campo maxLeverage (campo inexistente).
    FIX: usar status==1 + apiStateOpen=='true' para filtrar contratos
    activos. El filtro SCANNER_EXCLUDE_LOW_LEV se reserva para cuando
    se añada autenticación (endpoint /trade/leverage requiere firma).

  BUG #2 — /ticker no tiene campo tradeAmount (OI no está en ticker).
    FIX: OI obtenido de GET /openApi/swap/v2/quote/openInterest
    (batch paralelo, igual que funding).

  BUG #3 — priceChangePercent ya viene calculado en el ticker.
    FIX: usar t['priceChangePercent'] directamente en lugar de
    recalcular (last-open)/open.

  BUG #4 — filtro de estado en contracts usaba lógica de OKX.
    FIX: BingX usa status==1 (int) y apiStateOpen=='true' (string).

  Endpoints usados (todos públicos, sin firma):
    GET /openApi/swap/v2/quote/contracts      → activos (status+apiState)
    GET /openApi/swap/v2/quote/ticker         → vol, precio, cambio%
    GET /openApi/swap/v2/quote/premiumIndex   → funding rate real
    GET /openApi/swap/v2/quote/openInterest   → OI en USDT por símbolo

  Formato símbolo BingX: "BTC-USDT".
  Score: vol 40% + cambio% 30% + funding_abs 20% + OI 10%
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

_TOP_N                = int(float(os.getenv("SCANNER_TOP_N",           "25")))
_REFRESH_MIN          = int(float(os.getenv("SCANNER_REFRESH_MIN",     "15")))
_MIN_VOLUME           = float(os.getenv("SCANNER_MIN_VOLUME",          "500000"))
_MIN_CHANGE_PCT       = float(os.getenv("SCANNER_MIN_CHANGE",          "0.3"))
_FUNDING_ENRICH_BATCH = int(os.getenv("FUNDING_ENRICH_BATCH",          "50"))
_OI_ENRICH_BATCH      = int(os.getenv("OI_ENRICH_BATCH",               "50"))

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
    GET a la API pública de BingX (sin firma).
    Devuelve body["data"] (lista o dict según el endpoint).
    Lanza RuntimeError si code != 0.
    """
    async with aiohttp.ClientSession() as s:
        async with s.get(
            f"{_BINGX_BASE}{path}",
            params=params,
            headers={"Content-Type": "application/json"},
            timeout=aiohttp.ClientTimeout(total=15),
        ) as r:
            body = _json.loads(await r.text())
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
        self.exchange = _BingXExchangeStub()

        logger.info(
            "[PairScanner] Config: top_n=%d | refresh=%dmin | "
            "min_vol=$%s | min_change=%.1f%% | funding_trend_n=%d | "
            "funding_batch=%d | oi_batch=%d",
            self.top_n, self.refresh_interval // 60,
            f"{self.min_volume_usdt:,.0f}", self.min_price_change_pct,
            _FUNDING_TREND_N, _FUNDING_ENRICH_BATCH, _OI_ENRICH_BATCH,
        )

    def _is_valid(self, sym: str) -> bool:
        """Valida símbolo BingX (BTC-USDT): base no en blacklist, longitud ok."""
        base = sym.replace("-USDT", "").upper()
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
        Funding rates reales vía GET /openApi/swap/v2/quote/premiumIndex.
        Respuesta: {symbol, markPrice, indexPrice, lastFundingRate, ...}
        Limita a _FUNDING_ENRICH_BATCH pares por ciclo.
        """
        result: dict[str, float] = {}
        batch = symbols[:_FUNDING_ENRICH_BATCH]

        async def _fetch_one(sym: str) -> tuple[str, float]:
            try:
                data = await _bingx_get(
                    "/openApi/swap/v2/quote/premiumIndex",
                    {"symbol": sym},
                )
                rate = float(data.get("lastFundingRate", 0) or 0)
                return sym, rate
            except Exception as e:
                logger.debug("[PairScanner] funding %s error: %s", sym, e)
                return sym, 0.0

        for sym, rate in await asyncio.gather(*[_fetch_one(s) for s in batch]):
            result[sym] = rate
        return result

    async def _fetch_open_interests(self, symbols: list[str]) -> dict[str, float]:
        """
        Open Interest en USDT vía GET /openApi/swap/v2/quote/openInterest.
        Respuesta: {openInterest (str), symbol, time}
        openInterest está en moneda base → se multiplica por lastPrice en scan().
        Aquí devolvemos el valor crudo (base units); scan() lo convierte.
        Limita a _OI_ENRICH_BATCH pares por ciclo.
        """
        result: dict[str, float] = {}
        batch = symbols[:_OI_ENRICH_BATCH]

        async def _fetch_one(sym: str) -> tuple[str, float]:
            try:
                data = await _bingx_get(
                    "/openApi/swap/v2/quote/openInterest",
                    {"symbol": sym},
                )
                oi = float(data.get("openInterest", 0) or 0)
                return sym, oi
            except Exception as e:
                logger.debug("[PairScanner] OI %s error: %s", sym, e)
                return sym, 0.0

        for sym, oi in await asyncio.gather(*[_fetch_one(s) for s in batch]):
            result[sym] = oi
        return result

    async def scan(self) -> list:
        """
        Selecciona los mejores pares USDT-margined de BingX por score.

        Paso 1 — Fetch paralelo:
          GET /openApi/swap/v2/quote/contracts  → set de símbolos activos
          GET /openApi/swap/v2/quote/ticker     → vol, precio, cambio%

        Paso 2 — Filtrar por volumen y cambio%.

        Paso 3 — Enriquecer en paralelo:
          GET /openApi/swap/v2/quote/premiumIndex  → funding real
          GET /openApi/swap/v2/quote/openInterest  → OI en base units

        Paso 4 — Calcular score y ordenar.
        """
        try:
            contracts_raw, tickers_raw = await asyncio.gather(
                _bingx_get("/openApi/swap/v2/quote/contracts"),
                _bingx_get("/openApi/swap/v2/quote/ticker"),
            )
        except Exception as e:
            logger.error("[PairScanner] Error fetcheando datos BingX: %s", e)
            return []

        # /contracts → lista de dicts
        # Respuesta real: [{symbol, status (int), apiStateOpen (str), ...}]
        contracts_list: list = contracts_raw if isinstance(contracts_raw, list) else []

        # Construir set de símbolos activos:
        #   status == 1 (int) AND apiStateOpen == 'true' (string)
        # BUG #1 FIX: NO existe campo maxLeverage en esta respuesta.
        active_symbols: set[str] = set()
        for c in contracts_list:
            sym    = c.get("symbol", "")
            status = c.get("status", 0)
            api_ok = c.get("apiStateOpen", "false")
            if (
                sym.endswith("-USDT")
                and status == 1
                and str(api_ok).lower() == "true"
            ):
                active_symbols.add(sym)

        logger.debug("[PairScanner] Contratos activos USDT: %d", len(active_symbols))

        # /ticker → lista de dicts
        # Respuesta real campos: symbol, priceChange, priceChangePercent,
        #   lastPrice, lastQty, highPrice, lowPrice, volume (base),
        #   quoteVolume (USDT), openPrice, openTime, closeTime,
        #   askPrice, askQty, bidPrice, bidQty
        # BUG #2 FIX: NO existe campo tradeAmount (OI no está en ticker).
        # BUG #3 FIX: priceChangePercent ya viene calculado.
        tickers_list: list = tickers_raw if isinstance(tickers_raw, list) else []

        # ─ Primera pasada: filtrar por volumen / cambio ─
        pre_scored = []
        total_seen = skipped_inactive = skipped_bl = skipped_vol = skipped_chg = 0

        for t in tickers_list:
            sym = t.get("symbol", "")
            if not sym.endswith("-USDT"):
                continue

            # Solo contratos marcados como activos en /contracts
            if sym not in active_symbols:
                skipped_inactive += 1
                continue

            if not self._is_valid(sym):
                skipped_bl += 1
                continue
            total_seen += 1

            try:
                last_px = float(t.get("lastPrice", 0) or 0)
                # quoteVolume = volumen 24h ya en USDT
                vol24h_usdt = float(t.get("quoteVolume", 0) or 0)
                if vol24h_usdt == 0:
                    # Fallback: volume (base) × lastPrice
                    vol_base    = float(t.get("volume", 0) or 0)
                    vol24h_usdt = vol_base * last_px
                # BUG #3 FIX: usar priceChangePercent directamente
                pcp_raw    = t.get("priceChangePercent")
                if pcp_raw is not None:
                    change_pct = abs(float(pcp_raw))
                else:
                    # Fallback manual si el campo falta
                    open_px = float(t.get("openPrice", 0) or 0)
                    change_pct = abs((last_px - open_px) / open_px * 100) if open_px > 0 else None
            except (ValueError, TypeError):
                continue

            if vol24h_usdt < self.min_volume_usdt or last_px <= 0:
                skipped_vol += 1
                continue

            if change_pct is not None and change_pct < self.min_price_change_pct:
                skipped_chg += 1
                continue

            pre_scored.append({
                "symbol":      sym,
                "vol24h_usdt": vol24h_usdt,
                "change_pct":  change_pct,
                "last_price":  last_px,
                "oi_base":     0.0,   # se enriquece en paso 3
                "funding":     0.0,   # se enriquece en paso 3
            })

        logger.debug(
            "[PairScanner] scan pre-filter: total=%d | inactive=%d | bl=%d | "
            "vol=%d | chg=%d | passed=%d",
            total_seen, skipped_inactive, skipped_bl, skipped_vol, skipped_chg,
            len(pre_scored),
        )

        if not pre_scored:
            logger.warning(
                "[PairScanner] 0 pares pasaron el pre-filtro "
                "(activos=%d, seen=%d, vol_skip=%d, chg_skip=%d). "
                "Revisa SCANNER_MIN_VOLUME (%.0f) y SCANNER_MIN_CHANGE (%.2f%%)",
                len(active_symbols), total_seen, skipped_vol, skipped_chg,
                self.min_volume_usdt, self.min_price_change_pct,
            )
            return []

        # ─ Paso 3: enriquecer funding + OI en paralelo ─
        syms = [p["symbol"] for p in pre_scored]
        funding_map, oi_map = await asyncio.gather(
            self._fetch_funding_rates(syms),
            self._fetch_open_interests(syms),
        )
        for p in pre_scored:
            p["funding"] = funding_map.get(p["symbol"], 0.0)
            p["oi_base"] = oi_map.get(p["symbol"], 0.0)

        # ─ Paso 4: calcular score ─
        scored = []
        for p in pre_scored:
            funding     = p["funding"]
            change_pct  = p["change_pct"] or 0.0
            vol24h_usdt = p["vol24h_usdt"]
            last_px     = p["last_price"]
            # OI: base units × lastPrice → USDT
            oi_usdt     = p["oi_base"] * last_px

            funding_abs = abs(funding) * 10_000
            oi_m        = oi_usdt / 1_000_000
            vol_m       = vol24h_usdt / 1_000_000

            score = (
                vol_m       * 0.4 +
                change_pct  * 0.3 +
                funding_abs * 0.2 +
                oi_m        * 0.1
            )

            trend = _funding_trend(p["symbol"], funding)

            scored.append({
                "symbol":        p["symbol"],
                "volume_usdt":   round(vol_m, 2),
                "change_pct":    round(change_pct, 2),
                "last_price":    last_px,
                "funding":       round(funding * 100, 5),
                "funding_trend": trend,
                "oi_usdt":       round(oi_m, 2),
                "score":         round(score, 3),
                "max_leverage":  0,   # no disponible sin auth
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

        logger.info("🏆 Top %d pares seleccionados (BingX):", len(top))
        for p in top[:10]:
            ai_str = f" | IA: {p['ai_delta']:+.1f}" if p.get("ai_delta") else ""
            logger.info(
                "  %-14s Vol:$%sM Cambio:%.2f%% OI:$%sM Fund:%.4f%%(%s) Score:%.2f%s",
                p["symbol"], p["volume_usdt"], p["change_pct"],
                p["oi_usdt"], p["funding"], p.get("funding_trend", "?"),
                p["score"], ai_str,
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
        callback = on_update_callback or self.on_pairs_updated
        if callback is None:
            logger.error("[PairScanner] run_scanner_loop: no se proporcionó callback — abortando loop")
            return

        cb_params = len(inspect.signature(callback).parameters)

        while True:
            try:
                logger.info(
                    "🔍 Re-escaneando (top_n=%d | refresh=%dmin)...",
                    self.top_n, self.refresh_interval // 60,
                )
                new_pairs = await self.scan()
                if not new_pairs:
                    logger.warning("⚠️ Scanner devolvio 0 pares — manteniendo pares actuales")
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
                        logger.info("➕ Nuevos pares (%d): %s", len(added), ", ".join(sorted(added)))
                    if removed:
                        logger.info("➖ Pares eliminados (%d): %s", len(removed), ", ".join(sorted(removed)))

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
                        logger.info("✅ Sin cambios en pares activos (%d pares)", len(self.active_pairs))

            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error("[PairScanner] Error en run_scanner_loop: %s", e, exc_info=True)

            await asyncio.sleep(self.refresh_interval)


class _BingXExchangeStub:
    """Stub de compatibilidad (main.py puede acceder a scanner.exchange)."""
    pass
