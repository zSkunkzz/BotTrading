"""
pair_scanner.py — Escaner de pares para BingX perpetuos USDT-margined.

v5 — Fix scores y filtros (2026-06-06)
  Corrige 3 bugs adicionales descubiertos en los logs de producción:

  BUG #5 — OI en USDT ya venía en USDT pero se multiplicaba por precio.
    /openInterest devuelve openInterest en USDT (no en base units).
    Inflaba BTC score a 7,292,149 en vez de ~1,107.
    FIX: NO multiplicar por lastPrice.

  BUG #6 — change_pct sin cap (BTW=456835% dominaba score).
    FIX: cap en SCANNER_MAX_CHANGE (default=50%).

  BUG #7 — BTW y pares sintéticos NCCO*/NCSK* sin OHLCV.
    BingX los lista como activos pero no tienen velas ni precio real.
    FIX: filtrar por prefijos sintéticos conocidos.

v4 — Fix bugs API real BingX (2026-06-06)
  BUG #1 — /contracts no tiene maxLeverage → usar status+apiStateOpen.
  BUG #2 — OI no está en /ticker → GET /quote/openInterest por símbolo.
  BUG #3 — priceChangePercent ya viene en ticker → no recalcular.
  BUG #4 — filtro estado usaba lógica OKX → BingX: status==1 + apiStateOpen.
"""
import logging
import asyncio
import os
import re
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

# Prefijos de productos sintéticos de BingX (acciones/materias primas trackeadas)
# Sin datos OHLCV reales, set_leverage falla con 'price not exist'.
# BUG #7 FIX: filtrar antes de intentar tradear.
_SYNTHETIC_PREFIXES = ("NCCO", "NCSK", "BTW", "NCS", "NCX")

# BingX REST pública (no requiere API key ni firma)
_BINGX_BASE = "https://open-api.bingx.com"

_TRADER_STOP_TIMEOUT_S = float(os.getenv("TRADER_STOP_TIMEOUT_S", "15"))
_AI_NEWS_FILTER: bool  = os.getenv("AI_NEWS_FILTER", "false").lower() in ("true", "1", "yes")

_TOP_N                = int(float(os.getenv("SCANNER_TOP_N",           "25")))
_REFRESH_MIN          = int(float(os.getenv("SCANNER_REFRESH_MIN",     "15")))
_MIN_VOLUME           = float(os.getenv("SCANNER_MIN_VOLUME",          "500000"))
_MIN_CHANGE_PCT       = float(os.getenv("SCANNER_MIN_CHANGE",          "0.3"))
# BUG #6 FIX: cap para evitar que monedas recién listadas con cambio
# astronómico (BTW=456835%) dominen el score.
_MAX_CHANGE_PCT       = float(os.getenv("SCANNER_MAX_CHANGE",          "50.0"))
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
            "min_vol=$%s | min_change=%.1f%% | max_change=%.0f%% | "
            "funding_trend_n=%d | funding_batch=%d | oi_batch=%d",
            self.top_n, self.refresh_interval // 60,
            f"{self.min_volume_usdt:,.0f}", self.min_price_change_pct,
            _MAX_CHANGE_PCT, _FUNDING_TREND_N, _FUNDING_ENRICH_BATCH, _OI_ENRICH_BATCH,
        )

    def _is_valid(self, sym: str) -> bool:
        """
        Valida símbolo BingX (BTC-USDT):
          - base no en blacklist de non-crypto
          - no empieza por prefijo sintético (NCCO*, NCSK*, BTW*...)
          - longitud razonable
        """
        base = sym.replace("-USDT", "").upper()
        # BUG #7 FIX: productos sintéticos de BingX sin OHLCV real
        if base.startswith(_SYNTHETIC_PREFIXES):
            return False
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
        Open Interest vía GET /openApi/swap/v2/quote/openInterest.
        Respuesta: {openInterest (str en USDT), symbol, time}
        BUG #5 FIX: openInterest YA está en USDT — NO multiplicar por precio.
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
                # openInterest está en USDT directamente
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

        Paso 2 — Filtrar: activo + no-sintético + volumen + cambio%.

        Paso 3 — Enriquecer en paralelo:
          GET /openApi/swap/v2/quote/premiumIndex  → funding real
          GET /openApi/swap/v2/quote/openInterest  → OI en USDT

        Paso 4 — Score: vol×0.4 + change_capped×0.3 + funding×0.2 + OI×0.1
        """
        try:
            contracts_raw, tickers_raw = await asyncio.gather(
                _bingx_get("/openApi/swap/v2/quote/contracts"),
                _bingx_get("/openApi/swap/v2/quote/ticker"),
            )
        except Exception as e:
            logger.error("[PairScanner] Error fetcheando datos BingX: %s", e)
            return []

        contracts_list: list = contracts_raw if isinstance(contracts_raw, list) else []

        # Contratos activos: status==1 AND apiStateOpen=='true'
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

        tickers_list: list = tickers_raw if isinstance(tickers_raw, list) else []

        # ─ Primera pasada: filtrar ─
        pre_scored = []
        total_seen = skipped_inactive = skipped_bl = skipped_synth = 0
        skipped_vol = skipped_chg = 0

        for t in tickers_list:
            sym = t.get("symbol", "")
            if not sym.endswith("-USDT"):
                continue

            if sym not in active_symbols:
                skipped_inactive += 1
                continue

            base = sym.replace("-USDT", "").upper()
            # BUG #7 FIX: excluir sintéticos antes de _is_valid
            if base.startswith(_SYNTHETIC_PREFIXES):
                skipped_synth += 1
                continue

            if not self._is_valid(sym):
                skipped_bl += 1
                continue
            total_seen += 1

            try:
                last_px     = float(t.get("lastPrice", 0) or 0)
                vol24h_usdt = float(t.get("quoteVolume", 0) or 0)
                if vol24h_usdt == 0:
                    vol_base    = float(t.get("volume", 0) or 0)
                    vol24h_usdt = vol_base * last_px
                pcp_raw = t.get("priceChangePercent")
                if pcp_raw is not None:
                    change_pct = abs(float(pcp_raw))
                else:
                    open_px    = float(t.get("openPrice", 0) or 0)
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
                "oi_usdt":     0.0,   # se enriquece en paso 3
                "funding":     0.0,   # se enriquece en paso 3
            })

        logger.debug(
            "[PairScanner] scan pre-filter: total=%d | inactive=%d | synth=%d | "
            "bl=%d | vol=%d | chg=%d | passed=%d",
            total_seen, skipped_inactive, skipped_synth, skipped_bl,
            skipped_vol, skipped_chg, len(pre_scored),
        )

        if not pre_scored:
            logger.warning(
                "[PairScanner] 0 pares pasaron el pre-filtro "
                "(activos=%d, seen=%d, synth=%d, vol_skip=%d, chg_skip=%d). "
                "Revisa SCANNER_MIN_VOLUME (%.0f) y SCANNER_MIN_CHANGE (%.2f%%)",
                len(active_symbols), total_seen, skipped_synth,
                skipped_vol, skipped_chg,
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
            p["funding"]  = funding_map.get(p["symbol"], 0.0)
            p["oi_usdt"]  = oi_map.get(p["symbol"], 0.0)  # ya en USDT

        # ─ Paso 4: calcular score ─
        scored = []
        for p in pre_scored:
            funding     = p["funding"]
            change_pct  = p["change_pct"] or 0.0
            vol24h_usdt = p["vol24h_usdt"]
            # BUG #5 FIX: oi_usdt ya está en USDT, NO multiplicar por precio
            oi_usdt     = p["oi_usdt"]
            # BUG #6 FIX: cap de cambio para evitar que monedas recién
            # listadas con ratio absurdo dominen el score
            change_capped = min(change_pct, _MAX_CHANGE_PCT)

            funding_abs = abs(funding) * 10_000
            oi_m        = oi_usdt / 1_000_000
            vol_m       = vol24h_usdt / 1_000_000

            score = (
                vol_m         * 0.4 +
                change_capped * 0.3 +
                funding_abs   * 0.2 +
                oi_m          * 0.1
            )

            trend = _funding_trend(p["symbol"], funding)

            scored.append({
                "symbol":        p["symbol"],
                "volume_usdt":   round(vol_m, 2),
                "change_pct":    round(change_pct, 2),
                "change_capped": round(change_capped, 2),
                "last_price":    p["last_price"],
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
                "  %-14s Vol:$%sM Cambio:%.2f%%(cap:%.0f%%) OI:$%sM Fund:%.4f%%(%s) Score:%.2f%s",
                p["symbol"], p["volume_usdt"], p["change_pct"],
                p["change_capped"], p["oi_usdt"], p["funding"],
                p.get("funding_trend", "?"), p["score"], ai_str,
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
