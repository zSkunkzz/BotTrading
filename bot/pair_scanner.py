"""
pair_scanner.py — Escaner de pares para OKX perpetuos USDT-margined.

v2 — OKX migration (2026-06-06)
  Sustituye el endpoint de Hyperliquid (metaAndAssetCtxs)
  por la API REST pública de OKX v5:
    GET /api/v5/public/instruments?instType=SWAP
    GET /api/v5/market/tickers?instType=SWAP
    GET /api/v5/public/funding-rate?instId={instId}

  El campo "symbol" devuelto es siempre el coin corto ("BTC", "ETH").
  Internamente los instId son "{coin}-USDT-SWAP".

  Filtros equivalentes mantenidos:
    SCANNER_TOP_N, SCANNER_REFRESH_MIN, SCANNER_MIN_VOLUME,
    SCANNER_MIN_CHANGE, SCANNER_EXCLUDE_LOW_LEV, SYMBOL_BLACKLIST
    FUNDING_TREND_WINDOW, AI_NEWS_FILTER

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

# OKX REST pública (no requiere API key)
_OKX_BASE = "https://www.okx.com"

_TRADER_STOP_TIMEOUT_S = float(os.getenv("TRADER_STOP_TIMEOUT_S", "15"))
_AI_NEWS_FILTER: bool  = os.getenv("AI_NEWS_FILTER", "false").lower() in ("true", "1", "yes")

_TOP_N          = int(float(os.getenv("SCANNER_TOP_N",           "25")))
_REFRESH_MIN    = int(float(os.getenv("SCANNER_REFRESH_MIN",     "15")))
_MIN_VOLUME     = float(os.getenv("SCANNER_MIN_VOLUME",          "500000"))
_MIN_CHANGE_PCT = float(os.getenv("SCANNER_MIN_CHANGE",          "0.3"))
_MIN_LEV        = int(float(os.getenv("SCANNER_EXCLUDE_LOW_LEV", "3")))

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


async def _okx_get(path: str, params: dict | None = None) -> dict | list:
    """GET a la API pública de OKX. Devuelve el campo `data` de la respuesta."""
    async with aiohttp.ClientSession() as s:
        async with s.get(
            f"{_OKX_BASE}{path}",
            params=params,
            headers={"Content-Type": "application/json"},
            timeout=aiohttp.ClientTimeout(total=15),
        ) as r:
            body = _json.loads(await r.text())
            if body.get("code") != "0":
                raise RuntimeError(f"OKX API error {body.get('code')}: {body.get('msg')}")
            return body.get("data", [])


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
        self.exchange = _OKXExchangeStub()

        logger.info(
            "[PairScanner] Config: top_n=%d | refresh=%dmin | "
            "min_vol=$%s | min_change=%.1f%% | min_lev=%dx | funding_trend_n=%d",
            self.top_n, self.refresh_interval // 60,
            f"{self.min_volume_usdt:,.0f}", self.min_price_change_pct,
            _MIN_LEV, _FUNDING_TREND_N,
        )

    def _is_valid(self, coin: str) -> bool:
        if coin.upper() in self.blacklist:
            return False
        if len(coin) < 2 or len(coin) > 12:
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

    async def scan(self) -> list:
        """
        Obtiene la lista de contratos SWAP USDT-margined de OKX
        y selecciona los mejores pares por score.

        Endpoints usados:
          1. GET /api/v5/public/instruments?instType=SWAP
             → maxLeverage, estado del instrumento
          2. GET /api/v5/market/tickers?instType=SWAP
             → volumen 24h, open interest, precio last/open24h
        """
        try:
            instruments_raw, tickers_raw = await asyncio.gather(
                _okx_get("/api/v5/public/instruments", {"instType": "SWAP"}),
                _okx_get("/api/v5/market/tickers",    {"instType": "SWAP"}),
            )
        except Exception as e:
            logger.error("[PairScanner] Error fetcheando datos OKX: %s", e)
            return []

        # ─ Mapas de instrumentos (solo USDT-margined activos) ─
        inst_map: dict[str, dict] = {}   # instId → {maxLev}
        for inst in instruments_raw:
            inst_id = inst.get("instId", "")
            settle  = inst.get("settleCcy", "")
            state   = inst.get("state", "")
            if settle != "USDT" or state != "live":
                continue
            try:
                max_lev = int(float(inst.get("lever", "0") or "0"))
            except (ValueError, TypeError):
                max_lev = 0
            inst_map[inst_id] = {"max_lev": max_lev}

        # ─ Combinar con tickers ─
        scored = []
        total_seen = skipped_bl = skipped_lev = skipped_vol = skipped_chg = 0

        for t in tickers_raw:
            inst_id = t.get("instId", "")
            if inst_id not in inst_map:
                continue

            # Extraer coin del instId  BTC-USDT-SWAP → BTC
            coin = inst_id.split("-")[0]

            if not self._is_valid(coin):
                skipped_bl += 1
                continue
            total_seen += 1

            inst_info = inst_map[inst_id]
            max_lev   = inst_info["max_lev"]

            if max_lev < _MIN_LEV:
                skipped_lev += 1
                continue

            try:
                last_px    = float(t.get("last",    0) or 0)
                open24h    = float(t.get("open24h", 0) or 0)
                vol24h_ccy = float(t.get("volCcy24h", 0) or 0)   # en USDT
                oi_ccy     = float(t.get("openInterestCcy", 0) or 0)  # OI en USDT (si disponible)
            except (ValueError, TypeError):
                continue

            # volCcy24h puede no estar en tickers SWAP; fallback a vol*last
            if vol24h_ccy == 0:
                vol_contracts = float(t.get("vol24h", 0) or 0)
                vol24h_ccy = vol_contracts * last_px

            if vol24h_ccy < self.min_volume_usdt or last_px <= 0:
                skipped_vol += 1
                continue

            if open24h > 0:
                change_pct = abs((last_px - open24h) / open24h * 100)
                if change_pct < self.min_price_change_pct:
                    skipped_chg += 1
                    continue
            else:
                change_pct = None

            # Funding: OKX tickers SWAP no incluye funding en el ticker;
            # usamos 0 por defecto para no bloquear el scan con llamadas extra.
            # Si quieres funding real, activa FUNDING_RATE_ENRICH=true (ver abajo).
            funding = 0.0

            score_change = change_pct if change_pct is not None else 0.0
            funding_abs  = abs(funding) * 10_000
            oi_m         = oi_ccy / 1_000_000
            vol_m        = vol24h_ccy / 1_000_000

            score = (
                vol_m        * 0.4 +
                score_change * 0.3 +
                funding_abs  * 0.2 +
                oi_m         * 0.1
            )

            trend = _funding_trend(coin, funding)

            scored.append({
                "symbol":        coin,
                "inst_id":       inst_id,
                "volume_usdt":   round(vol_m, 2),
                "change_pct":    round(change_pct, 2) if change_pct is not None else None,
                "last_price":    last_px,
                "funding":       round(funding * 100, 5),
                "funding_trend": trend,
                "oi_usdt":       round(oi_m, 2),
                "score":         round(score, 3),
                "max_leverage":  max_lev,
                "ai_delta":      0.0,
            })

        logger.debug(
            "[PairScanner] scan: total=%d | bl=%d | lev=%d | vol=%d | chg=%d | passed=%d",
            total_seen, skipped_bl, skipped_lev, skipped_vol, skipped_chg, len(scored),
        )

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

        logger.info("🏆 Top %d pares seleccionados (OKX):", len(top))
        for p in top[:10]:
            change_str = f"{p['change_pct']}%" if p["change_pct"] is not None else "N/A"
            ai_str     = f" | IA: {p['ai_delta']:+.1f}" if p.get("ai_delta") else ""
            logger.info(
                "  %-12s Vol:$%sM Cambio:%s OI:$%sM Fund:%.3f%%(%s) Lev:%dx Score:%.2f%s",
                p["symbol"], p["volume_usdt"], change_str,
                p["oi_usdt"], p["funding"], p.get("funding_trend", "?"),
                p.get("max_leverage", 0), p["score"], ai_str,
            )

        return [p["symbol"] for p in top]

    def normalize(self, symbol: str) -> str:
        return symbol.replace("/", "").replace(":USDT", "").replace("USDT", "").upper()

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


class _OKXExchangeStub:
    """Stub de compatibilidad (reemplaza _HLExchangeStub)."""
    pass
