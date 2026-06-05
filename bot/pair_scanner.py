"""
pair_scanner.py — Escaner de pares para Hyperliquid perpetuos.

BUG #4 FIX: rotacion de par sin esperar cleanup del trader
  run_scanner_loop ahora llama on_update_callback con (new_pairs, added, removed)
  para que main.py pueda hacer cleanup SELECTIVO de los traders salientes
  antes de arrancar los nuevos.

FIX #2 (2026-06-02): primer re-scan inmediato al arrancar.

IA (2026-06-02): integración de news_score_adjustment()

FIX ROTÓ (2026-06-03): Rotación más agresiva para buscar más trades.
  - SCANNER_TOP_N (default 25, antes 15)
  - SCANNER_REFRESH_MIN (default 15, antes 30)
  - SCANNER_MIN_VOLUME (default 500_000 USDT, antes 1_000_000)
  - SCANNER_MIN_CHANGE (default 0.3%, antes 0.5%)
  - Score reformulado: vol 40% + cambio% 30% + funding_abs 20% + OI 10%
  - SCANNER_EXCLUDE_LOW_LEV: omite pares con maxLev < umbral (default 3)

Prioridad 5 (v21): Funding Rate Trend
  - _funding_history almacena los últimos N valores de funding por símbolo
  - _funding_trend() devuelve RISING / FALLING / NEUTRAL
  - El campo "funding_trend" se añade a cada par en el resultado de scan()

FIX OHLCV (2026-06-05): filtro de liquidez OHLCV antes de activar coins
  - _check_ohlcv_available(): petición rápida de 20 velas para verificar
    que HL tiene datos OHLCV para el coin en el timeframe de referencia.
  - scan(): tras calcular el top, descarta coins sin datos OHLCV en
    paralelo (respetando OHLCV_MAX_CONCURRENCY).
  - SCANNER_OHLCV_VERIFY=false para deshabilitar (debug).
  - SCANNER_OHLCV_VERIFY_TF=15m para elegir el timeframe de verificación.
  - Resultado: ningún coin sin velas llega al bot.

FIX LEVERAGE MAP (2026-06-05): eliminar import circular `main` en run_scanner_loop
  - Se pasaba _last_scored a main._update_leverage_map() via `import main`
    que fallaba silenciosamente en Railway (circular import).
  - Ahora run_scanner_loop pasa scored_data=self._last_scored al callback,
    y on_pairs_updated() llama _update_leverage_map() antes de arrancar
    los nuevos traders. Esto corrige el error 'Invalid leverage value' en
    coins con maxLeverage < LEVERAGE_BASE durante rotaciones.
"""
import logging
import asyncio
import os
import time
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

_USE_TESTNET = os.getenv("HL_TESTNET", "").lower() in ("true", "1", "yes")
_API_URL     = "https://api.hyperliquid-testnet.xyz" if _USE_TESTNET else "https://api.hyperliquid.xyz"

_TRADER_STOP_TIMEOUT_S = float(os.getenv("TRADER_STOP_TIMEOUT_S", "15"))
_AI_NEWS_FILTER: bool  = os.getenv("AI_NEWS_FILTER", "false").lower() in ("true", "1", "yes")

# FIX ROTÓ: parámetros configurables vía Railway
_TOP_N          = int(float(os.getenv("SCANNER_TOP_N",           "25")))
_REFRESH_MIN    = int(float(os.getenv("SCANNER_REFRESH_MIN",     "15")))
_MIN_VOLUME     = float(os.getenv("SCANNER_MIN_VOLUME",          "500000"))
_MIN_CHANGE_PCT = float(os.getenv("SCANNER_MIN_CHANGE",          "0.3"))
_MIN_LEV        = int(float(os.getenv("SCANNER_EXCLUDE_LOW_LEV", "3")))

# FIX OHLCV: verificación de liquidez OHLCV antes de activar coins
_OHLCV_VERIFY         = os.getenv("SCANNER_OHLCV_VERIFY", "true").lower() not in ("false", "0", "no")
_OHLCV_VERIFY_TF      = os.getenv("SCANNER_OHLCV_VERIFY_TF", "15m")
_OHLCV_MAX_CONCURRENT = int(os.getenv("OHLCV_MAX_CONCURRENCY", "5"))

# Cache de coins conocidos como sin datos OHLCV para evitar re-verificar siempre
# Se resetea cada _OHLCV_NO_DATA_RESET_S segundos
_OHLCV_NO_DATA_CACHE: set[str] = set()
_OHLCV_NO_DATA_RESET_S = float(os.getenv("OHLCV_NO_DATA_RESET_INTERVAL", "1800"))
_OHLCV_NO_DATA_LAST_RESET: float = 0.0

_TF_MINUTES = {
    "1m": 1, "3m": 3, "5m": 5, "15m": 15, "30m": 30,
    "1h": 60, "2h": 120, "4h": 240, "8h": 480, "1d": 1440,
}

# ── Prioridad 5: Funding trend ─────────────────────────────────────────────
FUNDING_TREND_N: int = int(os.getenv("FUNDING_TREND_WINDOW", "3"))
_funding_history: dict[str, list[float]] = defaultdict(list)


def _funding_trend(symbol: str, current: float) -> str:
    """Actualiza el historial de funding del símbolo y devuelve la tendencia.

    Returns:
        "RISING"  — el funding ha subido entre el scan más antiguo y el actual
        "FALLING" — el funding ha bajado
        "NEUTRAL" — sin suficientes datos o sin cambio
    """
    hist = _funding_history[symbol]
    hist.append(current)
    if len(hist) > FUNDING_TREND_N:
        hist.pop(0)
    if len(hist) < 2:
        return "NEUTRAL"
    return "RISING" if hist[-1] > hist[0] else "FALLING"
# ──────────────────────────────────────────────────────────────────────────────


async def _info_post(payload: dict) -> dict:
    async with aiohttp.ClientSession() as s:
        async with s.post(
            f"{_API_URL}/info",
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=aiohttp.ClientTimeout(total=15),
        ) as r:
            return _json.loads(await r.text())


async def _check_ohlcv_available(
    session: aiohttp.ClientSession,
    coin: str,
    timeframe: str = "15m",
    n_bars: int = 20,
) -> bool:
    """
    Verifica rápidamente si HL tiene datos OHLCV para el coin.
    Hace una sola petición candleSnapshot con 20 barras.
    Devuelve True si hay al menos 1 vela, False en caso contrario.
    """
    interval_min = _TF_MINUTES.get(timeframe, 15)
    end_ms   = int(time.time() * 1000)
    start_ms = end_ms - (n_bars * interval_min * 60 * 1000)

    try:
        async with session.post(
            f"{_API_URL}/info",
            json={
                "type": "candleSnapshot",
                "req": {
                    "coin":      coin,
                    "interval":  timeframe,
                    "startTime": start_ms,
                    "endTime":   end_ms,
                },
            },
            headers={"Content-Type": "application/json"},
            timeout=aiohttp.ClientTimeout(total=10),
        ) as resp:
            raw = _json.loads(await resp.text())
            return isinstance(raw, list) and len(raw) > 0
    except Exception:
        return False


async def _verify_ohlcv_batch(coins: list[str], timeframe: str) -> tuple[list[str], list[str]]:
    """
    Verifica en paralelo qué coins tienen datos OHLCV disponibles.
    Respeta OHLCV_MAX_CONCURRENCY para no saturar la API.

    Returns:
        (valid_coins, invalid_coins)
    """
    global _OHLCV_NO_DATA_CACHE, _OHLCV_NO_DATA_LAST_RESET

    # Reset periódico del cache de coins sin datos
    now = time.monotonic()
    if now - _OHLCV_NO_DATA_LAST_RESET > _OHLCV_NO_DATA_RESET_S:
        if _OHLCV_NO_DATA_CACHE:
            logger.info(
                "[PairScanner] Reset cache OHLCV sin datos (interval=%.0fs): %s",
                _OHLCV_NO_DATA_RESET_S,
                ", ".join(sorted(_OHLCV_NO_DATA_CACHE)),
            )
        _OHLCV_NO_DATA_CACHE = set()
        _OHLCV_NO_DATA_LAST_RESET = now

    # Separar coins ya conocidas como inválidas (no re-verificar)
    to_verify = [c for c in coins if c not in _OHLCV_NO_DATA_CACHE]
    already_invalid = [c for c in coins if c in _OHLCV_NO_DATA_CACHE]

    if not to_verify:
        return [], already_invalid

    sem = asyncio.Semaphore(_OHLCV_MAX_CONCURRENT)
    valid: list[str]   = []
    invalid: list[str] = list(already_invalid)

    async def _check_one(coin: str) -> tuple[str, bool]:
        async with sem:
            async with aiohttp.ClientSession() as session:
                ok = await _check_ohlcv_available(session, coin, timeframe)
                return coin, ok

    results = await asyncio.gather(*[_check_one(c) for c in to_verify])

    for coin, ok in results:
        if ok:
            valid.append(coin)
        else:
            invalid.append(coin)
            _OHLCV_NO_DATA_CACHE.add(coin)

    return valid, invalid


class PairScanner:
    def __init__(
        self,
        api_key=None, api_secret=None, passphrase=None,
        min_volume_usdt=None,
        min_price_change_pct=None,
        top_n=None,
        refresh_interval_min=None,
    ):
        # Prioridad: arg explícito → env var → default
        self.min_volume_usdt      = min_volume_usdt      if min_volume_usdt      is not None else _MIN_VOLUME
        self.min_price_change_pct = min_price_change_pct if min_price_change_pct is not None else _MIN_CHANGE_PCT
        self.top_n                = top_n                if top_n                is not None else _TOP_N
        self.refresh_interval     = (refresh_interval_min if refresh_interval_min is not None else _REFRESH_MIN) * 60
        self.active_pairs: list   = []
        self._last_scored: list   = []

        extra = os.getenv("SYMBOL_BLACKLIST", "")
        self.blacklist = NON_CRYPTO_BASES | {
            s.strip().upper() for s in extra.split(",") if s.strip()
        }

        self.exchange = _HLExchangeStub()

        logger.info(
            "[PairScanner] Config: top_n=%d | refresh=%dmin | "
            "min_vol=$%s | min_change=%.1f%% | min_lev=%dx | funding_trend_n=%d | "
            "ohlcv_verify=%s (tf=%s)",
            self.top_n, self.refresh_interval // 60,
            f"{self.min_volume_usdt:,.0f}", self.min_price_change_pct,
            _MIN_LEV, FUNDING_TREND_N,
            _OHLCV_VERIFY, _OHLCV_VERIFY_TF,
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
        # Añadir funding_trend a los resultados del snapshot
        for p in scored:
            raw_funding = p.get("funding", 0.0) / 100.0  # snapshot ya viene en %
            p["funding_trend"] = _funding_trend(p["symbol"], raw_funding)
        self._last_scored = scored
        self.active_pairs = [s["symbol"] for s in scored]
        logger.info(
            "[PairScanner] inject_snapshot: %d activos → top %d seleccionados",
            sum(1 for r in rows if r.active), len(scored),
        )
        for p in scored[:10]:
            logger.info(
                "  %-12s Vol: $%sM | Cambio: %.2f%% | Funding: %.4f%% (%s) | MaxLev: %dx | Score: %s",
                p["symbol"], p["volume_usdt"], p["change_pct"], p["funding"],
                p.get("funding_trend", "?"),
                p.get("max_leverage", 0), p["score"],
            )
        return self.active_pairs

    async def scan(self) -> list:
        try:
            data = await _info_post({"type": "metaAndAssetCtxs"})
        except Exception as e:
            logger.error("[PairScanner] Error fetching metaAndAssetCtxs: %s", e)
            return []

        universe = data[0].get("universe", []) if isinstance(data, list) and data else []
        ctxs     = data[1] if isinstance(data, list) and len(data) > 1 else []

        total_seen        = 0
        skipped_blacklist = 0
        skipped_lev       = 0
        skipped_volume    = 0
        skipped_change    = 0

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
                max_lev       = int(meta.get("maxLeverage", 0) or 0)
            except (ValueError, TypeError):
                continue

            if max_lev < _MIN_LEV:
                skipped_lev += 1
                continue

            if day_volume < self.min_volume_usdt or mark_px <= 0:
                skipped_volume += 1
                continue

            if prev_day_px > 0:
                change_pct: float | None = abs((mark_px - prev_day_px) / prev_day_px * 100)
                if change_pct < self.min_price_change_pct:
                    skipped_change += 1
                    continue
            else:
                change_pct = None

            # Score diversificado: vol + momentum + funding_extremo + OI
            score_change = change_pct if change_pct is not None else 0.0
            funding_abs  = abs(funding) * 10_000          # bps
            oi_usd       = open_interest * mark_px / 1_000_000
            vol_m        = day_volume / 1_000_000

            score = (
                vol_m        * 0.4 +
                score_change * 0.3 +
                funding_abs  * 0.2 +
                oi_usd       * 0.1
            )

            # Calcular tendencia de funding (actualiza historial en memoria)
            trend = _funding_trend(coin, funding)

            scored.append({
                "symbol":        coin,
                "volume_usdt":   round(vol_m, 2),
                "change_pct":    round(change_pct, 2) if change_pct is not None else None,
                "last_price":    mark_px,
                "funding":       round(funding * 100, 5),
                "funding_trend": trend,
                "oi_usdt":       round(oi_usd, 2),
                "score":         round(score, 3),
                "max_leverage":  max_lev,
                "ai_delta":      0.0,
            })

        logger.debug(
            "[PairScanner] scan: total=%d | blacklist=%d | lev=%d | vol=%d | change=%d | passed=%d",
            total_seen, skipped_blacklist, skipped_lev, skipped_volume, skipped_change, len(scored),
        )

        # ── Filtro IA de noticias ────────────────────────────────────────────────
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
                logger.warning("[PairScanner] Error filtro IA — scores sin modificar: %s", e)
        # ─────────────────────────────────────────────────

        scored.sort(key=lambda x: x["score"], reverse=True)
        top = scored[:self.top_n]

        # ── FIX OHLCV: verificar liquidez de velas antes de activar ───────────
        if _OHLCV_VERIFY and top:
            top_coins = [p["symbol"] for p in top]
            valid_coins, invalid_coins = await _verify_ohlcv_batch(top_coins, _OHLCV_VERIFY_TF)

            if invalid_coins:
                logger.warning(
                    "[PairScanner] ⚠️ %d coin(s) sin datos OHLCV en HL — excluidos: %s",
                    len(invalid_coins), ", ".join(sorted(invalid_coins)),
                )
                # Filtrar top para excluir coins sin datos
                valid_set = set(valid_coins)
                top = [p for p in top if p["symbol"] in valid_set]

                # Rellenar con los siguientes del ranking si hay hueco
                remaining = [p for p in scored[self.top_n:] if p["symbol"] not in valid_set]
                if remaining:
                    extra_coins = [p["symbol"] for p in remaining]
                    extra_valid, extra_invalid = await _verify_ohlcv_batch(extra_coins, _OHLCV_VERIFY_TF)
                    if extra_invalid:
                        logger.debug(
                            "[PairScanner] Candidatos de relleno también sin OHLCV: %s",
                            ", ".join(sorted(extra_invalid)),
                        )
                    extra_valid_set = set(extra_valid)
                    for p in remaining:
                        if len(top) >= self.top_n:
                            break
                        if p["symbol"] in extra_valid_set:
                            top.append(p)

            logger.info(
                "[PairScanner] Verificación OHLCV: %d/%d coins con datos (tf=%s)",
                len(top), len(top_coins), _OHLCV_VERIFY_TF,
            )
        # ───────────────────────────────────────────────────────────

        self._last_scored = top

        logger.info("🏆 Top %d pares seleccionados:", len(top))
        for p in top[:10]:
            change_str = f"{p['change_pct']}%" if p["change_pct"] is not None else "N/A"
            ai_str     = f" | IA: {p['ai_delta']:+.1f}" if p.get("ai_delta") else ""
            trend_str  = p.get("funding_trend", "?")
            logger.info(
                "  %-12s Vol:$%sM Cambio:%s OI:$%sM Fund:%.3f%%(%s) Lev:%dx Score:%.2f%s",
                p["symbol"], p["volume_usdt"], change_str,
                p["oi_usdt"], p["funding"], trend_str,
                p.get("max_leverage", 0), p["score"], ai_str,
            )

        return [p["symbol"] for p in top]

    def normalize(self, symbol: str) -> str:
        return symbol.replace("/", "").replace(":USDT", "").replace("USDT", "").upper()

    async def run_scanner_loop(self, on_update_callback):
        import inspect
        cb_sig    = inspect.signature(on_update_callback)
        cb_params = list(cb_sig.parameters.keys())
        has_scored_data = "scored_data" in cb_params

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

                    self.active_pairs = new_pairs

                    if added:
                        logger.info("➕ Nuevos pares (%d): %s", len(added), ", ".join(sorted(added)))
                    if removed:
                        logger.info("➖ Pares eliminados (%d): %s", len(removed), ", ".join(sorted(removed)))

                    if added or removed:
                        if has_scored_data:
                            # Pasar scored_data para que el callback actualice
                            # _max_leverage_map ANTES de arrancar nuevos traders.
                            await on_update_callback(
                                new_pairs, added, removed,
                                scored_data=self._last_scored,
                            )
                        elif len(cb_params) >= 3:
                            await on_update_callback(new_pairs, added, removed)
                        else:
                            logger.warning(
                                "[PairScanner] Callback con firma antigua — "
                                "traders salientes no esperaran al ciclo siguiente"
                            )
                            await on_update_callback(new_pairs)
                    else:
                        logger.info("✅ Sin cambios en pares activos (%d pares)", len(self.active_pairs))

            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error("[PairScanner] Error en run_scanner_loop: %s", e, exc_info=True)

            await asyncio.sleep(self.refresh_interval)


class _HLExchangeStub:
    """Stub mínimo para satisfacer referencias a self.exchange en PairScanner."""
    pass
