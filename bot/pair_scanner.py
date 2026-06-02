"""
pair_scanner.py — Escaner de pares para Hyperliquid perpetuos.

BUG #4 FIX: rotacion de par sin esperar cleanup del trader
  run_scanner_loop ahora llama on_update_callback con (new_pairs, removed)
  para que main.py pueda hacer cleanup SELECTIVO de los traders salientes
  antes de arrancar los nuevos. El callback debe:
    1. Para cada par en 'removed': cancelar tarea, llamar trader.cleanup(),
       y await trader._stopped_event.wait() con timeout.
    2. Arrancar traders para pares en 'added'.
  Si el callback solo acepta (new_pairs,), se hace fallback seguro.

FIX #2 (2026-06-02): run_scanner_loop arrancaba con sleep(refresh_interval)
  → el primer re-scan ocurría 30 minutos después del arranque.
  Fix: mover el sleep AL FINAL del ciclo para que el primer scan sea inmediato.
"""
import logging
import asyncio
import os
import aiohttp
import json as _json

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

# BUG #4: timeout maximo para esperar cleanup de un trader saliente
_TRADER_STOP_TIMEOUT_S = float(os.getenv("TRADER_STOP_TIMEOUT_S", "15"))


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
    def __init__(
        self,
        api_key=None, api_secret=None, passphrase=None,
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
        self._last_scored: list   = []

        extra = os.getenv("SYMBOL_BLACKLIST", "")
        self.blacklist = NON_CRYPTO_BASES | {
            s.strip().upper() for s in extra.split(",") if s.strip()
        }

        self.exchange = _HLExchangeStub()

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
        self._last_scored = scored
        self.active_pairs = [s["symbol"] for s in scored]
        logger.info(
            "[PairScanner] inject_snapshot: %d mercados activos → top %d seleccionados",
            sum(1 for r in rows if r.active), len(scored),
        )
        for p in scored[:5]:
            logger.info(
                "  %-12s Vol: $%sM | Cambio: %.2f%% | Funding: %.4f%% | MaxLev: %dx | Score: %s",
                p["symbol"], p["volume_usdt"], p["change_pct"], p["funding"],
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
                max_lev       = int(meta.get("maxLeverage", 0) or 0)
            except (ValueError, TypeError):
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

            score_change = change_pct if change_pct is not None else 0.0
            score = (day_volume / 1_000_000) * 0.6 + score_change * 0.4
            scored.append({
                "symbol":        coin,
                "volume_usdt":   round(day_volume / 1_000_000, 2),
                "change_pct":    round(change_pct, 2) if change_pct is not None else None,
                "last_price":    mark_px,
                "funding":       round(funding * 100, 5),
                "oi_usdt":       round(open_interest * mark_px / 1_000_000, 2),
                "score":         round(score, 3),
                "max_leverage":  max_lev,
            })

        logger.debug(
            "[PairScanner] scan: total=%d | blacklist=%d | vol_filter=%d | change_filter=%d | passed=%d",
            total_seen, skipped_blacklist, skipped_volume, skipped_change, len(scored),
        )

        scored.sort(key=lambda x: x["score"], reverse=True)
        top = scored[:self.top_n]
        self._last_scored = top

        logger.info("🏆 Top %d pares Hyperliquid seleccionados:", len(top))
        for p in top[:5]:
            change_str = f"{p['change_pct']}%" if p["change_pct"] is not None else "N/A"
            logger.info(
                "  %-12s Vol: $%sM | Cambio: %s | MaxLev: %dx | Score: %s",
                p["symbol"], p["volume_usdt"], change_str,
                p.get("max_leverage", 0), p["score"],
            )

        return [p["symbol"] for p in top]

    def normalize(self, symbol: str) -> str:
        return symbol.replace("/", "").replace(":USDT", "").replace("USDT", "").upper()

    async def run_scanner_loop(self, on_update_callback):
        """
        BUG #4 FIX: el callback recibe (new_pairs, added, removed) para
        que main.py pueda hacer cleanup SELECTIVO antes de arrancar nuevos.

        FIX #2: sleep movido AL FINAL del ciclo → primer re-scan inmediato
        al arrancar (antes dormía 30 min antes del primer scan).

        Protocolo del callback en main.py:
          async def _on_pairs_updated(new_pairs, added, removed):
              # 1. Cleanup traders salientes
              for sym in removed:
                  trader = active_traders.get(sym)
                  if trader:
                      task = trader_tasks.get(sym)
                      if task: task.cancel()
                      try:
                          await asyncio.wait_for(
                              trader._stopped_event.wait(),
                              timeout=TRADER_STOP_TIMEOUT_S
                          )
                      except asyncio.TimeoutError:
                          logger.warning("Trader %s no paro en tiempo", sym)
                      await trader.cleanup()
              # 2. Arrancar traders para pares nuevos
              for sym in added:
                  start_trader(sym)

        Fallback: si el callback solo acepta 1 argumento (new_pairs),
        se llama con la firma antigua para compatibilidad.
        """
        import inspect
        cb_params = len(inspect.signature(on_update_callback).parameters)

        while True:
            # FIX #2: scan ejecuta PRIMERO, sleep va al FINAL del ciclo
            try:
                logger.info("🔍 Re-escaneando mercado Hyperliquid...")
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
                        logger.info("➕ Nuevos pares: %s", ", ".join(added))
                    if removed:
                        logger.info("➖ Pares eliminados: %s", ", ".join(removed))

                    if added or removed:
                        # BUG #4 FIX: pasar added y removed al callback
                        if cb_params >= 3:
                            # Nueva firma: callback(new_pairs, added, removed)
                            await on_update_callback(new_pairs, added, removed)
                        else:
                            # Fallback: firma antigua callback(new_pairs)
                            logger.warning(
                                "[PairScanner] Callback con firma antigua — "
                                "traders salientes no esperaran al ciclo siguiente"
                            )
                            await on_update_callback(new_pairs)
                    else:
                        logger.info("✅ Sin cambios en pares activos")

            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error("[PairScanner] Error en run_scanner_loop: %s", e, exc_info=True)

            # FIX #2: sleep AL FINAL → próximo ciclo en refresh_interval
            await asyncio.sleep(self.refresh_interval)


class _HLExchangeStub:
    """Stub mínimo para satisfacer referencias a self.exchange en PairScanner."""
    pass
