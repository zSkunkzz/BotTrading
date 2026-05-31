"""
recover_tpsl.py — Coloca SL y TP en posiciones abiertas que no los tienen.

Uso:
  python scripts/recover_tpsl.py

El script:
  1. Lee todas las posiciones abiertas del exchange
  2. Lee el estado guardado localmente (data/positions/*.json)
  3. Para cada posición que NO tenga SL/TP en el exchange,
     los coloca usando los valores guardados en el estado local.
  4. Si no hay estado local, calcula SL/TP con ATR fallback.

Variables de entorno necesarias (las mismas del bot):
  HL_API_PRIVATE_KEY + HL_API_WALLET_ADDRESS  (modo agente)
  o HL_PRIVATE_KEY                             (modo directo)
"""
from __future__ import annotations

import asyncio
import json
import logging
import math
import os
import sys
import time
from pathlib import Path

import aiohttp

# ── path para importar bot/
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("recover_tpsl")

_USE_TESTNET = os.getenv("HL_TESTNET", "").lower() in ("true", "1", "yes")
_API_URL     = "https://api.hyperliquid-testnet.xyz" if _USE_TESTNET else "https://api.hyperliquid.xyz"

# ATR fallback multipliers (igual que trader.py)
_ATR_SL_MULT  = float(os.getenv("FALLBACK_ATR_SL_MULT",  "1.8"))
_ATR_TP1_MULT = float(os.getenv("FALLBACK_ATR_TP1_MULT", "3.5"))

_POSITIONS_DIR = ROOT / "data" / "positions"


# ── Helpers ────────────────────────────────────────────────────────────

def _norm_coin(symbol: str) -> str:
    s = symbol.replace("/", "").replace(":USDT", "").upper()
    for suffix in ("USDTUSDT", "USDT"):
        if s.endswith(suffix):
            s = s[: -len(suffix)]
    return s


async def _post(session: aiohttp.ClientSession, payload: dict) -> dict:
    async with session.post(
        f"{_API_URL}/info",
        json=payload,
        headers={"Content-Type": "application/json"},
        timeout=aiohttp.ClientTimeout(total=15),
    ) as r:
        return json.loads(await r.text())


def _load_state(coin: str) -> dict | None:
    """
    Intenta cargar el estado guardado para este coin.
    Busca archivos con el coin en el nombre dentro de data/positions/.
    """
    if not _POSITIONS_DIR.exists():
        return None
    coin_upper = coin.upper()
    for f in _POSITIONS_DIR.glob("*.json"):
        if coin_upper in f.name.upper():
            try:
                return json.loads(f.read_text())
            except Exception:
                pass
    return None


def _is_sl(o: dict) -> bool:
    ot = o.get("orderType", "")
    if isinstance(ot, str):
        ot_l = ot.lower()
        return "stop" in ot_l or ot_l in ("sl",)
    if isinstance(ot, dict):
        return ot.get("trigger", {}).get("tpsl", "") == "sl"
    return False


def _is_tp(o: dict) -> bool:
    ot = o.get("orderType", "")
    if isinstance(ot, str):
        ot_l = ot.lower()
        return "take profit" in ot_l or "take_profit" in ot_l or ot_l == "tp"
    if isinstance(ot, dict):
        return ot.get("trigger", {}).get("tpsl", "") == "tp"
    return False


async def _get_atr(session: aiohttp.ClientSession, coin: str, fallback_pct: float = 0.025) -> float:
    """Calcula ATR aproximado a partir de las últimas 14 velas de 1h."""
    try:
        now   = int(time.time() * 1000)
        start = now - 20 * 3600 * 1000
        data  = await _post(session, {
            "type": "candleSnapshot",
            "req":  {"coin": coin, "interval": "1h", "startTime": start, "endTime": now},
        })
        if not isinstance(data, list) or len(data) < 5:
            raise ValueError("insuficientes velas")
        trs = [float(c["h"]) - float(c["l"]) for c in data[-14:]]
        return sum(trs) / len(trs)
    except Exception as e:
        logger.warning("[%s] ATR no disponible (%s), usando %.1f%% del precio", coin, e, fallback_pct * 100)
        return 0.0  # se calcula en el caller con fallback_pct * price


# ── Core ─────────────────────────────────────────────────────────────────────

async def recover(dry_run: bool = False):
    from bot.core.hl_client import HLClient, _HLCore

    core = _HLCore()
    account_addr = core.account_addr

    async with aiohttp.ClientSession() as session:
        # 1. Obtener todas las posiciones abiertas
        state = await _post(session, {"type": "clearinghouseState", "user": account_addr})
        if not state or not isinstance(state, dict):
            logger.error("No se pudo obtener clearinghouseState")
            return

        open_positions = [
            p["position"] for p in state.get("assetPositions", [])
            if abs(float(p["position"].get("szi", 0))) > 0
        ]

        if not open_positions:
            logger.info("✅ No hay posiciones abiertas.")
            return

        logger.info("📊 Posiciones abiertas: %d", len(open_positions))

        # 2. Obtener todas las órdenes abiertas (triggers)
        open_orders_raw = await _post(session, {"type": "openOrders", "user": account_addr})
        open_orders = open_orders_raw if isinstance(open_orders_raw, list) else []

        # 3. Obtener todos los precios
        all_mids_raw = await _post(session, {"type": "allMids"})
        all_mids = all_mids_raw if isinstance(all_mids_raw, dict) else {}

        for pos in open_positions:
            coin     = pos["coin"]
            szi      = float(pos["szi"])
            entry_px = float(pos.get("entryPx") or 0)
            is_long  = szi > 0
            qty      = abs(szi)

            # Precio mid actual
            mid_price = float(all_mids.get(coin, entry_px))

            # Órdenes abiertas de este coin
            coin_orders = [o for o in open_orders if o.get("coin") == coin]
            has_sl      = any(_is_sl(o) for o in coin_orders)
            has_tp      = any(_is_tp(o) for o in coin_orders)

            side_str = "LONG" if is_long else "SHORT"
            sl_sym   = "✅" if has_sl else "❌"
            tp_sym   = "✅" if has_tp else "❌"
            logger.info(
                "[%s] %s qty=%.4f entry=%.5f mid=%.5f | SL:%s TP:%s | triggers_activos=%d",
                coin, side_str, qty, entry_px, mid_price, sl_sym, tp_sym, len(coin_orders),
            )

            if has_sl and has_tp:
                logger.info("[%s] ✓ Ya tiene SL y TP — nada que hacer.", coin)
                continue

            # 4. Determinar SL y TP desde estado local o ATR
            saved = _load_state(coin)
            sl_price = None
            tp_price = None

            if saved:
                sl_price = saved.get("sl")
                tp_price = saved.get("tp1")
                logger.info("[%s] Estado local encontrado: SL=%s TP1=%s", coin, sl_price, tp_price)

            if not sl_price or not tp_price:
                # Calcular con ATR
                atr = await _get_atr(session, coin)
                if atr <= 0:
                    atr = entry_px * 0.025  # 2.5% del entry como fallback
                risk_dist = atr * _ATR_SL_MULT
                if is_long:
                    sl_price  = entry_px - risk_dist if not sl_price else sl_price
                    tp_price  = entry_px + risk_dist * _ATR_TP1_MULT if not tp_price else tp_price
                else:
                    sl_price  = entry_px + risk_dist if not sl_price else sl_price
                    tp_price  = entry_px - risk_dist * _ATR_TP1_MULT if not tp_price else tp_price
                logger.info(
                    "[%s] Fallback ATR=%.5f | SL=%.5f TP=%.5f",
                    coin, atr, sl_price, tp_price,
                )

            # Validación de seguridad: SL/TP deben estar del lado correcto
            if is_long:
                if sl_price >= mid_price:
                    logger.error(
                        "[%s] ⚠️ SL=%.5f >= mid=%.5f para LONG — ajustando al 2.5%% bajo entry",
                        coin, sl_price, mid_price,
                    )
                    sl_price = entry_px * 0.975
                if tp_price <= mid_price:
                    logger.error(
                        "[%s] ⚠️ TP=%.5f <= mid=%.5f para LONG — ajustando al 3.5%% sobre mid",
                        coin, tp_price, mid_price,
                    )
                    tp_price = mid_price * 1.035
            else:
                if sl_price <= mid_price:
                    logger.error(
                        "[%s] ⚠️ SL=%.5f <= mid=%.5f para SHORT — ajustando al 2.5%% sobre entry",
                        coin, sl_price, mid_price,
                    )
                    sl_price = entry_px * 1.025
                if tp_price >= mid_price:
                    logger.error(
                        "[%s] ⚠️ TP=%.5f >= mid=%.5f para SHORT — ajustando al 3.5%% bajo mid",
                        coin, tp_price, mid_price,
                    )
                    tp_price = mid_price * 0.965

            logger.info(
                "[%s] 📈 Colocando: SL=%.5f TP=%.5f (%s)",
                coin, sl_price, tp_price,
                "DRY-RUN" if dry_run else "REAL",
            )

            if dry_run:
                logger.info("[%s] DRY-RUN — no se envía ninguna orden.", coin)
                continue

            # 5. Colocar en el exchange usando HLClient
            try:
                import asyncio as _asyncio
                client = HLClient(coin)
                loop   = _asyncio.get_event_loop()
                close_is_buy = not is_long

                if not has_sl and not has_tp:
                    sl_px  = client.round_px(sl_price)
                    tp_px  = client.round_px(tp_price)
                    buf    = 0.001
                    tp_lim = client.round_px(tp_px * (1 - buf) if is_long else tp_px * (1 + buf))
                    orders = [
                        {
                            "coin":       client.coin,
                            "is_buy":     close_is_buy,
                            "sz":         qty,
                            "limit_px":   sl_px,
                            "order_type": {"trigger": {"triggerPx": sl_px, "isMarket": True, "tpsl": "sl"}},
                            "reduce_only": True,
                        },
                        {
                            "coin":       client.coin,
                            "is_buy":     close_is_buy,
                            "sz":         qty,
                            "limit_px":   tp_lim,
                            "order_type": {"trigger": {"triggerPx": tp_px, "isMarket": False, "tpsl": "tp"}},
                            "reduce_only": True,
                        },
                    ]
                    result = await loop.run_in_executor(None, lambda: client.place_bulk(orders))
                    logger.info("[%s] ✅ Bulk SL+TP colocados | resultado: %s", coin, result)
                elif not has_sl:
                    sl_px = client.round_px(sl_price)
                    await loop.run_in_executor(
                        None, lambda sl_px=sl_px: client.place_sl(
                            is_buy=close_is_buy, sz=qty, trigger_px=sl_px
                        )
                    )
                    logger.info("[%s] ✅ SL=%.5f colocado", coin, sl_px)
                elif not has_tp:
                    tp_px  = client.round_px(tp_price)
                    buf    = 0.001
                    tp_lim = client.round_px(tp_px * (1 - buf) if is_long else tp_px * (1 + buf))
                    await loop.run_in_executor(
                        None, lambda tp_px=tp_px: client.place_tp(
                            is_buy=close_is_buy, sz=qty, trigger_px=tp_px
                        )
                    )
                    logger.info("[%s] ✅ TP=%.5f colocado", coin, tp_px)

            except Exception as e:
                logger.error("[%s] ❌ Error al colocar SL/TP: %s", coin, e, exc_info=True)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Recuperar SL/TP de posiciones sin protección")
    parser.add_argument("--dry-run", action="store_true", help="Solo mostrar, no colocar órdenes")
    args = parser.parse_args()

    asyncio.run(recover(dry_run=args.dry_run))
