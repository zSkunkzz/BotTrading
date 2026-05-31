"""
trader.py — Motor de trading para Hyperliquid perpetuos.

Autenticación soportada:
  Opción A (recomendada): API Key de agente
    - HL_API_PRIVATE_KEY     : private key del wallet AGENTE generado en app.hyperliquid.xyz
    - HL_API_WALLET_ADDRESS  : dirección del wallet PRINCIPAL (el que tiene fondos y aprobó el agente)

  Opción B: Private key directa
    - HL_PRIVATE_KEY         : private key del wallet principal
    - HL_ACCOUNT_ADDR        : dirección pública (opcional, se deriva automáticamente)

Opcionales:
  HL_TESTNET        — "true" para usar testnet de Hyperliquid
  LOOP_SLEEP        — segundos entre iteraciones del loop (default 10)
  OHLCV_TF          — timeframe OHLCV (default 15m)
  OHLCV_LIMIT       — número de velas a cargar (default 200)
  OHLCV_MIN_BARS    — mínimo de velas requeridas (default 55)
  TP2_PARTIAL_RATIO — ratio de cierre parcial en TP2 (default 0.5)
"""
from __future__ import annotations

import asyncio
import logging
import os
import time
import json as _json
from typing import Optional

import aiohttp
from eth_account import Account

from bot.strategy import decide
from bot.ai_trader import ai_decide
from bot.telegram_bot import notify_open, notify_close, notify_tp_partial
from bot.state import save_position, load_position, clear_position, mark_tp2_hit
from bot.balance_service import balance_svc
from bot.pretrade_risk import pretrade_risk
from bot.kill_switch import kill_switch
from bot.execution.execution_engine import execution_engine
from bot.ohlcv_cache import ohlcv_cache

logger = logging.getLogger("Trader")

LOOP_SLEEP        = float(os.getenv("LOOP_SLEEP", "10"))
TP2_PARTIAL_RATIO = float(os.getenv("TP2_PARTIAL_RATIO", "0.5"))
OHLCV_TF          = os.getenv("OHLCV_TF", "15m")
OHLCV_LIMIT       = int(os.getenv("OHLCV_LIMIT", "200"))
OHLCV_MIN_BARS    = int(os.getenv("OHLCV_MIN_BARS", "55"))

_POS_CHECK_INTERVAL_S   = int(os.getenv("POS_CHECK_INTERVAL_S", "30"))
_TPSL_VERIFY_INTERVAL_S = int(os.getenv("TPSL_VERIFY_INTERVAL_S", "120"))
_SL_SW_MARGIN           = float(os.getenv("SL_SW_MARGIN", "0.001"))

# FIX: tiempo de espera tras apertura antes de confirmar posición en exchange
# Hyperliquid puede tardar hasta 2-3s en propagar el estado de la posición
_POST_FILL_CONFIRM_DELAY_S = float(os.getenv("POST_FILL_CONFIRM_DELAY_S", "2.5"))
_POST_FILL_CONFIRM_RETRIES = int(os.getenv("POST_FILL_CONFIRM_RETRIES", "3"))

_USE_TESTNET = os.getenv("HL_TESTNET", "").lower() in ("true", "1", "yes")
_API_URL     = "https://api.hyperliquid-testnet.xyz" if _USE_TESTNET else "https://api.hyperliquid.xyz"

_SET_LEVERAGE_TIMEOUT = float(os.getenv("SET_LEVERAGE_TIMEOUT", "20"))

# ── Rate limiter global para /info ─────────────────────────────────────────
_HL_REST_LOCK    = asyncio.Lock()
_HL_LAST_CALL    = 0.0
_HL_MIN_INTERVAL = 0.6

async def _hl_throttle():
    global _HL_LAST_CALL
    async with _HL_REST_LOCK:
        now = time.monotonic()
        wait = _HL_MIN_INTERVAL - (now - _HL_LAST_CALL)
        if wait > 0:
            await asyncio.sleep(wait)
        _HL_LAST_CALL = time.monotonic()


# ── Helpers ─────────────────────────────────────────────────────────────────

def _norm_coin(symbol: str) -> str:
    s = symbol.replace("/", "").replace(":USDT", "").upper()
    if s.endswith("USDTUSDT"):
        s = s[:-4]
    if s.endswith("USDT"):
        s = s[:-4]
    return s


def _hl_side_to_str(raw_side: str) -> str:
    """Convierte el side que devuelve HL ('A'=ask=short, 'B'=bid=long) a 'long'/'short'."""
    if raw_side == "B":
        return "long"
    if raw_side == "A":
        return "short"
    s = raw_side.lower()
    if s in ("long", "buy"):
        return "long"
    if s in ("short", "sell"):
        return "short"
    raise ValueError(f"Side desconocido de HL: {raw_side!r}")


class FuturesTrader:
    def __init__(self, api_key, api_secret, symbol,
                 leverage, margin_mode, dry_run,
                 passphrase=None):
        self.symbol      = symbol
        self.coin        = _norm_coin(symbol)
        self.leverage    = leverage
        self.margin_mode = os.getenv("MARGIN_MODE", margin_mode or "isolated").lower()
        self.dry_run     = dry_run

        api_pk     = os.getenv("HL_API_PRIVATE_KEY", "").strip()
        api_wallet = os.getenv("HL_API_WALLET_ADDRESS", "").strip()

        if api_pk:
            if not api_wallet:
                raise ValueError(
                    f"[{symbol}] HL_API_WALLET_ADDRESS es OBLIGATORIA en modo agente. "
                    "Debe ser la dirección del wallet PRINCIPAL (el que tiene fondos) "
                    "que aprobó al agente en app.hyperliquid.xyz → Settings → API."
                )
            self._private_key  = api_pk
            self._agent_mode   = True
            agent_acct         = Account.from_key(api_pk)
            self._agent_addr   = agent_acct.address
            self._master_addr  = api_wallet
            self._account_addr = self._master_addr

            from bot.core.hl_client import HLClient
            self._hl_client = HLClient(symbol)

            logger.info(
                "[%s] Auth: modo agente | master=%s | agente=%s",
                symbol, self._master_addr[:10] + "...", self._agent_addr[:10] + "...",
            )
        else:
            pk = os.getenv("HL_PRIVATE_KEY", api_secret or "").strip()
            if not pk:
                raise ValueError(
                    f"[{symbol}] No hay ninguna clave configurada. "
                    "Configura HL_API_PRIVATE_KEY (modo agente) o HL_PRIVATE_KEY (modo directo)."
                )
            self._private_key   = pk
            self._account_addr  = os.getenv("HL_ACCOUNT_ADDR", "").strip()
            self._agent_mode    = False
            self._agent_addr    = ""
            if not self._account_addr:
                acct = Account.from_key(pk)
                self._account_addr = acct.address
                logger.debug("[%s] HL_ACCOUNT_ADDR derivada: %s", symbol, self._account_addr)
            self._master_addr  = self._account_addr

            from bot.core.hl_client import HLClient
            self._hl_client = HLClient(symbol)

            logger.info(
                "[%s] Auth: modo directo | addr=%s",
                symbol, self._account_addr[:10] + "..." if self._account_addr else "N/A",
            )

        self.position       = None
        self.entry_price    = None
        self.sl             = None
        self.tp1 = self.tp2 = self.tp3 = None
        self.tp2_hit        = False
        self.trade_count    = 0
        self.win_count      = 0
        self.total_pnl      = 0.0
        self._open_notional = 0.0
        self._open_leverage = 1
        self._protection_ok = False
        self._last_pos_check_at:   float = 0.0
        self._last_tpsl_verify_at: float = 0.0
        self._ccxt_exchange = None

    # ── ccxt ──────────────────────────────────────────────────────────────────

    async def _get_ccxt(self):
        if self._ccxt_exchange is None:
            import ccxt.async_support as ccxt
            if self._agent_mode:
                self._ccxt_exchange = ccxt.hyperliquid({
                    "walletAddress":   self._master_addr,
                    "privateKey":      self._private_key,
                    "enableRateLimit": True,
                    "options": {"agentAddress": self._agent_addr},
                })
            else:
                self._ccxt_exchange = ccxt.hyperliquid({
                    "walletAddress":   self._master_addr,
                    "privateKey":      self._private_key,
                    "enableRateLimit": True,
                })
        return self._ccxt_exchange

    async def _close_ccxt(self):
        if self._ccxt_exchange is not None:
            try:
                await self._ccxt_exchange.close()
            except Exception:
                pass
            self._ccxt_exchange = None

    # ── HTTP helpers ──────────────────────────────────────────────────────────

    async def _info_post(self, payload: dict) -> dict:
        await _hl_throttle()
        async with aiohttp.ClientSession() as s:
            async with s.post(
                f"{_API_URL}/info", json=payload,
                headers={"Content-Type": "application/json"},
                timeout=aiohttp.ClientTimeout(total=10),
            ) as r:
                if r.status == 429:
                    logger.warning("[%s] 429 en /info, esperando 5s...", self.symbol)
                    await asyncio.sleep(5.0)
                    await _hl_throttle()
                    async with aiohttp.ClientSession() as s2:
                        async with s2.post(
                            f"{_API_URL}/info", json=payload,
                            headers={"Content-Type": "application/json"},
                            timeout=aiohttp.ClientTimeout(total=10),
                        ) as r2:
                            return _json.loads(await r2.text())
                return _json.loads(await r.text())

    # ── Init / cleanup ────────────────────────────────────────────────────────

    async def _init(self, usdc_per_trade: float):
        await self._get_ccxt()
        saved = load_position(self.symbol)
        if saved:
            # FIX: verificar en el exchange que la posición guardada realmente existe
            # antes de restaurarla — evita state mismatch si fue cerrada externamente
            exchange_pos = await self._get_positions()
            if exchange_pos is not None and len(exchange_pos) > 0:
                self.position       = saved["side"]
                self.entry_price    = saved["entry"]
                self.sl             = saved.get("sl")
                self.tp1            = saved.get("tp1")
                self.tp2            = saved.get("tp2")
                self.tp3            = saved.get("tp3")
                self.tp2_hit        = saved.get("tp2_hit", False)
                self._open_notional = saved.get("usdc_amount", saved.get("usdt_amount", 0.0))
                self._open_leverage = saved.get("leverage", self.leverage)
                self._protection_ok = True
                logger.info("[%s] Posicion restaurada: %s @ %s", self.symbol, self.position, self.entry_price)
            elif exchange_pos is not None and len(exchange_pos) == 0:
                # Posición guardada pero ya no existe en Hyperliquid — limpiar
                logger.warning(
                    "[%s] Posición guardada localmente pero NO existe en exchange — limpiando estado.",
                    self.symbol,
                )
                clear_position(self.symbol)
            else:
                # exchange_pos es None → error de red, restaurar con cautela sin _protection_ok
                logger.warning(
                    "[%s] No se pudo verificar posición en exchange al arrancar (error de red) — "
                    "restaurando estado local sin marcar protección OK.",
                    self.symbol,
                )
                self.position       = saved["side"]
                self.entry_price    = saved["entry"]
                self.sl             = saved.get("sl")
                self.tp1            = saved.get("tp1")
                self.tp2            = saved.get("tp2")
                self.tp3            = saved.get("tp3")
                self.tp2_hit        = saved.get("tp2_hit", False)
                self._open_notional = saved.get("usdc_amount", saved.get("usdt_amount", 0.0))
                self._open_leverage = saved.get("leverage", self.leverage)
                self._protection_ok = False  # forzar reverificación en próximo ciclo

        if not balance_svc.is_ready():
            balance_svc.init_hl(self._master_addr, self._info_post)

        try:
            await asyncio.wait_for(
                self._set_leverage(self.leverage),
                timeout=_SET_LEVERAGE_TIMEOUT,
            )
        except asyncio.TimeoutError:
            logger.warning(
                "[%s] _set_leverage tardó más de %ss — continuando sin confirmar leverage",
                self.symbol, _SET_LEVERAGE_TIMEOUT,
            )
        except Exception as e:
            logger.warning("[%s] _set_leverage error (no crítico): %s", self.symbol, e)

        logger.info(
            "[%s] Trader iniciado | coin=%s | master=%s | agent_mode=%s | agente=%s",
            self.symbol, self.coin,
            self._master_addr[:10] + "..." if self._master_addr else "N/A",
            self._agent_mode,
            self._agent_addr[:10] + "..." if self._agent_addr else "N/A",
        )

    async def cleanup(self):
        await self._close_ccxt()

    # ── Precio y OHLCV ──────────────────────────────────────────────────────────

    async def get_price(self) -> float:
        try:
            from bot.ws_feed import ws_feed
            if ws_feed.is_price_fresh(self.coin):
                price = ws_feed.get_price(self.coin)
                if price and price > 0:
                    return price
        except Exception:
            pass
        data = await self._info_post({"type": "allMids"})
        mid = data.get(self.coin)
        if mid:
            return float(mid)
        raise ValueError(f"No se pudo obtener precio para {self.coin}")

    async def get_ohlcv(self, tf: str = OHLCV_TF) -> list:
        try:
            from bot.ws_feed import ws_feed
            if ws_feed.has_data(self.coin, tf=tf, min_candles=OHLCV_MIN_BARS):
                df = ws_feed.get_ohlcv(self.coin, tf)
                if not df.empty and len(df) >= OHLCV_MIN_BARS:
                    df_reset = df.reset_index()
                    return [
                        [int(row["ts"].timestamp() * 1000),
                         float(row["open"]), float(row["high"]),
                         float(row["low"]),  float(row["close"]), float(row["volume"])]
                        for _, row in df_reset.iterrows()
                    ]
        except Exception as e:
            logger.debug("[%s] get_ohlcv WS error: %s", self.symbol, e)

        async def _fetch_rest(timeframe: str) -> list:
            tf_ms = {"15m": 15*60*1000, "1h": 60*60*1000, "4h": 4*60*60*1000}.get(timeframe, 15*60*1000)
            now   = int(time.time() * 1000)
            start = now - OHLCV_LIMIT * tf_ms
            data  = await self._info_post({
                "type": "candleSnapshot",
                "req":  {"coin": self.coin, "interval": timeframe, "startTime": start, "endTime": now},
            })
            if not isinstance(data, list) or not data:
                return []
            return [
                [int(c["t"]), float(c["o"]), float(c["h"]),
                 float(c["l"]), float(c["c"]), float(c["v"])]
                for c in data
            ]

        try:
            return await ohlcv_cache.get(self.coin, tf, _fetch_rest)
        except Exception as e:
            logger.error("[%s] get_ohlcv cache error: %s", self.symbol, e)
            return []

    async def get_balance(self) -> float | None:
        return await balance_svc.get()

    # ── Leverage — usa SDK directamente (fix: evita signing manual) ──────────

    async def _set_leverage(self, leverage: int):
        """
        Establece el leverage usando el SDK oficial de Hyperliquid.

        El SDK maneja internamente el signing EIP-712 con los tipos correctos,
        evitando el error 'Unsupported type: str' del signing manual.
        """
        if self.dry_run:
            return
        is_cross = self.margin_mode != "isolated"
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(
            None,
            lambda: self._hl_client._exchange.update_leverage(
                int(leverage), self.coin, is_cross
            ),
        )
        if isinstance(result, dict) and result.get("status") == "ok":
            logger.debug("[%s] Leverage %sx OK (cross=%s)", self.symbol, leverage, is_cross)
        else:
            logger.debug("[%s] set_leverage respuesta: %s", self.symbol, result)

    # ── Órdenes ───────────────────────────────────────────────────────────────────

    async def _get_order_status(self, order_id) -> dict:
        try:
            return await self._info_post({
                "type": "orderStatus",
                "user": self._account_addr,
                "oid":  int(order_id),
            })
        except Exception as e:
            logger.debug("[%s] _get_order_status error: %s", self.symbol, e)
            return {}

    async def _place_order(self, side: str, qty: float, reduce_only: bool = False,
                           sl: float | None = None, tp: float | None = None) -> dict:
        """Wrapper público hacia execution_engine."""
        try:
            arrival_price = await self.get_price()
        except Exception:
            arrival_price = 0.0

        ask = bid = None
        try:
            from bot.ws_feed import ws_feed
            ob = ws_feed.get_orderbook_metrics(self.coin)
            if ob:
                ask = ob.get("ask")
                bid = ob.get("bid")
        except Exception:
            pass

        trade_side = "close" if reduce_only else "open"
        r = await execution_engine.execute(
            trader=self, side=side, qty=qty, arrival_price=arrival_price,
            ask=ask, bid=bid, trade_side=trade_side, reduce_only=reduce_only,
            sl=sl, tp=tp,
        )
        if r.get("status") == "ok":
            balance_svc.invalidate()
            await kill_switch.on_order_result(rejected=False)
        else:
            await kill_switch.on_order_result(rejected=True)
        return r

    # ── Posiciones ─────────────────────────────────────────────────────────────

    async def _get_positions(self) -> list | None:
        """
        Consulta posiciones abiertas en Hyperliquid para este símbolo.

        Retorna:
          - list[dict]  : lista de posiciones (puede ser vacía si no hay ninguna)
          - None        : error de red / API — no se pudo determinar el estado real

        IMPORTANTE: retornar None en vez de [] ante errores permite a los llamantes
        distinguir "sin posición" de "no lo sé", evitando limpiar estado local
        innecesariamente por un 429 o timeout.
        """
        try:
            data = await self._info_post({"type": "clearinghouseState", "user": self._account_addr})
            if not data or not isinstance(data, dict):
                logger.warning("[%s] _get_positions: respuesta vacía o inválida", self.symbol)
                return None  # FIX: None = error, no lista vacía
            positions = []
            for p in data.get("assetPositions", []):
                pos = p.get("position", {})
                if pos.get("coin") == self.coin:
                    szi = float(pos.get("szi", 0))
                    if abs(szi) > 0:
                        positions.append(pos)
            return positions
        except Exception as e:
            logger.error("[%s] _get_positions error: %s", self.symbol, e)
            return None  # FIX: None = error de red, no lista vacía

    async def _confirm_position_with_retry(self) -> list | None:
        """
        FIX: Verifica la posición en el exchange con reintentos y backoff.

        Hyperliquid puede tardar 2-3s en propagar el estado de la posición tras
        un fill. Sin este delay, la confirmación inmediata devuelve [] aunque la
        posición exista, lo que causaba que el bot abortara el registro de estado.

        Retorna la primera respuesta no-vacía encontrada, o None si hay error de red.
        """
        for attempt in range(_POST_FILL_CONFIRM_RETRIES):
            if attempt > 0:
                # Backoff creciente: 2.5s, 5s, 7.5s...
                delay = _POST_FILL_CONFIRM_DELAY_S * attempt
                logger.debug(
                    "[%s] Post-fill confirm: intento %d/%d (esperando %.1fs)...",
                    self.symbol, attempt + 1, _POST_FILL_CONFIRM_RETRIES, delay,
                )
                await asyncio.sleep(delay)
            else:
                # Primer intento: esperar siempre el delay base
                logger.debug(
                    "[%s] Post-fill confirm: esperando %.1fs para propagación...",
                    self.symbol, _POST_FILL_CONFIRM_DELAY_S,
                )
                await asyncio.sleep(_POST_FILL_CONFIRM_DELAY_S)

            positions = await self._get_positions()
            if positions is None:
                # Error de red — retornar None directamente, no reintentar
                return None
            if len(positions) > 0:
                # Posición confirmada
                return positions
            # Posición vacía — puede ser propagación tardía, reintentar

        logger.warning(
            "[%s] Post-fill confirm: posición no visible tras %d intentos (%.1fs total). "
            "Puede ser propagación muy lenta o fill parcial a cero.",
            self.symbol,
            _POST_FILL_CONFIRM_RETRIES,
            _POST_FILL_CONFIRM_DELAY_S * _POST_FILL_CONFIRM_RETRIES,
        )
        return []  # Devolver lista vacía (no error de red, simplemente no hay posición)

    # ── Loop principal ───────────────────────────────────────────────────────────

    async def run(self, risk, *, global_risk=None):
        """Loop principal del trader para un símbolo."""
        await self._init(risk.usdc_per_trade)
        while True:
            try:
                await self._iteration(risk, global_risk)
            except asyncio.CancelledError:
                logger.info("[%s] Trader cancelado.", self.symbol)
                raise
            except Exception as e:
                logger.error("[%s] Error en iteración: %s", self.symbol, e, exc_info=True)
            await asyncio.sleep(LOOP_SLEEP)

    async def _iteration(self, risk, global_risk):
        """Una iteración del loop de trading."""
        if kill_switch.is_halted(self.symbol):
            logger.debug("[%s] Kill switch activo — skip.", self.symbol)
            return

        try:
            price = await self.get_price()
        except Exception as e:
            logger.warning("[%s] No se pudo obtener precio: %s", self.symbol, e)
            return

        now = time.monotonic()
        did_check_exchange = False
        exchange_positions = None  # FIX: None = no consultado aún
        if now - self._last_pos_check_at >= _POS_CHECK_INTERVAL_S:
            exchange_positions      = await self._get_positions()
            self._last_pos_check_at = now
            did_check_exchange      = True

        if did_check_exchange:
            if exchange_positions is None:
                # FIX: error de red — NO tocar el estado local, solo loggear
                logger.warning(
                    "[%s] No se pudo verificar posición en exchange (error de red) — "
                    "manteniendo estado local sin cambios.",
                    self.symbol,
                )
            elif exchange_positions:
                ep = exchange_positions[0]
                if self.position is None:
                    raw_side = ep.get("side", "")
                    try:
                        parsed_side = _hl_side_to_str(raw_side)
                    except ValueError:
                        logger.warning("[%s] Side desconocido del exchange: %r — skip sync", self.symbol, raw_side)
                        parsed_side = None
                    if parsed_side:
                        self.position    = parsed_side
                        self.entry_price = float(ep.get("entryPx") or 0)
                        logger.info("[%s] Posición detectada en exchange: %s @ %s",
                                    self.symbol, self.position, self.entry_price)
            else:
                # exchange_positions == [] → confirmado que NO hay posición abierta
                if self.position is not None:
                    logger.info("[%s] Posición cerrada externamente.", self.symbol)
                    try:
                        loop = asyncio.get_event_loop()
                        await loop.run_in_executor(None, self._hl_client.cancel_all_open_tpsl)
                        logger.info("[%s] Trigger orders huérfanos cancelados.", self.symbol)
                    except Exception as e:
                        logger.warning("[%s] No se pudieron cancelar triggers huérfanos: %s", self.symbol, e)
                    self.position    = None
                    self.entry_price = None
                    self.sl = self.tp1 = self.tp2 = self.tp3 = None
                    self.tp2_hit = False
                    clear_position(self.symbol)

        if self.position is not None:
            await self._manage_open_position(price, risk)
            return

        if global_risk:
            allowed, reason = await global_risk.can_open()
            if not allowed:
                logger.debug("[%s] GlobalRisk: %s", self.symbol, reason)
                return

        balance = await self.get_balance()
        if balance is not None and balance < risk.usdc_per_trade:
            logger.warning("[%s] Balance insuficiente (%.2f < %.2f USDC).",
                           self.symbol, balance, risk.usdc_per_trade)
            return

        try:
            if not await pretrade_risk.check(self.symbol, risk, balance or 0.0):
                logger.debug("[%s] pretrade_risk bloqueó la entrada.", self.symbol)
                return
        except Exception as e:
            logger.debug("[%s] pretrade_risk error (ignorando): %s", self.symbol, e)

        try:
            exch = await self._get_ccxt()
            decision = await decide(
                exch=exch,
                symbol=self.symbol,
                ai_decide_fn=ai_decide,
                has_open_position=False,
                current_pnl=None,
            )
        except Exception as e:
            logger.error("[%s] decide() error: %s", self.symbol, e)
            return

        action = decision.get("action", "HOLD")
        signal = decision.get("signal")
        if action not in ("BUY", "SELL"):
            return

        if signal:
            entry = signal.entry or price
            sl    = signal.sl
            tp1   = signal.tp1
            tp2   = signal.tp2
            tp3   = getattr(signal, "tp3", None)
            lev   = signal.suggested_lev or self.leverage
        else:
            entry = price
            sl = tp1 = tp2 = tp3 = None
            lev = self.leverage

        lev = min(int(lev), self.leverage)
        if lev != self.leverage:
            await self._set_leverage(lev)

        notional = risk.usdc_per_trade * lev
        qty      = round(notional / entry, 6)
        if qty <= 0:
            logger.warning("[%s] Cantidad calculada <= 0, skip.", self.symbol)
            return

        side = "buy" if action == "BUY" else "sell"
        logger.info(
            "[%s] 📈 Abriendo %s · qty=%s · entry=~%s · sl=%s · tp1=%s | %s",
            self.symbol, action, qty, round(entry, 4),
            round(sl, 4) if sl else "N/A",
            round(tp1, 4) if tp1 else "N/A",
            decision.get("reason", ""),
        )

        if self.dry_run:
            result = {"status": "ok", "_fill_price": entry}
            fill_price = entry
            confirmed_positions = [{"szi": qty}]  # simular posición confirmada en dry_run
        else:
            result = await self._place_order(side, qty, sl=sl, tp=tp1)
            if result.get("status") != "ok":
                # FIX: la orden falló — NO actualizar self.position ni guardar estado
                logger.error(
                    "[%s] ❌ Orden rechazada por el exchange — NO se registra posición local. "
                    "Respuesta: %s",
                    self.symbol, result,
                )
                return

            # FIX: extraer fill price del response
            fill_price = entry
            try:
                fill_price = float(
                    result.get("response", {}).get("data", {}).get("statuses", [{}])[0]
                    .get("filled", {}).get("avgPx") or entry
                )
            except Exception:
                fill_price = entry

            # FIX: confirmar con reintentos — Hyperliquid tarda 2-3s en propagar
            # el estado de la posición tras el fill. Sin el delay, la verificación
            # inmediata devuelve [] y el bot abortaba el registro de estado.
            confirmed_positions = await self._confirm_position_with_retry()
            if confirmed_positions is not None and len(confirmed_positions) == 0:
                logger.error(
                    "[%s] ❌ Orden enviada con status=ok pero NO hay posición abierta en Hyperliquid "
                    "tras %d intentos (%.1fs). Posible fill parcial a cero o orden expirada. "
                    "NO se registra estado local.",
                    self.symbol,
                    _POST_FILL_CONFIRM_RETRIES,
                    _POST_FILL_CONFIRM_DELAY_S * _POST_FILL_CONFIRM_RETRIES,
                )
                return
            if confirmed_positions is None:
                logger.warning(
                    "[%s] ⚠️ No se pudo confirmar posición en exchange (error de red) tras apertura. "
                    "Registrando estado local pero marcando protección pendiente.",
                    self.symbol,
                )

        if fill_price and fill_price != entry:
            qty = round(notional / fill_price, 6)
            logger.debug("[%s] Fill real: %.4f (estimado: %.4f) — qty ajustada a %.6f",
                         self.symbol, fill_price, entry, qty)

        self.position       = "long" if action == "BUY" else "short"
        self.entry_price    = fill_price
        self.sl             = sl
        self.tp1            = tp1
        self.tp2            = tp2
        self.tp3            = tp3
        self.tp2_hit        = False
        self._open_notional = notional
        self._open_leverage = lev
        # FIX: _protection_ok = True solo si el exchange confirmó la posición
        self._protection_ok = (
            confirmed_positions is not None and len(confirmed_positions) > 0
        ) if not self.dry_run else True
        self.trade_count   += 1

        save_position(self.symbol, {
            "side":        self.position,
            "entry":       self.entry_price,
            "sl":          self.sl,
            "tp1":         self.tp1,
            "tp2":         self.tp2,
            "tp3":         self.tp3,
            "tp2_hit":     self.tp2_hit,
            "usdc_amount": notional,
            "leverage":    lev,
        })

        if global_risk:
            await global_risk.register_open()

        await notify_open(
            symbol=self.symbol,
            side=self.position,
            price=self.entry_price,
            leverage=lev,
            usdt=notional,
            sl=self.sl,
            tp1=self.tp1,
            tp2=self.tp2,
        )

    async def _manage_open_position(self, price: float, risk):
        """Gestiona TP parciales, trailing stop y cierre de posición."""
        if self.position is None or self.entry_price is None:
            return

        is_long = self.position == "long"

        # ── TP2 parcial ────────────────────────────────────────────────────
        if self.tp2 and not self.tp2_hit:
            tp2_triggered = (is_long and price >= self.tp2) or (not is_long and price <= self.tp2)
            if tp2_triggered:
                self.tp2_hit = True
                mark_tp2_hit(self.symbol)
                partial_qty = round((self._open_notional / self.entry_price) * TP2_PARTIAL_RATIO, 6)
                if partial_qty > 0 and not self.dry_run:
                    close_side = "sell" if is_long else "buy"

                    try:
                        loop = asyncio.get_event_loop()
                        await loop.run_in_executor(None, self._hl_client.cancel_all_open_tpsl)
                    except Exception as e:
                        logger.warning("[%s] TP2: no se pudieron cancelar triggers: %s", self.symbol, e)

                    r = await self._place_order(close_side, partial_qty, reduce_only=True)
                    if r.get("status") == "ok":
                        logger.info("[%s] TP2 parcial ejecutado (%.1f%%)", self.symbol, TP2_PARTIAL_RATIO * 100)
                        await notify_tp_partial(
                            symbol=self.symbol,
                            side=self.position,
                            price=price,
                            tp_level=2,
                            ratio=TP2_PARTIAL_RATIO,
                        )

                        remaining_notional = self._open_notional * (1 - TP2_PARTIAL_RATIO)
                        remaining_qty = round(remaining_notional / self.entry_price, 6)
                        if remaining_qty > 0 and (self.tp3 or self.sl):
                            try:
                                await self._place_tpsl(remaining_qty, self.sl, self.tp3)
                            except Exception as e:
                                logger.warning("[%s] No se pudieron re-colocar TP/SL tras parcial: %s", self.symbol, e)

        # ── Trailing stop ────────────────────────────────────────────────────
        if risk.trailing_sl and self.sl is not None:
            activation_px = self.entry_price * (
                1 + risk.trailing_activation_pct / 100 if is_long
                else 1 - risk.trailing_activation_pct / 100
            )
            activated = (is_long and price >= activation_px) or (not is_long and price <= activation_px)
            if activated:
                callback = risk.trailing_callback_pct / 100
                new_sl = price * (1 - callback if is_long else 1 + callback)
                sl_moved = (
                    (is_long and new_sl > self.sl) or
                    (not is_long and new_sl < self.sl)
                )
                if sl_moved:
                    old_sl = self.sl
                    self.sl = new_sl
                    logger.debug("[%s] Trailing SL → %.4f (era %.4f)", self.symbol, self.sl, old_sl)
                    try:
                        qty_positions = await self._get_positions()
                        if qty_positions:
                            live_qty = abs(float(qty_positions[0].get("szi", 0)))
                            if live_qty > 0:
                                await self._place_tpsl(live_qty, self.sl, None)
                    except Exception as e:
                        logger.warning("[%s] No se pudo actualizar trailing SL en exchange: %s", self.symbol, e)

        # ── Evaluar SL / TP ───────────────────────────────────────────────────
        sl_hit  = self.sl  and ((is_long and price <= self.sl)  or (not is_long and price >= self.sl))
        tp3_hit = self.tp3 and ((is_long and price >= self.tp3) or (not is_long and price <= self.tp3))
        tp1_hit = self.tp1 and not self.tp2 and ((is_long and price >= self.tp1) or (not is_long and price <= self.tp1))

        close_reason = "SL" if sl_hit else ("TP3" if tp3_hit else ("TP1" if tp1_hit else None))
        if not close_reason:
            return

        positions = await self._get_positions()
        if positions is None:
            # FIX: error de red — no intentar cerrar, reintentar en próxima iteración
            logger.warning(
                "[%s] Cierre por %s: no se pudo verificar posición en exchange (error de red) — "
                "reintentando en próxima iteración.",
                self.symbol, close_reason,
            )
            return
        if not positions:
            logger.warning("[%s] Cierre por %s: posición no encontrada en exchange (ya cerrada?).",
                           self.symbol, close_reason)
            self.position = self.entry_price = self.sl = None
            self.tp1 = self.tp2 = self.tp3 = None
            self.tp2_hit = False
            clear_position(self.symbol)
            return

        qty = abs(float(positions[0].get("szi", 0)))
        if qty <= 0:
            logger.error("[%s] Cierre por %s: qty=0, no se envía orden.", self.symbol, close_reason)
            return

        close_side = "sell" if is_long else "buy"
        fill_price = price

        if not self.dry_run:
            try:
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(None, self._hl_client.cancel_all_open_tpsl)
            except Exception as e:
                logger.warning("[%s] No se pudieron cancelar triggers antes del cierre: %s", self.symbol, e)

            result = await self._place_order(close_side, qty, reduce_only=True)

            if result.get("status") == "ok":
                try:
                    fill_price = float(
                        result.get("response", {}).get("data", {}).get("statuses", [{}])[0]
                        .get("filled", {}).get("avgPx", price)
                    )
                except Exception:
                    pass

        pnl_pct = ((fill_price - self.entry_price) / self.entry_price) * (1 if is_long else -1) * 100
        pnl_usd = (pnl_pct / 100) * self._open_notional
        if pnl_usd > 0:
            self.win_count += 1
        self.total_pnl += pnl_usd
        logger.info("[%s] 🔒 Cerrado por %s · fill=%.4f · PnL=%.2f USDC (%.2f%%)",
                    self.symbol, close_reason, fill_price, pnl_usd, pnl_pct)

        entry_copy = self.entry_price
        pos_copy   = self.position
        self.position = self.entry_price = self.sl = None
        self.tp1 = self.tp2 = self.tp3 = None
        self.tp2_hit = False
        clear_position(self.symbol)

        await notify_close(
            symbol=self.symbol,
            side=pos_copy,
            exit_p=fill_price,
            pnl=pnl_pct,
            entry=entry_copy,
            reason=close_reason,
        )

    async def _place_tpsl(self, qty: float, sl: float | None, tp: float | None) -> None:
        """Coloca trigger orders TP/SL para una qty dada."""
        if not sl and not tp:
            return
        is_long = self.position == "long"
        orders = []
        if sl:
            orders.append(("sl", not is_long, qty, sl))
        if tp:
            orders.append(("tp", not is_long, qty, tp))
        for order_type, side_is_buy, q, px in orders:
            try:
                loop = asyncio.get_event_loop()
                if order_type == "sl":
                    await loop.run_in_executor(None, lambda: self._hl_client.place_sl(is_buy=side_is_buy, sz=q, trigger_px=px))
                else:
                    await loop.run_in_executor(None, lambda: self._hl_client.place_tp(is_buy=side_is_buy, sz=q, trigger_px=px))
            except Exception as e:
                logger.warning("[%s] No se pudo colocar %s: %s", self.symbol, order_type, e)
