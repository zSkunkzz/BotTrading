"""
Kill Switch — escalera de 4 niveles desacoplada del loop de ejecución.

Niveles
-------
L1  pausa nuevas entradas (trading bloqueado; posiciones existentes siguen vivas)
L2  halt de símbolo/estrategia  (igual que L1 pero con flag de símbolo individual)
L3  cancelar nuevas órdenes + bloquear entradas (requiere re-arm manual)
L4  cierre forzado de todo + hard kill  (requiere re-arm manual)

Re-arm
------
- L1/L2 → se pueden resetear automáticamente o llamando a .reset().
- L3/L4 → sólo se resetean con .manual_reset() + clave de seguridad.

Persistencia
------------
Cada activación guarda un snapshot JSON en /tmp/kill_switch_state.json.
"""

import asyncio
import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger("KillSwitch")

_STATE_PATH = os.getenv("KILL_SWITCH_STATE_PATH", "/tmp/kill_switch_state.json")
_REARM_KEY  = os.getenv("KILL_SWITCH_REARM_KEY", "REARM-BOTTRADING")

# Umbrales por defecto (sobreponibles con env vars)
_CFG = {
    "max_daily_loss_pct":     float(os.getenv("KS_MAX_DAILY_LOSS_PCT",      "8.0")),
    "max_consec_losses":      int(os.getenv("KS_MAX_CONSEC_LOSSES",         "5")),
    "max_reject_rate":        float(os.getenv("KS_MAX_REJECT_RATE",         "0.5")),
    "max_slippage_bps":       float(os.getenv("KS_MAX_SLIPPAGE_BPS",        "80.0")),
    "max_api_reconnects":     int(os.getenv("KS_MAX_API_RECONNECTS",        "10")),
    "max_state_mismatch":     int(os.getenv("KS_MAX_STATE_MISMATCH",        "3")),
    "watchdog_interval_s":    int(os.getenv("KS_WATCHDOG_INTERVAL_S",       "30")),
}


class KillSwitch:
    """
    Singleton thread-safe con asyncio.Lock.
    Importar la instancia global `kill_switch` desde este módulo.
    """

    def __init__(self):
        self._lock             = asyncio.Lock()
        self._level: int       = 0          # 0 = OK, 1-4 = activo
        self._trigger: str     = ""
        self._triggered_at: Optional[float] = None
        self._consec_losses: int = 0
        self._daily_pnl: float   = 0.0
        self._order_count: int   = 0
        self._reject_count: int  = 0
        self._api_reconnects: int = 0
        self._state_mismatches: int = 0
        self._slippage_samples: list[float] = []
        self._halted_symbols: set[str] = set()
        self._hard_killed: bool = False
        # Símbolos con TPSL retry activo — el watchdog los ignora para state_mismatch
        self._tpsl_retrying: set[str] = set()
        self._load_state()

    # ── Estado ────────────────────────────────────────────────────────────────

    def level(self) -> int:
        return self._level

    def is_halted(self, symbol: str | None = None) -> bool:
        """Devuelve True si el bot (o el símbolo) está detenido para nuevas entradas."""
        if self._level >= 1:
            return True
        if symbol and symbol in self._halted_symbols:
            return True
        return False

    def is_hard_killed(self) -> bool:
        return self._hard_killed

    # ── TPSL retry tracking ───────────────────────────────────────────────────

    def mark_tpsl_retrying(self, symbol: str):
        """Llamar cuando empieza un retry de TPSL (code 31008). Inhibe state_mismatch."""
        self._tpsl_retrying.add(symbol)

    def clear_tpsl_retrying(self, symbol: str):
        """Llamar cuando el TPSL se coloca OK o se agotan los reintentos."""
        self._tpsl_retrying.discard(symbol)

    # ── Activación ────────────────────────────────────────────────────────────

    async def activate(self, level: int, trigger: str):
        """Activa el kill switch al nivel indicado (no baja de nivel ya activo)."""
        async with self._lock:
            if level <= self._level:
                return
            self._level        = level
            self._trigger      = trigger
            self._triggered_at = time.time()
            self._save_state()
            logger.critical(
                f"🛑 KILL SWITCH L{level} activado — trigger: {trigger}"
            )
            # Importación diferida para evitar circulares
            try:
                from bot.telegram_bot import notify_kill_switch
                asyncio.create_task(notify_kill_switch(level, trigger))
            except Exception:
                pass

    async def activate_symbol(self, symbol: str, trigger: str):
        """L2: pausa sólo un símbolo concreto."""
        async with self._lock:
            self._halted_symbols.add(symbol)
            logger.warning(f"⚠️ KS L2 — símbolo {symbol} pausado: {trigger}")
            self._save_state()

    async def hard_kill(self, trigger: str):
        """L4: hard kill total. Persiste y requiere re-arm manual."""
        await self.activate(4, trigger)
        async with self._lock:
            self._hard_killed = True
            self._save_state()
            logger.critical("💀 HARD KILL L4 — bot parado completamente")

    # ── Reset ─────────────────────────────────────────────────────────────────

    async def reset(self, level: int = 1):
        """Resetea L1/L2 automáticamente. L3/L4 requieren manual_reset()."""
        async with self._lock:
            if self._level >= 3:
                logger.error("KS L3/L4 activo — usar manual_reset() con clave de seguridad")
                return
            self._level    = 0
            self._trigger  = ""
            self._halted_symbols.clear()
            self._save_state()
            logger.info("✅ Kill switch reseteado (L1/L2)")

    async def manual_reset(self, key: str):
        """Resetea cualquier nivel si la clave coincide con KILL_SWITCH_REARM_KEY."""
        if key != _REARM_KEY:
            logger.error("KS manual_reset: clave incorrecta")
            return False
        async with self._lock:
            self._level        = 0
            self._trigger      = ""
            self._triggered_at = None
            self._hard_killed  = False
            self._halted_symbols.clear()
            self._consec_losses = 0
            self._daily_pnl     = 0.0
            self._api_reconnects = 0
            self._state_mismatches = 0
            self._reject_count   = 0
            self._order_count    = 0
            self._tpsl_retrying.clear()
            self._slippage_samples.clear()
            self._save_state()
            logger.info("✅ Kill switch re-armado manualmente")
            return True

    # ── Registro de eventos (llamar desde trader / webhook) ───────────────────

    async def on_trade_result(self, pnl_pct: float):
        """Registrar resultado de un trade (positivo = ganancia)."""
        async with self._lock:
            self._daily_pnl += pnl_pct
            if pnl_pct < 0:
                self._consec_losses += 1
            else:
                self._consec_losses = 0

        # Evaluamos fuera del lock para poder activar con await
        if self._daily_pnl <= -_CFG["max_daily_loss_pct"]:
            await self.activate(3, f"Daily loss {self._daily_pnl:.2f}% ≥ límite {_CFG['max_daily_loss_pct']}%")
        elif self._consec_losses >= _CFG["max_consec_losses"]:
            await self.activate(1, f"{self._consec_losses} pérdidas consecutivas")

    async def on_order_result(self, rejected: bool):
        """Registrar si una orden fue rechazada (para reject-rate)."""
        async with self._lock:
            self._order_count  += 1
            if rejected:
                self._reject_count += 1
            window = max(10, self._order_count)
            rate = self._reject_count / window

        if rate >= _CFG["max_reject_rate"] and self._order_count >= 10:
            await self.activate(2, f"Reject rate {rate:.0%} en {self._order_count} órdenes")

    async def on_slippage(self, slippage_bps: float):
        """Registrar slippage de un fill."""
        async with self._lock:
            self._slippage_samples.append(slippage_bps)
            if len(self._slippage_samples) > 20:
                self._slippage_samples.pop(0)
            avg = sum(self._slippage_samples) / len(self._slippage_samples)

        if avg >= _CFG["max_slippage_bps"]:
            await self.activate(2, f"Slippage medio {avg:.1f} bps ≥ límite {_CFG['max_slippage_bps']} bps")

    async def on_api_reconnect(self):
        """Contar reconexiones de API/WS."""
        async with self._lock:
            self._api_reconnects += 1
            count = self._api_reconnects

        if count >= _CFG["max_api_reconnects"]:
            await self.activate(2, f"API/WS reconexiones excesivas: {count}")

    async def on_state_mismatch(self, symbol: str):
        """Mismatch entre estado local y exchange."""
        # Ignorar si el símbolo tiene TPSL retry activo (code 31008 lag de API)
        if symbol in self._tpsl_retrying:
            logger.debug(f"[{symbol}] KS: state_mismatch ignorado — TPSL retry activo")
            return

        async with self._lock:
            self._state_mismatches += 1
            count = self._state_mismatches

        await self.activate_symbol(symbol, f"State mismatch #{count} en {symbol}")
        if count >= _CFG["max_state_mismatch"]:
            await self.activate(3, f"State mismatch acumulado: {count} veces")

    def reset_daily_pnl(self):
        """Llamar al inicio de cada día UTC."""
        self._daily_pnl     = 0.0
        self._consec_losses = 0
        self._api_reconnects = 0
        logger.info("KS: contadores diarios reseteados")

    # ── Watchdog ──────────────────────────────────────────────────────────────

    async def run_watchdog(self, traders: dict):
        """
        Loop independiente que corre cada KS_WATCHDOG_INTERVAL_S segundos.
        `traders` es el dict {symbol: FuturesTrader} de main.py.
        """
        logger.info("🐕 Kill Switch Watchdog arrancado")
        interval = _CFG["watchdog_interval_s"]
        while True:
            try:
                await asyncio.sleep(interval)
                await self._watchdog_tick(traders)
            except asyncio.CancelledError:
                logger.info("KS Watchdog cancelado")
                break
            except Exception as e:
                logger.error(f"KS Watchdog error: {e}")

    async def _watchdog_tick(self, traders: dict):
        """Una iteración del watchdog."""
        if self._level >= 4:
            return  # ya está parado todo

        # Reset diario a las 00:00 UTC
        now_utc = datetime.now(timezone.utc)
        if now_utc.hour == 0 and now_utc.minute < 1:
            self.reset_daily_pnl()

        # FIX: Comprobar posiciones sin protección verificando SIEMPRE contra el exchange.
        # El estado local (trader.position) puede ser stale por 429 u otros errores de red.
        # Solo se dispara state_mismatch si el exchange CONFIRMA que hay posición abierta
        # pero _protection_ok es False — nunca basándose solo en estado local.
        for symbol, trader in list(traders.items()):
            try:
                if symbol in self._tpsl_retrying:
                    logger.debug(f"[{symbol}] KS watchdog: omitido (TPSL retry en curso)")
                    continue

                if not trader.position or trader._protection_ok:
                    # Sin posición local o ya protegida → nada que comprobar
                    continue

                # FIX: verificar en el exchange antes de disparar state_mismatch
                exchange_positions = await trader._get_positions()

                if exchange_positions is None:
                    # Error de red — no podemos saber el estado real, ignorar este ciclo
                    logger.warning(
                        f"[{symbol}] KS watchdog: no se pudo verificar posición en exchange "
                        f"(error de red) — ignorando este ciclo para evitar falso positivo."
                    )
                    continue

                if len(exchange_positions) == 0:
                    # No hay posición en el exchange — el estado local está stale
                    # Limpiar el estado local silenciosamente (el trader lo hará en su propio ciclo)
                    logger.info(
                        f"[{symbol}] KS watchdog: estado local dice 'posición abierta' pero "
                        f"NO hay posición en Hyperliquid — estado local stale, skip state_mismatch."
                    )
                    continue

                # Hay posición real en el exchange Y _protection_ok es False → mismatch real
                await self.on_state_mismatch(symbol)
                logger.warning(f"[{symbol}] ⚠️ Watchdog: posición sin protección detectada (confirmado en exchange)")

            except Exception as e:
                logger.debug(f"KS watchdog tick error [{symbol}]: {e}")

        if self._level > 0:
            logger.warning(
                f"🛑 KS nivel {self._level} activo — trigger: {self._trigger} "
                f"(activado {time.strftime('%H:%M:%S', time.localtime(self._triggered_at or 0))})"
            )

    # ── Persistencia ──────────────────────────────────────────────────────────

    def _save_state(self):
        state = {
            "level":           self._level,
            "trigger":         self._trigger,
            "triggered_at":    self._triggered_at,
            "hard_killed":     self._hard_killed,
            "halted_symbols":  list(self._halted_symbols),
            "daily_pnl":       self._daily_pnl,
            "consec_losses":   self._consec_losses,
            "saved_at":        time.time(),
        }
        try:
            with open(_STATE_PATH, "w") as f:
                json.dump(state, f, indent=2)
        except Exception as e:
            logger.warning(f"KS _save_state: {e}")

    def _load_state(self):
        try:
            with open(_STATE_PATH) as f:
                state = json.load(f)
            self._level           = state.get("level", 0)
            self._trigger         = state.get("trigger", "")
            self._triggered_at    = state.get("triggered_at")
            self._hard_killed     = state.get("hard_killed", False)
            self._halted_symbols  = set(state.get("halted_symbols", []))
            self._daily_pnl       = state.get("daily_pnl", 0.0)
            self._consec_losses   = state.get("consec_losses", 0)
            if self._level > 0:
                logger.warning(
                    f"⚠️ KS arrancado con estado L{self._level} previo — trigger: {self._trigger}"
                )
        except FileNotFoundError:
            pass
        except Exception as e:
            logger.warning(f"KS _load_state: {e}")


# Singleton global
kill_switch = KillSwitch()
