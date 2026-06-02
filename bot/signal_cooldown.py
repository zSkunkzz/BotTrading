#!/usr/bin/env python3
"""
signal_cooldown.py — Cooldown diferenciado por entry_mode y cooldown dinámico
                      por pérdidas consecutivas en el mismo símbolo.

MEJORAS v2:
  - Cooldown escala con SL consecutivos por símbolo:
      1 SL  → cooldown normal (base)
      2 SL  → base × 2
      3+ SL → base × 4  (bot sospecha que el régimen cambió)
  - Se resetea al cerrar en TP1/TP2/TP3 (cierre ganador)
  - Ventana de "consecutivo": solo cuenta SLs sin TP intermedio

MEJORAS v3:
  - Cooldown MANUAL_CLOSE: evita reentrar en un símbolo recién cerrado a mano
      por defecto 600s (env: COOLDOWN_MANUAL_CLOSE). Bypass si override=True.
  - Cooldown TP_HIT reducido: tras TP, cooldown = base × 0.5 (ya existía como
      "timeout") ahora con reason='TP' explícito para claridad en logs.

MEJORAS v4 (PERSISTENCE):
  - Persistencia en JSON: _cooldown_until, _consecutive_sl y _manual_close_until
    se guardan en COOLDOWN_PERSIST_PATH tras cada write.
  - Al iniciar, se cargan del fichero. Las entradas expiradas se descartan.
  - Esto evita que un redeploy/crash en Railway resetee cooldowns activos.

Config Railway:
  COOLDOWN_EARLY          → cooldown base modo EARLY  (default 300s = 5min)
  COOLDOWN_NORMAL         → cooldown base modo NORMAL (default 180s = 3min)
  COOLDOWN_STRONG         → cooldown base modo STRONG (default 120s = 2min)
  COOLDOWN_SL_MULT        → multiplicador por SL (default 2.0 para 2º SL, 4.0 para 3º+)
  COOLDOWN_MANUAL_CLOSE   → cooldown tras cierre manual (default 600s = 10min)
  COOLDOWN_PERSIST_PATH   → ruta del fichero JSON de persistencia
                            (default /tmp/cooldown_state.json en Railway)
"""
from __future__ import annotations

import json
import logging
import os
import time
from typing import Dict

log = logging.getLogger(__name__)

COOLDOWN_BY_MODE: Dict[str, float] = {
    "EARLY":   float(os.getenv("COOLDOWN_EARLY",  "300")),
    "NORMAL":  float(os.getenv("COOLDOWN_NORMAL", "180")),
    "STRONG":  float(os.getenv("COOLDOWN_STRONG", "120")),
    "NONE":    float(os.getenv("COOLDOWN_NORMAL", "180")),
}
COOLDOWN_SL_MULT    = float(os.getenv("COOLDOWN_SL_MULT",      "2.0"))
COOLDOWN_MANUAL_SEC = float(os.getenv("COOLDOWN_MANUAL_CLOSE", "600"))
_PERSIST_PATH       = os.getenv("COOLDOWN_PERSIST_PATH", "/tmp/cooldown_state.json")


class SignalCooldown:
    """
    Rastrea el cooldown por símbolo con escalado dinámico por SL consecutivos.
    Incluye cooldown separado para cierres manuales (MANUAL_CLOSE).
    Estado persistido en JSON para sobrevivir reinicios de Railway.
    """

    def __init__(self) -> None:
        # {symbol: timestamp_hasta_el_que_está_en_cooldown}
        self._cooldown_until: Dict[str, float] = {}
        # {symbol: número de SL consecutivos sin TP intermedio}
        self._consecutive_sl: Dict[str, int] = {}
        # {symbol: timestamp de cierre manual}
        self._manual_close_until: Dict[str, float] = {}
        self._load()

    # ── Persistencia ──────────────────────────────────────────────────────────

    def _load(self) -> None:
        """Carga estado desde JSON. Descarta entradas expiradas."""
        try:
            with open(_PERSIST_PATH, "r") as f:
                data = json.load(f)
            now = time.time()
            self._cooldown_until = {
                k: v for k, v in data.get("cooldown_until", {}).items() if v > now
            }
            self._consecutive_sl = data.get("consecutive_sl", {})
            self._manual_close_until = {
                k: v for k, v in data.get("manual_close_until", {}).items() if v > now
            }
            active = len(self._cooldown_until) + len(self._manual_close_until)
            if active:
                log.info("[cooldown] Estado restaurado: %d cooldowns activos", active)
        except FileNotFoundError:
            pass
        except Exception as e:
            log.warning("[cooldown] No se pudo cargar estado persistido: %s", e)

    def _save(self) -> None:
        """Persiste estado actual en JSON."""
        try:
            data = {
                "cooldown_until":      self._cooldown_until,
                "consecutive_sl":      self._consecutive_sl,
                "manual_close_until":  self._manual_close_until,
                "saved_at":            time.time(),
            }
            with open(_PERSIST_PATH, "w") as f:
                json.dump(data, f)
        except Exception as e:
            log.warning("[cooldown] No se pudo persistir estado: %s", e)

    # ── Consultas ─────────────────────────────────────────────────────────────

    def is_in_cooldown(self, symbol: str) -> bool:
        """True si el símbolo está en cooldown (SL/TP/TIMEOUT o MANUAL_CLOSE)."""
        if time.time() < self._cooldown_until.get(symbol, 0.0):
            return True
        if time.time() < self._manual_close_until.get(symbol, 0.0):
            return True
        return False

    def remaining(self, symbol: str) -> float:
        """Segundos restantes del cooldown más largo activo (0 si expiró)."""
        sl_tp_rem  = max(0.0, self._cooldown_until.get(symbol, 0.0) - time.time())
        manual_rem = max(0.0, self._manual_close_until.get(symbol, 0.0) - time.time())
        return max(sl_tp_rem, manual_rem)

    def is_manual_close_cooldown(self, symbol: str) -> bool:
        """True si el símbolo está específicamente en cooldown por cierre manual."""
        until = self._manual_close_until.get(symbol, 0.0)
        if time.time() < until:
            return True
        self._manual_close_until.pop(symbol, None)
        return False

    # ── Registro de cierres ───────────────────────────────────────────────────

    def mark_manual_close(self, symbol: str) -> None:
        """
        Llamar cuando se detecta cierre manual (posición desaparece sin SL/TP hit).
        Bloquea nuevas entradas durante COOLDOWN_MANUAL_CLOSE segundos.
        """
        until = time.time() + COOLDOWN_MANUAL_SEC
        self._manual_close_until[symbol] = until
        log.warning(
            "[cooldown] %s: MANUAL_CLOSE → cooldown %.0fs (hasta %s)",
            symbol, COOLDOWN_MANUAL_SEC,
            time.strftime("%H:%M:%S", time.localtime(until)),
        )
        self._save()

    def mark_closed(self, symbol: str, reason: str, entry_mode: str = "NORMAL") -> None:
        """
        Llamar al cerrar una posición por SL, TP o timeout.

        reason: 'SL' | 'TP1' | 'TP2' | 'TP3' | 'TP' | 'TIMEOUT'
        """
        # Limpiar cooldown manual si existía (cierre ya registrado correctamente)
        self._manual_close_until.pop(symbol, None)

        if reason == "SL":
            self._consecutive_sl[symbol] = self._consecutive_sl.get(symbol, 0) + 1
            n_sl = self._consecutive_sl[symbol]

            base = COOLDOWN_BY_MODE.get(entry_mode, COOLDOWN_BY_MODE["NORMAL"])
            if n_sl >= 3:
                cooldown = base * 4.0
                log.warning(
                    "[cooldown] %s: %d SL consecutivos → cooldown EXTENDIDO %.0fs",
                    symbol, n_sl, cooldown,
                )
            elif n_sl == 2:
                cooldown = base * COOLDOWN_SL_MULT
                log.info(
                    "[cooldown] %s: 2º SL consecutivo → cooldown x%.1f = %.0fs",
                    symbol, COOLDOWN_SL_MULT, cooldown,
                )
            else:
                cooldown = base
                log.info("[cooldown] %s: SL → cooldown base %.0fs", symbol, cooldown)
        else:
            # TP o timeout → reset contador consecutivo, cooldown reducido
            self._consecutive_sl[symbol] = 0
            cooldown = COOLDOWN_BY_MODE.get(entry_mode, COOLDOWN_BY_MODE["NORMAL"]) * 0.5
            log.info(
                "[cooldown] %s: %s → cooldown %.0fs (reset SL consecutivos)",
                symbol, reason, cooldown,
            )

        self._cooldown_until[symbol] = time.time() + cooldown
        self._save()

    def reset(self, symbol: str) -> None:
        """Forzar reset completo (para tests o admin manual)."""
        self._cooldown_until.pop(symbol, None)
        self._consecutive_sl.pop(symbol, None)
        self._manual_close_until.pop(symbol, None)
        self._save()

    def consecutive_sl(self, symbol: str) -> int:
        """Número de SL consecutivos actuales para el símbolo."""
        return self._consecutive_sl.get(symbol, 0)


# Instancia global
signal_cooldown = SignalCooldown()
