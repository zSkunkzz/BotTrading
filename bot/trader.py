#!/usr/bin/env python3
"""
bot/trader.py — Re-export canónico de FuturesTrader.

Historia:
  Este fichero fue generado como PLACEHOLDER durante una refactorización
  y debía ser reemplazado por CI antes del deploy. El CI no siempre lo
  reemplaza, lo que causaba un ImportError fatal al arrancar el bot.

Fix (Bug 0):
  FuturesTrader es un alias de AiTrader, que acepta exactamente la misma
  firma que main.py espera:

      FuturesTrader(
          api_key      = str | None,
          api_secret   = str,
          passphrase   = None,
          symbol       = str,
          leverage     = int,
          margin_mode  = str,
          dry_run      = bool,
      )

  Si en el futuro se crea una clase FuturesTrader independiente, basta
  con sustituir este import por el nuevo módulo.
"""
from bot.ai_trader import AiTrader as FuturesTrader  # noqa: F401

__all__ = ["FuturesTrader"]
