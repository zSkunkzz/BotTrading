# BotTrading — Arquitectura

## Estructura de módulos

```
bot/
├── core/                     # Loop de trading, decisión, gestión de posición
│   ├── trading_loop.py       # TradingLoop — orquesta _iteration()
│   ├── decision_engine.py    # DecisionEngine — evalúa entradas (extraído de trader.py)
│   ├── position_manager.py   # PositionManager — TP parcial, trailing, SL/TP cierre
│   └── http_client.py        # HyperliquidHTTPClient — signing, throttle, nonce
│
├── analysis/                 # Análisis técnico y señales
│   ├── signal_engine.py      # Motor de señales multi-timeframe
│   ├── strategy.py           # Lógica de decisión (decide())
│   ├── microstructure.py     # Análisis de order book
│   └── indicators.py         # Cálculo de indicadores
│
├── execution/                # Ejecución de órdenes
│   └── execution_engine.py   # Limit→timeout→market fallback, slippage tracking
│
├── risk/                     # Gestión de riesgo (punto de entrada unificado)
│   └── risk_manager.py       # RiskManager + GlobalRiskManager + PreTradeRiskChecker
│
├── infra/                    # Infraestructura
│   ├── balance_service.py    # Balance USDC con caché
│   ├── ohlcv_cache.py        # Caché de velas OHLCV
│   ├── pair_scanner.py       # Escaneo y scoring de pares
│   └── notifier.py           # Notificaciones básicas
│
├── utils/                    # Utilidades
│   ├── state.py              # Persistencia de posiciones en disco
│   ├── logger.py             # Setup de logging
│   └── shadow_mode.py        # Dry-run sin dinero real
│
├── trader.py                 # FuturesTrader — punto de entrada público (compatibilidad)
├── ws_feed.py                # WebSocket feed en tiempo real
├── kill_switch.py            # Kill Switch Watchdog
├── telegram_bot.py           # Notificaciones Telegram
├── ai_trader.py              # Integración IA (Gemini/OpenRouter)
├── ai_filter.py              # Filtrado IA de pares
├── backtester.py             # Backtesting sobre histórico
└── balance_service.py        # (legacy — usar infra/balance_service.py)
```

## Flujo de datos

```
PairScanner → ai_rank_pairs → final_pairs
     ↓
WSFeed (WebSocket tiempo real) ← precios tick-by-tick
     ↓
FuturesTrader.run()
  └── TradingLoop._iteration()
        ├── [sin posición] DecisionEngine.evaluate()
        │     ├── pretrade_risk.check()
        │     ├── decide() → signal_engine → strategy
        │     ├── ai_decide() (si score >= umbral)
        │     └── ExecutionEngine.execute() → orden con SL/TP real
        │
        └── [con posición] PositionManager.manage()
              ├── TP2 parcial
              ├── Trailing stop update
              └── SL / TP1 / TP3 → cierre
```

## Módulos de riesgo

| Módulo | Scope | Descripción |
|--------|-------|-------------|
| `risk/risk_manager.py:RiskManager` | Por símbolo | USDC por trade, TP/SL%, trailing |
| `risk/risk_manager.py:GlobalRiskManager` | Portfolio | Max trades concurrentes, pérdida diaria máxima |
| `risk/risk_manager.py:PreTradeRiskChecker` | Pre-orden | Balance, posición duplicada |
| `kill_switch.py` | Sistema | Halt ante drawdown severo |

## Compatibilidad hacia atrás

Los módulos originales (`bot/risk.py`, `bot/pretrade_risk.py`, `bot/global_risk.py`)
se mantienen intactos. `bot/trader.py` sigue siendo el punto de entrada para `main.py`.
Los nuevos módulos en `bot/core/` y `bot/risk/` son aditivos — no rompen nada existente.
