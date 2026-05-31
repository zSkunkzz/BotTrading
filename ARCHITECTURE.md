# BotTrading — Arquitectura

## Estructura final de módulos

```
BotTrading/
├── main.py                        # Punto de entrada: orquesta scanner, traders, webhook
├── webhook.py                     # Servidor HTTP aiohttp (alertas externas)
├── Procfile                       # Railway: worker: python main.py
├── railway.toml
├── requirements.txt
├── .env.example
├── ARCHITECTURE.md
└── bot/
    ├── trader.py                  # FuturesTrader — punto de entrada público (compatibilidad)
    ├── telegram_bot.py            # Notificaciones Telegram
    ├── kill_switch.py             # Kill Switch Watchdog
    ├── backtester.py              # Backtesting sobre histórico
    │
    ├── core/                      # 🔵 Loop de trading, decisión, posición, HTTP
    │   ├── trading_loop.py        # TradingLoop — _iteration(), sinc con exchange
    │   ├── decision_engine.py     # DecisionEngine — evalúa entradas
    │   ├── position_manager.py    # PositionManager — TP2, trailing, SL/TP cierre
    │   └── http_client.py         # HyperliquidHTTPClient — signing, throttle, nonce
    │
    ├── analysis/                  # 🟢 Análisis técnico y señales
    │   ├── signal_engine.py       # Motor de señales multi-timeframe (22KB)
    │   ├── strategy.py            # decide() — lógica de decisión
    │   ├── indicators.py          # Cálculo de indicadores (RSI, EMA, MACD...)
    │   ├── microstructure.py      # Análisis de order book
    │   └── data_enricher.py       # Contexto externo: Fear&Greed, OI, funding, noticias
    │
    ├── execution/                 # 🟠 Ejecución de órdenes
    │   └── execution_engine.py    # Limit→timeout→market fallback, slippage tracking
    │
    ├── risk/                      # 🔴 Gestión de riesgo (punto unificado)
    │   └── risk_manager.py        # RiskManager + GlobalRiskManager + PreTradeRiskChecker
    │
    ├── ai/                        # 🤖 Capa IA: decisión, filtrado, rate limiting
    │   ├── ai_trader.py           # ai_decide() — LLM para trade final
    │   ├── ai_filter.py           # ai_rank_pairs() — ranking IA de pares
    │   └── ai_rate_limiter.py     # Throttle global de llamadas IA
    │
    ├── infra/                     # ⚪ Infraestructura
    │   ├── balance_service.py     # Balance USDC con caché
    │   ├── ohlcv_cache.py         # Caché de velas OHLCV
    │   ├── pair_scanner.py        # Escaneo y scoring de pares
    │   └── ws_feed.py             # WebSocket feed en tiempo real
    │
    └── utils/                     # ⚪ Utilidades
        ├── state.py               # Persistencia de posiciones en disco
        ├── logger.py              # Setup de logging
        └── shadow_mode.py         # Dry-run sin dinero real
```

## Archivos eliminados

| Archivo | Razón |
|---|---|
| `bot/trader_run_patch.py` | Autodescrito como eliminado; lanzaba `ImportError` |
| `ai_trader_patch.py` | Instrucciones de merge ya integradas en `bot/ai/ai_trader.py` |
| `strategy_patch.py` | `EnricherCache` absorbida en `bot/analysis/data_enricher.py` |

## Flujo de datos

```
PairScanner (infra) → ai_rank_pairs (ai) → final_pairs
        ↓
WSFeed (infra) ← precios tick-by-tick
        ↓
FuturesTrader.run()
  └── TradingLoop._iteration() (core)
        ├── [sin posición] DecisionEngine.evaluate() (core)
        │     ├── PreTradeRiskChecker.check() (risk)
        │     ├── decide() → SignalEngine (analysis)
        │     ├── ai_decide() (ai)
        │     └── ExecutionEngine.execute() (execution) → orden con SL/TP real
        │
        └── [con posición] PositionManager.manage() (core)
              ├── TP2 parcial
              ├── Trailing stop update
              └── SL / TP1 / TP3 → cierre + notify (Telegram)
```

## Capas y responsabilidades

| Capa | Color | Descripción |
|---|---|---|
| `core/` | 🔵 | Loop principal, decisión, posición, HTTP signing |
| `analysis/` | 🟢 | Señales, estrategia, indicadores, microestructura, datos externos |
| `execution/` | 🟠 | Ejecución de órdenes con fallback y tracking |
| `risk/` | 🔴 | Riesgo por símbolo, global y pre-trade |
| `ai/` | 🤖 | Filtrado IA, decisión LLM, rate limiting |
| `infra/` | ⚪ | Balance, OHLCV, scanner, WebSocket |
| `utils/` | ⚪ | Estado en disco, logging, dry-run |

## Módulos de riesgo

| Clase | Scope | Descripción |
|---|---|---|
| `risk/risk_manager.py:RiskManager` | Por símbolo | USDC por trade, TP/SL %, trailing |
| `risk/risk_manager.py:GlobalRiskManager` | Portfolio | Max trades concurrentes, pérdida diaria |
| `risk/risk_manager.py:PreTradeRiskChecker` | Pre-orden | Balance disponible, posición duplicada |
| `kill_switch.py` | Sistema | Halt ante drawdown severo |

## Compatibilidad hacia atrás

Todos los módulos originales en `bot/` raíz se mantienen **intactos**.
Los nuevos paquetes en `bot/core/`, `bot/analysis/`, `bot/execution/`,
`bot/risk/`, `bot/ai/`, `bot/infra/` y `bot/utils/` son **aditivos**.
`main.py` y `bot/trader.py` no necesitan cambios para seguir funcionando.
