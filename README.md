# Bot Hyperliquid Auto‑Optimizado

## Cambios principales
- **R:R adaptativo** según régimen, ADX y volatilidad.
- **SL_MIN_PCT = 1.2%** (reducción de stops por ruido).
- **Régimen bull/bear solo si ADX ≥ 22** (más selectivo).
- **Auto‑optimizador cada 72h** que ajusta `MIN_SCORE`, `MARGIN_USDT`, `SHORT_MIN_SCORE_EXTRA`, `SL_MIN_PCT` y más, basado en el historial real de trades.

## Archivos modificados
- `main.py` – integración del optimizador.
- `risk.py` – SL_MIN_PCT y R:R adaptativo.
- `signals.py` – régimen endurecido y pesos rebalanceados.
- `config.py` – nuevas variables (`SHORT_MIN_SCORE_EXTRA`, `BLACKLIST_SYMBOLS`).
- `optimizer.py` – nuevo módulo (añadir al proyecto).
- `weights.json` – se genera automáticamente (puede editarse manualmente).

## Variables de entorno necesarias
- `GIST_TOKEN` y `GIST_ID` para leer el historial de trades.
- `TELEGRAM_TOKEN` y `TELEGRAM_CHAT_ID` para notificaciones.
- El resto de variables habituales (`HYPERLIQUID_PRIVATE_KEY`, etc.) siguen siendo necesarias.

## Puesta en marcha
1. Coloca todos los archivos en tu proyecto.
2. Asegúrate de que `optimizer.py` esté en el mismo directorio que `main.py`.
3. Ejecuta `python main.py` normalmente.
4. El optimizador se ejecutará automáticamente cada 3 días (o al arrancar si no hay registro previo).

## Monitoreo
- Recibirás un mensaje en Telegram cada vez que el optimizador aplique cambios.
- Puedes forzar una optimización manual con `optimizer.optimize()` desde una consola interactiva.