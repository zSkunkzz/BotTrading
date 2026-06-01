# Mejoras v4 — Bot de Trading Automático

Commit de mejoras de rentabilidad. Todos los módulos son **opt-in via variables Railway**.

---

## Módulos nuevos

### 1. `bot/market_regime.py` — Filtro régimen BTC
Semáforo global basado en ADX + EMA trend de BTC/1h.
- `GREEN` → operar con normalidad
- `YELLOW` → operar con cautela (size reducido)
- `RED` → pausar nuevas entradas

**Config Railway:**
```
REGIME_FILTER=true
REGIME_ADX_MIN=18
REGIME_CACHE_TTL=300
```

**Integración en `decision_engine.py`:**
```python
from bot.market_regime import market_regime
await market_regime.refresh(exch)
if not market_regime.is_tradeable():
    return  # pausar
```

---

### 2. `bot/daily_drawdown.py` — Drawdown diario
Bloquea nuevas entradas si las pérdidas del día superan el límite.
Reset automático a las 00:00 UTC.

**Config Railway:**
```
MAX_DAILY_DRAWDOWN_PCT=5.0
DRAWDOWN_RESET_HOUR_UTC=0
```

**Integración:**
```python
from bot.daily_drawdown import daily_drawdown
# Al cerrar un trade:
daily_drawdown.record_trade(pnl_usd)
# Antes de abrir:
if daily_drawdown.is_blocked():
    return
```

---

### 3. `bot/kelly_sizer.py` — Kelly Criterion sizing
Multiplicador de sizing basado en win rate histórico por entry_mode y R/R del trade.
Desactivado por defecto hasta tener ≥30 trades por modo.

**Config Railway:**
```
KELLY_ENABLED=false      # activar cuando haya historial
KELLY_FRACTION=0.25      # quarter-Kelly (conservador)
KELLY_MIN_MULT=0.5
KELLY_MAX_MULT=2.0
KELLY_MIN_TRADES=30
```

**Integración:**
```python
from bot.kelly_sizer import kelly_multiplier
mult = kelly_multiplier(entry_mode, rr)
notional = usdc_per_trade * mult * leverage
```

---

### 4. `bot/structure_analyzer.py` — Break of Structure (BOS)
Detecta Higher Highs/Lows y Break of Structure con confirmación de volumen.
Añade hasta +2 puntos al score de la señal.

**Config Railway:**
```
STRUCTURE_ENABLED=true
STRUCTURE_SWING_N=5
STRUCTURE_VOL_CONFIRM=true
```

**Integración en `signal_engine.analyze_pair()`:**
```python
from bot.structure_analyzer import analyze_structure
struct = analyze_structure(df1h, direction=1 if direction=="LONG" else -1)
score = min(score + struct["score"], SCORE_MAX)
```

---

### 5. `bot/position_timeout.py` — Timeout de posición
Cierra automáticamente posiciones que llevan >24h sin avanzar hacia TP1.

**Config Railway:**
```
POSITION_TIMEOUT_ENABLED=true
POSITION_TIMEOUT_HOURS=24
POSITION_MIN_MOVE_PCT=0.3
```

**Integración en el loop de gestión de posiciones:**
```python
from bot.position_timeout import should_timeout
close_it, reason = should_timeout(
    symbol, entry, current_price, tp1, side, opened_at, tp1_hit
)
if close_it:
    await close_position(symbol, reason=reason)
```

---

### 6. `bot/correlation_guard.py` — Guarda de correlación
Limita el número de posiciones en la misma dirección y el total de abiertas.
Penaliza el size si BTC va contra la dirección propuesta.

**Config Railway:**
```
CORR_ENABLED=true
CORR_MAX_SAME_DIR=3
CORR_MAX_OPEN=5
```

**Integración:**
```python
from bot.correlation_guard import check_correlation, size_penalty_btc
ok, reason = check_correlation(direction, open_positions)
if not ok:
    return
penalty = size_penalty_btc(direction, market_regime.btc_trend())
notional *= penalty
```

---

### 7. `bot/trailing_hl.py` — Trailing stop nativo HL
Coloca y actualiza órdenes de trailing stop directamente en Hyperliquid.
Si el bot crashea, la posición sigue protegida por el exchange.

**Config Railway:**
```
TRAILING_HL_ENABLED=true
TRAILING_ACTIVATION_PCT=1.0    # activar tras 1% de ganancia
TRAILING_CALLBACK_PCT=1.5      # 1.5% de retroceso desde máximo
```

**Integración:**
```python
from bot.trailing_hl import trailing_hl
# Al abrir:
trailing_hl.on_position_open(symbol, entry, side)
# En cada tick/vela:
new_trail = await trailing_hl.update(symbol, current_price, exch, size)
# Al cerrar:
trailing_hl.on_position_close(symbol)
```

---

### 8. `bot/auto_backtest.py` — Backtest automático
Valida los parámetros actuales contra las últimas N velas históricas.
Se ejecuta al arranque y cada 24h. Envía resumen por Telegram si el WR baja del mínimo.

**Config Railway:**
```
AUTO_BACKTEST_ENABLED=false    # activar cuando quieras validación continua
AUTO_BACKTEST_HOURS=24
AUTO_BACKTEST_CANDLES=500
AUTO_BACKTEST_MIN_WR=0.45
```

**Integración:**
```python
from bot.auto_backtest import auto_backtest
await auto_backtest.maybe_run(exch, symbols=["BTC", "ETH", "SOL"])
```

---

## Variables Railway — resumen completo

```bash
# Régimen BTC
REGIME_FILTER=true
REGIME_ADX_MIN=18

# Drawdown diario
MAX_DAILY_DRAWDOWN_PCT=5.0

# Kelly sizing
KELLY_ENABLED=false
KELLY_FRACTION=0.25
KELLY_MIN_TRADES=30

# Estructura de mercado
STRUCTURE_ENABLED=true

# Timeout posición
POSITION_TIMEOUT_ENABLED=true
POSITION_TIMEOUT_HOURS=24

# Correlación
CORR_ENABLED=true
CORR_MAX_SAME_DIR=3
CORR_MAX_OPEN=5

# Trailing HL nativo
TRAILING_HL_ENABLED=true
TRAILING_ACTIVATION_PCT=1.0
TRAILING_CALLBACK_PCT=1.5

# Backtest automático
AUTO_BACKTEST_ENABLED=false
```
