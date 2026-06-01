# Integration Guide – Patches v4

This guide shows **exactly where** to call the new lifecycle hooks in your trading loop.

---

## 1. After order placement is confirmed by the exchange

```python
# In trader.py / trading_loop.py – after exchange.create_order() succeeds:

order = await exchange.create_order(...)
if order and order.get("id"):  # exchange accepted it
    await decision_engine.on_order_confirmed(
        symbol=symbol,
        margin=usdc_per_trade,   # same value used in evaluate()
    )
```

---

## 2. After a position is fully closed

```python
# In position_manager.py / trader.py – after SL/TP/manual close is confirmed:

await decision_engine.on_position_closed(
    symbol=symbol,
    margin=position["_margin"],  # stored in enriched signal
)
await state.clear_position()
```

---

## 3. Decision gate in the main signal loop

```python
# Before placing any order:

approved, reason, enriched_signal = await decision_engine.evaluate(
    symbol=symbol,
    signal=raw_signal,
    price=current_price,
)
if not approved:
    log.info("Trade skipped [%s]: %s", symbol, reason)
    continue

# … place order with enriched_signal …
```

---

## 4. state.py – side field

The `side` field is now stored as `None` (not `""`) when absent.
Always check:

```python
pos = await state.get_position()
if pos and pos.get("side"):   # None-safe
    ...
```

---

## 5. Railway ephemeral FS

Set the `STATE_FILE` env var to a persistent volume mount, e.g.:

```
STATE_FILE=/vol/data/bot_state.json
```

Otherwise state is lost on every redeploy.
