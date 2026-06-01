# main.py – Patch Notes

## 1. global_risk=None → defensive initialisation

### ANTES
```python
global_risk = None
# ... much later ...
await global_risk.check(...)   # AttributeError if init fails
```

### DESPUÉS
```python
try:
    global_risk = GlobalRisk(
        max_daily_loss=float(os.getenv("MAX_DAILY_LOSS", 100)),
        ...
    )
except Exception as exc:
    log.critical("Failed to initialise GlobalRisk: %s", exc)
    raise SystemExit(1) from exc
```

---

## 2. Typo: USBC_PER_TRADE → USDC_PER_TRADE

### ANTES
```python
usdc_per_trade = float(os.getenv("USBC_PER_TRADE", 50))
```

### DESPUÉS
```python
usdc_per_trade = float(os.getenv("USDC_PER_TRADE", 50))
```
