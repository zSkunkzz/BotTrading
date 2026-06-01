# position_manager.py – Patch Notes

## Bug: ZeroDivisionError in `_calc_partial_qty_safe()` during TP2 partial close

When `entry_price` is 0.0 (state restored with missing data), dividing
position size by entry price raises `ZeroDivisionError`.

### ANTES
```python
def _calc_partial_qty(self, pos: dict, pct: float) -> float:
    notional = pos["qty"] * pos["entry_price"]
    return notional * pct / pos["entry_price"]
```

### DESPUÉS
```python
def _calc_partial_qty(self, pos: dict, pct: float) -> float:
    entry = float(pos.get("entry_price") or 0)
    if entry <= 0:
        log.error(
            "[PositionManager] entry_price is 0 for %s – cannot compute partial qty",
            pos.get("symbol"),
        )
        return 0.0
    notional = float(pos["qty"]) * entry
    return notional * pct / entry
```

Also add a guard before calling this function:

```python
partial_qty = self._calc_partial_qty(pos, 0.5)
if partial_qty <= 0:
    log.warning("Skipping TP2 partial close – qty is zero")
    return
```
