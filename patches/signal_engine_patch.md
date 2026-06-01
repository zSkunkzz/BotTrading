# signal_engine.py – Patch Notes

## Missing `_closes_15m` and `_closes_1h` in indicators dict

### ANTES
```python
indicators = {
    "rsi": rsi,
    "ema_fast": ema_fast,
    "ema_slow": ema_slow,
    # ... other keys ...
}
```

### DESPUÉS
```python
indicators = {
    "rsi": rsi,
    "ema_fast": ema_fast,
    "ema_slow": ema_slow,
    # ... other keys ...
    "closes_15m": self._closes_15m,   # list[float] – last N 15-min closes
    "closes_1h":  self._closes_1h,    # list[float] – last N 1h closes
}
```

Make sure `self._closes_15m` and `self._closes_1h` are populated in
`_update_ohlcv()` (or equivalent) before `_build_indicators()` is called.
