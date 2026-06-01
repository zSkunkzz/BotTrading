# kill_switch.py – Patch Notes

## Bug 1 – TOCTOU in `on_trade_result()`

### ANTES
```python
def on_trade_result(self, pnl: float) -> None:
    if self._triggered:
        return
    self._consecutive_losses = ...
    if self._consecutive_losses >= self._max_losses:
        self._triggered = True   # race: another coroutine may have set this
```

### DESPUÉS
```python
async def on_trade_result(self, pnl: float) -> None:
    async with self._lock:
        if self._triggered:
            return
        self._consecutive_losses = ...
        if self._consecutive_losses >= self._max_losses:
            self._triggered = True
            log.critical("Kill switch activated")
```

---

## Bug 2 – `on_order_result()` counter never resets (sliding window)

### ANTES
```python
self._order_count += 1
if self._order_count > 200:
    self._triggered = True
```

### DESPUÉS
```python
# Use a deque-based sliding window (same pattern as pretrade_risk.py)
from collections import deque
self._order_timestamps: deque = deque()

async def on_order_result(self) -> None:
    async with self._lock:
        now = time.monotonic()
        cutoff = now - self._window_seconds   # e.g. 3600.0
        while self._order_timestamps and self._order_timestamps[0] < cutoff:
            self._order_timestamps.popleft()
        self._order_timestamps.append(now)
        if len(self._order_timestamps) > self._max_orders_per_window:  # e.g. 200
            self._triggered = True
            log.critical("Kill switch: order rate limit exceeded")
```

---

## Bug 3 – Railway persistence

Add the same warning as `state.py`:

```python
if os.getenv("RAILWAY_ENVIRONMENT"):
    log.warning("KillSwitch state is in-memory only on Railway – resets on redeploy.")
```
