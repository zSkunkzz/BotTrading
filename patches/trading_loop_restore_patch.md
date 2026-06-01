# trading_loop.py – Position Restore Patch

## Problem
On restart the bot loads `state.get_position()` but does NOT restore:
- `sl` (stop-loss price)
- `tp1`, `tp2`, `tp3` (take-profit levels)
- `_open_notional` (used by pretrade_risk open-margin accounting)

This means the kill-switch and pretrade_risk ledger start at zero even though
a real position is open on the exchange.

---

## Fix – in `_restore_position()` (or wherever startup restore happens)

```python
async def _restore_position(self) -> None:
    pos = await self.state.get_position()
    if pos is None:
        return

    symbol   = pos["symbol"]
    side     = pos.get("side")
    entry    = float(pos.get("entry_price", 0))
    qty      = float(pos.get("qty", 0))
    margin   = float(pos.get("_margin", self._usdc_per_trade))

    # --- restore SL/TP from saved state, else recalculate ---
    sl  = pos.get("sl")
    tp1 = pos.get("tp1")
    tp2 = pos.get("tp2")
    tp3 = pos.get("tp3")

    if not sl or not tp1:
        log.warning("[Restore] SL/TP missing from saved state – recalculating")
        # recalculate using your risk manager
        sl, tp1, tp2, tp3 = self._risk_manager.calc_levels(
            side=side, entry=entry, qty=qty
        )
        await self.state.update_position(sl=sl, tp1=tp1, tp2=tp2, tp3=tp3)

    # --- restore pretrade_risk open-margin ledger ---
    await self.decision_engine.on_order_confirmed(symbol=symbol, margin=margin)

    log.info(
        "[Restore] position %s %s entry=%.4f sl=%.4f tp1=%.4f margin=%.2f",
        symbol, side, entry, sl, tp1, margin,
    )
```

---

## Fix – save SL/TP when opening a position

Make sure these fields are stored when you call `state.set_position()`:

```python
await self.state.set_position({
    "symbol":      symbol,
    "side":        side,
    "entry_price": entry_price,
    "qty":         qty,
    "sl":          sl,
    "tp1":         tp1,
    "tp2":         tp2,
    "tp3":         tp3,
    "_margin":     usdc_per_trade,
})
```
