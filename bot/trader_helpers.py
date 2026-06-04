"""
bot/trader_helpers.py — Pure helper functions extracted from FuturesTrader.

These functions have no side-effects and carry no exchange state, which makes
them easy to unit-test in isolation.

Exported:
  _check_price_staleness(signal, current_price, is_long) -> str | None
  _adjust_levels_to_fill(signal, fill_price, signal_entry) -> tuple[float, float, float]
"""
from __future__ import annotations

# ── Staleness thresholds ──────────────────────────────────────────────────────
# Max relative drift (vs. signal entry) before we refuse to enter.
_STALE_DRIFT_PCT: float = 0.025   # 2.5 % one-directional tolerance
_STALE_ABS_PCT:   float = 0.08    # 8 % absolute cap in either direction


def _check_price_staleness(
    signal: dict,
    current_price: float,
    is_long: bool,
) -> str | None:
    """
    Return a human-readable reason string if *current_price* is too far from
    the signal's entry price to be worth entering, else return ``None``.

    Rules (symmetric for long/short):
    - Entry missing or zero  → skip silently (returns None, does not block).
    - Adverse drift > STALE_DRIFT_PCT → skip.
    - |drift| > STALE_ABS_PCT in either direction → skip.
    """
    entry = float(signal.get("entry") or 0)
    if entry <= 0:
        return None  # Cannot validate without an entry price; allow trade through.

    drift = (current_price - entry) / entry  # positive = price rose

    # Absolute cap: refuse if price has moved > 8 % in any direction.
    if abs(drift) > _STALE_ABS_PCT:
        return (
            f"absolute drift {drift:+.2%} exceeds cap ±{_STALE_ABS_PCT:.0%}; "
            f"entry={entry} current={current_price}"
        )

    # Directional tolerance: for longs price must not be too far above entry;
    # for shorts price must not be too far below entry.
    if is_long and drift > _STALE_DRIFT_PCT:
        return (
            f"long signal stale: price {current_price} is {drift:+.2%} above "
            f"entry {entry} (max +{_STALE_DRIFT_PCT:.1%})"
        )
    if not is_long and drift < -_STALE_DRIFT_PCT:
        return (
            f"short signal stale: price {current_price} is {drift:+.2%} below "
            f"entry {entry} (max -{_STALE_DRIFT_PCT:.1%})"
        )

    # Also reject if price has moved adversely past the entry in the wrong direction.
    if is_long and drift < -_STALE_DRIFT_PCT:
        return (
            f"long signal stale: price {current_price} dropped {drift:+.2%} below "
            f"entry {entry}"
        )
    if not is_long and drift > _STALE_DRIFT_PCT:
        return (
            f"short signal stale: price {current_price} rose {drift:+.2%} above "
            f"entry {entry}"
        )

    return None


# ── Fill-price rescaling ──────────────────────────────────────────────────────
_RESCALE_MIN_DRIFT: float = 0.001  # ignore sub-0.1 % drift (round-trip noise)


def _adjust_levels_to_fill(
    signal: dict,
    fill_price: float,
    signal_entry: float,
) -> tuple[float, float, float]:
    """
    Rescale SL/TP1/TP2 proportionally when the actual fill price differs from
    the signal's entry price.

    If the entry recorded in ``signal`` is zero (edge-case), ``signal_entry``
    is used as the reference instead so we don't divide by zero.

    Returns (sl, tp1, tp2) as floats.
    """
    sl   = float(signal.get("sl")  or 0)
    tp1  = float(signal.get("tp1") or 0)
    tp2  = float(signal.get("tp2") or 0)

    ref = float(signal.get("entry") or 0) or signal_entry
    if ref <= 0 or fill_price <= 0:
        return sl, tp1, tp2

    drift = abs(fill_price - ref) / ref
    if drift < _RESCALE_MIN_DRIFT:
        return sl, tp1, tp2

    # Rescale: preserve the *proportional* distance of each level from entry.
    #   new_level = fill + (old_level - ref) / ref * fill
    def _rescale(level: float) -> float:
        if level <= 0:
            return level
        ratio = (level - ref) / ref
        return fill_price * (1.0 + ratio)

    return _rescale(sl), _rescale(tp1), _rescale(tp2)


__all__ = ["_check_price_staleness", "_adjust_levels_to_fill"]
