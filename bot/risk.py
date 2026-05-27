import logging
from datetime import date

logger = logging.getLogger("RiskManager")


class RiskManager:
    def __init__(self, usdt_per_trade, tp_pct, sl_pct,
                 trailing_sl, trailing_activation_pct, trailing_callback_pct,
                 max_daily_loss_pct, max_open_trades):
        self.usdt_per_trade = usdt_per_trade
        self.tp_pct = tp_pct
        self.sl_pct = sl_pct
        self.trailing_sl = trailing_sl
        self.trailing_activation_pct = trailing_activation_pct
        self.trailing_callback_pct = trailing_callback_pct
        self.max_daily_loss_pct = max_daily_loss_pct
        self.max_open_trades = max_open_trades

        self.daily_pnl = 0.0
        self.daily_date = date.today()
        self.open_trades = 0
        self.trailing_high = None
        self.trailing_low  = None
        self.trailing_active = False

    def _reset_daily_if_needed(self):
        today = date.today()
        if today != self.daily_date:
            logger.info(f"Nuevo dia - PnL diario reset. Ayer: {self.daily_pnl:.2f}%")
            self.daily_pnl = 0.0
            self.daily_date = today

    def can_open_trade(self, balance_usdt: float) -> tuple:
        self._reset_daily_if_needed()
        if self.open_trades >= self.max_open_trades:
            return False, f"Maximo trades abiertos ({self.max_open_trades})"
        if self.daily_pnl <= -self.max_daily_loss_pct:
            return False, f"Limite perdida diaria ({self.daily_pnl:.2f}%)"
        if self.usdt_per_trade > balance_usdt * 0.95:
            return False, f"Balance insuficiente: {balance_usdt:.2f} USDT"
        return True, "OK"

    def on_trade_open(self, entry_price: float, side: str):
        self.open_trades += 1
        self.trailing_high = entry_price
        self.trailing_low  = entry_price
        self.trailing_active = False

    def on_trade_close(self, pnl_pct: float):
        self.open_trades = max(0, self.open_trades - 1)
        self.daily_pnl += pnl_pct
        self.trailing_high = None
        self.trailing_low  = None
        self.trailing_active = False

    def check_exit(self, entry_price: float, current_price: float, side: str) -> tuple:
        if side == "long":
            pnl = (current_price - entry_price) / entry_price * 100
        else:
            pnl = (entry_price - current_price) / entry_price * 100

        if pnl >= self.tp_pct:
            return True, f"TP +{pnl:.2f}%"
        if pnl <= -self.sl_pct:
            return True, f"SL {pnl:.2f}%"

        if self.trailing_sl:
            if side == "long":
                if current_price > (self.trailing_high or entry_price):
                    self.trailing_high = current_price
                best_pnl = (self.trailing_high - entry_price) / entry_price * 100
                if best_pnl >= self.trailing_activation_pct:
                    self.trailing_active = True
                if self.trailing_active:
                    dd = (self.trailing_high - current_price) / self.trailing_high * 100
                    if dd >= self.trailing_callback_pct:
                        return True, f"Trailing SL (retroceso {dd:.2f}%)"
            else:
                if current_price < (self.trailing_low or entry_price):
                    self.trailing_low = current_price
                best_pnl = (entry_price - self.trailing_low) / entry_price * 100
                if best_pnl >= self.trailing_activation_pct:
                    self.trailing_active = True
                if self.trailing_active:
                    dd = (current_price - self.trailing_low) / self.trailing_low * 100
                    if dd >= self.trailing_callback_pct:
                        return True, f"Trailing SL (subida {dd:.2f}%)"

        return False, None
