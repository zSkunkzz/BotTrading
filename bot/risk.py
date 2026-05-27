import logging

logger = logging.getLogger("Risk")

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
        self.open_trades = 0
        self.entry_price = None
        self.side = None
        self.peak_pnl = 0.0

    def can_open_trade(self, balance):
        if self.open_trades >= self.max_open_trades:
            return False, f"Max trades ({self.max_open_trades}) alcanzado"
        if self.daily_pnl <= -self.max_daily_loss_pct:
            return False, f"Daily loss limit {self.max_daily_loss_pct}% alcanzado"
        if balance < self.usdt_per_trade:
            return False, f"Balance insuficiente: {balance:.2f} USDT"
        return True, "OK"

    def on_trade_open(self, entry_price, side):
        self.open_trades += 1
        self.entry_price = entry_price
        self.side = side
        self.peak_pnl = 0.0

    def on_trade_close(self, pnl_pct):
        self.open_trades = max(0, self.open_trades - 1)
        self.daily_pnl += pnl_pct
        self.entry_price = None
        self.side = None
        self.peak_pnl = 0.0

    def check_exit(self, current_price):
        if not self.entry_price or not self.side:
            return False, ""
        if self.side == "long":
            pnl = (current_price - self.entry_price) / self.entry_price * 100
        else:
            pnl = (self.entry_price - current_price) / self.entry_price * 100
        if pnl >= self.tp_pct:
            return True, f"TP +{pnl:.2f}%"
        if pnl <= -self.sl_pct:
            return True, f"SL {pnl:.2f}%"
        if self.trailing_sl and pnl >= self.trailing_activation_pct:
            self.peak_pnl = max(self.peak_pnl, pnl)
            if self.peak_pnl - pnl >= self.trailing_callback_pct:
                return True, f"Trailing SL (peak {self.peak_pnl:.2f}% → {pnl:.2f}%)"
        return False, ""
