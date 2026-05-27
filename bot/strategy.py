import logging
from bot.indicators import ema, rsi, macd, supertrend

logger = logging.getLogger("Strategy")


class MultiStrategy:
    def __init__(self, timeframe, st_atr_period, st_factor,
                 ema_fast, ema_slow, rsi_period, rsi_ob, rsi_os,
                 macd_fast, macd_slow, macd_signal, min_confirmations):
        self.timeframe = timeframe
        self.st_atr_period = st_atr_period
        self.st_factor = st_factor
        self.ema_fast = ema_fast
        self.ema_slow = ema_slow
        self.rsi_period = rsi_period
        self.rsi_ob = rsi_ob
        self.rsi_os = rsi_os
        self.macd_fast = macd_fast
        self.macd_slow = macd_slow
        self.macd_signal = macd_signal
        self.min_confirmations = min_confirmations

    def analyze(self, bars: list) -> dict:
        if len(bars) < self.macd_slow + self.macd_signal + 5:
            return {"signal": "HOLD", "reason": "Datos insuficientes",
                    "long_score": 0, "short_score": 0, "close": bars[-1][4] if bars else 0}

        highs  = [b[2] for b in bars]
        lows   = [b[3] for b in bars]
        closes = [b[4] for b in bars]

        st_dir, st_val = supertrend(highs, lows, closes, self.st_atr_period, self.st_factor)
        st_bull = st_dir == 1

        ema_f = ema(closes, self.ema_fast)
        ema_s = ema(closes, self.ema_slow)
        ema_bull = ema_f[-1] > ema_s[-1] if (ema_f and ema_s) else False
        cross_up   = len(ema_f) >= 2 and len(ema_s) >= 2 and ema_f[-2] <= ema_s[-2] and ema_f[-1] > ema_s[-1]
        cross_down = len(ema_f) >= 2 and len(ema_s) >= 2 and ema_f[-2] >= ema_s[-2] and ema_f[-1] < ema_s[-1]

        rsi_val = rsi(closes, self.rsi_period)
        rsi_bull = rsi_val < self.rsi_ob
        rsi_bear = rsi_val > self.rsi_os

        _, _, hist = macd(closes, self.macd_fast, self.macd_slow, self.macd_signal)
        macd_bull = hist > 0
        macd_bear = hist < 0

        long_score  = sum([st_bull,      ema_bull,      rsi_bull, macd_bull]) + (1 if cross_up   else 0)
        short_score = sum([not st_bull,  not ema_bull,  rsi_bear, macd_bear]) + (1 if cross_down else 0)

        if long_score >= self.min_confirmations and long_score > short_score:
            signal = "BUY"
        elif short_score >= self.min_confirmations and short_score > long_score:
            signal = "SELL"
        else:
            signal = "HOLD"

        result = {
            "signal": signal,
            "close": closes[-1],
            "supertrend_dir": "BULL" if st_bull else "BEAR",
            "supertrend_val": st_val,
            "ema_fast": round(ema_f[-1], 4) if ema_f else 0,
            "ema_slow": round(ema_s[-1], 4) if ema_s else 0,
            "ema_trend": "BULL" if ema_bull else "BEAR",
            "ema_cross": "CRUCE UP" if cross_up else ("CRUCE DOWN" if cross_down else "-"),
            "rsi": rsi_val,
            "macd_hist": hist,
            "long_score": long_score,
            "short_score": short_score,
        }

        logger.info(
            f"SIGNAL:{signal} | ST:{result['supertrend_dir']} | "
            f"EMA:{result['ema_trend']} | RSI:{rsi_val} | MACD hist:{hist:.5f} | "
            f"Score L:{long_score}/S:{short_score}"
        )
        return result
