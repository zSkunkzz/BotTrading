import os
from dotenv import load_dotenv

load_dotenv()

# BingX
API_KEY    = os.environ["BINGX_API_KEY"]
API_SECRET = os.environ["BINGX_API_SECRET"]
BASE_URL   = "https://open-api.bingx.com"

# Multi-par — top 30 por market cap
SYMBOLS = [
    "BTC-USDT", "ETH-USDT", "BNB-USDT", "SOL-USDT", "XRP-USDT",
    "DOGE-USDT", "ADA-USDT", "AVAX-USDT", "SHIB-USDT", "TON-USDT",
    "DOT-USDT", "LINK-USDT", "TRX-USDT", "MATIC-USDT", "LTC-USDT",
    "BCH-USDT", "UNI-USDT", "NEAR-USDT", "ICP-USDT", "APT-USDT",
    "ETC-USDT", "STX-USDT", "FIL-USDT", "INJ-USDT", "OP-USDT",
    "ARB-USDT", "ATOM-USDT", "SUI-USDT", "VET-USDT", "HBAR-USDT",
]

MAX_POSITIONS  = int(os.getenv("MAX_POSITIONS", "3"))    # máximo simultáneo
LEVERAGE       = int(os.getenv("LEVERAGE", "10"))
MARGIN_USDT    = float(os.getenv("MARGIN_USDT", "20"))   # margen fijo por trade

# SL/TP (ratio ATR — solo fallback)
SL_PCT         = float(os.getenv("SL_PCT", "1.5"))
TP_PCT         = float(os.getenv("TP_PCT", "3.0"))

# Señales
TIMEFRAME      = os.getenv("TIMEFRAME", "15m")
LOOP_SLEEP     = int(os.getenv("LOOP_SLEEP", "15"))      # segundos entre scans

# Telegram
TG_TOKEN       = os.getenv("TELEGRAM_TOKEN", "")
TG_CHAT_ID     = os.getenv("TELEGRAM_CHAT_ID", "")
