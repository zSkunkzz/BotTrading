import os
from dotenv import load_dotenv

load_dotenv()

# BingX
API_KEY    = os.environ["BINGX_API_KEY"]
API_SECRET = os.environ["BINGX_API_SECRET"]
BASE_URL   = "https://open-api.bingx.com"

# Trading
SYMBOL     = os.getenv("SYMBOL", "BTC-USDT")
LEVERAGE   = int(os.getenv("LEVERAGE", "10"))
USDC_SIZE  = float(os.getenv("USDC_SIZE", "10"))   # capital por trade en USDT
SL_PCT     = float(os.getenv("SL_PCT", "1.5"))     # % stop loss
TP_PCT     = float(os.getenv("TP_PCT", "3.0"))     # % take profit

# Señales
TIMEFRAME  = os.getenv("TIMEFRAME", "15m")
LOOP_SLEEP = int(os.getenv("LOOP_SLEEP", "15"))    # segundos entre scans

# Telegram
TG_TOKEN   = os.getenv("TELEGRAM_TOKEN", "")
TG_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
