import os
from dotenv import load_dotenv

load_dotenv()

# BingX
API_KEY    = os.environ["BINGX_API_KEY"]
API_SECRET = os.environ["BINGX_API_SECRET"]
BASE_URL   = "https://open-api.bingx.com"

# Pares verificados en BingX perpetual futures (top market cap, junio 2026)
SYMBOLS = [
    # --- Top 10 ---
    "BTC-USDT",  "ETH-USDT",  "BNB-USDT",  "XRP-USDT",  "SOL-USDT",
    "TRX-USDT",  "DOGE-USDT", "XLM-USDT",  "ADA-USDT",  "LINK-USDT",
    # --- 11–20 ---
    "BCH-USDT",  "HBAR-USDT", "LTC-USDT",  "SUI-USDT",  "AVAX-USDT",
    "NEAR-USDT", "SHIB-USDT", "DOT-USDT",  "UNI-USDT",  "ICP-USDT",
    # --- 21–30 ---
    "ETC-USDT",  "FIL-USDT",  "INJ-USDT",  "APT-USDT",  "ARB-USDT",
    "VET-USDT",  "STX-USDT",  "ATOM-USDT", "TAO-USDT",  "WLD-USDT",
    # --- 31–40 ---
    "ONDO-USDT", "MNT-USDT",  "FET-USDT",  "OP-USDT",   "POL-USDT",
    "HYPE-USDT", "GOLD-USDT", "JUP-USDT",  "PENGU-USDT","TIA-USDT",
    # --- 41–50 ---
    "RENDER-USDT","SEI-USDT", "BONK-USDT", "PEPE-USDT", "NOT-USDT",
    "ZK-USDT",   "EIGEN-USDT","LISTA-USDT","MANTA-USDT","AERO-USDT",
]

# Pares en modo alerta manual: el bot detecta la señal pero NO abre posición.
# En su lugar manda un aviso a Telegram para que el usuario la ejecute a mano.
MANUAL_ALERT_SYMBOLS = {"GOLD-USDT"}

MAX_POSITIONS  = int(os.getenv("MAX_POSITIONS", "7"))    # máximo simultáneo
LEVERAGE       = int(os.getenv("LEVERAGE", "10"))
MARGIN_USDT    = float(os.getenv("MARGIN_USDT", "20"))   # margen fijo por trade

# SL/TP (fallback si ATR falla)
SL_PCT         = float(os.getenv("SL_PCT", "1.5"))
TP_PCT         = float(os.getenv("TP_PCT", "3.0"))

# Señales
TIMEFRAME      = os.getenv("TIMEFRAME", "15m")
LOOP_SLEEP     = int(os.getenv("LOOP_SLEEP", "20"))      # segundos entre scans (20s con 50 pares)
MIN_SCORE      = int(os.getenv("MIN_SCORE", "55"))       # score mínimo para abrir posición

# Telegram
TG_TOKEN       = os.getenv("TELEGRAM_TOKEN", "")
TG_CHAT_ID     = os.getenv("TELEGRAM_CHAT_ID", "")
