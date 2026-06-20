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
    # --- 11-20 ---
    "BCH-USDT",  "HBAR-USDT", "LTC-USDT",  "SUI-USDT",  "AVAX-USDT",
    "NEAR-USDT", "SHIB-USDT", "DOT-USDT",  "UNI-USDT",  "ICP-USDT",
    # --- 21-30 ---
    "ETC-USDT",  "FIL-USDT",  "INJ-USDT",  "APT-USDT",  "ARB-USDT",
    "VET-USDT",  "STX-USDT",  "ATOM-USDT", "TAO-USDT",  "WLD-USDT",
    # --- 31-40 ---
    "ONDO-USDT", "MNT-USDT",  "FET-USDT",  "OP-USDT",   "POL-USDT",
    "HYPE-USDT", "JUP-USDT",  "PENGU-USDT","TIA-USDT",
    # --- 41+ ---
    "RENDER-USDT","SEI-USDT", "NOT-USDT",
    "ZK-USDT",   "EIGEN-USDT","LISTA-USDT","MANTA-USDT","AERO-USDT",
]

# Pares en modo alerta manual (no se tradean automáticamente)
MANUAL_ALERT_SYMBOLS: set[str] = set()

# Grupos de correlación
CORR_GROUPS: list[set[str]] = [
    {"DOGE-USDT", "SHIB-USDT", "NOT-USDT"},
    {"BTC-USDT", "ETH-USDT", "BNB-USDT", "SOL-USDT", "AVAX-USDT", "APT-USDT",
     "SUI-USDT", "NEAR-USDT", "DOT-USDT", "ICP-USDT", "ARB-USDT", "OP-USDT"},
    {"LINK-USDT", "UNI-USDT", "AERO-USDT", "JUP-USDT", "ONDO-USDT"},
    {"XRP-USDT", "XLM-USDT", "TRX-USDT", "HBAR-USDT"},
    {"FIL-USDT", "RENDER-USDT", "TAO-USDT", "EIGEN-USDT"},
]
MAX_CORR_PER_GROUP = int(os.getenv("MAX_CORR_PER_GROUP", "2"))

MAX_POSITIONS  = int(os.getenv("MAX_POSITIONS", "7"))
LEVERAGE       = int(os.getenv("LEVERAGE", "10"))
MARGIN_USDT    = float(os.getenv("MARGIN_USDT", "20"))

# SL/TP fallback
SL_PCT         = float(os.getenv("SL_PCT", "1.5"))
TP_PCT         = float(os.getenv("TP_PCT", "3.0"))

# Señales
TIMEFRAME           = os.getenv("TIMEFRAME", "15m")
LOOP_SLEEP          = int(os.getenv("LOOP_SLEEP", "20"))
MIN_SCORE           = int(os.getenv("MIN_SCORE", "55"))
WEEKEND_MIN_SCORE   = int(os.getenv("WEEKEND_MIN_SCORE", "90"))

# Riesgo diario
MAX_DAILY_LOSS_USDT = float(os.getenv("MAX_DAILY_LOSS_USDT", "30"))

# Win rate monitor
WINRATE_LOOKBACK    = int(os.getenv("WINRATE_LOOKBACK", "10"))
WINRATE_ALERT_PCT   = float(os.getenv("WINRATE_ALERT_PCT", "30"))

# Filtro de spread/liquidez: máximo spread permitido como % del mid price.
# Con 10x de apalancamiento, un spread de 0.15% = 1.5% de coste sobre el capital.
# Pares ilíquidos (MANTA, LISTA, AERO en horas bajas) pueden superar 0.3-0.5%.
MAX_SPREAD_PCT = float(os.getenv("MAX_SPREAD_PCT", "0.15"))

# Telegram
TG_TOKEN       = os.getenv("TELEGRAM_TOKEN", "")
TG_CHAT_ID     = os.getenv("TELEGRAM_CHAT_ID", "")
