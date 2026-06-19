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
    "HYPE-USDT", "GOLD-USDT", "JUP-USDT",  "PENGU-USDT","TIA-USDT",
    # --- 41-50 ---
    "RENDER-USDT","SEI-USDT", "BONK-USDT", "PEPE-USDT", "NOT-USDT",
    "ZK-USDT",   "EIGEN-USDT","LISTA-USDT","MANTA-USDT","AERO-USDT",
    # --- Materias primas y forex (modo alerta manual) ---
    "SILVER-USDT", "EUR-USDT",
]

# Pares en modo alerta manual: el bot detecta la señal pero NO abre posición.
MANUAL_ALERT_SYMBOLS = {"GOLD-USDT", "SILVER-USDT", "EUR-USDT"}

# Grupos de correlación: máx MAX_CORR_PER_GROUP posiciones simultáneas por grupo.
# Si un par no aparece en ningún grupo, no se limita.
CORR_GROUPS: list[set[str]] = [
    {"DOGE-USDT", "SHIB-USDT", "BONK-USDT", "PEPE-USDT", "NOT-USDT"},          # memecoins
    {"BTC-USDT", "ETH-USDT", "BNB-USDT", "SOL-USDT", "AVAX-USDT", "APT-USDT",
     "SUI-USDT", "NEAR-USDT", "DOT-USDT", "ICP-USDT", "ARB-USDT", "OP-USDT"},  # L1/L2
    {"LINK-USDT", "UNI-USDT", "AERO-USDT", "JUP-USDT", "ONDO-USDT"},           # DeFi
    {"XRP-USDT", "XLM-USDT", "TRX-USDT", "HBAR-USDT"},                          # pagos
    {"FIL-USDT", "RENDER-USDT", "TAO-USDT", "EIGEN-USDT"},                       # IA/storage
]
MAX_CORR_PER_GROUP = int(os.getenv("MAX_CORR_PER_GROUP", "2"))

MAX_POSITIONS  = int(os.getenv("MAX_POSITIONS", "7"))    # máximo simultáneo
LEVERAGE       = int(os.getenv("LEVERAGE", "10"))
MARGIN_USDT    = float(os.getenv("MARGIN_USDT", "20"))   # margen fijo por trade

# SL/TP (fallback si ATR falla)
SL_PCT         = float(os.getenv("SL_PCT", "1.5"))
TP_PCT         = float(os.getenv("TP_PCT", "3.0"))

# Señales
TIMEFRAME           = os.getenv("TIMEFRAME", "15m")
LOOP_SLEEP          = int(os.getenv("LOOP_SLEEP", "20"))          # segundos entre scans
MIN_SCORE           = int(os.getenv("MIN_SCORE", "55"))           # score mínimo días de semana
WEEKEND_MIN_SCORE   = int(os.getenv("WEEKEND_MIN_SCORE", "90"))   # score mínimo fin de semana

# Riesgo diario
MAX_DAILY_LOSS_USDT = float(os.getenv("MAX_DAILY_LOSS_USDT", "30"))  # pérdida máxima diaria

# Alerta de win rate: si los últimos N trades tienen win rate < umbral → aviso TG
WINRATE_LOOKBACK    = int(os.getenv("WINRATE_LOOKBACK", "10"))      # ventana de trades
WINRATE_ALERT_PCT   = float(os.getenv("WINRATE_ALERT_PCT", "30"))   # umbral % (def 30%)

# Telegram
TG_TOKEN       = os.getenv("TELEGRAM_TOKEN", "")
TG_CHAT_ID     = os.getenv("TELEGRAM_CHAT_ID", "")
