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
# Cada grupo limita cuántas posiciones simultáneas se abren en pares
# que se mueven de forma correlacionada. Abrir 3 pares del mismo grupo
# es equivalente a 1 posición con 3x el riesgo.
#
# El grupo original BTC-mega tenía 12 símbolos con MAX_CORR_PER_GROUP=2,
# lo que bloqueaba hasta 10 pares cuando BTC+ETH estaban abiertos.
# Dividido en grupos semánticos más pequeños para evitar ese bloqueo:
#
#   mega_caps  (3): BTC, ETH, BNB     — correlación ~0.95, movimientos idénticos
#   L1s        (5): SOL, AVAX, APT, SUI, NEAR  — correlacionadas entre sí pero
#                   no tanto con BTC/ETH en el corto plazo
#   L2s_infra  (4): ARB, OP, DOT, ICP — sector infra/L2, correlación alta
#   defi       (5): LINK, UNI, AERO, JUP, ONDO — DeFi, menor correlación con L1s
CORR_GROUPS: list[set[str]] = [
    # Memes — correlación extrema entre sí, mueven juntos en eventos de mercado
    {"DOGE-USDT", "SHIB-USDT", "NOT-USDT"},
    # Mega caps — BTC/ETH/BNB se mueven casi idénticos
    {"BTC-USDT", "ETH-USDT", "BNB-USDT"},
    # L1s alternativas — SOL, AVAX, APT, SUI, NEAR rotan juntos en alt-season
    {"SOL-USDT", "AVAX-USDT", "APT-USDT", "SUI-USDT", "NEAR-USDT"},
    # L2s e infraestructura
    {"ARB-USDT", "OP-USDT", "DOT-USDT", "ICP-USDT"},
    # DeFi
    {"LINK-USDT", "UNI-USDT", "AERO-USDT", "JUP-USDT", "ONDO-USDT"},
    # Pagos / RippleNet
    {"XRP-USDT", "XLM-USDT", "TRX-USDT", "HBAR-USDT"},
    # Storage / AI infra
    {"FIL-USDT", "RENDER-USDT", "TAO-USDT", "EIGEN-USDT"},
]
MAX_CORR_PER_GROUP = int(os.getenv("MAX_CORR_PER_GROUP", "2"))

# Máximo de posiciones en la misma dirección (long o short) simultáneamente.
# Evita concentrar todo el capital en una sola dirección en mercados bull/bear.
# Con MAX_POSITIONS=7 y MAX_SAME_SIDE=4 siempre quedan 3 slots para la
# dirección contraria si aparece una señal de reversión.
MAX_SAME_SIDE  = int(os.getenv("MAX_SAME_SIDE", "4"))

MAX_POSITIONS  = int(os.getenv("MAX_POSITIONS", "7"))
LEVERAGE       = int(os.getenv("LEVERAGE", "10"))
MARGIN_USDT    = float(os.getenv("MARGIN_USDT", "20"))

# SL/TP fallback
SL_PCT         = float(os.getenv("SL_PCT", "1.5"))
TP_PCT         = float(os.getenv("TP_PCT", "3.0"))

# Señales
TIMEFRAME           = os.getenv("TIMEFRAME", "15m")
LOOP_SLEEP          = int(os.getenv("LOOP_SLEEP", "20"))
WEEKDAY_MIN_SCORE   = int(os.getenv("WEEKDAY_MIN_SCORE", os.getenv("MIN_SCORE", "70")))
WEEKEND_MIN_SCORE   = int(os.getenv("WEEKEND_MIN_SCORE", "90"))
MIN_SCORE           = WEEKDAY_MIN_SCORE   # alias legacy

# Riesgo diario
DAILY_MAX_LOSS_PCT  = float(os.getenv("DAILY_MAX_LOSS_PCT", "-3.0"))
MAX_DAILY_LOSS_USDT = float(os.getenv("MAX_DAILY_LOSS_USDT", "30"))

# Win rate monitor
WINRATE_LOOKBACK    = int(os.getenv("WINRATE_LOOKBACK", "10"))
WINRATE_ALERT_PCT   = float(os.getenv("WINRATE_ALERT_PCT", "30"))

# Filtro de spread/liquidez
MAX_SPREAD_PCT = float(os.getenv("MAX_SPREAD_PCT", "0.15"))

# Telegram
TG_TOKEN       = os.getenv("TELEGRAM_TOKEN", "")
TG_CHAT_ID     = os.getenv("TELEGRAM_CHAT_ID", "")
