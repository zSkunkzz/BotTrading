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

CORR_GROUPS: list[set[str]] = [
    {"BTC-USDT", "ETH-USDT", "BNB-USDT"},
    {"SOL-USDT", "AVAX-USDT", "APT-USDT", "SUI-USDT", "NEAR-USDT", "TIA-USDT"},
    {"ARB-USDT", "OP-USDT", "DOT-USDT", "ICP-USDT", "POL-USDT", "ZK-USDT"},
    {"LINK-USDT", "UNI-USDT", "AERO-USDT", "JUP-USDT", "ONDO-USDT"},
    {"XRP-USDT", "XLM-USDT", "TRX-USDT", "HBAR-USDT", "ADA-USDT"},
    {"FIL-USDT", "RENDER-USDT", "TAO-USDT", "EIGEN-USDT", "FET-USDT", "WLD-USDT"},
    {"ATOM-USDT", "INJ-USDT", "SEI-USDT", "MANTA-USDT"},
    {"DOGE-USDT", "SHIB-USDT", "NOT-USDT", "PENGU-USDT",
     "VET-USDT",  "STX-USDT",  "LISTA-USDT",
     "BCH-USDT",  "LTC-USDT",  "ETC-USDT"},
    {"HYPE-USDT", "MNT-USDT"},
]
MAX_CORR_PER_GROUP = int(os.getenv("MAX_CORR_PER_GROUP", "2"))

def _check_corr_coverage() -> None:
    all_grouped = {sym for group in CORR_GROUPS for sym in group}
    missing = [s for s in SYMBOLS if s not in all_grouped]
    if missing:
        import logging
        logging.getLogger("config").warning(
            "CORR_GROUPS: %d símbolos sin grupo de correlación asignado — "
            "el guard no aplica para ellos: %s",
            len(missing), missing,
        )

_check_corr_coverage()

MAX_SAME_SIDE  = int(os.getenv("MAX_SAME_SIDE", "4"))
MAX_POSITIONS  = int(os.getenv("MAX_POSITIONS", "7"))
LEVERAGE       = int(os.getenv("LEVERAGE", "10"))
MARGIN_USDT    = float(os.getenv("MARGIN_USDT", "20"))

SL_PCT         = float(os.getenv("SL_PCT", "1.5"))
TP_PCT         = float(os.getenv("TP_PCT", "3.0"))

TIMEFRAME           = os.getenv("TIMEFRAME", "15m")
LOOP_SLEEP          = int(os.getenv("LOOP_SLEEP", "20"))
# MIN_SCORE subido de 70 → 72.
# Con 70 los trades borderline (score exactamente 70) tenían winrate bajo.
# 72 filtra esas señales débiles sin reducir significativamente la frecuencia
# ya que la diferencia de 2 puntos equivale a no tener rsi_dir (+4) o
# no tener volumen ok (+8) — señales que ya eran débiles de por sí.
WEEKDAY_MIN_SCORE   = int(os.getenv("WEEKDAY_MIN_SCORE", os.getenv("MIN_SCORE", "72")))
WEEKEND_MIN_SCORE   = int(os.getenv("WEEKEND_MIN_SCORE", "90"))
MIN_SCORE           = WEEKDAY_MIN_SCORE

DAILY_MAX_LOSS_PCT  = float(os.getenv("DAILY_MAX_LOSS_PCT", "-3.0"))
MAX_DAILY_LOSS_USDT = float(os.getenv("MAX_DAILY_LOSS_USDT", "30"))

WINRATE_LOOKBACK    = int(os.getenv("WINRATE_LOOKBACK", "10"))
WINRATE_ALERT_PCT   = float(os.getenv("WINRATE_ALERT_PCT", "30"))

MAX_SPREAD_PCT = float(os.getenv("MAX_SPREAD_PCT", "0.15"))

# Telegram
TG_TOKEN       = os.getenv("TELEGRAM_TOKEN", "")
TG_CHAT_ID     = os.getenv("TELEGRAM_CHAT_ID", "")
