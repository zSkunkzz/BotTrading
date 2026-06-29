import os
from dotenv import load_dotenv

load_dotenv()

# BingX
API_KEY    = os.environ["BINGX_API_KEY"]
API_SECRET = os.environ["BINGX_API_SECRET"]
BASE_URL   = "https://open-api.bingx.com"

# Pares verificados en BingX perpetual futures (top 100 CMC, junio 2026)
# Criterios: perp activo en BingX + spread limpio + liquidez >= media.
# Excluidos explícitamente:
#   PENGU-USDT, NOT-USDT, LISTA-USDT, MANTA-USDT
#   -> spreads/spikes impredecibles, el filtro de liquidez los rechazaba
#      en runtime pero consumían conexiones WS y ciclos de loop.
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
    "HYPE-USDT", "JUP-USDT",  "TIA-USDT",
    # --- 41-45 ---
    "RENDER-USDT", "SEI-USDT",
    "ZK-USDT",   "EIGEN-USDT", "AERO-USDT",
    # --- 46-50 (nuevos, top 100 CMC verificados BingX) ---
    "AAVE-USDT",  # #47 CMC — DeFi blue chip, spread limpio
    "GRT-USDT",   # #55 CMC — The Graph, DeFi infra
    "LDO-USDT",   # #58 CMC — Lido DAO, liquid staking
    "ENA-USDT",   # #63 CMC — Ethena, DeFi yield
    "ALGO-USDT",  # #66 CMC — Algorand, L1
    "DYDX-USDT",  # #68 CMC — dYdX, DeFi exchange
    "RUNE-USDT",  # #72 CMC — THORChain, cross-chain
]

# Pares en modo alerta manual (no se tradean automáticamente)
MANUAL_ALERT_SYMBOLS: set[str] = set()

# Grupos de correlación.
# Regla: pares del mismo grupo compiten por MAX_CORR_PER_GROUP slots.
CORR_GROUPS: list[set[str]] = [
    {"BTC-USDT", "ETH-USDT", "BNB-USDT", "LDO-USDT"},
    {"SOL-USDT", "AVAX-USDT", "APT-USDT", "SUI-USDT", "NEAR-USDT", "TIA-USDT"},
    {"ARB-USDT", "OP-USDT", "DOT-USDT", "ICP-USDT", "POL-USDT", "ZK-USDT", "DYDX-USDT"},
    {"LINK-USDT", "UNI-USDT", "AERO-USDT", "JUP-USDT", "ONDO-USDT", "AAVE-USDT", "GRT-USDT"},
    {"XRP-USDT", "XLM-USDT", "TRX-USDT", "HBAR-USDT", "ADA-USDT", "ALGO-USDT"},
    {"FIL-USDT", "RENDER-USDT", "TAO-USDT", "EIGEN-USDT", "FET-USDT", "WLD-USDT"},
    {"ATOM-USDT", "INJ-USDT", "SEI-USDT", "RUNE-USDT"},
    {"ONDO-USDT", "AAVE-USDT", "ENA-USDT"},
    # Memes puros — alta correlación en días de risk-on
    {"DOGE-USDT", "SHIB-USDT"},
    # Legacy PoW/fork coins — correlacionadas entre sí, NO con memes
    {"BCH-USDT", "LTC-USDT", "ETC-USDT"},
    # Misc sin correlación clara — grupo propio para limitar exposición
    {"VET-USDT", "STX-USDT"},
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

# LEGACY — no usados por risk.py en condiciones normales.
# risk.py calcula SL desde ATR 1h × 1.2 directamente.
# SL_PCT solo actúa como fallback de emergencia cuando ATR=0 (caso excepcional).
SL_PCT         = float(os.getenv("SL_PCT", "1.5"))   # fallback ATR=0 únicamente
TP_PCT         = float(os.getenv("TP_PCT", "3.0"))   # no utilizado actualmente

TIMEFRAME           = os.getenv("TIMEFRAME", "15m")
LOOP_SLEEP          = int(os.getenv("LOOP_SLEEP", "20"))
WEEKDAY_MIN_SCORE   = int(os.getenv("WEEKDAY_MIN_SCORE", os.getenv("MIN_SCORE", "70")))
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
