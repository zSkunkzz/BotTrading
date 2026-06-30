import os
from dotenv import load_dotenv

load_dotenv()

# ── Hyperliquid ────────────────────────────────────────────────────────────────
# HYPERLIQUID_PRIVATE_KEY : clave privada EVM en hex (con o sin 0x)
# HYPERLIQUID_WALLET_ADDRESS : opcional, se deriva de la pk si no se pone
# HL_MAINNET : "true" para mainnet (producción), "false" para testnet
# Las variables se leen directamente en exchange.py para no exponerlas aquí.

# Pares verificados en Hyperliquid perpetual futures
# Hyperliquid usa el token base sin "-USDT": "BTC", "ETH", etc.
# Las funciones de exchange.py normalizan automáticamente "BTC-USDT" → "BTC".
# Excluidos explícitamente:
#   PENGU, NOT, LISTA, MANTA → spreads/spikes impredecibles
# Nota: 1000SHIB cotiza igual en Hyperliquid (precio por 1000 tokens).
SYMBOLS = [
    # --- Top 10 ---
    "BTC-USDT",  "ETH-USDT",  "BNB-USDT",  "XRP-USDT",  "SOL-USDT",
    "TRX-USDT",  "DOGE-USDT", "XLM-USDT",  "ADA-USDT",  "LINK-USDT",
    # --- 11-20 ---
    "BCH-USDT",  "HBAR-USDT", "LTC-USDT",  "SUI-USDT",  "AVAX-USDT",
    "NEAR-USDT", "1000SHIB-USDT", "DOT-USDT",  "UNI-USDT",  "ICP-USDT",
    # --- 21-30 ---
    "ETC-USDT",  "FIL-USDT",  "INJ-USDT",  "APT-USDT",  "ARB-USDT",
    "VET-USDT",  "STX-USDT",  "ATOM-USDT", "TAO-USDT",  "WLD-USDT",
    # --- 31-40 ---
    "ONDO-USDT", "MNT-USDT",  "FET-USDT",  "OP-USDT",   "POL-USDT",
    "HYPE-USDT", "JUP-USDT",  "TIA-USDT",
    # --- 41-45 ---
    "RENDER-USDT", "SEI-USDT",
    "ZK-USDT",   "EIGEN-USDT", "AERO-USDT",
    # --- 46-50 ---
    "AAVE-USDT",
    "GRT-USDT",
    "LDO-USDT",
    "ENA-USDT",
    "ALGO-USDT",
    "DYDX-USDT",
    "RUNE-USDT",
    # --- Memes/narrativa ---
    "PUMP-USDT",
    "ASTER-USDT",
    "ANSEM-USDT",
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
    {"DOGE-USDT", "1000SHIB-USDT", "PUMP-USDT", "ANSEM-USDT"},
    # Legacy PoW/fork coins
    {"BCH-USDT", "LTC-USDT", "ETC-USDT"},
    # Misc sin correlación clara
    {"VET-USDT", "STX-USDT"},
    {"HYPE-USDT", "MNT-USDT"},
    # Nuevos narrativa/misc
    {"ASTER-USDT"},
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

# LEGACY — SL_PCT solo actúa como fallback de emergencia cuando ATR=0.
SL_PCT         = float(os.getenv("SL_PCT", "1.5"))
TP_PCT         = float(os.getenv("TP_PCT", "3.0"))

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
