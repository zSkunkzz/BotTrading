import os
from dotenv import load_dotenv

load_dotenv()

# ── Hyperliquid ────────────────────────────────────────────────────────────────
# HYPERLIQUID_PRIVATE_KEY : clave privada EVM en hex (con o sin 0x)
# HYPERLIQUID_WALLET_ADDRESS : opcional, se deriva de la pk si no se pone
# HL_MAINNET : "true" para mainnet (producción), "false" para testnet
# Las variables se leen directamente en exchange.py para no exponerlas aquí.

# Pares seleccionados por volumen real 24h en Hyperliquid perpetual futures
# (datos consultados 2026-07-01). Ordenados de mayor a menor vol.
# Hyperliquid usa el token base sin "-USDT": "BTC", "ETH", etc.
# Las funciones de exchange.py normalizan automáticamente "BTC-USDT" → "BTC".
#
# Excluidos explícitamente (motivo):
#   PUMP-USDT    → token meme con spreads impredecibles
#   PAXG-USDT    → pegged a oro, no tendencial
#   FARTCOIN     → meme puro, sin estructura técnica
#   SPX-USDT     → liquidez concentrada en eventos puntuales
#   XPL-USDT     → muy nuevo, sin histórico suficiente
#   MON-USDT     → muy nuevo, sin histórico suficiente
#   MET-USDT     → muy nuevo, sin histórico suficiente
#   PENGU-USDT   → spreads/spikes impredecibles
#   RESOLV-USDT  → muy nuevo, sin histórico suficiente
#   WLFI-USDT    → token político, alta correlación con TRUMP
SYMBOLS = [
    # --- Top por vol (>100M USD/24h) ---
    "BTC-USDT",   "ETH-USDT",   "HYPE-USDT",  "SOL-USDT",
    # --- Alto vol (10M-100M USD/24h) ---
    "ZEC-USDT",   "LIT-USDT",   "WLD-USDT",   "VVV-USDT",
    "XRP-USDT",   "NEAR-USDT",  "MORPHO-USDT","DYDX-USDT",
    "AAVE-USDT",  "DOGE-USDT",  "kPEPE-USDT", "JTO-USDT",
    "XLM-USDT",   "SUI-USDT",   "ADA-USDT",   "ENA-USDT",
    "TAO-USDT",   "JUP-USDT",   "BNB-USDT",
    # --- Vol medio (2M-10M USD/24h) ---
    "BCH-USDT",   "XMR-USDT",   "TRUMP-USDT", "AVAX-USDT",
    "UNI-USDT",   "CRV-USDT",   "ARB-USDT",   "ZRO-USDT",
    "LINK-USDT",  "ONDO-USDT",  "PYTH-USDT",  "LTC-USDT",
    "APT-USDT",   "WIF-USDT",   "DOT-USDT",   "kBONK-USDT",
    "GRASS-USDT", "AERO-USDT",  "FET-USDT",   "TRX-USDT",
    # --- Vol bajo-medio (700k-2M USD/24h) ---
    "HBAR-USDT",  "VIRTUAL-USDT","EIGEN-USDT", "OP-USDT",
    "PENDLE-USDT","INJ-USDT",   "kSHIB-USDT", "SEI-USDT",
    "TIA-USDT",   "LDO-USDT",
]

# Pares en modo alerta manual (no se tradean automáticamente)
MANUAL_ALERT_SYMBOLS: set[str] = set()

# Grupos de correlación.
# Regla: pares del mismo grupo compiten por MAX_CORR_PER_GROUP slots.
CORR_GROUPS: list[set[str]] = [
    # BTC / ETH y derivados de staking
    {"BTC-USDT", "ETH-USDT", "LDO-USDT", "EIGEN-USDT"},
    # Ecosistema Solana
    {"SOL-USDT", "JUP-USDT", "JTO-USDT", "WIF-USDT", "kBONK-USDT", "kPEPE-USDT",
     "kSHIB-USDT", "PYTH-USDT", "GRASS-USDT"},
    # L1 alternativas (Avalanche, Near, Aptos, Sui)
    {"AVAX-USDT", "NEAR-USDT", "APT-USDT", "SUI-USDT", "TIA-USDT"},
    # L2 Ethereum
    {"ARB-USDT", "OP-USDT", "DYDX-USDT", "ZRO-USDT"},
    # DeFi blue chips
    {"AAVE-USDT", "UNI-USDT", "CRV-USDT", "PENDLE-USDT", "MORPHO-USDT"},
    # Oráculos y datos on-chain
    {"LINK-USDT", "ONDO-USDT", "FET-USDT", "VIRTUAL-USDT"},
    # Pagos / XRP-esfera
    {"XRP-USDT", "XLM-USDT", "TRX-USDT", "HBAR-USDT", "ADA-USDT"},
    # Interoperabilidad / Cosmos
    {"DOT-USDT", "INJ-USDT", "SEI-USDT", "TAO-USDT"},
    # AI / Compute
    {"WLD-USDT", "AERO-USDT", "LIT-USDT"},
    # Memes puros
    {"DOGE-USDT", "TRUMP-USDT"},
    # Legacy PoW / fork coins
    {"BCH-USDT", "LTC-USDT", "ZEC-USDT", "XMR-USDT"},
    # Misc / standalone
    {"BNB-USDT"},
    {"HYPE-USDT"},
    {"ENA-USDT"},
    {"VVV-USDT"},
    {"JTO-USDT"},
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
