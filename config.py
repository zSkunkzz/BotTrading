import os
from dotenv import load_dotenv

load_dotenv()

# ── Hyperliquid ────────────────────────────────────────────────────────────────
# HYPERLIQUID_PRIVATE_KEY : clave privada EVM en hex (con o sin 0x)
# HYPERLIQUID_WALLET_ADDRESS : opcional, se deriva de la pk si no se pone
# HL_MAINNET : "true" para mainnet (producción), "false" para testnet
# Las variables se leen directamente en exchange.py para no exponerlas aquí.

# Pares obtenidos de metaAndAssetCtxs — 2026-07-04.
# Filtros aplicados: no delisted, no hyna (onlyIsolated), vol24h > 50 000 USD.
# Sin blacklist manual — todos los mercados nativos activos incluidos.
SYMBOLS = [
    # ── Top vol (>100M USD/24h) ─────────────────────────────────────────────
    "BTC-USDT",    "ETH-USDT",    "HYPE-USDT",   "SOL-USDT",
    # ── Alto vol (10M–100M USD/24h) ─────────────────────────────────────────
    "ZEC-USDT",    "XRP-USDT",    "ADA-USDT",    "WLD-USDT",
    "NEAR-USDT",   "GRAM-USDT",   "AAVE-USDT",   "kPEPE-USDT",
    "DOGE-USDT",   "SUI-USDT",    "kBONK-USDT",  "ENA-USDT",
    "HMSTR-USDT",
    # ── Vol medio-alto (1M–10M USD/24h) ─────────────────────────────────────
    "ETHFI-USDT",  "TAO-USDT",    "ZRO-USDT",    "XLM-USDT",
    "XMR-USDT",    "AVAX-USDT",   "BNB-USDT",    "LINK-USDT",
    "JTO-USDT",    "BCH-USDT",    "ONDO-USDT",   "WIF-USDT",
    "LTC-USDT",    "TIA-USDT",    "ASTER-USDT",  "JUP-USDT",
    "CRV-USDT",    "UNI-USDT",    "POPCAT-USDT", "TRUMP-USDT",
    "FET-USDT",    "CHIP-USDT",   "PENDLE-USDT", "TRX-USDT",
    "MEGA-USDT",   "MORPHO-USDT", "DYDX-USDT",   "HBAR-USDT",
    "ARB-USDT",    "LIT-USDT",
    # ── Vol medio (500k–1M USD/24h) ─────────────────────────────────────────
    "DOT-USDT",    "BERA-USDT",   "APT-USDT",    "INJ-USDT",
    "ICP-USDT",    "VIRTUAL-USDT","PNUT-USDT",   "kNEIRO-USDT",
    "AXS-USDT",    "TRB-USDT",    "SEI-USDT",    "S-USDT",
    "PYTH-USDT",   "ORDI-USDT",   "KAITO-USDT",  "ENS-USDT",
    "OP-USDT",     "ALGO-USDT",   "LDO-USDT",    "STRK-USDT",
    "DYM-USDT",    "BIO-USDT",    "POL-USDT",    "DASH-USDT",
    "ETC-USDT",    "EIGEN-USDT",
    # ── Vol bajo-medio (100k–500k USD/24h) ──────────────────────────────────
    "NIL-USDT",    "HEMI-USDT",   "SYRUP-USDT",  "INIT-USDT",
    "SAND-USDT",   "CC-USDT",     "ALT-USDT",    "MANTA-USDT",
    "STABLE-USDT", "ATOM-USDT",   "PURR-USDT",   "AR-USDT",
    "MELANIA-USDT","MINA-USDT",   "MEME-USDT",   "AVNT-USDT",
    "W-USDT",      "KAS-USDT",    "APE-USDT",    "FIL-USDT",
    "ZK-USDT",     "RENDER-USDT", "MNT-USDT",    "SNX-USDT",
    "GOAT-USDT",   "MOODENG-USDT","SKR-USDT",    "IMX-USDT",
    "SAGA-USDT",   "BSV-USDT",    "STBL-USDT",   "SKY-USDT",
    "ACE-USDT",    "VINE-USDT",   "ANIME-USDT",  "2Z-USDT",
    "CELO-USDT",   "FOGO-USDT",   "IO-USDT",     "ME-USDT",
    "BLUR-USDT",   "STX-USDT",    "kFLOKI-USDT", "PEOPLE-USDT",
    "CAKE-USDT",   "COMP-USDT",   "BANANA-USDT", "0G-USDT",
    "RUNE-USDT",   "ZEN-USDT",    "LAYER-USDT",  "AIXBT-USDT",
    "REZ-USDT",    "HYPER-USDT",  "BABY-USDT",   "AZTEC-USDT",
    "GRIFFAIN-USDT","kLUNC-USDT", "GMT-USDT",    "LINEA-USDT",
    "ZETA-USDT",   "APEX-USDT",   "GALA-USDT",   "BOME-USDT",
    "BRETT-USDT",  "ZORA-USDT",   "NXPC-USDT",   "SUPER-USDT",
    "TURBO-USDT",  "SUSHI-USDT",  "GMX-USDT",    "MERL-USDT",
    "NOT-USDT",    "GAS-USDT",    "PROVE-USDT",  "NEO-USDT",
    "IOTA-USDT",   "YGG-USDT",    "CFX-USDT",    "BIGTIME-USDT",
    "POLYX-USDT",  "MOVE-USDT",   "WCT-USDT",    "XAI-USDT",
    "UMA-USDT",    "AERO-USDT",   "GRASS-USDT",  "kSHIB-USDT",
    "VVV-USDT",    "PENGU-USDT",  "SPX-USDT",    "FARTCOIN-USDT",
    "PUMP-USDT",   "PAXG-USDT",   "WLFI-USDT",   "RESOLV-USDT",
    "XPL-USDT",    "MON-USDT",    "MET-USDT",
]

# Pares en modo alerta manual (no se tradean automáticamente)
MANUAL_ALERT_SYMBOLS: set[str] = set()

# ── Blacklist (vacía por defecto) ───────────────────────────────────────────
BLACKLIST_SYMBOLS = set(os.getenv("BLACKLIST_SYMBOLS", "").split(","))
BLACKLIST_SYMBOLS = {s.strip() for s in BLACKLIST_SYMBOLS if s.strip()}

# ── Grupos de correlación ────────────────────────────────────────────────────
# Regla: dentro de cada grupo solo pueden abrirse MAX_CORR_PER_GROUP posiciones.
# Criterio: correlación de precio alta y narrativa compartida.
CORR_GROUPS: list[set[str]] = [
    # BTC / ETH core + derivados de staking ETH
    {"BTC-USDT", "ETH-USDT", "ETHFI-USDT", "LDO-USDT", "STX-USDT"},

    # Ecosistema Solana
    {"SOL-USDT", "JUP-USDT", "JTO-USDT", "WIF-USDT", "kBONK-USDT", "kPEPE-USDT",
     "PYTH-USDT", "POPCAT-USDT", "PNUT-USDT", "kNEIRO-USDT", "PURR-USDT"},

    # L1 de alta capitalización (Avalanche, NEAR, Aptos, Sui, Algorand, ICP)
    {"AVAX-USDT", "NEAR-USDT", "APT-USDT", "SUI-USDT", "ALGO-USDT", "ICP-USDT",
     "TIA-USDT", "ASTER-USDT", "DOT-USDT"},

    # L2 Ethereum + zkRollups
    {"ARB-USDT", "OP-USDT", "STRK-USDT", "ZK-USDT", "LINEA-USDT", "MANTA-USDT",
     "DYM-USDT", "POL-USDT", "ZRO-USDT"},

    # DeFi blue chips (lending, DEX, derivados)
    {"AAVE-USDT", "UNI-USDT", "CRV-USDT", "PENDLE-USDT", "MORPHO-USDT",
     "GMX-USDT", "SNX-USDT", "SUSHI-USDT", "COMP-USDT", "DYDX-USDT", "CAKE-USDT",
     "SYRUP-USDT", "APEX-USDT", "SKY-USDT", "STABLE-USDT", "STBL-USDT"},

    # Oráculos, datos on-chain e infraestructura Web3
    {"LINK-USDT", "ONDO-USDT", "ENS-USDT", "KAITO-USDT", "AVNT-USDT", "UMA-USDT",
     "POLYX-USDT", "EIGEN-USDT"},

    # AI / Compute / DePin
    {"FET-USDT", "VIRTUAL-USDT", "RENDER-USDT", "AIXBT-USDT", "IO-USDT", "0G-USDT",
     "TAO-USDT", "WLD-USDT", "BIO-USDT", "GRASS-USDT"},

    # Pagos / XRP-esfera / CBDC-adjacent
    {"XRP-USDT", "XLM-USDT", "TRX-USDT", "HBAR-USDT", "ADA-USDT"},

    # Interoperabilidad / Cosmos / IBC
    {"ATOM-USDT", "INJ-USDT", "SEI-USDT", "RUNE-USDT", "W-USDT", "ALT-USDT",
     "SAGA-USDT", "ZETA-USDT", "MINA-USDT", "CELO-USDT"},

    # L1 nuevas generación + Move VM
    {"BERA-USDT", "S-USDT", "MOVE-USDT", "IOTA-USDT", "INIT-USDT", "AZTEC-USDT",
     "NIL-USDT", "HEMI-USDT", "FOGO-USDT", "LAYER-USDT", "ZORA-USDT",
     "MON-USDT", "MET-USDT"},

    # Legacy PoW / fork coins
    {"BCH-USDT", "LTC-USDT", "ZEC-USDT", "XMR-USDT", "DASH-USDT", "ETC-USDT",
     "BSV-USDT", "ZEN-USDT", "PAXG-USDT"},

    # Bitcoin L2 / Ordinals / ecosystem
    {"ORDI-USDT", "MERL-USDT", "GAS-USDT", "NEO-USDT"},

    # Gaming / Metaverse / NFT infrastructure
    {"AXS-USDT", "SAND-USDT", "IMX-USDT", "YGG-USDT", "BIGTIME-USDT", "GALA-USDT",
     "ACE-USDT", "SUPER-USDT", "XAI-USDT", "GMT-USDT", "BLUR-USDT", "ME-USDT"},

    # Memes puros
    {"DOGE-USDT", "TRUMP-USDT", "MELANIA-USDT", "MEME-USDT", "GOAT-USDT",
     "MOODENG-USDT", "BOME-USDT", "TURBO-USDT", "BRETT-USDT", "VINE-USDT",
     "BABY-USDT", "GRIFFAIN-USDT", "ANIME-USDT", "BANANA-USDT", "SKR-USDT",
     "PEOPLE-USDT", "CHIP-USDT", "MEGA-USDT", "NOT-USDT", "kFLOKI-USDT",
     "kLUNC-USDT", "HMSTR-USDT", "NXPC-USDT", "FARTCOIN-USDT", "PUMP-USDT",
     "PENGU-USDT", "SPX-USDT", "kSHIB-USDT"},

    # Política / eventos (alta correlación entre sí)
    {"WLFI-USDT", "TRUMP-USDT", "MELANIA-USDT"},

    # DeFi yield / restaking emergente
    {"RESOLV-USDT", "AERO-USDT", "VVV-USDT"},

    # Misc / tokens con dinámica propia (standalone)
    {"BNB-USDT"},
    {"HYPE-USDT"},
    {"ENA-USDT"},
    {"GRAM-USDT"},
    {"KAS-USDT"},
    {"TRB-USDT"},
    {"CC-USDT"},
    {"PROVE-USDT"},
    {"HYPER-USDT"},
    {"REZ-USDT"},
    {"FIL-USDT"},
    {"AR-USDT"},
    {"MNT-USDT"},
    {"APE-USDT"},
    {"WCT-USDT"},
    {"CFX-USDT"},
    {"2Z-USDT"},
    {"LIT-USDT"},
    {"XPL-USDT"},
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

# ── Parámetros ajustables por el optimizador ──────────────────────────────
SHORT_MIN_SCORE_EXTRA = int(os.getenv("SHORT_MIN_SCORE_EXTRA", "0"))

# Telegram
TG_TOKEN       = os.getenv("TELEGRAM_TOKEN", "")
TG_CHAT_ID     = os.getenv("TELEGRAM_CHAT_ID", "")
