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
# Cobertura: los 47 pares de SYMBOLS están todos asignados a al menos un grupo.
# Pares sin grupo quedan fuera del guard de correlación (único riesgo: no
# limitaría su apertura simultánea con pares correlacionados).
#
# Grupos (8) con MAX_CORR_PER_GROUP=2:
#   mega_caps   (3): BTC, ETH, BNB             — correlación ~0.95
#   l1s         (6): SOL, AVAX, APT, SUI, NEAR, TIA — alt-season rotation
#   l2s_infra   (6): ARB, OP, DOT, ICP, POL, ZK    — L2/infra sector
#   defi        (5): LINK, UNI, AERO, JUP, ONDO    — DeFi
#   pagos       (6): XRP, XLM, TRX, HBAR, XLM, ADA — pagos/RippleNet
#   ai_infra    (6): FIL, RENDER, TAO, EIGEN, FET, WLD — AI/storage
#   cosmos_eco  (4): ATOM, INJ, SEI, MANTA          — ecosistema Cosmos/IBC
#   misc_alts   (7): DOGE, SHIB, NOT, PENGU, VET, STX, LISTA — memes+misc
CORR_GROUPS: list[set[str]] = [
    # Mega caps — BTC/ETH/BNB se mueven casi idénticos
    {"BTC-USDT", "ETH-USDT", "BNB-USDT"},
    # L1s alternativas — rotan juntas en alt-season
    {"SOL-USDT", "AVAX-USDT", "APT-USDT", "SUI-USDT", "NEAR-USDT", "TIA-USDT"},
    # L2s e infraestructura — se mueven juntas en narrativas de escalado
    {"ARB-USDT", "OP-USDT", "DOT-USDT", "ICP-USDT", "POL-USDT", "ZK-USDT"},
    # DeFi — correlacionadas en narrativas DeFi
    {"LINK-USDT", "UNI-USDT", "AERO-USDT", "JUP-USDT", "ONDO-USDT"},
    # Pagos / RippleNet — alta correlación en noticias regulatorias
    {"XRP-USDT", "XLM-USDT", "TRX-USDT", "HBAR-USDT", "ADA-USDT"},
    # AI e infraestructura de almacenamiento — narrativa AI crypto
    {"FIL-USDT", "RENDER-USDT", "TAO-USDT", "EIGEN-USDT", "FET-USDT", "WLD-USDT"},
    # Cosmos ecosystem — IBC, Cosmos SDK
    {"ATOM-USDT", "INJ-USDT", "SEI-USDT", "MANTA-USDT"},
    # Memes + misc alts — alta correlación en eventos de mercado especulativos
    # BCH, LTC, ETC agrupados aquí como PoW-forks (correlación alta con BTC en
    # rallies pero con beta diferente — grupo separado de mega_caps a propósito)
    {"DOGE-USDT", "SHIB-USDT", "NOT-USDT", "PENGU-USDT",
     "VET-USDT",  "STX-USDT",  "LISTA-USDT",
     "BCH-USDT",  "LTC-USDT",  "ETC-USDT"},
    # Infra modular + perp ecosystems — HYPE (HyperLiquid) y MNT (Mantle L2)
    # tienen correlación moderada con L2s pero narrativa distinta; se agrupan
    # juntos para no contaminar el grupo l2s_infra principal
    {"HYPE-USDT", "MNT-USDT"},
]
MAX_CORR_PER_GROUP = int(os.getenv("MAX_CORR_PER_GROUP", "2"))

# Verificación en tiempo de importación: avisa si algún símbolo de SYMBOLS
# no tiene grupo asignado (el guard de correlación no aplicaría para él).
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

# Máximo de posiciones en la misma dirección (long o short) simultáneamente.
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
