from bot.core.bingx_client import BingXClient, _BingXCore, _norm_coin, _to_symbol

# Alias de compatibilidad: cualquier import antiguo de OKXClient
# o HLClient apuntará a BingXClient.
OKXClient = BingXClient
_OKXCore  = _BingXCore
HLClient  = BingXClient
_HLCore   = _BingXCore

# _to_inst_id es alias de _to_symbol (mismo comportamiento)
_to_inst_id = _to_symbol
