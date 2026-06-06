from bot.core.okx_client import OKXClient, _OKXCore, _norm_coin, _to_inst_id

# Alias de compatibilidad: cualquier import antiguo de HLClient
# que no se haya actualizado aún apuntará a OKXClient.
HLClient = OKXClient
_HLCore  = _OKXCore
