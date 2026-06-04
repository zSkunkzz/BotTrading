"""
bot/kelly_sizer.py — shim de retrocompatibilidad.

Todo el código real vive en bot/risk/kelly.py.
Este archivo permite que imports legacy (p.ej. `import bot.kelly_sizer`)
sigan funcionando sin cambios hasta que sean migrados.
"""
from bot.risk.kelly import *        # noqa: F401, F403
from bot.risk.kelly import KellySizer, _parse_int_env  # noqa: F401 (reexport explícito)
