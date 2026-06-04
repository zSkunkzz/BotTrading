"""
conftest.py — configuración global de pytest para BotTrading.
Añade el root del proyecto al sys.path para que los imports funcionen
tanto en local como en CI/CD sin instalar el paquete.
"""
import sys
import os

sys.path.insert(0, os.path.dirname(__file__))
