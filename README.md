# BitgetProBot v5.0

Bot de trading de futuros perpetuos en Bitget con IA como motor de decisiones.

## Características
- **IA decide** BUY / SELL / HOLD / CLOSE en cada ciclo (Groq → Gemini → fallback técnico)
- **Scanner dinámico**: detecta todos los pares USDT perpetuos de Bitget en tiempo real
- **Multi-par**: opera hasta N pares en paralelo con riesgo global controlado
- **Filtro de confianza**: solo opera cuando la IA tiene confianza ≥ AI_MIN_CONFIDENCE
- **Railway ready**: deploy directo

## Variables clave

```
BITGET_API_KEY / SECRET / PASSPHRASE
GROQ_API_KEY        — IA principal
GEMINI_API_KEY      — IA fallback
AI_MIN_CONFIDENCE   — umbral mínimo de confianza (1-10, recomendado: 6)
DRY_RUN=true        — modo simulación (cambiar a false para operar real)
TOP_PAIRS=15        — cuántos pares analizar en paralelo
MAX_CONCURRENT_TRADES=3  — máx posiciones abiertas simultáneas
```

## Arranque

```bash
pip install -r requirements.txt
cp .env.example .env  # rellenar credenciales
python main.py
```
