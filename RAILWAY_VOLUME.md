# Volumen persistente en Railway

El bot guarda el estado de posiciones abiertas en un fichero JSON.
En Railway, el filesystem es **ephemeral**: se borra en cada redeploy.
Para que el estado sobreviva, necesitas un **Volume** de Railway.

## Pasos (5 minutos)

1. Abre tu proyecto en [railway.app](https://railway.app)
2. Haz clic en tu servicio → pestaña **Volumes**
3. Pulsa **New Volume**
4. Configura:
   - **Mount path**: `/data`
   - Tamaño: 1 GB es más que suficiente
5. Guarda y Railway redespliega automáticamente

El bot detecta `/data` automáticamente y guarda el estado en
`/data/bot_state.json`. No necesitas tocar ninguna variable de entorno.

## Verificar que funciona

En los logs del bot verás:
```
Estado persistente en Railway Volume: /data/bot_state.json
```

Si en cambio ves el warning de ephemeral, el volume no está montado correctamente.

## Alternativa: variable de entorno

Si prefieres otra ruta, configura en Railway:
```
STATE_FILE=/tu/ruta/bot_state.json
```
