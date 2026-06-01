# webhook.py – Patch Notes

## 1. WEBHOOK_SECRET must be mandatory in production

### ANTES
```python
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "")
```

### DESPUÉS
```python
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "")
if not WEBHOOK_SECRET and os.getenv("RAILWAY_ENVIRONMENT"):
    raise RuntimeError(
        "WEBHOOK_SECRET env var is required in production. "
        "Set it in Railway → Variables."
    )
```

---

## 2. /health endpoint should require a key

### ANTES
```python
@app.get("/health")
async def health():
    return {"status": "ok"}
```

### DESPUÉS
```python
_HEALTH_KEY = os.getenv("HEALTH_KEY", "")

@app.get("/health")
async def health(request: Request):
    if _HEALTH_KEY and request.headers.get("X-Health-Key") != _HEALTH_KEY:
        raise HTTPException(status_code=403, detail="Forbidden")
    return {"status": "ok"}
```
