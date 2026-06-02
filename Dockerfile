FROM python:3.12-slim

WORKDIR /app

# setuptools provides pkg_resources — must be installed first
RUN pip install --no-cache-dir --upgrade pip setuptools wheel

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Patch eth_keyfile to not crash on Python 3.12:
# eth_keyfile/__init__.py does `import pkg_resources` which fails because
# pkg_resources is not auto-importable in Python 3.12 even with setuptools installed.
# We overwrite it with a patched version that imports from importlib.metadata instead.
RUN python - <<'EOF'
import site, os, re
for sp in site.getsitepackages():
    init = os.path.join(sp, "eth_keyfile", "__init__.py")
    if os.path.exists(init):
        content = open(init).read()
        if "import pkg_resources" in content:
            patched = content.replace(
                "import pkg_resources",
                "try:\n    import pkg_resources\nexcept ImportError:\n    import importlib.metadata as pkg_resources"
            )
            open(init, "w").write(patched)
            print(f"Patched {init}")
EOF

COPY . .

CMD ["python", "main.py"]
