FROM python:3.12-slim

WORKDIR /app

# Ensure pkg_resources / setuptools is always present BEFORE any other deps
RUN pip install --no-cache-dir --upgrade pip setuptools wheel

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python", "main.py"]
