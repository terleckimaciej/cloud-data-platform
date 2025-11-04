# --- Dockerfile ---
FROM python:3.11-slim

# Instalacja zależności systemowych dla Chromium
RUN apt-get update && apt-get install -y \
    chromium chromium-driver \
    libatk-bridge2.0-0 libgtk-3-0 libgbm-dev libx11-xcb1 \
    libxcomposite1 libxdamage1 libxrandr2 libasound2 \
    fonts-liberation libnss3 libxshmfence-dev \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Ustawienie zmiennej środowiskowej dla pyppeteer
ENV PYPPETEER_EXECUTABLE_PATH=/usr/bin/chromium

# Ustawienie katalogu roboczego
WORKDIR /app

# Kopiowanie plików projektu
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py .

# Uruchamianie skryptu scraper.py
CMD ["python", "main.py"]
