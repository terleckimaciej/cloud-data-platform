FROM python:3.11-slim

# unikamy interakcji apt
ENV DEBIAN_FRONTEND=noninteractive

# instalacja Chromium i bibliotek
RUN apt-get update && apt-get install -y \
    chromium chromium-driver \
    libatk-bridge2.0-0 libgtk-3-0 libgbm-dev libx11-xcb1 \
    libxcomposite1 libxdamage1 libxrandr2 libasound2 \
    fonts-liberation libnss3 libxshmfence-dev \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

ENV PYPPETEER_EXECUTABLE_PATH=/usr/bin/chromium

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py .

CMD ["python", "main.py"]
