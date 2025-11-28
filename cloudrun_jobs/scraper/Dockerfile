# --- Dockerfile ---
FROM python:3.11-slim

# Installation of system dependencies for Chromium
RUN apt-get update && apt-get install -y \
    chromium chromium-driver \
    libatk-bridge2.0-0 libgtk-3-0 libgbm-dev libx11-xcb1 \
    libxcomposite1 libxdamage1 libxrandr2 libasound2 \
    fonts-liberation libnss3 libxshmfence-dev \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Setting environment variable for pyppeteer
ENV PYPPETEER_EXECUTABLE_PATH=/usr/bin/chromium

# Setting the working directory
WORKDIR /app

# Copying project files
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py .

# Running the scraper.py script
CMD ["python", "main.py"]
