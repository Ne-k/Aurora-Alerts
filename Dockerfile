# Use Python 3.12 to keep discord.py voice dependencies available
FROM python:3.12-slim

# Install runtime deps (tzdata for schedule conversions)
RUN apt-get update \
	&& apt-get install -y --no-install-recommends bash tzdata ca-certificates \
	&& rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy requirements first for layer caching
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy application source
COPY aurora ./aurora
COPY README.md ./README.md
COPY forecastExample.txt ./forecastExample.txt
COPY alert_state.json ./alert_state.json

# Ensure runtime data directory exists
RUN mkdir -p /app/data

# Environment defaults (can be overridden in compose)
ENV PYTHONUNBUFFERED=1 \
	UPDATE_INTERVAL_HOURS=2 \
	ALERT_DELETE_AFTER_MINUTES=15

# Run the Discord bot directly
CMD ["python", "-m", "aurora.bot"]