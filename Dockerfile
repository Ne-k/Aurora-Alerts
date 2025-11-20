# Use Python 3.12 to avoid stdlib removals (audioop) breaking discord.py
FROM python:3.12-slim

# Install runtime deps (git optional for some packages), create non-root user
RUN apt-get update \
	&& apt-get install -y --no-install-recommends bash tzdata ca-certificates \
	&& rm -rf /var/lib/apt/lists/* \
	&& useradd -m -u 1000 botuser

WORKDIR /app

# Copy requirements first for layer caching
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy application source
COPY aurora ./aurora
COPY README.md ./README.md
COPY .env.example ./

# Ensure data directory exists and set ownership
RUN mkdir -p /app/data && chown -R botuser:botuser /app

USER botuser

# Environment defaults (can be overridden in compose)
ENV PYTHONUNBUFFERED=1 \
	UPDATE_INTERVAL_HOURS=2 \
	ALERT_DELETE_AFTER_MINUTES=15

# Run the Discord bot directly
CMD ["python", "-m", "aurora.bot"]