FROM python:3.11-slim

# Install cron and bash
RUN apt-get update && apt-get install -y --no-install-recommends cron bash \
	&& rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Normalize line endings and make startup script executable
RUN sed -i 's/\r$//' /app/start.sh && chmod +x /app/start.sh

# Default command runs the startup script; compose can override if needed
CMD ["/app/start.sh"]