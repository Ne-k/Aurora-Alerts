FROM python:3.11-slim

# Install cron and bash
RUN apt-get update && apt-get install -y --no-install-recommends cron bash \
	&& rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Default command is provided by docker-compose (starts cron and tails logs)
CMD ["bash", "-lc", "echo 'Use docker-compose to start services'"]