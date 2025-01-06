FROM python:3.11-slim

# Install cron
RUN apt-get update && apt-get install -y cron

WORKDIR /app

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

COPY . .

# Start cron in the foreground
CMD ["cron", "-f"]