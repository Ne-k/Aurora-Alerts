FROM python:3.11-slim

# Install cron and bash
RUN apt-get update && apt-get install -y cron bash

WORKDIR /app

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

COPY . .

# Make sure the script file has proper line endings and is executable
RUN chmod +x /app/run.sh

# Start cron in the foreground
CMD ["cron", "-f"]