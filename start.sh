#!/usr/bin/env bash
set -euo pipefail

# Prepare cron job
echo '0 8 * * * cd /app && python -u "noaa alert.py" >> /var/log/cron.log 2>&1' > /etc/cron.d/noaa_alert
chmod 0644 /etc/cron.d/noaa_alert
crontab /etc/cron.d/noaa_alert

# Ensure log file exists
touch /var/log/cron.log

# Startup messages
echo "[UTC $(date -u +'%F %T')] Container started. Cron scheduled for 08:00 UTC daily." >> /var/log/cron.log
echo "[UTC $(date -u +'%F %T')] Running once on startup..." >> /var/log/cron.log

# Run one immediate check
cd /app
python -u "noaa alert.py" >> /var/log/cron.log 2>&1 || true

# Start cron and stream logs to keep the container running
cron
exec tail -F /var/log/cron.log
