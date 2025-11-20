# Aurora-Alerts (Discord Bot)

This repo now includes a Discord bot that posts and updates an aurora forecast message per guild, and sends an extra alert if Kp rises to your configured threshold.

## Features

- Slash commands to configure per-guild settings:
  - `/aurora-set-channel` – select the target channel
  - `/aurora-set-threshold` – set the Kp threshold
  - `/aurora-set-location` – set latitude/longitude and a display name
  - `/aurora-start` – post the initial embed
  - `/aurora-show` – preview current content ephemerally
- Background updater runs every 2 hours (configurable via `UPDATE_INTERVAL_HOURS`)
- When a new 3-day forecast window meets or exceeds your threshold, the bot sends an extra ephemeral alert message listing only the newly added high-Kp window(s); this auto-deletes after a configurable delay
- Uses SQLite for persistent per-guild configuration (`data/aurora.db`)

## Setup

1. Create a bot in the Discord Developer Portal and invite it to your server with the `applications.commands` and bot permissions to send messages.
2. Configure environment:

Create a `.env` file:

```
DISCORD_BOT_TOKEN=your_bot_token_here
KP_THRESHOLD=6.5
LATITUDE=45.5152
LONGITUDE=-122.6784
LOCATION_NAME=Portland, OR
UPDATE_INTERVAL_HOURS=2
ALERT_DELETE_AFTER_MINUTES=15
```

3. Install dependencies:

```
python -m venv .venv
. .venv/Scripts/activate
pip install -r requirements.txt
```

4. Run the bot:

Local:
```
python -m aurora.bot
```

Docker (build & run):
```
docker compose build
docker compose up -d
```
The container uses `python -m aurora.bot` directly; legacy cron + `noaa alert.py` has been removed.

## Commands

- `/aurora-set-channel #channel`
- `/aurora-set-threshold 6.5`
- `/aurora-set-location 45.5152 -122.6784 "Portland, OR"`
- `/aurora-start` – posts the initial embed in the configured channel
- `/aurora-show` – shows a preview ephemerally to you
- `/aurora-next-30` – quick-look probability for the next 30 minutes
- `/aurora-gfz-hourly` – latest GFZ Kp values plus NOAA outlook in a dedicated embed

## Notes

- Legacy one-off webhook script `noaa alert.py` has been deprecated in the Docker flow (still in repo if needed manually).
- The bot avoids hardcoding secrets; set `DISCORD_BOT_TOKEN` in `.env` or your environment.
- Ephemeral high-Kp alerts: On each scheduled update the bot compares the new detection signature (day:UT-block:Kp) against the previous one stored in SQLite. Any newly added above-threshold windows are announced in a transient message (with consolidated sources line if available) and deleted after `ALERT_DELETE_AFTER_MINUTES`.
