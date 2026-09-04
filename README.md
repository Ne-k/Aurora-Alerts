# Aurora-Alerts (Discord Bot)

This repo now includes a Discord bot that posts and updates an aurora forecast message per guild, and sends an extra alert if Kp rises to your configured threshold.

## Features

- Slash commands to configure per-guild settings:
  - `/aurora-set-channel` – select the target channel
  - `/aurora-set-threshold` – set the Kp threshold
  - `/aurora-set-location` – set latitude/longitude and a display name
  - `/aurora-start` – post the initial embed
  - `/aurora-show` – preview current content ephemerally
- Works out **when you can actually see it**, by intersecting above-threshold Kp
  windows with local darkness and cloud cover, and says why when nothing lines up
- Background updater runs every 2 hours (configurable via `UPDATE_INTERVAL_HOURS`), retrying after `UPDATE_RETRY_MINUTES` if a cycle fails
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
- `/aurora-refresh` – rebuild the tracked embed now
- `/aurora-stop` – stop updates in this server
- `/aurora-health` – per-source status of every upstream feed

## Data sources

| Source | Used for |
| --- | --- |
| NOAA SWPC 3-day forecast | Kp per 3-hour block |
| SWPC planetary K, 1-minute | Current Kp |
| SWPC propagated solar wind | Bz, speed, density at Earth |
| SWPC alerts | Official geomagnetic watches and warnings |
| SWPC Ovation, hemispheric power | Nowcast probability and energy input |
| GFZ Potsdam | Independent Kp series |
| Open-Meteo | Cloud cover split into low, mid and high |
| Computed locally | Sun altitude, moon phase and altitude, geomagnetic latitude |

Solar wind is the only feed that leads rather than lags. Kp is a three-hour average
published after the fact, while southward Bz at L1 drives the substorm that follows.
Sun and moon positions are computed from latitude, longitude and time, so they cost
no network call and cannot break when a third-party site goes down.

Auroral oval boundaries are quoted in geomagnetic latitude, which in North America
runs several degrees higher than geographic. Portland sits at 45.5 degrees
geographic but 51.0 degrees geomagnetic, so comparing the two directly understated
every score. Positions use a centered dipole, accurate to about a degree in North
America and less so near the eccentric-dipole extremes.

## Reliability

All outbound HTTP goes through one pooled session in `aurora/net.py` with a hard cap
on sockets per host, bounded retries, and a short TTL cache. Before this, every fetch
built its own connection pool and the per-call cloudscraper session was never closed,
so the container leaked file descriptors until every source failed with
`OSError: [Errno 24] Too many open files` and the embed stopped updating.

Behaviour that follows from that:

- The health sweep runs once at startup, not on every gateway reconnect.
- A failed cycle keeps the embed alive: it shows the last good data with a visible
  "live data unavailable" banner instead of going silently stale.
- If the tracked message is deleted, the bot reposts on the next cycle rather than
  clearing the channel configuration.
- The updater writes `data/heartbeat`; `python -m aurora.healthcheck` fails when that
  file goes stale, which surfaces as an unhealthy container in `docker ps`. Plain
  Docker does not restart on health failure, so treat it as a signal, not a fix.

## Tests

```
python -m unittest discover -s tests -p "test_*.py"
```

## Notes

- Legacy one-off webhook script has been removed; use `python -m aurora.bot` for all alerting flows.
- The bot avoids hardcoding secrets; set `DISCORD_BOT_TOKEN` in `.env` or your environment.
- Ephemeral high-Kp alerts: On each scheduled update the bot compares the new detection signature (day:UT-block:Kp) against the previous one stored in SQLite. Any newly added above-threshold windows are announced in a transient message (with consolidated sources line if available) and deleted after `ALERT_DELETE_AFTER_MINUTES`.
