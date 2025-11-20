from __future__ import annotations
import os
import asyncio
from datetime import datetime, timezone
import logging
from typing import Optional, List, cast
import tempfile
import requests
import io

import discord
from discord import app_commands
from discord.ext import tasks
from dotenv import load_dotenv

from .db import init_db, get_config, set_channel, set_threshold, set_location, set_message_id, set_last_window, clear_channel
from .forecast import ForecastEngine, AlertBuild

load_dotenv()
logging.basicConfig(level=logging.INFO)

DISCORD_TOKEN = os.getenv('DISCORD_BOT_TOKEN', '')
DISCORD_CLIENT_ID = os.getenv('DISCORD_CLIENT_ID')
DEFAULT_KP = float(os.getenv('KP_THRESHOLD', '6.5'))
DEFAULT_LAT = float(os.getenv('LATITUDE', '45.5152'))
DEFAULT_LON = float(os.getenv('LONGITUDE', '-122.6784'))
DEFAULT_LOC = os.getenv('LOCATION_NAME', 'Portland, OR')
DEFAULT_TZ = os.getenv('TIMEZONE_NAME', 'America/Los_Angeles')

intents = discord.Intents.default()
intents.guilds = True
if DISCORD_CLIENT_ID:
    try:
        app_id = int(DISCORD_CLIENT_ID)
    except Exception:
        app_id = None
else:
    app_id = None

bot = discord.Client(intents=intents, application_id=app_id) if app_id else discord.Client(intents=intents)
tree = app_commands.CommandTree(bot)

# Explicit guard to prevent duplicate loop starts (e.g., multiple on_ready events)
_UPDATER_STARTED = False
_LAST_HEALTH: dict | None = None

# Background task interval in hours
UPDATE_INTERVAL_HOURS = float(os.getenv('UPDATE_INTERVAL_HOURS', '2'))
ALERT_DELETE_AFTER_MINUTES = int(os.getenv('ALERT_DELETE_AFTER_MINUTES', '15'))  # ephemeral high-Kp alert lifetime
STARTUP_HEALTH_BLOCK = os.getenv('STARTUP_HEALTH_BLOCK', 'true').strip().lower() in ('1','true','yes')
STARTUP_HEALTH_TIMEOUT = int(os.getenv('STARTUP_HEALTH_TIMEOUT_SECONDS', '25'))

REQUIRED_SOURCES = ["noaa_forecast", "gfz", "swpc_planetary"]
OPTIONAL_SOURCES = ["cloud_cover", "ovation", "maf", "afm_snapshot", "swpc_hemi"]

def _engine_for_guild(cfg: Optional[dict]) -> ForecastEngine:
    if not cfg:
        return ForecastEngine(kp_threshold=DEFAULT_KP, latitude=DEFAULT_LAT, longitude=DEFAULT_LON, location_name=DEFAULT_LOC, timezone_name=DEFAULT_TZ)
    return ForecastEngine(
        kp_threshold=float(cfg.get('kp_threshold') or DEFAULT_KP),
        latitude=float(cfg.get('latitude') or DEFAULT_LAT),
        longitude=float(cfg.get('longitude') or DEFAULT_LON),
        location_name=str(cfg.get('location_name') or DEFAULT_LOC),
        timezone_name=DEFAULT_TZ,
    )

async def _run_blocking(fn, *args, timeout: int = 20):
    """Run a blocking function in executor with a timeout."""
    loop = asyncio.get_running_loop()
    return await asyncio.wait_for(loop.run_in_executor(None, lambda: fn(*args)), timeout=timeout)

async def perform_startup_health(engine: ForecastEngine) -> dict:
    """Check external data sources; returns dict of booleans + timestamp."""
    started = datetime.now(timezone.utc)
    results = {}
    async def safe_call(label: str, coro):
        try:
            val = await coro
            results[label] = val
        except Exception:
            logging.exception(f"Health check failed for {label}")
            results[label] = None
    await asyncio.gather(
        safe_call('noaa_raw', _run_blocking(engine.fetch_forecast, timeout=STARTUP_HEALTH_TIMEOUT)),
        safe_call('gfz_raw', _run_blocking(engine.gfz_recent_blocks, 24, timeout=STARTUP_HEALTH_TIMEOUT)),
        safe_call('ovation_raw', _run_blocking(engine.fetch_ovation_probability, engine.latitude, engine.longitude, timeout=STARTUP_HEALTH_TIMEOUT)),
        safe_call('maf_raw', _run_blocking(engine.fetch_maf_data, engine.latitude, engine.longitude, engine.timezone_name, timeout=STARTUP_HEALTH_TIMEOUT)),
        safe_call('cloud_raw', _run_blocking(engine.fetch_cloud_cover, timeout=STARTUP_HEALTH_TIMEOUT)),
        safe_call('afm_raw', _run_blocking(engine.fetch_aurora_snapshot, engine.latitude, engine.longitude, timeout=STARTUP_HEALTH_TIMEOUT)),
        safe_call('swpc_planetary_raw', _run_blocking(engine.fetch_swpc_planetary_k_latest, timeout=STARTUP_HEALTH_TIMEOUT)),
        safe_call('swpc_hemi_raw', _run_blocking(engine.fetch_swpc_hemi_power, timeout=STARTUP_HEALTH_TIMEOUT)),
    )
    health = {
        'noaa_forecast': isinstance(results.get('noaa_raw'), str) and 'NOAA Kp index breakdown' in (results.get('noaa_raw') or ''),
        'gfz': isinstance(results.get('gfz_raw'), dict) and bool((results.get('gfz_raw') or {}).get('records')),
        'ovation': isinstance(results.get('ovation_raw'), int),
        'maf': isinstance(results.get('maf_raw'), dict) and len(results.get('maf_raw') or {}) > 0,
        'cloud_cover': isinstance(results.get('cloud_raw'), dict) and len(results.get('cloud_raw') or {}) > 0,
        'afm_snapshot': isinstance(results.get('afm_raw'), dict) and 'tonight' in (results.get('afm_raw') or {}),
        'swpc_planetary': isinstance(results.get('swpc_planetary_raw'), dict),
        'swpc_hemi': isinstance(results.get('swpc_hemi_raw'), dict),
        'checked_at': int(started.timestamp()),
    }
    summary_parts = []
    for k in REQUIRED_SOURCES + OPTIONAL_SOURCES:
        if k in health:
            summary_parts.append(f"{k}={'OK' if health[k] else 'FAIL'}")
    logging.info("Startup health: " + ", ".join(summary_parts))
    return health

@tasks.loop(minutes=30)
async def health_refresher():
    """Periodic refresh of external source health so /aurora-health stays current."""
    if not bot.is_ready():
        return
    engine = _engine_for_guild(None)
    try:
        health = await perform_startup_health(engine)
        global _LAST_HEALTH
        _LAST_HEALTH = health
    except Exception:
        logging.exception("Health refresher failed")

def format_embed(build: Optional[AlertBuild], engine: Optional[ForecastEngine]) -> discord.Embed:
    detected_ts = int(datetime.now(timezone.utc).timestamp())
    loc_name = engine.location_name if engine else os.getenv('LOCATION_NAME', 'Location')
    lat = engine.latitude if engine else float(os.getenv('LATITUDE', '0') or 0)
    lon = engine.longitude if engine else float(os.getenv('LONGITUDE', '0') or 0)
    desc = (
        f"Location: {loc_name} ({lat:.4f}, {lon:.4f})\n"
        f"Updated: <t:{detected_ts}:R>\n"
        f"[SWPC Aurora Dashboard](https://www.swpc.noaa.gov/communities/aurora-dashboard-experimental)"
    )
    embed = discord.Embed(title="Aurora viewing windows", description=desc, color=0x00ffcc)
    # Dual image logic with cache-busting: tonight as main image, tomorrow as thumbnail. Fallback if tonight missing.
    if build:
        tonight = (build.tonight_image_url or '').strip()
        tomorrow = (build.tomorrow_image_url or '').strip()
        # Cache bust token: floor by interval to avoid excessive re-fetching.
        interval_min = int(os.getenv('IMAGE_CACHE_BUST_INTERVAL_MIN', '30') or '30')
        if interval_min < 1:
            interval_min = 1
        bust_token = int(detected_ts // (interval_min * 60))
        def _bust(url: str) -> str:
            if not url:
                return url
            sep = '&' if ('?' in url) else '?'
            return f"{url}{sep}v={bust_token}"
        tonight_busted = _bust(tonight)
        tomorrow_busted = _bust(tomorrow)
        main_image = tonight_busted or tomorrow_busted
        if main_image:
            embed.set_image(url=main_image)
        if tonight_busted and tomorrow_busted and tomorrow_busted != tonight_busted:
            embed.set_thumbnail(url=tomorrow_busted)
    # Suppress individual source displays (GFZ/NOAA/AFM/MAF) per request
    # Place 3-Day NOAA table at the top of fields
    if build and build.all_forecast_lines:
        table_text_top = "\n".join(build.all_forecast_lines)
        if len(table_text_top) > 1024:
            table_text_top = table_text_top[:1000] + "\n…"
        embed.add_field(name="3-Day NOAA Kp", value=table_text_top, inline=False)
    # Remove tonight forecast/summary section per request
    # Remove AFM and MAF individual source sections
    # Kp forecasts formatted by date with UT → localized time range bullets (chunk to avoid mid-line cutoffs)
    if build and build.detection_groups:
        for date_label in sorted(build.detection_groups.keys()):
            lines = [f"{date_label}"] + list(build.detection_groups[date_label])
            chunk: List[str] = []
            chunk_len = 0
            first_chunk = True
            def flush_chunk(first: bool):
                nonlocal chunk, chunk_len, first_chunk
                if not chunk:
                    return
                name = "Kp Forecasts" if first else "Kp Forecasts (cont)"
                value = "\n".join(chunk)
                embed.add_field(name=name, value=value, inline=False)
                chunk = []
                chunk_len = 0
                first_chunk = False
            for line in lines:
                # If a single line is extremely long, hard-truncate that line safely
                safe_line = line
                if len(safe_line) > 1024:
                    safe_line = safe_line[:1000] + " …"
                add_len = (1 if chunk else 0) + len(safe_line)
                if chunk_len + add_len > 1024:
                    flush_chunk(first_chunk)
                    chunk = [safe_line]
                    chunk_len = len(safe_line)
                else:
                    if chunk:
                        chunk.append(safe_line)
                        chunk_len += 1 + len(safe_line)
                    else:
                        chunk = [safe_line]
                        chunk_len = len(safe_line)
            flush_chunk(first_chunk)
    else:
        # Keep the header with a placeholder when no above-threshold forecasts are present
        placeholder = f"No high Kp forecasts ≥ {engine.kp_threshold if engine else DEFAULT_KP} in the next 3 days."
        embed.add_field(name="Kp Forecasts", value=placeholder[:1024], inline=False)
    # Include best viewing date(s) if there are windows ≥ threshold
    if build and build.upcoming_days_lines and build.detections:
        best_text = "\n".join(build.upcoming_days_lines)
        if len(best_text) > 1024:
            best_text = best_text[:1000] + "\n…"
        embed.add_field(name="Best Viewing Dates", value=best_text, inline=False)
    # Remove 'upcoming days' summary per request
    # No individual source sections or notes appended
    return embed

async def _prepare_image_attachments(tonight_url: str | None, tomorrow_url: str | None, detected_ts: int) -> tuple[List[discord.File], dict]:
    """Download images to temporary files and prepare discord.File attachments.
    Returns (files, meta) where meta contains keys: 'main_name', 'thumb_name'.
    Deletes temp files are the caller's responsibility after send/edit completes.
    """
    interval_min = int(os.getenv('IMAGE_CACHE_BUST_INTERVAL_MIN', '30') or '30')
    if interval_min < 1:
        interval_min = 1
    bust_token = int(detected_ts // (interval_min * 60))

    def _bust(url: str) -> str:
        sep = '&' if ('?' in url) else '?'
        return f"{url}{sep}v={bust_token}"

    candidates: List[tuple[str, str]] = []  # (name, url)
    if tonight_url and tonight_url.strip():
        candidates.append(('tonight.png', _bust(tonight_url.strip())))
    if tomorrow_url and tomorrow_url.strip():
        # Avoid duplicate if identical
        busted_tom = _bust(tomorrow_url.strip())
        if not candidates or candidates[0][1] != busted_tom:
            candidates.append(('tomorrow.png', busted_tom))

    tmp_paths: List[tuple[str, str]] = []  # (name, path)
    files: List[discord.File] = []
    for name, url in candidates:
        try:
            resp = await asyncio.to_thread(lambda: requests.get(url, timeout=20))
            if resp.status_code == 200 and resp.content:
                # Write to temp file
                fd, path = tempfile.mkstemp(prefix='aurora_', suffix='_' + name)
                with os.fdopen(fd, 'wb') as f:
                    f.write(resp.content)
                tmp_paths.append((name, path))
        except Exception:
            logging.exception(f"Image download failed for {url}")
    # Build discord.File objects from temp paths
    for name, path in tmp_paths:
        try:
            fp = open(path, 'rb')
            files.append(discord.File(fp=fp, filename=name))
        except Exception:
            logging.exception(f"Failed creating discord.File for {path}")

    meta = {}
    if tmp_paths:
        # Determine main and thumb by names if present
        names = [n for n, _ in tmp_paths]
        if 'tonight.png' in names:
            meta['main_name'] = 'tonight.png'
            if 'tomorrow.png' in names:
                meta['thumb_name'] = 'tomorrow.png'
        else:
            meta['main_name'] = names[0]
            if len(names) > 1:
                meta['thumb_name'] = names[1]
    return files, meta

def _cleanup_attachments(files: List[discord.File]):
    for f in files:
        try:
            fp = getattr(f, 'fp', None)
            name = getattr(fp, 'name', None) if fp else None
            if fp and hasattr(fp, 'close'):
                try:
                    fp.close()
                except Exception:
                    pass
            if isinstance(name, str) and os.path.exists(name):
                try:
                    os.unlink(name)
                except Exception:
                    pass
        except Exception:
            pass

async def _find_latest_bot_embed(channel: discord.TextChannel) -> Optional[discord.Message]:
    """Find the most recent message in the channel authored by this bot that has an embed."""
    try:
        me = channel.guild.me if hasattr(channel.guild, 'me') else None
        bot_user = me if me else bot.user
        async for msg in channel.history(limit=50):
            try:
                if bot_user and msg.author.id == bot_user.id and msg.embeds:
                    return msg
            except Exception:
                continue
    except Exception:
        pass
    return None

async def build_update_for_guild(guild: discord.Guild) -> tuple[str, str, str, str, str, Optional[ForecastEngine], Optional[AlertBuild]]:
    cfg = await get_config(guild.id)
    engine = _engine_for_guild(cfg)
    def _work():
        text = engine.fetch_forecast()
        build = engine.build_alert(text)
        return build
    build = await asyncio.to_thread(_work)
    content = build.message if build else "No data."
    tonight_url = build.tonight_image_url if build else ''
    tomorrow_url = build.tomorrow_image_url if build else ''
    window_id = build.window_id if build else ''
    # Build detection signature from real-time high Kp blocks only (GFZ + SWPC) to reduce false positives.
    det_sig = ''
    if build:
        tokens: List[str] = []
        try:
            if build.gfz_high_blocks:
                for blk in build.gfz_high_blocks:
                    ts = blk.get('ts')
                    kp = blk.get('kp')
                    if isinstance(ts, int) and isinstance(kp, (int, float)):
                        tokens.append(f"GFZ:{ts}:{kp}")
            if build.swpc_high_block:
                ts = build.swpc_high_block.get('ts') if isinstance(build.swpc_high_block, dict) else None
                kp = build.swpc_high_block.get('kp') if isinstance(build.swpc_high_block, dict) else None
                if isinstance(ts, int) and isinstance(kp, (int, float)):
                    tokens.append(f"SWPC:{ts}:{kp}")
        except Exception:
            pass
        if tokens:
            # Sort tokens for stable ordering
            det_sig = '|'.join(sorted(tokens))
    return content, tonight_url, tomorrow_url, window_id, det_sig, engine, build

async def _auto_delete(message: discord.Message, minutes: int):
    try:
        await asyncio.sleep(max(1, minutes) * 60)
        try:
            await message.delete()
        except (discord.NotFound, discord.Forbidden):
            pass
        except Exception:
            logging.exception("Failed to delete alert message")
    except Exception:
        pass

@bot.event
async def on_ready():
    await init_db()
    try:
        await tree.sync()
    except Exception as e:
        print(f"Command sync failed: {e}")
    # Ensure commands appear immediately by syncing per guild
    try:
        for g in bot.guilds:
            try:
                tree.copy_global_to(guild=g)
                await tree.sync(guild=g)
            except Exception as ge:
                print(f"Guild sync failed for {getattr(g, 'id', '?')}: {ge}")
    except Exception as e:
        print(f"Per-guild sync loop failed: {e}")
    engine = _engine_for_guild(None)
    health = await perform_startup_health(engine)
    global _LAST_HEALTH, _UPDATER_STARTED
    _LAST_HEALTH = health
    required_ok = all(health.get(src) for src in REQUIRED_SOURCES)
    if not required_ok and STARTUP_HEALTH_BLOCK:
        logging.warning("Required sources not healthy; deferring updater and scheduling retries.")
        async def _retry_start():
            global _LAST_HEALTH, _UPDATER_STARTED
            for attempt in range(1, 6):
                await asyncio.sleep(60)
                h2 = await perform_startup_health(engine)
                _LAST_HEALTH = h2
                if all(h2.get(src) for src in REQUIRED_SOURCES):
                    logging.info(f"Health recovered on attempt {attempt}; starting updater.")
                    if not _UPDATER_STARTED and not updater.is_running():
                        try:
                            updater.start(); _UPDATER_STARTED = True
                        except RuntimeError:
                            pass
                    return
                else:
                    logging.warning(f"Attempt {attempt}: required sources still unhealthy.")
            logging.error("Health retries exhausted; starting updater anyway.")
            if not _UPDATER_STARTED and not updater.is_running():
                try:
                    updater.start(); _UPDATER_STARTED = True
                except RuntimeError:
                    pass
        asyncio.create_task(_retry_start())
    else:
        if not _UPDATER_STARTED and not updater.is_running():
            try:
                updater.start(); _UPDATER_STARTED = True
            except RuntimeError as e:
                print(f"Updater start ignored: {e}")
    # Start periodic health refresh
    if not health_refresher.is_running():
        health_refresher.start()
    if bot.user:
        print(f"Logged in as {bot.user} (ID: {bot.user.id})")
    else:
        print("Logged in (bot user unknown)")

@bot.event
async def on_error(event_method, *args, **kwargs):
    logging.exception(f"Unhandled exception in event {event_method}")

@bot.event
async def on_raw_message_delete(payload: discord.RawMessageDeleteEvent):
    """Clear channel configuration if the tracked aurora message is deleted."""
    try:
        guild_id = getattr(payload, 'guild_id', None)
        msg_id = getattr(payload, 'message_id', None)
        if guild_id and msg_id:
            cfg = await get_config(guild_id)
            tracked = (cfg or {}).get('message_id')
            if tracked and int(tracked) == int(msg_id):
                try:
                    await clear_channel(guild_id)
                    logging.info("Guild %s: tracked aurora message deleted; cleared channel configuration.", guild_id)
                except Exception:
                    logging.exception("Guild %s: failed to clear channel on message delete.", guild_id)
    except Exception:
        logging.exception("on_raw_message_delete handler failed")

@tree.command(name="aurora-set-channel", description="Set the channel for aurora updates")
@app_commands.describe(channel="Channel to post the aurora updates to")
async def set_channel_cmd(interaction: discord.Interaction, channel: discord.TextChannel):
    if interaction.guild is None or interaction.guild_id is None:
        await interaction.response.send_message("Guild context required.", ephemeral=True)
        return
    await set_channel(interaction.guild_id, channel.id)
    await interaction.response.send_message(f"Channel set to {channel.mention}", ephemeral=True)

@tree.command(name="aurora-set-threshold", description="Set the Kp threshold")
@app_commands.describe(kp="Kp threshold, e.g., 6.5")
async def set_threshold_cmd(interaction: discord.Interaction, kp: float):
    if interaction.guild is None or interaction.guild_id is None:
        await interaction.response.send_message("Guild context required.", ephemeral=True)
        return
    await set_threshold(interaction.guild_id, kp)
    await interaction.response.send_message(f"Kp threshold set to {kp}", ephemeral=True)

@tree.command(name="aurora-set-location", description="Set the location for clouds and AFM")
@app_commands.describe(latitude="Latitude", longitude="Longitude", name="Location display name")
async def set_location_cmd(interaction: discord.Interaction, latitude: float, longitude: float, name: Optional[str] = None):
    if interaction.guild is None or interaction.guild_id is None:
        await interaction.response.send_message("Guild context required.", ephemeral=True)
        return
    await set_location(interaction.guild_id, latitude, longitude, name or DEFAULT_LOC)
    await interaction.response.send_message(f"Location set to {name or DEFAULT_LOC} ({latitude}, {longitude})", ephemeral=True)

@tree.command(name="aurora-show", description="Show the current aurora message content")
async def show_cmd(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    if interaction.guild is None:
        await interaction.followup.send("Guild context required.", ephemeral=True)
        return
    content, tonight_url, tomorrow_url, _, _, engine, build = await build_update_for_guild(interaction.guild)
    embed = format_embed(build, engine)
    await interaction.followup.send(embed=embed, ephemeral=True)

@tree.command(name="aurora-health", description="Show last source health status")
async def health_cmd(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    global _LAST_HEALTH
    if not _LAST_HEALTH:
        await interaction.followup.send("No health check recorded yet.", ephemeral=True)
        return
    ts = _LAST_HEALTH.get('checked_at')
    lines: List[str] = []
    def fmt(key: str, required: bool = True):
        ok = bool(_LAST_HEALTH and _LAST_HEALTH.get(key))
        mark = '✅' if ok else ('❌' if required else '⚠️')
        lines.append(f"{mark} {key}")
    for k in REQUIRED_SOURCES:
        fmt(k, True)
    for k in OPTIONAL_SOURCES:
        if k in _LAST_HEALTH:
            fmt(k, False)
    if isinstance(ts, int):
        lines.append(f"Checked: <t:{ts}:R>")
    embed = discord.Embed(title="Aurora Source Health", description="\n".join(lines), color=0x8888ff)
    await interaction.followup.send(embed=embed, ephemeral=True)

@tree.command(name="aurora-health-refresh", description="Force a new source health check now")
async def health_refresh_cmd(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    engine = _engine_for_guild(None)
    health = await perform_startup_health(engine)
    global _LAST_HEALTH
    _LAST_HEALTH = health
    ok_required = all(health.get(src) for src in REQUIRED_SOURCES)
    status = "OK" if ok_required else "DEGRADED"
    await interaction.followup.send(f"Health refreshed (required status: {status}). Use /aurora-health to view details.", ephemeral=True)

@tree.command(name="aurora-sync", description="Resync slash commands in this guild (admin)")
async def sync_cmd(interaction: discord.Interaction):
    try:
        await interaction.response.defer(ephemeral=True)
        if interaction.guild is None:
            await interaction.followup.send("Guild context required.", ephemeral=True)
            return
        tree.copy_global_to(guild=interaction.guild)
        await tree.sync(guild=interaction.guild)
        await interaction.followup.send("Commands resynced for this guild.", ephemeral=True)
    except Exception as e:
        await interaction.followup.send(f"Sync failed: {e}", ephemeral=True)

@tree.command(name="aurora-start", description="Start and post the initial aurora message in the configured channel")
async def start_cmd(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    if interaction.guild is None or interaction.guild_id is None:
        await interaction.followup.send("Guild context required.", ephemeral=True)
        return
    cfg = await get_config(interaction.guild_id)
    channel_id = cfg.get('channel_id') if cfg else None
    if not channel_id:
        await interaction.followup.send("Please set a channel first with /aurora-set-channel", ephemeral=True)
        return
    channel = interaction.guild.get_channel(int(channel_id))
    if not channel:
        await interaction.followup.send("Configured channel not found.", ephemeral=True)
        return
    if not isinstance(channel, discord.TextChannel):
        await interaction.followup.send("Configured channel isn't a text channel.", ephemeral=True)
        return
    existing_id = cfg.get('message_id') if cfg else None
    content, tonight_url, tomorrow_url, _, _, engine, build = await build_update_for_guild(interaction.guild)
    embed = format_embed(build, engine)
    if existing_id:
        try:
            msg = await channel.fetch_message(int(existing_id))
            await msg.edit(embed=embed)
            await interaction.followup.send(f"Updated existing aurora message in {channel.mention}", ephemeral=True)
            return
        except Exception:
            # Try to locate the latest bot embed in the channel and edit it
            latest = await _find_latest_bot_embed(channel)
            if latest:
                try:
                    await latest.edit(embed=embed)
                    await set_message_id(interaction.guild_id, latest.id)
                    await interaction.followup.send(f"Updated latest bot embed in {channel.mention}", ephemeral=True)
                    return
                except Exception:
                    pass
    # Fall back to sending once if nothing to edit
    msg = await channel.send(embed=embed)
    await set_message_id(interaction.guild_id, msg.id)
    await interaction.followup.send(f"Posted aurora message in {channel.mention}", ephemeral=True)

@tree.command(name="aurora-refresh", description="Force an immediate full refresh of the tracked aurora embed")
async def refresh_cmd(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    if interaction.guild is None or interaction.guild_id is None:
        await interaction.followup.send("Guild context required.", ephemeral=True)
        return
    cfg = await get_config(interaction.guild_id)
    if not cfg or not cfg.get('channel_id'):
        await interaction.followup.send("No channel configured. Use /aurora-set-channel first.", ephemeral=True)
        return
    channel = interaction.guild.get_channel(int(cfg['channel_id']))
    if not isinstance(channel, discord.TextChannel):
        await interaction.followup.send("Configured channel isn't a text channel.", ephemeral=True)
        return
    tracked_id = cfg.get('message_id')
    if not tracked_id:
        await interaction.followup.send("No existing aurora message to refresh. Use /aurora-start first.", ephemeral=True)
        return
    msg = None
    try:
        msg = await channel.fetch_message(int(tracked_id))
    except Exception:
        # Try to find the latest bot embed instead of bailing
        msg = await _find_latest_bot_embed(channel)
        if msg:
            await set_message_id(interaction.guild_id, msg.id)
        else:
            # Original message appears deleted and no replacement found: clear configuration
            try:
                await clear_channel(interaction.guild_id)
            except Exception:
                logging.exception("Failed clearing channel after missing tracked message in refresh")
            await interaction.followup.send("Original aurora message was deleted. Channel configuration cleared. Use /aurora-set-channel to reconfigure.", ephemeral=True)
            return
    content, tonight_url, tomorrow_url, window_id, det_sig, engine, build = await build_update_for_guild(interaction.guild)
    embed = format_embed(build, engine)
    try:
        if msg:
            await msg.edit(embed=embed)
    except Exception:
        await interaction.followup.send("Failed to update message.", ephemeral=True)
        return
    # Update last_window_id if changed (reuse updater logic simplified)
    combined_id = f"{window_id}|{det_sig}" if window_id else ''
    prev = cfg.get('last_window_id') or ''
    if combined_id and combined_id != prev:
        ts_now = int(datetime.now(timezone.utc).timestamp())
        await set_last_window(interaction.guild_id, combined_id, ts_now)
    await interaction.followup.send("Aurora embed refreshed.", ephemeral=True)

@tree.command(name="aurora-next-30", description="Show viewing probability every 5 minutes for the next 30 minutes and tonight's clouds")
async def next_30_cmd(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    if interaction.guild is None or interaction.guild_id is None:
        await interaction.followup.send("Guild context required.", ephemeral=True)
        return
    cfg = await get_config(interaction.guild_id)
    engine = _engine_for_guild(cfg)
    def _work_series():
        return engine.short_term_visibility_series(minutes=30, step=5)
    series = await asyncio.to_thread(_work_series)
    points = series.get('points', []) if isinstance(series, dict) else []
    cloud_tonight = series.get('cloud_tonight') if isinstance(series, dict) else None
    cloud_now = series.get('cloud_now') if isinstance(series, dict) else None
    maf_kp = series.get('maf_kp') if isinstance(series, dict) else None
    ov_prob = series.get('ovation_prob') if isinstance(series, dict) else None
    maf_prob = series.get('maf_prob') if isinstance(series, dict) else None
    desc_parts = [f"Location: {engine.location_name} ({engine.latitude:.4f}, {engine.longitude:.4f})"]
    if isinstance(maf_kp, (int, float)):
        desc_parts.append(f"MAF KP: {float(maf_kp):.2f}")
    if isinstance(ov_prob, int):
        desc_parts.append(f"Ovation: {ov_prob}%")
    if isinstance(maf_prob, int):
        desc_parts.append(f"MAF chance: {maf_prob}%")
    if isinstance(cloud_now, int):
        desc_parts.append(f"Cloud now: {cloud_now}%")
    header = " • ".join(desc_parts)
    embed = discord.Embed(title="Next 30 minutes (every 5 min)", description=header, color=0x33cc99)
    if points:
        lines = [f"• <t:{p['ts']}:t>: {p['prob']}%" for p in points]
        value = "\n".join(lines)
        embed.add_field(name="Viewing probability", value=value[:1024], inline=False)
    if isinstance(cloud_tonight, int):
        embed.add_field(name="Cloud coverage tonight", value=f"☁️ {cloud_tonight}%", inline=True)
    await interaction.followup.send(embed=embed, ephemeral=True)

@tree.command(name="aurora-gfz-hourly", description="Show recent GFZ Potsdam Kp values and NOAA outlook")
@app_commands.describe(hours_back="Number of hours back to include (multiples of 3, max 240)")
async def gfz_hourly_cmd(interaction: discord.Interaction, hours_back: int = 72):
    await interaction.response.defer(ephemeral=True)
    if interaction.guild is None or interaction.guild_id is None:
        await interaction.followup.send("Guild context required.", ephemeral=True)
        return
    cfg = await get_config(interaction.guild_id)
    engine = _engine_for_guild(cfg)
    requested_hours = max(3, min(240, int(hours_back or 72)))
    if requested_hours % 3 != 0:
        requested_hours -= requested_hours % 3
        if requested_hours < 3:
            requested_hours = 3
    def _work_bundle():
        gfz_data = engine.gfz_recent_blocks(hours_back=requested_hours)
        forecast_text = engine.fetch_forecast()
        build = engine.build_alert(forecast_text)
        return gfz_data, build
    gfz_data, build = await asyncio.to_thread(_work_bundle)
    records = gfz_data.get('records') if isinstance(gfz_data, dict) else None
    latest = gfz_data.get('latest') if isinstance(gfz_data, dict) else None
    source_note = gfz_data.get('source_note') if isinstance(gfz_data, dict) else None
    desc_parts = [f"Location: {engine.location_name} ({engine.latitude:.4f}, {engine.longitude:.4f})"]
    if isinstance(latest, dict):
        latest_ts = latest.get('ts')
        latest_kp = latest.get('kp')
        latest_status = latest.get('status_label')
        if isinstance(latest_ts, int) and isinstance(latest_kp, (int, float)):
            desc = f"Latest Kp {float(latest_kp):.2f}"
            if isinstance(latest_status, str) and latest_status:
                desc += f" ({latest_status})"
            desc += f" at <t:{latest_ts}:t>"
            desc_parts.append(desc)
    header = " • ".join(desc_parts)
    embed = discord.Embed(title=f"GFZ Potsdam Kp (last {requested_hours}h)", description=header, color=0x3366ff)
    if isinstance(records, list) and records:
        display_rows = records[-min(len(records), 12):]
        lines = []
        for row in display_rows:
            ts = row.get('ts')
            kp_val = row.get('kp')
            status_label = row.get('status_label') or row.get('status')
            if isinstance(ts, int) and isinstance(kp_val, (int, float)):
                line = f"• <t:{ts}:t> • Kp {float(kp_val):.2f}"
                if isinstance(status_label, str) and status_label:
                    line += f" ({status_label})"
                lines.append(line)
        if lines:
            embed.add_field(name="Recent 3h blocks", value="\n".join(lines)[:1024], inline=False)
    if build and build.recommendation_lines:
        embed.add_field(name="Tonight outlook", value="\n".join(build.recommendation_lines)[:1024], inline=False)
    if build and build.detections:
        top_lines = []
        for det in build.detections[:5]:
            top_lines.append(det.bullet)
        if top_lines:
            embed.add_field(name="Upcoming NOAA windows", value="\n".join(top_lines)[:1024], inline=False)
    if source_note:
        embed.set_footer(text=str(source_note)[:2048])
    await interaction.followup.send(embed=embed, ephemeral=True)

@tasks.loop(hours=UPDATE_INTERVAL_HOURS)
async def updater():
    await bot.wait_until_ready()
    for guild in bot.guilds:
        try:
            cfg = await get_config(guild.id)
            if not cfg or not cfg.get('channel_id'):
                continue
            channel = guild.get_channel(int(cfg['channel_id']))
            if not isinstance(channel, discord.TextChannel):
                continue
            logging.info(f"Updater iteration guild={guild.id}")
            tracked_id = cfg.get('message_id')
            if not tracked_id:
                logging.info(f"Guild {guild.id}: no tracked message id; skipping (awaiting /aurora-start).")
                continue
            content, tonight_url, tomorrow_url, window_id, det_sig, engine, build = await build_update_for_guild(guild)
            embed = format_embed(build, engine)
            try:
                msg = await channel.fetch_message(int(tracked_id))
                await msg.edit(embed=embed)
            except discord.NotFound:
                # Tracked message is gone. Try latest bot embed; else clear channel config.
                latest = await _find_latest_bot_embed(channel)
                if latest:
                    try:
                        await latest.edit(embed=embed)
                        await set_message_id(guild.id, latest.id)
                    except Exception:
                        logging.exception("Guild %s: failed to edit latest bot embed", guild.id)
                        continue
                else:
                    try:
                        await clear_channel(guild.id)
                        logging.info("Guild %s: tracked message deleted; cleared channel configuration.", guild.id)
                    except Exception:
                        logging.exception("Guild %s: failed to clear channel after deletion.", guild.id)
                    continue
            except Exception:
                logging.exception(f"Guild {guild.id}: failed to edit message; skipping this cycle.")
                continue
            combined_id = f"{window_id}|{det_sig}" if window_id else ''
            prev = cfg.get('last_window_id') or ''
            if combined_id:
                if not prev:
                    ts_now = int(datetime.now(timezone.utc).timestamp())
                    await set_last_window(guild.id, combined_id, ts_now)
                elif combined_id != prev:
                    old_sig = ''
                    if '|' in prev:
                        old_sig = prev.split('|', 1)[1]
                    old_tokens = set([t for t in old_sig.split('|') if t])
                    new_tokens = set([t for t in det_sig.split('|') if t])
                    added = [t for t in new_tokens if t not in old_tokens]
                    alert_lines: List[str] = []
                    for t in added:
                        try:
                            if t.startswith('GFZ:'):
                                _, ts_str, kp_str = t.split(':', 2)
                                ts_val = int(float(ts_str))
                                alert_lines.append(f"GFZ Kp {kp_str} at <t:{ts_val}:t>")
                            elif t.startswith('SWPC:'):
                                _, ts_str, kp_str = t.split(':', 2)
                                ts_val = int(float(ts_str))
                                alert_lines.append(f"SWPC Planetary Kp {kp_str} at <t:{ts_val}:t>")
                        except Exception:
                            continue
                    if not alert_lines:
                        alert_lines = ["New real-time high Kp activity detected."]
                    threshold_display = engine.kp_threshold if engine else DEFAULT_KP
                    header = f"⚠️ New high Kp window(s) ≥ {threshold_display} detected"
                    if build and build.aggregated_sources_line:
                        header += f"\n{build.aggregated_sources_line}"
                    safe_lines = [str(x) for x in alert_lines if isinstance(x, str)]
                    alert_text = header + "\n" + "\n".join(safe_lines)
                    alert_text += f"\n_(Will auto-delete in {ALERT_DELETE_AFTER_MINUTES} min)_"
                    try:
                        alert_msg = await channel.send(alert_text)
                        asyncio.create_task(_auto_delete(alert_msg, ALERT_DELETE_AFTER_MINUTES))
                    except Exception:
                        logging.exception("Failed to send high-Kp alert message")
                    ts_now = int(datetime.now(timezone.utc).timestamp())
                    await set_last_window(guild.id, combined_id, ts_now)
        except Exception as e:
            logging.exception(f"Update failed for guild {guild.id}: {e}")

if __name__ == '__main__':
    import argparse
    import sys
    parser = argparse.ArgumentParser(description='Aurora Alerts Bot')
    parser.add_argument('--test', action='store_true', help='Run a one-off test using forecastExample.txt and NOAA SWPC, printing output to console')
    args, unknown = parser.parse_known_args()

    if args.test:
        try:
            engine = _engine_for_guild(None)
            # Read example forecast text from repo root
            here = os.path.dirname(os.path.abspath(__file__))
            example_path = os.path.abspath(os.path.join(here, '..', 'forecastExample.txt'))
            with open(example_path, 'r', encoding='utf-8') as f:
                text = f.read()
            build = engine.build_alert(text)
            if not build:
                print('Failed to build alert from forecastExample.txt')
                sys.exit(2)
            # Print concise report resembling the embed content
            print('=== Aurora viewing windows (TEST MODE) ===')
            print(f"Location: {engine.location_name} ({engine.latitude:.4f}, {engine.longitude:.4f})")
            # Kp Forecasts (by date with bullets)
            if build.detection_groups:
                print('\nKp Forecasts:')
                for date_label in sorted(build.detection_groups.keys()):
                    print(date_label)
                    for bullet in build.detection_groups[date_label]:
                        print(bullet)
            else:
                print(f"\nKp Forecasts:\nNo high Kp forecasts \u2265 {engine.kp_threshold} in the next 3 days.")
            # 3-Day NOAA Kp table
            if build.all_forecast_lines:
                print('\n3-Day NOAA Kp:')
                for line in build.all_forecast_lines:
                    print(line)
            # NOAA SWPC real-time
            swpc_lines = []
            if build.swpc_planetary_line:
                swpc_lines.append(build.swpc_planetary_line)
            if build.swpc_summary_lines:
                swpc_lines.extend(build.swpc_summary_lines)
            if swpc_lines:
                print('\nNOAA SWPC:')
                for line in swpc_lines:
                    print(line)
            sys.exit(0)
        except FileNotFoundError:
            print('forecastExample.txt not found; ensure it exists in the repository root.')
            sys.exit(3)
        except Exception as e:
            print(f"Test mode failed: {e}")
            sys.exit(1)
    else:
        if not DISCORD_TOKEN:
            raise SystemExit("Missing DISCORD_BOT_TOKEN in environment.")
        bot.run(DISCORD_TOKEN)
