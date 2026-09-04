from __future__ import annotations
import os
import asyncio
import contextlib
from datetime import datetime, timezone
import logging
from typing import Optional, List

try:
    import audioop  # type: ignore  # noqa: F401
except ModuleNotFoundError:
    import sys
    import types
    import warnings

    audioop_stub = types.ModuleType("audioop")

    class AudioopUnavailable(RuntimeError):
        """Raised when audioop functionality is requested without support."""

    def _audioop_placeholder(*args, **kwargs):
        raise AudioopUnavailable("audioop module is unavailable; Discord voice features are disabled.")

    audioop_stub.error = AudioopUnavailable  # type: ignore[attr-defined]
    audioop_stub.__all__ = []  # type: ignore[attr-defined]

    def _module_getattr(name: str):  # pragma: no cover - defensive stub
        return _audioop_placeholder

    audioop_stub.__getattr__ = _module_getattr  # type: ignore[attr-defined]
    sys.modules["audioop"] = audioop_stub
    warnings.warn("audioop module not available; Discord voice features disabled.", RuntimeWarning)

import discord
from discord import app_commands
from discord.ext import tasks
from dotenv import load_dotenv

from .db import (
    init_db,
    get_config,
    set_channel,
    set_threshold,
    set_location,
    set_message_id,
    set_last_window,
    set_started,
    clear_channel,
)
from .forecast import ForecastEngine, AlertBuild
from . import net

load_dotenv()
logging.basicConfig(
    level=getattr(logging, os.getenv('LOG_LEVEL', 'INFO').strip().upper(), logging.INFO),
    format='%(asctime)s %(levelname)s %(name)s: %(message)s',
)

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
# on_ready fires again on every gateway RESUME. Without this guard each reconnect
# kicked off another full health sweep (and another retry task), which is how the
# container ran itself out of file descriptors.
_STARTUP_DONE = False
_STARTUP_LOCK: Optional[asyncio.Lock] = None
_LAST_GOOD_BUILD: dict[int, tuple[int, AlertBuild, ForecastEngine]] = {}

# Background task interval in hours
UPDATE_INTERVAL_HOURS = float(os.getenv('UPDATE_INTERVAL_HOURS', '2'))
ALERT_DELETE_AFTER_MINUTES = int(os.getenv('ALERT_DELETE_AFTER_MINUTES', '15'))  # ephemeral high-Kp alert lifetime
# Defaults to off: refusing to start the updater when a source is briefly down is
# what left the embed frozen for days.
STARTUP_HEALTH_BLOCK = os.getenv('STARTUP_HEALTH_BLOCK', 'false').strip().lower() in ('1', 'true', 'yes')
STARTUP_HEALTH_TIMEOUT = int(os.getenv('STARTUP_HEALTH_TIMEOUT_SECONDS', '25'))
HEALTH_REFRESH_MINUTES = int(os.getenv('HEALTH_REFRESH_MINUTES', '60'))
UPDATE_RETRY_MINUTES = int(os.getenv('UPDATE_RETRY_MINUTES', '15'))
HEARTBEAT_PATH = os.path.abspath(
    os.getenv('HEARTBEAT_PATH', os.path.join(os.path.dirname(__file__), '..', 'data', 'heartbeat'))
)

REQUIRED_SOURCES = ["noaa_forecast", "gfz", "swpc_planetary"]
OPTIONAL_SOURCES = ["solar_wind", "swpc_alerts", "cloud_cover", "ovation", "maf", "afm_snapshot", "swpc_hemi"]

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
            results[label] = await coro
        except Exception as exc:
            net.log_fetch_error(f"health:{label}", exc)
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
        safe_call('solar_wind_raw', _run_blocking(engine.fetch_solar_wind, timeout=STARTUP_HEALTH_TIMEOUT)),
        safe_call('alerts_raw', _run_blocking(engine.fetch_space_weather_alerts, timeout=STARTUP_HEALTH_TIMEOUT)),
    )
    swpc_planetary_raw = results.get('swpc_planetary_raw')
    swpc_planetary_ok = False
    if isinstance(swpc_planetary_raw, dict):
        swpc_planetary_ok = True
    elif isinstance(swpc_planetary_raw, tuple) and swpc_planetary_raw:
        swpc_planetary_ok = isinstance(swpc_planetary_raw[0], dict)

    health = {
        'noaa_forecast': isinstance(results.get('noaa_raw'), str) and 'NOAA Kp index breakdown' in (results.get('noaa_raw') or ''),
        'gfz': isinstance(results.get('gfz_raw'), dict) and bool((results.get('gfz_raw') or {}).get('records')),
        'ovation': isinstance(results.get('ovation_raw'), int),
        'maf': isinstance(results.get('maf_raw'), dict) and len(results.get('maf_raw') or {}) > 0,
        'cloud_cover': isinstance(results.get('cloud_raw'), dict) and len(results.get('cloud_raw') or {}) > 0,
        'afm_snapshot': isinstance(results.get('afm_raw'), dict) and 'tonight' in (results.get('afm_raw') or {}),
        'swpc_planetary': swpc_planetary_ok,
        'swpc_hemi': isinstance(results.get('swpc_hemi_raw'), dict),
        'solar_wind': isinstance(results.get('solar_wind_raw'), dict),
        'swpc_alerts': isinstance(results.get('alerts_raw'), list),
        'checked_at': int(started.timestamp()),
    }
    failed = [k for k in REQUIRED_SOURCES + OPTIONAL_SOURCES if k in health and not health[k]]
    if failed:
        logging.warning("Source health: %d/%d OK, down: %s",
                        len(REQUIRED_SOURCES + OPTIONAL_SOURCES) - len(failed),
                        len(REQUIRED_SOURCES + OPTIONAL_SOURCES),
                        ", ".join(failed))
    else:
        logging.info("Source health: all OK")
    return health

def _write_heartbeat() -> None:
    """Touch a file the container healthcheck watches, so a wedged bot restarts."""
    try:
        os.makedirs(os.path.dirname(HEARTBEAT_PATH), exist_ok=True)
        with open(HEARTBEAT_PATH, 'w', encoding='utf-8') as fh:
            fh.write(str(int(datetime.now(timezone.utc).timestamp())))
    except Exception:
        logging.debug("Could not write heartbeat", exc_info=True)


@tasks.loop(minutes=max(5, HEALTH_REFRESH_MINUTES))
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

def _next_update_ts() -> Optional[int]:
    try:
        nxt = updater.next_iteration
        if nxt is not None:
            return int(nxt.timestamp())
    except Exception:
        pass
    return None


def format_embed(
    build: Optional[AlertBuild],
    engine: Optional[ForecastEngine],
    stale_since_ts: Optional[int] = None,
) -> discord.Embed:
    detected_ts = int(datetime.now(timezone.utc).timestamp())
    loc_name = engine.location_name if engine else os.getenv('LOCATION_NAME', 'Location')
    lat = engine.latitude if engine else float(os.getenv('LATITUDE', '0') or 0)
    lon = engine.longitude if engine else float(os.getenv('LONGITUDE', '0') or 0)
    threshold = engine.kp_threshold if engine else DEFAULT_KP

    desc_lines: List[str] = []
    if build and build.now_lines:
        desc_lines.extend(build.now_lines)
    desc_lines.append(f"📍 {loc_name} ({lat:.4f}, {lon:.4f}) · alerting at Kp ≥ {threshold:g}")
    if stale_since_ts:
        desc_lines.append(
            f"⚠️ Live data unavailable. Showing the last good reading from <t:{stale_since_ts}:R>."
        )
    else:
        stamp = f"🔄 Updated <t:{detected_ts}:R>"
        nxt = _next_update_ts()
        if nxt:
            stamp += f" · next <t:{nxt}:R>"
        desc_lines.append(stamp)
    desc_lines.append(
        "[SWPC Aurora Dashboard](https://www.swpc.noaa.gov/communities/aurora-dashboard-experimental)"
    )

    color = ForecastEngine.kp_color(build.max_forecast_kp if build else None)
    embed = discord.Embed(
        title="Aurora viewing windows",
        description="\n".join(desc_lines)[:4096],
        color=color,
    )
    embed.timestamp = datetime.now(timezone.utc)
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
    # When you could actually see it. This is the whole point, so it goes first.
    if build and build.viewable_lines:
        value = "\n".join(f"• {line}" for line in build.viewable_lines[:5])
        if len(build.viewable_lines) > 5:
            value += f"\n• plus {len(build.viewable_lines) - 5} more"
        embed.add_field(name="🌌 When to look", value=value[:1024], inline=False)
    elif build and build.no_viewable_reason:
        embed.add_field(name="🌌 When to look", value=build.no_viewable_reason[:1024], inline=False)

    if build and build.alert_lines:
        embed.add_field(
            name="NOAA space weather alerts",
            value="\n".join(f"• {line}" for line in build.alert_lines[:3])[:1024],
            inline=False,
        )

    # Only worth showing when there is no concrete window to point at, otherwise
    # it repeats or contradicts the field above.
    if build and build.recommendation_lines and not build.viewable_lines and not build.no_viewable_reason:
        embed.add_field(name="Tonight", value=build.recommendation_lines[0][:1024], inline=False)

    # 3-day NOAA table as three side-by-side columns, one per day. The old
    # single-column layout repeated the same clock time three times per row.
    columns = (build.forecast_columns if build else None) or []
    rendered_columns = 0
    for col in columns[:3]:
        try:
            label = col.get('label')
            lines = col.get('lines') or []
        except Exception:
            continue
        if not isinstance(lines, list) or not lines:
            continue
        name = str(label) if label else "Forecast"
        embed.add_field(name=name, value="\n".join(str(x) for x in lines)[:1024], inline=True)
        rendered_columns += 1
    if not rendered_columns and build and build.all_forecast_lines:
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
        peak = build.max_forecast_kp if build else None
        placeholder = f"Nothing at or above Kp {threshold:g} in the next 3 days."
        if isinstance(peak, (int, float)):
            placeholder += f" Forecast peaks at Kp {float(peak):.2f}."
        embed.add_field(name="Kp Forecasts", value=placeholder[:1024], inline=False)
    # Include best viewing date(s) if there are windows ≥ threshold
    if build and build.upcoming_days_lines and build.detections:
        best_text = "\n".join(build.upcoming_days_lines)
        if len(best_text) > 1024:
            best_text = best_text[:1000] + "\n…"
        embed.add_field(name="Best Viewing Dates", value=best_text, inline=False)

    footer_bits: List[str] = []
    sources = (build.sources_ok if build else None) or {}
    if sources:
        ok = sum(1 for v in sources.values() if v)
        footer_bits.append(f"{ok}/{len(sources)} sources OK")
        degraded = [k for k, v in sources.items() if not v]
        if degraded:
            footer_bits.append("down: " + ", ".join(sorted(degraded)))
    footer_bits.append("NOAA SWPC · GFZ Potsdam · Open-Meteo")
    embed.set_footer(text=" · ".join(footer_bits)[:2048])
    return embed

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

def _detection_signature(build: Optional[AlertBuild]) -> str:
    """Stable token set describing what is worth alerting on.

    SWPC tokens cover "a storm is happening now". VIEW tokens cover "there is a
    span tonight when you could actually see it", so a newly viewable window
    fires a notification even when the Kp reading has not changed.
    """
    if not build:
        return ''
    tokens: List[str] = []
    try:
        for blk in getattr(build, 'swpc_high_blocks', None) or []:
            if not isinstance(blk, dict):
                continue
            ts, kp = blk.get('ts'), blk.get('kp')
            if isinstance(ts, int) and isinstance(kp, (int, float)):
                tokens.append(f"SWPC:{ts}:{kp}")
        if not tokens and isinstance(getattr(build, 'swpc_high_block', None), dict):
            ts = build.swpc_high_block.get('ts')
            kp = build.swpc_high_block.get('kp')
            if isinstance(ts, int) and isinstance(kp, (int, float)):
                tokens.append(f"SWPC:{ts}:{kp}")
        for window in (build.viewable_windows or []):
            # Uses the unclamped token so an in-progress window keeps its
            # identity as the clock moves and does not re-alert every cycle.
            token = window.get('token')
            if isinstance(token, str) and token:
                tokens.append(token)
    except Exception:
        logging.debug("Failed building detection signature", exc_info=True)
    return '|'.join(sorted(tokens))


def compose_alert_message(
    build: Optional[AlertBuild],
    engine: Optional[ForecastEngine],
    added_tokens: List[str],
) -> Optional[str]:
    """The notification body. Leads with when you can actually see it."""
    if not build:
        return None
    threshold = engine.kp_threshold if engine else DEFAULT_KP
    location = engine.location_name if engine else DEFAULT_LOC
    windows = build.viewable_windows or []
    new_view = [t for t in added_tokens if t.startswith('VIEW:')]
    new_swpc = [t for t in added_tokens if t.startswith('SWPC:')]

    lines: List[str] = []
    if windows:
        best = max(int(w.get('score') or 0) for w in windows)
        quality = ForecastEngine.window_quality(best)
        headline = "🌌 Aurora may be visible" if new_view else "⚠️ High Kp detected"
        lines.append(f"**{headline} from {location}** · best odds {quality}")
        lines.append("")
        lines.append("**When to look:**")
        for window in windows[:4]:
            lines.append(f"• {ForecastEngine.describe_window(window)}")
        if len(windows) > 4:
            lines.append(f"• plus {len(windows) - 4} more window(s)")
    elif new_swpc:
        lines.append(f"**⚠️ High Kp detected (≥ {threshold:g}) near {location}**")
        reason = build.no_viewable_reason
        lines.append(reason or "No dark, clear window lines up with it right now.")
    else:
        return None

    if build.solar_wind_line:
        lines.append("")
        lines.append(f"Solar wind: {build.solar_wind_line}")
    if build.alert_lines:
        lines.append(f"NOAA: {build.alert_lines[0]}")
    elif build.aggregated_sources_line:
        lines.append(build.aggregated_sources_line)

    lines.append("")
    lines.append(f"_Auto-deletes in {ALERT_DELETE_AFTER_MINUTES} min._")
    return "\n".join(lines)[:1900]


async def build_update_for_guild(
    guild: discord.Guild, cfg: Optional[dict] = None
) -> tuple[str, str, str, str, str, Optional[ForecastEngine], Optional[AlertBuild]]:
    if cfg is None:
        cfg = await get_config(guild.id)
    engine = _engine_for_guild(cfg)

    def _work():
        text = engine.fetch_forecast()
        return engine.build_alert(text)

    build = await asyncio.to_thread(_work)
    if build is not None:
        _LAST_GOOD_BUILD[guild.id] = (
            int(datetime.now(timezone.utc).timestamp()),
            build,
            engine,
        )
    content = build.message if build else "No data."
    tonight_url = build.tonight_image_url if build else ''
    tomorrow_url = build.tomorrow_image_url if build else ''
    window_id = build.window_id if build else ''
    det_sig = _detection_signature(build)
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

def _start_updater() -> None:
    global _UPDATER_STARTED
    if _UPDATER_STARTED or updater.is_running():
        return
    try:
        updater.start()
        _UPDATER_STARTED = True
    except RuntimeError as exc:
        logging.warning("Updater start ignored: %s", exc)


async def _sync_commands() -> None:
    try:
        await tree.sync()
    except Exception as exc:
        logging.warning("Global command sync failed: %s", exc)
    for g in bot.guilds:
        try:
            tree.copy_global_to(guild=g)
            await tree.sync(guild=g)
        except Exception as exc:
            logging.warning("Guild sync failed for %s: %s", getattr(g, 'id', '?'), exc)


@bot.event
async def on_ready():
    global _STARTUP_DONE, _STARTUP_LOCK, _LAST_HEALTH
    if bot.user:
        logging.info("Logged in as %s (ID: %s)", bot.user, bot.user.id)

    if _STARTUP_LOCK is None:
        _STARTUP_LOCK = asyncio.Lock()
    async with _STARTUP_LOCK:
        if _STARTUP_DONE:
            # Gateway RESUME, not a cold start. Re-running the health sweep here
            # is what used to pile up concurrent fetch storms.
            logging.info("Reconnected; startup already completed.")
            return
        _STARTUP_DONE = True

    await init_db()
    await _sync_commands()
    _write_heartbeat()

    engine = _engine_for_guild(None)
    health = await perform_startup_health(engine)
    _LAST_HEALTH = health
    required_ok = all(health.get(src) for src in REQUIRED_SOURCES)
    if not required_ok:
        logging.warning(
            "Required sources degraded at startup; the updater will retry on its own schedule."
        )
    if required_ok or not STARTUP_HEALTH_BLOCK:
        _start_updater()
    else:
        asyncio.create_task(_deferred_updater_start(engine))

    if not health_refresher.is_running():
        health_refresher.start()


async def _deferred_updater_start(engine: ForecastEngine) -> None:
    """Only used when STARTUP_HEALTH_BLOCK is explicitly enabled."""
    global _LAST_HEALTH
    for attempt in range(1, 6):
        await asyncio.sleep(60)
        health = await perform_startup_health(engine)
        _LAST_HEALTH = health
        if all(health.get(src) for src in REQUIRED_SOURCES):
            logging.info("Health recovered on attempt %d; starting updater.", attempt)
            _start_updater()
            return
    logging.error("Health retries exhausted; starting updater anyway.")
    _start_updater()

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
                    # Forget the message but keep the channel, so the next cycle
                    # reposts instead of silently going dark forever.
                    await set_message_id(guild_id, None)
                    logging.info(
                        "Guild %s: tracked aurora message deleted; will repost on the next update.",
                        guild_id,
                    )
                except Exception:
                    logging.exception("Guild %s: failed to clear tracked message id.", guild_id)
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
    try:
        _content, _t, _m, _w, _d, engine, build = await build_update_for_guild(interaction.guild, cfg)
    except Exception as exc:
        await interaction.followup.send(
            f"Could not reach the forecast sources right now ({type(exc).__name__}). Try again shortly.",
            ephemeral=True,
        )
        return
    embed = format_embed(build, engine)
    msg = await _publish_embed(interaction.guild, channel, existing_id, embed)
    if msg is None:
        await interaction.followup.send(
            f"Could not post in {channel.mention}. Check the bot's Send Messages and Embed Links permissions.",
            ephemeral=True,
        )
        return
    await set_started(interaction.guild_id, True)
    await interaction.followup.send(f"Aurora message is live in {channel.mention}.", ephemeral=True)

@tree.command(name="aurora-stop", description="Stop aurora updates in this server")
async def stop_cmd(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    if interaction.guild_id is None:
        await interaction.followup.send("Guild context required.", ephemeral=True)
        return
    await clear_channel(interaction.guild_id)
    _LAST_GOOD_BUILD.pop(interaction.guild_id, None)
    await interaction.followup.send(
        "Aurora updates stopped. The existing message stays put. "
        "Use /aurora-set-channel and /aurora-start to resume.",
        ephemeral=True,
    )


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
    try:
        _content, _t, _m, window_id, det_sig, engine, build = await build_update_for_guild(interaction.guild, cfg)
    except Exception as exc:
        await interaction.followup.send(
            f"Could not reach the forecast sources right now ({type(exc).__name__}). Try again shortly.",
            ephemeral=True,
        )
        return
    embed = format_embed(build, engine)
    msg = await _publish_embed(interaction.guild, channel, tracked_id, embed)
    if msg is None:
        await interaction.followup.send("Failed to update the message.", ephemeral=True)
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

async def _publish_embed(
    guild: discord.Guild,
    channel: discord.TextChannel,
    tracked_id,
    embed: discord.Embed,
) -> Optional[discord.Message]:
    """Edit the tracked embed, adopting or reposting it if it has gone missing."""
    if tracked_id:
        try:
            msg = await channel.fetch_message(int(tracked_id))
            await msg.edit(embed=embed)
            return msg
        except discord.NotFound:
            logging.info("Guild %s: tracked message missing; looking for a replacement.", guild.id)
        except discord.Forbidden:
            logging.warning("Guild %s: missing permissions to edit in #%s.", guild.id, channel.name)
            return None
        except Exception:
            logging.exception("Guild %s: failed to edit tracked message.", guild.id)
            return None

    latest = await _find_latest_bot_embed(channel)
    if latest:
        try:
            await latest.edit(embed=embed)
            await set_message_id(guild.id, latest.id)
            return latest
        except Exception:
            logging.exception("Guild %s: failed to adopt latest bot embed.", guild.id)

    try:
        msg = await channel.send(embed=embed)
        await set_message_id(guild.id, msg.id)
        logging.info("Guild %s: posted a fresh aurora embed in #%s.", guild.id, channel.name)
        return msg
    except discord.Forbidden:
        logging.warning("Guild %s: missing permissions to post in #%s.", guild.id, channel.name)
    except Exception:
        logging.exception("Guild %s: failed to post a replacement embed.", guild.id)
    return None


async def _run_update_cycle() -> bool:
    """One pass over every configured guild. Returns True if all guilds updated."""
    all_ok = True
    for guild in bot.guilds:
        try:
            cfg = await get_config(guild.id)
            if not cfg or not cfg.get('channel_id'):
                continue
            channel = guild.get_channel(int(cfg['channel_id']))
            if not isinstance(channel, discord.TextChannel):
                logging.warning("Guild %s: configured channel is not a text channel.", guild.id)
                continue
            tracked_id = cfg.get('message_id')
            if not tracked_id and not cfg.get('started'):
                logging.info("Guild %s: awaiting /aurora-start.", guild.id)
                continue
            logging.info("Updater iteration guild=%s", guild.id)

            stale_since = None
            try:
                _content, tonight_url, tomorrow_url, window_id, det_sig, engine, build = \
                    await build_update_for_guild(guild, cfg)
            except Exception as exc:
                # Upstream is down. Show the last good data, clearly marked,
                # rather than leaving an embed that silently rots.
                all_ok = False
                net.log_fetch_error(f"guild:{guild.id}", exc)
                cached = _LAST_GOOD_BUILD.get(guild.id)
                if not cached:
                    continue
                stale_since, build, engine = cached
                window_id, det_sig = '', ''

            embed = format_embed(build, engine, stale_since_ts=stale_since)
            msg = await _publish_embed(guild, channel, tracked_id, embed)
            if msg is None:
                all_ok = False
                continue
            if stale_since:
                continue
            combined_id = f"{window_id}|{det_sig}" if window_id else ''
            prev = cfg.get('last_window_id') or ''
            if combined_id:
                ts_now = int(datetime.now(timezone.utc).timestamp())
                if not prev:
                    await set_last_window(guild.id, combined_id, ts_now)
                elif combined_id != prev:
                    old_sig = prev.split('|', 1)[1] if '|' in prev else ''
                    old_tokens = {t for t in old_sig.split('|') if t}
                    new_tokens = {t for t in det_sig.split('|') if t}
                    added = sorted(new_tokens - old_tokens)
                    if added:
                        alert_text = compose_alert_message(build, engine, added)
                        if alert_text:
                            try:
                                alert_msg = await channel.send(alert_text)
                                asyncio.create_task(_auto_delete(alert_msg, ALERT_DELETE_AFTER_MINUTES))
                                logging.info(
                                    "Guild %s: sent alert for %d new window(s)/reading(s).",
                                    guild.id, len(added),
                                )
                            except discord.Forbidden:
                                logging.warning("Guild %s: cannot post alert, missing permissions.", guild.id)
                            except Exception:
                                logging.exception("Failed to send aurora alert message")
                    await set_last_window(guild.id, combined_id, ts_now)
        except Exception as e:
            all_ok = False
            logging.exception("Update failed for guild %s: %s", guild.id, e)
    return all_ok


@tasks.loop(hours=UPDATE_INTERVAL_HOURS)
async def updater():
    await bot.wait_until_ready()
    ok = await _run_update_cycle()
    _write_heartbeat()
    if not ok and UPDATE_RETRY_MINUTES > 0:
        # Do not wait a full interval after a transient upstream failure.
        asyncio.create_task(_retry_update_soon())


async def _retry_update_soon() -> None:
    await asyncio.sleep(max(1, UPDATE_RETRY_MINUTES) * 60)
    if updater.is_running():
        try:
            logging.info("Retrying update after an earlier failure.")
            if await _run_update_cycle():
                _write_heartbeat()
        except Exception:
            logging.exception("Retry update cycle failed")


@updater.error
async def updater_error(exc: BaseException) -> None:
    logging.exception("Updater loop crashed; restarting it.", exc_info=exc)
    with contextlib.suppress(Exception):
        updater.restart()


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
