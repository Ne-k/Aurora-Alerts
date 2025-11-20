from __future__ import annotations
import os
from typing import Optional, Any, Dict
import aiosqlite

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'aurora.db')
DB_PATH = os.path.abspath(DB_PATH)

SCHEMA = """
CREATE TABLE IF NOT EXISTS guild_config (
  guild_id INTEGER PRIMARY KEY,
  channel_id INTEGER,
  kp_threshold REAL,
  latitude REAL,
  longitude REAL,
  location_name TEXT,
  message_id INTEGER,
  last_window_id TEXT,
  last_alert_ts INTEGER,
  updated_at INTEGER
);
"""

async def init_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    async with aiosqlite.connect(DB_PATH) as db:
        await db.executescript(SCHEMA)
        await db.commit()

async def get_config(guild_id: int) -> Optional[Dict[str, Any]]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM guild_config WHERE guild_id = ?", (guild_id,)) as cur:
            row = await cur.fetchone()
            return dict(row) if row else None

async def upsert_config(guild_id: int, **kwargs):
    existing = await get_config(guild_id)
    fields = [
        'channel_id','kp_threshold','latitude','longitude','location_name','message_id','last_window_id','last_alert_ts','updated_at'
    ]
    data = {k: kwargs.get(k) if k in kwargs else (existing.get(k) if existing else None) for k in fields}
    async with aiosqlite.connect(DB_PATH) as db:
        if existing:
            await db.execute(
                """
                UPDATE guild_config SET channel_id=?, kp_threshold=?, latitude=?, longitude=?, location_name=?, message_id=?, last_window_id=?, last_alert_ts=?, updated_at=?
                WHERE guild_id=?
                """,
                (data['channel_id'], data['kp_threshold'], data['latitude'], data['longitude'], data['location_name'], data['message_id'], data['last_window_id'], data['last_alert_ts'], data['updated_at'], guild_id)
            )
        else:
            await db.execute(
                """
                INSERT INTO guild_config (guild_id, channel_id, kp_threshold, latitude, longitude, location_name, message_id, last_window_id, last_alert_ts, updated_at)
                VALUES (?,?,?,?,?,?,?,?,?,?)
                """,
                (guild_id, data['channel_id'], data['kp_threshold'], data['latitude'], data['longitude'], data['location_name'], data['message_id'], data['last_window_id'], data['last_alert_ts'], data['updated_at'])
            )
        await db.commit()

async def set_channel(guild_id: int, channel_id: Optional[int]):
    await upsert_config(guild_id, channel_id=channel_id)

async def set_threshold(guild_id: int, kp_threshold: float):
    await upsert_config(guild_id, kp_threshold=kp_threshold)

async def set_location(guild_id: int, latitude: float, longitude: float, location_name: str):
    await upsert_config(guild_id, latitude=latitude, longitude=longitude, location_name=location_name)

async def set_message_id(guild_id: int, message_id: Optional[int]):
    """Set or clear the tracked aurora message id. Pass None to clear."""
    await upsert_config(guild_id, message_id=message_id)

async def set_last_window(guild_id: int, window_id: str, alerted_ts: int):
    await upsert_config(guild_id, last_window_id=window_id, last_alert_ts=alerted_ts)

async def clear_channel(guild_id: int):
    """Clear the configured channel and tracked message for a guild."""
    await upsert_config(guild_id, channel_id=None, message_id=None)
