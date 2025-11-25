from __future__ import annotations
import os
import re
import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone, date
import math
from typing import Dict, List, Optional, Tuple

import pytz
import requests
from dotenv import load_dotenv


@dataclass
class GFZRecord:
    timestamp: datetime
    value: float
    status: str

@dataclass
class Detection:
    day_label: str
    day_date: date
    ut_block: str
    kp: float
    start_ts: int
    end_ts: int
    cloud_avg_display: str
    visibility_pct: int
    local_date_label: str
    bullet: str

@dataclass
class AlertBuild:
    message: str
    detections: List[Detection]
    window_id: str
    cloud_available: bool
    afm_snapshot: Optional[dict]
    tonight_image_url: str
    tomorrow_image_url: str
    ovation_prob: Optional[int]
    maf_summary: Optional[str]
    afm_tonight_line: Optional[str]
    afm_conditions_line: Optional[str]
    afm_next_hours_lines: Optional[List[str]]
    detection_groups: Dict[str, List[str]]
    recommendation_lines: List[str]
    upcoming_days_lines: List[str]
    gfz_summary_lines: List[str]
    gfz_latest_line: Optional[str]
    gfz_source_note: Optional[str]
    swpc_planetary_line: Optional[str]
    swpc_summary_lines: List[str]
    swpc_source_note: Optional[str]
    aggregated_sources_line: Optional[str]
    all_forecast_lines: List[str]
    # Real-time high Kp blocks (for alert triggering, reduces false positives from forecast-only windows)
    gfz_high_blocks: List[dict]
    swpc_high_blocks: List[dict]
    swpc_high_block: Optional[dict]
KP_EQ_BOUNDARY = [
    (0.0, 80.0),
    (1.0, 75.0),
    (2.0, 70.0),
    (3.0, 67.0),
    (4.0, 64.0),
    (5.0, 61.0),
    (6.0, 58.0),
    (7.0, 55.0),
    (8.0, 53.0),
    (9.0, 51.0),
]


class ForecastEngine:
    """
    Extracts NOAA Kp windows, enriches with clouds and AFM, and builds a message string.
    This class does NOT talk to Discord. Callers can format into embeds as desired.
    """
    def __init__(self,
                 kp_threshold: float = 6.5,
                 latitude: float = 45.5152,
                 longitude: float = -122.6784,
                 location_name: str = "Portland, OR",
                 timezone_name: str = "America/Los_Angeles",
                 cloud_cover_good_max: float = 60,
                 cloud_cover_partial_max: float = 80,
                 gfz_api_url: str = 'https://kp.gfz.de/app/json/'):
        load_dotenv()
        self.url = "https://services.swpc.noaa.gov/text/3-day-forecast.txt"
        self.kp_threshold = kp_threshold
        self.latitude = latitude
        self.longitude = longitude
        self.location_name = location_name
        self.timezone_name = timezone_name
        self.cloud_cover_good_max = cloud_cover_good_max
        self.cloud_cover_partial_max = cloud_cover_partial_max
        base_candidate = os.getenv('GFZ_JSON_BASE_URL') or gfz_api_url
        if not base_candidate:
            base_candidate = 'https://kp.gfz.de/app/json/'
        self.gfz_json_base = base_candidate if base_candidate.endswith('/') else f"{base_candidate}/"
        # Retain attribute name for backwards compatibility / debugging
        self.gfz_api_url = self.gfz_json_base

    def fetch_forecast(self) -> str:
        r = requests.get(self.url, timeout=20)
        r.raise_for_status()
        return r.text

    def fetch_cloud_cover(self) -> Dict[datetime, int]:
        base = "https://api.open-meteo.com/v1/forecast"
        params = (
            f"latitude={self.latitude}&longitude={self.longitude}&hourly=cloudcover&timezone=UTC&forecast_days=3"
        )
        url = f"{base}?{params}"
        data: Dict[datetime, int] = {}
        try:
            r = requests.get(url, timeout=15)
            r.raise_for_status()
            j = r.json()
            hours = j.get('hourly', {}).get('time', [])
            cover = j.get('hourly', {}).get('cloudcover', [])
            for t_str, cc in zip(hours, cover):
                try:
                    dt = datetime.fromisoformat(t_str.replace('Z', '+00:00')).astimezone(timezone.utc)
                    data[dt] = int(cc)
                except Exception:
                    continue
        except Exception:
            pass
        # Fallback to OpenWeather if no data and API key is present
        if not data:
            try:
                ow = self.fetch_cloud_cover_openweather()
                if ow:
                    data = ow
            except Exception:
                pass
        return data

    def fetch_cloud_cover_openweather(self) -> Dict[datetime, int]:
        """Optional fallback cloud cover from OpenWeatherMap.
        Requires environment variable OPENWEATHER_API_KEY.
        Enforces daily cap, min interval, and uses on-disk cache per location.
        Returns a map of UTC datetimes to cloud %.
        """
        api_key = os.getenv('OPENWEATHER_API_KEY', '').strip()
        if not api_key:
            return {}

        # Throttling & caching parameters
        max_calls = int(os.getenv('OPENWEATHER_MAX_CALLS_PER_DAY', '900'))
        min_interval_min = int(os.getenv('OPENWEATHER_MIN_INTERVAL_MIN', '30'))
        cache_ttl_min = int(os.getenv('OPENWEATHER_CACHE_TTL_MIN', '120'))

        # Paths
        data_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'data'))
        try:
            os.makedirs(data_dir, exist_ok=True)
        except Exception:
            pass
        usage_path = os.path.join(data_dir, 'openweather_usage.json')
        cache_path = os.path.join(
            data_dir, f"ow_cache_{round(self.latitude,2)}_{round(self.longitude,2)}.json"
        )

        def _load_json(p: str) -> Optional[dict]:
            try:
                with open(p, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception:
                return None

        def _save_json(p: str, obj: dict) -> None:
            try:
                with open(p, 'w', encoding='utf-8') as f:
                    json.dump(obj, f)
            except Exception:
                pass

        out: Dict[datetime, int] = {}
        now = datetime.now(timezone.utc)
        today = now.date().isoformat()
        usage = _load_json(usage_path) or {}
        if usage.get('date') != today:
            usage = {'date': today, 'count': 0, 'last_call_ts': 0}

        # Try cache first if fresh
        cache = _load_json(cache_path) or {}
        fetched_ts = cache.get('fetched_ts')
        if isinstance(fetched_ts, int):
            age_min = int((now.timestamp() - fetched_ts) / 60)
            if age_min <= cache_ttl_min:
                hours = cache.get('hours') or []
                for h in hours:
                    try:
                        ts = int(h.get('ts'))
                        clouds = int(h.get('clouds'))
                        out[datetime.fromtimestamp(ts, tz=timezone.utc)] = clouds
                    except Exception:
                        continue
                if out:
                    return out

        # Enforce daily cap and minimum interval
        last_call_ts = int(usage.get('last_call_ts') or 0)
        since_last_min = int((now.timestamp() - last_call_ts) / 60)
        if usage.get('count', 0) >= max_calls or since_last_min < min_interval_min:
            # Respect limits; return stale cache if present, else empty
            return out

        # Try One Call 3.0
        try:
            url = (
                f"https://api.openweathermap.org/data/3.0/onecall?lat={self.latitude}&lon={self.longitude}"
                f"&exclude=minutely,daily,alerts,current&appid={api_key}&units=metric"
            )
            r = requests.get(url, timeout=15)
            if r.status_code == 200:
                j = r.json()
                for h in j.get('hourly', []) or []:
                    try:
                        ts = int(h.get('dt'))
                        clouds = int(h.get('clouds'))
                        dt_utc = datetime.fromtimestamp(ts, tz=timezone.utc)
                        out[dt_utc] = clouds
                    except Exception:
                        continue
                if out:
                    usage['count'] = int(usage.get('count', 0)) + 1
                    usage['last_call_ts'] = int(now.timestamp())
                    _save_json(usage_path, usage)
                    _save_json(cache_path, {
                        'lat': self.latitude,
                        'lon': self.longitude,
                        'fetched_ts': int(now.timestamp()),
                        'hours': [{'ts': int(dt.timestamp()), 'clouds': val} for dt, val in out.items()]
                    })
                    return out
        except Exception:
            pass

        # Fallback to 2.5 5-day/3-hour forecast if needed
        try:
            url = (
                f"https://api.openweathermap.org/data/2.5/forecast?lat={self.latitude}&lon={self.longitude}"
                f"&appid={api_key}&units=metric"
            )
            r = requests.get(url, timeout=15)
            if r.status_code == 200:
                j = r.json()
                for it in j.get('list', []) or []:
                    try:
                        ts = int(it.get('dt'))
                        cval = (it.get('clouds') or {}).get('all')
                        if cval is None:
                            continue
                        clouds = int(cval)
                        dt_utc = datetime.fromtimestamp(ts, tz=timezone.utc)
                        out[dt_utc] = clouds
                    except Exception:
                        continue
                if out:
                    usage['count'] = int(usage.get('count', 0)) + 1
                    usage['last_call_ts'] = int(now.timestamp())
                    _save_json(usage_path, usage)
                    _save_json(cache_path, {
                        'lat': self.latitude,
                        'lon': self.longitude,
                        'fetched_ts': int(now.timestamp()),
                        'hours': [{'ts': int(dt.timestamp()), 'clouds': val} for dt, val in out.items()]
                    })
        except Exception:
            pass
        return out

    def fetch_ovation_probability(self, lat: float, lon: float) -> Optional[int]:
        """Fetch NOAA Ovation aurora latest JSON and estimate the current
        probability (%) at the given lat/lon using nearest-neighbor on the grid.
        Returns an integer 0..100 or None if unavailable.
        """
        url = "https://services.swpc.noaa.gov/json/ovation_aurora_latest.json"
        try:
            r = requests.get(url, timeout=15)
            r.raise_for_status()
            j = r.json()
            candidates: List[Tuple[float, float, float]] = []  # (lat, lon, prob)
            # Known structures seen in variants of this dataset
            # 1) FeatureCollection with features[{geometry:{coordinates:[lon,lat]}, properties:{probability:..}}]
            if isinstance(j, dict) and 'features' in j and isinstance(j['features'], list):
                for feat in j['features']:
                    try:
                        coords = feat.get('geometry', {}).get('coordinates')
                        props = feat.get('properties', {})
                        if isinstance(coords, (list, tuple)) and len(coords) >= 2:
                            flon, flat = float(coords[0]), float(coords[1])
                            prob = props.get('probability')
                            if prob is None:
                                prob = props.get('value')
                            if prob is not None:
                                candidates.append((flat, flon, float(prob)))
                    except Exception:
                        continue
            # 2) Plain dict with 'coordinates' as list of [lon, lat, prob] triplets
            if not candidates and isinstance(j, dict) and 'coordinates' in j:
                coords = j.get('coordinates')
                # Sometimes it's nested lists
                def walk(obj):
                    if isinstance(obj, (list, tuple)):
                        if len(obj) == 3 and all(isinstance(x, (int, float)) for x in obj):
                            yield obj
                        else:
                            for it in obj:
                                yield from walk(it)
                for trip in walk(coords):
                    try:
                        flon, flat, p = float(trip[0]), float(trip[1]), float(trip[2])
                        candidates.append((flat, flon, p))
                    except Exception:
                        continue
            # 3) Dict with arrays: {'latitude': [...], 'longitude': [...], 'probability': [...]}
            if not candidates and isinstance(j, dict) and all(k in j for k in ('latitude','longitude','probability')):
                lats = j.get('latitude') or []
                lons = j.get('longitude') or []
                probs = j.get('probability') or []
                for flat, flon, p in zip(lats, lons, probs):
                    try:
                        candidates.append((float(flat), float(flon), float(p)))
                    except Exception:
                        continue
            if not candidates:
                return None
            # nearest neighbor by simple haversine approximation (small distances)
            best = None
            best_d2 = 1e18
            for flat, flon, p in candidates:
                dlat = flat - lat
                dlon = (flon - lon) * (0.5 if abs(lat) < 60 else 1.0)  # coarse lon scaling
                d2 = dlat*dlat + dlon*dlon
                if d2 < best_d2:
                    best_d2 = d2
                    best = p
            if best is None:
                return None
            return max(0, min(100, int(round(best))))
        except Exception:
            return None

    def fetch_maf_data(self, lat: float, lon: float, tz_name: str) -> Optional[dict]:
        """Fetch data from My Aurora Forecast API (v2). Uses environment variables:
        - MAF_APP_USER_ID (required by the service); if missing, request may fail.
        - MAF_USER_AGENT (optional; default provided).
        """
        url = "https://www.jrustonapps.com/app-apis/aurora/get-data-v2.php"
        app_user_id = os.getenv('MAF_APP_USER_ID', '').strip()
        ua = os.getenv('MAF_USER_AGENT', 'My Aurora Forecast/1 CFNetwork/3860.200.71 Darwin/25.1.0')
        headers = {
            'User-Agent': ua,
            'Accept': '*/*',
            'Content-Type': 'application/x-www-form-urlencoded',
            'Accept-Language': 'en-US,en;q=0.9',
            'App-User-ID': app_user_id,
            'App-Request-Time': str(int(datetime.now(timezone.utc).timestamp()))
        }
        data = {
            'latitude': str(lat),
            'longitude': str(lon),
            'timezone': tz_name,
        }
        try:
            # Explicitly disable proxies (disregard any proxy settings)
            r = requests.post(url, headers=headers, data=data, timeout=20, proxies={})
            r.raise_for_status()
            # First try direct JSON
            try:
                return r.json()
            except Exception:
                pass
            # Some responses are wrapped, e.g. "<--STARTOFCONTENT-->{...}"
            txt = r.text or ""
            # Find the first JSON object in the text
            start = txt.find('{')
            end = txt.rfind('}')
            if start != -1 and end != -1 and end > start:
                snippet = txt[start:end+1]
                try:
                    return json.loads(snippet)
                except Exception:
                    # Last resort: return raw
                    return {'raw': txt}
            return {'raw': txt}
        except Exception:
            return None

    def fetch_gfz_series(self, start: datetime, end: datetime, index: str = 'Kp', status: Optional[str] = None) -> Tuple[List[GFZRecord], Optional[dict]]:
        """Fetch GFZ geomagnetic index data (default: Kp) between start and end timestamps.
        Returns a tuple of (records, meta) where records is a list of GFZRecord sorted by time.
        """
        base_url = self.gfz_json_base or 'https://kp.gfz.de/app/json/'
        params = {
            'start': start.strftime('%Y-%m-%dT%H:%M:%SZ'),
            'end': end.strftime('%Y-%m-%dT%H:%M:%SZ'),
            'index': index,
        }
        if status:
            params['status'] = status
        headers = {
            'User-Agent': os.getenv('GFZ_USER_AGENT', 'AuroraAlertsBot/1.0 (+https://github.com/Ne-k/Aurora-Alerts)')
        }
        try:
            r = requests.get(base_url, params=params, timeout=20, headers=headers)
            r.raise_for_status()
            payload = r.json()
        except Exception:
            return [], None
        if not isinstance(payload, dict):
            return [], None
        datetimes = payload.get('datetime')
        values_raw = payload.get(index) or payload.get(str(index).lower())
        statuses = payload.get('status') or []
        if not isinstance(datetimes, list) or not isinstance(values_raw, list):
            return [], payload.get('meta') if isinstance(payload.get('meta'), dict) else None
        records: List[GFZRecord] = []
        for idx, dt_str in enumerate(datetimes):
            try:
                raw_val = values_raw[idx]
            except Exception:
                continue
            try:
                val = float(raw_val)
            except Exception:
                continue
            try:
                ts = datetime.fromisoformat(str(dt_str).replace('Z', '+00:00')).astimezone(timezone.utc)
            except Exception:
                continue
            status_code = ''
            if isinstance(statuses, list) and idx < len(statuses):
                status_code = str(statuses[idx] or '').strip()
            records.append(GFZRecord(timestamp=ts, value=val, status=status_code))
        meta = payload.get('meta') if isinstance(payload.get('meta'), dict) else payload.get('meta')
        meta = meta if isinstance(meta, dict) else None
        return records, meta

    @staticmethod
    def _gfz_status_label(code: str) -> str:
        if not code:
            return "Unspecified"
        mapping = {
            'pre': 'Preliminary',
            'def': 'Definitive',
            'now': 'Nowcast',
        }
        lower = code.lower()
        if lower in mapping:
            return mapping[lower]
        return code.upper() if len(code) <= 4 else code

    def _latitude_factor(self, kp_value: float) -> float:
        try:
            kp_clamped = max(0.0, min(9.0, float(kp_value)))
        except Exception:
            return 1.0
        boundary = KP_EQ_BOUNDARY[0][1]
        for idx in range(len(KP_EQ_BOUNDARY) - 1):
            k0, lat0 = KP_EQ_BOUNDARY[idx]
            k1, lat1 = KP_EQ_BOUNDARY[idx + 1]
            if k0 <= kp_clamped <= k1:
                if k1 == k0:
                    boundary = lat0
                else:
                    frac = (kp_clamped - k0) / (k1 - k0)
                    boundary = lat0 + (lat1 - lat0) * frac
                break
        else:
            if kp_clamped >= KP_EQ_BOUNDARY[-1][0]:
                boundary = KP_EQ_BOUNDARY[-1][1]
        lat_abs = abs(self.latitude)
        diff = boundary - lat_abs
        if diff <= 0:
            return 1.0
        # Exponential decay keeps a soft floor while penalizing large latitude gaps
        return max(0.05, math.exp(-diff / 6.0))

    def gfz_recent_blocks(self, hours_back: int = 72) -> dict:
        hours_window = max(3, min(240, int(hours_back)))
        end = datetime.now(timezone.utc)
        start = end - timedelta(hours=hours_window)
        records, meta = self.fetch_gfz_series(start, end)
        local_tz = pytz.timezone(self.timezone_name)
        rows = []
        for rec in records:
            ts = int(rec.timestamp.timestamp())
            local_dt = rec.timestamp.astimezone(local_tz)
            rows.append({
                'ts': ts,
                'kp': round(rec.value, 3),
                'status': rec.status,
                'status_label': self._gfz_status_label(rec.status),
                'local_label': local_dt.strftime('%b %d %H:%M'),
            })
        latest = rows[-1] if rows else None
        source_note = None
        if isinstance(meta, dict):
            source = meta.get('source')
            license_info = meta.get('license')
            if source and license_info:
                source_note = f"{source} ({license_info})"
            elif source:
                source_note = str(source)
            elif license_info:
                source_note = str(license_info)
        if not source_note:
            source_note = 'GFZ German Research Centre for Geosciences (CC BY 4.0)'
        return {
            'records': rows,
            'latest': latest,
            'source_note': source_note,
            'meta': meta,
        }

    def fetch_swpc_planetary_k_latest(self) -> Tuple[Optional[dict], List[dict]]:
        url = "https://services.swpc.noaa.gov/json/planetary_k_index_1m.json"
        try:
            r = requests.get(url, timeout=15)
            r.raise_for_status()
            data = r.json()
        except Exception:
            return None, []
        if not isinstance(data, list):
            return None, []
        latest = None
        high_blocks: List[dict] = []
        now = datetime.now(timezone.utc)
        for item in data:
            if not isinstance(item, dict):
                continue
            time_tag = item.get('time_tag') or item.get('timeTag')
            kp_index = item.get('kp_index')
            est_kp = item.get('estimated_kp')
            flag = item.get('kp')
            try:
                ts = datetime.fromisoformat(str(time_tag))
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)
                else:
                    ts = ts.astimezone(timezone.utc)
            except Exception:
                continue
            latest = {
                'timestamp': ts,
                'kp_index': float(kp_index) if isinstance(kp_index, (int, float)) else None,
                'estimated_kp': float(est_kp) if isinstance(est_kp, (int, float)) else None,
                'kp_flag': str(flag) if flag is not None else None,
            }
            # Collect high Kp entries within the last 12 hours
            effective_candidates: List[float] = []
            if isinstance(kp_index, (int, float)):
                effective_candidates.append(float(kp_index))
            if isinstance(est_kp, (int, float)):
                effective_candidates.append(float(est_kp))
            effective = max(effective_candidates) if effective_candidates else None
            if isinstance(effective, (int, float)):
                age_hours = (now - ts).total_seconds() / 3600.0
                if age_hours <= 12 and effective >= self.kp_threshold:
                    kind = 'observed' if isinstance(kp_index, (int, float)) and float(kp_index) == effective else 'estimated'
                    high_blocks.append({
                        'ts': int(ts.timestamp()),
                        'kp': round(float(effective), 2),
                        'kind': kind,
                    })
        high_blocks.sort(key=lambda blk: blk.get('ts', 0))
        return latest, high_blocks

    def fetch_swpc_hemi_power(self) -> Optional[dict]:
        """Fetch hemispheric power (GW). Parse current SWPC tabular feed (observation + forecast + north + south).
        Returns latest dict with: timestamp (observation time), forecast_timestamp, north_gw, south_gw, total_gw.
        The public text feed currently has 4 columns after comments:
        Observation(YYYY-MM-DD_HH:MM) Forecast(YYYY-MM-DD_HH:MM) NorthGW SouthGW
        Older formats had more tokens; this parser adapts accordingly.
        """
        text_url = "https://services.swpc.noaa.gov/text/aurora-nowcast-hemi-power.txt"
        latest: Optional[dict] = None
        try:
            r = requests.get(text_url, timeout=15)
            r.raise_for_status()
            for raw_line in r.text.splitlines():
                line = raw_line.strip()
                if not line or line.startswith('#'):
                    continue
                parts = [p for p in line.split() if p]
                # Expect at least 4 parts: obs, forecast, north, south
                if len(parts) < 4:
                    continue
                obs_str, fc_str = parts[0], parts[1]
                north_str, south_str = parts[2], parts[3]
                try:
                    obs_dt = datetime.strptime(obs_str, "%Y-%m-%d_%H:%M").replace(tzinfo=timezone.utc)
                    fc_dt = datetime.strptime(fc_str, "%Y-%m-%d_%H:%M").replace(tzinfo=timezone.utc)
                except Exception:
                    continue
                try:
                    north_val = float(north_str)
                    south_val = float(south_str)
                except Exception:
                    continue
                # Derive total: choose max as representation of peak hemispheric power
                total_val = max(north_val, south_val)
                latest = {
                    'timestamp': obs_dt,
                    'forecast_timestamp': fc_dt,
                    'north_gw': north_val,
                    'south_gw': south_val,
                    'total_gw': total_val,
                    'source': 'text',
                }
            return latest
        except Exception:
            return None

    def visibility_percent(
        self,
        kp: float,
        cloud_avg: Optional[float],
        ovation_prob: Optional[int] = None,
        sky_darkness: Optional[str] = None,
        maf_prob: Optional[int] = None,
        gfz_kp: Optional[float] = None,
        swpc_kp: Optional[float] = None,
        hemi_power: Optional[float] = None,
    ) -> int:
        """Combined visibility percentage using multiple signals.
        Design goal: avoid hard 0% from any single source (e.g., Ovation=0) while still reflecting conditions.
        - kp_factor: min(1, kp/9) — storm strength
        - cloud_factor: 1 - clouds (if unknown, assume 1)
        - dark_factor: 1.0 at night, 0.6 twilight, 0.25 daylight
        - ovation_weight: if provided, scale to a soft factor with a minimum floor so 0 doesn't zero out score
        - maf_weight: same idea for MAF probability if present
        - lat_factor: penalize locations far equatorward of the estimated auroral oval boundary for the given Kp
        - swpc_kp / hemi_power: include NOAA real-time measurements for extra confidence
        Final score = base * ovation_weight * maf_weight, clamped to 0..100
        """
        effective_kp = kp
        if isinstance(gfz_kp, (int, float)):
            effective_kp = max(effective_kp, float(gfz_kp))
        if isinstance(swpc_kp, (int, float)):
            effective_kp = max(effective_kp, float(swpc_kp))
        # Core multiplicative base
        kp_factor = max(0.0, min(1.0, effective_kp / 9.0))
        cloud_factor = 1.0 if cloud_avg is None else max(0.0, 1.0 - (cloud_avg / 100.0))
        dark_factor = 1.0
        if isinstance(sky_darkness, str):
            key = sky_darkness.strip().lower()
            if 'day' in key:
                dark_factor = 0.25
            elif 'twilight' in key:
                dark_factor = 0.6
            else:
                dark_factor = 1.0
        lat_factor = self._latitude_factor(effective_kp)
        hemi_weight = 1.0
        if isinstance(hemi_power, (int, float)):
            try:
                hemi_norm = max(0.0, min(1.2, float(hemi_power) / 60.0))
                hemi_weight = 0.3 + 0.7 * min(1.0, hemi_norm)
            except Exception:
                hemi_weight = 1.0
        base = kp_factor * cloud_factor * dark_factor * lat_factor * hemi_weight

        # Soft weights for ovation and MAF: map [0..100] -> [floor..1]
        def soft_weight(pct: Optional[int], floor: float) -> float:
            if pct is None:
                return 1.0
            x = max(0.0, min(1.0, float(pct) / 100.0))
            return floor + (1.0 - floor) * x

        ovation_weight = soft_weight(ovation_prob, floor=0.25)
        maf_weight = soft_weight(maf_prob, floor=0.5)

        score = base * ovation_weight * maf_weight
        return max(0, min(100, int(round(100.0 * score))))

    def fetch_aurora_snapshot(self, lat: float, lon: float) -> Optional[dict]:
        url = f"https://auroraforecast.me/api/seoSnapshot?lat={lat}&lon={lon}"
        headers = {
            'User-Agent': 'Mozilla/5.0',
            'Accept': '*/*',
            'Referer': 'https://auroraforecast.me/portland',
        }
        try:
            try:
                import cloudscraper  # type: ignore
                scraper = cloudscraper.create_scraper()
                r = scraper.get(url, headers=headers, timeout=15)
            except Exception:
                r = requests.get(url, headers=headers, timeout=15)
            if r.status_code == 200:
                return r.json()
        except Exception:
            pass
        return None

    def short_term_visibility_series(self, minutes: int = 30, step: int = 5) -> dict:
        """Compute short-term viewing probability for the next `minutes` in `step`-minute increments."""
        now_utc = datetime.now(timezone.utc)

        # Core data sources
        maf = self.fetch_maf_data(self.latitude, self.longitude, self.timezone_name)
        ovation_prob = self.fetch_ovation_probability(self.latitude, self.longitude)
        clouds = self.fetch_cloud_cover()
        snapshot = self.fetch_aurora_snapshot(self.latitude, self.longitude)
        gfz_records, gfz_meta = self.fetch_gfz_series(now_utc - timedelta(hours=24), now_utc + timedelta(hours=3))
        gfz_latest = gfz_records[-1] if gfz_records else None
        gfz_latest_status = self._gfz_status_label(gfz_latest.status) if gfz_latest else None
        gfz_source_note = None
        if isinstance(gfz_meta, dict):
            source = gfz_meta.get('source')
            license_info = gfz_meta.get('license')
            if source and license_info:
                gfz_source_note = f"{source} ({license_info})"
            elif source:
                gfz_source_note = str(source)
            elif license_info:
                gfz_source_note = str(license_info)
        if not gfz_source_note:
            gfz_source_note = 'GFZ German Research Centre for Geosciences (CC BY 4.0)'

        swpc_planetary, swpc_high_blocks_recent = self.fetch_swpc_planetary_k_latest()
        swpc_hemi = self.fetch_swpc_hemi_power()
        swpc_kp_latest = None
        swpc_est_kp = None
        swpc_planetary_line = None
        swpc_source_note = 'NOAA SWPC (public domain)'
        hemi_total = None
        if isinstance(swpc_planetary, dict):
            swpc_kp_latest = swpc_planetary.get('kp_index')
            swpc_est_kp = swpc_planetary.get('estimated_kp')
            ts = swpc_planetary.get('timestamp')
            parts: List[str] = []
            if isinstance(swpc_kp_latest, (int, float)):
                parts.append(f"Planetary Kp {float(swpc_kp_latest):.2f}")
            if isinstance(swpc_est_kp, (int, float)) and swpc_est_kp != swpc_kp_latest:
                parts.append(f"Estimated {float(swpc_est_kp):.2f}")
            if isinstance(ts, datetime):
                label = " | ".join(parts) if parts else "Planetary Kp"
                swpc_planetary_line = f"{label} at <t:{int(ts.timestamp())}:t>"
            elif parts:
                swpc_planetary_line = " | ".join(parts)
        if isinstance(swpc_hemi, dict):
            hemi_total = swpc_hemi.get('total_gw') or swpc_hemi.get('north_gw')

        # Helper for nested numeric search (re-used within function scope)
        def _find_num_nested(obj, keys_lower):
            try:
                if isinstance(obj, dict):
                    for k, v in obj.items():
                        if isinstance(k, str) and k.lower() in keys_lower and isinstance(v, (int, float)):
                            return float(v)
                        res = _find_num_nested(v, keys_lower)
                        if res is not None:
                            return res
                elif isinstance(obj, list):
                    for it in obj:
                        res = _find_num_nested(it, keys_lower)
                        if res is not None:
                            return res
            except Exception:
                return None
            return None

        maf_kp = None
        maf_prob = None
        if isinstance(maf, dict):
            val = _find_num_nested(maf, ['currentkp', 'kp', 'kp_index', 'kp current', 'kp_current'])
            if isinstance(val, (int, float)):
                maf_kp = float(val)
            pval = _find_num_nested(maf, ['chance', 'probability', 'visibility', 'aurora_probability'])
            if isinstance(pval, (int, float)):
                maf_prob = int(round(float(pval)))

        cloud_now = None
        if clouds:
            hour_dt = now_utc.replace(minute=0, second=0, microsecond=0)
            cloud_now = clouds.get(hour_dt)
            if cloud_now is None:
                cloud_now = clouds.get(hour_dt - timedelta(hours=1))
            if isinstance(cloud_now, float):
                cloud_now = int(round(cloud_now))

        sky_darkness = None
        cloud_tonight = None
        if snapshot and isinstance(snapshot, dict):
            try:
                cond = snapshot.get('conditions', {}) or {}
                sky_darkness = cond.get('skyDarkness')
                ct = cond.get('cloudCover')
                if isinstance(ct, (int, float)):
                    cloud_tonight = int(round(float(ct)))
            except Exception:
                pass

        kp_for_calc = maf_kp if isinstance(maf_kp, (int, float)) else max(self.kp_threshold, 5.0)

        points = []
        for delta_min in range(0, minutes + 1, step):
            ts = int((now_utc + timedelta(minutes=delta_min)).timestamp())
            prob = self.visibility_percent(
                kp=kp_for_calc,
                cloud_avg=cloud_now,
                ovation_prob=ovation_prob,
                sky_darkness=sky_darkness,
                maf_prob=maf_prob,
                gfz_kp=gfz_latest.value if gfz_latest else None,
                swpc_kp=swpc_kp_latest if isinstance(swpc_kp_latest, (int, float)) else swpc_est_kp,
                hemi_power=hemi_total,
            )
            points.append({'ts': ts, 'prob': prob})

        return {
            'points': points,
            'ovation_prob': ovation_prob,
            'maf_kp': maf_kp,
            'maf_prob': maf_prob,
            'cloud_now': cloud_now,
            'cloud_tonight': cloud_tonight,
            'gfz_latest_kp': gfz_latest.value if gfz_latest else None,
            'gfz_latest_status': gfz_latest_status,
            'gfz_source_note': gfz_source_note,
            'swpc_planetary_line': swpc_planetary_line,
            'swpc_kp_latest': swpc_kp_latest,
            'swpc_estimated_kp': swpc_est_kp,
            'swpc_hemi_power': swpc_hemi,
            'swpc_source_note': swpc_source_note,
        }

    def build_alert(self, forecast_text: str, debug: bool = False) -> Optional[AlertBuild]:
        # Extract Kp table section
        kp_section_pattern = re.compile(
            r'NOAA Kp index breakdown[\s\S]*?(?=Rationale:|B\. NOAA|C\. NOAA|$)',
            re.DOTALL
        )
        kp_section_match = kp_section_pattern.search(forecast_text)
        if not kp_section_match:
            return None
        kp_section = kp_section_match.group()

        # Day headers
        months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
        months_pattern = "|".join(months)
        header_match = re.search(
            rf'^\s*((?:{months_pattern})\s+\d{{1,2}})\s+((?:{months_pattern})\s+\d{{1,2}})\s+((?:{months_pattern})\s+\d{{1,2}})\s*$',
            kp_section,
            flags=re.M
        )
        if header_match:
            days = [header_match.group(1), header_match.group(2), header_match.group(3)]
        else:
            days = []
            for line in kp_section.splitlines():
                found = re.findall(rf'(?:{months_pattern})\s+\d{{1,2}}', line)
                if len(found) >= 3:
                    days = found[:3]
                    break
            if not days:
                return None
        days = [d.strip() for d in days][:3]

        issued_year_match = re.search(r':Issued:\s+(\d{4})', forecast_text)
        try:
            issued_year = int(issued_year_match.group(1)) if issued_year_match else datetime.now(timezone.utc).year
        except Exception:
            issued_year = datetime.now(timezone.utc).year
        day_dates = [datetime.strptime(f"{issued_year} {d}", "%Y %b %d").date() for d in days]

        time_rows = re.findall(r'^\s*(\d{2}-\d{2})UT\s+([^\n]+)$', kp_section, flags=re.M)
        if len(time_rows) < 8:
            return None

        # Build full localized forecast lines (all values regardless of threshold)
        all_forecast_lines: List[str] = []
        if days:
            # Midnight UTC timestamps for Discord localized date display (<t:ts:D>)
            midnight_ts = []
            for d in day_dates:
                dt_mid = datetime(d.year, d.month, d.day, 0, 0, tzinfo=timezone.utc)
                midnight_ts.append(int(dt_mid.timestamp()))
            header = f"Dates: <t:{midnight_ts[0]}:D> | <t:{midnight_ts[1]}:D> | <t:{midnight_ts[2]}:D>"
            all_forecast_lines.append(header)
        for interval, rest in time_rows:
            rest_clean_full = re.sub(r'\s*\(G\d+\)', '', rest)
            vals_full = re.findall(r'(\d+(?:\.\d+)?)', rest_clean_full)
            if len(vals_full) < 3:
                continue
            start_hour = int(interval.split('-')[0])
            # Build per-day timestamp labels for start of block
            ts_labels: List[str] = []
            for d in day_dates:
                block_start = datetime(d.year, d.month, d.day, start_hour, 0, tzinfo=timezone.utc)
                ts_labels.append(f"<t:{int(block_start.timestamp())}:t>")
            formatted_vals = []
            for i in range(3):
                raw = vals_full[i]
                try:
                    v = float(raw)
                    val_disp = f"{v:.2f}"
                    if v >= self.kp_threshold:
                        val_disp = f"**{val_disp}**"
                    formatted_vals.append(val_disp)
                except Exception:
                    formatted_vals.append(raw)
            # Compose line with localized time for each day's block start + value
            # Example: 00-03UT: <t:...:t> 8.00 | <t:...:t> 4.67 | <t:...:t> 2.33
            line = (
                f"{interval}UT: "
                f"{ts_labels[0]} {formatted_vals[0]} | "
                f"{ts_labels[1]} {formatted_vals[1]} | "
                f"{ts_labels[2]} {formatted_vals[2]}"
            )
            all_forecast_lines.append(line)

        above_info: List[Tuple[str, date, str, float]] = []
        for interval, rest in time_rows:
            rest_clean = re.sub(r'\s*\(G\d+\)', '', rest)
            values = re.findall(r'(\d+(?:\.\d+)?)', rest_clean)
            if len(values) < 3:
                continue
            for col_idx, day_label in enumerate(days):
                try:
                    kp = float(values[col_idx])
                except ValueError:
                    continue
                if kp >= self.kp_threshold:
                    above_info.append((day_label, day_dates[col_idx], f"{interval}UT", kp))

        now_utc = datetime.now(timezone.utc)
        gfz_records, gfz_meta = self.fetch_gfz_series(now_utc - timedelta(hours=36), now_utc + timedelta(hours=3))
        gfz_summary_lines: List[str] = []
        gfz_latest_line: Optional[str] = None
        gfz_source_note: Optional[str] = None
        gfz_latest_value: Optional[float] = None
        if gfz_records:
            gfz_latest = gfz_records[-1]
            gfz_latest_value = gfz_latest.value
            latest_ts = int(gfz_latest.timestamp.timestamp())
            gfz_latest_line = (
                f"Latest GFZ Kp: {gfz_latest.value:.2f} at <t:{latest_ts}:t> ({self._gfz_status_label(gfz_latest.status)})"
            )
            recent = gfz_records[-12:]  # show a broader recent window for filtering
            for rec in recent:
                if rec.value < self.kp_threshold:
                    continue  # suppress below-threshold noise
                ts = int(rec.timestamp.timestamp())
                gfz_summary_lines.append(
                    f"• <t:{ts}:t> • Kp {rec.value:.2f} ({self._gfz_status_label(rec.status)})"
                )
            # If nothing met threshold, keep at least one contextual line (last record before latest)
            if not gfz_summary_lines:
                fallback_slice = [r for r in recent[-4:]]  # last few for context
                for rec in fallback_slice:
                    ts = int(rec.timestamp.timestamp())
                    gfz_summary_lines.append(
                        f"• <t:{ts}:t> • Kp {rec.value:.2f} ({self._gfz_status_label(rec.status)})"
                    )
        if isinstance(gfz_meta, dict):
            source = gfz_meta.get('source')
            license_info = gfz_meta.get('license')
            if source and license_info:
                gfz_source_note = f"{source} ({license_info})"
            elif source:
                gfz_source_note = str(source)
            elif license_info:
                gfz_source_note = str(license_info)
        if not gfz_source_note:
            gfz_source_note = 'GFZ German Research Centre for Geosciences (CC BY 4.0)'

        swpc_planetary, swpc_high_blocks_recent = self.fetch_swpc_planetary_k_latest()
        swpc_hemi = self.fetch_swpc_hemi_power()
        swpc_planetary_line = None
        swpc_summary_lines: List[str] = []
        swpc_kp_latest = None
        swpc_est_kp = None
        swpc_effective_kp = None
        hemi_total = None
        swpc_source_note = 'NOAA SWPC (public domain)'
        if isinstance(swpc_planetary, dict):
            swpc_kp_latest = swpc_planetary.get('kp_index')
            swpc_est_kp = swpc_planetary.get('estimated_kp')
            ts = swpc_planetary.get('timestamp')
            parts: List[str] = []
            if isinstance(swpc_kp_latest, (int, float)):
                swpc_effective_kp = float(swpc_kp_latest)
                parts.append(f"Planetary Kp {float(swpc_kp_latest):.2f}")
            if isinstance(swpc_est_kp, (int, float)) and swpc_est_kp != swpc_kp_latest:
                if swpc_effective_kp is None:
                    swpc_effective_kp = float(swpc_est_kp)
                parts.append(f"Estimated {float(swpc_est_kp):.2f}")
            if isinstance(ts, datetime):
                ts_int = int(ts.timestamp())
                label = " | ".join(parts) if parts else "Planetary Kp"
                swpc_planetary_line = f"{label} at <t:{ts_int}:t>"
            elif parts:
                swpc_planetary_line = " | ".join(parts)
        if isinstance(swpc_hemi, dict):
            hemi_total = swpc_hemi.get('total_gw') or swpc_hemi.get('north_gw')
            ts = swpc_hemi.get('timestamp')
            if isinstance(hemi_total, (int, float)):
                hemi_line = f"Hemispheric power {float(hemi_total):.1f} GW"
                if isinstance(ts, datetime):
                    hemi_line += f" at <t:{int(ts.timestamp())}:t>"
                swpc_summary_lines.append(hemi_line)
            north = swpc_hemi.get('north_gw')
            south = swpc_hemi.get('south_gw')
            if isinstance(north, (int, float)) and isinstance(south, (int, float)):
                swpc_summary_lines.append(f"North {north:.1f} GW • South {south:.1f} GW")
        # Keep planetary line separate; avoid inserting into summary to prevent duplicate display
        # Summary lines will only contain hemispheric power and related details.

        # NOTE: We no longer use forecast-derived detections for alert triggering to avoid false positives.
        # Even if no above-threshold forecast windows exist, we still proceed to collect real-time high blocks below.
        if not above_info:
            above_info = []  # keep empty; embed will show placeholder later

        # Enrich
        cloud_map = self.fetch_cloud_cover()
        cloud_available = bool(cloud_map)
        snapshot = self.fetch_aurora_snapshot(self.latitude, self.longitude)
        sky_darkness = None
        if snapshot:
            try:
                sky_darkness = (snapshot.get('conditions', {}) or {}).get('skyDarkness')
            except Exception:
                sky_darkness = None
        # NOAA Ovation nowcast probability at location
        ovation_prob = self.fetch_ovation_probability(self.latitude, self.longitude)
        # My Aurora Forecast data
        maf = self.fetch_maf_data(self.latitude, self.longitude, self.timezone_name)
        maf_prob: Optional[int] = None
        def find_num_nested(obj, keys: List[str]) -> Optional[float]:
            try:
                if isinstance(obj, dict):
                    for k, v in obj.items():
                        if isinstance(k, str) and k.lower() in keys:
                            if isinstance(v, (int, float)):
                                return float(v)
                        res = find_num_nested(v, keys)
                        if res is not None:
                            return res
                elif isinstance(obj, list):
                    for it in obj:
                        res = find_num_nested(it, keys)
                        if res is not None:
                            return res
            except Exception:
                return None
            return None
        if maf and isinstance(maf, dict):
            # try several common probability keys anywhere in the payload
            res = find_num_nested(maf, ['chance','probability','visibility','aurora_probability'])
            if isinstance(res, (int, float)):
                maf_prob = int(round(res))

        # Sort and compute per detection structures
        try:
            above_info.sort(key=lambda x: (x[1], int(x[2].split('-')[0])))
        except Exception:
            pass

        detections: List[Detection] = []  # retained for embed display (forecast windows)
        utc = pytz.utc
        local_tz = pytz.timezone('America/Los_Angeles')
        for day_label, day_date, time_block, kp in above_info:
            start_hour = int(time_block.split('-')[0])
            end_hour = int(time_block.split('-')[1][:2])
            start_time_utc = utc.localize(datetime(day_date.year, day_date.month, day_date.day, start_hour, 0))
            end_time_utc = utc.localize(datetime(day_date.year, day_date.month, day_date.day, end_hour, 0))
            if end_hour <= start_hour:
                end_time_utc = end_time_utc + timedelta(days=1)
            start_ts = int(start_time_utc.timestamp())
            end_ts = int(end_time_utc.timestamp())
            # cloud avg
            cloud_avg_display = "N/A"
            vis_pct = 0
            if cloud_available:
                # Collect any cloud entries that fall within the window (works for 1h and 3h steps)
                values = []
                try:
                    for t, v in cloud_map.items():
                        if v is None:
                            continue
                        if start_time_utc <= t < end_time_utc:
                            values.append(int(v))
                except Exception:
                    values = []
                if values:
                    avg = sum(values) / float(len(values))
                    cloud_avg_display = f"{avg:.0f}%"
                    vis_pct = self.visibility_percent(kp, avg, ovation_prob=ovation_prob, sky_darkness=sky_darkness, maf_prob=maf_prob, gfz_kp=gfz_latest_value, swpc_kp=swpc_effective_kp, hemi_power=hemi_total)
                else:
                    # No datapoints fell inside the window; try nearest neighbor at start or end within +/- 180 minutes
                    nearest_vals = []
                    for t, v in cloud_map.items():
                        if v is None:
                            continue
                        dtmin = abs((t - start_time_utc).total_seconds()) / 60.0
                        if dtmin <= 180:
                            nearest_vals.append(int(v))
                    if not nearest_vals:
                        for t, v in cloud_map.items():
                            if v is None:
                                continue
                            dtmin = abs((t - end_time_utc).total_seconds()) / 60.0
                            if dtmin <= 180:
                                nearest_vals.append(int(v))
                    if nearest_vals:
                        avg = sum(nearest_vals) / float(len(nearest_vals))
                        cloud_avg_display = f"{avg:.0f}%"
                        vis_pct = self.visibility_percent(kp, avg, ovation_prob=ovation_prob, sky_darkness=sky_darkness, maf_prob=maf_prob, gfz_kp=gfz_latest_value, swpc_kp=swpc_effective_kp, hemi_power=hemi_total)
                    else:
                        # As a last resort, try OpenWeather specifically for this window even if Open-Meteo had partial data
                        try:
                            ow_map = self.fetch_cloud_cover_openweather()
                        except Exception:
                            ow_map = {}
                        ow_vals = []
                        if ow_map:
                            for t, v in ow_map.items():
                                if v is None:
                                    continue
                                if start_time_utc <= t < end_time_utc:
                                    ow_vals.append(int(v))
                        if ow_vals:
                            avg = sum(ow_vals) / float(len(ow_vals))
                            cloud_avg_display = f"{avg:.0f}%"
                            vis_pct = self.visibility_percent(kp, avg, ovation_prob=ovation_prob, sky_darkness=sky_darkness, maf_prob=maf_prob, gfz_kp=gfz_latest_value, swpc_kp=swpc_effective_kp, hemi_power=hemi_total)
                        else:
                            vis_pct = self.visibility_percent(kp, None, ovation_prob=ovation_prob, sky_darkness=sky_darkness, maf_prob=maf_prob, gfz_kp=gfz_latest_value, swpc_kp=swpc_effective_kp, hemi_power=hemi_total)
            else:
                vis_pct = self.visibility_percent(kp, None, ovation_prob=ovation_prob, sky_darkness=sky_darkness, maf_prob=maf_prob, gfz_kp=gfz_latest_value, swpc_kp=swpc_effective_kp, hemi_power=hemi_total)
            local_date_label = f"<t:{start_ts}:D>"
            # Build bullet with UT block label and localized Discord timestamps for the time range
            ut_label = time_block
            try:
                if ut_label.endswith('UT') and not ut_label.endswith(' UT'):
                    ut_label = ut_label[:-2] + ' UT'
            except Exception:
                pass
            bullet = (
                f"• {ut_label} → <t:{start_ts}:t> - <t:{end_ts}:t> • "
                f"KP {kp:.2f} • ☁️ {cloud_avg_display} • 👀 {vis_pct}%"
            )
            detections.append(Detection(day_label, day_date, time_block, kp, start_ts, end_ts, cloud_avg_display, vis_pct, local_date_label, bullet))

        window_id = f"{day_dates[0].isoformat()}_to_{day_dates[-1].isoformat()}_kp>={self.kp_threshold}"
        # Build a concise My Aurora Forecast summary if available
        maf_summary = None
        if maf and isinstance(maf, dict):
            # attempt to find a few useful indicators across the whole payload
            def first_num_any(obj, keys: List[str]) -> Optional[float]:
                return find_num_nested(obj, [k.lower() for k in keys])
            maf_kp = first_num_any(maf, ['kp','kp_index','kp_current'])
            maf_cloud = first_num_any(maf, ['cloud_cover','clouds','cloud'])
            maf_prob_disp = first_num_any(maf, ['chance','probability','visibility','aurora_probability'])
            parts = []
            if isinstance(maf_kp, (int, float)):
                parts.append(f"KP {float(maf_kp):.2f}")
            if isinstance(maf_prob_disp, (int, float)):
                parts.append(f"chance {int(round(float(maf_prob_disp)))}%")
            if isinstance(maf_cloud, (int, float)):
                parts.append(f"☁️ {int(round(float(maf_cloud)))}%")
            if parts:
                maf_summary = "My Aurora Forecast: " + " • ".join(parts)
        # Prepare AFM section lines for structured embed
        afm_tonight_line = None
        afm_conditions_line = None
        afm_next_hours_lines: List[str] | None = None
        if snapshot:
            try:
                tonight = snapshot.get('tonight', {})
                cond = snapshot.get('conditions', {})
                ui = snapshot.get('ui', {})
                status_texts = (ui.get('statusTexts') or {}) if isinstance(ui, dict) else {}
                status_key = tonight.get('status', 'n/a')
                status_human = status_texts.get(status_key, str(status_key).replace('_', ' ').title())
                prob = tonight.get('probability', 'n/a')
                best = tonight.get('bestHour', 'n/a')
                updated_at = tonight.get('updatedAt') or snapshot.get('updatedAt')
                updated_line = ''
                if isinstance(updated_at, str):
                    try:
                        upd_dt = datetime.fromisoformat(updated_at.replace('Z', '+00:00'))
                        upd_ts = int(upd_dt.timestamp())
                        updated_line = f" • updated <t:{upd_ts}:R>"
                    except Exception:
                        pass
                kp_idx = cond.get('kpIndex', 'n/a')
                cc = cond.get('cloudCover', 'n/a')
                darkness = cond.get('skyDarkness', 'n/a')
                afm_tonight_line = f"Tonight: {status_human} • {prob}% • Best: {best}{updated_line}"
                afm_conditions_line = f"Conditions: KP {kp_idx} • ☁️ {cc}% • Sky: {darkness}"
                h12 = snapshot.get('h12', [])
                if h12:
                    afm_next_hours_lines = []
                    for item in h12[:3]:
                        iso = item.get('time')
                        ts_part = ''
                        if isinstance(iso, str):
                            try:
                                ts_dt = datetime.fromisoformat(iso.replace('Z', '+00:00'))
                                ts_part = f"<t:{int(ts_dt.timestamp())}:t>"
                            except Exception:
                                ts_part = ''
                        if not ts_part:
                            ts_part = item.get('displayTime12') or item.get('displayTime24') or '?'
                        kpv = item.get('kp', '?')
                        pbase = item.get('probBase', '?')
                        padj = item.get('probAdj', '?')
                        try:
                            kpv = f"{float(kpv):.2f}"
                        except Exception:
                            pass
                        try:
                            pbase = f"{float(pbase):.0f}"
                        except Exception:
                            pass
                        try:
                            padj = f"{float(padj):.1f}"
                        except Exception:
                            pass
                        afm_next_hours_lines.append(f"• {ts_part}: KP {kpv} • base {pbase}% • adj +{padj}%")
            except Exception:
                afm_tonight_line = "AFM snapshot: parse error"

        # Prepare aggregated sources line to merge core signals (GFZ, NOAA planetary, hemispheric power, Ovation, MAF)
        agg_parts: List[str] = []
        if isinstance(gfz_latest_value, (int, float)) and gfz_latest_line:
            status_short = 'Def' if 'Definitive' in gfz_latest_line else ('Prelim' if 'Preliminary' in gfz_latest_line else None)
            piece = f"GFZ {gfz_latest_value:.2f}"
            if status_short:
                piece += f" ({status_short})"
            agg_parts.append(piece)
        if isinstance(swpc_effective_kp, (int, float)):
            agg_parts.append(f"NOAA {swpc_effective_kp:.2f}")
        if isinstance(hemi_total, (int, float)):
            agg_parts.append(f"Hemi {hemi_total:.0f} GW")
        if isinstance(ovation_prob, int):
            agg_parts.append(f"Ovation {ovation_prob}%")
        if isinstance(maf_prob, int):
            agg_parts.append(f"MAF {maf_prob}%")
        aggregated_sources_line = "Sources: " + " • ".join(agg_parts) if agg_parts else None

        # Build flat message (legacy) and structured groups (now suppressing separate GFZ/NOAA blocks if aggregated line present)
        message = self._render_message(
            detections,
            snapshot,
            cloud_available,
            ovation_prob,
            maf_summary,
            gfz_latest_line=None if aggregated_sources_line else gfz_latest_line,
            gfz_summary_lines=[] if aggregated_sources_line else gfz_summary_lines,
            gfz_source_note=None if aggregated_sources_line else gfz_source_note,
            swpc_planetary_line=None if aggregated_sources_line else swpc_planetary_line,
            swpc_summary_lines=[] if aggregated_sources_line else swpc_summary_lines,
            swpc_source_note=None if aggregated_sources_line else swpc_source_note,
            aggregated_sources_line=aggregated_sources_line,
        )
        # Detection groups by date for embed fields
        detection_groups: Dict[str, List[str]] = {}
        for d in detections:
            detection_groups.setdefault(d.local_date_label, []).append(d.bullet)
        # Recommendations
        recommendation_lines: List[str] = []
        upcoming_days_lines: List[str] = []

        # Helper to combine percents with weights
        def _combine_percents(pairs: List[Tuple[Optional[int], float]]) -> Optional[int]:
            total = 0.0
            wsum = 0.0
            for val, w in pairs:
                if isinstance(val, int):
                    total += max(0, min(100, val)) * w
                    wsum += w
            if wsum <= 0:
                return None
            return int(round(total / wsum))

        # Tonight best window (next ~18h)
        now_ts = int(datetime.now(timezone.utc).timestamp())
        horizon_ts = now_ts + 18 * 3600
        next_windows = [x for x in detections if x.start_ts >= now_ts and x.start_ts <= horizon_ts]
        best_next = max(next_windows, key=lambda x: x.visibility_pct) if next_windows else None

        # AFM tonight probability (if provided)
        afm_prob = None
        try:
            if snapshot and isinstance(snapshot, dict):
                tprob = (snapshot.get('tonight') or {}).get('probability')
                if isinstance(tprob, (int, float)):
                    afm_prob = int(round(float(tprob)))
        except Exception:
            afm_prob = None

        if best_next:
            combined = _combine_percents([
                (best_next.visibility_pct, 0.5),
                (afm_prob, 0.25),
                (ovation_prob if isinstance(ovation_prob, int) else None, 0.15),
                (maf_prob if isinstance(maf_prob, int) else None, 0.10),
            ])
            if combined is None:
                combined = best_next.visibility_pct
            label = "Unlikely"
            if combined >= 60:
                label = "Good opportunity"
            elif combined >= 30:
                label = "Maybe"
            extras: List[str] = []
            try:
                best_hour = ((snapshot or {}).get('tonight') or {}).get('bestHour')
                if isinstance(best_hour, str) and best_hour:
                    extras.append(f"AFM best: {best_hour}")
            except Exception:
                pass
            if isinstance(gfz_latest_value, (int, float)):
                extras.append(f"GFZ Kp {float(gfz_latest_value):.1f}")
            if isinstance(ovation_prob, int):
                extras.append(f"Ovation {ovation_prob}%")
            if isinstance(maf_prob, int):
                extras.append(f"MAF {maf_prob}%")
            rng = f"<t:{best_next.start_ts}:t>-<t:{best_next.end_ts}:t>"
            line = (
                f"Tonight: {label} ({combined}%). Best window {best_next.ut_block} ({rng}) • "
                f"KP {best_next.kp:.2f} • ☁️ {best_next.cloud_avg_display}"
            )
            if extras:
                line += " • " + " • ".join(extras)
            recommendation_lines.append(line)
        else:
            combined = _combine_percents([
                (afm_prob, 0.6),
                (ovation_prob if isinstance(ovation_prob, int) else None, 0.4),
            ])
            if combined is not None:
                label = "Unlikely"
                if combined >= 60:
                    label = "Good opportunity"
                elif combined >= 30:
                    label = "Maybe"
                recommendation_lines.append(
                    f"Tonight: {label} ({combined}%). No high Kp windows detected in the immediate horizon."
                )
            else:
                recommendation_lines.append("Tonight: Insufficient data for a recommendation.")

        # Upcoming days best windows
        by_date: Dict[date, List[Detection]] = {}
        for det in detections:
            by_date.setdefault(det.day_date, []).append(det)
        for ddate in sorted(by_date.keys()):
            best = max(by_date[ddate], key=lambda x: x.visibility_pct)
            ds = ddate.strftime("%b %d")
            rng = f"<t:{best.start_ts}:t>-<t:{best.end_ts}:t>"
            # Derive a simple viewline hint from latitude factor at this Kp
            try:
                lat_factor = float(self._latitude_factor(best.kp))
            except Exception:
                lat_factor = 1.0
            if lat_factor >= 0.8:
                vl_label = "VL: favorable"
            elif lat_factor >= 0.5:
                vl_label = "VL: near edge"
            else:
                vl_label = "VL: equatorward"
            extras: List[str] = [vl_label]
            if isinstance(ovation_prob, int):
                extras.append(f"Ovation {ovation_prob}%")
            if isinstance(hemi_total, (int, float)):
                extras.append(f"Hemi {float(hemi_total):.0f} GW")
            if isinstance(gfz_latest_value, (int, float)):
                extras.append(f"GFZ {float(gfz_latest_value):.1f}")
            try:
                darkness = None
                if snapshot and isinstance(snapshot, dict):
                    darkness = ((snapshot.get('conditions') or {}) or {}).get('skyDarkness')
                if isinstance(darkness, str) and darkness:
                    extras.append(f"Sky {darkness}")
            except Exception:
                pass
            line = f"{ds}: {rng} • 👀 {best.visibility_pct}% • KP {best.kp:.2f} • ☁️ {best.cloud_avg_display}"
            if extras:
                line += " • " + " • ".join(extras)
            upcoming_days_lines.append(line)
        # Build real-time high Kp block structures (GFZ 3h blocks and latest SWPC planetary) for alert triggering
        gfz_high_blocks: List[dict] = []  # retained for compatibility; NOAA SWPC is now sole trigger source
        swpc_high_block: Optional[dict] = None
        swpc_high_blocks = swpc_high_blocks_recent or []
        if swpc_high_blocks:
            swpc_high_block = swpc_high_blocks[-1]

        return AlertBuild(
            message=message,
            detections=detections,
            window_id=window_id,
            cloud_available=cloud_available,
            afm_snapshot=snapshot,
            tonight_image_url=self._tonight_url(),
            tomorrow_image_url=self._tomorrow_url(),
            ovation_prob=ovation_prob,
            maf_summary=maf_summary,
            afm_tonight_line=afm_tonight_line,
            afm_conditions_line=afm_conditions_line,
            afm_next_hours_lines=afm_next_hours_lines,
            detection_groups=detection_groups,
            recommendation_lines=recommendation_lines,
            upcoming_days_lines=upcoming_days_lines,
            gfz_summary_lines=gfz_summary_lines,
            gfz_latest_line=gfz_latest_line,
            gfz_source_note=gfz_source_note,
            swpc_planetary_line=swpc_planetary_line,
            swpc_summary_lines=swpc_summary_lines,
            swpc_source_note=swpc_source_note,
            aggregated_sources_line=aggregated_sources_line,
            all_forecast_lines=all_forecast_lines,
            gfz_high_blocks=gfz_high_blocks,
            swpc_high_blocks=swpc_high_blocks,
            swpc_high_block=swpc_high_block,
        )

    def _tonight_url(self) -> str:
        return "https://services.swpc.noaa.gov/experimental/images/aurora_dashboard/tonights_static_viewline_forecast.png"

    def _tomorrow_url(self) -> str:
        return "https://services.swpc.noaa.gov/experimental/images/aurora_dashboard/tomorrow_nights_static_viewline_forecast.png"

    def _render_message(
        self,
        detections: List[Detection],
        snapshot: Optional[dict],
        cloud_available: bool,
        ovation_prob: Optional[int],
        maf_summary: Optional[str] = None,
        gfz_latest_line: Optional[str] = None,
        gfz_summary_lines: Optional[List[str]] = None,
        gfz_source_note: Optional[str] = None,
        swpc_planetary_line: Optional[str] = None,
        swpc_summary_lines: Optional[List[str]] = None,
        swpc_source_note: Optional[str] = None,
        aggregated_sources_line: Optional[str] = None,
    ) -> str:
        detected_ts = int(datetime.now(timezone.utc).timestamp())
        msg = "🌌 **AURORA UPDATE**\n\n"
        msg += f"Detected: <t:{detected_ts}:F> (→ <t:{detected_ts}:R>)\n"
        msg += f"Location: {self.location_name} ({self.latitude:.4f}, {self.longitude:.4f})\n"
        msg += "[Aurora Dashboard](https://www.swpc.noaa.gov/communities/aurora-dashboard-experimental)\n"
        # Condensed merged sources line replaces individual GFZ/NOAA sections when provided
        if aggregated_sources_line:
            msg += aggregated_sources_line + "\n"
        else:
            if gfz_latest_line:
                msg += gfz_latest_line + "\n"
            if gfz_summary_lines:
                msg += f"\n**GFZ Potsdam (recent Kp ≥ {self.kp_threshold})**\n"
                for line in gfz_summary_lines:
                    msg += line + "\n"
            if gfz_source_note:
                msg += gfz_source_note + "\n"
            if swpc_planetary_line or swpc_summary_lines:
                msg += "\n**NOAA SWPC (real-time)**\n"
                if swpc_planetary_line and (not swpc_summary_lines or (swpc_summary_lines and swpc_summary_lines[0] != swpc_planetary_line)):
                    msg += swpc_planetary_line + "\n"
                if swpc_summary_lines:
                    for line in swpc_summary_lines:
                        if swpc_planetary_line and line == swpc_planetary_line:
                            continue
                        msg += line + "\n"
            if swpc_source_note:
                msg += swpc_source_note + "\n"
        if cloud_available:
            msg += f"☁️ Cloud data: retrieved for {self.location_name}\n"
        else:
            msg += f"☁️ Cloud data: unavailable for {self.location_name}\n"
        if ovation_prob is not None:
            msg += f"🌐 NOAA Ovation (now): {ovation_prob}% at your location\n"
        if maf_summary:
            msg += f"📱 {maf_summary}\n"

        if snapshot:
            try:
                tonight = snapshot.get('tonight', {})
                cond = snapshot.get('conditions', {})
                ui = snapshot.get('ui', {})
                status_texts = (ui.get('statusTexts') or {}) if isinstance(ui, dict) else {}
                status_key = tonight.get('status', 'n/a')
                status_human = status_texts.get(status_key, str(status_key).replace('_', ' ').title())
                prob = tonight.get('probability', 'n/a')
                best = tonight.get('bestHour', 'n/a')
                updated_at = tonight.get('updatedAt') or snapshot.get('updatedAt')
                updated_line = ''
                if isinstance(updated_at, str):
                    try:
                        upd_dt = datetime.fromisoformat(updated_at.replace('Z', '+00:00'))
                        upd_ts = int(upd_dt.timestamp())
                        updated_line = f" • updated <t:{upd_ts}:R>"
                    except Exception:
                        pass
                kp_idx = cond.get('kpIndex', 'n/a')
                cc = cond.get('cloudCover', 'n/a')
                darkness = cond.get('skyDarkness', 'n/a')
                msg += "\n**AuroraForecast.me**\n"
                msg += f"Tonight: {status_human} • {prob}% • Best: {best}{updated_line}\n"
                msg += f"Conditions: KP {kp_idx} • ☁️ {cc}% • Sky: {darkness}\n"
                h12 = snapshot.get('h12', [])
                if h12:
                    msg += "Next hours:\n"
                    for item in h12[:3]:
                        iso = item.get('time')
                        ts_part = ''
                        if isinstance(iso, str):
                            try:
                                ts_dt = datetime.fromisoformat(iso.replace('Z', '+00:00'))
                                ts_part = f"<t:{int(ts_dt.timestamp())}:t>"
                            except Exception:
                                ts_part = ''
                        if not ts_part:
                            ts_part = item.get('displayTime12') or item.get('displayTime24') or '?'
                        kpv = item.get('kp', '?')
                        pbase = item.get('probBase', '?')
                        padj = item.get('probAdj', '?')
                        try:
                            kpv = f"{float(kpv):.2f}"
                        except Exception:
                            pass
                        try:
                            pbase = f"{float(pbase):.0f}"
                        except Exception:
                            pass
                        try:
                            padj = f"{float(padj):.1f}"
                        except Exception:
                            pass
                        msg += f"  • {ts_part}: KP {kpv} • base {pbase}% • adj +{padj}%\n"
            except Exception:
                msg += "AFM snapshot: parse error\n"

        if detections:
            grouped: Dict[str, List[str]] = {}
            for d in detections:
                grouped.setdefault(d.local_date_label, []).append(d.bullet)
            for date_label in sorted(grouped.keys()):
                msg += f"**{date_label}**\n"
                for bullet in grouped[date_label]:
                    msg += bullet + "\n"
        else:
            msg += "\nNo high Kp windows currently at or above your threshold.\n"
        return msg
