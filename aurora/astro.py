from __future__ import annotations

import math
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple

RAD = math.pi / 180.0
DEG = 180.0 / math.pi

# Sun altitude thresholds, in degrees, for how dark the sky is.
CIVIL = -6.0
NAUTICAL = -12.0
ASTRONOMICAL = -18.0
# Aurora at low elevation is washed out until roughly nautical dark.
DARK_ENOUGH = NAUTICAL


def _days_since_2000(dt: datetime) -> float:
    """Schlyter's day number: days since 2000 Jan 0.0 UT."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    dt = dt.astimezone(timezone.utc)
    y, m, d = dt.year, dt.month, dt.day
    hours = dt.hour + dt.minute / 60.0 + dt.second / 3600.0
    day = (
        367 * y
        - 7 * (y + (m + 9) // 12) // 4
        + 275 * m // 9
        + d
        - 730530
    )
    return day + hours / 24.0


def _rev(angle: float) -> float:
    return angle - 360.0 * math.floor(angle / 360.0)


def _sun_ecliptic(d: float) -> Tuple[float, float, float]:
    """Return (true longitude deg, distance AU, mean longitude deg)."""
    w = 282.9404 + 4.70935e-5 * d
    e = 0.016709 - 1.151e-9 * d
    M = _rev(356.0470 + 0.9856002585 * d)
    E = M + DEG * e * math.sin(M * RAD) * (1.0 + e * math.cos(M * RAD))
    x = math.cos(E * RAD) - e
    y = math.sin(E * RAD) * math.sqrt(1.0 - e * e)
    r = math.hypot(x, y)
    v = math.atan2(y, x) * DEG
    return _rev(v + w), r, _rev(w + M)


def _obliquity(d: float) -> float:
    return 23.4393 - 3.563e-7 * d


def _ecliptic_to_equatorial(lon: float, lat: float, r: float, obl: float) -> Tuple[float, float]:
    """Return (right ascension deg, declination deg)."""
    xe = r * math.cos(lat * RAD) * math.cos(lon * RAD)
    ye = r * math.cos(lat * RAD) * math.sin(lon * RAD)
    ze = r * math.sin(lat * RAD)
    xq = xe
    yq = ye * math.cos(obl * RAD) - ze * math.sin(obl * RAD)
    zq = ye * math.sin(obl * RAD) + ze * math.cos(obl * RAD)
    ra = _rev(math.atan2(yq, xq) * DEG)
    dec = math.atan2(zq, math.hypot(xq, yq)) * DEG
    return ra, dec


def _local_sidereal_deg(d: float, dt: datetime, lon_east: float) -> float:
    _lon, _r, mean_lon = _sun_ecliptic(d)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    dt = dt.astimezone(timezone.utc)
    ut_hours = dt.hour + dt.minute / 60.0 + dt.second / 3600.0
    gmst0_hours = (mean_lon + 180.0) / 15.0
    return _rev((gmst0_hours + ut_hours + lon_east / 15.0) * 15.0)


def _altitude(ra_deg: float, dec_deg: float, lst_deg: float, lat: float) -> float:
    ha = _rev(lst_deg - ra_deg) * RAD
    sin_alt = (
        math.sin(lat * RAD) * math.sin(dec_deg * RAD)
        + math.cos(lat * RAD) * math.cos(dec_deg * RAD) * math.cos(ha)
    )
    return math.asin(max(-1.0, min(1.0, sin_alt))) * DEG


def sun_altitude(dt: datetime, lat: float, lon: float) -> float:
    """Sun altitude in degrees above the horizon. Positive means daylight."""
    d = _days_since_2000(dt)
    slon, r, _mean = _sun_ecliptic(d)
    ra, dec = _ecliptic_to_equatorial(slon, 0.0, r, _obliquity(d))
    return _altitude(ra, dec, _local_sidereal_deg(d, dt, lon), lat)


def _moon_ecliptic(d: float) -> Tuple[float, float, float]:
    """Return (longitude deg, latitude deg, distance in earth radii)."""
    N = _rev(125.1228 - 0.0529538083 * d)
    i = 5.1454
    w = _rev(318.0634 + 0.1643573223 * d)
    a = 60.2666
    e = 0.054900
    M = _rev(115.3654 + 13.0649929509 * d)

    E = M + DEG * e * math.sin(M * RAD) * (1.0 + e * math.cos(M * RAD))
    for _ in range(6):
        delta = (E - DEG * e * math.sin(E * RAD) - M) / (1.0 - e * math.cos(E * RAD))
        E -= delta
        if abs(delta) < 1e-8:
            break

    x = a * (math.cos(E * RAD) - e)
    y = a * math.sqrt(1.0 - e * e) * math.sin(E * RAD)
    r = math.hypot(x, y)
    v = _rev(math.atan2(y, x) * DEG)

    xec = r * (
        math.cos(N * RAD) * math.cos((v + w) * RAD)
        - math.sin(N * RAD) * math.sin((v + w) * RAD) * math.cos(i * RAD)
    )
    yec = r * (
        math.sin(N * RAD) * math.cos((v + w) * RAD)
        + math.cos(N * RAD) * math.sin((v + w) * RAD) * math.cos(i * RAD)
    )
    zec = r * math.sin((v + w) * RAD) * math.sin(i * RAD)

    lon = _rev(math.atan2(yec, xec) * DEG)
    lat = math.atan2(zec, math.hypot(xec, yec)) * DEG

    # Largest periodic perturbations (Schlyter). Without these the phase can be
    # off by more than a degree, which shifts the illuminated fraction.
    _slon, _sr, sun_mean_lon = _sun_ecliptic(d)
    sun_M = _rev(356.0470 + 0.9856002585 * d)
    Ls = sun_mean_lon
    Lm = _rev(N + w + M)
    D = _rev(Lm - Ls)
    F = _rev(Lm - N)

    lon += (
        -1.274 * math.sin((M - 2 * D) * RAD)
        + 0.658 * math.sin(2 * D * RAD)
        - 0.186 * math.sin(sun_M * RAD)
        - 0.059 * math.sin((2 * M - 2 * D) * RAD)
        - 0.057 * math.sin((M - 2 * D + sun_M) * RAD)
        + 0.053 * math.sin((M + 2 * D) * RAD)
        + 0.046 * math.sin((2 * D - sun_M) * RAD)
        + 0.041 * math.sin((M - sun_M) * RAD)
        - 0.035 * math.sin(D * RAD)
        - 0.031 * math.sin((M + sun_M) * RAD)
        - 0.015 * math.sin((2 * F - 2 * D) * RAD)
        + 0.011 * math.sin((M - 4 * D) * RAD)
    )
    lat += (
        -0.173 * math.sin((F - 2 * D) * RAD)
        - 0.055 * math.sin((M - F - 2 * D) * RAD)
        - 0.046 * math.sin((M + F - 2 * D) * RAD)
        + 0.033 * math.sin((F + 2 * D) * RAD)
        + 0.017 * math.sin((2 * M + F) * RAD)
    )
    r += -0.58 * math.cos((M - 2 * D) * RAD) - 0.46 * math.cos(2 * D * RAD)
    return _rev(lon), lat, r


def moon_altitude(dt: datetime, lat: float, lon: float) -> float:
    """Moon altitude in degrees, corrected for parallax."""
    d = _days_since_2000(dt)
    mlon, mlat, r = _moon_ecliptic(d)
    ra, dec = _ecliptic_to_equatorial(mlon, mlat, r, _obliquity(d))
    alt = _altitude(ra, dec, _local_sidereal_deg(d, dt, lon), lat)
    # The moon is close enough that topocentric parallax matters.
    parallax = math.asin(1.0 / max(r, 1e-6)) * DEG
    return alt - parallax * math.cos(alt * RAD)


def moon_illumination(dt: datetime) -> float:
    """Illuminated fraction of the moon's disc, 0.0 new to 1.0 full."""
    d = _days_since_2000(dt)
    mlon, mlat, _r = _moon_ecliptic(d)
    slon, _sr, _sm = _sun_ecliptic(d)
    elongation = math.acos(
        max(-1.0, min(1.0, math.cos((mlon - slon) * RAD) * math.cos(mlat * RAD)))
    ) * DEG
    phase_angle = 180.0 - elongation
    return (1.0 + math.cos(phase_angle * RAD)) / 2.0


def moon_phase_name(illum: float, dt: Optional[datetime] = None) -> str:
    if illum < 0.04:
        return "new moon"
    if illum > 0.96:
        return "full moon"
    if illum < 0.35:
        return "crescent moon"
    if illum < 0.65:
        return "half moon"
    return "gibbous moon"


def darkness_label(sun_alt: float) -> str:
    if sun_alt > 0:
        return "daylight"
    if sun_alt > CIVIL:
        return "civil twilight"
    if sun_alt > NAUTICAL:
        return "nautical twilight"
    if sun_alt > ASTRONOMICAL:
        return "astronomical twilight"
    return "night"


def darkness_factor(sun_alt: float) -> float:
    """0.0 in daylight, 1.0 in full night, ramped through twilight."""
    if sun_alt >= 0:
        return 0.0
    if sun_alt <= ASTRONOMICAL:
        return 1.0
    return min(1.0, max(0.0, -sun_alt / abs(ASTRONOMICAL)))


def moon_factor(illum: float, moon_alt: float) -> float:
    """How much of the aurora survives moonlight. 1.0 means no washout."""
    if moon_alt <= 0:
        return 1.0
    # A full moon high in the sky costs roughly a third of faint detail.
    height = min(1.0, moon_alt / 45.0)
    return max(0.55, 1.0 - 0.45 * illum * height)


def dark_intervals(
    start: datetime,
    end: datetime,
    lat: float,
    lon: float,
    threshold: float = DARK_ENOUGH,
    step_minutes: int = 10,
) -> List[Tuple[datetime, datetime]]:
    """Contiguous spans between start and end where the sun sits below threshold."""
    if end <= start:
        return []
    spans: List[Tuple[datetime, datetime]] = []
    step = timedelta(minutes=max(1, step_minutes))
    cursor = start
    span_start: Optional[datetime] = None
    while cursor <= end:
        dark = sun_altitude(cursor, lat, lon) <= threshold
        if dark and span_start is None:
            span_start = cursor
        elif not dark and span_start is not None:
            spans.append((span_start, cursor))
            span_start = None
        cursor += step
    if span_start is not None:
        spans.append((span_start, end))
    return spans


def overlap(
    a_start: datetime, a_end: datetime, b_start: datetime, b_end: datetime
) -> Optional[Tuple[datetime, datetime]]:
    start = max(a_start, b_start)
    end = min(a_end, b_end)
    if end <= start:
        return None
    return start, end


# Centered dipole north geomagnetic pole, IGRF 2020 epoch.
GEOMAGNETIC_POLE_LAT = 80.65
GEOMAGNETIC_POLE_LON = -72.68


def geomagnetic_latitude(lat: float, lon: float) -> float:
    """Convert geographic to geomagnetic latitude.

    Auroral oval boundaries are quoted in geomagnetic latitude. In North America
    that runs several degrees higher than geographic, so comparing the two
    directly understates how far south the aurora reaches.
    """
    lat_r = lat * RAD
    pole_r = GEOMAGNETIC_POLE_LAT * RAD
    delta_lon = (lon - GEOMAGNETIC_POLE_LON) * RAD
    sin_mlat = (
        math.sin(lat_r) * math.sin(pole_r)
        + math.cos(lat_r) * math.cos(pole_r) * math.cos(delta_lon)
    )
    return math.asin(max(-1.0, min(1.0, sin_mlat))) * DEG


def sky_conditions(dt: datetime, lat: float, lon: float) -> Dict[str, object]:
    sun_alt = sun_altitude(dt, lat, lon)
    m_alt = moon_altitude(dt, lat, lon)
    illum = moon_illumination(dt)
    return {
        'sun_altitude': round(sun_alt, 2),
        'darkness': darkness_label(sun_alt),
        'darkness_factor': round(darkness_factor(sun_alt), 3),
        'moon_altitude': round(m_alt, 2),
        'moon_illumination': round(illum, 3),
        'moon_phase': moon_phase_name(illum),
        'moon_factor': round(moon_factor(illum, m_alt), 3),
    }
