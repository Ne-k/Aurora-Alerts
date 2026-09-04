from __future__ import annotations

import os
import sys
import unittest
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from aurora import astro  # noqa: E402

PORTLAND = (45.5152, -122.6784)


class SunTests(unittest.TestCase):
    def test_altitude_at_known_sunset(self):
        # Open-Meteo puts Portland sunset at 2026-09-05T02:43Z. Sunset is defined
        # at the upper limb, roughly -0.833 deg of geometric altitude.
        when = datetime(2026, 9, 5, 2, 43, tzinfo=timezone.utc)
        alt = astro.sun_altitude(when, *PORTLAND)
        self.assertAlmostEqual(alt, -0.833, delta=0.35)

    def test_noon_is_higher_than_midnight(self):
        noon = datetime(2026, 6, 21, 20, 0, tzinfo=timezone.utc)
        midnight = datetime(2026, 6, 21, 8, 0, tzinfo=timezone.utc)
        self.assertGreater(astro.sun_altitude(noon, *PORTLAND), 60)
        self.assertLess(astro.sun_altitude(midnight, *PORTLAND), -10)

    def test_summer_sun_is_higher_than_winter(self):
        summer = astro.sun_altitude(datetime(2026, 6, 21, 20, 0, tzinfo=timezone.utc), *PORTLAND)
        winter = astro.sun_altitude(datetime(2026, 12, 21, 20, 0, tzinfo=timezone.utc), *PORTLAND)
        self.assertGreater(summer - winter, 40)

    def test_darkness_labels(self):
        self.assertEqual(astro.darkness_label(10), "daylight")
        self.assertEqual(astro.darkness_label(-3), "civil twilight")
        self.assertEqual(astro.darkness_label(-20), "night")

    def test_darkness_factor_bounds(self):
        self.assertEqual(astro.darkness_factor(5), 0.0)
        self.assertEqual(astro.darkness_factor(-25), 1.0)
        self.assertTrue(0 < astro.darkness_factor(-9) < 1)


class MoonTests(unittest.TestCase):
    def test_illumination_in_range(self):
        for day in range(0, 60, 3):
            when = datetime(2026, 3, 1, tzinfo=timezone.utc) + timedelta(days=day)
            self.assertTrue(0.0 <= astro.moon_illumination(when) <= 1.0)

    def test_synodic_period(self):
        """Full moon to full moon should be close to 29.53 days."""
        start = datetime(2026, 1, 1, tzinfo=timezone.utc)
        peaks = []
        prev = astro.moon_illumination(start)
        rising = True
        for i in range(1, 400):
            when = start + timedelta(hours=6 * i)
            cur = astro.moon_illumination(when)
            if rising and cur < prev:
                peaks.append(when)
                rising = False
            elif not rising and cur > prev:
                rising = True
            prev = cur
        gaps = [(peaks[i + 1] - peaks[i]).total_seconds() / 86400 for i in range(len(peaks) - 1)]
        self.assertTrue(gaps)
        for gap in gaps:
            self.assertAlmostEqual(gap, 29.53, delta=0.6)

    def test_moon_factor_penalises_bright_high_moon(self):
        dark = astro.moon_factor(0.0, 40)
        bright = astro.moon_factor(1.0, 40)
        below = astro.moon_factor(1.0, -10)
        self.assertEqual(dark, 1.0)
        self.assertEqual(below, 1.0)
        self.assertLess(bright, dark)


class GeomagneticTests(unittest.TestCase):
    def test_known_sites(self):
        # Published centered-dipole values.
        self.assertAlmostEqual(astro.geomagnetic_latitude(45.52, -122.68), 51.0, delta=1.0)
        self.assertAlmostEqual(astro.geomagnetic_latitude(47.61, -122.33), 53.1, delta=1.0)
        self.assertAlmostEqual(astro.geomagnetic_latitude(61.22, -149.90), 61.9, delta=1.5)

    def test_north_america_runs_higher_than_geographic(self):
        self.assertGreater(astro.geomagnetic_latitude(45.52, -122.68), 45.52)


class DarkIntervalTests(unittest.TestCase):
    def test_finds_one_night(self):
        start = datetime(2026, 9, 4, 18, 0, tzinfo=timezone.utc)
        spans = astro.dark_intervals(start, start + timedelta(hours=24), *PORTLAND)
        self.assertEqual(len(spans), 1)
        span_start, span_end = spans[0]
        hours = (span_end - span_start).total_seconds() / 3600
        self.assertTrue(6 < hours < 11, hours)

    def test_no_dark_span_during_daytime_range(self):
        start = datetime(2026, 6, 21, 17, 0, tzinfo=timezone.utc)
        spans = astro.dark_intervals(start, start + timedelta(hours=4), *PORTLAND)
        self.assertEqual(spans, [])

    def test_overlap(self):
        a = datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc)
        self.assertIsNone(astro.overlap(a, a + timedelta(hours=1), a + timedelta(hours=2), a + timedelta(hours=3)))
        got = astro.overlap(a, a + timedelta(hours=2), a + timedelta(hours=1), a + timedelta(hours=3))
        self.assertEqual(got, (a + timedelta(hours=1), a + timedelta(hours=2)))


if __name__ == '__main__':
    unittest.main()
