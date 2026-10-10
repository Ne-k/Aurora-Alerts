from __future__ import annotations

import os
import sys
import unittest
from datetime import datetime, timezone
from unittest import mock

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from aurora import forecast  # noqa: E402
from aurora.forecast import ForecastEngine, GFZRecord  # noqa: E402


class OvationLongitudeTests(unittest.TestCase):
    def test_western_longitude_matches_0_360_grid(self):
        # OVATION lists [lon 0..359, lat, prob]; Portland (-122.68) is 237E, not the lon-0 cell in Europe
        grid = {'coordinates': [[0, 46, 90], [237, 45, 33], [238, 46, 44]]}
        with mock.patch.object(forecast.net, 'cached_json', return_value=grid):
            prob = ForecastEngine()._ovation_probability_uncached(45.5152, -122.6784)
        self.assertIn(prob, (33, 44))


class CloudTimezoneTests(unittest.TestCase):
    def test_naive_open_meteo_tags_are_utc(self):
        payload = {'hourly': {'time': ['2026-10-10T03:00'], 'cloudcover': [10], 'cloudcover_low': [5],
                              'cloudcover_mid': [0], 'cloudcover_high': [0]}}
        with mock.patch.object(forecast.net, 'cached_json', return_value=payload), \
                mock.patch.dict(os.environ, {'TZ': 'America/Los_Angeles'}):
            if hasattr(__import__('time'), 'tzset'):
                __import__('time').tzset()
            out = ForecastEngine().fetch_cloud_detail()
        self.assertIn(datetime(2026, 10, 10, 3, tzinfo=timezone.utc), out)


class Hp30Tests(unittest.TestCase):
    def test_latest_hp30_uses_last_record(self):
        recs = [GFZRecord(datetime(2026, 10, 10, 19, tzinfo=timezone.utc), 6.0, ''),
                GFZRecord(datetime(2026, 10, 10, 19, 30, tzinfo=timezone.utc), 6.667, '')]
        e = ForecastEngine()
        e.fetch_gfz_series = lambda *a, **k: (recs, {})
        self.assertEqual(e.latest_hp30(), 6.667)
        e.fetch_gfz_series = lambda *a, **k: ([], None)
        self.assertIsNone(e.latest_hp30())


if __name__ == '__main__':
    unittest.main()
