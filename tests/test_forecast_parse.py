from __future__ import annotations

import os
import sys
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from aurora.forecast import ForecastEngine  # noqa: E402

EXAMPLE = os.path.join(os.path.dirname(__file__), '..', 'forecastExample.txt')


def _example_text() -> str:
    with open(EXAMPLE, 'r', encoding='utf-8') as fh:
        return fh.read()


class ParseTableTests(unittest.TestCase):
    """Parsing runs offline: the network calls inside build_alert are stubbed."""

    def setUp(self):
        self.engine = ForecastEngine(kp_threshold=6.5)
        self.engine.fetch_cloud_cover = lambda: {}
        self.engine.fetch_cloud_cover_openweather = lambda: {}
        self.engine.fetch_aurora_snapshot = lambda lat, lon: None
        self.engine.fetch_ovation_probability = lambda lat, lon: None
        self.engine.fetch_maf_data = lambda lat, lon, tz: None
        self.engine.fetch_gfz_series = lambda start, end, index='Kp', status=None: ([], None)
        self.engine.fetch_swpc_planetary_k_latest = lambda: (None, [])
        self.engine.fetch_swpc_hemi_power = lambda: None

    def test_builds_from_example(self):
        build = self.engine.build_alert(_example_text())
        self.assertIsNotNone(build)

    def test_three_day_columns(self):
        build = self.engine.build_alert(_example_text())
        columns = build.forecast_columns or []
        self.assertEqual(len(columns), 3)
        for col in columns:
            self.assertEqual(len(col['lines']), 8)
            self.assertTrue(col['label'])

    def test_peak_kp_matches_table(self):
        build = self.engine.build_alert(_example_text())
        self.assertAlmostEqual(build.max_forecast_kp, 7.67, places=2)

    def test_threshold_selects_windows(self):
        build = self.engine.build_alert(_example_text())
        # Example table has 6.67, 7.00 and 7.67 at or above 6.5.
        self.assertEqual(len(build.detections), 3)
        self.assertTrue(all(d.kp >= 6.5 for d in build.detections))

    def test_high_threshold_yields_no_windows(self):
        self.engine.kp_threshold = 9.0
        build = self.engine.build_alert(_example_text())
        self.assertEqual(build.detections, [])

    def test_rejects_unparsable_text(self):
        self.assertIsNone(self.engine.build_alert("not a forecast"))


class ScaleTests(unittest.TestCase):
    def test_activity_labels(self):
        self.assertEqual(ForecastEngine.kp_activity_label(1.0), "quiet")
        self.assertEqual(ForecastEngine.kp_activity_label(5.0), "G1 minor storm")
        self.assertEqual(ForecastEngine.kp_activity_label(8.5), "G4 severe storm")

    def test_color_varies_with_activity(self):
        self.assertNotEqual(ForecastEngine.kp_color(2.0), ForecastEngine.kp_color(7.5))


if __name__ == '__main__':
    unittest.main()
