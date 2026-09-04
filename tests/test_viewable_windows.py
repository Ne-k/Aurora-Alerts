from __future__ import annotations

import os
import sys
import unittest
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from aurora.forecast import ForecastEngine, Detection  # noqa: E402

PORTLAND = dict(latitude=45.5152, longitude=-122.6784, location_name="Portland, OR")
# 7 PM local, before dark, so every window lies ahead.
EVENING = datetime(2026, 9, 5, 2, 0, tzinfo=timezone.utc)


def make_detection(base: datetime, start_h: float, end_h: float, kp: float) -> Detection:
    start = base + timedelta(hours=start_h)
    end = base + timedelta(hours=end_h)
    return Detection(
        'day', start.date(), 'block', kp,
        int(start.timestamp()), int(end.timestamp()), 'N/A', 0, '', '',
    )


def clouds(base: datetime, low: int, total: int | None = None) -> dict:
    out = {}
    cursor = base - timedelta(hours=6)
    for i in range(72):
        out[cursor + timedelta(hours=i)] = {
            'total': total if total is not None else low,
            'low': low,
            'mid': 5,
            'high': 10,
        }
    return out


class ViewableWindowTests(unittest.TestCase):
    def setUp(self):
        self.engine = ForecastEngine(kp_threshold=6.2, **PORTLAND)

    def test_clear_dark_storm_is_viewable(self):
        dets = [make_detection(EVENING, 5, 8, 7.0)]
        windows, reason = self.engine.compute_viewable_windows(
            dets, clouds(EVENING, 10), now_utc=EVENING, wind_drive=0.8
        )
        self.assertEqual(len(windows), 1)
        self.assertIsNone(reason)
        self.assertGreater(windows[0]['score'], 0)

    def test_overcast_blocks_with_reason(self):
        dets = [make_detection(EVENING, 5, 8, 7.0)]
        windows, reason = self.engine.compute_viewable_windows(
            dets, clouds(EVENING, 95), now_utc=EVENING
        )
        self.assertEqual(windows, [])
        self.assertIn("overcast", reason)

    def test_daylight_blocks_with_reason(self):
        # A block entirely in local daytime.
        dets = [make_detection(EVENING, 16, 19, 7.0)]
        windows, reason = self.engine.compute_viewable_windows(
            dets, clouds(EVENING, 10), now_utc=EVENING
        )
        self.assertEqual(windows, [])
        self.assertIn("daylight", reason)

    def test_window_is_trimmed_to_darkness(self):
        """A block straddling sunset should start when it actually gets dark."""
        dets = [make_detection(EVENING, 0, 6, 7.0)]
        windows, _ = self.engine.compute_viewable_windows(
            dets, clouds(EVENING, 10), now_utc=EVENING
        )
        self.assertEqual(len(windows), 1)
        self.assertGreater(windows[0]['start_ts'], int(EVENING.timestamp()))

    def test_past_windows_ignored(self):
        dets = [make_detection(EVENING, -8, -5, 8.0)]
        windows, reason = self.engine.compute_viewable_windows(
            dets, clouds(EVENING, 10), now_utc=EVENING
        )
        self.assertEqual(windows, [])
        self.assertIsNone(reason)

    def test_adjacent_blocks_merge_into_one_session(self):
        dets = [
            make_detection(EVENING, 2, 5, 6.7),
            make_detection(EVENING, 5, 8, 7.0),
            make_detection(EVENING, 8, 11, 6.5),
        ]
        windows, _ = self.engine.compute_viewable_windows(
            dets, clouds(EVENING, 15), now_utc=EVENING, wind_drive=0.7
        )
        self.assertEqual(len(windows), 1)
        self.assertEqual(windows[0]['kp'], 7.0)
        span_hours = (windows[0]['end_ts'] - windows[0]['start_ts']) / 3600
        self.assertGreater(span_hours, 5)

    def test_token_stays_stable_as_time_advances(self):
        """Regression: a clamped start would re-alert on every update cycle."""
        dets = [make_detection(EVENING, 2, 5, 7.0), make_detection(EVENING, 5, 8, 7.0)]
        cloud_map = clouds(EVENING, 15)
        tokens = []
        for offset in range(0, 5):
            now = EVENING + timedelta(hours=offset)
            windows, _ = self.engine.compute_viewable_windows(
                dets, cloud_map, now_utc=now, wind_drive=0.7
            )
            tokens.append(tuple(w['token'] for w in windows))
        self.assertEqual(len(set(tokens)), 1, f"tokens drifted: {tokens}")

    def test_higher_latitude_scores_higher(self):
        dets = [make_detection(EVENING, 5, 8, 5.5)]
        south = ForecastEngine(kp_threshold=4.0, **PORTLAND)
        north = ForecastEngine(kp_threshold=4.0, latitude=61.2181, longitude=-149.9003,
                               location_name="Anchorage, AK")
        s_win, _ = south.compute_viewable_windows(dets, clouds(EVENING, 10), now_utc=EVENING)
        n_win, _ = north.compute_viewable_windows(dets, clouds(EVENING, 10), now_utc=EVENING)
        self.assertTrue(s_win and n_win)
        self.assertGreater(n_win[0]['score'], s_win[0]['score'])

    def test_cloud_cover_lowers_score(self):
        dets = [make_detection(EVENING, 5, 8, 7.0)]
        clear, _ = self.engine.compute_viewable_windows(dets, clouds(EVENING, 0), now_utc=EVENING)
        murky, _ = self.engine.compute_viewable_windows(dets, clouds(EVENING, 60), now_utc=EVENING)
        self.assertGreater(clear[0]['score'], murky[0]['score'])


class SolarWindTests(unittest.TestCase):
    def test_drive_rises_with_southward_bz(self):
        north = ForecastEngine.solar_wind_drive({'bz': 5.0, 'speed': 400})
        calm = ForecastEngine.solar_wind_drive({'bz': -2.0, 'speed': 400})
        storm = ForecastEngine.solar_wind_drive({'bz': -12.0, 'speed': 650})
        self.assertEqual(north, 0.0)
        self.assertGreater(storm, calm)
        self.assertLessEqual(storm, 1.0)

    def test_drive_none_without_data(self):
        self.assertIsNone(ForecastEngine.solar_wind_drive(None))
        self.assertIsNone(ForecastEngine.solar_wind_drive({'speed': 400}))

    def test_line_mentions_direction(self):
        self.assertIn("south", ForecastEngine.solar_wind_line({'bz': -6.0, 'speed': 500}))
        self.assertIn("north", ForecastEngine.solar_wind_line({'bz': 3.0, 'speed': 400}))

    def test_southward_bz_raises_visibility(self):
        engine = ForecastEngine(kp_threshold=6.2, **PORTLAND)
        quiet = engine.visibility_percent(kp=7.0, cloud_avg=10, sun_altitude=-20, solar_wind_drive=0.0)
        active = engine.visibility_percent(kp=7.0, cloud_avg=10, sun_altitude=-20, solar_wind_drive=1.0)
        self.assertGreater(active, quiet)


class AlertRelevanceTests(unittest.TestCase):
    def test_low_k_alerts_filtered_out(self):
        engine = ForecastEngine(kp_threshold=6.2, **PORTLAND)
        alerts = [
            {'level': 4.0, 'kind': 'WARNING', 'headline': 'K 4'},
            {'level': 7.0, 'kind': 'WARNING', 'headline': 'K 7'},
            {'level': None, 'kind': 'WATCH', 'headline': 'G3 watch'},
            {'level': 8.0, 'kind': 'CANCEL WARNING', 'headline': 'cancelled'},
        ]
        kept = engine.relevant_alerts(alerts)
        headlines = [a['headline'] for a in kept]
        self.assertIn('K 7', headlines)
        self.assertIn('G3 watch', headlines)
        self.assertNotIn('K 4', headlines)
        self.assertNotIn('cancelled', headlines)


if __name__ == '__main__':
    unittest.main()
