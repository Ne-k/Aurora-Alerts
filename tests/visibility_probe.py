import os, sys
from datetime import datetime
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)
from aurora.forecast import ForecastEngine

# Use live NOAA forecast text; print bullets to inspect 👀 values

def main():
    lat = float(os.getenv('LATITUDE', '45.5152'))
    lon = float(os.getenv('LONGITUDE', '-122.6784'))
    tz = os.getenv('TIMEZONE_NAME', 'America/Los_Angeles')
    eng = ForecastEngine(latitude=lat, longitude=lon, timezone_name=tz)
    text = eng.fetch_forecast()
    build = eng.build_alert(text)
    if not build:
        print('no build')
        return
    print('window:', build.window_id)
    for d in build.detections:
        ut_block_label = (d.ut_block[:-2] + ' UT') if d.ut_block.endswith('UT') else d.ut_block
        print(f"• {ut_block_label} → <t:{d.start_ts}:t> - <t:{d.end_ts}:t> • KP {d.kp:.2f} • ☁️ {d.cloud_avg_display} • 👀 {d.visibility_pct}%")

if __name__ == '__main__':
    main()
