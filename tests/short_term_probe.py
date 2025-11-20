import os, sys
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)
from aurora.forecast import ForecastEngine

def main():
    lat = float(os.getenv('LATITUDE', '45.5152'))
    lon = float(os.getenv('LONGITUDE', '-122.6784'))
    tz = os.getenv('TIMEZONE_NAME', 'America/Los_Angeles')
    eng = ForecastEngine(latitude=lat, longitude=lon, timezone_name=tz)
    s = eng.short_term_visibility_series(minutes=30, step=5)
    print('keys:', list(s.keys()))
    print('points:', len(s.get('points', [])))
    print('sample:', s.get('points', [])[:3])
    print('ovation_prob:', s.get('ovation_prob'))
    print('maf_kp:', s.get('maf_kp'))
    print('maf_prob:', s.get('maf_prob'))
    print('cloud_now:', s.get('cloud_now'))
    print('cloud_tonight:', s.get('cloud_tonight'))

if __name__ == '__main__':
    main()
