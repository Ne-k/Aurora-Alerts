import os, sys, json
from datetime import datetime, timezone

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from dotenv import load_dotenv
from aurora.forecast import ForecastEngine

def _find_num_nested(obj, keys_lower):
    try:
        if isinstance(obj, dict):
            for k, v in obj.items():
                try:
                    if isinstance(k, str) and k.lower() in keys_lower and isinstance(v, (int, float)):
                        return float(v)
                except Exception:
                    pass
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


def main():
    load_dotenv()
    lat = float(os.getenv('LATITUDE', '45.5152'))
    lon = float(os.getenv('LONGITUDE', '-122.6784'))
    tz = os.getenv('TIMEZONE_NAME', 'America/Los_Angeles')
    eng = ForecastEngine(latitude=lat, longitude=lon, timezone_name=tz)
    data = eng.fetch_maf_data(lat, lon, tz)
    print('Fetched at:', datetime.now(timezone.utc).isoformat())
    if data is None:
        print('MAF response: None (request failed or not configured)')
        return

    # Print a compact overview of top-level keys and a small preview
    try:
        if isinstance(data, dict):
            print('Top-level keys:', ', '.join(list(data.keys())[:15]))
        preview = json.dumps(data, indent=2)
        print(preview[:1200])
    except Exception:
        print(str(data)[:1200])

    # Extract the fields the bot uses for the My Aurora Forecast summary
    maf_kp = _find_num_nested(data, ['kp', 'kp_index', 'kp current', 'kp_current'])
    maf_prob = _find_num_nested(data, ['chance', 'probability', 'visibility', 'aurora_probability'])
    maf_cloud = _find_num_nested(data, ['cloud_cover', 'clouds', 'cloud'])

    parts = []
    if isinstance(maf_kp, (int, float)):
        parts.append(f"KP {float(maf_kp):.2f}")
    if isinstance(maf_prob, (int, float)):
        parts.append(f"chance {int(round(float(maf_prob)))}%")
    if isinstance(maf_cloud, (int, float)):
        parts.append(f"\u2601\ufe0f {int(round(float(maf_cloud)))}%")

    if parts:
        summary = "My Aurora Forecast: " + " • ".join(parts)
    else:
        summary = "My Aurora Forecast: no recognizable KP/probability/cloud fields found"

    print('\nDerived summary for bot embed:')
    print(summary)

if __name__ == '__main__':
    main()
