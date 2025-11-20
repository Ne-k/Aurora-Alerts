import os, sys
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)
from aurora.forecast import ForecastEngine

def main():
    with open('forecastExample.txt','r') as f:
        text = f.read()
    eng = ForecastEngine(kp_threshold=6.2)
    build = eng.build_alert(text, debug=False)
    print('detections:', [(d.day_label, d.ut_block, d.kp) for d in build.detections])
    print('window_id:', build.window_id)
    print('message first line:', build.message.splitlines()[0])

if __name__ == '__main__':
    main()
