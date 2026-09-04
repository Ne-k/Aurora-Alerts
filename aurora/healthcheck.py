from __future__ import annotations

import os
import sys
import time

HEARTBEAT_PATH = os.path.abspath(
    os.getenv('HEARTBEAT_PATH', os.path.join(os.path.dirname(__file__), '..', 'data', 'heartbeat'))
)


def max_age_seconds() -> float:
    try:
        interval_hours = float(os.getenv('UPDATE_INTERVAL_HOURS', '2') or 2)
    except Exception:
        interval_hours = 2.0
    # Allow two missed cycles plus a grace period before declaring the bot dead.
    return max(1800.0, interval_hours * 3600.0 * 2 + 900.0)


def main() -> int:
    if not os.path.exists(HEARTBEAT_PATH):
        # Nothing written yet; the start_period in compose covers first boot.
        print("no heartbeat file yet")
        return 1
    try:
        with open(HEARTBEAT_PATH, 'r', encoding='utf-8') as fh:
            written = int((fh.read() or '0').strip() or 0)
    except Exception as exc:
        print(f"unreadable heartbeat: {exc}")
        return 1
    age = time.time() - written
    limit = max_age_seconds()
    if age > limit:
        print(f"heartbeat stale: {int(age)}s old (limit {int(limit)}s)")
        return 1
    print(f"ok: heartbeat {int(age)}s old")
    return 0


if __name__ == '__main__':
    sys.exit(main())
