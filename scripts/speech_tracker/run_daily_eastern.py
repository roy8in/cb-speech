#!/usr/bin/env python3
"""Run the speech pipeline once per US Eastern day at 20:00 Eastern.

Cron runs this wrapper at the two possible Korea-time equivalents of
20:00 America/New_York. The wrapper handles DST and prevents duplicate
pipeline runs for the same Eastern calendar date.
"""

from __future__ import annotations

import subprocess
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo


PROJECT_ROOT = Path(__file__).resolve().parents[2]
STATE_DIR = PROJECT_ROOT / "state"
RUN_MARKER_DIR = STATE_DIR / "daily_runs"
EASTERN = ZoneInfo("America/New_York")


def main() -> int:
    now_et = datetime.now(EASTERN)
    if now_et.hour != 20:
        print(f"Skip: current Eastern time is {now_et.isoformat()}, not 20:00.")
        return 0

    RUN_MARKER_DIR.mkdir(parents=True, exist_ok=True)
    marker = RUN_MARKER_DIR / f"{now_et.strftime('%Y-%m-%d')}.done"
    if marker.exists():
        print(f"Skip: pipeline already ran for Eastern date {now_et.strftime('%Y-%m-%d')}.")
        return 0

    script = PROJECT_ROOT / "scripts" / "speech_tracker" / "sync_and_analyze.py"
    result = subprocess.run([sys.executable, str(script)], cwd=PROJECT_ROOT)
    if result.returncode == 0:
        marker.write_text(datetime.now(EASTERN).isoformat(), encoding="utf-8")
    return result.returncode


if __name__ == "__main__":
    raise SystemExit(main())
