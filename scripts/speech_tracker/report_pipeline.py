#!/usr/bin/env python3
"""
Pipeline reporting utility.

Shows recent collection runs and the stage-by-stage timings stored in pipeline_logs.
"""

import argparse
import json
import sqlite3
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from tools.speech_tracker.models import get_db_path


def load_rows(run_id=None, limit=5):
    conn = sqlite3.connect(get_db_path())
    conn.row_factory = sqlite3.Row
    try:
        params = []
        where = ""
        if run_id:
            where = "WHERE run_id = ?"
            params.append(run_id)

        runs = conn.execute(
            f"""
            SELECT run_id,
                   MIN(started_at) AS started_at,
                   MAX(finished_at) AS finished_at
            FROM pipeline_logs
            {where}
            GROUP BY run_id
            ORDER BY started_at DESC
            LIMIT ?
            """,
            (*params, limit),
        ).fetchall()

        if not runs:
            return [], {}

        run_ids = [r["run_id"] for r in runs]
        placeholders = ", ".join(["?"] * len(run_ids))
        stages = conn.execute(
            f"""
            SELECT *
            FROM pipeline_logs
            WHERE run_id IN ({placeholders})
            ORDER BY started_at ASC, id ASC
            """,
            run_ids,
        ).fetchall()

        stage_map = defaultdict(list)
        for row in stages:
            stage_map[row["run_id"]].append(dict(row))

        return [dict(r) for r in runs], stage_map
    finally:
        conn.close()


def print_run(run, stages):
    print(f"Run: {run['run_id']}")
    print(f"  Started:  {run['started_at']}")
    print(f"  Finished: {run['finished_at']}")
    for stage in stages:
        duration = stage.get("duration_seconds")
        if duration is None and stage.get("started_at") and stage.get("finished_at"):
            try:
                duration = (
                    datetime.fromisoformat(stage["finished_at"]) - datetime.fromisoformat(stage["started_at"])
                ).total_seconds()
            except Exception:
                duration = None
        duration_text = f"{duration:.2f}s" if duration is not None else "n/a"
        items = stage.get("item_count")
        status = stage.get("status") or "unknown"
        print(f"  - {stage['stage_name']}: {status}, items={items}, duration={duration_text}")
        details = stage.get("details_json")
        if details:
            try:
                parsed = json.loads(details)
                print(f"    details: {json.dumps(parsed, ensure_ascii=False)}")
            except Exception:
                print(f"    details: {details}")


def main():
    parser = argparse.ArgumentParser(description="Show recent pipeline logs")
    parser.add_argument("--run-id", help="Show only one run_id")
    parser.add_argument("--limit", type=int, default=5, help="Number of recent runs to show")
    args = parser.parse_args()

    runs, stage_map = load_rows(run_id=args.run_id, limit=args.limit)
    if not runs:
        print("No pipeline logs found.")
        return

    for idx, run in enumerate(runs):
        if idx:
            print()
        print_run(run, stage_map.get(run["run_id"], []))


if __name__ == "__main__":
    main()
