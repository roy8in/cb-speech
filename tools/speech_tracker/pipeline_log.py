"""
Structured pipeline logging for the static log viewer.

The log format intentionally follows docs/pipeline-log-viewer.md:
YYYY-MM-DD HH:MM:SS,mmm | LEVEL | logger | message | key=value, key=value
"""

from __future__ import annotations

import csv
import logging
import sys
import time
import uuid
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo


PROJECT_ROOT = Path(__file__).resolve().parents[2]
LOG_DIR = PROJECT_ROOT / "logs"
LOGGER_NAME = "cb_speeches"
LOG_TZ = ZoneInfo("America/New_York")


class EasternFormatter(logging.Formatter):
    def formatTime(self, record, datefmt=None):
        dt = datetime.fromtimestamp(record.created, LOG_TZ)
        if datefmt:
            return dt.strftime(datefmt)
        return dt.strftime("%Y-%m-%d %H:%M:%S") + f",{int(record.msecs):03d}"


def _safe_value(value) -> str:
    if value is None:
        return ""
    text = str(value).replace("\n", " ").replace("\r", " ")
    return text.replace(", ", "; ").replace("=", ":").strip()


def _format_extra(extra: dict) -> str:
    if not extra:
        return ""
    parts = [f"{key}={_safe_value(value)}" for key, value in extra.items()]
    return " | " + ", ".join(parts)


def setup_run_logging(run_id: str | None = None) -> dict:
    LOG_DIR.mkdir(exist_ok=True)

    run_started_at = datetime.now(LOG_TZ)
    run_id = run_id or f"{run_started_at.strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
    app_log_path = LOG_DIR / f"app_{run_started_at.strftime('%Y-%m-%d')}.log"
    summary_log_path = LOG_DIR / f"summary_{run_started_at.strftime('%Y-%m')}.csv"

    logger = logging.getLogger(LOGGER_NAME)
    logger.setLevel(logging.INFO)
    logger.propagate = False

    for handler in list(logger.handlers):
        logger.removeHandler(handler)
        handler.close()

    formatter = EasternFormatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")

    file_handler = logging.FileHandler(app_log_path, encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="backslashreplace")

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    return {
        "run_id": run_id,
        "run_started_at": run_started_at,
        "run_started_perf": time.perf_counter(),
        "app_log_path": app_log_path,
        "summary_log_path": summary_log_path,
        "logger": logger,
    }


def log_event(logger: logging.Logger, level: str, message: str, **extra) -> None:
    log_method = getattr(logger, level.lower())
    log_method(f"{message}{_format_extra(extra)}")


def log_pipeline_job(
    logger: logging.Logger | None,
    job_name: str,
    status: str,
    started_at: float | None = None,
    **extra,
) -> None:
    if logger is None:
        return

    payload = {
        "job_name": job_name,
        "status": status,
    }
    if started_at is not None:
        payload["duration_sec"] = round(time.perf_counter() - started_at, 3)
    payload.update(extra)
    log_event(logger, "info", "Pipeline job status", **payload)


def append_summary(
    summary_log_path: Path,
    *,
    run_id: str,
    started_at: datetime,
    status: str,
    duration_sec: float,
    total_new: int,
    total_refreshed: int,
    analyzed_items: int,
    synced_items: int,
    failed_steps: str = "",
) -> None:
    summary_log_path.parent.mkdir(exist_ok=True)
    exists = summary_log_path.exists()
    fieldnames = [
        "run_id",
        "started_at",
        "status",
        "duration_sec",
        "total_new",
        "total_refreshed",
        "analyzed_items",
        "synced_items",
        "failed_steps",
    ]
    with summary_log_path.open("a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        if not exists:
            writer.writeheader()
        writer.writerow(
            {
                "run_id": run_id,
                "started_at": started_at.isoformat(),
                "status": status,
                "duration_sec": round(duration_sec, 3),
                "total_new": total_new,
                "total_refreshed": total_refreshed,
                "analyzed_items": analyzed_items,
                "synced_items": synced_items,
                "failed_steps": failed_steps,
            }
        )
