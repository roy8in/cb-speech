from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, time
from pathlib import Path
from zoneinfo import ZoneInfo


ROOT_DIR = Path(__file__).resolve().parent
STATE_DIR = ROOT_DIR / "state"
STATUS_PATH = STATE_DIR / "cb_speeches_status.json"
EVENT_PATH = STATE_DIR / "cb_speeches_events.jsonl"
KST = ZoneInfo("Asia/Seoul")
EASTERN = ZoneInfo("America/New_York")
DEFAULT_SERVICE = "cb-speeches"
DEFAULT_ENVIRONMENT = "home"
DEFAULT_HOST = os.uname().nodename if hasattr(os, "uname") else os.environ.get("COMPUTERNAME", "unknown")
BANKS = ("FRB", "ECB", "BOE", "BOJ", "RBA", "BOC")


def now() -> datetime:
    return datetime.now(KST)


def iso(value: datetime | None = None) -> str:
    return (value or now()).isoformat()


def next_daily_eastern_run(value: datetime | None = None) -> str:
    current = (value or now()).astimezone(EASTERN)
    candidate = datetime.combine(current.date(), time(20, 0), tzinfo=EASTERN)
    if current >= candidate:
        candidate += timedelta(days=1)
    return candidate.isoformat()


def next_three_hour_run(value: datetime | None = None) -> str:
    """Backward-compatible name; schedule is now daily at 20:00 America/New_York."""
    return next_daily_eastern_run(value)


def _read_json(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp_path.replace(path)


def append_event(event: dict) -> None:
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    payload = dict(event)
    payload.setdefault("event_at", iso())
    with EVENT_PATH.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=False) + "\n")


def read_status() -> dict:
    return _read_json(STATUS_PATH)


def write_status(payload: dict) -> None:
    _write_json(STATUS_PATH, payload)


def ensure_status() -> dict:
    status = read_status()
    status.setdefault("service", DEFAULT_SERVICE)
    status.setdefault("environment", DEFAULT_ENVIRONMENT)
    status.setdefault("host", DEFAULT_HOST)
    status.setdefault("state", "idle")
    status.setdefault("generated_at", iso())
    status.setdefault("next_run_at", next_daily_eastern_run())
    status.setdefault("schedule", {"timezone": "America/New_York", "time": "20:00", "frequency": "daily"})
    status.setdefault("summary", {})
    status.setdefault("stages", {})
    status.setdefault("banks", {bank: {} for bank in BANKS})
    for bank in BANKS:
        status["banks"].setdefault(bank, {})
    return status


def update_status(**fields) -> dict:
    status = ensure_status()
    status.update(fields)
    status["generated_at"] = iso()
    write_status(status)
    return status


def update_bank(bank_code: str, **fields) -> dict:
    status = ensure_status()
    bank = dict(status["banks"].get(bank_code) or {})
    bank.update(fields)
    status["banks"][bank_code] = bank
    status["generated_at"] = iso()
    status["state"] = _aggregate_state(status["banks"])
    write_status(status)
    return bank


def update_stage(stage_name: str, **fields) -> dict:
    status = ensure_status()
    stage = dict(status["stages"].get(stage_name) or {})
    stage.update(fields)
    status["stages"][stage_name] = stage
    status["generated_at"] = iso()
    status["state"] = _aggregate_state(status["banks"])
    write_status(status)
    return stage


def _aggregate_state(banks: dict) -> str:
    order = {"failed": 4, "partial": 3, "running": 2, "success": 1, "skipped": 0, "idle": 0}
    ranked = sorted((bank.get("state", "idle") for bank in banks.values()), key=lambda s: order.get(s, 0), reverse=True)
    return ranked[0] if ranked else "idle"
