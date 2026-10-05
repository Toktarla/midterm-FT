"""Append-only experiment log shared by every process."""

from __future__ import annotations

import json
import os
import time
from typing import Any

import fcntl

from ftuis.config import EVENTS_PATH, ensure_dirs


def emit(event: str, **fields: Any) -> float:
    ensure_dirs()
    ts = time.time()
    record = {
        "ts": ts,
        "event": event,
        "instance": os.environ.get("INSTANCE_ID", "supervisor"),
        "service": os.environ.get("SERVICE", "control"),
        "mode": os.environ.get("FT_MODE", "baseline"),
        **fields,
    }
    line = json.dumps(record, separators=(",", ":")) + "\n"
    with EVENTS_PATH.open("a", encoding="utf-8") as handle:
        fcntl.flock(handle, fcntl.LOCK_EX)
        handle.write(line)
        handle.flush()
        fcntl.flock(handle, fcntl.LOCK_UN)
    return ts


def read_events(since: float = 0.0) -> list[dict[str, Any]]:
    if not EVENTS_PATH.exists():
        return []
    rows: list[dict[str, Any]] = []
    for line in EVENTS_PATH.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        row = json.loads(line)
        if row.get("ts", 0) >= since:
            rows.append(row)
    return rows


def clear_events() -> None:
    ensure_dirs()
    EVENTS_PATH.write_text("", encoding="utf-8")
