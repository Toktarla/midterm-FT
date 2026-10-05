"""Failure-injection flags. Experiments write the file; services read it."""

from __future__ import annotations

import json
import os
import time
from typing import Any

from ftuis.config import CHAOS_PATH, ensure_dirs


def load() -> dict[str, Any]:
    if not CHAOS_PATH.exists():
        return {}
    # A replace can be observed mid-write on the Docker Desktop file share.
    for _ in range(4):
        try:
            raw = CHAOS_PATH.read_text(encoding="utf-8")
        except OSError:
            time.sleep(0.02)
            continue
        if not raw.strip():
            time.sleep(0.02)
            continue
        try:
            loaded = json.loads(raw)
        except json.JSONDecodeError:
            time.sleep(0.02)
            continue
        if isinstance(loaded, dict):
            return loaded
    return {}


def write(spec: dict[str, Any]) -> None:
    ensure_dirs()
    temporary = CHAOS_PATH.with_name(f".chaos-{os.getpid()}.tmp")
    with temporary.open("w", encoding="utf-8") as handle:
        handle.write(json.dumps(spec))
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, CHAOS_PATH)
    directory = os.open(str(CHAOS_PATH.parent), os.O_RDONLY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


def clear() -> None:
    write({})


def delay_s(instance_id: str) -> float:
    raw = load().get("delay_ms", {}).get(instance_id, 0)
    return float(raw) / 1000.0


def instance_failed(instance_id: str) -> bool:
    return instance_id in load().get("fail_instances", [])


def crash_point(instance_id: str) -> str | None:
    value = load().get("crash_points", {}).get(instance_id)
    return str(value) if value else None
