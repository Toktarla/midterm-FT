"""Process configuration. The supervisor sets these variables per instance."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
LOGS = ROOT / "logs"
RUNTIME = ROOT / "runtime"
RESULTS = ROOT / "results"
CHAOS_PATH = RUNTIME / "chaos.json"
REGISTRY_PATH = RUNTIME / "registry.json"
EVENTS_PATH = LOGS / "events.jsonl"

TUITION_AMOUNT = 5000.0
COURSES = (
    ("Fault Tolerance", "Mon", "09:00"),
    ("Databases", "Tue", "11:00"),
    ("Calculus", "Wed", "14:00"),
)


@dataclass(frozen=True)
class Settings:
    mode: str
    ft: bool
    instance_id: str
    node_id: str
    service: str
    # Baseline waits longer and tries once. FT fails fast and retries elsewhere.
    upstream_timeout_s: float
    upstream_attempts: int
    work_slots: int
    work_wait_s: float
    work_duration_s: float


def load_settings() -> Settings:
    mode = os.environ.get("FT_MODE", "baseline").strip().lower()
    if mode in {"fault_tolerant", "fault-tolerant"}:
        mode = "ft"
    if mode not in {"baseline", "ft"}:
        raise ValueError(f"FT_MODE must be baseline or ft, got {mode!r}")
    ft = mode == "ft"
    return Settings(
        mode=mode,
        ft=ft,
        instance_id=os.environ.get("INSTANCE_ID", "local"),
        node_id=os.environ.get("NODE_ID", "a"),
        service=os.environ.get("SERVICE", "unknown"),
        upstream_timeout_s=0.6 if ft else 2.0,
        upstream_attempts=3 if ft else 1,
        work_slots=20 if ft else 5,
        work_wait_s=0.02 if ft else 0.5,
        work_duration_s=0.25,
    )


def ensure_dirs() -> None:
    for path in (DATA, LOGS, RUNTIME, RESULTS):
        path.mkdir(parents=True, exist_ok=True)
