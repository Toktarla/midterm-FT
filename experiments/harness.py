"""Process control and HTTP calls used by the experiments."""

from __future__ import annotations

import json
import os
import shutil
import signal
import sqlite3
import subprocess
import sys
import time
import uuid
from dataclasses import dataclass, field

import httpx

from ftuis import chaos
from ftuis.audit import audit
from ftuis.config import DATA, EVENTS_PATH, LOGS, REGISTRY_PATH, ROOT, ensure_dirs
from ftuis.events import clear_events, read_events

CONTROL = "http://127.0.0.1:9000"


@dataclass
class Call:
    status: int
    body: dict | list | None
    error: str | None = None

    @property
    def ok(self) -> bool:
        return self.status == 200


class Control:
    def __init__(self) -> None:
        self.http = httpx.Client(timeout=5.0)

    def status(self) -> dict:
        return self.http.get(f"{CONTROL}/status").json()

    def kill(self, instance_id: str) -> None:
        self.http.post(f"{CONTROL}/kill", json={"id": instance_id})

    def hold(self, node: str) -> None:
        self.http.post(f"{CONTROL}/hold", json={"node": node})

    def release(self, node: str) -> None:
        self.http.post(f"{CONTROL}/release", json={"node": node})

    def repair(self, instance_id: str | None = None) -> None:
        self.http.post(f"{CONTROL}/repair", json={} if instance_id is None else {"id": instance_id})

    def shutdown(self) -> None:
        try:
            self.http.post(f"{CONTROL}/shutdown")
        except httpx.HTTPError:
            return


class CampusClient:
    def __init__(self) -> None:
        self.http = httpx.Client(timeout=8.0)

    def call(self, method: str, path: str, body: dict | None = None, headers: dict | None = None, timeout: float = 6.0) -> Call:
        last_error = "no gateway"
        for url in _gateway_urls():
            try:
                response = self.http.request(method, url + path, json=body, headers=headers, timeout=timeout)
            except httpx.ConnectError as exc:
                last_error = str(exc)
                continue
            except httpx.HTTPError as exc:
                return Call(0, None, str(exc))
            parsed = _json(response)
            return Call(response.status_code, parsed, None if response.status_code < 500 else response.text)
        return Call(0, None, last_error)

    def register(self, name: str) -> Call:
        return self.call(
            "POST",
            "/api/register",
            {
                "name": name,
                "program": "Computer Science",
                "idempotency_key": str(uuid.uuid4()),
            },
        )


def _endpoint(row: dict) -> str:
    return str(row.get("host_url") or row["url"])


def _gateway_urls() -> list[str]:
    if not REGISTRY_PATH.exists():
        return []
    payload = json.loads(REGISTRY_PATH.read_text(encoding="utf-8"))
    return [_endpoint(row) for row in payload["instances"] if row["service"] == "gateway"]


def _json(response: httpx.Response):
    try:
        return response.json()
    except json.JSONDecodeError:
        return None


def wipe() -> None:
    ensure_dirs()
    for path in (DATA, LOGS):
        if path.exists():
            shutil.rmtree(path)
        path.mkdir(parents=True)
    clear_events()
    chaos.clear()


def start_supervisor(mode: str) -> subprocess.Popen:
    wipe()
    return subprocess.Popen(
        [sys.executable, "-m", "runner.supervisor", "--mode", mode],
        cwd=str(ROOT),
        start_new_session=True,
    )


def stop_supervisor(process: subprocess.Popen) -> None:
    if process.poll() is not None:
        return
    process.send_signal(signal.SIGTERM)
    try:
        process.wait(timeout=6)
    except subprocess.TimeoutExpired:
        os.kill(process.pid, signal.SIGKILL)
        process.wait(timeout=3)


def wait_ready(control: Control, timeout: float = 25.0) -> None:
    deadline = time.time() + timeout
    last_error = "not started"
    while time.time() < deadline:
        try:
            status = control.status()
        except httpx.HTTPError as exc:
            last_error = str(exc)
            time.sleep(0.15)
            continue
        if not status["instances"] or not all(row["running"] for row in status["instances"]):
            repair = getattr(control, "repair", None)
            if callable(repair):
                for row in status["instances"]:
                    if not row["running"]:
                        repair(row["id"])
            last_error = "a process is still down"
            time.sleep(0.15)
            continue
        try:
            checks = [
                httpx.get(_endpoint(row) + "/health", timeout=0.5)
                for row in status["instances"]
            ]
            gateways = [row for row in status["instances"] if row["service"] == "gateway"]
            boards = [httpx.get(_endpoint(row) + "/api/health", timeout=0.5) for row in gateways]
        except httpx.HTTPError as exc:
            last_error = str(exc)
            time.sleep(0.15)
            continue
        if all(item.status_code == 200 for item in checks) and all(
            item.status_code == 200 and item.json().get("system_up") for item in boards
        ):
            return
        last_error = "health checks have not gone green"
        time.sleep(0.15)
    raise TimeoutError(last_error)


def reset_world(control: Control) -> None:
    chaos.clear()
    control.release("a")
    control.release("b")
    control.repair()
    wait_ready(control)
    status = control.status()
    for row in status["instances"]:
        if row["service"] == "gateway":
            response = httpx.post(_endpoint(row) + "/admin/reset-breakers", timeout=2.0)
        else:
            response = httpx.post(_endpoint(row) + "/admin/reset", timeout=2.0)
        response.raise_for_status()


def instance_view(control: Control, instance_id: str) -> str:
    """Prefer the gateway health board. Fall back to the supervisor if the gateway is dead."""
    seen: list[str] = []
    for url in _gateway_urls():
        try:
            payload = httpx.get(url + "/api/health", timeout=0.5).json()
        except httpx.HTTPError:
            continue
        state = payload.get("instances", {}).get(instance_id)
        if state:
            seen.append(state)
    if "down" in seen:
        return "down"
    if "up" in seen:
        return "up"
    for row in control.status()["instances"]:
        if row["id"] == instance_id:
            return "up" if row["running"] else "down"
    return "unknown"


def system_up(control: Control) -> bool:
    for url in _gateway_urls():
        try:
            if httpx.get(url + "/api/health", timeout=0.5).json().get("system_up"):
                return True
        except httpx.HTTPError:
            continue
    return False


def poll(predicate, timeout: float, interval: float = 0.05):
    deadline = time.time() + timeout
    while time.time() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(interval)
    return None


def _registry_rows() -> list[dict]:
    payload = json.loads(REGISTRY_PATH.read_text(encoding="utf-8"))
    return list(payload["instances"])


def publish_chaos(spec: dict, watch: list[str]) -> bool:
    """Make this injection visible to the service process before the next call.

    A host write onto the Docker Desktop bind mount can show up late, or out of
    order with the previous replace. The copy that matters is written from
    inside a container, then read back by the same process that will handle
    the request. If that process has just crashed, another live container
    writes the file on the shared mount.
    """
    chaos.write(spec)
    rows = _registry_rows()
    watched = [row for row in rows if row["id"] in watch]
    if not watched:
        return False
    containers = [row.get("container") or f"campus-{row['id']}" for row in watched]
    containers.extend(row.get("container") or f"campus-{row['id']}" for row in rows)
    payload = json.dumps(spec)
    wrote = False
    seen_containers: set[str] = set()
    for container in containers:
        if container in seen_containers:
            continue
        seen_containers.add(container)
        result = subprocess.run(
            [
                "docker",
                "exec",
                "-i",
                container,
                "python",
                "-c",
                "import json,sys\nfrom ftuis.chaos import write\nwrite(json.load(sys.stdin))\n",
            ],
            input=payload,
            text=True,
            capture_output=True,
        )
        if result.returncode == 0:
            wrote = True
            break
    if not wrote:
        return False
    deadline = time.time() + 4
    saw_wrong = False
    while time.time() < deadline:
        pending = False
        for row in watched:
            try:
                seen = httpx.get(_endpoint(row) + "/admin/chaos", timeout=1.5).json()
            except httpx.HTTPError:
                pending = True
                continue
            if seen != spec:
                saw_wrong = True
                pending = True
        if not pending:
            return True
        if spec == {} and not saw_wrong:
            return True
        time.sleep(0.05)
    return False


def release_databases(service: str) -> None:
    payload = json.loads(REGISTRY_PATH.read_text(encoding="utf-8"))
    for row in payload["instances"]:
        if row["service"] == service:
            httpx.post(_endpoint(row) + "/admin/release-db", timeout=2.0)


def operator_backup(service: str, dest) -> None:
    source = sqlite3.connect(DATA / service / "primary.db")
    target = sqlite3.connect(dest)
    try:
        source.backup(target)
    finally:
        target.close()
        source.close()


def corrupt_primary(service: str) -> None:
    primary = DATA / service / "primary.db"
    primary.write_bytes(b"this is not a sqlite database")
    for suffix in ("-wal", "-shm"):
        extra = primary.parent / f"primary.db{suffix}"
        if extra.exists():
            extra.unlink()


def restore_primary(service: str, backup) -> None:
    primary = DATA / service / "primary.db"
    for suffix in ("", "-wal", "-shm"):
        extra = primary.parent / f"primary.db{suffix}"
        if extra.exists():
            extra.unlink()
    shutil.copy(backup, primary)


def count_events(since: float, name: str) -> int:
    return sum(1 for row in read_events(since) if row["event"] == name)


def wait_for_event(since: float, name: str, timeout: float = 2.0, **matches):
    deadline = time.time() + timeout
    while True:
        found = first_event(since, name, **matches)
        if found is not None or time.time() >= deadline:
            return found
        time.sleep(0.05)


def first_event(since: float, name: str, **matches):
    for row in read_events(since):
        if row["event"] != name:
            continue
        if all(row.get(key) == value for key, value in matches.items()):
            return row
    return None


def blank(experiment: str, mode: str, failure: str) -> dict:
    return {
        "experiment": experiment,
        "mode": mode,
        "failure": failure,
        "detection_time_s": None,
        "recovery_time_s": None,
        "failed_requests": 0,
        "successful_requests": 0,
        "recovered_requests": 0,
        "consistency": None,
        "consistency_issues": [],
        "notes": "",
        "extra": {},
    }


def finish(row: dict, started: float) -> dict:
    report = audit()
    row["consistency"] = "pass" if report["ok"] else "fail"
    row["consistency_issues"] = report["issues"]
    row["counts"] = report["counts"]
    row["recovered_requests"] = count_events(started, "request_recovered")
    return row


@dataclass
class World:
    mode: str
    control: Control
    client: CampusClient = field(default_factory=CampusClient)

    @property
    def ft(self) -> bool:
        return self.mode == "ft"
