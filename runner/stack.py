"""Docker Compose is the orchestrator for the measured system.

Baseline containers are started with restart policy "no". A crash stays down
until the experiment starts the container again.

Fault-tolerant containers are switched to "unless-stopped" after they are
up. Docker then restarts a container whose process exits or is killed.
Stopping a container on purpose, which is how a whole logical node is held
down, does not restart it.
"""

from __future__ import annotations

import json
import os
import subprocess

from ftuis.config import REGISTRY_PATH, ROOT, ensure_dirs
from ftuis.events import emit
from ftuis.topology import PORTS, SERVICES


def _env(mode: str) -> dict[str, str]:
    env = os.environ.copy()
    env["FT_MODE"] = mode
    env["HOST_UID"] = str(os.getuid())
    env["HOST_GID"] = str(os.getgid())
    return env


def compose(args: list[str], mode: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        ["docker", "compose", "-p", "campus", *args],
        cwd=str(ROOT),
        env=_env(mode),
        text=True,
        capture_output=True,
    )
    if check and result.returncode != 0:
        detail = (result.stderr or result.stdout or "docker compose failed").strip()
        raise RuntimeError(detail)
    return result


def docker(args: list[str], check: bool = True) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(["docker", *args], text=True, capture_output=True)
    if check and result.returncode != 0:
        detail = (result.stderr or result.stdout or "docker failed").strip()
        raise RuntimeError(detail)
    return result


def planned(mode: str) -> list[dict[str, str]]:
    nodes = ("a", "b") if mode == "ft" else ("a",)
    rows = []
    for service in SERVICES:
        for node in nodes:
            port = PORTS[(service, node)]
            identity = f"{service}-{node}"
            rows.append(
                {
                    "id": identity,
                    "service": service,
                    "node": node,
                    "url": f"http://{identity}:{port}",
                    "host_url": f"http://127.0.0.1:{port}",
                    "container": f"campus-{identity}",
                }
            )
    return rows


def write_registry(mode: str) -> None:
    ensure_dirs()
    payload = {"mode": mode, "runtime": "docker-compose", "instances": planned(mode)}
    temporary = REGISTRY_PATH.with_suffix(".tmp")
    temporary.write_text(json.dumps(payload), encoding="utf-8")
    os.replace(temporary, REGISTRY_PATH)


def start_stack(mode: str) -> None:
    if docker(["info"], check=False).returncode != 0:
        raise RuntimeError("Docker is not running. Start Docker Desktop, then run the experiments again.")
    write_registry(mode)
    compose(["--profile", "baseline", "--profile", "ft", "down", "--remove-orphans"], mode, check=False)
    compose(["--profile", mode, "up", "-d", "--build"], mode)
    _wait_until_running(mode)
    emit("stack_started", instance="docker", mode=mode, runtime="docker-compose")


def _wait_until_running(mode: str, timeout: float = 40.0) -> None:
    import time

    deadline = time.time() + timeout
    last = "containers have not stayed up"
    while time.time() < deadline:
        problems: list[str] = []
        for row in planned(mode):
            inspected = docker(
                ["inspect", "-f", "{{.State.Status}}", row["container"]],
                check=False,
            )
            status = inspected.stdout.strip() or "missing"
            if status == "running":
                if mode == "ft":
                    # Arm restart as soon as the container is up, so a crash
                    # during the rest of startup is brought back by Docker.
                    docker(["update", "--restart=unless-stopped", row["container"]], check=False)
                continue
            logs = docker(["logs", "--tail", "30", row["container"]], check=False)
            detail = (logs.stderr or logs.stdout or "").strip()
            problems.append(f"{row['container']} is {status}\n{detail}")
        if not problems:
            return
        exited = [item for item in problems if " is exited" in item]
        if exited and time.time() + 8 < deadline:
            # A container can be reported exited for a moment while Compose replaces it.
            last = "\n".join(problems)
            time.sleep(0.5)
            continue
        if exited:
            raise RuntimeError("\n".join(problems))
        last = "\n".join(problems)
        time.sleep(0.4)
    raise RuntimeError(last)


def stop_stack() -> None:
    compose(["--profile", "baseline", "--profile", "ft", "down", "--remove-orphans"], "ft", check=False)


class DockerControl:
    def __init__(self, mode: str) -> None:
        self.mode = mode
        self.ft = mode == "ft"

    def status(self) -> dict[str, object]:
        instances = []
        for row in planned(self.mode):
            inspected = docker(["inspect", "-f", "{{.State.Running}}", row["container"]], check=False)
            running = inspected.stdout.strip() == "true"
            instances.append(
                {
                    "id": row["id"],
                    "service": row["service"],
                    "node": row["node"],
                    "url": row["host_url"],
                    "host_url": row["host_url"],
                    "running": running,
                }
            )
        return {"mode": self.mode, "runtime": "docker-compose", "instances": instances}

    def kill(self, instance_id: str) -> None:
        container = f"campus-{instance_id}"
        if not self.ft:
            docker(["update", "--restart=no", container], check=False)
        docker(["kill", container])
        emit("orchestrator_kill", instance=instance_id, restart=self.ft)

    def hold(self, node: str) -> None:
        for row in planned(self.mode):
            if row["node"] != node:
                continue
            docker(["update", "--restart=no", row["container"]], check=False)
            docker(["stop", "-t", "1", row["container"]], check=False)
        emit("orchestrator_hold", instance=f"node-{node}", node=node)

    def release(self, node: str) -> None:
        for row in planned(self.mode):
            if row["node"] != node:
                continue
            if self.ft:
                docker(["update", "--restart=unless-stopped", row["container"]], check=False)
            docker(["start", row["container"]], check=False)

    def repair(self, instance_id: str | None = None) -> None:
        rows = planned(self.mode)
        if instance_id is not None:
            rows = [row for row in rows if row["id"] == instance_id]
        for row in rows:
            docker(["start", row["container"]], check=False)

    def restart(self, instance_id: str) -> None:
        # Drops a process that still holds the database file we just replaced.
        docker(["restart", f"campus-{instance_id}"])
        emit("orchestrator_restart", instance=instance_id)

    def shutdown(self) -> None:
        stop_stack()
