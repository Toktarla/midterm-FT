"""Starts the campus processes and restarts them in fault-tolerant mode.

Baseline mode leaves a dead process down until /repair. That is the operator.
Fault-tolerant mode starts a second logical node and restarts anything that
exits, unless the experiment is holding that node down on purpose.
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import subprocess
import sys
import threading
import time
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from ftuis.config import LOGS, REGISTRY_PATH, ROOT, RUNTIME, ensure_dirs
from ftuis.events import emit
from ftuis.topology import CONTROL_PORT, MODULES, PORTS, SERVICES

PID_PATH = RUNTIME / "pids.json"


@dataclass
class Instance:
    id: str
    service: str
    node: str
    port: int
    module: str
    proc: subprocess.Popen | None = None
    log: object | None = None


@dataclass
class Supervisor:
    mode: str
    instances: dict[str, Instance] = field(default_factory=dict)
    held_nodes: set[str] = field(default_factory=set)
    lock: threading.RLock = field(default_factory=threading.RLock)
    stop_event: threading.Event = field(default_factory=threading.Event)
    httpd: ThreadingHTTPServer | None = None

    @property
    def ft(self) -> bool:
        return self.mode == "ft"

    def plan(self) -> None:
        nodes = ("a", "b") if self.ft else ("a",)
        for service in SERVICES:
            for node in nodes:
                self.instances[f"{service}-{node}"] = Instance(
                    id=f"{service}-{node}",
                    service=service,
                    node=node,
                    port=PORTS[(service, node)],
                    module=MODULES[service],
                )

    def reap_stale(self) -> None:
        if not PID_PATH.exists():
            return
        try:
            pids = json.loads(PID_PATH.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return
        for pid in pids:
            if pid == os.getpid():
                continue
            try:
                os.kill(int(pid), signal.SIGKILL)
            except (ProcessLookupError, PermissionError, ValueError):
                continue
        time.sleep(0.2)

    def spawn_all(self) -> None:
        with self.lock:
            for instance in self.instances.values():
                self.spawn(instance)

    def spawn(self, instance: Instance) -> None:
        if instance.proc is not None and instance.proc.poll() is None:
            return
        if instance.log is not None:
            instance.log.close()
            instance.log = None
        ensure_dirs()
        log_path = LOGS / f"{instance.id}.log"
        handle = log_path.open("a", encoding="utf-8")
        env = os.environ.copy()
        env.update(
            {
                "FT_MODE": self.mode,
                "INSTANCE_ID": instance.id,
                "NODE_ID": instance.node,
                "SERVICE": instance.service,
                "PYTHONUNBUFFERED": "1",
            }
        )
        instance.proc = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "uvicorn",
                instance.module,
                "--host",
                "127.0.0.1",
                "--port",
                str(instance.port),
                "--log-level",
                "warning",
            ],
            cwd=str(ROOT),
            env=env,
            stdout=handle,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        instance.log = handle
        emit("instance_started", instance=instance.id, pid=instance.proc.pid, node=instance.node)
        self.write_registry()

    def write_registry(self) -> None:
        payload = {
            "mode": self.mode,
            "instances": [
                {
                    "id": instance.id,
                    "service": instance.service,
                    "node": instance.node,
                    "url": f"http://127.0.0.1:{instance.port}",
                }
                for instance in self.instances.values()
            ],
        }
        temporary = REGISTRY_PATH.with_suffix(".tmp")
        temporary.write_text(json.dumps(payload), encoding="utf-8")
        os.replace(temporary, REGISTRY_PATH)
        pids = [os.getpid()]
        for instance in self.instances.values():
            if instance.proc is not None and instance.proc.poll() is None:
                pids.append(instance.proc.pid)
        PID_PATH.write_text(json.dumps(pids), encoding="utf-8")

    def running(self, instance: Instance) -> bool:
        return instance.proc is not None and instance.proc.poll() is None

    def should_restart(self, instance: Instance) -> bool:
        return self.ft and instance.node not in self.held_nodes

    def signal(self, instance: Instance, sig: int) -> None:
        if not self.running(instance):
            return
        assert instance.proc is not None
        try:
            os.killpg(instance.proc.pid, sig)
        except ProcessLookupError:
            return

    def watch(self) -> None:
        while not self.stop_event.is_set():
            time.sleep(0.2)
            with self.lock:
                for instance in self.instances.values():
                    if instance.proc is None:
                        continue
                    if instance.proc.poll() is None:
                        continue
                    code = instance.proc.returncode
                    emit("instance_exit", instance=instance.id, code=code, node=instance.node)
                    instance.proc = None
                    if self.should_restart(instance):
                        self.spawn(instance)
                        emit("instance_restarted", instance=instance.id, node=instance.node)

    def kill(self, instance_id: str) -> None:
        with self.lock:
            self.signal(self.instances[instance_id], signal.SIGKILL)

    def hold(self, node: str) -> None:
        with self.lock:
            self.held_nodes.add(node)
            for instance in self.instances.values():
                if instance.node == node:
                    self.signal(instance, signal.SIGKILL)

    def release(self, node: str) -> None:
        with self.lock:
            self.held_nodes.discard(node)
            for instance in self.instances.values():
                if instance.node == node and not self.running(instance):
                    self.spawn(instance)

    def repair(self, instance_id: str | None = None) -> None:
        with self.lock:
            chosen = self.instances.values() if instance_id is None else [self.instances[instance_id]]
            for instance in chosen:
                if not self.running(instance):
                    self.spawn(instance)

    def status(self) -> dict[str, object]:
        with self.lock:
            return {
                "mode": self.mode,
                "held_nodes": sorted(self.held_nodes),
                "instances": [
                    {
                        "id": instance.id,
                        "service": instance.service,
                        "node": instance.node,
                        "url": f"http://127.0.0.1:{instance.port}",
                        "pid": instance.proc.pid if self.running(instance) else None,
                        "running": self.running(instance),
                    }
                    for instance in self.instances.values()
                ],
            }

    def stop_children(self) -> None:
        with self.lock:
            for instance in self.instances.values():
                self.signal(instance, signal.SIGTERM)
            for instance in self.instances.values():
                if instance.proc is None:
                    continue
                try:
                    instance.proc.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    self.signal(instance, signal.SIGKILL)

    def serve(self) -> None:
        supervisor = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, _format: str, *_args) -> None:
                return

            def _send(self, code: int, payload: dict) -> None:
                raw = json.dumps(payload).encode()
                self.send_response(code)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(raw)))
                self.end_headers()
                self.wfile.write(raw)

            def do_GET(self) -> None:  # noqa: N802
                if self.path == "/status":
                    self._send(200, supervisor.status())
                else:
                    self._send(404, {"error": "not found"})

            def do_POST(self) -> None:  # noqa: N802
                length = int(self.headers.get("Content-Length", "0"))
                raw = self.rfile.read(length) if length else b"{}"
                try:
                    body = json.loads(raw.decode() or "{}")
                except json.JSONDecodeError:
                    self._send(400, {"error": "invalid json"})
                    return
                path = self.path
                try:
                    if path == "/kill":
                        supervisor.kill(body["id"])
                    elif path == "/hold":
                        supervisor.hold(body["node"])
                    elif path == "/release":
                        supervisor.release(body["node"])
                    elif path == "/repair":
                        supervisor.repair(body.get("id"))
                    elif path == "/shutdown":
                        self._send(200, {"ok": True})
                        threading.Thread(target=supervisor.stop, daemon=True).start()
                        return
                    else:
                        self._send(404, {"error": "not found"})
                        return
                except KeyError as exc:
                    self._send(404, {"error": f"unknown instance or field {exc}"})
                    return
                self._send(200, {"ok": True})

        self.httpd = ThreadingHTTPServer(("127.0.0.1", CONTROL_PORT), Handler)
        self.httpd.serve_forever()

    def stop(self) -> None:
        if self.stop_event.is_set():
            return
        self.stop_event.set()
        self.stop_children()
        if self.httpd is not None:
            self.httpd.shutdown()


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the campus process supervisor")
    parser.add_argument("--mode", choices=("baseline", "ft"), default="ft")
    args = parser.parse_args()
    ensure_dirs()
    os.environ["FT_MODE"] = args.mode
    os.environ["INSTANCE_ID"] = "supervisor"
    os.environ["SERVICE"] = "control"
    supervisor = Supervisor(mode=args.mode)
    supervisor.reap_stale()
    supervisor.plan()
    supervisor.spawn_all()
    threading.Thread(target=supervisor.watch, daemon=True).start()

    def _handle(_signum, _frame) -> None:
        threading.Thread(target=supervisor.stop, daemon=True).start()

    signal.signal(signal.SIGTERM, _handle)
    signal.signal(signal.SIGINT, _handle)
    gateway = 8001
    print(f"campus supervisor mode={args.mode} control=http://127.0.0.1:{CONTROL_PORT} gateway=http://127.0.0.1:{gateway}", flush=True)
    try:
        supervisor.serve()
    finally:
        supervisor.stop_children()


if __name__ == "__main__":
    main()
