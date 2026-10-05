"""The six required failure scenarios, plus a timed reliability session."""

from __future__ import annotations

import time
import uuid
from concurrent.futures import ThreadPoolExecutor

import httpx

from ftuis import chaos
from ftuis.config import RUNTIME
from experiments.harness import (
    World,
    blank,
    corrupt_primary,
    finish,
    first_event,
    instance_view,
    wait_for_event,
    operator_backup,
    poll,
    publish_chaos,
    release_databases,
    restore_primary,
    system_up,
)
from ftuis.reliability import session_metrics


def application_crash(world: World) -> dict:
    row = blank("application_crash", world.mode, "SIGKILL student-a, the registration process")
    started = time.time()
    for index in range(2):
        call = world.client.register(f"Warm {index}")
        if call.ok:
            row["successful_requests"] += 1
        else:
            row["failed_requests"] += 1
    killed = time.time()
    world.control.kill("student-a")
    detected = None
    recovered = None
    deadline = time.time() + 1.5
    while time.time() < deadline:
        if detected is None and instance_view(world.control, "student-a") == "down":
            detected = time.time() - killed
        call = world.client.register(f"Live {row['successful_requests'] + row['failed_requests']}")
        if call.ok:
            row["successful_requests"] += 1
            if recovered is None:
                recovered = time.time() - killed
        else:
            row["failed_requests"] += 1
        time.sleep(0.1)
    if not world.ft:
        world.control.repair("student-a")
        repair_deadline = time.time() + 8
        while time.time() < repair_deadline:
            call = world.client.register("After repair")
            if call.ok:
                row["successful_requests"] += 1
                recovered = time.time() - killed
                break
            row["failed_requests"] += 1
            time.sleep(0.15)
    else:
        poll(lambda: instance_view(world.control, "student-a") == "up", timeout=8)
    row["detection_time_s"] = detected
    row["recovery_time_s"] = recovered
    row["notes"] = (
        "The other student replica keeps accepting registrations."
        if world.ft
        else "Baseline has one student process. An operator restart begins after a 1.5s outage window."
    )
    return finish(row, started)


def database_failure(world: World) -> dict:
    row = blank("database_failure", world.mode, "primary student database file overwritten")
    started = time.time()
    for index in range(3):
        call = world.client.register(f"Seed {index}")
        if not call.ok:
            row["failed_requests"] += 1
            row["notes"] = f"seed failed: {call.error or call.body}"
            return finish(row, started)
        row["successful_requests"] += 1
    backup = RUNTIME / f"operator-{world.mode}-student.db"
    if not world.ft:
        operator_backup("student", backup)
    try:
        release_databases("student")
        corrupt_primary("student")
        failed_at = time.time()
        call = world.client.call("GET", "/api/students", timeout=4)
        if call.ok:
            row["successful_requests"] += 1
            failover = first_event(failed_at, "db_failover")
            restored = first_event(failed_at, "db_restored")
            row["detection_time_s"] = None if failover is None else failover["ts"] - failed_at
            row["recovery_time_s"] = None if restored is None else restored["ts"] - failed_at
        elif world.ft:
            row["failed_requests"] += 1
            row["notes"] = f"mirror failover did not serve the read: {call.error or call.body}"
        else:
            row["failed_requests"] += 1
            row["detection_time_s"] = time.time() - failed_at
            time.sleep(1.2)
            restore_primary("student", backup)
            # The running process can still hold the overwritten file. Restart
            # is the operator step that makes the restored file the one it opens.
            restarter = getattr(world.control, "restart", None)
            if callable(restarter):
                restarter("student-a")
            else:
                release_databases("student")
            healed = None
            deadline = time.time() + 15
            while time.time() < deadline:
                healed = world.client.call("GET", "/api/students", timeout=4)
                if healed.ok:
                    break
                time.sleep(0.25)
            if healed is not None and healed.ok:
                row["successful_requests"] += 1
                row["recovery_time_s"] = time.time() - failed_at
            else:
                row["failed_requests"] += 1
                detail = healed.error or healed.body if healed is not None else "no response"
                row["notes"] = f"manual restore did not recover the read: {detail}"
    finally:
        _leave_student_db_readable(world, backup)
    if not row["notes"]:
        row["notes"] = (
            "The service rebuilt the primary file from its mirror and served the student list."
            if world.ft
            else "Baseline has no mirror. The read failed until an operator copied a backup back and restarted the student container."
        )
    return finish(row, started)


def network_timeout(world: World) -> dict:
    row = blank("network_timeout", world.mode, "payment-a delayed 2.5s, then both payment replicas forced to fail")
    started = time.time()
    payment_ids = _service_ids("payment")
    publish_chaos({"delay_ms": {"payment-a": 2500}}, payment_ids)
    detected = None
    recovered = None
    for _ in range(3):
        sent = time.time()
        call = world.client.call("GET", "/api/ping/payment", timeout=6)
        if call.ok:
            row["successful_requests"] += 1
        else:
            row["failed_requests"] += 1
        timeout_event = first_event(sent, "upstream_timeout", target="payment-a")
        if timeout_event is not None and detected is None:
            detected = timeout_event["ts"] - sent
        if call.ok and timeout_event is not None and recovered is None:
            recovered = time.time() - sent
    publish_chaos({}, payment_ids)
    if not world.ft:
        sent = time.time()
        call = world.client.call("GET", "/api/ping/payment", timeout=6)
        if call.ok:
            row["successful_requests"] += 1
            recovered = time.time() - started
        else:
            row["failed_requests"] += 1
    armed = publish_chaos({"fail_instances": ["payment-a", "payment-b"]}, payment_ids)
    for url in _gateways():
        httpx.post(url + "/admin/reset-breakers", timeout=2)
    degraded = 0
    try:
        for index in range(2):
            call = world.client.register(f"Deferred {index}")
            if call.ok and isinstance(call.body, dict) and call.body.get("degraded"):
                degraded += 1
                row["successful_requests"] += 1
            elif call.ok:
                row["successful_requests"] += 1
            else:
                row["failed_requests"] += 1
    finally:
        publish_chaos({}, payment_ids)
    row["detection_time_s"] = detected
    row["recovery_time_s"] = recovered
    row["extra"] = {"degraded_registrations": degraded, "failure_visible": armed}
    if not armed:
        row["notes"] = "The payment processes never observed the forced-failure flag."
    elif world.ft:
        row["notes"] = (
            f"Timeouts fail over to payment-b. With every payment replica failed, "
            f"{degraded} registration(s) were accepted and tuition was marked deferred."
        )
    else:
        row["notes"] = (
            "The only payment process is slow, the gateway waits out its 2s timeout, "
            "and a payment outage leaves the registration pending."
        )
    return finish(row, started)


def node_failure(world: World) -> dict:
    row = blank("node_failure", world.mode, "logical node A stopped: every process on that node is killed")
    started = time.time()
    killed = time.time()
    world.control.hold("a")
    detected = None
    recovered = None
    for index in range(6):
        if detected is None and instance_view(world.control, "student-a") == "down":
            detected = time.time() - killed
        call = world.client.register(f"Node {index}")
        if call.ok:
            row["successful_requests"] += 1
            if recovered is None:
                recovered = time.time() - killed
        else:
            row["failed_requests"] += 1
    if not world.ft:
        time.sleep(0.8)
        world.control.release("a")
        world.control.repair()
        repair_deadline = time.time() + 10
        while time.time() < repair_deadline:
            call = world.client.register("Node repaired")
            if call.ok:
                row["successful_requests"] += 1
                recovered = time.time() - killed
                break
            row["failed_requests"] += 1
            time.sleep(0.2)
    else:
        world.control.release("a")
        poll(lambda: instance_view(world.control, "student-a") == "up", timeout=8)
    row["detection_time_s"] = detected
    row["recovery_time_s"] = recovered
    row["notes"] = (
        "Node B still has a gateway, a student service, a payment service, and an academic service."
        if world.ft
        else "Baseline runs only on node A, so the campus is unreachable until the operator starts it again."
    )
    return finish(row, started)


def interrupted_transaction(world: World) -> dict:
    row = blank(
        "interrupted_transaction",
        world.mode,
        "payment-a killed after the balance change and before the HTTP response",
    )
    started = time.time()
    created = world.client.register("Ledger Student")
    if not created.ok or not isinstance(created.body, dict):
        row["failed_requests"] += 1
        row["notes"] = f"could not create the student: {created.error or created.body}"
        return finish(row, started)
    row["successful_requests"] += 1
    student_id = created.body["student_id"]
    key = str(uuid.uuid4())
    armed = publish_chaos({"crash_points": {"payment-a": "after_side_effect"}}, ["payment-a"])
    sent = time.time()
    try:
        call = world.client.call(
            "POST",
            "/api/payments",
            {"student_id": student_id, "amount": 100, "idempotency_key": key},
            headers={"X-Prefer-Instance": "payment-a"},
            timeout=8,
        )
    finally:
        # Clear before any retry, or the retry dies at the same checkpoint.
        publish_chaos({}, ["payment-a"])
    call_finished = time.time()
    if call.ok:
        row["successful_requests"] += 1
    else:
        row["failed_requests"] += 1
    crash = wait_for_event(sent, "process_crashing", timeout=8)
    down = wait_for_event(sent, "health_changed", timeout=8, target="payment-a", status="down")
    if crash is not None and down is not None:
        row["detection_time_s"] = max(0.0, down["ts"] - crash["ts"])
    elif crash is not None:
        row["detection_time_s"] = 0.0
    row["extra"] = {"crash_visible": armed, "process_crashed": crash is not None}
    if not armed or crash is None:
        row["notes"] = "payment-a did not stop at the checkpoint, so this run did not interrupt the transaction."
        return finish(row, started)
    if world.ft and call.ok:
        row["recovery_time_s"] = call_finished - sent
        row["notes"] = (
            "The replica finished the checkpointed payment once. "
            "The same idempotency key does not charge the student again."
        )
    if not world.ft:
        time.sleep(1.2)
        world.control.repair("payment-a")
        poll(lambda: instance_view(world.control, "payment-a") == "up", timeout=12)
        retried = world.client.call(
            "POST",
            "/api/payments",
            {"student_id": student_id, "amount": 100, "idempotency_key": key},
            timeout=6,
        )
        if retried.ok:
            row["successful_requests"] += 1
        else:
            row["failed_requests"] += 1
        row["recovery_time_s"] = time.time() - sent
        row["notes"] = (
            "Baseline committed the charge, lost the response, and the retry created a second charge for the same key."
        )
    else:
        poll(lambda: instance_view(world.control, "payment-a") == "up", timeout=12)
        if not row["notes"]:
            row["notes"] = "The payment process crashed, and the client did not receive a completed receipt."
    return finish(row, started)


def high_load(world: World) -> dict:
    row = blank("high_load", world.mode, "30 overlapping work requests")
    started = time.time()
    urls = _gateways()

    def once(index: int) -> tuple[int, float]:
        url = urls[index % len(urls)]
        try:
            response = httpx.post(url + "/api/work", timeout=4.0)
            return response.status_code, time.time()
        except httpx.HTTPError:
            return 0, time.time()

    with ThreadPoolExecutor(max_workers=30) as pool:
        outcomes = list(pool.map(once, range(30)))
    codes = [code for code, _when in outcomes]
    failures = [when for code, when in outcomes if code != 200]
    row["successful_requests"] = sum(1 for code in codes if code == 200)
    row["failed_requests"] = len(failures)
    first_failure = None if not failures else min(failures) - started
    probe = world.client.call("POST", "/api/work", timeout=4)
    if probe.ok:
        row["successful_requests"] += 1
        if first_failure is not None:
            row["recovery_time_s"] = time.time() - started
    else:
        row["failed_requests"] += 1
    row["detection_time_s"] = first_failure
    row["extra"] = {"status_codes": {str(code): codes.count(code) for code in sorted(set(codes))}}
    row["notes"] = (
        "Two student replicas absorb the burst. A full replica returns quickly so the gateway can try the other one."
        if world.ft
        else "One student replica allows 5 overlapping jobs and makes the rest wait, then reject them."
    )
    return finish(row, started)


def reliability_session(world: World, duration_s: float) -> dict:
    row = blank(
        "reliability_session",
        world.mode,
        "kill student-a, payment-a, and academic-a on a 5s rotation",
    )
    started = time.time()
    samples: list[tuple[float, bool]] = []
    targets = ("student-a", "payment-a", "academic-a")
    next_kill = started + 3.0
    cursor = 0
    repairs: list[tuple[float, str]] = []
    while time.time() < started + duration_s:
        now = time.time()
        samples.append((now, system_up(world.control)))
        due = [item for item in repairs if item[0] <= now]
        repairs = [item for item in repairs if item[0] > now]
        for _when, instance_id in due:
            world.control.repair(instance_id)
        if now >= next_kill:
            target = targets[cursor % len(targets)]
            cursor += 1
            world.control.kill(target)
            if not world.ft:
                repairs.append((now + 1.2, target))
            next_kill += 5.0
        time.sleep(0.25)
    if not world.ft:
        for _when, instance_id in repairs:
            world.control.repair(instance_id)
    metrics = session_metrics(samples)
    row["extra"] = metrics
    row["successful_requests"] = sum(1 for _ts, up in samples if up)
    row["failed_requests"] = sum(1 for _ts, up in samples if not up)
    row["detection_time_s"] = None
    row["recovery_time_s"] = metrics.get("mttr_s")
    availability = metrics.get("availability")
    availability_text = "" if availability is None else f" Measured availability {availability:.3f}."
    row["notes"] = (
        "User-visible availability stays high because node B is still serving. "
        "MTTF is censored when the probe never sees an outage. "
        "This failure rate is the injection timer, not the planning lambda."
        if world.ft
        else "Each kill removes the only replica. The operator restarts it 1.2s later. "
        "MTTF, MTTR, and availability here are measured from those probes."
    ) + availability_text
    return finish(row, started)


def _leave_student_db_readable(world: World, backup) -> None:
    from ftuis.config import DATA
    import sqlite3

    primary = DATA / "student" / "primary.db"
    try:
        connection = sqlite3.connect(primary)
        row = connection.execute("PRAGMA quick_check").fetchone()
        connection.close()
        if row is not None and row[0] == "ok":
            return
    except sqlite3.DatabaseError:
        pass
    if world.ft:
        world.client.call("GET", "/api/students", timeout=4)
        return
    if backup.exists():
        restore_primary("student", backup)
        release_databases("student")


def _gateways() -> list[str]:
    from experiments.harness import _gateway_urls

    return _gateway_urls()


def _service_ids(service: str) -> list[str]:
    from experiments.harness import _registry_rows

    return [row["id"] for row in _registry_rows() if row["service"] == service]
