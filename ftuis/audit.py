"""Read the service databases and report consistency problems."""

from __future__ import annotations

import sqlite3
from collections import Counter, defaultdict

from ftuis.config import DATA


def _rows(service: str, sql: str) -> list[dict[str, object]]:
    path = DATA / service / "primary.db"
    if not path.exists():
        raise sqlite3.DatabaseError(f"{service} database is missing")
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    try:
        return [dict(row) for row in conn.execute(sql).fetchall()]
    finally:
        conn.close()


def audit() -> dict[str, object]:
    issues: list[str] = []
    counts: dict[str, int] = {}
    try:
        students = _rows(
            "student",
            "SELECT id, registration_status, tuition_status, idempotency_key FROM students",
        )
        payments = _rows(
            "payment",
            "SELECT id, student_id, amount, idempotency_key, step FROM payments",
        )
        accounts = _rows("payment", "SELECT student_id, balance FROM accounts")
        transcripts = _rows(
            "academic",
            "SELECT student_id, course, grade FROM transcripts",
        )
    except sqlite3.DatabaseError as exc:
        return {"ok": False, "issues": [f"database unreadable: {exc}"], "counts": {}}

    counts = {
        "students": len(students),
        "payments": len(payments),
        "committed_payments": sum(1 for row in payments if row["step"] == "committed"),
        "transcripts": len(transcripts),
    }

    key_counts = Counter(
        str(row["idempotency_key"]) for row in payments if row["idempotency_key"]
    )
    for key, count in key_counts.items():
        if count > 1:
            issues.append(f"duplicate payment idempotency key {key} appeared {count} times")

    committed: dict[str, float] = defaultdict(float)
    for row in payments:
        if row["step"] == "committed":
            committed[str(row["student_id"])] += float(row["amount"])
        elif row["step"] in {"prepared", "applied"}:
            issues.append(f"payment {row['id']} stuck in step {row['step']}")
    balances = {str(row["student_id"]): float(row["balance"]) for row in accounts}
    for student_id in set(committed) | set(balances):
        if abs(committed.get(student_id, 0.0) - balances.get(student_id, 0.0)) > 0.01:
            issues.append(
                f"balance mismatch for {student_id}: "
                f"account={balances.get(student_id, 0.0)} committed={committed.get(student_id, 0.0)}"
            )

    courses_for: dict[str, int] = Counter(str(row["student_id"]) for row in transcripts)
    for student in students:
        status = str(student["registration_status"])
        student_id = str(student["id"])
        if status == "pending":
            issues.append(f"registration {student_id} left pending")
        if status in {"active", "deferred"} and courses_for[student_id] < 3:
            issues.append(f"registration {student_id} is missing transcript rows")
        if status == "active":
            paid = [
                row
                for row in payments
                if row["student_id"] == student_id and row["step"] == "committed"
            ]
            if not paid:
                issues.append(f"active student {student_id} has no committed payment")

    return {"ok": not issues, "issues": issues, "counts": counts}
