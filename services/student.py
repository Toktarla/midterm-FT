"""Student registration service."""

from __future__ import annotations

import asyncio
import time
import uuid

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from ftuis.config import load_settings
from ftuis.events import emit
from ftuis.storage import Store
from services.runtime import install

settings = load_settings()
store = Store(
    "student",
    """
    CREATE TABLE IF NOT EXISTS students (
        id TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        program TEXT NOT NULL,
        registration_status TEXT NOT NULL,
        tuition_status TEXT NOT NULL,
        idempotency_key TEXT UNIQUE,
        created_at REAL NOT NULL
    );
    """,
    mirrored=settings.ft,
)

_slots: asyncio.Semaphore | None = None


def _slots_for() -> asyncio.Semaphore:
    global _slots
    if _slots is None:
        _slots = asyncio.Semaphore(settings.work_slots)
    return _slots


def _reset() -> None:
    store.write(lambda conn: conn.execute("DELETE FROM students"))


app = FastAPI(title="Student service")
install(app, settings, store, _reset)


class StudentIn(BaseModel):
    name: str = Field(min_length=1)
    program: str = Field(min_length=1)
    idempotency_key: str = Field(min_length=1)


class TuitionIn(BaseModel):
    status: str = Field(pattern="^(unpaid|paid|deferred)$")
    registration_status: str = Field(default="", pattern="^(|pending|active|deferred)$")


def _public(row) -> dict[str, object]:
    return dict(row)


@app.post("/students")
def create_student(body: StudentIn) -> dict[str, object]:
    def _run(conn):
        existing = conn.execute(
            "SELECT * FROM students WHERE idempotency_key=?",
            (body.idempotency_key,),
        ).fetchone()
        if existing:
            emit("duplicate_detected", key=body.idempotency_key)
            return _public(existing)
        student_id = str(uuid.uuid4())
        conn.execute(
            """
            INSERT INTO students
                (id, name, program, registration_status, tuition_status, idempotency_key, created_at)
            VALUES (?, ?, ?, 'pending', 'unpaid', ?, ?)
            """,
            (student_id, body.name, body.program, body.idempotency_key, time.time()),
        )
        return _public(conn.execute("SELECT * FROM students WHERE id=?", (student_id,)).fetchone())

    return store.write(_run)


@app.get("/students")
def list_students() -> dict[str, object]:
    def _run(conn):
        rows = conn.execute("SELECT * FROM students ORDER BY created_at").fetchall()
        return [dict(row) for row in rows]

    return {"students": store.write(_run)}


@app.get("/students/{student_id}")
def get_student(student_id: str) -> dict[str, object]:
    def _run(conn):
        return conn.execute("SELECT * FROM students WHERE id=?", (student_id,)).fetchone()

    row = store.write(_run)
    if row is None:
        raise HTTPException(status_code=404, detail="student not found")
    return dict(row)


@app.post("/students/{student_id}/tuition")
def set_tuition(student_id: str, body: TuitionIn) -> dict[str, object]:
    registration = body.registration_status or (
        "active" if body.status == "paid" else "deferred" if body.status == "deferred" else "pending"
    )

    def _run(conn):
        cursor = conn.execute(
            """
            UPDATE students
            SET tuition_status=?, registration_status=?
            WHERE id=?
            """,
            (body.status, registration, student_id),
        )
        if cursor.rowcount != 1:
            return None
        return dict(conn.execute("SELECT * FROM students WHERE id=?", (student_id,)).fetchone())

    row = store.write(_run)
    if row is None:
        raise HTTPException(status_code=404, detail="student not found")
    return row


@app.post("/work")
async def work() -> dict[str, object]:
    try:
        await asyncio.wait_for(_slots_for().acquire(), timeout=settings.work_wait_s)
    except TimeoutError:
        emit("overloaded")
        raise HTTPException(status_code=503, detail="busy") from None
    try:
        await asyncio.sleep(settings.work_duration_s)
        return {"ok": True, "instance": settings.instance_id}
    finally:
        _slots_for().release()
