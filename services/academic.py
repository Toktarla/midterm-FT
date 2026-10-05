"""Transcripts and timetable generation."""

from __future__ import annotations

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from ftuis.config import COURSES, load_settings
from ftuis.events import emit
from ftuis.storage import Store
from services.runtime import install

settings = load_settings()
store = Store(
    "academic",
    """
    CREATE TABLE IF NOT EXISTS transcripts (
        student_id TEXT NOT NULL,
        course TEXT NOT NULL,
        grade TEXT,
        PRIMARY KEY (student_id, course)
    );
    CREATE TABLE IF NOT EXISTS timetables (
        student_id TEXT NOT NULL,
        course TEXT NOT NULL,
        day TEXT NOT NULL,
        slot TEXT NOT NULL,
        PRIMARY KEY (student_id, course)
    );
    """,
    mirrored=settings.ft,
)


def _reset() -> None:
    def _run(conn):
        conn.execute("DELETE FROM transcripts")
        conn.execute("DELETE FROM timetables")

    store.write(_run)


app = FastAPI(title="Academic records service")
install(app, settings, store, _reset)


class InitIn(BaseModel):
    student_id: str = Field(min_length=1)
    idempotency_key: str = Field(min_length=1)


class GradeIn(BaseModel):
    student_id: str = Field(min_length=1)
    course: str = Field(min_length=1)
    grade: str = Field(min_length=1, max_length=2)


@app.post("/transcripts/init")
def init_transcript(body: InitIn) -> dict[str, object]:
    def _run(conn):
        for course, day, slot in COURSES:
            conn.execute(
                "INSERT OR IGNORE INTO transcripts (student_id, course, grade) VALUES (?, ?, NULL)",
                (body.student_id, course),
            )
            conn.execute(
                """
                INSERT OR IGNORE INTO timetables (student_id, course, day, slot)
                VALUES (?, ?, ?, ?)
                """,
                (body.student_id, course, day, slot),
            )
        emit("transcript_ready", student_id=body.student_id)
        return body.student_id

    student_id = store.write(_run)
    return get_transcript(student_id)


@app.post("/records")
def record_grade(body: GradeIn) -> dict[str, object]:
    def _run(conn):
        cursor = conn.execute(
            "UPDATE transcripts SET grade=? WHERE student_id=? AND course=?",
            (body.grade.upper(), body.student_id, body.course),
        )
        return cursor.rowcount

    if store.write(_run) != 1:
        raise HTTPException(status_code=404, detail="course is not on the transcript")
    return get_transcript(body.student_id)


@app.get("/transcripts/{student_id}")
def get_transcript(student_id: str) -> dict[str, object]:
    def _run(conn):
        courses = [
            dict(row)
            for row in conn.execute(
                "SELECT course, grade FROM transcripts WHERE student_id=? ORDER BY course",
                (student_id,),
            ).fetchall()
        ]
        slots = [
            dict(row)
            for row in conn.execute(
                "SELECT course, day, slot FROM timetables WHERE student_id=? ORDER BY day",
                (student_id,),
            ).fetchall()
        ]
        return {"student_id": student_id, "courses": courses, "timetable": slots}

    body = store.write(_run)
    if not body["courses"]:
        raise HTTPException(status_code=404, detail="transcript not found")
    return body
