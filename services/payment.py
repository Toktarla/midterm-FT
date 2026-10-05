"""Tuition payments with a checkpointed state machine.

Fault-tolerant mode:
  prepared  -> durable checkpoint, no balance change yet
  applied   -> balance changed, not yet acknowledged
  committed -> complete
  rolled_back -> prepared work discarded, balance untouched

A crash in `prepared` rolls back on the next startup. A crash in `applied`
rolls forward: the money move already happened, so the receipt is completed
instead of charging the student again. The idempotency key makes the retry
return that same receipt.

Baseline mode commits the balance and the receipt in one step and has no
idempotency. A lost response followed by a retry charges the student twice.
"""

from __future__ import annotations

import os
import sqlite3
import time
import uuid
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from ftuis import chaos
from ftuis.config import load_settings
from ftuis.events import emit
from ftuis.storage import Store
from services.runtime import install

settings = load_settings()

_key_column = "idempotency_key TEXT UNIQUE" if settings.ft else "idempotency_key TEXT"
store = Store(
    "payment",
    f"""
    CREATE TABLE IF NOT EXISTS payments (
        id TEXT PRIMARY KEY,
        student_id TEXT NOT NULL,
        amount REAL NOT NULL,
        {_key_column},
        step TEXT NOT NULL,
        created_at REAL NOT NULL
    );
    CREATE TABLE IF NOT EXISTS accounts (
        student_id TEXT PRIMARY KEY,
        balance REAL NOT NULL
    );
    """,
    mirrored=settings.ft,
)


def _public(row) -> dict[str, object]:
    body = dict(row)
    body["status"] = body["step"]
    return body


def recover() -> dict[str, int]:
    if not settings.ft:
        return {"rolled_forward": 0, "rolled_back": 0}

    def _run(conn):
        forwarded = conn.execute("SELECT id FROM payments WHERE step='applied'").fetchall()
        for row in forwarded:
            conn.execute(
                "UPDATE payments SET step='committed' WHERE id=? AND step='applied'",
                (row["id"],),
            )
            emit("roll_forward", payment_id=row["id"])
        abandoned = conn.execute("SELECT id FROM payments WHERE step='prepared'").fetchall()
        for row in abandoned:
            conn.execute(
                "UPDATE payments SET step='rolled_back' WHERE id=? AND step='prepared'",
                (row["id"],),
            )
            emit("roll_back", payment_id=row["id"])
        return {"rolled_forward": len(forwarded), "rolled_back": len(abandoned)}

    result = store.write(_run)
    if result["rolled_forward"] or result["rolled_back"]:
        emit("recovery_complete", **result)
    return result


@asynccontextmanager
async def lifespan(_app: FastAPI):
    recover()
    yield
    store.close()


def _reset() -> None:
    def _run(conn):
        conn.execute("DELETE FROM payments")
        conn.execute("DELETE FROM accounts")

    store.write(_run)


app = FastAPI(title="Payment service", lifespan=lifespan)
install(app, settings, store, _reset)


class PaymentIn(BaseModel):
    student_id: str = Field(min_length=1)
    amount: float = Field(gt=0)
    idempotency_key: str = Field(min_length=1)


def _lookup(conn, key: str):
    return conn.execute("SELECT * FROM payments WHERE idempotency_key=?", (key,)).fetchone()


def _add_balance(conn, student_id: str, amount: float) -> None:
    current = conn.execute(
        "SELECT balance FROM accounts WHERE student_id=?",
        (student_id,),
    ).fetchone()
    if current is None:
        conn.execute(
            "INSERT INTO accounts (student_id, balance) VALUES (?, ?)",
            (student_id, amount),
        )
    else:
        conn.execute(
            "UPDATE accounts SET balance=? WHERE student_id=?",
            (float(current["balance"]) + amount, student_id),
        )


def _maybe_crash(point: str) -> None:
    if chaos.crash_point(settings.instance_id) == point:
        emit("process_crashing", point=point)
        os._exit(1)


def _finish_existing(conn, existing) -> dict[str, object]:
    emit("duplicate_detected", key=existing["idempotency_key"], step=existing["step"])
    if existing["step"] == "applied":
        conn.execute(
            "UPDATE payments SET step='committed' WHERE id=? AND step='applied'",
            (existing["id"],),
        )
        existing = conn.execute("SELECT * FROM payments WHERE id=?", (existing["id"],)).fetchone()
    return _public(existing)


@app.post("/payments")
def create_payment(body: PaymentIn) -> dict[str, object]:
    if settings.ft:
        existing = store.write(lambda conn: _lookup(conn, body.idempotency_key))
        if existing is not None:
            return store.write(lambda conn: _finish_existing(conn, _lookup(conn, body.idempotency_key)))

        payment_id = str(uuid.uuid4())
        store.write(
            lambda conn: conn.execute(
                """
                INSERT INTO payments
                    (id, student_id, amount, idempotency_key, step, created_at)
                VALUES (?, ?, ?, ?, 'prepared', ?)
                """,
                (payment_id, body.student_id, body.amount, body.idempotency_key, time.time()),
            )
        )
        _maybe_crash("after_prepare")

        def _apply(conn) -> None:
            _add_balance(conn, body.student_id, body.amount)
            updated = conn.execute(
                "UPDATE payments SET step='applied' WHERE id=? AND step='prepared'",
                (payment_id,),
            )
            if updated.rowcount != 1:
                raise HTTPException(status_code=409, detail="payment checkpoint changed")

        try:
            store.write(_apply)
        except sqlite3.IntegrityError:
            emit("duplicate_detected", key=body.idempotency_key, step="race")
            return store.write(lambda conn: _finish_existing(conn, _lookup(conn, body.idempotency_key)))
        _maybe_crash("after_side_effect")
        store.write(
            lambda conn: conn.execute(
                "UPDATE payments SET step='committed' WHERE id=? AND step='applied'",
                (payment_id,),
            )
        )
        return store.write(lambda conn: _public(_lookup(conn, body.idempotency_key)))

    prior = store.write(lambda conn: _lookup(conn, body.idempotency_key))
    if prior is not None:
        emit("duplicate_not_prevented", key=body.idempotency_key)

    payment_id = str(uuid.uuid4())

    def _commit_once(conn) -> None:
        conn.execute(
            """
            INSERT INTO payments
                (id, student_id, amount, idempotency_key, step, created_at)
            VALUES (?, ?, ?, ?, 'committed', ?)
            """,
            (payment_id, body.student_id, body.amount, body.idempotency_key, time.time()),
        )
        _add_balance(conn, body.student_id, body.amount)

    store.write(_commit_once)
    _maybe_crash("after_side_effect")
    return store.write(lambda conn: _public(conn.execute("SELECT * FROM payments WHERE id=?", (payment_id,)).fetchone()))


@app.get("/payments")
def list_payments() -> dict[str, object]:
    def _run(conn):
        rows = conn.execute("SELECT * FROM payments ORDER BY created_at").fetchall()
        return [_public(row) for row in rows]

    return {"payments": store.write(_run)}
