"""SQLite storage with an optional mirrored copy.

Fault-tolerant mode writes every committed transaction to a second database
file. That second file is the RAID-1 stand-in: lose the primary file and the
service rebuilds it from the mirror. Baseline mode has only the primary file.

This is storage redundancy in software. It is not a hardware RAID controller
and it is not memory ECC. A software Hamming model of ECC lives in ftuis.ecc.
"""

from __future__ import annotations

import shutil
import sqlite3
import threading
from pathlib import Path
from typing import Callable

import fcntl

from ftuis.config import DATA, ensure_dirs
from ftuis.events import emit


class DatabaseUnavailable(Exception):
    pass


class Store:
    def __init__(self, service: str, schema: str, mirrored: bool) -> None:
        ensure_dirs()
        self.service = service
        self.schema = schema
        self.mirrored = mirrored
        self.directory = DATA / service
        self.directory.mkdir(parents=True, exist_ok=True)
        self.primary = self.directory / "primary.db"
        self.mirror = self.directory / "mirror.db"
        self.lock_path = self.directory / ".write.lock"
        self._thread = threading.RLock()
        self.conn: sqlite3.Connection | None = None
        self.write(lambda conn: conn.executescript(schema))

    def close(self) -> None:
        with self._thread:
            with self._file_lock():
                self._close_unlocked()

    def write(self, fn: Callable[[sqlite3.Connection], object]) -> object:
        with self._thread:
            with self._file_lock():
                conn = self._connection()
                try:
                    result = fn(conn)
                    conn.commit()
                except Exception:
                    conn.rollback()
                    raise
                if self.mirrored:
                    self._snapshot(conn)
                return result

    def _connection(self) -> sqlite3.Connection:
        if self.conn is not None:
            return self.conn
        try:
            self.conn = self._open(self.primary)
        except DatabaseUnavailable as exc:
            if not self.mirrored or not self.mirror.exists():
                raise
            emit(
                "db_failover",
                service=self.service,
                reason=str(exc),
                mirror=str(self.mirror),
            )
            self._restore_primary_from_mirror()
            self.conn = self._open(self.primary)
            emit("db_restored", service=self.service)
        self.conn.executescript(self.schema)
        return self.conn

    def _open(self, path: Path) -> sqlite3.Connection:
        if not path.exists() or path.stat().st_size == 0:
            if path == self.primary and not self.mirror.exists():
                path.touch()
            else:
                raise DatabaseUnavailable(f"{path.name} is missing")
        conn: sqlite3.Connection | None = None
        try:
            conn = sqlite3.connect(path, timeout=5, check_same_thread=False)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA busy_timeout=3000")
            row = conn.execute("PRAGMA quick_check").fetchone()
        except sqlite3.DatabaseError as exc:
            if conn is not None:
                conn.close()
            raise DatabaseUnavailable(str(exc)) from exc
        if row is None or row[0] != "ok":
            assert conn is not None
            conn.close()
            raise DatabaseUnavailable(f"integrity check failed for {path.name}: {row}")
        # DELETE, not WAL. The replicas share the file through a Docker bind
        # mount, and WAL on that file share returns disk I/O errors. The file
        # lock already serializes writers, so a commit is in the main file
        # when the host reads it back.
        conn.execute("PRAGMA journal_mode=DELETE")
        conn.execute("PRAGMA synchronous=FULL")
        conn.execute("PRAGMA mmap_size=0")
        return conn

    def _snapshot(self, conn: sqlite3.Connection) -> None:
        temporary = self.mirror.with_suffix(".db.tmp")
        if temporary.exists():
            temporary.unlink()
        dest = sqlite3.connect(temporary)
        try:
            conn.backup(dest)
        finally:
            dest.close()
        os_replace(temporary, self.mirror)

    def _restore_primary_from_mirror(self) -> None:
        for extra in (
            self.primary,
            Path(str(self.primary) + "-wal"),
            Path(str(self.primary) + "-shm"),
        ):
            if extra.exists():
                extra.unlink()
        shutil.copy(self.mirror, self.primary)

    def _close_unlocked(self) -> None:
        if self.conn is not None:
            self.conn.close()
            self.conn = None

    def _file_lock(self):
        return _FileLock(self.lock_path)


def os_replace(src: Path, dest: Path) -> None:
    src.replace(dest)


class _FileLock:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.handle = None

    def __enter__(self):
        self.handle = self.path.open("a")
        fcntl.flock(self.handle, fcntl.LOCK_EX)
        return self.handle

    def __exit__(self, exc_type, exc, tb) -> None:
        if self.handle is not None:
            fcntl.flock(self.handle, fcntl.LOCK_UN)
            self.handle.close()
