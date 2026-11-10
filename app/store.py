"""Task store: the state machine and every transition.

server.py only parses requests and calls into here. Delivery is at-least-once:
a worker that dies after doing the work but before ack causes a re-run, so
consumers must be idempotent.
"""
from __future__ import annotations

import json
import random
import sqlite3
import threading
import time
import uuid
from dataclasses import dataclass
from typing import Any, Optional

PENDING, LEASED, SUCCEEDED, DEAD, CANCELED = "PENDING", "LEASED", "SUCCEEDED", "DEAD", "CANCELED"
STATUSES = (PENDING, LEASED, SUCCEEDED, DEAD, CANCELED)

SCHEMA = """
CREATE TABLE IF NOT EXISTS tasks (
  id               TEXT PRIMARY KEY,
  queue            TEXT NOT NULL,
  payload          TEXT NOT NULL,
  status           TEXT NOT NULL,
  attempts         INTEGER NOT NULL DEFAULT 0,
  max_attempts     INTEGER NOT NULL DEFAULT 3,
  idempotency_key  TEXT,
  lease_owner      TEXT,
  lease_token      TEXT,
  lease_expires_at REAL,
  next_run_at      REAL NOT NULL,
  priority         INTEGER NOT NULL DEFAULT 0,
  result           TEXT,
  last_error       TEXT,
  created_at       REAL NOT NULL,
  updated_at       REAL NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_tasks_idem
  ON tasks(queue, idempotency_key) WHERE idempotency_key IS NOT NULL;
CREATE INDEX IF NOT EXISTS ix_tasks_expiry   ON tasks(status, lease_expires_at);
"""

# Additive migrations for databases created by an older version.
# Each entry: (column, DDL to add it). The leasable index is (re)built after,
# because its column order must match the lease ORDER BY to be useful.
MIGRATIONS = [
    ("priority", "ALTER TABLE tasks ADD COLUMN priority INTEGER NOT NULL DEFAULT 0"),
]
LEASABLE_INDEX = """
DROP INDEX IF EXISTS ix_tasks_leasable;
CREATE INDEX IF NOT EXISTS ix_tasks_leasable
  ON tasks(queue, status, priority DESC, next_run_at);
"""


class Conflict(Exception):  # state or token mismatch -> 409
    pass


class NotFound(Exception):  # -> 404
    pass


@dataclass
class BackoffPolicy:
    base_seconds: float = 1.0
    max_seconds: float = 300.0
    jitter: float = 0.2  # +/- 20%

    def delay(self, attempts: int) -> float:
        raw = min(self.max_seconds, self.base_seconds * (2 ** max(0, attempts - 1)))
        return raw * (1 + random.uniform(-self.jitter, self.jitter))


class TaskStore:
    """
    SQLite-backed store. One connection guarded by a lock: SQLite is a
    single-writer database anyway, and the lock keeps lease/ack/sweep
    transactions from interleaving across server threads.
    """

    def __init__(self, path: str = "minisched.db", backoff: BackoffPolicy | None = None,
                 clock=time.time):
        self._conn = sqlite3.connect(path, check_same_thread=False, isolation_level=None)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA busy_timeout=5000")
        self._conn.executescript(SCHEMA)
        self._migrate()
        self._lock = threading.RLock()
        # Long polling: waiters block on this condition; any transition
        # that can make a task runnable (submit, nack, reclaim) notifies it.
        self._runnable = threading.Condition(self._lock)
        self.backoff = backoff or BackoffPolicy()
        self.now = clock

    # helpers
    def _migrate(self) -> None:
        have = {r["name"] for r in self._conn.execute("PRAGMA table_info(tasks)")}
        for column, ddl in MIGRATIONS:
            if column not in have:
                self._conn.execute(ddl)
        self._conn.executescript(LEASABLE_INDEX)

    @staticmethod
    def _row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
        d = dict(row)
        d["payload"] = json.loads(d["payload"])
        d["result"] = json.loads(d["result"]) if d["result"] else None
        return d

    def _get(self, task_id: str) -> sqlite3.Row:
        row = self._conn.execute("SELECT * FROM tasks WHERE id = ?", (task_id,)).fetchone()
        if row is None:
            raise NotFound(task_id)
        return row

    # producer side
    def submit(self, queue: str, payload: Any, *, max_attempts: int = 3,
               idempotency_key: Optional[str] = None, delay_seconds: float = 0.0,
               priority: int = 0) -> tuple[dict, bool]:
        """
        Returns (task, created). With an idempotency key, resubmitting the same
        request returns the original task and created=False. Same key with a
        different payload is a Conflict, so a bug can't silently overwrite work.

        priority: higher runs first within a queue (default 0). Ties keep FIFO.
        """
        if max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")
        if not -1000 <= int(priority) <= 1000:
            raise ValueError("priority must be between -1000 and 1000")
        body = json.dumps(payload, sort_keys=True, separators=(",", ":"))
        now = self.now()
        with self._lock:
            if idempotency_key is not None:
                existing = self._conn.execute(
                    "SELECT * FROM tasks WHERE queue = ? AND idempotency_key = ?",
                    (queue, idempotency_key)).fetchone()
                if existing is not None:
                    if existing["payload"] != body:
                        raise Conflict("idempotency_key reused with a different payload")
                    return self._row_to_dict(existing), False
            task_id = str(uuid.uuid4())
            self._conn.execute(
                """INSERT INTO tasks (id, queue, payload, status, attempts, max_attempts,
                                      idempotency_key, next_run_at, priority, created_at, updated_at)
                   VALUES (?, ?, ?, ?, 0, ?, ?, ?, ?, ?, ?)""",
                (task_id, queue, body, PENDING, max_attempts, idempotency_key,
                 now + max(0.0, delay_seconds), int(priority), now, now))
            self._runnable.notify_all()
            return self._row_to_dict(self._get(task_id)), True

    def get(self, task_id: str) -> dict:
        with self._lock:
            return self._row_to_dict(self._get(task_id))

    def list(self, *, queue: Optional[str] = None, status: Optional[str] = None,
             limit: int = 100) -> list[dict]:
        sql, args = "SELECT * FROM tasks WHERE 1=1", []
        if queue:
            sql += " AND queue = ?"; args.append(queue)
        if status:
            sql += " AND status = ?"; args.append(status)
        sql += " ORDER BY created_at DESC LIMIT ?"; args.append(max(1, min(limit, 1000)))
        with self._lock:
            return [self._row_to_dict(r) for r in self._conn.execute(sql, args)]

    def cancel(self, task_id: str) -> dict:
        with self._lock:
            cur = self._conn.execute(
                "UPDATE tasks SET status = ?, updated_at = ? WHERE id = ? AND status = ?",
                (CANCELED, self.now(), task_id, PENDING))
            if cur.rowcount == 0:
                row = self._get(task_id)  # raises NotFound if missing
                raise Conflict(f"cannot cancel a task in status {row['status']}")
            return self._row_to_dict(self._get(task_id))

    # worker side
    def lease(self, queue: str, worker_id: str, *, max_tasks: int = 1,
              lease_seconds: float = 30.0, wait_seconds: float = 0.0) -> list[dict]:
        """
        Atomically claim up to max_tasks runnable tasks. Each claim gets a fresh
        lease_token; later heartbeat/ack/nack must present it (fencing), so a
        worker whose lease expired and was handed to someone else is rejected.

        wait_seconds > 0 turns this into a long poll: if nothing is runnable,
        block until something becomes runnable or the wait runs out. Returns
        as soon as at least one task is claimed (does not wait to fill max_tasks).
        """
        deadline = time.monotonic() + max(0.0, min(wait_seconds, 60.0))
        with self._lock:
            while True:
                claimed = self._try_lease(queue, worker_id, max_tasks, lease_seconds)
                if claimed:
                    return claimed
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return []
                # Wake early when the earliest delayed/backoff task becomes due,
                # otherwise sleep until notified or the poll deadline.
                row = self._conn.execute(
                    "SELECT MIN(next_run_at) AS t FROM tasks WHERE queue = ? AND status = ?",
                    (queue, PENDING)).fetchone()
                until_due = (row["t"] - self.now()) if row and row["t"] is not None else remaining
                self._runnable.wait(timeout=max(0.0, min(remaining, until_due, 1.0)))

    def _try_lease(self, queue: str, worker_id: str, max_tasks: int, lease_seconds: float) -> list[dict]:
        # one non-blocking claim attempt; caller holds the lock
        now = self.now()
        self._conn.execute("BEGIN IMMEDIATE")
        try:
            rows = self._conn.execute(
                """SELECT id FROM tasks
                   WHERE queue = ? AND status = ? AND next_run_at <= ?
                   ORDER BY priority DESC, next_run_at, created_at LIMIT ?""",
                (queue, PENDING, now, max(1, min(max_tasks, 100)))).fetchall()
            claimed = []
            for r in rows:
                token = uuid.uuid4().hex
                cur = self._conn.execute(
                    """UPDATE tasks SET status = ?, attempts = attempts + 1,
                           lease_owner = ?, lease_token = ?, lease_expires_at = ?, updated_at = ?
                       WHERE id = ? AND status = ?""",
                    (LEASED, worker_id, token, now + lease_seconds, now, r["id"], PENDING))
                if cur.rowcount == 1:
                    claimed.append(r["id"])
            self._conn.execute("COMMIT")
        except Exception:
            self._conn.execute("ROLLBACK")
            raise
        return [self._row_to_dict(self._get(i)) for i in claimed]

    def _fenced_update(self, task_id: str, lease_token: str, set_sql: str, args: tuple) -> dict:
        cur = self._conn.execute(
            f"UPDATE tasks SET {set_sql} WHERE id = ? AND status = ? AND lease_token = ?",
            (*args, task_id, LEASED, lease_token))
        if cur.rowcount == 0:
            row = self._get(task_id)
            raise Conflict(f"lease token rejected (task is {row['status']}, "
                           f"owned by {row['lease_owner']})")
        return self._row_to_dict(self._get(task_id))

    def heartbeat(self, task_id: str, lease_token: str, *, lease_seconds: float = 30.0) -> dict:
        now = self.now()
        with self._lock:
            return self._fenced_update(
                task_id, lease_token,
                "lease_expires_at = ?, updated_at = ?", (now + lease_seconds, now))

    def ack(self, task_id: str, lease_token: str, result: Any = None) -> dict:
        now = self.now()
        with self._lock:
            return self._fenced_update(
                task_id, lease_token,
                "status = ?, result = ?, lease_owner = NULL, lease_token = NULL, "
                "lease_expires_at = NULL, updated_at = ?",
                (SUCCEEDED, json.dumps(result), now))

    def nack(self, task_id: str, lease_token: str, error: str = "",
             retry_after_seconds: Optional[float] = None) -> dict:
        """Worker gave up on this attempt. Retry with backoff, or bury as DEAD."""
        now = self.now()
        with self._lock:
            row = self._get(task_id)
            if row["status"] != LEASED or row["lease_token"] != lease_token:
                raise Conflict("lease token rejected")
            return self._release(row, error or "nack", now, retry_after_seconds)

    def _release(self, row: sqlite3.Row, error: str, now: float,
                 retry_after: Optional[float] = None) -> dict:
        exhausted = row["attempts"] >= row["max_attempts"]
        status = DEAD if exhausted else PENDING
        delay = retry_after if retry_after is not None else self.backoff.delay(row["attempts"])
        self._conn.execute(
            """UPDATE tasks SET status = ?, last_error = ?, next_run_at = ?,
                   lease_owner = NULL, lease_token = NULL, lease_expires_at = NULL, updated_at = ?
               WHERE id = ?""",
            (status, error[:2000], now + (0 if exhausted else delay), now, row["id"]))
        if not exhausted:
            self._runnable.notify_all()
        return self._row_to_dict(self._get(row["id"]))

    # background
    def reclaim_expired(self) -> int:
        """
        Return tasks whose lease expired to PENDING (or DEAD). This is how the
        system survives a worker that crashed mid-task: nobody has to notice.
        """
        now = self.now()
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM tasks WHERE status = ? AND lease_expires_at < ?",
                (LEASED, now)).fetchall()
            for row in rows:
                self._release(row, f"lease expired (owner {row['lease_owner']})", now)
            return len(rows)

    def stats(self) -> dict:
        with self._lock:
            rows = self._conn.execute(
                "SELECT queue, status, COUNT(*) AS n FROM tasks GROUP BY queue, status").fetchall()
        out: dict[str, dict[str, int]] = {}
        for r in rows:
            out.setdefault(r["queue"], {s: 0 for s in STATUSES})[r["status"]] = r["n"]
        return out

    def close(self) -> None:
        with self._lock:
            self._conn.close()


class Sweeper(threading.Thread):
    """Calls store.reclaim_expired() on an interval. Daemon so it dies with the server."""

    def __init__(self, store: TaskStore, interval: float = 1.0):
        super().__init__(name="sweeper", daemon=True)
        self.store, self.interval = store, interval
        self._stop = threading.Event()
        self.reclaimed_total = 0

    def run(self) -> None:
        while not self._stop.wait(self.interval):
            try:
                self.reclaimed_total += self.store.reclaim_expired()
            except Exception as exc:  # keep sweeping; log and move on
                print(f"[sweeper] error: {exc!r}")

    def stop(self) -> None:
        self._stop.set()
