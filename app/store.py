"""Task store: the state machine and every transition.

server.py only parses requests and calls into here. Delivery is at-least-once:
a worker that dies after doing the work but before ack causes a re-run, so
consumers must be idempotent.
"""
from __future__ import annotations

import json
import sqlite3
import threading
import time
import uuid
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
  result           TEXT,
  last_error       TEXT,
  created_at       REAL NOT NULL,
  updated_at       REAL NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_tasks_idem
  ON tasks(queue, idempotency_key) WHERE idempotency_key IS NOT NULL;
CREATE INDEX IF NOT EXISTS ix_tasks_leasable ON tasks(queue, status, next_run_at);
CREATE INDEX IF NOT EXISTS ix_tasks_expiry   ON tasks(status, lease_expires_at);
"""


class Conflict(Exception):  # state or token mismatch -> 409
    pass


class NotFound(Exception):  # -> 404
    pass


class TaskStore:
    """
    SQLite-backed store. One connection guarded by a lock: SQLite is a
    single-writer database anyway, and the lock keeps lease/ack/sweep
    transactions from interleaving across server threads.
    """

    def __init__(self, path: str = "minisched.db", clock=time.time):
        self._conn = sqlite3.connect(path, check_same_thread=False, isolation_level=None)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA busy_timeout=5000")
        self._conn.executescript(SCHEMA)
        self._lock = threading.RLock()
        self.now = clock

    # helpers
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
               idempotency_key: Optional[str] = None, delay_seconds: float = 0.0) -> tuple[dict, bool]:
        """
        Returns (task, created). With an idempotency key, resubmitting the same
        request returns the original task and created=False. Same key with a
        different payload is a Conflict, so a bug can't silently overwrite work.
        """
        if max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")
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
                                      idempotency_key, next_run_at, created_at, updated_at)
                   VALUES (?, ?, ?, ?, 0, ?, ?, ?, ?, ?)""",
                (task_id, queue, body, PENDING, max_attempts, idempotency_key,
                 now + max(0.0, delay_seconds), now, now))
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
              lease_seconds: float = 30.0) -> list[dict]:
        """
        Atomically claim up to max_tasks runnable tasks. Each claim gets a fresh
        lease_token; later heartbeat/ack/nack must present it (fencing), so a
        worker whose lease expired and was handed to someone else is rejected.
        """
        now = self.now()
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                rows = self._conn.execute(
                    """SELECT id FROM tasks
                       WHERE queue = ? AND status = ? AND next_run_at <= ?
                       ORDER BY next_run_at, created_at LIMIT ?""",
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

    # background
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
