"""
Producer-side client. Turns "run this prompt" into a minisched task.

Caching is not a separate store: the idempotency key IS the cache key.
  key = sha256(the whole payload: kind, provider, model, prompt, system, schema,
               temperature, max_tokens, tags)
Submitting the same request twice returns the same task; if it already
SUCCEEDED the result is right there and no tokens are spent. The key covers
the entire payload, so it can never collide with minisched's own guard
(same key + different payload -> 409). Consequence worth knowing: tags are
part of a request's identity. Re-running a batch with the same tags hits the
cache; a new batch with different tags does not. Omit tags if you want
cross-batch dedupe.
"""
from __future__ import annotations

import hashlib
import json
import time
import urllib.error
import urllib.request
from typing import Any, Optional

QUEUE = "llm"


def _canon(obj: Any) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def request_key(payload: dict) -> str:
    return "llm:" + hashlib.sha256(_canon(payload).encode()).hexdigest()[:32]


class SchedulerClient:
    def __init__(self, base: str = "http://127.0.0.1:8080"):
        self.base = base.rstrip("/")
        self.cache_hits = 0
        self.submitted = 0

    def _call(self, method: str, path: str, body: Optional[dict] = None, timeout: float = 30):
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(self.base + path, data=data, method=method,
                                     headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return r.status, json.loads(r.read() or b"{}")
        except urllib.error.HTTPError as e:
            return e.code, json.loads(e.read() or b"{}")

    # ---------------------------------------------------------------- submit
    def _submit(self, payload: dict, *, priority: int = 0, max_attempts: int = 5,
                depends_on: Optional[list[str]] = None) -> dict:
        body = {"queue": QUEUE, "payload": payload, "idempotency_key": request_key(payload),
                "priority": priority, "max_attempts": max_attempts}
        if depends_on:
            body["depends_on"] = depends_on
        status, resp = self._call("POST", "/tasks", body)
        if status not in (200, 201):
            raise RuntimeError(f"submit failed: {status} {resp}")
        task = resp["task"]
        if status == 200 and task["status"] == "SUCCEEDED":
            self.cache_hits += 1          # same request already answered
        elif status == 201:
            self.submitted += 1
        return task

    def complete(self, prompt: str, *, model: str = "fake-1", provider: str = "fake", system: str = "",
                 schema: Optional[dict] = None, temperature: float = 0.0, max_tokens: int = 512,
                 priority: int = 0, max_attempts: int = 5, tags: Optional[dict] = None) -> dict:
        payload = {"kind": "complete", "provider": provider, "model": model, "prompt": prompt,
                   "system": system, "schema": schema, "temperature": temperature,
                   "max_tokens": max_tokens, "tags": tags or {}}
        return self._submit(payload, priority=priority, max_attempts=max_attempts)

    def embed(self, text: str, *, model: str = "fake-embed", provider: str = "fake",
              priority: int = 0, max_attempts: int = 5, tags: Optional[dict] = None) -> dict:
        payload = {"kind": "embed", "provider": provider, "model": model, "text": text, "tags": tags or {}}
        return self._submit(payload, priority=priority, max_attempts=max_attempts)

    # ---------------------------------------------------------------- read back
    def get(self, task_id: str) -> dict:
        status, resp = self._call("GET", f"/tasks/{task_id}")
        if status != 200:
            raise RuntimeError(f"get failed: {status} {resp}")
        return resp["task"]

    def wait(self, task_ids: list[str], timeout: float = 60.0, poll: float = 0.25) -> dict[str, dict]:
        """Block until every task is terminal (SUCCEEDED/DEAD/CANCELED) or timeout."""
        deadline = time.monotonic() + timeout
        done: dict[str, dict] = {}
        pending = set(task_ids)
        while pending and time.monotonic() < deadline:
            for tid in list(pending):
                t = self.get(tid)
                if t["status"] in ("SUCCEEDED", "DEAD", "CANCELED"):
                    done[tid] = t
                    pending.discard(tid)
            if pending:
                time.sleep(poll)
        for tid in pending:
            done[tid] = self.get(tid)
        return done

    def list(self, status: Optional[str] = None, limit: int = 1000) -> list[dict]:
        q = f"/tasks?queue={QUEUE}&limit={limit}" + (f"&status={status}" if status else "")
        s, resp = self._call("GET", q)
        return resp.get("tasks", []) if s == 200 else []
