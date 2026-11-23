"""
LLM worker. Leases tasks from the "llm" queue and runs them through a provider.

What it adds on top of a plain minisched worker:
  * per-provider concurrency limit (a semaphore per provider name), so one
    noisy provider can't starve the others and we stay under rate limits
  * error classification -> scheduler action
        RateLimited(retry_after)  nack(retry_after_seconds=retry_after)
        TransientError            nack()               (exponential backoff)
        PermanentError            nack(retry_after=0)  (drain attempts to DEAD fast)
        bad/invalid JSON output   nack()               (treated as transient; models are flaky)
  * JSON schema validation of structured outputs before ack
  * usage accounting: tokens, cost, latency on every result, plus /metrics

Everything is injectable so tests run in-process with a FakeProvider.
"""
from __future__ import annotations

import argparse
import json
import os
import threading
import time
from typing import Callable, Optional

from .client import QUEUE, SchedulerClient
from .metrics import Metrics
from .providers import (PermanentError, Provider, RateLimited, TransientError,
                        cost_usd, make_provider)
from .schema import validate


class LLMWorker:
    def __init__(self, scheduler: str, providers: dict[str, Provider], *,
                 concurrency: dict[str, int] | None = None, default_concurrency: int = 4,
                 lease_seconds: float = 60.0, worker_id: Optional[str] = None,
                 metrics: Optional[Metrics] = None, log: Callable[[str], None] = print):
        self.client = SchedulerClient(scheduler)
        self.providers = providers
        self.sems = {name: threading.BoundedSemaphore((concurrency or {}).get(name, default_concurrency))
                     for name in providers}
        self.lease_seconds = lease_seconds
        self.worker_id = worker_id or f"llm-{os.getpid()}"
        self.metrics = metrics or Metrics()
        self.log = log
        self._stop = threading.Event()

    # ------------------------------------------------------------- one task
    def run_task(self, task: dict) -> None:
        tid, tok = task["id"], task["lease_token"]
        p = task["payload"]
        pname = p.get("provider", "fake")
        provider = self.providers.get(pname)
        if provider is None:
            self.client._call("POST", f"/tasks/{tid}/nack",
                              {"lease_token": tok, "error": f"no provider {pname!r}", "retry_after_seconds": 0})
            return

        sem = self.sems[pname]
        with sem:
            self.metrics.set_inflight(pname, +1)
            stop_hb = threading.Event()
            hb = threading.Thread(target=self._heartbeat, args=(tid, tok, stop_hb), daemon=True)
            hb.start()
            t0 = time.monotonic()
            try:
                result = self._execute(provider, p)
                outcome = "ok"
            except RateLimited as e:
                outcome, err, retry = "rate_limited", f"429 retry_after={e.retry_after}", e.retry_after
            except TransientError as e:
                outcome, err, retry = "transient", repr(e), None
            except PermanentError as e:
                outcome, err, retry = "permanent", repr(e), 0.0
            except BadOutput as e:
                outcome, err, retry = "bad_output", str(e), None
            finally:
                stop_hb.set()
                self.metrics.set_inflight(pname, -1)
            latency = time.monotonic() - t0

        if outcome == "ok":
            result["latency_ms"] = round(latency * 1000)
            self.metrics.observe(pname, p.get("model", "?"), "ok", latency,
                                 result.get("usage", {}).get("prompt_tokens", 0),
                                 result.get("usage", {}).get("completion_tokens", 0),
                                 result.get("usage", {}).get("cost_usd", 0.0))
            s, body = self.client._call("POST", f"/tasks/{tid}/ack", {"lease_token": tok, "result": result})
            if s != 200:
                self.log(f"{tid[:8]} ack rejected ({s}): {body.get('error')}  (lease lost; result discarded)")
            else:
                self.log(f"{tid[:8]} ok  {p.get('kind')} {p.get('model')}  {result['latency_ms']}ms  "
                         f"${result.get('usage', {}).get('cost_usd', 0):.5f}")
        else:
            self.metrics.observe(pname, p.get("model", "?"), outcome, latency)
            body = {"lease_token": tok, "error": err}
            if retry is not None:
                body["retry_after_seconds"] = retry
            s, resp = self.client._call("POST", f"/tasks/{tid}/nack", body)
            st = resp.get("task", {}).get("status", s)
            self.log(f"{tid[:8]} {outcome}: {err[:80]} -> {st} (attempt {task['attempts']})")

    def _heartbeat(self, tid: str, tok: str, stop: threading.Event):
        while not stop.wait(self.lease_seconds / 3):
            s, _ = self.client._call("POST", f"/tasks/{tid}/heartbeat",
                                     {"lease_token": tok, "lease_seconds": self.lease_seconds})
            if s == 409:
                return

    def _execute(self, provider: Provider, p: dict) -> dict:
        kind = p.get("kind")
        model = p.get("model", "")
        if kind == "complete":
            schema = p.get("schema")
            c = provider.complete(model, p["prompt"], system=p.get("system", ""),
                                  max_tokens=int(p.get("max_tokens", 512)),
                                  temperature=float(p.get("temperature", 0.0)), json_mode=schema is not None)
            out: dict = {"text": c.text}
            if schema is not None:
                parsed = _parse_json(c.text)
                errs = validate(parsed, schema)
                if errs:
                    raise BadOutput("schema: " + "; ".join(errs[:3]))
                out["json"] = parsed
            usage = {"prompt_tokens": c.prompt_tokens, "completion_tokens": c.completion_tokens,
                     "cost_usd": round(cost_usd(c.model, c.prompt_tokens, c.completion_tokens), 8)}
            return {**out, "model": c.model, "usage": usage, "tags": p.get("tags", {})}
        if kind == "embed":
            e = provider.embed(model, p["text"])
            usage = {"prompt_tokens": e.prompt_tokens, "completion_tokens": 0,
                     "cost_usd": round(cost_usd(e.model, e.prompt_tokens, 0), 8)}
            return {"vector": e.vector, "dim": len(e.vector), "model": e.model, "usage": usage,
                    "tags": p.get("tags", {})}
        raise PermanentError(f"unknown task kind {kind!r}")

    # ------------------------------------------------------------- loop
    def run(self, *, once: bool = False, wait_seconds: float = 5.0, max_inflight: int = 16):
        self.log(f"[{self.worker_id}] providers={list(self.providers)} "
                 f"concurrency={ {k: s._initial_value for k, s in self.sems.items()} }")
        inflight = threading.BoundedSemaphore(max_inflight)
        idle = 0
        while not self._stop.is_set():
            inflight.acquire()
            s, body = self.client._call(
                "POST", f"/queues/{QUEUE}/lease",
                {"worker_id": self.worker_id, "max_tasks": 1, "lease_seconds": self.lease_seconds,
                 "wait_seconds": min(wait_seconds, 1.0) if once else wait_seconds},
                timeout=wait_seconds + 10)
            tasks = body.get("tasks", []) if s == 200 else []
            if not tasks:
                inflight.release()
                idle += 1
                if once and idle >= 3 and inflight._value == max_inflight:
                    self.log("queue drained, exiting")
                    return
                continue
            idle = 0

            def job(t=tasks[0]):
                try:
                    self.run_task(t)
                finally:
                    inflight.release()
            threading.Thread(target=job, daemon=True).start()

    def stop(self):
        self._stop.set()


class BadOutput(Exception):
    """Model returned something that isn't the JSON we asked for."""


def _parse_json(text: str):
    """Tolerate code fences and leading prose: take the first {...} or [...] block."""
    text = text.strip()
    if text.startswith("```"):
        text = text.strip("`")
        if text.lower().startswith("json"):
            text = text[4:]
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass
    for open_c, close_c in (("{", "}"), ("[", "]")):
        i, j = text.find(open_c), text.rfind(close_c)
        if i != -1 and j > i:
            try:
                return json.loads(text[i:j + 1])
            except json.JSONDecodeError:
                continue
    raise BadOutput(f"not JSON: {text[:60]!r}")


def main(argv=None):
    ap = argparse.ArgumentParser(description="llmbatch worker")
    ap.add_argument("--scheduler", default="http://127.0.0.1:8080")
    ap.add_argument("--provider", action="append", default=None,
                    help="provider name to serve (fake|openai|anthropic); repeatable. Default: $LLM_PROVIDER or fake")
    ap.add_argument("--concurrency", type=int, default=4, help="max in-flight calls per provider")
    ap.add_argument("--lease-seconds", type=float, default=60)
    ap.add_argument("--metrics-port", type=int, default=9100)
    ap.add_argument("--once", action="store_true")
    a = ap.parse_args(argv)
    names = a.provider or [os.environ.get("LLM_PROVIDER", "fake")]
    providers = {n: make_provider(n) for n in names}
    m = Metrics()
    httpd = m.serve(a.metrics_port) if a.metrics_port else None
    w = LLMWorker(a.scheduler, providers, default_concurrency=a.concurrency,
                  lease_seconds=a.lease_seconds, metrics=m)
    if httpd:
        print(f"metrics on http://127.0.0.1:{a.metrics_port}/metrics")
    try:
        w.run(once=a.once)
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
