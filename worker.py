"""
Example worker. Polls a queue, "executes" tasks, heartbeats while running,
then acks or nacks. Run several copies to see leasing and fencing at work.

Payload contract for this demo worker:
  {"sleep": 2.0}            work takes 2 seconds
  {"fail": true}            always fail (ends DEAD after max_attempts)
  {"fail_first": 2}         fail the first 2 attempts, then succeed
  {"crash": true}           simulate a worker crash: stop heartbeating and
                            never ack, so the sweeper has to reclaim the lease

Usage:
  python3 worker.py --queue default --concurrency 2
"""
from __future__ import annotations

import argparse
import json
import os
import random
import threading
import time
import urllib.error
import urllib.request


class Client:
    def __init__(self, base: str):
        self.base = base.rstrip("/")

    def _call(self, method: str, path: str, body: dict | None = None, timeout: float = 10):
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(self.base + path, data=data, method=method,
                                     headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return r.status, json.loads(r.read() or b"{}")
        except urllib.error.HTTPError as e:
            return e.code, json.loads(e.read() or b"{}")

    def lease(self, queue, worker_id, max_tasks, lease_seconds, wait_seconds=0):
        return self._call("POST", f"/queues/{queue}/lease",
                          {"worker_id": worker_id, "max_tasks": max_tasks,
                           "lease_seconds": lease_seconds, "wait_seconds": wait_seconds},
                          timeout=wait_seconds + 10)

    def heartbeat(self, task_id, token, lease_seconds):
        return self._call("POST", f"/tasks/{task_id}/heartbeat",
                          {"lease_token": token, "lease_seconds": lease_seconds})

    def ack(self, task_id, token, result):
        return self._call("POST", f"/tasks/{task_id}/ack", {"lease_token": token, "result": result})

    def nack(self, task_id, token, error):
        return self._call("POST", f"/tasks/{task_id}/nack", {"lease_token": token, "error": error})


def execute(task: dict) -> dict:
    """The 'business logic'. Replace this with real work."""
    p = task["payload"] if isinstance(task["payload"], dict) else {}
    time.sleep(float(p.get("sleep", 0.5)))
    if p.get("fail"):
        raise RuntimeError("payload asked to fail")
    if task["attempts"] <= int(p.get("fail_first", 0)):
        raise RuntimeError(f"transient failure on attempt {task['attempts']}")
    return {"echo": p, "attempt": task["attempts"]}


def run_one(client: Client, task: dict, worker_id: str, lease_seconds: float, log):
    tid, token = task["id"], task["lease_token"]
    p = task["payload"] if isinstance(task["payload"], dict) else {}
    if p.get("crash"):
        log(f"{tid[:8]} simulating crash: holding the lease and vanishing")
        return  # no heartbeat, no ack: the sweeper will reclaim it

    stop = threading.Event()

    def beat():
        # heartbeat at a third of the lease so one missed beat is survivable
        while not stop.wait(lease_seconds / 3):
            status, body = client.heartbeat(tid, token, lease_seconds)
            if status == 409:
                log(f"{tid[:8]} heartbeat rejected: {body.get('error')}; abandoning")
                stop.set()

    t = threading.Thread(target=beat, daemon=True)
    t.start()
    try:
        result = execute(task)
    except Exception as exc:
        stop.set()
        status, body = client.nack(tid, token, repr(exc))
        log(f"{tid[:8]} attempt {task['attempts']} failed -> {body.get('task', {}).get('status', status)}")
        return
    stop.set()
    status, body = client.ack(tid, token, result)
    if status == 200:
        log(f"{tid[:8]} done (attempt {task['attempts']})")
    else:
        # Lost the lease while working: someone else may have redone the task.
        # This is the at-least-once window; work must be idempotent.
        log(f"{tid[:8]} ack rejected ({status}): {body.get('error')}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--server", default="http://127.0.0.1:8080")
    ap.add_argument("--queue", default="default")
    ap.add_argument("--concurrency", type=int, default=2)
    ap.add_argument("--lease-seconds", type=float, default=10.0)
    ap.add_argument("--worker-id", default=f"worker-{os.getpid()}")
    ap.add_argument("--wait-seconds", type=float, default=5.0,
                    help="long-poll budget per lease call (0 = plain polling)")
    ap.add_argument("--once", action="store_true", help="exit when the queue is empty")
    args = ap.parse_args()

    client = Client(args.server)
    sem = threading.BoundedSemaphore(args.concurrency)
    lock = threading.Lock()

    def log(msg):
        with lock:
            print(f"[{args.worker_id}] {msg}", flush=True)

    log(f"polling {args.queue} with concurrency {args.concurrency}")
    idle_polls = 0
    while True:
        sem.acquire()
        # Long poll: the server holds the request until a task is runnable or
        # wait_seconds pass, so an idle worker costs one request per wait window
        # instead of one every half second, and pickup latency is ~0 not ~poll interval.
        wait = min(args.wait_seconds, 1.0) if args.once else args.wait_seconds
        status, body = client.lease(args.queue, args.worker_id, 1, args.lease_seconds, wait)
        tasks = body.get("tasks", []) if status == 200 else []
        if not tasks:
            sem.release()
            idle_polls += 1
            if args.once and idle_polls > 3:
                log("queue drained, exiting")
                return
            if wait == 0:
                time.sleep(0.5 + random.random() * 0.5)  # plain polling fallback
            continue
        idle_polls = 0

        def job(task=tasks[0]):
            try:
                run_one(client, task, args.worker_id, args.lease_seconds, log)
            finally:
                sem.release()

        threading.Thread(target=job, daemon=True).start()


if __name__ == "__main__":
    main()
