# minisched

A small task scheduler with zero dependencies: Python standard library + SQLite.
It is the core slice of any job queue or task scheduler (SQS, Celery, Sidekiq, a container orchestrator's placement loop): **submit → worker leases a task → heartbeat → ack / nack → retry with backoff → reclaim after a crash**.

The goal is not production use. It is a working, tested implementation of the ideas that come up in every distributed-systems design discussion: idempotency, leases, fencing tokens, at-least-once delivery, exponential backoff.

## Running it

Python 3.10+, nothing to install.

```bash
# tests
pip install -r requirements-dev.txt   # pytest, the only dev dependency
pytest
```

## State machine

```
PENDING ──lease──▶ LEASED ──ack──▶ SUCCEEDED
   ▲                 │
   │ nack / expiry   │  attempts < max_attempts, with backoff
   └─────────────────┤
                     └──▶ DEAD     attempts exhausted
PENDING ──cancel──▶ CANCELED
```

- `attempts` is incremented **at lease time**, not at nack time. A worker crash or lease expiry therefore counts as an attempt, so a poison task that keeps killing workers ends up DEAD instead of retrying forever.
- Backoff: `base × 2^(attempts-1)`, capped at 300s, ±20% jitter. `nack` can override the delay with `retry_after_seconds` (e.g. when a downstream returns 429).

## Next

1. HTTP layer (submit / lease / heartbeat / ack / nack) with end-to-end tests.
2. An example worker: polling loop, heartbeat thread, ack/nack, crash simulation.
3. A one-shot demo script.
