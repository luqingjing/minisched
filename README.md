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

# by hand
python3 -m app --port 8080                 # terminal 1: server
curl -X POST localhost:8080/tasks -H 'Content-Type: application/json' \
     -d '{"queue":"default","payload":{"sleep":2}}'
curl localhost:8080/stats
```

## Layout

```
app/store.py    state machine and every transition (the only place that touches the database)
app/server.py   HTTP routes: parsing and status-code mapping only
tests/          store unit tests (fake clock) + HTTP end-to-end
```

## API

| Method | Path | Purpose |
|---|---|---|
| POST | `/tasks` | Submit. `{queue, payload, max_attempts?, idempotency_key?, delay_seconds?}`; `Idempotency-Key` header also accepted |
| GET | `/tasks/{id}` | Get one task |
| GET | `/tasks?queue=&status=&limit=` | List |
| POST | `/tasks/{id}/cancel` | Cancel (PENDING only) |
| POST | `/queues/{queue}/lease` | Worker claims tasks. `{worker_id, max_tasks?, lease_seconds?}`. Returns tasks with a `lease_token` |
| POST | `/tasks/{id}/heartbeat` | Extend the lease. `{lease_token, lease_seconds?}` |
| POST | `/tasks/{id}/ack` | Success. `{lease_token, result?}` |
| POST | `/tasks/{id}/nack` | This attempt failed. `{lease_token, error?, retry_after_seconds?}` |
| GET | `/stats` | Task counts per queue and status |
| GET | `/healthz` | Liveness |

Errors: 400 bad input; 404 unknown task; **409 state or token mismatch** (idempotency-key conflict, stale `lease_token`, duplicate ack, cancelling a non-PENDING task).

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

1. An example worker: polling loop, heartbeat thread, ack/nack, crash simulation.
2. A one-shot demo script.
