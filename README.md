# minisched

A small task scheduler with zero dependencies: Python standard library + SQLite, about 900 lines.
It is the core slice of any job queue or task scheduler (SQS, Celery, Sidekiq, a container orchestrator's placement loop): **submit → worker leases a task → heartbeat → ack / nack → retry with backoff → reclaim after a crash**.

The goal is not production use. It is a working, tested implementation of the ideas that come up in every distributed-systems design discussion: idempotency, leases, fencing tokens, at-least-once delivery, exponential backoff.

The history is incremental. The first five commits build the scheduler from the schema up; the next six add priority, long polling, task dependencies, observability, DLQ replay, and a chaos test. `git log` reads as a walkthrough.

## Running it

Python 3.10+, nothing to install.

```bash
# tests (41, about 3 seconds, chaos test included)
pip install -r requirements-dev.txt   # pytest, the only dev dependency
pytest

# one-shot demo: server + 5 tasks + 2 workers, one of which "crashes"
./demo.sh

# by hand
python3 -m app --port 8080                 # terminal 1: server
python3 worker.py --worker-id A            # terminal 2: worker
python3 worker.py --worker-id B            # terminal 3: another worker
curl -X POST localhost:8080/tasks -H 'Content-Type: application/json' \
     -d '{"queue":"default","payload":{"sleep":2}}'
curl localhost:8080/stats
```

## Layout

```
app/store.py    state machine and every transition (the only place that touches the database)
app/server.py   HTTP routes: parsing and status-code mapping only
worker.py       example worker: long polling, heartbeat, ack/nack, crash simulation
tests/          store unit tests (fake clock) + HTTP end-to-end + chaos test
demo.sh         one-shot demo
```

## API

| Method | Path | Purpose |
|---|---|---|
| POST | `/tasks` | Submit. `{queue, payload, max_attempts?, idempotency_key?, delay_seconds?, priority?, depends_on?}`; `Idempotency-Key` header also accepted |
| GET | `/tasks/{id}` | Get one task |
| GET | `/tasks?queue=&status=&limit=` | List |
| POST | `/tasks/{id}/cancel` | Cancel (PENDING only) |
| POST | `/tasks/{id}/retry` | DLQ replay: DEAD → PENDING, attempts reset. `{max_attempts?}` |
| POST | `/queues/{queue}/retry-dead` | Replay a queue's DEAD tasks, oldest first. `{limit?}` |
| POST | `/queues/{queue}/lease` | Worker claims tasks. `{worker_id, max_tasks?, lease_seconds?, wait_seconds?}`; `wait_seconds > 0` long-polls. Returns tasks with a `lease_token` |
| POST | `/tasks/{id}/heartbeat` | Extend the lease. `{lease_token, lease_seconds?}` |
| POST | `/tasks/{id}/ack` | Success. `{lease_token, result?}` |
| POST | `/tasks/{id}/nack` | This attempt failed. `{lease_token, error?, retry_after_seconds?}` |
| GET | `/stats` | Task counts per queue and status |
| GET | `/metrics` | Prometheus text format: task gauges, transition counters, lease-latency summary |
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
- A task is leasable when: `status = PENDING`, `next_run_at <= now`, every dependency has SUCCEEDED, and it sorts first by `priority DESC, next_run_at`.
- When a dependency ends DEAD or CANCELED, every PENDING dependent cascades to DEAD with `last_error` naming the dependency.

## Design decisions

**1. Leases, not locks.**
A worker that claims a task gets a lease with an expiry, not permanent ownership. A healthy worker renews it with heartbeats; a dead worker's lease simply expires and the background sweeper returns the task to PENDING. Nothing has to detect whether a worker is alive; time is the detector. This is the same model as Kafka consumer group sessions, the DynamoDB lock client, and the agent heartbeats in most container orchestrators.

**2. Fencing tokens against zombie workers.**
Every lease issues a fresh `lease_token`, and heartbeat / ack / nack must present it: `WHERE status='LEASED' AND lease_token=?`. Scenario: worker A pauses for 40 seconds in GC, its lease expires, the task is handed to B; A wakes up and acks, and gets a 409 instead of overwriting B's progress. Without this, leases give you liveness but not safety.

**3. At-least-once, not exactly-once.**
If a worker finishes the work and dies before acking, the task runs again. The scheduler cannot close that window on its own (two generals), so consumers must be idempotent. In production you close it with a business-level idempotency key, or by committing the work and the ack in the same transaction.

**4. Idempotent submission.**
Unique index on `(queue, idempotency_key)`. Same key and same payload returns the original task (200, not 201); same key with a different payload is a 409 rather than a silent overwrite. These are the Stripe API semantics.

**5. Leasing is one atomic transaction.**
`BEGIN IMMEDIATE` + `SELECT … LIMIT n` + a conditional `UPDATE`. Two workers leasing at the same moment never receive the same task (tested). SQLite is single-writer, which keeps this simple; on Postgres this becomes `SELECT … FOR UPDATE SKIP LOCKED`.

**6. One field for delay and retry.**
`next_run_at` means both "run no earlier than" and "retry no earlier than". The lease query is `status='PENDING' AND next_run_at <= now`, covered by the `(queue, status, priority, next_run_at)` index.

## Scaling it up

| Now | Bottleneck | Next step |
|---|---|---|
| Single process + SQLite | single writer, single point of failure | Postgres (`SKIP LOCKED`) or DynamoDB (conditional writes + TTL) |
| Worker polling | idle traffic, latency = poll interval | Done in-process: long polling. Across instances: server-side long poll or `LISTEN/NOTIFY` |
| One table | hot queues, large scans | shard by queue; archive finished tasks to a cold table |
| Sweeper scans the table | slow with many LEASED rows | index scan on `lease_expires_at`, first N only; or DynamoDB TTL / Redis sorted set |
| Priority | low priority starves | Done. Next: aging so effective priority rises with wait time |
| Observability | nothing visible when it breaks | Done: structured events + `/metrics`. Next: histogram instead of summary for p99; alert on the `fenced` rate |
| DEAD tasks sit there | no DLQ workflow | Done: single and bulk replay. Next: alert on the DEAD rate |

## Further work

1. Priority aging, so a low-priority task cannot starve.
2. Replace the lease-latency summary in `/metrics` with a fixed-bucket histogram to get p99.
3. Cron-style schedules: a `schedules` table the sweeper materialises into tasks when due.
4. Swap the store for Postgres and run the chaos test against two server instances.
5. Graceful shutdown: on SIGTERM stop leasing, wait for in-flight tasks to ack, then exit.
