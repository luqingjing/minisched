# minisched

A small task scheduler with zero dependencies: Python standard library + SQLite, about 500 lines.
It is the core slice of any job queue or task scheduler (SQS, Celery, Sidekiq, a container orchestrator's placement loop): **submit → worker leases a task → heartbeat → ack / nack → retry with backoff → reclaim after a crash**.

The goal is not production use. It is a working, tested implementation of the ideas that come up in every distributed-systems design discussion: idempotency, leases, fencing tokens, at-least-once delivery, exponential backoff.

## Running it

Python 3.10+, nothing to install.

```bash
# tests (20, under a second)
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
worker.py       example worker: polling, heartbeat, ack/nack, crash simulation
tests/          store unit tests (fake clock) + HTTP end-to-end
demo.sh         one-shot demo
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
`next_run_at` means both "run no earlier than" and "retry no earlier than". The lease query is `status='PENDING' AND next_run_at <= now`, covered by the `(queue, status, next_run_at)` index.

## Scaling it up

| Now | Bottleneck | Next step |
|---|---|---|
| Single process + SQLite | single writer, single point of failure | Postgres (`SKIP LOCKED`) or DynamoDB (conditional writes + TTL) |
| Worker polling | idle traffic, latency = poll interval | long polling (as SQS does) or push notifications |
| One table | hot queues, large scans | shard by queue; archive finished tasks to a cold table |
| Sweeper scans the table | slow with many LEASED rows | index scan on `lease_expires_at`, first N only; or DynamoDB TTL / Redis sorted set |
| No priority | urgent tasks wait behind everything else | `priority` column in the ordering and the index; or one queue per priority |
| No observability | nothing visible when it breaks | structured log per transition; Prometheus metrics: queue depth, p99 lease latency, retry rate, DEAD rate |
| DEAD tasks sit there | no DLQ workflow | DLQ inspect / replay endpoints; alert on the DEAD rate |

## Further work

In rough order of difficulty:

1. A `priority` field, with the index and tests to match.
2. Long polling: if the queue is empty, wait up to N seconds before returning.
3. Task dependencies: `depends_on: [id]`, leasable only once every dependency has SUCCEEDED.
4. Swap the store for Postgres and compare `SKIP LOCKED` with `BEGIN IMMEDIATE` here.
5. A chaos test: kill workers at random and check that every task ends SUCCEEDED or DEAD, none stuck in LEASED.
