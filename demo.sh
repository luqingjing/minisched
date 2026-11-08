#!/usr/bin/env bash
# One-shot demo: start the server, submit a few tasks (one flaky, one that always
# fails, one whose worker "crashes"), run two workers, print the final stats.
set -euo pipefail
cd "$(dirname "$0")"
PORT=${PORT:-8099}
rm -f demo.db demo.db-wal demo.db-shm

python3 -m app --port "$PORT" --db demo.db --sweep-interval 0.5 >server.log 2>&1 &
SERVER=$!
trap 'kill $SERVER 2>/dev/null || true' EXIT
sleep 0.8

post() { curl -s -X POST "localhost:$PORT/tasks" -H 'Content-Type: application/json' -d "$1" >/dev/null; }
post '{"queue":"default","payload":{"sleep":0.3}}'
post '{"queue":"default","payload":{"sleep":0.3,"fail_first":1},"max_attempts":3}'
post '{"queue":"default","payload":{"fail":true},"max_attempts":2}'
post '{"queue":"default","payload":{"crash":true},"max_attempts":3}'
post '{"queue":"default","payload":{"sleep":0.1},"idempotency_key":"order-42"}'
post '{"queue":"default","payload":{"sleep":0.1},"idempotency_key":"order-42"}'   # replay: no new task
echo "submitted 5 tasks (6 requests, one idempotent replay)"

python3 worker.py --server "http://localhost:$PORT" --worker-id A --lease-seconds 2 --once &
python3 worker.py --server "http://localhost:$PORT" --worker-id B --lease-seconds 2 --once &
wait %2 %3 2>/dev/null || true
sleep 3   # let the crashed worker's lease expire so the sweeper reclaims it

echo; echo "--- /stats"; curl -s "localhost:$PORT/stats"; echo
echo "--- DEAD tasks"
curl -s "localhost:$PORT/tasks?status=DEAD" | python3 -c \
  "import sys,json;[print(t['id'][:8],'attempts=%d'%t['attempts'],t['last_error']) for t in json.load(sys.stdin)['tasks']]"
echo "--- still PENDING (the crashed one, waiting out its backoff)"
curl -s "localhost:$PORT/tasks?status=PENDING" | python3 -c \
  "import sys,json;[print(t['id'][:8],'attempts=%d'%t['attempts'],t['last_error']) for t in json.load(sys.stdin)['tasks']]"
