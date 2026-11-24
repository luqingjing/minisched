#!/usr/bin/env bash
# llmbatch demo: scheduler + fake provider. Shows dedupe at submit, cache hits after
# results exist, the per-model cost/latency report, and vector search.
set -euo pipefail
cd "$(dirname "$0")"
PORT=${PORT:-8111}
rm -f llmdemo.db llmdemo.db-wal llmdemo.db-shm
python3 -m app --port "$PORT" --db llmdemo.db --sweep-interval 0.5 --log-level WARNING >/dev/null 2>&1 &
trap 'kill $! 2>/dev/null || true' EXIT
sleep 0.8
S="http://127.0.0.1:$PORT"
cat > /tmp/llmbatch_prompts.jsonl <<'J'
{"prompt": "Summarize: Q3 revenue grew 12% while churn fell to 2.1%.", "tags": {"doc": 1}}
{"prompt": "Summarize: The migration caused a 40 minute outage in us-east-1.", "tags": {"doc": 2}}
{"prompt": "Summarize: Q3 revenue grew 12% while churn fell to 2.1%.", "tags": {"doc": 1}}
{"text": "apple pie recipe with cinnamon"}
{"text": "kubernetes operator reconcile loop"}
{"text": "postgres index tuning for slow queries"}
J
cat > /tmp/llmbatch_schema.json <<'J'
{"type":"object","required":["summary","sentiment"],"properties":{"summary":{"type":"string"},"sentiment":{"type":"string","enum":["positive","neutral","negative"]},"confidence":{"type":"number","minimum":0,"maximum":1}}}
J
echo "--- submit (line 3 duplicates line 1)"
python3 -m llmbatch --scheduler "$S" submit /tmp/llmbatch_prompts.jsonl --schema /tmp/llmbatch_schema.json
echo "--- worker (fake provider)"
python3 -m llmbatch worker --scheduler "$S" --metrics-port 0 --once
echo "--- submit again: everything served from cache"
python3 -m llmbatch --scheduler "$S" submit /tmp/llmbatch_prompts.jsonl --schema /tmp/llmbatch_schema.json
echo "--- report"; python3 -m llmbatch --scheduler "$S" report
echo "--- search"; python3 -m llmbatch --scheduler "$S" search "apple dessert" -k 2
