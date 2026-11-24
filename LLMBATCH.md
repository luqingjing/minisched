# llmbatch

An LLM batch-processing layer on top of minisched. The scheduler owns leases,
retries, backoff, idempotency and crash recovery; llmbatch maps the problems
every LLM integration runs into onto those primitives.

Zero dependencies. Fully testable offline: `FakeProvider` can be scripted to
return 429s, 5xx, malformed output and latency, so no test needs an API key.

## Running it

```bash
pytest tests/test_llmbatch.py                        # 15 offline tests; each end-to-end test starts its own scheduler

python3 -m app --port 8080                           # terminal 1: scheduler
python3 -m llmbatch worker --concurrency 4           # terminal 2: LLM worker (FakeProvider by default)
python3 -m llmbatch submit prompts.jsonl --schema schema.json   # terminal 3: submit a batch
python3 -m llmbatch report                           # tokens / cost / latency per model
python3 -m llmbatch search "apple dessert" -k 3      # nearest embedded texts

# real providers (written against the public APIs; exercise one request first)
export LLM_PROVIDER=openai OPENAI_API_KEY=sk-...
python3 -m llmbatch worker --provider openai
python3 -m llmbatch --provider openai --model gpt-4o-mini --embed-model text-embedding-3-small submit prompts.jsonl
```

`prompts.jsonl` holds one object per line: `{"prompt": "...", "tags": {...}}`
for a completion, `{"text": "..."}` for an embedding. `./demo_llm.sh` runs the
whole loop against the fake provider.

## Layout

```
llmbatch/providers.py   Provider interface; FakeProvider (scriptable failures); OpenAI / Anthropic over urllib; price table
llmbatch/client.py      producer side: prompt hash = idempotency key = cache key; wait / list
llmbatch/worker.py      consumer side: per-provider concurrency, error class -> scheduler action, schema validation, usage accounting
llmbatch/schema.py      small JSON Schema validator (type / required / enum / items / min / max ...)
llmbatch/metrics.py     Prometheus text: requests by outcome, tokens, cost, latency histogram, in-flight
llmbatch/vectors.py     in-memory cosine index loaded from succeeded embed tasks
llmbatch/cli.py         submit / report / search
tests/test_llmbatch.py  offline end-to-end tests
```

## How each integration problem maps onto the scheduler

| Problem | What llmbatch does | Scheduler primitive it lands on |
|---|---|---|
| 429 rate limits | provider raises `RateLimited(retry_after)`; worker nacks with `retry_after_seconds` | nack's retry override: wait exactly as long as the provider asked |
| 5xx / timeouts | `TransientError`; plain nack | exponential backoff with jitter |
| 4xx permanent errors | `PermanentError`; nack with `retry_after_seconds=0` | remaining attempts drain straight into DEAD; replayable later via `retry` |
| per-provider concurrency | one `BoundedSemaphore` per provider inside the worker | none: the scheduler is unaware, so one worker process is one concurrency budget |
| not paying twice for the same prompt | sha256 of the whole payload is the `idempotency_key` | idempotent submit: same key returns the same task, with its result if already SUCCEEDED |
| structured output | request JSON mode, parse tolerantly (code fences, leading prose), validate against the schema, raise `BadOutput` on failure | treated as transient; repeated failures burn attempts to DEAD with the field-level error in `last_error` |

Crash recovery, heartbeats and fencing are inherited: the LLM worker is just
another minisched worker.

## Design notes

**The cache key is the idempotency key.** There is no second cache table. A
cache hit means "this task already SUCCEEDED", so cache and task state can
never disagree. The price is that `tags` are part of the payload and therefore
part of a request's identity: re-running a batch with the same tags hits the
cache, a new batch with different tags does not. Omit tags for cross-batch
dedupe. (Excluding tags from the key while keeping them in the payload would
trip the scheduler's 409 on key reuse with a different payload.)

**Permanent errors don't get a shortcut to DEAD.** The scheduler has no
"fail now" call on purpose: a worker shouldn't be able to bypass the retry
budget. `retry_after_seconds=0` achieves the same effect (three 400s go DEAD
within milliseconds) while keeping one record per attempt.

**Schema failures count as transient.** Model output is stochastic; one retry
usually fixes it. A prompt that is genuinely wrong fails consistently and ends
DEAD with `schema: $.sentiment: 'meh' not in [...]` in `last_error`, which
points at the prompt rather than the pipeline. Both cases are tested.

**Cost is an estimate.** `PRICES` is a static table multiplied by token
counts. The provider's bill is authoritative; this number answers "what did
this batch cost" and "which template is expensive".

**Vector search is the smallest honest version.** Brute-force cosine over a
few thousand vectors. `FakeProvider` embeddings are a deterministic
bag-of-words hash, so offline tests can assert that "apple pie" is closer to
"apple tart" than to "kubernetes" without mocking the distance.

## What changes at scale

| Now | Next |
|---|---|
| concurrency limited per worker process | global limiter (Redis token bucket), or let the scheduler cap concurrent LEASED tasks per queue |
| each task backs off on 429 independently | provider-level circuit breaker: pause all tasks for that provider on sustained 429s |
| static price table | load from config; aggregate cost by `tags` (customer, prompt template) |
| brute-force vectors | pgvector / FAISS; persist embeddings outside task results |
| no prompt versioning | add `prompt_version` to the payload so old answers are never served for a new template |
| one call per task | provider batch APIs (OpenAI Batch, Anthropic Message Batches): bundle a queue's tasks, collect asynchronously, roughly half the cost |
