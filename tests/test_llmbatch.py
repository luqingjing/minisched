"""
llmbatch tests. Everything runs offline: a real scheduler in-process on a
random port, a FakeProvider with scripted failures, and the real worker.
"""
import contextlib
import io
import time
import urllib.request

import pytest

from llmbatch.client import SchedulerClient, request_key
from llmbatch.metrics import Metrics
from llmbatch.providers import FakeProvider, cost_usd, fake_embedding
from llmbatch.schema import validate
from llmbatch.vectors import VectorIndex, cosine
from llmbatch.worker import BadOutput, LLMWorker, _parse_json

SCHEMA = {"type": "object", "required": ["summary", "sentiment"],
          "properties": {"summary": {"type": "string", "minLength": 1},
                         "sentiment": {"type": "string", "enum": ["positive", "neutral", "negative"]},
                         "confidence": {"type": "number", "minimum": 0, "maximum": 1}},
          "additionalProperties": False}


# ---------------------------------------------------------------- pure units
def test_schema_valid():
    assert validate({"summary": "x", "sentiment": "neutral", "confidence": 0.5}, SCHEMA) == []


def test_schema_errors_are_specific():
    errs = validate({"summary": "", "sentiment": "meh", "extra": 1}, SCHEMA)
    assert any("shorter" in e for e in errs)
    assert any("not in" in e for e in errs)
    assert any("unexpected property" in e for e in errs)


def test_schema_type_mismatch_and_arrays():
    assert validate("str", {"type": "object"})
    s = {"type": "array", "items": {"type": "integer"}, "minItems": 2}
    assert validate([1, 2], s) == []
    assert validate([1], s)
    assert validate([1, "x"], s)
    assert validate(None, {"type": ["string", "null"]}) == []


def test_parse_json_tolerates_fences_and_prose():
    assert _parse_json('```json\n{"a": 1}\n```') == {"a": 1}
    assert _parse_json('Sure! {"a": [1,2]} hope that helps') == {"a": [1, 2]}
    with pytest.raises(BadOutput):
        _parse_json("no json here")


def test_request_key_covers_the_whole_payload():
    a = {"kind": "complete", "provider": "fake", "model": "m", "prompt": "p", "temperature": 0}
    assert request_key(a) != request_key(dict(a, temperature=0.7))
    assert request_key(a) != request_key(dict(a, tags={"batch": 1}))  # tags are part of identity
    assert request_key(a) == request_key(dict(a))


def test_cost_and_fake_embedding():
    assert cost_usd("gpt-4o-mini", 1_000_000, 1_000_000) == pytest.approx(0.75)
    assert cost_usd("unknown", 10, 10) == 0.0
    a, b, c = fake_embedding("red apple pie"), fake_embedding("apple pie recipe"), fake_embedding("kubernetes operator")
    assert cosine(a, b) > cosine(a, c)


# ---------------------------------------------------------------- end to end
def run_worker(base, provider, concurrency=4, metrics=None):
    """Drain the queue once with a worker bound to the given provider; return (worker, log lines)."""
    logs = []
    w = LLMWorker(base, {"fake": provider}, default_concurrency=concurrency,
                  lease_seconds=5, metrics=metrics or Metrics(), log=logs.append)
    w.run(once=True, wait_seconds=0.3)
    return w, logs


def test_rate_limit_honours_retry_after_then_succeeds(fast_server):
    c = SchedulerClient(fast_server)
    t = c.complete("rate me", tags={"t": "rl"})
    m = Metrics()
    t0 = time.monotonic()
    run_worker(fast_server, FakeProvider(script=["429:0.3"]), metrics=m)
    done = c.get(t["id"])
    assert done["status"] == "SUCCEEDED"
    assert done["attempts"] == 2
    assert time.monotonic() - t0 >= 0.3  # waited out retry_after
    assert m.requests[("fake", "fake-1", "rate_limited")] == 1
    assert m.requests[("fake", "fake-1", "ok")] == 1
    assert done["result"]["usage"]["cost_usd"] > 0


def test_bad_json_is_retried_and_schema_enforced(fast_server):
    c = SchedulerClient(fast_server)
    t = c.complete("Summarize: the launch went well", schema=SCHEMA)
    _, logs = run_worker(fast_server, FakeProvider(script=["bad_json"]))
    done = c.get(t["id"])
    assert done["status"] == "SUCCEEDED"
    assert done["attempts"] == 2
    assert done["result"]["json"]["sentiment"] == "neutral"
    assert any("bad_output" in line for line in logs)


def test_schema_violation_from_model_burns_attempts_to_dead(fast_server):
    c = SchedulerClient(fast_server)
    t = c.complete("always wrong", schema=SCHEMA, max_attempts=2)
    run_worker(fast_server, FakeProvider(responder=lambda p: '{"summary": "s", "sentiment": "meh"}'))
    done = c.get(t["id"])
    assert (done["status"], done["attempts"]) == ("DEAD", 2)
    assert "schema" in done["last_error"]


def test_permanent_error_drains_fast(fast_server):
    c = SchedulerClient(fast_server)
    t = c.complete("bad request", max_attempts=3, tags={"t": "perm"})
    t0 = time.monotonic()
    run_worker(fast_server, FakeProvider(script=["400", "400", "400"]))
    done = c.get(t["id"])
    assert (done["status"], done["attempts"]) == ("DEAD", 3)
    assert time.monotonic() - t0 < 3.0  # no backoff between attempts


def test_prompt_hash_cache_returns_same_task_and_skips_provider(fast_server):
    c = SchedulerClient(fast_server)
    a = c.complete("cache this exact prompt", model="fake-1")
    b = c.complete("cache this exact prompt", model="fake-1")
    assert a["id"] == b["id"]
    assert c.submitted == 1
    prov = FakeProvider()
    run_worker(fast_server, prov)
    assert prov.calls == 1
    c2 = SchedulerClient(fast_server)
    hit = c2.complete("cache this exact prompt", model="fake-1")
    assert hit["status"] == "SUCCEEDED"
    assert (c2.cache_hits, c2.submitted) == (1, 0)
    different = c2.complete("cache this exact prompt", model="fake-1", temperature=0.9)
    assert different["id"] != a["id"]


def test_per_provider_concurrency_limit(fast_server):
    c = SchedulerClient(fast_server)
    for i in range(6):
        c.complete(f"concurrent {i}")
    prov = FakeProvider(latency=0.15)
    run_worker(fast_server, prov, concurrency=2)
    assert prov.calls == 6
    assert prov.max_concurrent == 2


def test_embeddings_and_vector_search(fast_server):
    c = SchedulerClient(fast_server)
    docs = ["apple pie recipe with cinnamon", "kubernetes operator reconcile loop",
            "fresh apple tart dessert", "postgres index tuning"]
    ids = [c.embed(d, tags={"i": i})["id"] for i, d in enumerate(docs)]
    run_worker(fast_server, FakeProvider())
    idx = VectorIndex()
    assert idx.load_from_tasks(c.list(status="SUCCEEDED")) >= 4
    top = idx.search(fake_embedding("apple dessert pie"), k=2)
    assert {m["text"] for _, _, m in top} <= {docs[0], docs[2]}
    assert c.get(ids[0])["result"]["dim"] == 32


def test_metrics_endpoint_text(fast_server):
    m = Metrics()
    c = SchedulerClient(fast_server)
    c.complete("metrics please", tags={"m": 1})
    run_worker(fast_server, FakeProvider(), metrics=m)
    httpd = m.serve(0)
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{httpd.server_address[1]}/metrics", timeout=5) as r:
            text = r.read().decode()
    finally:
        httpd.shutdown()
        httpd.server_close()
    assert 'llm_requests_total{provider="fake",model="fake-1",outcome="ok"} 1' in text
    assert 'llm_cost_usd_total{provider="fake",model="fake-1"}' in text
    assert 'llm_latency_seconds_bucket{provider="fake",le="+Inf"}' in text
    assert "llm_tokens_total" in text


def test_report_cli_runs(fast_server):
    from llmbatch.cli import main as cli_main

    c = SchedulerClient(fast_server)
    c.complete("for the report")
    run_worker(fast_server, FakeProvider())
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        cli_main(["--scheduler", fast_server, "report"])
    assert "fake/fake-1" in buf.getvalue()
    assert "cost $" in buf.getvalue()
