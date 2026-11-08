"""State-machine tests against the store directly, on a fake clock."""
import os
import sqlite3
import tempfile

import pytest

from app.store import CANCELED, DEAD, LEASED, PENDING, SUCCEEDED, Conflict, TaskStore


# ---------------------------------------------------------------- idempotency
def test_idempotent_submit_returns_same_task(store):
    a, created_a = store.submit("q", {"n": 1}, idempotency_key="k1")
    b, created_b = store.submit("q", {"n": 1}, idempotency_key="k1")
    assert created_a and not created_b
    assert a["id"] == b["id"]
    assert len(store.list(queue="q")) == 1


def test_idempotency_key_with_different_payload_conflicts(store):
    store.submit("q", {"n": 1}, idempotency_key="k1")
    with pytest.raises(Conflict):
        store.submit("q", {"n": 2}, idempotency_key="k1")


def test_same_key_different_queue_is_a_different_task(store):
    a, _ = store.submit("q1", {}, idempotency_key="k")
    b, _ = store.submit("q2", {}, idempotency_key="k")
    assert a["id"] != b["id"]


# ---------------------------------------------------------------- leasing
def test_lease_is_exclusive(store):
    for i in range(3):
        store.submit("q", {"i": i})
    w1 = store.lease("q", "w1", max_tasks=2)
    w2 = store.lease("q", "w2", max_tasks=2)
    assert len(w1) == 2 and len(w2) == 1
    assert {t["id"] for t in w1}.isdisjoint({t["id"] for t in w2})
    assert store.lease("q", "w3") == []


def test_lease_is_fifo_by_next_run_at(store, clock):
    a, _ = store.submit("q", {"a": 1})
    clock.advance(1)
    b, _ = store.submit("q", {"b": 1})
    assert [t["id"] for t in store.lease("q", "w", max_tasks=2)] == [a["id"], b["id"]]


def test_higher_priority_leases_first_then_fifo(store, clock):
    low, _ = store.submit("q", {"p": "low"}, priority=0)
    clock.advance(1)
    high_a, _ = store.submit("q", {"p": "high-a"}, priority=10)
    clock.advance(1)
    high_b, _ = store.submit("q", {"p": "high-b"}, priority=10)
    got = [t["id"] for t in store.lease("q", "w", max_tasks=3)]
    assert got == [high_a["id"], high_b["id"], low["id"]]


def test_priority_does_not_bypass_next_run_at(store):
    store.submit("q", {}, priority=100, delay_seconds=10)
    now_task, _ = store.submit("q", {}, priority=0)
    assert [t["id"] for t in store.lease("q", "w", max_tasks=2)] == [now_task["id"]]


def test_priority_out_of_range_rejected(store):
    with pytest.raises(ValueError):
        store.submit("q", {}, priority=5000)


def test_migration_adds_priority_to_old_database(clock):
    path = os.path.join(tempfile.mkdtemp(), "old.db")
    old = sqlite3.connect(path)
    old.executescript("""
        CREATE TABLE tasks (id TEXT PRIMARY KEY, queue TEXT NOT NULL, payload TEXT NOT NULL,
          status TEXT NOT NULL, attempts INTEGER NOT NULL DEFAULT 0, max_attempts INTEGER NOT NULL DEFAULT 3,
          idempotency_key TEXT, lease_owner TEXT, lease_token TEXT, lease_expires_at REAL,
          next_run_at REAL NOT NULL, result TEXT, last_error TEXT, created_at REAL NOT NULL, updated_at REAL NOT NULL);
        INSERT INTO tasks VALUES ('t1','q','{}','PENDING',0,3,NULL,NULL,NULL,NULL,0,NULL,NULL,0,0);
    """)
    old.commit()
    old.close()
    s = TaskStore(path, clock=clock)
    try:
        assert s.get("t1")["priority"] == 0
        assert [t["id"] for t in s.lease("q", "w")] == ["t1"]
    finally:
        s.close()


def test_delayed_task_not_leasable_until_due(store, clock):
    t, _ = store.submit("q", {}, delay_seconds=10)
    assert store.lease("q", "w") == []
    clock.advance(10)
    assert [x["id"] for x in store.lease("q", "w")] == [t["id"]]


# ---------------------------------------------------------------- ack / fencing
def test_ack_with_token_succeeds(store):
    t, _ = store.submit("q", {})
    leased = store.lease("q", "w")[0]
    done = store.ack(t["id"], leased["lease_token"], {"ok": 1})
    assert done["status"] == SUCCEEDED
    assert done["result"] == {"ok": 1}
    assert done["lease_token"] is None


def test_stale_token_is_fenced_out(store, clock):
    t, _ = store.submit("q", {}, max_attempts=5)
    old = store.lease("q", "w1", lease_seconds=5)[0]
    clock.advance(6)
    assert store.reclaim_expired() == 1  # w1 presumed dead
    clock.advance(1)                      # past the 1s backoff
    new = store.lease("q", "w2")[0]       # handed to w2
    with pytest.raises(Conflict):
        store.ack(t["id"], old["lease_token"])  # w1 comes back late
    with pytest.raises(Conflict):
        store.heartbeat(t["id"], old["lease_token"])
    assert store.ack(t["id"], new["lease_token"])["status"] == SUCCEEDED


def test_heartbeat_extends_lease(store, clock):
    t, _ = store.submit("q", {})
    leased = store.lease("q", "w", lease_seconds=5)[0]
    clock.advance(4)
    store.heartbeat(t["id"], leased["lease_token"], lease_seconds=5)
    clock.advance(4)  # 8s total, would have expired without the heartbeat
    assert store.reclaim_expired() == 0
    assert store.get(t["id"])["status"] == LEASED


# ---------------------------------------------------------------- retry / backoff
def test_nack_retries_with_exponential_backoff_then_dies(store, clock):
    t, _ = store.submit("q", {}, max_attempts=3)
    l1 = store.lease("q", "w")[0]
    r = store.nack(t["id"], l1["lease_token"], "boom")
    assert (r["status"], r["attempts"]) == (PENDING, 1)
    assert r["next_run_at"] - clock() == pytest.approx(1.0)
    assert store.lease("q", "w") == []  # not yet due
    clock.advance(1)
    l2 = store.lease("q", "w")[0]
    r = store.nack(t["id"], l2["lease_token"], "boom")
    assert r["next_run_at"] - clock() == pytest.approx(2.0)
    clock.advance(2)
    l3 = store.lease("q", "w")[0]
    r = store.nack(t["id"], l3["lease_token"], "boom")
    assert (r["status"], r["attempts"]) == (DEAD, 3)
    assert r["last_error"] == "boom"
    assert store.lease("q", "w") == []


def test_nack_can_override_retry_delay(store, clock):
    t, _ = store.submit("q", {})
    leased = store.lease("q", "w")[0]
    r = store.nack(t["id"], leased["lease_token"], "rate limited", retry_after_seconds=30)
    assert r["next_run_at"] - clock() == pytest.approx(30.0)


def test_expired_lease_counts_as_an_attempt(store, clock):
    t, _ = store.submit("q", {}, max_attempts=2)
    store.lease("q", "w", lease_seconds=1)
    clock.advance(2)
    store.reclaim_expired()
    assert store.get(t["id"])["status"] == PENDING
    clock.advance(5)  # past backoff
    store.lease("q", "w", lease_seconds=1)
    clock.advance(2)
    store.reclaim_expired()
    got = store.get(t["id"])
    assert (got["status"], got["attempts"]) == (DEAD, 2)
    assert "lease expired" in got["last_error"]


# ---------------------------------------------------------------- cancel / observability / stats
def test_cancel_only_pending(store):
    t, _ = store.submit("q", {})
    assert store.cancel(t["id"])["status"] == CANCELED
    assert store.lease("q", "w") == []
    t2, _ = store.submit("q", {})
    store.lease("q", "w")
    with pytest.raises(Conflict):
        store.cancel(t2["id"])


def test_stats(store):
    store.submit("q", {})
    store.submit("q", {})
    store.lease("q", "w")
    s = store.stats()["q"]
    assert (s[PENDING], s[LEASED]) == (1, 1)
