"""State-machine tests against the store directly, on a fake clock."""

import pytest

from app.store import CANCELED, LEASED, PENDING, SUCCEEDED, Conflict


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
