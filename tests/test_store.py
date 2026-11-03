"""State-machine tests against the store directly, on a fake clock."""

import pytest

from app.store import Conflict


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
