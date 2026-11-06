"""End-to-end over real HTTP against a scheduler on a random port."""
import time

from conftest import call


def test_health(server):
    assert call(server, "GET", "/healthz") == (200, {"ok": True})


def test_submit_validation(server):
    s, b = call(server, "POST", "/tasks", {"payload": {}})
    assert s == 400 and "queue" in b["error"]
    s, _ = call(server, "POST", "/tasks", {"queue": "q", "payload": {}, "max_attempts": 0})
    assert s == 400


def test_idempotency_header(server):
    body = {"queue": "api", "payload": {"x": 1}}
    s1, b1 = call(server, "POST", "/tasks", body, {"Idempotency-Key": "abc"})
    s2, b2 = call(server, "POST", "/tasks", body, {"Idempotency-Key": "abc"})
    assert (s1, s2) == (201, 200)
    assert b1["task"]["id"] == b2["task"]["id"]
    s3, _ = call(server, "POST", "/tasks", {"queue": "api", "payload": {"x": 2}}, {"Idempotency-Key": "abc"})
    assert s3 == 409


def test_full_lifecycle(server):
    _, created = call(server, "POST", "/tasks", {"queue": "life", "payload": {"job": 1}})
    tid = created["task"]["id"]
    s, leased = call(server, "POST", "/queues/life/lease", {"worker_id": "w1"})
    assert s == 200 and leased["tasks"][0]["id"] == tid
    token = leased["tasks"][0]["lease_token"]
    s, _ = call(server, "POST", f"/tasks/{tid}/heartbeat", {"lease_token": token})
    assert s == 200
    s, done = call(server, "POST", f"/tasks/{tid}/ack", {"lease_token": token, "result": 42})
    assert (s, done["task"]["status"], done["task"]["result"]) == (200, "SUCCEEDED", 42)
    s, _ = call(server, "POST", f"/tasks/{tid}/ack", {"lease_token": token})
    assert s == 409  # already acked
    _, got = call(server, "GET", f"/tasks/{tid}")
    assert got["task"]["attempts"] == 1
    _, lst = call(server, "GET", "/tasks?queue=life&status=SUCCEEDED")
    assert [t["id"] for t in lst["tasks"]] == [tid]


def test_sweeper_reclaims_crashed_worker(server):
    _, created = call(server, "POST", "/tasks", {"queue": "crash", "payload": {}, "max_attempts": 5})
    tid = created["task"]["id"]
    _, leased = call(server, "POST", "/queues/crash/lease", {"worker_id": "zombie", "lease_seconds": 0.3})
    assert len(leased["tasks"]) == 1
    deadline = time.time() + 3
    while time.time() < deadline:
        _, got = call(server, "GET", f"/tasks/{tid}")
        if got["task"]["status"] == "PENDING":
            break
        time.sleep(0.1)
    assert got["task"]["status"] == "PENDING"
    assert "lease expired" in got["task"]["last_error"]
    assert got["task"]["lease_owner"] is None


def test_unknown_task_and_route(server):
    assert call(server, "GET", "/tasks/nope")[0] == 404
    assert call(server, "GET", "/nothing")[0] == 404
