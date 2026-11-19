"""
Chaos test: many workers misbehaving at random against one store, real clock,
short leases, fast sweeper. Afterwards we check the invariants the design
promises, not any particular schedule.

Worker behaviours per attempt (chosen at random):
  ok       do the work, ack
  fail     do the work, nack
  crash    take the lease and vanish (no heartbeat, no ack)  -> sweeper reclaims
  slow     work longer than the lease without heartbeating, then try to ack
           -> ack must be rejected (fencing), task was handed to someone else
  careful  work longer than the lease but heartbeat -> ack must succeed

Invariants checked at the end:
  1. nothing is left LEASED; every task is SUCCEEDED or DEAD
  2. a SUCCEEDED task was acked exactly once; a DEAD task never successfully acked
  3. attempts never exceeds max_attempts; DEAD means attempts == max_attempts
  4. every SUCCEEDED task was executed at least once (at-least-once delivery)
  5. the fencing path was actually exercised (some stale acks were rejected)
"""
import collections
import random
import threading
import time

from app.store import DEAD, LEASED, PENDING, SUCCEEDED, BackoffPolicy, Conflict, Sweeper, TaskStore

LEASE = 0.2
N_TASKS, N_WORKERS, MAX_ATTEMPTS = 60, 6, 4


def test_invariants_hold_under_random_failures():
    store = TaskStore(":memory:", BackoffPolicy(base_seconds=0.05, max_seconds=0.2, jitter=0.5))
    sweeper = Sweeper(store, interval=0.05)
    sweeper.start()
    ids = [store.submit("chaos", {"i": i}, max_attempts=MAX_ATTEMPTS)[0]["id"] for i in range(N_TASKS)]

    acks = collections.Counter()        # task -> successful acks
    executions = collections.Counter()  # task -> times work ran
    stale_rejections = [0]
    book = threading.Lock()
    stop = threading.Event()

    def worker(seed):
        rng = random.Random(seed)
        wid = f"w{seed}"
        while not stop.is_set():
            tasks = store.lease("chaos", wid, lease_seconds=LEASE, wait_seconds=0.1)
            if not tasks:
                continue
            t = tasks[0]
            tid, tok = t["id"], t["lease_token"]
            mode = rng.choices(["ok", "fail", "crash", "slow", "careful"], weights=[5, 2, 1, 1, 1])[0]
            if mode == "crash":
                continue  # vanish: no ack, lease expires
            if mode in ("slow", "careful"):
                end = time.monotonic() + LEASE * 2.5
                while time.monotonic() < end:
                    time.sleep(LEASE / 4)
                    if mode == "careful":
                        try:
                            store.heartbeat(tid, tok, lease_seconds=LEASE)
                        except Conflict:
                            break
            else:
                time.sleep(rng.uniform(0, LEASE / 4))
            with book:
                executions[tid] += 1
            try:
                if mode == "fail":
                    store.nack(tid, tok, "chaos nack")
                else:
                    store.ack(tid, tok, {"by": wid})
                    with book:
                        acks[tid] += 1
            except Conflict:
                with book:
                    stale_rejections[0] += 1

    threads = [threading.Thread(target=worker, args=(i,), daemon=True) for i in range(N_WORKERS)]
    for th in threads:
        th.start()

    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        s = store.stats().get("chaos", {})
        if s.get(PENDING, 0) == 0 and s.get(LEASED, 0) == 0:
            break
        time.sleep(0.1)
    stop.set()
    for th in threads:
        th.join(timeout=5)
    sweeper.stop()

    final = {tid: store.get(tid) for tid in ids}
    statuses = collections.Counter(t["status"] for t in final.values())

    # 1. terminal states only
    assert statuses[LEASED] == 0, "a lease leaked past the sweeper"
    assert statuses[PENDING] == 0, "work did not drain within the time limit"
    assert statuses[SUCCEEDED] + statuses[DEAD] == N_TASKS
    # 2. exactly one successful ack per success; never for DEAD
    for tid, t in final.items():
        if t["status"] == SUCCEEDED:
            assert acks[tid] == 1, f"{tid} acked {acks[tid]} times"
        else:
            assert acks[tid] == 0, f"DEAD task {tid} has a successful ack"
    # 3. attempt budget respected
    for t in final.values():
        assert t["attempts"] <= t["max_attempts"]
        if t["status"] == DEAD:
            assert t["attempts"] == t["max_attempts"]
    # 4. at-least-once
    for tid, t in final.items():
        if t["status"] == SUCCEEDED:
            assert executions[tid] >= 1
    # 5. the fencing path ran
    assert stale_rejections[0] > 0, "no stale ack was ever rejected; fencing untested"
    assert store.counters["lease_expired"] > 0, "sweeper never reclaimed anything"

    print(f"\n[chaos] {dict(statuses)}  stale_acks_rejected={stale_rejections[0]} "
          f"reclaimed={store.counters['lease_expired']} "
          f"duplicate_executions={sum(max(0, n - 1) for n in executions.values())}")
    store.close()
