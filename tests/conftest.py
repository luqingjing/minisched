"""Shared fixtures: a store on a fake clock, and a real HTTP server on a random port."""
import json
import os
import sys
import threading
import urllib.error
import urllib.request

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from app.server import serve  # noqa: E402
from app.store import BackoffPolicy, TaskStore  # noqa: E402


class FakeClock:
    def __init__(self, t=1000.0):
        self.t = t

    def __call__(self):
        return self.t

    def advance(self, seconds):
        self.t += seconds


@pytest.fixture
def clock():
    return FakeClock()


@pytest.fixture
def store(clock):
    """In-memory store with deterministic backoff (1s, 2s, 4s ...) driven by the fake clock."""
    s = TaskStore(":memory:", BackoffPolicy(1.0, 60.0, 0.0), clock=clock)
    yield s
    s.close()


def _serve(sweep_interval=0.2):
    httpd = serve("127.0.0.1", 0, ":memory:", sweep_interval=sweep_interval, quiet=True)
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    return httpd


def _shutdown(httpd):
    httpd.sweeper.stop()
    httpd.shutdown()
    httpd.server_close()


@pytest.fixture
def server():
    """Base URL of a real scheduler on a random port, torn down after the test."""
    httpd = _serve()
    yield f"http://127.0.0.1:{httpd.server_address[1]}"
    _shutdown(httpd)


def call(base, method, path, body=None, headers=None, timeout=5):
    """Minimal JSON client used by the HTTP tests. Returns (status, parsed body)."""
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(base + path, data=data, method=method,
                                 headers={"Content-Type": "application/json", **(headers or {})})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, json.loads(r.read() or b"{}")
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read() or b"{}")
