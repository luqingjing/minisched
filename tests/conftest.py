"""Shared fixtures: a store on a fake clock."""
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from app.store import TaskStore  # noqa: E402


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
    """In-memory store driven by the fake clock."""
    s = TaskStore(":memory:", clock=clock)
    yield s
    s.close()
