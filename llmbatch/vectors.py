"""
Tiny in-memory vector index built from SUCCEEDED embed tasks.

This is the "vector search" piece in its smallest honest form: brute-force
cosine similarity over a few thousand vectors. Swapping it for pgvector,
FAISS or a managed store changes `search`, not the pipeline around it.
"""
from __future__ import annotations

import math
from typing import Iterable


def cosine(a: list[float], b: list[float]) -> float:
    dot = sum(x * y for x, y in zip(a, b))
    na = math.sqrt(sum(x * x for x in a)) or 1.0
    nb = math.sqrt(sum(y * y for y in b)) or 1.0
    return dot / (na * nb)


class VectorIndex:
    def __init__(self):
        self.items: list[tuple[str, list[float], dict]] = []   # (id, vector, meta)

    def add(self, item_id: str, vector: list[float], meta: dict | None = None) -> None:
        self.items.append((item_id, vector, meta or {}))

    def load_from_tasks(self, tasks: Iterable[dict]) -> int:
        """Pull every SUCCEEDED embed task into the index. Returns how many."""
        n = 0
        for t in tasks:
            if t.get("status") != "SUCCEEDED" or t["payload"].get("kind") != "embed":
                continue
            self.add(t["id"], t["result"]["vector"],
                     {"text": t["payload"]["text"], **t["payload"].get("tags", {})})
            n += 1
        return n

    def search(self, query_vector: list[float], k: int = 5) -> list[tuple[float, str, dict]]:
        scored = [(cosine(query_vector, v), i, m) for i, v, m in self.items]
        scored.sort(key=lambda x: -x[0])
        return scored[:k]

    def __len__(self):
        return len(self.items)
