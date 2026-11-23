"""
CLI for the producer side.

  python3 -m llmbatch submit prompts.jsonl [--model M] [--provider P] [--schema schema.json]
      one JSON object per line: {"prompt": "...", "tags": {...}}  or  {"text": "..."} for embeddings
  python3 -m llmbatch report
      per-model totals: tasks, tokens, cost, latency, cache hits, DEAD with reasons
  python3 -m llmbatch search "query text" [-k 5]
      embed the query (same provider/model as the index) and show nearest texts
"""
from __future__ import annotations

import argparse
import collections
import json
import sys

from .client import SchedulerClient
from .providers import make_provider
from .vectors import VectorIndex


def cmd_submit(a):
    c = SchedulerClient(a.scheduler)
    schema = json.load(open(a.schema)) if a.schema else None
    ids = []
    for line in open(a.file):
        line = line.strip()
        if not line:
            continue
        row = json.loads(line)
        if "text" in row and "prompt" not in row:
            t = c.embed(row["text"], model=a.embed_model, provider=a.provider, tags=row.get("tags"))
        else:
            t = c.complete(row["prompt"], model=a.model, provider=a.provider, system=row.get("system", a.system),
                           schema=row.get("schema", schema), tags=row.get("tags"), priority=row.get("priority", 0))
        ids.append(t["id"])
    print(f"submitted {c.submitted} new, {c.cache_hits} served from cache, {len(ids) - c.submitted - c.cache_hits} already queued")
    if a.wait:
        done = c.wait(ids, timeout=a.wait)
        by = collections.Counter(t["status"] for t in done.values())
        print(dict(by))


def cmd_report(a):
    c = SchedulerClient(a.scheduler)
    tasks = c.list()
    agg = collections.defaultdict(lambda: {"tasks": 0, "ok": 0, "dead": 0, "prompt_tokens": 0,
                                           "completion_tokens": 0, "cost_usd": 0.0, "latency_ms": []})
    dead_reasons = collections.Counter()
    for t in tasks:
        key = (t["payload"].get("provider"), t["payload"].get("model"))
        r = agg[key]
        r["tasks"] += 1
        if t["status"] == "SUCCEEDED":
            r["ok"] += 1
            u = t["result"].get("usage", {})
            r["prompt_tokens"] += u.get("prompt_tokens", 0)
            r["completion_tokens"] += u.get("completion_tokens", 0)
            r["cost_usd"] += u.get("cost_usd", 0.0)
            r["latency_ms"].append(t["result"].get("latency_ms", 0))
        elif t["status"] == "DEAD":
            r["dead"] += 1
            dead_reasons[(t["last_error"] or "")[:50]] += 1
    print(f"{'provider/model':28} {'tasks':>5} {'ok':>4} {'dead':>4} {'prompt':>8} {'compl':>7} {'cost $':>9} {'p50 ms':>7} {'p95 ms':>7}")
    for (p, m), r in sorted(agg.items()):
        lat = sorted(r["latency_ms"])
        p50 = lat[len(lat) // 2] if lat else 0
        p95 = lat[int(len(lat) * 0.95)] if lat else 0
        print(f"{(p or '?') + '/' + (m or '?'):28} {r['tasks']:>5} {r['ok']:>4} {r['dead']:>4} "
              f"{r['prompt_tokens']:>8} {r['completion_tokens']:>7} {r['cost_usd']:>9.5f} {p50:>7} {p95:>7}")
    if dead_reasons:
        print("\nDEAD reasons:")
        for reason, n in dead_reasons.most_common():
            print(f"  {n:>3}  {reason}")


def cmd_search(a):
    c = SchedulerClient(a.scheduler)
    idx = VectorIndex()
    n = idx.load_from_tasks(c.list(status="SUCCEEDED"))
    if not n:
        print("index is empty: submit some {\"text\": ...} rows first")
        return
    provider = make_provider(a.provider)
    q = provider.embed(a.embed_model, a.query).vector
    for score, tid, meta in idx.search(q, k=a.k):
        print(f"{score:6.3f}  {tid[:8]}  {meta.get('text', '')[:80]}")


def main(argv=None):
    ap = argparse.ArgumentParser(prog="llmbatch")
    ap.add_argument("--scheduler", default="http://127.0.0.1:8080")
    ap.add_argument("--provider", default=None, help="fake|openai|anthropic (default $LLM_PROVIDER or fake)")
    ap.add_argument("--model", default="fake-1")
    ap.add_argument("--embed-model", default="fake-embed")
    sp = ap.add_subparsers(dest="cmd", required=True)
    s = sp.add_parser("submit"); s.add_argument("file"); s.add_argument("--schema"); s.add_argument("--system", default="")
    s.add_argument("--wait", type=float, default=0, help="seconds to wait for results")
    sp.add_parser("report")
    q = sp.add_parser("search"); q.add_argument("query"); q.add_argument("-k", type=int, default=5)
    a = ap.parse_args(argv)
    import os
    a.provider = a.provider or os.environ.get("LLM_PROVIDER", "fake")
    {"submit": cmd_submit, "report": cmd_report, "search": cmd_search}[a.cmd](a)


if __name__ == "__main__":
    main()
