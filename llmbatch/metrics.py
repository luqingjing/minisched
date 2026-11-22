"""
Worker-side metrics: counters, token/cost totals and a latency histogram,
rendered in Prometheus text format. Served by the worker on --metrics-port.
Labels are bounded (provider, model, outcome), never task ids or prompts.
"""
from __future__ import annotations

import collections
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

BUCKETS = (0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60)


class Metrics:
    def __init__(self):
        self._lock = threading.Lock()
        self.requests = collections.Counter()       # (provider, model, outcome) -> n
        self.tokens = collections.Counter()         # (provider, model, kind) -> n
        self.cost = collections.Counter()           # (provider, model) -> usd
        self.lat_buckets = collections.Counter()    # (provider, le) -> n
        self.lat_sum = collections.Counter()        # provider -> seconds
        self.lat_count = collections.Counter()      # provider -> n
        self.inflight = collections.Counter()       # provider -> n

    def observe(self, provider: str, model: str, outcome: str, latency: float,
                prompt_tokens: int = 0, completion_tokens: int = 0, cost_usd: float = 0.0):
        with self._lock:
            self.requests[(provider, model, outcome)] += 1
            if prompt_tokens:
                self.tokens[(provider, model, "prompt")] += prompt_tokens
            if completion_tokens:
                self.tokens[(provider, model, "completion")] += completion_tokens
            if cost_usd:
                self.cost[(provider, model)] += cost_usd
            for b in BUCKETS:
                if latency <= b:
                    self.lat_buckets[(provider, str(b))] += 1
            self.lat_buckets[(provider, "+Inf")] += 1
            self.lat_sum[provider] += latency
            self.lat_count[provider] += 1

    def set_inflight(self, provider: str, delta: int):
        with self._lock:
            self.inflight[provider] += delta

    def text(self) -> str:
        with self._lock:
            L = ["# HELP llm_requests_total Provider calls by outcome (ok, rate_limited, transient, permanent, bad_output).",
                 "# TYPE llm_requests_total counter"]
            for (p, m, o), n in sorted(self.requests.items()):
                L.append(f'llm_requests_total{{provider="{p}",model="{m}",outcome="{o}"}} {n}')
            L += ["# HELP llm_tokens_total Tokens billed.", "# TYPE llm_tokens_total counter"]
            for (p, m, k), n in sorted(self.tokens.items()):
                L.append(f'llm_tokens_total{{provider="{p}",model="{m}",kind="{k}"}} {n}')
            L += ["# HELP llm_cost_usd_total Estimated spend from the price table.", "# TYPE llm_cost_usd_total counter"]
            for (p, m), v in sorted(self.cost.items()):
                L.append(f'llm_cost_usd_total{{provider="{p}",model="{m}"}} {v:.6f}')
            L += ["# HELP llm_inflight Calls currently in progress.", "# TYPE llm_inflight gauge"]
            for p, n in sorted(self.inflight.items()):
                L.append(f'llm_inflight{{provider="{p}"}} {n}')
            L += ["# HELP llm_latency_seconds Provider call latency.", "# TYPE llm_latency_seconds histogram"]
            for p in sorted(self.lat_count):
                for b in list(BUCKETS) + ["+Inf"]:
                    L.append(f'llm_latency_seconds_bucket{{provider="{p}",le="{b}"}} {self.lat_buckets[(p, str(b))]}')
                L.append(f'llm_latency_seconds_sum{{provider="{p}"}} {self.lat_sum[p]:.6f}')
                L.append(f'llm_latency_seconds_count{{provider="{p}"}} {self.lat_count[p]}')
            return "\n".join(L) + "\n"

    def serve(self, port: int) -> ThreadingHTTPServer:
        metrics = self

        class H(BaseHTTPRequestHandler):
            def log_message(self, *a):  # quiet
                pass

            def do_GET(self):
                if self.path != "/metrics":
                    self.send_response(404); self.end_headers(); return
                data = metrics.text().encode()
                self.send_response(200)
                self.send_header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

        httpd = ThreadingHTTPServer(("127.0.0.1", port), H)
        threading.Thread(target=httpd.serve_forever, daemon=True).start()
        return httpd
