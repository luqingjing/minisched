"""
HTTP layer. Zero dependencies: http.server + json.

Routes
------
  GET  /healthz
  GET  /stats
  POST /tasks                         {queue, payload, max_attempts?, idempotency_key?, delay_seconds?,
                                       priority?, depends_on?: [task_id]}
  GET  /tasks?queue=&status=&limit=
  GET  /tasks/{id}
  POST /tasks/{id}/cancel
  POST /queues/{queue}/lease          {worker_id, max_tasks?, lease_seconds?, wait_seconds?}
  POST /tasks/{id}/heartbeat          {lease_token, lease_seconds?}
  POST /tasks/{id}/ack                {lease_token, result?}
  POST /tasks/{id}/nack               {lease_token, error?, retry_after_seconds?}

Idempotency-Key may also be sent as a header on POST /tasks (Stripe style).
"""
from __future__ import annotations

import json
import re
import sys
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit

from .store import Conflict, NotFound, Sweeper, TaskStore

ROUTES = [
    ("GET",  re.compile(r"^/healthz$"),                     "healthz"),
    ("GET",  re.compile(r"^/stats$"),                       "stats"),
    ("POST", re.compile(r"^/tasks$"),                       "submit"),
    ("GET",  re.compile(r"^/tasks$"),                       "list"),
    ("GET",  re.compile(r"^/tasks/(?P<id>[^/]+)$"),         "get"),
    ("POST", re.compile(r"^/tasks/(?P<id>[^/]+)/cancel$"),  "cancel"),
    ("POST", re.compile(r"^/queues/(?P<queue>[^/]+)/lease$"), "lease"),
    ("POST", re.compile(r"^/tasks/(?P<id>[^/]+)/heartbeat$"), "heartbeat"),
    ("POST", re.compile(r"^/tasks/(?P<id>[^/]+)/ack$"),     "ack"),
    ("POST", re.compile(r"^/tasks/(?P<id>[^/]+)/nack$"),    "nack"),
]


class BadRequest(Exception):
    pass


def _require(body: dict, key: str, typ=None):
    if key not in body:
        raise BadRequest(f"missing field '{key}'")
    if typ is not None and not isinstance(body[key], typ):
        raise BadRequest(f"field '{key}' must be {typ.__name__}")
    return body[key]


def _optional_list(body: dict, key: str) -> list[str] | None:
    v = body.get(key)
    if v is None:
        return None
    if not isinstance(v, list) or not all(isinstance(x, str) for x in v):
        raise BadRequest(f"field '{key}' must be a list of task ids")
    return v


def make_handler(store: TaskStore, quiet: bool = False):
    class Handler(BaseHTTPRequestHandler):
        server_version = "minisched/0.1"

        # plumbing
        def log_message(self, fmt, *args):
            if not quiet:
                sys.stderr.write("%s - %s\n" % (self.address_string(), fmt % args))

        def _send(self, status: int, obj) -> None:
            data = json.dumps(obj).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def _body(self) -> dict:
            n = int(self.headers.get("Content-Length") or 0)
            if n == 0:
                return {}
            try:
                obj = json.loads(self.rfile.read(n))
            except json.JSONDecodeError as e:
                raise BadRequest(f"invalid JSON: {e.msg}")
            if not isinstance(obj, dict):
                raise BadRequest("body must be a JSON object")
            return obj

        def _dispatch(self, method: str) -> None:
            url = urlsplit(self.path)
            for m, pattern, name in ROUTES:
                match = pattern.match(url.path)
                if match and m == method:
                    try:
                        getattr(self, "h_" + name)(match.groupdict(), parse_qs(url.query))
                    except BadRequest as e:
                        self._send(HTTPStatus.BAD_REQUEST, {"error": str(e)})
                    except ValueError as e:
                        self._send(HTTPStatus.BAD_REQUEST, {"error": str(e)})
                    except NotFound as e:
                        self._send(HTTPStatus.NOT_FOUND, {"error": f"task {e} not found"})
                    except Conflict as e:
                        self._send(HTTPStatus.CONFLICT, {"error": str(e)})
                    return
            self._send(HTTPStatus.NOT_FOUND, {"error": f"no route for {method} {url.path}"})

        def do_GET(self):  self._dispatch("GET")
        def do_POST(self): self._dispatch("POST")

        # handlers
        def h_healthz(self, _p, _q):
            self._send(200, {"ok": True})

        def h_stats(self, _p, _q):
            self._send(200, {"queues": store.stats()})

        def h_submit(self, _p, _q):
            b = self._body()
            queue = _require(b, "queue", str)
            payload = _require(b, "payload")
            key = b.get("idempotency_key") or self.headers.get("Idempotency-Key")
            task, created = store.submit(
                queue, payload,
                max_attempts=int(b.get("max_attempts", 3)),
                idempotency_key=key,
                delay_seconds=float(b.get("delay_seconds", 0)),
                priority=int(b.get("priority", 0)),
                depends_on=_optional_list(b, "depends_on"))
            self._send(201 if created else 200, {"task": task, "created": created})

        def h_list(self, _p, q):
            self._send(200, {"tasks": store.list(
                queue=q.get("queue", [None])[0],
                status=q.get("status", [None])[0],
                limit=int(q.get("limit", ["100"])[0]))})

        def h_get(self, p, _q):
            self._send(200, {"task": store.get(p["id"])})

        def h_cancel(self, p, _q):
            self._send(200, {"task": store.cancel(p["id"])})

        def h_lease(self, p, _q):
            b = self._body()
            tasks = store.lease(
                p["queue"], _require(b, "worker_id", str),
                max_tasks=int(b.get("max_tasks", 1)),
                lease_seconds=float(b.get("lease_seconds", 30)),
                wait_seconds=float(b.get("wait_seconds", 0)))
            self._send(200, {"tasks": tasks})

        def h_heartbeat(self, p, _q):
            b = self._body()
            self._send(200, {"task": store.heartbeat(
                p["id"], _require(b, "lease_token", str),
                lease_seconds=float(b.get("lease_seconds", 30)))})

        def h_ack(self, p, _q):
            b = self._body()
            self._send(200, {"task": store.ack(
                p["id"], _require(b, "lease_token", str), b.get("result"))})

        def h_nack(self, p, _q):
            b = self._body()
            retry = b.get("retry_after_seconds")
            self._send(200, {"task": store.nack(
                p["id"], _require(b, "lease_token", str),
                error=str(b.get("error", "")),
                retry_after_seconds=float(retry) if retry is not None else None)})

    return Handler


def serve(host: str = "127.0.0.1", port: int = 8080, db: str = "minisched.db",
          sweep_interval: float = 1.0, quiet: bool = False) -> ThreadingHTTPServer:
    store = TaskStore(db)
    sweeper = Sweeper(store, sweep_interval)
    sweeper.start()
    httpd = ThreadingHTTPServer((host, port), make_handler(store, quiet))
    httpd.daemon_threads = True
    httpd.store, httpd.sweeper = store, sweeper  # type: ignore[attr-defined]
    return httpd


def main(argv=None):
    import argparse
    ap = argparse.ArgumentParser(description="minisched: a tiny task scheduler")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=8080)
    ap.add_argument("--db", default="minisched.db")
    ap.add_argument("--sweep-interval", type=float, default=1.0)
    args = ap.parse_args(argv)
    httpd = serve(args.host, args.port, args.db, args.sweep_interval)
    print(f"minisched listening on http://{args.host}:{args.port}  (db: {args.db})")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        httpd.sweeper.stop()
        httpd.server_close()


if __name__ == "__main__":
    main()
