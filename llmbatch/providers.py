"""
Provider abstraction. The worker only ever talks to this interface.

  FakeProvider      deterministic, offline, scriptable failures (tests, demos)
  OpenAIProvider    chat completions + embeddings over urllib (OPENAI_API_KEY)
  AnthropicProvider messages over urllib (ANTHROPIC_API_KEY); no embeddings

Errors are classified so the worker can decide what to do:
  RateLimited(retry_after)  -> nack with retry_after_seconds (honour the provider's hint)
  TransientError            -> nack (scheduler's exponential backoff)
  PermanentError            -> nack with retry_after_seconds=0, so the remaining
                               attempts drain straight into DEAD instead of
                               waiting out backoff for an error that won't change
"""
from __future__ import annotations

import hashlib
import json
import math
import os
import random
import threading
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from typing import Any, Optional


class ProviderError(Exception):
    pass


class RateLimited(ProviderError):
    def __init__(self, retry_after: float, msg: str = "rate limited"):
        super().__init__(msg)
        self.retry_after = retry_after


class TransientError(ProviderError):
    """5xx, timeouts, connection resets: retry with backoff."""


class PermanentError(ProviderError):
    """4xx other than 429, e.g. bad request / auth: retrying won't help."""


@dataclass
class Completion:
    text: str
    prompt_tokens: int
    completion_tokens: int
    model: str
    raw: Any = None


@dataclass
class Embedding:
    vector: list[float]
    prompt_tokens: int
    model: str


# USD per 1M tokens: (input, output). Edit freely; it only feeds the cost metric.
PRICES = {
    "fake-1": (0.10, 0.40),
    "fake-embed": (0.02, 0.0),
    "gpt-4o-mini": (0.15, 0.60),
    "gpt-4o": (2.50, 10.00),
    "text-embedding-3-small": (0.02, 0.0),
    "claude-haiku-5-5": (1.00, 5.00),
    "claude-sonnet-5-5": (3.00, 15.00),
}


def cost_usd(model: str, prompt_tokens: int, completion_tokens: int) -> float:
    inp, out = PRICES.get(model, (0.0, 0.0))
    return (prompt_tokens * inp + completion_tokens * out) / 1_000_000


class Provider:
    name = "base"

    def complete(self, model: str, prompt: str, *, system: str = "", max_tokens: int = 512,
                 temperature: float = 0.0, json_mode: bool = False) -> Completion:
        raise NotImplementedError

    def embed(self, model: str, text: str) -> Embedding:
        raise NotImplementedError


# --------------------------------------------------------------------------- fake
@dataclass
class FakeProvider(Provider):
    """
    Deterministic offline provider.
      script: per-call behaviours consumed in order, e.g.
              ["429:2.5", "bad_json", "500", "ok"] -> first call raises RateLimited(2.5),
              second returns non-JSON text, third raises TransientError, then normal.
      latency: seconds to sleep per call (simulates network).
      responder: optional callable(prompt) -> text, to control content.
    Tracks max concurrency so tests can assert the worker's semaphore works.
    """
    name: str = "fake"
    script: list[str] = field(default_factory=list)
    latency: float = 0.0
    responder: Any = None
    calls: int = 0
    max_concurrent: int = 0
    _active: int = 0
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def _enter(self) -> Optional[str]:
        with self._lock:
            self.calls += 1
            self._active += 1
            self.max_concurrent = max(self.max_concurrent, self._active)
            return self.script.pop(0) if self.script else None

    def _exit(self):
        with self._lock:
            self._active -= 1

    def _apply(self, step: Optional[str]):
        if step is None or step == "ok":
            return None
        if step.startswith("429"):
            ra = float(step.split(":")[1]) if ":" in step else 1.0
            raise RateLimited(ra)
        if step == "500":
            raise TransientError("fake 500")
        if step == "400":
            raise PermanentError("fake 400")
        return step  # e.g. "bad_json" -> returned as the text

    def complete(self, model, prompt, *, system="", max_tokens=512, temperature=0.0, json_mode=False):
        step = self._enter()
        try:
            if self.latency:
                time.sleep(self.latency)
            special = self._apply(step)
            if special == "bad_json":
                text = "Sure! Here is your answer: {not json"
            elif self.responder:
                text = self.responder(prompt)
            elif json_mode:
                # echo a structured summary so schema tests have something real
                text = json.dumps({"summary": prompt[:40], "sentiment": "neutral", "confidence": 0.9})
            else:
                text = f"fake answer to: {prompt[:60]}"
            pt = max(1, len((system + prompt).split()))
            return Completion(text, pt, max(1, len(text.split())), model)
        finally:
            self._exit()

    def embed(self, model, text):
        step = self._enter()
        try:
            if self.latency:
                time.sleep(self.latency)
            self._apply(step)
            return Embedding(fake_embedding(text), max(1, len(text.split())), model)
        finally:
            self._exit()


def fake_embedding(text: str, dim: int = 32) -> list[float]:
    """
    Deterministic bag-of-words embedding: texts that share words are close.
    Good enough to make vector search meaningful offline.
    """
    vec = [0.0] * dim
    for tok in text.lower().split():
        h = int(hashlib.sha256(tok.encode()).hexdigest(), 16)
        rng = random.Random(h)
        for i in range(dim):
            vec[i] += rng.uniform(-1, 1)
    norm = math.sqrt(sum(x * x for x in vec)) or 1.0
    return [round(x / norm, 6) for x in vec]


# --------------------------------------------------------------------------- http helpers
def _post_json(url: str, headers: dict, body: dict, timeout: float = 60.0) -> dict:
    req = urllib.request.Request(url, data=json.dumps(body).encode(), method="POST",
                                 headers={"Content-Type": "application/json", **headers})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read())
    except urllib.error.HTTPError as e:
        detail = e.read().decode(errors="replace")[:300]
        if e.code == 429:
            ra = e.headers.get("retry-after")
            try:
                ra = float(ra) if ra is not None else 5.0
            except ValueError:
                ra = 5.0
            raise RateLimited(ra, f"429: {detail}")
        if e.code >= 500 or e.code in (408, 409):
            raise TransientError(f"{e.code}: {detail}")
        raise PermanentError(f"{e.code}: {detail}")
    except (urllib.error.URLError, TimeoutError, ConnectionError) as e:
        raise TransientError(repr(e))


# --------------------------------------------------------------------------- openai
class OpenAIProvider(Provider):
    name = "openai"

    def __init__(self, api_key: Optional[str] = None, base_url: str = "https://api.openai.com/v1"):
        self.key = api_key or os.environ.get("OPENAI_API_KEY") or ""
        if not self.key:
            raise PermanentError("OPENAI_API_KEY is not set")
        self.base = base_url.rstrip("/")

    def _headers(self):
        return {"Authorization": f"Bearer {self.key}"}

    def complete(self, model, prompt, *, system="", max_tokens=512, temperature=0.0, json_mode=False):
        msgs = ([{"role": "system", "content": system}] if system else []) + [{"role": "user", "content": prompt}]
        body = {"model": model, "messages": msgs, "max_tokens": max_tokens, "temperature": temperature}
        if json_mode:
            body["response_format"] = {"type": "json_object"}
        r = _post_json(f"{self.base}/chat/completions", self._headers(), body)
        u = r.get("usage", {})
        return Completion(r["choices"][0]["message"]["content"], u.get("prompt_tokens", 0),
                          u.get("completion_tokens", 0), r.get("model", model), r)

    def embed(self, model, text):
        r = _post_json(f"{self.base}/embeddings", self._headers(), {"model": model, "input": text})
        return Embedding(r["data"][0]["embedding"], r.get("usage", {}).get("prompt_tokens", 0), r.get("model", model))


# --------------------------------------------------------------------------- anthropic
class AnthropicProvider(Provider):
    name = "anthropic"

    def __init__(self, api_key: Optional[str] = None, base_url: str = "https://api.anthropic.com/v1"):
        self.key = api_key or os.environ.get("ANTHROPIC_API_KEY") or ""
        if not self.key:
            raise PermanentError("ANTHROPIC_API_KEY is not set")
        self.base = base_url.rstrip("/")

    def complete(self, model, prompt, *, system="", max_tokens=512, temperature=0.0, json_mode=False):
        body = {"model": model, "max_tokens": max_tokens, "temperature": temperature,
                "messages": [{"role": "user", "content": prompt}]}
        sys_text = system
        if json_mode:
            sys_text = (sys_text + "\n" if sys_text else "") + "Respond with a single JSON object and nothing else."
        if sys_text:
            body["system"] = sys_text
        r = _post_json(f"{self.base}/messages",
                       {"x-api-key": self.key, "anthropic-version": "2023-06-01"}, body)
        text = "".join(b.get("text", "") for b in r.get("content", []) if b.get("type") == "text")
        u = r.get("usage", {})
        return Completion(text, u.get("input_tokens", 0), u.get("output_tokens", 0), r.get("model", model), r)

    def embed(self, model, text):
        raise PermanentError("Anthropic has no embeddings endpoint; use openai or fake for embed tasks")


def make_provider(name: Optional[str] = None, **kw) -> Provider:
    """Pick a provider from LLM_PROVIDER (default fake)."""
    name = (name or os.environ.get("LLM_PROVIDER") or "fake").lower()
    if name == "fake":
        return FakeProvider(**kw)
    if name == "openai":
        return OpenAIProvider(**kw)
    if name == "anthropic":
        return AnthropicProvider(**kw)
    raise PermanentError(f"unknown provider {name!r}")
