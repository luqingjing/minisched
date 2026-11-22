"""llmbatch: an LLM batch-processing layer on top of minisched.

minisched is the queue; this package adds the pieces every LLM backend
integration ends up needing: a provider abstraction, rate-limit-aware retries,
per-provider concurrency control, prompt-hash caching, JSON schema validation,
per-task cost/latency accounting, and a tiny vector index for embeddings.
"""
