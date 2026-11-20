# Unit tests

```bash
uv run --locked python -m unittest discover -s tests/unit -v
```

Run from the repository root. Tests cover producer validation, stable IDs,
bounded asynchronous delivery, queue-full retries, delivery failures, Bronze
settings, and Silver JSON types and exact monetary values. Producer tests use a
fake Kafka client; no running services are needed.

For live Bronze checks, see the root README and
`tests/integration/bronze_pipeline.py`. For isolated Silver integration tests,
see the [Silver contract](../../docs/architecture/silver.md).
