# Producer tests

Run from the repository root:

```bash
uv run --locked python -m unittest discover -s tests/unit -v
```

Covers stable IDs, decimal preservation, invalid records, fixture replay, and
failed or timed-out deliveries. Uses a fake Kafka client; no broker is required.
