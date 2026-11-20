# Unit tests

```bash
uv run --locked python -m unittest discover -s tests/unit -v
```

Run from the repository root. Tests cover producer validation (including PaySim
scientific notation), stable IDs, bounded asynchronous delivery, queue-full retries, delivery failures,
and Bronze settings. They
use a fake Kafka client and do not need running services.

For the live pipeline check, see the root README and
`tests/integration/bronze_pipeline.py`.
