# Unit tests

Producer tests cover CSV validation, stable IDs, bounded asynchronous delivery,
queue-full retries, and delivery failures. A fake Kafka client keeps them
independent of running services. Other tests cover Bronze settings and Silver's
JSON types and exact monetary values.

Integration checks are in `tests/integration/`. Spark self-tests are stored with
the Silver, Gold, and serving jobs.
