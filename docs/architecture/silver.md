# Silver transactions

The Silver stream reads Bronze Delta records and writes four Delta tables under
`s3a://flousflow/silver/`: `transactions`, `rejected`, `conflicts`, and `audit`.
It has a separate checkpoint at `/checkpoints/silver-transactions`. Run one
writer for these paths.

`transactions` contains typed amounts and balances (`DECIMAL(18,2)`), timestamps,
boolean labels, event and replay IDs, and Kafka source metadata. Account and
transaction-type columns are renamed; their source values are preserved. Amounts
that require rounding are rejected. The content hash excludes `produced_at` and
processing metadata so retries can vary those fields.

`rejected` retains the raw key/value, source metadata, available IDs, validation
codes and explanations. Invalid JSON, UTF-8, duplicate JSON keys, incorrect field
types, missing fields, invalid IDs, timestamps, transaction types, labels, and
amounts are rejected. IDs are recalculated from the source identity and replay ID. Zero amounts are valid;
balance reconciliation is not required.

`conflicts` retains valid records whose event ID already exists with different
business content, including the original Kafka key/value and both content hashes.
The first valid record ordered by Kafka topic, partition, and offset within a
batch wins; an existing Silver row wins across batches. Do not infer global
transaction order across Kafka partitions from this rule.

`audit` records one outcome per Kafka topic/partition/offset: `accepted`,
`duplicate`, `conflict`, or `rejected`. Group by outcome for quality counts; the
sum is the number of distinct source offsets processed. Repeated Bronze copies of
the same Kafka offset are counted once. This assumes topic/partition/offset
coordinates are not reused after Kafka topic deletion and recreation. Replays
with new Kafka offsets are counted as deliveries, and a new replay ID creates
distinct transactions.

Each table is committed independently. The single writer inserts idempotently
using event ID for transactions and Kafka coordinates for other tables. A failed
batch can be retried to finish remaining outputs; they are not visible atomically
across all tables. Do not run concurrent Silver writers against the same paths.
Do not delete a checkpoint without a recovery plan.

Table paths and checkpoints use stable names. When a development change makes
existing data incompatible, [reset the local pipeline](docker.md#reset-local-data)
and replay the source CSV.

Silver logs the input row count and duration after each completed batch. Its
health check passes after a successful batch or idle notification and clears when
the query terminates. This indicates readiness, not catch-up or a maximum
processing delay; use the inspection command and batch logs to check progress.

## Isolated test

```bash
docker compose --profile silver build silver
docker compose --profile silver run --rm --no-deps silver /opt/flousflow/spark/silver/selftest.py
```

The test uses temporary local Delta tables, not the existing MinIO Silver paths.
To repeat it on a unique, isolated MinIO prefix, MinIO must be healthy and
`minio-init` must have finished successfully:

```bash
docker compose --profile silver run --rm --no-deps -e FLOUSFLOW_SELFTEST_S3=1 silver /opt/flousflow/spark/silver/selftest.py
```

It checks an injected partial-write failure, replay, malformed inputs, conflicts,
and checkpoint restart. The MinIO test removes its unique prefix on success; on
failure it leaves that prefix for investigation. The container needs configured
MinIO environment variables because it shares the normal Spark session setup.
