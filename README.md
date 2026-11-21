# FlousFlow

A financial data engineering project using synthetic PaySim transactions.
Replays CSV events through Kafka into Bronze and Silver Delta tables in MinIO.

```text
CSV → Python producer → Kafka → Spark Bronze → Spark Silver
                                  └── Delta tables in MinIO ──┘
```

Silver parses and validates Bronze records, deduplicates by event ID, and records
rejections, conflicts, and quality outcomes. Gold metrics and serving come later.

## Run locally

Requires Python 3.12–3.14, uv, Docker Engine, and Docker Compose.
Run commands from the repository root.

Create `.env` from `.env.example` if you do not already have one. Set
`MINIO_ROOT_USER` (at least three characters) and `MINIO_ROOT_PASSWORD` (at least
eight). Keep `.env` local.

```bash
uv sync --locked
docker compose --profile silver build minio minio-init
docker compose --profile silver up -d --build silver
docker compose --profile silver ps -a
```

This starts Kafka, MinIO, and both Spark writers on the `flousflow` network.
Their container names are `kafka`, `minio`, `bronze`, and `silver`; the setup
containers are `kafka-init` and `minio-init`. The first build compiles MinIO;
Spark downloads its JVM dependencies on first startup. The initialization services
should exit with code `0`. Follow startup and batch progress with:

```bash
docker compose logs -f bronze silver
```

Replay the included three-record sample:

```bash
uv run --locked flousflow-producer
```

Wait for a completed Silver batch before inspecting the tables. Producer success
means Kafka acknowledged the events; Spark processes them afterward.

```bash
# Raw Kafka records in Bronze
docker compose run --rm --no-deps bronze /opt/flousflow/spark/bronze/inspect_table.py

# Validated transactions and quality outcomes in Silver
docker compose --profile silver run --rm --no-deps silver /opt/flousflow/spark/silver/inspect_silver.py
```

After a fresh start with empty data, the sample produces three Silver transactions
and three accepted audit outcomes. Replaying it with the same replay ID adds
Bronze deliveries and duplicate audit outcomes, but no additional transactions.

MinIO's console is at http://localhost:9001. Bronze is stored under
`s3a://flousflow/bronze/transactions`; Silver tables are under
`s3a://flousflow/silver/`.

## Producer configuration

The host producer reads exported environment variables; it does not load `.env`
automatically. Defaults are listed in [.env.example](.env.example).

```bash
FLOUSFLOW_SOURCE_PATH=data/raw/PS_20174392719_1491204439457_log.csv \
FLOUSFLOW_REPLAY_ID=run-001 uv run --locked flousflow-producer
```

The default limit is 10 events per second; a full replay takes at least 7.4 days.
Change `FLOUSFLOW_REPLAY_RATE_PER_SECOND` to adjust it. The producer queues up to
1,000 pending events, checks delivery callbacks, and flushes before reporting
success. It stops submitting on invalid input or a
reported delivery failure; already queued events may still arrive. Restart begins at
row one; reuse the replay ID for recovery. Bronze preserves repeated deliveries.

## Stop or reset

Stop the pipeline while keeping its data:

```bash
docker compose --profile bronze --profile silver down
```

Topics, table paths, and checkpoints keep stable names as the project changes.
For incompatible schema changes, [reset the local data](docs/architecture/docker.md#reset-local-data)
and replay the CSV. Git tracks the code and event contract; dependency pins keep
builds reproducible.

## Tests

```bash
# Unit tests; no services required
uv run --locked python -m unittest discover -s tests/unit -v

# Bronze integration checks; requires the local PaySim CSV and no other publishers
uv run --locked python tests/integration/bronze_pipeline.py
```

The live checks publish 10,000 records, verify Delta storage, restart Spark, publish
100 additional records, and recreate its container. They also check malformed JSON
and repeated event IDs. The script accounts for existing records and leaves the
writer and test data in place.

For Silver retry, validation, and checkpoint tests, see the
[Silver test instructions](docs/architecture/silver.md#isolated-test).

## Files

- `producer/`: CSV validation and Kafka replay.
- `spark/bronze/`: ingestion and inspection jobs.
- `spark/silver/`: validation, merge, audit, and quality checks.
- `spark/common/`: Spark, Kafka, storage, and environment settings.
- `infra/docker/`: image builds and dependency configuration.
- `tests/`: unit and live integration tests.

[Docker setup](docs/architecture/docker.md) ·
[Event contract](docs/architecture/event-contract.md) ·
[Dataset notes](data/README.md)
