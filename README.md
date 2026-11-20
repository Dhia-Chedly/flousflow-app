# FlousFlow

A financial data engineering project using synthetic PaySim transactions.
Replays CSV events through Kafka into a Bronze Delta table in MinIO.

```text
CSV → Python producer (host) → Kafka → Spark Structured Streaming → Delta / MinIO
```

Next: Silver validation and deduplication, then Gold metrics and analytical serving.

## Run locally

Requires Python 3.12–3.14, uv, Docker Engine, and Docker Compose.
Run commands from the repository root.

Create `.env` from `.env.example` if you do not already have one. Set
`MINIO_ROOT_USER` (at least three characters) and `MINIO_ROOT_PASSWORD` (at least
eight). Keep `.env` local.

```bash
uv sync --locked
docker compose --profile bronze build minio minio-init
docker compose up -d --build spark-bronze
docker compose --profile bronze ps -a
```

The first build downloads base images and compiles MinIO. Spark downloads its JVM
dependencies on first startup. Wait for `spark-bronze` to become healthy; follow
progress with `docker compose logs -f spark-bronze`. Both initialization services
should exit with code `0`.

Replay the included three-record fixture:

```bash
uv run --locked flousflow-producer
```

Inspect Bronze:

```bash
docker compose run --rm --no-deps spark-bronze /opt/flousflow/spark/bronze/inspect_table.py
```

MinIO's console is at http://localhost:9001. The table is stored at
`s3a://flousflow/bronze/transactions`.

## Producer configuration

The host producer reads exported environment variables; it does not load `.env`
automatically. Defaults are listed in [.env.example](.env.example).

```bash
FLOUSFLOW_SOURCE_PATH=data/raw/PS_20174392719_1491204439457_log.csv \
FLOUSFLOW_REPLAY_ID=run-001 uv run --locked flousflow-producer
```

The default limit is 10 events per second; a full replay takes at least 7.4 days.
Change `FLOUSFLOW_REPLAY_RATE_PER_SECOND` to adjust it. The producer queues up to 1,000 pending events, checks delivery callbacks, and
flushes before reporting success. It stops submitting on invalid input or a
reported delivery failure; already queued events may still arrive. Restart begins at
row one; reuse the replay ID for recovery. Bronze preserves repeated deliveries.

## Tests

```bash
# Unit tests; no services required
uv run --locked python -m unittest discover -s tests/unit -v

# Live acceptance checks; requires the local PaySim CSV and no other publishers
uv run --locked python tests/integration/bronze_pipeline.py
```

The live checks publish 10,000 records, verify Delta storage, restart Spark, publish
100 additional records, and recreate its container. All six checks passed on 20 November 2026, including malformed JSON and repeated
event IDs. The 10,000-event producer run took 7.27 seconds on the local machine. The script accounts for existing records
and leaves the writer and test data in place.

## Files

- `producer/`: CSV validation and Kafka replay.
- `spark/bronze/`: ingestion and inspection jobs.
- `spark/common/`: Spark, Kafka, storage, and environment settings.
- `infra/docker/`: image builds and dependency configuration.
- `tests/`: unit and live integration tests.

[Docker setup](docs/architecture/docker.md) ·
[Event contract](docs/architecture/event-contract.md) ·
[Dataset notes](data/README.md)
