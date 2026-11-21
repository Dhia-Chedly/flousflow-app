# FlousFlow

A local data pipeline for synthetic PaySim transactions.

```text
CSV → Python producer → Kafka → Bronze → Silver → Gold
```

Spark writes Delta tables to MinIO. Bronze keeps the raw events. Silver validates
and deduplicates them, keeping rejected records and conflicting deliveries.
Gold produces hourly transaction metrics and account activity in batch jobs.

## Run

Requires Python 3.12–3.14, uv, Docker Engine, and Docker Compose.
Run commands from the repository root. Copy `.env.example` to `.env` and set
`MINIO_ROOT_USER` and `MINIO_ROOT_PASSWORD`.

```bash
uv sync --locked
docker compose --profile silver build minio minio-init
docker compose --profile silver up -d --build silver
docker compose --profile silver ps -a
uv run --locked flousflow-producer
```

The producer defaults to the three-record sample. Kafka, MinIO, Bronze, and Silver
stay running; `kafka-init` and `minio-init` exit after setup. The first build compiles
MinIO and Spark downloads its dependencies. Logs: `docker compose logs -f bronze silver`.
MinIO console: http://localhost:9001.

For the full dataset, place the CSV in `data/raw/`:

```bash
FLOUSFLOW_SOURCE_PATH=data/raw/PS_20174392719_1491204439457_log.csv \
FLOUSFLOW_REPLAY_ID=run-001 uv run --locked flousflow-producer
```

The producer reads shell variables, not `.env`. Defaults are in [.env.example](.env.example).
The default rate is 10 events/second; change `FLOUSFLOW_REPLAY_RATE_PER_SECOND` for
larger replays. Restarting begins at row one. Reusing the source and replay ID
preserves transaction identity; Silver removes duplicate deliveries.

## Gold

Run after Silver has processed the replay. Rebuild the `silver` image after code changes.

```bash
docker compose --profile silver run --rm --no-deps silver /opt/flousflow/spark/gold/hourly.py
docker compose --profile silver run --rm --no-deps silver /opt/flousflow/spark/gold/accounts.py
```

| Table under `s3a://flousflow/gold/` | Grouping | Metrics |
| --- | --- | --- |
| `hourly_transaction_metrics` | Replay, UTC hour, transaction type | Count, total/average amount, fraud count and rate |
| `account_activity` | Replay, account | Incoming/outgoing counts, amounts and fraud counts; first/last activity |

Fraud metrics use the source labels. Account amounts describe flows, not balances;
a self-transfer counts once in each direction. Each job replaces its table from a
Silver snapshot. Run one writer per Gold table. The two tables commit independently.

## Inspect

```bash
docker compose run --rm --no-deps bronze /opt/flousflow/spark/bronze/inspect_table.py
docker compose --profile silver run --rm --no-deps silver /opt/flousflow/spark/silver/inspect_silver.py
docker compose --profile silver run --rm --no-deps silver /opt/flousflow/spark/gold/inspect_gold.py
docker compose --profile silver run --rm --no-deps silver /opt/flousflow/spark/gold/inspect_gold.py --accounts
```

Producer success means Kafka acknowledged the records. Wait for Silver to finish
before refreshing Gold; tables do not exist until their first write.

## Tests

Unit tests need no services. The Gold self-test uses temporary Delta tables.
Both pipeline tests require the running services and local PaySim CSV; they leave
test data in place. The Gold pipeline test also replaces both Gold tables.

```bash
uv run --locked python -m unittest discover -s tests/unit -v
docker compose --profile silver run --rm --no-deps silver /opt/flousflow/spark/gold/selftest.py
uv run --locked python tests/integration/bronze_pipeline.py
uv run --locked python tests/integration/gold_pipeline.py
```

The Gold pipeline test replays 10,000 records, reconciles counts, amounts, and fraud
counts with Silver, and compares repeated writes. Run it without other Gold writers.
Silver's isolated tests are listed in [silver.md](docs/architecture/silver.md).

## Stop

```bash
docker compose --profile bronze --profile silver down
```

Data stays in Docker volumes. For a clean replay, use the
[reset commands](docs/architecture/docker.md#reset-local-data).

[Docker](docs/architecture/docker.md) ·
[Event contract](docs/architecture/event-contract.md) ·
[Silver](docs/architecture/silver.md) ·
[Dataset](data/README.md)
