# FlousFlow

A financial data engineering project built around synthetic PaySim transactions.
The producer currently replays CSV records into a local Kafka broker.

Planned pipeline:

```text
PaySim → Python → Kafka → Spark → Delta Lake (Bronze / Silver / Gold)
  → PostgreSQL → dbt → Metabase
```

Next: MinIO storage and Spark Bronze ingestion. AWS integration comes later.

## Run locally

Requires Python 3.12–3.14, uv, and a running Docker Engine with Compose.
Run from the repository root:

```bash
docker compose up -d
uv sync --locked
uv run --locked flousflow-producer
```

The default input is a three-record synthetic fixture. Kafka uses one KRaft broker
and a persistent volume. The `kafka-init` service creates
`flousflow.transactions.v1` with three partitions, then exits successfully.
Host clients connect to `localhost:9092`; Compose services use `kafka:19092`.
This setup is for local development and uses plaintext connections.

Inspect the events:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:19092 \
  --topic flousflow.transactions.v1 --from-beginning --max-messages 3
```

Stop services with `docker compose down`. Adding `-v` also deletes stored events.

## Configuration

See [.env.example](.env.example) for environment variables and defaults. The
producer does not load `.env` automatically; export variables in your shell.

To replay the full dataset:

```bash
FLOUSFLOW_SOURCE_PATH=data/raw/PS_20174392719_1491204439457_log.csv \
FLOUSFLOW_REPLAY_ID=run-001 uv run --locked flousflow-producer
```

The default rate is 10 events per second; a full replay takes at least 7.4 days.
Set `FLOUSFLOW_REPLAY_RATE_PER_SECOND` to change the limit. The producer waits for
an acknowledgement per event and stops on invalid input or delivery failure.
Restarting begins at row one. Reuse the replay ID for recovery; duplicate events
are possible and will require downstream deduplication.

## Tests

```bash
uv run --locked python -m unittest discover -s tests/unit -v
```

Tests use a fake Kafka client and need no running services.

See [dataset notes](data/README.md) and the [event contract](docs/architecture/event-contract.md).
