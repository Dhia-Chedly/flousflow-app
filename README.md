# FlousFlow

A local financial data pipeline built with Python, Kafka, Spark, Delta Lake,
MinIO, and PostgreSQL. It replays synthetic PaySim transactions, validates the
stream, and produces queryable transaction and account metrics.

```text
CSV → Python producer → Kafka → Bronze → Silver → Gold → PostgreSQL
                               └──── Delta Lake / MinIO ────┘
```

Bronze and Silver run as streaming jobs. Gold aggregation and PostgreSQL refresh
run in batches.

## Pipeline

| Component | Implemented behavior |
| --- | --- |
| Producer | CSV validation, configurable replay rate, stable event IDs, bounded delivery queue, Kafka acknowledgements |
| Bronze | Raw Kafka keys and values, source offsets, ingestion timestamps, checkpoint recovery |
| Silver | Strict JSON and monetary validation, event deduplication, rejected records, content conflicts, delivery audit |
| Gold | Hourly transaction metrics and account activity, separated by replay |
| PostgreSQL | Transactional refresh of both serving tables, reconciliation, refresh metadata, read-only query access |

The serving refresh builds both Gold tables from one Silver snapshot. Gold rows
are streamed into PostgreSQL staging tables with `COPY`; both serving tables and
refresh metadata commit together. Failed loads leave the committed serving data unchanged.
A shared local writer lock coordinates Gold jobs, validation, and serving.
PostgreSQL also prevents overlapping serving refreshes. An optional replay-count
requirement waits for the expected valid Silver transactions before refreshing.

## Scope

The project runs locally in Docker Compose with one Kafka broker and Spark in
`local[2]` mode. PostgreSQL refreshes are manual. Gold tables commit independently
in Delta; PostgreSQL publishes its two serving tables atomically. A single Silver
writer owns its output paths. A dashboard and automated scheduling are not included.

## Repository

| Path | Contents |
| --- | --- |
| `producer/` | Python CSV replay producer |
| `spark/bronze/`, `spark/silver/` | Streaming ingestion and validation |
| `spark/gold/` | Aggregations and reconciliation checks |
| `spark/serving/` | PostgreSQL refresh and load checks |
| `infra/` | Docker images and database schema |
| `tests/` | Unit and integration tests |

[Infrastructure](docs/architecture/docker.md) ·
[Event contract](docs/architecture/event-contract.md) ·
[Silver rules](docs/architecture/silver.md) ·
[Dataset](data/README.md)
