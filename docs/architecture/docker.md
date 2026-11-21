# Local infrastructure

Docker Compose connects the services through the `flousflow` bridge network.
The Python producer runs on the host.

| Service | Image | Role |
| --- | --- | --- |
| `kafka` | `apache/kafka:4.1.1` | Single KRaft broker/controller |
| `kafka-init` | `apache/kafka:4.1.1` | Creates `flousflow.transactions` with three partitions |
| `minio` | Locally compiled MinIO | Stores Bronze, Silver, and Gold Delta tables |
| `minio-init` | Locally compiled mc | Creates the `flousflow` bucket |
| `bronze` | Local Spark image | Kafka-to-Delta streaming job |
| `silver` | Local Spark image | Bronze-to-Silver streaming job |
| `postgres` | `postgres:17.7-alpine` | SQL serving database |
| `serving` | Local Spark image with psycopg | Temporary Gold refresh and PostgreSQL loader |

Initialization containers exit after setup. Kafka, MinIO, Bronze, Silver, and
PostgreSQL remain running. Gold jobs use temporary Spark containers. A shared
Dockerfile defines ingestion and serving targets; the serving target adds psycopg 3.2.6.

## Connections

| Connection | Address |
| --- | --- |
| Host producer → Kafka | `localhost:9092` |
| Bronze → Kafka | `kafka:19092` |
| Spark → MinIO | `http://minio:9000` |
| Serving loader → PostgreSQL | `postgres:5432` |
| Host SQL client → PostgreSQL | `localhost:5432` |
| Host browser → MinIO console | `http://localhost:9001` |

Published ports bind to localhost. Container traffic uses service names and
plaintext connections. Kafka has separate advertised host and container listeners.

## Storage and recovery

| Volume | Contents |
| --- | --- |
| `kafka-data` | Kafka records and broker state |
| `minio-data` | Delta data files and transaction logs |
| `spark-checkpoints` | Separate Bronze and Silver streaming progress |
| `spark-ivy` | Cached JVM dependencies |
| `gold-lock` | Shared local Gold writer lock |
| `postgres-data` | Serving tables, roles, and refresh metadata |

Volumes survive container recreation. Bronze resumes from its Kafka checkpoint;
Silver resumes from its Delta checkpoint. A development reset clears Kafka,
lakehouse data, and streaming checkpoints together. PostgreSQL is a derived copy
and remains unchanged until a successful refresh.

Bronze starts after topic and bucket initialization; Silver waits for Bronze
readiness. Streaming health checks record a successful batch or idle notification.
They indicate readiness, not catch-up or stalled-batch detection.

## Configuration

Compose profiles separate ingestion (`bronze`, `silver`) from serving (`serving`).
Credentials come from the ignored `.env`; [.env.example](../../.env.example) lists
the settings. The host producer reads exported environment variables.

PostgreSQL has separate admin, loader, and reader roles. Database initialization
runs on an empty volume; existing passwords are stored in PostgreSQL. Readers
have SELECT access to `serving`. Multi-statement queries can use repeatable-read
transactions to share one committed refresh.

The Spark base is 3.5.6 with Java 17 and Python. JVM dependencies are pinned in
`spark-defaults.conf`: Delta 3.3.3, Kafka connector 3.5.6, Hadoop S3A 3.3.4, and AWS
SDK 1.12.262. Spark runs with two local worker threads.

MinIO and mc are compiled from checksum-pinned source releases using Go 1.24.2,
with Alpine runtime images. Their source releases are recorded in the Dockerfile.

Serving waits for PostgreSQL health and MinIO bucket initialization. It can start
those dependencies from a stopped project; Silver data must already exist or
arrive while an optional replay requirement waits.

`FLOUSFLOW_REFRESH_REPLAY_ID` and `FLOUSFLOW_REFRESH_EXPECTED_COUNT` together set
an optional completion requirement, measured in valid Silver transactions.
`FLOUSFLOW_REFRESH_WAIT_SECONDS` bounds the wait. The snapshot that meets the
requirement is the one used for aggregation. Without a requirement, refresh uses
the current Silver snapshot. Rejected events are excluded from the expected count.

Gold writers share a file lock through the `gold-lock` volume. The lock covers
snapshot selection, Delta writes, and serving publication. It applies to this
single-host Compose deployment. GitHub Actions checks unit tests and Compose
configuration on pushes and pull requests.
