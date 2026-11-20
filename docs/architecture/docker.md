# Local Docker setup

The producer runs on the host. Compose runs Kafka, MinIO, and Spark on a shared
bridge network (`fina_default` with the current directory name).

| Service | Purpose |
| --- | --- |
| `kafka` | One KRaft broker/controller, Kafka 4.1.1 |
| `kafka-init` | Create `flousflow.transactions` with three partitions and one replica |
| `minio` | Store Bronze and Silver Delta tables |
| `minio-init` | Create the `flousflow` bucket |
| `spark-bronze` | Write Kafka records to Bronze with `local[2]` |
| `spark-silver` | Validate and deduplicate Bronze records with `local[2]` |

The initialization services exit with code `0` after setup. Bronze waits for both;
Silver waits for Bronze to be healthy. Each writer's health check passes after a
successful streaming batch or idle notification and clears on query termination.
Readiness does not prove catch-up or detect a stalled batch. Silver logs completed
batch row counts and durations; the Bronze integration test checks Kafka offsets.

## Addresses

| Client | Destination |
| --- | --- |
| Host producer | `localhost:9092` |
| Bronze → Kafka | `kafka:19092` |
| Spark → MinIO | `http://minio:9000` |
| Host browser → MinIO console | `http://localhost:9001` |

Host ports bind to `127.0.0.1`. Connections use plaintext. Inside a container,
`localhost` refers to that container; use service names to reach another one.
Kafka advertises separate host and container addresses. Port 9093 is its internal
controller listener.

Compose reads MinIO credentials from `.env`. The host producer reads exported
shell variables. The Bronze path is `s3a://flousflow/bronze/transactions`.

## Persistence

| Volume | Mount | Data |
| --- | --- | --- |
| `kafka-data` | `/var/lib/kafka/data` | Kafka messages and broker state |
| `minio-data` | `/data` | Parquet files and Delta log |
| `spark-checkpoints` | `/checkpoints` | Streaming progress |
| `spark-ivy` | `/opt/spark/ivy` | Downloaded jars |

Compose prefixes these names with the project name. Rebuilding containers keeps
the volumes. `docker compose --profile bronze --profile silver down -v` deletes them.

Use one writer for the Bronze table and its checkpoint. First startup reads the
earliest retained Kafka records; restarts resume from the checkpoint. Deleting
the checkpoint can replay records.

Silver reads the Bronze Delta table in MinIO, not Kafka directly. It uses
`/checkpoints/silver-transactions`, separate from Bronze's
`/checkpoints/bronze-transactions` in the shared volume. Run one writer per
layer. See the [Silver contract](silver.md) for output paths and retry behavior.

## Commands

```bash
# Build storage images once
docker compose --profile silver build minio minio-init
# Build/start both writers and their dependencies
docker compose --profile silver up -d --build spark-silver
# Status and logs
docker compose --profile silver ps -a
docker compose logs -f spark-bronze spark-silver
# Stop/resume ingestion
docker compose stop spark-silver spark-bronze
docker compose --profile silver up -d spark-silver
# Stop everything, keeping data
docker compose --profile bronze --profile silver down
```

Plain `docker compose up -d` starts Kafka only, including its initializer. MinIO
and Bronze belong to both the `bronze` and `silver` profiles. Silver belongs to
the `silver` profile. Targeting a writer starts its dependencies too.

## Builds

`infra/docker/spark.Dockerfile` extends Spark 3.5.6 with Java 17 and Python.
`spark-defaults.conf` pins Delta 3.3.3, Kafka connector 3.5.6, Hadoop S3A 3.3.4,
and AWS SDK modules 1.12.262. Jars download from `repo.maven.apache.org` on first
startup and are cached. Spark runs as user 185.

`infra/docker/minio.Dockerfile` compiles MinIO and mc from checksum-pinned source
releases. The server release is `2025-04-22T22-12-26Z`; the client release is
`2025-04-16T18-13-26Z`. This build was added after registry pulls failed. It uses
Go 1.24.2 for compilation and Alpine for both runtime images. A small compiled
HTTP probe checks the server.

## Troubleshooting

Check service logs first. Slow initial startup usually means dependency downloads
or catch-up are still running. A missing table can mean no batch has committed yet.
For authentication errors, check `.env` and recreate services after changing it.
For connection errors, check the host/container addresses above. IPv6 `localhost`
attempts can fail because the host Kafka port is bound to IPv4.

## Reusing MinIO images

Build once locally:

```bash
docker compose --profile silver build minio minio-init
```

Compose uses `flousflow/minio:2025-04-22` and `flousflow/mc:2025-04-16`
without pulling. Reuse these local tags across runs. To move the built images to
another machine with the same CPU architecture:

```bash
docker image save -o /tmp/flousflow-storage.tar flousflow/minio:2025-04-22 flousflow/mc:2025-04-16
# Transfer the archive, then on the other machine:
docker image load -i /tmp/flousflow-storage.tar
```

No registry publishing is configured. The source Dockerfile remains the fallback
for a clean machine or another architecture. Rebuild the images when changing
the source or runtime image.

## Reset local data

When a schema change needs a clean replay, stop the pipeline and remove its Kafka,
MinIO, and checkpoint volumes. This deletes all local messages and lakehouse tables;
the source CSV and downloaded Spark dependencies are kept.

```bash
docker compose --profile bronze --profile silver down
docker volume rm fina_kafka-data fina_minio-data fina_spark-checkpoints
docker compose --profile silver up -d --build spark-silver
# Wait for Kafka and Bronze to be healthy before replaying.
docker compose --profile silver ps -a
uv run --locked flousflow-producer
```

Reset all three data volumes together so Kafka offsets, Delta tables, and streaming
checkpoints start from the same empty state. Routine code changes only need a
rebuild; they do not require deleting data.

These volume names assume the Compose project is named `fina`. Use your project
prefix if you changed it. The producer defaults to the sample CSV; use the source
path and replay settings in the README for a full replay.
