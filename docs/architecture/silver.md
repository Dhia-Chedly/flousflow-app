# Silver transactions

Silver reads the Bronze Delta stream and writes four tables under
`s3a://flousflow/silver/`. Its checkpoint is `/checkpoints/silver-transactions`.

| Table | Contents | Insert key |
| --- | --- | --- |
| `transactions` | Validated, typed transaction data | Event ID |
| `rejected` | Raw input, source metadata, validation codes and explanations | Kafka topic/partition/offset |
| `conflicts` | Valid input with an existing event ID and different business content | Kafka topic/partition/offset |
| `audit` | Accepted, duplicate, conflict, or rejected outcome | Kafka topic/partition/offset |

## Validation

Validation covers UTF-8 JSON, duplicate keys, field types, required fields, IDs,
timestamps, transaction types, fraud labels, and monetary values. IDs are
recomputed from source identity and replay ID. Amounts and balances use
`DECIMAL(18,2)`; values requiring rounding are rejected. Zero amounts are valid.
Balance reconciliation is not enforced because the source dataset does not
always satisfy it.

Account and transaction-type columns are renamed while preserving their values.
Fraud labels become booleans and timestamps use UTC. The content hash excludes
publication and processing metadata so retries can vary those fields.

## Deduplication and recovery

Within a batch, valid records are ordered by Kafka topic, partition, and offset
for each event ID. The first record wins; an existing Silver row wins across
batches. This does not establish global ordering across Kafka partitions.

The writer inserts each output idempotently. Partial batches can be retried, but
the four Delta tables commit independently. One writer owns the paths.

Audit counts represent distinct Kafka deliveries. Repeated Bronze copies of the
same offset count once; new offsets count as new deliveries. A new replay ID
creates distinct transactions. Kafka offset coordinates must not be reused while
retaining the existing audit tables.
