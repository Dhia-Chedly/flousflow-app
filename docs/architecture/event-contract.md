# Transaction event v1

One UTF-8 JSON object per CSV record, published to `flousflow.transactions.v1`.
The Kafka key is `event_id`. All fields are required and non-null.

| Field | Type | Meaning |
| --- | --- | --- |
| `schema_version` | Integer | `1` |
| `source_dataset_id` | String | SHA-256 of the original CSV bytes |
| `source_row_number` | Integer | One-based data-record ordinal, excluding the header |
| `transaction_id` | String | Source-record identity |
| `replay_id` | String | Nonempty simulation identity |
| `event_id` | String | Record identity within a replay |
| `event_time` | String | Simulated UTC timestamp |
| `produced_at` | String | Publication-attempt UTC timestamp |
| `payload` | Object | All eleven source columns |

## Identifiers

Hash these UTF-8 strings with SHA-256, without a trailing newline. Use lowercase
hexadecimal output and base-ten row numbers without leading zeros:

```text
transaction_id = SHA256("flousflow:v1:{source_dataset_id}:{source_row_number}")
event_id       = SHA256("flousflow:v1:{transaction_id}:{replay_id}")
```

Identical rows at different positions remain separate source records.

## Payload

| Fields | JSON type | Validation |
| --- | --- | --- |
| `step` | Integer | Positive |
| `type` | String | `PAYMENT`, `TRANSFER`, `CASH_OUT`, `DEBIT`, or `CASH_IN` |
| `nameOrig`, `nameDest` | String | Nonempty; preserve source values |
| `amount`, `oldbalanceOrg`, `newbalanceOrig`, `oldbalanceDest`, `newbalanceDest` | Decimal string | Nonnegative, at most two decimal places, fits `DECIMAL(18,2)` |
| `isFraud`, `isFlaggedFraud` | Integer | `0` or `1` |

Zero amounts are valid. Do not enforce balance reconciliation. Keep fraud labels
separate from predictive features. The producer stops at the first invalid record
and reports its ordinal; earlier records may already have been published.

## Time and recovery

The replay convention treats each step as one hour:

```text
event_time = configured UTC origin + (step - 1) hours
```

The default origin is `2020-01-01T00:00:00Z`. Timestamps use RFC 3339 UTC format.
Replay speed controls publication pace independently of simulated time.

Recovery reuses the source bytes, replay ID, and time origin. A new simulation
uses a new replay ID. The producer waits for each acknowledgement and stops on
delivery failure or timeout. A timeout has an unknown outcome. Restart begins at
row one, so duplicate deliveries remain possible despite client idempotence.

Bronze will preserve events and add Kafka offsets and ingestion time. Silver will
own validation and deduplication by `event_id`; a bounded deduplication window
alone cannot remove duplicates across arbitrary full replays.
