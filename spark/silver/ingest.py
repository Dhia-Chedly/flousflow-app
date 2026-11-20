"""Single-writer Bronze to Silver stream."""
import os
import signal
from pathlib import Path
from pyspark.sql.streaming import StreamingQueryListener
from delta.tables import DeltaTable
from pyspark.sql import Window, functions as F
from common.session import create_session
from common.settings import settings
from silver.transform import classify, transactions


REASONS = {
    'MALFORMED_JSON': 'Payload is not valid UTF-8 JSON or has duplicate keys',
    'INVALID_FIELD_TYPE': 'A field has an unexpected JSON type',
    'MISSING_FIELD': 'A required field is missing or empty',
    'INVALID_ID': 'An identifier has invalid syntax or does not match its source',
    'INVALID_SOURCE_ROW': 'Source row number must be a positive 64-bit integer',
    'INVALID_STEP': 'Step must be a positive 32-bit integer',
    'INVALID_TYPE': 'Transaction type is not supported',
    'INVALID_TIMESTAMP': 'Timestamp must be a valid UTC timestamp',
    'INVALID_LABEL': 'Fraud labels must be 0 or 1',
    'INVALID_AMOUNT': 'Amount or balance cannot be represented exactly as nonnegative decimal(18,2)',
}


def insert_once(spark, rows, path, keys):
    if not DeltaTable.isDeltaTable(spark, path):
        rows.limit(0).write.format('delta').mode('errorifexists').save(path)
    rows = rows.dropDuplicates(keys)
    condition = ' AND '.join(f't.`{k}` = s.`{k}`' for k in keys)
    DeltaTable.forPath(spark, path).alias('t').merge(rows.alias('s'), condition).whenNotMatchedInsertAll().execute()


def process_batch(spark, bronze, paths):
    parsed = classify(bronze).cache()
    try:
        reason_map = F.create_map(*[item for pair in REASONS.items() for item in (F.lit(pair[0]), F.lit(pair[1]))])
        rejected = parsed.filter(F.size('rejection_codes') > 0).select(
            'raw_key', 'raw_value', 'kafka_topic', 'kafka_partition', 'kafka_offset', 'kafka_timestamp',
            'ingested_at', 'event_id', 'transaction_id', 'replay_id', 'rejection_codes',
            F.transform('rejection_codes', lambda code: F.element_at(reason_map, code)).alias('rejection_reasons'),
            F.current_timestamp().alias('rejected_at'))
        insert_once(spark, rejected, paths['rejects'], ['kafka_topic', 'kafka_partition', 'kafka_offset'])
        valid = transactions(parsed.filter(F.size('rejection_codes') == 0)).cache()
        compared = None
        order = Window.partitionBy('event_id').orderBy('kafka_topic', 'kafka_partition', 'kafka_offset')
        ranked = valid.withColumn('_rank', F.row_number().over(order))
        winners = ranked.filter('_rank = 1').drop('_rank').cache()
        try:
            # Insert winners first. Retrying after a partial commit is safe.
            insert_once(spark, winners, paths['transactions'], ['event_id'])
            existing = spark.read.format('delta').load(paths['transactions']).select(
                F.col('event_id').alias('_existing_id'), F.col('content_hash').alias('_existing_hash'),
                *[F.col(c).alias('_winner_' + c) for c in ('kafka_topic', 'kafka_partition', 'kafka_offset')])
            compared = valid.join(existing, valid.event_id == existing._existing_id).cache()
            raw = parsed.select('kafka_topic', 'kafka_partition', 'kafka_offset', 'raw_key', 'raw_value').dropDuplicates(
                ['kafka_topic', 'kafka_partition', 'kafka_offset'])
            conflicts = (compared.filter(F.col('content_hash') != F.col('_existing_hash'))
                         .drop('_existing_id', '_winner_kafka_topic', '_winner_kafka_partition', '_winner_kafka_offset')
                         .withColumnRenamed('_existing_hash', 'existing_content_hash')
                         .join(raw, ['kafka_topic', 'kafka_partition', 'kafka_offset']))
            insert_once(spark, conflicts, paths['conflicts'], ['kafka_topic', 'kafka_partition', 'kafka_offset'])
            winner = ((F.col('kafka_topic') == F.col('_winner_kafka_topic')) &
                      (F.col('kafka_partition') == F.col('_winner_kafka_partition')) &
                      (F.col('kafka_offset') == F.col('_winner_kafka_offset')))
            valid_audit = compared.select('kafka_topic', 'kafka_partition', 'kafka_offset',
                F.when(F.col('content_hash') != F.col('_existing_hash'), 'conflict')
                 .when(winner, 'accepted').otherwise('duplicate').alias('outcome'))
            rejected_audit = rejected.select('kafka_topic', 'kafka_partition', 'kafka_offset',
                                             F.lit('rejected').alias('outcome'))
            audit = valid_audit.unionByName(rejected_audit).withColumn('recorded_at', F.current_timestamp())
            insert_once(spark, audit, paths['audit'], ['kafka_topic', 'kafka_partition', 'kafka_offset'])
        finally:
            if compared is not None:
                compared.unpersist()
            winners.unpersist()
            valid.unpersist()
    finally:
        parsed.unpersist()


class Readiness(StreamingQueryListener):
    """Mark a successful batch or idle notification; log batch progress."""

    def __init__(self, ready):
        super().__init__()
        self.ready = ready

    def onQueryStarted(self, event):
        pass

    def onQueryProgress(self, event):
        progress = event.progress
        self.ready.touch()
        print(f"Silver batch {progress.batchId}: {progress.numInputRows} input rows, "
              f"{progress.durationMs.get('triggerExecution', 0)} ms", flush=True)

    def onQueryIdle(self, event):
        self.ready.touch()

    def onQueryTerminated(self, event):
        self.ready.unlink(missing_ok=True)


def main():
    ready = Path('/tmp/silver-ready')
    ready.unlink(missing_ok=True)
    config = settings()
    paths = {'transactions': os.getenv('FLOUSFLOW_SILVER_PATH', 's3a://flousflow/silver/transactions'),
             'rejects': os.getenv('FLOUSFLOW_SILVER_REJECTS', 's3a://flousflow/silver/rejected'),
             'conflicts': os.getenv('FLOUSFLOW_SILVER_CONFLICTS', 's3a://flousflow/silver/conflicts'),
             'audit': os.getenv('FLOUSFLOW_SILVER_AUDIT', 's3a://flousflow/silver/audit')}
    checkpoint = os.getenv('FLOUSFLOW_SILVER_CHECKPOINT', '/checkpoints/silver-transactions')
    spark = create_session(config, app_name='flousflow-silver')
    spark.conf.set('spark.databricks.delta.snapshotPartitions', '2')
    spark.streams.addListener(Readiness(ready))
    try:
        query = (spark.readStream.format('delta').option('maxFilesPerTrigger', '10').load(config['path']).writeStream
                 .foreachBatch(lambda df, batch_id: process_batch(spark, df, paths))
                 .option('checkpointLocation', checkpoint).trigger(processingTime='30 seconds').start())
        signal.signal(signal.SIGTERM, lambda *_: query.stop())
        query.awaitTermination()
    finally:
        ready.unlink(missing_ok=True)
        spark.stop()


if __name__ == '__main__':
    main()
