"""Integration test: isolated local Delta paths; no Kafka or user tables are modified."""
import hashlib
import json
import os
import uuid
import tempfile
from datetime import datetime
from pathlib import Path
from pyspark.sql import types as T
from common.session import create_session
from common.settings import settings
import silver.ingest as ingestion


def digest(value):
    return hashlib.sha256(value.encode()).hexdigest()


def event(row, replay='run', amount='1.25', produced='2020-01-01T00:00:00Z'):
    dataset = 'a' * 64
    tid = digest(f'flousflow:{dataset}:{row}')
    eid = digest(f'flousflow:{tid}:{replay}')
    return {'source_dataset_id': dataset, 'source_row_number': row,
            'transaction_id': tid, 'replay_id': replay, 'event_id': eid,
            'event_time': '2020-01-01T00:00:00Z', 'produced_at': produced,
            'payload': {'step': 1, 'type': 'TRANSFER', 'amount': amount, 'nameOrig': 'C1',
                        'oldbalanceOrg': '100', 'newbalanceOrig': '0', 'nameDest': 'C2',
                        'oldbalanceDest': '1.916920493E7', 'newbalanceDest': '0',
                        'isFraud': 1, 'isFlaggedFraud': 0}}


def main():
    spark = create_session(settings(), app_name='flousflow-silver-selftest')
    spark.conf.set('spark.databricks.delta.snapshotPartitions', '2')
    schema = T.StructType([T.StructField('raw_key', T.BinaryType()), T.StructField('raw_value', T.BinaryType()),
        T.StructField('kafka_topic', T.StringType()), T.StructField('kafka_partition', T.IntegerType()),
        T.StructField('kafka_offset', T.LongType()), T.StructField('kafka_timestamp', T.TimestampType()),
        T.StructField('kafka_timestamp_type', T.IntegerType()), T.StructField('ingested_at', T.TimestampType())])
    def batch(values, start):
        rows = []
        for i, value in enumerate(values, start):
            raw = value if isinstance(value, str) else json.dumps(value)
            rows.append((b'key', raw.encode(), 'test', 0, i, datetime(2020, 1, 1), 0, datetime(2020, 1, 1)))
        return spark.createDataFrame(rows, schema)
    try:
        with tempfile.TemporaryDirectory(prefix='silver-test-') as tmp:
            test_root = ('s3a://flousflow/silver-selftests/' + uuid.uuid4().hex
                         if os.getenv('FLOUSFLOW_SELFTEST_S3') == '1' else tmp)
            paths = {name: test_root + '/' + name for name in ('transactions', 'rejects', 'conflicts', 'audit')}
            # Duplicate with a different publication time, conflicting amount, malformed and rounded values.
            wrong_type = event(4)
            wrong_type['source_row_number'] = '1'
            wrong_step = event(8)
            wrong_step['payload']['step'] = 0
            wrong_id = event(9)
            wrong_id['event_id'] = 'f' * 64
            wrong_time = event(10)
            wrong_time['event_time'] = 'not-a-time'
            wrong_label = event(11)
            wrong_label['payload']['isFraud'] = 2
            missing = event(5)
            del missing['payload']['nameOrig']
            data = batch([event(1), event(1, produced='2020-01-02T00:00:00Z'),
                          event(1, amount='2.25'), '{bad-json', event(2, amount='0.001'),
                          event(1, replay='second'), event(3, amount='0'), wrong_type, missing,
                          wrong_step, wrong_id, wrong_time, wrong_label], 0)
            original = ingestion.insert_once
            def fail_after_transactions(spark, rows, path, keys):
                if path == paths['conflicts']:
                    raise RuntimeError('injected interruption')
                return original(spark, rows, path, keys)
            ingestion.insert_once = fail_after_transactions
            try:
                try:
                    ingestion.process_batch(spark, data, paths)
                    raise AssertionError('interruption not raised')
                except RuntimeError as exc:
                    assert str(exc) == 'injected interruption'
            finally:
                ingestion.insert_once = original
            ingestion.process_batch(spark, data, paths)
            ingestion.process_batch(spark, data, paths)
            outcomes = {row['outcome']: row['count'] for row in spark.read.format('delta').load(paths['audit']).groupBy('outcome').count().collect()}
            assert outcomes == {'accepted': 3, 'duplicate': 1, 'conflict': 1, 'rejected': 8}, outcomes
            assert spark.read.format('delta').load(paths['transactions']).count() == 3
            assert spark.read.format('delta').load(paths['rejects']).count() == 8
            assert spark.read.format('delta').load(paths['conflicts']).count() == 1
            rejects = spark.read.format('delta').load(paths['rejects']).collect()
            codes = {code for row in rejects for code in row.rejection_codes}
            assert {'MALFORMED_JSON', 'INVALID_AMOUNT', 'INVALID_FIELD_TYPE', 'MISSING_FIELD',
                    'INVALID_STEP', 'INVALID_ID', 'INVALID_TIMESTAMP', 'INVALID_LABEL'} <= codes, codes
            assert any(bytes(row.raw_value) == b'{bad-json' and row.rejection_reasons for row in rejects)
            conflict = spark.read.format('delta').load(paths['conflicts']).first()
            assert conflict.content_hash != conflict.existing_content_hash and conflict.raw_value
            typed = spark.read.format('delta').load(paths['transactions']).collect()
            assert any(str(row.destination_balance_before) == '19169204.93' for row in typed)
            assert any(str(row.amount) == '0.00' for row in typed)
            # Exercise checkpoint restart and cross-batch duplicates/conflicts.
            bronze_path = test_root + '/bronze'
            checkpoint = str(Path(tmp) / 'checkpoint')
            batch([event(1), event(1, amount='3.00'), event(6)], 100).write.format('delta').save(bronze_path)
            def stream_once():
                query = (spark.readStream.format('delta').load(bronze_path).writeStream
                         .foreachBatch(lambda df, batch_id: ingestion.process_batch(spark, df, paths))
                         .option('checkpointLocation', checkpoint).trigger(availableNow=True).start())
                query.awaitTermination()
            stream_once()
            before = spark.read.format('delta').load(paths['audit']).count()
            stream_once()
            assert spark.read.format('delta').load(paths['audit']).count() == before
            batch([event(7)], 103).write.format('delta').mode('append').save(bronze_path)
            stream_once()
            counts = {row['outcome']: row['count'] for row in spark.read.format('delta').load(paths['audit']).groupBy('outcome').count().collect()}
            assert counts == {'accepted': 5, 'duplicate': 2, 'conflict': 2, 'rejected': 8}, counts
            if os.getenv('FLOUSFLOW_SELFTEST_S3') == '1':
                target = spark._jvm.org.apache.hadoop.fs.Path(test_root)
                assert target.getFileSystem(spark._jsc.hadoopConfiguration()).delete(target, True)
            print('SILVER_SELFTEST_PASS: retry, duplicates, conflicts, malformed, exact decimals, replay, checkpoint restart', flush=True)
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
