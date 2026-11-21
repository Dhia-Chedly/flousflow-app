"""Reconcile Gold with a fixed Silver snapshot and verify identical repeat writes.

Optionally wait for a named replay before taking the snapshot. Run one Gold writer.
"""
import os
import time
from pyspark.sql import functions as F
from delta.tables import DeltaTable
from common.session import create_session
from common.settings import settings
from gold.hourly import aggregate as hourly
from gold.accounts import aggregate as accounts

spark = create_session(settings(), app_name='flousflow-gold-validation')
spark.conf.set('spark.databricks.delta.snapshotPartitions', '2')
source = None
try:
    path = os.getenv('FLOUSFLOW_SILVER_PATH', 's3a://flousflow/silver/transactions')
    replay = os.getenv('FLOUSFLOW_VALIDATE_REPLAY_ID')
    expected = int(os.getenv('FLOUSFLOW_VALIDATE_COUNT', '10000'))
    deadline = time.monotonic() + 600
    while replay:
        count = spark.read.format('delta').load(path).filter(F.col('replay_id') == replay).count()
        if count == expected:
            break
        if count > expected or time.monotonic() > deadline:
            raise AssertionError(f'Replay {replay}: expected {expected}, found {count}')
        time.sleep(10)
    source = spark.read.format('delta').load(path).cache()
    baseline = source.agg(F.count('*'), F.sum('amount'), F.sum(F.col('is_fraud').cast('long'))).first()
    assert baseline[0] > 0, 'Validation requires nonempty Silver data'
    for name, function, variable in (
        ('hourly_transaction_metrics', hourly, 'FLOUSFLOW_GOLD_HOURLY_PATH'),
        ('account_activity', accounts, 'FLOUSFLOW_GOLD_ACCOUNTS_PATH')):
        target = os.getenv(variable, 's3a://flousflow/gold/' + name)
        first = None
        try:
            for attempt in range(2):
                function(source).write.format('delta').mode('overwrite').save(target)
                result = spark.read.format('delta').load(target)
                if name == 'hourly_transaction_metrics':
                    totals = result.agg(F.sum('transaction_count'), F.sum('total_amount'), F.sum('fraud_count')).first()
                    assert tuple(totals) == tuple(baseline), (totals, baseline)
                else:
                    for side in ('incoming', 'outgoing'):
                        totals = result.agg(*[F.sum(side + suffix) for suffix in ('_count', '_amount', '_fraud_count')]).first()
                        assert tuple(totals) == tuple(baseline), (side, totals, baseline)
                if attempt == 0:
                    committed = DeltaTable.forPath(spark, target).history(1).first().version
                    first = spark.read.format('delta').option('versionAsOf', committed).load(target)
                else:
                    assert first.exceptAll(result).isEmpty() and result.exceptAll(first).isEmpty(), name
            print(f'PASS {name}: reconciled with {baseline[0]} Silver rows; repeat output identical', flush=True)
        finally:
            if first is not None:
                first.unpersist()
    print(f'GOLD_VALIDATION_PASS replay={replay} count={expected if replay else baseline[0]}', flush=True)
finally:
    if source is not None:
        source.unpersist()
    spark.stop()
