"""Shared snapshot selection, reconciliation, and Delta writes for Gold jobs."""
import os
import time
from contextlib import contextmanager
from delta.tables import DeltaTable
from pyspark.sql import functions as F
from gold.locking import writer_lock
from gold.replay import replay_requirement

TABLES = ('hourly_transaction_metrics', 'account_activity')


@contextmanager
def snapshot(spark):
    with writer_lock():
        path = os.getenv('FLOUSFLOW_SILVER_PATH', 's3a://flousflow/silver/transactions')
        replay, expected, timeout = replay_requirement()
        deadline = time.monotonic() + timeout
        while True:
            source = None
            version = None
            if DeltaTable.isDeltaTable(spark, path):
                version = DeltaTable.forPath(spark, path).history(1).first().version
                source = spark.read.format('delta').option('versionAsOf', version).load(path)
            if not replay:
                if source is None:
                    raise ValueError('Silver transactions table does not exist')
                break
            count = source.filter(F.col('replay_id') == replay).count() if source is not None else 0
            if count == expected:
                break
            if count > expected or time.monotonic() >= deadline:
                raise ValueError(f'Replay {replay}: expected {expected} valid Silver transactions, found {count}')
            time.sleep(min(5, max(0, deadline - time.monotonic())))
        source = source.cache()
        try:
            baseline = tuple(source.agg(F.count('*'), F.sum('amount'), F.sum(F.col('is_fraud').cast('long'))).first())
            if not baseline[0]:
                raise ValueError('Silver is empty; refusing to replace Gold data')
            yield source, baseline, (path, version)
        finally:
            source.unpersist()


def write_table(spark, name, source, baseline):
    # Lazy imports allow aggregation modules to expose their standalone entry points.
    from gold.hourly import aggregate as hourly
    from gold.accounts import aggregate as accounts
    function, env = {
        TABLES[0]: (hourly, 'FLOUSFLOW_GOLD_HOURLY_PATH'),
        TABLES[1]: (accounts, 'FLOUSFLOW_GOLD_ACCOUNTS_PATH'),
    }[name]
    target = os.getenv(env, 's3a://flousflow/gold/' + name)
    function(source).write.format('delta').mode('overwrite').save(target)
    committed = DeltaTable.forPath(spark, target).history(1).first().version
    frame = spark.read.format('delta').option('versionAsOf', committed).load(target)
    columns = [('transaction_count', 'total_amount', 'fraud_count')] if name == TABLES[0] else [
        ('incoming_count', 'incoming_amount', 'incoming_fraud_count'),
        ('outgoing_count', 'outgoing_amount', 'outgoing_fraud_count')]
    for cols in columns:
        if tuple(frame.agg(*[F.sum(c) for c in cols]).first()) != baseline:
            raise ValueError(f'{name} does not reconcile with Silver')
    return frame, committed


def run_table(name):
    from common.session import create_session
    from common.settings import settings
    spark = create_session(settings(), app_name='flousflow-gold')
    spark.conf.set('spark.databricks.delta.snapshotPartitions', '2')
    try:
        with snapshot(spark) as (source, baseline, _):
            write_table(spark, name, source, baseline)
        print(f'Gold refreshed: {name}', flush=True)
    finally:
        spark.stop()
