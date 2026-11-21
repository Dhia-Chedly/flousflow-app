"""Check grouping, reconciliation and repeated writes with isolated Delta tables."""
import tempfile
from datetime import datetime
from decimal import Decimal
from pyspark.sql import types as T, functions as F
from common.session import create_session
from common.settings import settings
from gold.hourly import aggregate

spark = create_session(settings(), app_name='flousflow-gold-selftest')
try:
    schema = T.StructType([T.StructField('replay_id', T.StringType()), T.StructField('event_time', T.TimestampType()),
        T.StructField('transaction_type', T.StringType()), T.StructField('amount', T.DecimalType(18, 2)),
        T.StructField('is_fraud', T.BooleanType())])
    source = spark.createDataFrame([
        ('a', datetime(2020, 1, 1, 1, 5), 'TRANSFER', Decimal('1.00'), True),
        ('a', datetime(2020, 1, 1, 1, 30), 'TRANSFER', Decimal('2.00'), False),
        ('b', datetime(2020, 1, 1, 1, 30), 'TRANSFER', Decimal('4.00'), False),
        ('a', datetime(2020, 1, 1, 2, 0), 'PAYMENT', Decimal('3.00'), True)], schema)
    with tempfile.TemporaryDirectory(prefix='gold-test-') as directory:
        path = directory + '/hourly'
        for _ in range(2):
            aggregate(source).write.format('delta').mode('overwrite').save(path)
            gold = spark.read.format('delta').load(path)
            assert gold.count() == 3
            totals = gold.agg(F.sum('transaction_count'), F.sum('total_amount'), F.sum('fraud_count')).first()
            assert tuple(totals) == (4, Decimal('10.00'), 2), totals
            row = gold.filter((F.col('replay_id') == 'a') & (F.col('transaction_type') == 'TRANSFER')).first()
            assert row.transaction_count == 2 and row.total_amount == Decimal('3.00')
            assert row.average_amount == Decimal('1.50') and row.fraud_rate == 0.5
    print('GOLD_SELFTEST_PASS', flush=True)
finally:
    spark.stop()
