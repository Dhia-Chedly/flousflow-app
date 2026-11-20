"""Read-only smoke check for the Silver parser on existing Bronze data."""
from common.session import create_session
from common.settings import settings
from silver.transform import classify, transactions
from pyspark.sql import functions as F

config = settings()
spark = create_session(config, app_name='flousflow-silver')
try:
    bronze = spark.read.format('delta').load(config['path']).limit(100)
    parsed = classify(bronze).cache()
    summary = parsed.groupBy(F.size('rejection_codes').alias('errors')).count().collect()
    transactions(parsed.filter(F.size('rejection_codes') == 0)).show(2, truncate=40)
    print('SILVER_SMOKE=' + str(summary), flush=True)
finally:
    spark.stop()
