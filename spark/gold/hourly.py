"""Build hourly transaction metrics from a Silver Delta snapshot."""
import os
from pyspark.sql import functions as F
from common.session import create_session
from common.settings import settings


def aggregate(transactions):
    return (transactions.groupBy('replay_id', F.date_trunc('hour', 'event_time').alias('hour'), 'transaction_type')
            .agg(F.count('*').alias('transaction_count'),
                 F.sum('amount').alias('total_amount'),
                 F.avg('amount').alias('average_amount'),
                 F.sum(F.when(F.col('is_fraud'), 1).otherwise(0)).alias('fraud_count'))
            .withColumn('fraud_rate', F.col('fraud_count') / F.col('transaction_count')))


def main():
    spark = create_session(settings(), app_name='flousflow-gold')
    source = os.getenv('FLOUSFLOW_SILVER_PATH', 's3a://flousflow/silver/transactions')
    target = os.getenv('FLOUSFLOW_GOLD_HOURLY_PATH', 's3a://flousflow/gold/hourly_transaction_metrics')
    try:
        metrics = aggregate(spark.read.format('delta').load(source))
        metrics.write.format('delta').mode('overwrite').save(target)
        print(f'Gold hourly metrics written to {target}', flush=True)
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
