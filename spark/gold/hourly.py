"""Build hourly transaction metrics from a Silver Delta snapshot."""
from pyspark.sql import functions as F


def aggregate(transactions):
    return (transactions.groupBy('replay_id', F.date_trunc('hour', 'event_time').alias('hour'), 'transaction_type')
            .agg(F.count('*').alias('transaction_count'),
                 F.sum('amount').alias('total_amount'),
                 F.avg('amount').alias('average_amount'),
                 F.sum(F.when(F.col('is_fraud'), 1).otherwise(0)).alias('fraud_count'))
            .withColumn('fraud_rate', F.col('fraud_count') / F.col('transaction_count')))


def main():
    from gold.refresh import run_table
    run_table('hourly_transaction_metrics')


if __name__ == '__main__':
    main()
