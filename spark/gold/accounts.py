"""Aggregate account activity; amounts are flows, not account balances."""
from pyspark.sql import functions as F


def aggregate(transactions):
    def side(account, direction):
        return transactions.select('replay_id', 'event_time', 'amount', 'is_fraud',
                                   F.col(account).alias('account_id'), F.lit(direction).alias('direction'))
    activity = side('origin_account_id', 'outgoing').unionByName(side('destination_account_id', 'incoming'))
    metrics = []
    for direction in ('incoming', 'outgoing'):
        match = F.col('direction') == direction
        metrics.extend([
            F.sum(F.when(match, 1).otherwise(0)).alias(direction + '_count'),
            F.sum(F.when(match, F.col('amount')).otherwise(F.lit(0))).alias(direction + '_amount'),
            F.sum(F.when(match & F.col('is_fraud'), 1).otherwise(0)).alias(direction + '_fraud_count')])
    return activity.groupBy('replay_id', 'account_id').agg(
        *metrics, F.min('event_time').alias('first_activity_at'), F.max('event_time').alias('last_activity_at'))


def main():
    from gold.refresh import run_table
    run_table('account_activity')


if __name__ == '__main__':
    main()
