"""Display hourly metrics from the Gold Delta table."""
import os
import argparse
from common.session import create_session
from common.settings import settings

parser = argparse.ArgumentParser()
parser.add_argument('--accounts', action='store_true', help='Show account activity instead of hourly metrics')
args = parser.parse_args()

spark = create_session(settings(), app_name='flousflow-gold-inspect')
try:
    if args.accounts:
        path = os.getenv('FLOUSFLOW_GOLD_ACCOUNTS_PATH', 's3a://flousflow/gold/account_activity')
        order = ['replay_id', 'account_id']
    else:
        path = os.getenv('FLOUSFLOW_GOLD_HOURLY_PATH', 's3a://flousflow/gold/hourly_transaction_metrics')
        order = ['replay_id', 'hour', 'transaction_type']
    spark.read.format('delta').load(path).orderBy(*order).show(50, truncate=False)
finally:
    spark.stop()
