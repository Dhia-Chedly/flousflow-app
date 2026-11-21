"""Display hourly metrics from the Gold Delta table."""
import os
from common.session import create_session
from common.settings import settings

spark = create_session(settings(), app_name='flousflow-gold-inspect')
try:
    path = os.getenv('FLOUSFLOW_GOLD_HOURLY_PATH', 's3a://flousflow/gold/hourly_transaction_metrics')
    spark.read.format('delta').load(path).orderBy('replay_id', 'hour', 'transaction_type').show(50, truncate=False)
finally:
    spark.stop()
