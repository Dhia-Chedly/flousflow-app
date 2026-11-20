"""Show Silver table counts and quality outcomes."""
import os
from common.session import create_session
from common.settings import settings

config = settings()
spark = create_session(config, app_name='flousflow-silver')
try:
    for name, variable, suffix in (
        ('transactions', 'FLOUSFLOW_SILVER_PATH', 'transactions'),
        ('rejected', 'FLOUSFLOW_SILVER_REJECTS', 'rejected'),
        ('conflicts', 'FLOUSFLOW_SILVER_CONFLICTS', 'conflicts'),
        ('audit', 'FLOUSFLOW_SILVER_AUDIT', 'audit'),
    ):
        path = os.getenv(variable, 's3a://flousflow/silver/' + suffix)
        table = spark.read.format('delta').load(path)
        if name == 'transactions' and os.getenv('FLOUSFLOW_INSPECT_REPLAY_ID'):
            match = table.filter(table.replay_id == os.environ['FLOUSFLOW_INSPECT_REPLAY_ID'])
            print(f"replay_matches: {match.count()}", flush=True)
        if name == 'audit':
            table.groupBy('outcome').count().orderBy('outcome').show()
        else:
            print(f'{name}: {table.count()}', flush=True)
finally:
    spark.stop()
