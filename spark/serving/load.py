"""Stage Gold rows and publish both tables in one PostgreSQL transaction."""
import os
from datetime import datetime, timezone
import psycopg
from psycopg import sql
from pyspark.sql import functions as F

TABLES = {
    'hourly_transaction_metrics': ['replay_id', 'hour', 'transaction_type', 'transaction_count',
                                  'total_amount', 'average_amount', 'fraud_count', 'fraud_rate'],
    'account_activity': ['replay_id', 'account_id', 'incoming_count', 'incoming_amount', 'incoming_fraud_count',
                         'outgoing_count', 'outgoing_amount', 'outgoing_fraud_count',
                         'first_activity_at', 'last_activity_at'],
}


def connect(reader=False):
    return psycopg.connect(host=os.getenv('FLOUSFLOW_POSTGRES_HOST', 'postgres'),
        dbname='flousflow', user='flousflow_reader' if reader else 'flousflow_loader',
        password=os.environ['FLOUSFLOW_POSTGRES_READER_PASSWORD' if reader else 'FLOUSFLOW_POSTGRES_LOADER_PASSWORD'],
        autocommit=True, options='-c timezone=UTC', connect_timeout=15)


def publish(conn, frames, metadata, fail_after_first=False):
    """Caller holds the refresh lock. Failure injection is only used by the self-test."""
    counts = {}
    with conn.transaction():
        with conn.cursor() as cursor:
            for name, columns in TABLES.items():
                stage = sql.Identifier('stage_' + name)
                target = sql.Identifier('serving', name)
                cursor.execute(sql.SQL('CREATE TEMP TABLE {} (LIKE {} INCLUDING ALL) ON COMMIT DROP').format(stage, target))
                with cursor.copy(sql.SQL('COPY {} ({}) FROM STDIN').format(
                        stage, sql.SQL(', ').join(map(sql.Identifier, columns)))) as copy:
                    for row in frames[name].select(*columns).toLocalIterator():
                        copy.write_row(tuple(value.replace(tzinfo=timezone.utc) if isinstance(value, datetime) else value for value in row))
                cursor.execute(sql.SQL('SELECT count(*) FROM {}').format(stage))
                counts[name] = cursor.fetchone()[0]
                frame = frames[name]
                if counts[name] != frame.count():
                    raise ValueError(f'{name}: PostgreSQL row count differs from Gold')
                count_col, amount_col, fraud_col = (('transaction_count', 'total_amount', 'fraud_count')
                    if name == 'hourly_transaction_metrics' else ('incoming_count', 'incoming_amount', 'incoming_fraud_count'))
                for cols in [(count_col, amount_col, fraud_col)] + ([('outgoing_count', 'outgoing_amount', 'outgoing_fraud_count')]
                                                                 if name == 'account_activity' else []):
                    cursor.execute(sql.SQL('SELECT {} FROM {}').format(
                        sql.SQL(', ').join(sql.SQL('sum({})').format(sql.Identifier(c)) for c in cols), stage))
                    actual = tuple(cursor.fetchone())
                    expected = tuple(frame.agg(*[F.sum(c) for c in cols]).first())
                    if actual != expected:
                        raise ValueError(f'{name}: PostgreSQL totals differ from Gold')
            for index, name in enumerate(TABLES):
                target = sql.Identifier('serving', name)
                cursor.execute(sql.SQL('DELETE FROM {}').format(target))
                cursor.execute(sql.SQL('INSERT INTO {} SELECT * FROM {}').format(target, sql.Identifier('stage_' + name)))
                if index == 0 and fail_after_first:
                    raise RuntimeError('injected refresh failure')
            cursor.execute('DELETE FROM serving.refresh_status')
            cursor.execute('''INSERT INTO serving.refresh_status
                (refreshed_at, silver_path, silver_snapshot, hourly_snapshot, accounts_snapshot, hourly_rows, account_rows)
                VALUES (clock_timestamp(), %s, %s, %s, %s, %s, %s)''',
                (*metadata, counts['hourly_transaction_metrics'], counts['account_activity']))
    return counts
