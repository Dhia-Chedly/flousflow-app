"""Live serving checks. Replaces serving data using the current Silver snapshot."""
import psycopg
from psycopg import sql
from unittest.mock import patch
from common.session import create_session
from common.settings import settings
from serving.refresh import refresh
from serving.load import connect, publish, TABLES


def fingerprint(conn):
    values = []
    for name in [*TABLES, 'refresh_status']:
        # Compare complete contents, including refresh metadata after rollback.
        values.append(conn.execute(sql.SQL('SELECT count(*), md5(string_agg(row_to_json(t)::text, %s ORDER BY row_to_json(t)::text)) FROM {} t').format(
            sql.Identifier('serving', name)), ('',)).fetchone())
    return values

spark = create_session(settings(), app_name='flousflow-serving-selftest')
spark.conf.set('spark.databricks.delta.snapshotPartitions', '2')
try:
    with connect() as conn:
        frames, metadata = refresh(spark, conn)
        before_gate = fingerprint(conn)
        with patch.dict('os.environ', {'FLOUSFLOW_REFRESH_REPLAY_ID': 'missing-refresh-check',
                                      'FLOUSFLOW_REFRESH_EXPECTED_COUNT': '1',
                                      'FLOUSFLOW_REFRESH_WAIT_SECONDS': '0'}):
            try:
                refresh(spark, conn)
                raise AssertionError('Incomplete replay was published')
            except ValueError as exc:
                assert 'expected 1 valid Silver transactions, found 0' in str(exc)
        assert fingerprint(conn) == before_gate
        conn.execute('SELECT pg_advisory_lock(736219)')
        try:
            before = fingerprint(conn)
            for _ in range(2):
                publish(conn, frames, metadata)
                assert fingerprint(conn)[:2] == before[:2], 'Repeat load changed data'
            before = fingerprint(conn)
            try:
                publish(conn, frames, metadata, fail_after_first=True)
                raise AssertionError('Failure injection did not run')
            except RuntimeError as exc:
                assert str(exc) == 'injected refresh failure'
            assert fingerprint(conn) == before, 'Failure changed committed tables'
            with connect(reader=True) as reader:
                assert fingerprint(reader) == before
                try:
                    reader.execute('DELETE FROM serving.account_activity')
                    raise AssertionError('Reader can write')
                except psycopg.errors.InsufficientPrivilege:
                    pass
            with connect() as other:
                assert not other.execute('SELECT pg_try_advisory_lock(736219)').fetchone()[0]
        finally:
            conn.execute('SELECT pg_advisory_unlock(736219)')
    print('SERVING_SELFTEST_PASS: reconciliation, replay requirement, repeat load, rollback, reader, lock', flush=True)
finally:
    spark.stop()
