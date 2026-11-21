"""Refresh both Gold tables from one Silver snapshot, then publish PostgreSQL."""
from common.session import create_session
from common.settings import settings
from gold.refresh import snapshot, write_table, TABLES
from serving.load import connect, publish


def refresh(spark, conn):
    # Session lock covers Gold writes and PostgreSQL publication. Released on disconnect.
    if not conn.execute('SELECT pg_try_advisory_lock(736219)').fetchone()[0]:
        raise RuntimeError('Another serving refresh is running')
    try:
        with snapshot(spark) as (source, baseline, metadata):
            frames, versions = {}, []
            for name in TABLES:
                frames[name], version = write_table(spark, name, source, baseline)
                versions.append(version)
            metadata = (*metadata, *versions)
            counts = publish(conn, frames, metadata)
            print(f'Serving refreshed: {counts}', flush=True)
            return frames, metadata
    finally:
        conn.execute('SELECT pg_advisory_unlock(736219)')


def main():
    spark = create_session(settings(), app_name='flousflow-serving')
    spark.conf.set('spark.databricks.delta.snapshotPartitions', '2')
    try:
        with connect() as conn:
            refresh(spark, conn)
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
