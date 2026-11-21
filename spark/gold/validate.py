"""Reconcile both Gold tables and compare repeated writes under the shared lock."""
from common.session import create_session
from common.settings import settings
from gold.refresh import snapshot, write_table, TABLES


def main():
    spark = create_session(settings(), app_name='flousflow-gold-validation')
    spark.conf.set('spark.databricks.delta.snapshotPartitions', '2')
    try:
        with snapshot(spark) as (source, baseline, _):
            for name in TABLES:
                first, _ = write_table(spark, name, source, baseline)
                second, _ = write_table(spark, name, source, baseline)
                assert first.exceptAll(second).isEmpty() and second.exceptAll(first).isEmpty(), name
                print(f'PASS {name}: reconciled with {baseline[0]} Silver rows; repeat output identical', flush=True)
        print('GOLD_VALIDATION_PASS', flush=True)
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
