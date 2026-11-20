"""Spark/Delta session; JVM dependencies are pinned in spark-defaults.conf."""
from pyspark.sql import SparkSession
from common.storage import configure_storage


def create_session(config, app_name='flousflow-bronze'):
    builder = (SparkSession.builder.appName(app_name)
               .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension')
               .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog')
               .config('spark.sql.session.timeZone', 'UTC')
               .config('spark.sql.shuffle.partitions', '2')
               .config('spark.redaction.regex', '(?i)secret|password|access.key|token'))
    spark = configure_storage(builder, config).getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    return spark
