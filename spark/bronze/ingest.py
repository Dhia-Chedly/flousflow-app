"""Append every received Kafka record to Bronze, including malformed payloads."""
import signal
from pathlib import Path
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQueryListener
from common.kafka import read_kafka
from common.session import create_session
from common.settings import settings


def bronze_records(source):
    return source.select(
        F.col('key').alias('raw_key'), F.col('value').alias('raw_value'),
        F.col('topic').alias('kafka_topic'), F.col('partition').alias('kafka_partition'),
        F.col('offset').alias('kafka_offset'), F.col('timestamp').alias('kafka_timestamp'),
        F.col('timestampType').alias('kafka_timestamp_type'),
        F.current_timestamp().alias('ingested_at'))


def main():
    ready = Path("/tmp/bronze-ready")
    ready.unlink(missing_ok=True)
    config = settings()
    spark = create_session(config)
    class Readiness(StreamingQueryListener):
        def onQueryStarted(self, event):
            pass

        def onQueryProgress(self, event):
            ready.touch()

        def onQueryIdle(self, event):
            ready.touch()

        def onQueryTerminated(self, event):
            ready.unlink(missing_ok=True)

    spark.streams.addListener(Readiness())
    try:
        query = (bronze_records(read_kafka(spark, config)).writeStream
                 .format('delta').outputMode('append')
                 .option('checkpointLocation', config['checkpoint'])
                 .trigger(processingTime='5 seconds').start(config['path']))
        signal.signal(signal.SIGTERM, lambda *_: query.stop())
        query.awaitTermination()
    finally:
        ready.unlink(missing_ok=True)
        spark.stop()


if __name__ == '__main__':
    main()
