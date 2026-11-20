"""Inspect Bronze once, or serve test requests on container loopback."""
import json
import os
import sys
from http.server import BaseHTTPRequestHandler, HTTPServer
from pyspark.sql import functions as F
from common.session import create_session
from common.settings import settings


def snapshot(spark, path, request):
    table = spark.read.format('delta').load(path)
    replay = F.get_json_object(F.col('raw_value').cast('string'), '$.replay_id')
    match = replay == request.get('replay_id', '')
    raw = F.col('raw_value') == F.lit(request.get('raw_value', '').encode())
    result = table.agg(
        F.count('*').alias('count'),
        F.countDistinct('kafka_topic', 'kafka_partition', 'kafka_offset').alias('unique_offsets'),
        F.count(F.when(match, 1)).alias('replay_count'),
        F.count(F.when(raw, 1)).alias('raw_count'),
    ).first().asDict()
    result['offsets'] = [row.asDict() for row in table.groupBy('kafka_topic', 'kafka_partition')
                         .agg(F.max('kafka_offset').alias('max_offset')).collect()]
    return result


def main():
    config = settings()
    spark = create_session(config)
    try:
        if '--serve' in sys.argv:
            class Handler(BaseHTTPRequestHandler):
                def do_POST(self):
                    try:
                        request = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
                        result = snapshot(spark, config['path'], request)
                    except Exception as exc:
                        result = {'error': str(exc)}
                    payload = json.dumps(result).encode()
                    self.send_response(200)
                    self.send_header('Content-Length', str(len(payload)))
                    self.end_headers()
                    self.wfile.write(payload)

            HTTPServer(('127.0.0.1', 8765), Handler).serve_forever()
        else:
            table = spark.read.format('delta').load(config['path'])
            table.printSchema()
            table.select(F.col('raw_value').cast('string')).show(3, truncate=120)
            print('BRONZE_RESULT=' + json.dumps(snapshot(spark, config['path'],
                  {'replay_id': os.environ.get('FLOUSFLOW_INSPECT_REPLAY_ID', '')})))
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
