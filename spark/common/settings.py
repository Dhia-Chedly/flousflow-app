"""Environment settings shared by ingestion and inspection jobs."""
import os


def settings():
    def required(name):
        value = os.environ.get(name, '')
        if not value.strip():
            raise ValueError(f'{name} must be set')
        return value

    return {
        'endpoint': os.environ.get('FLOUSFLOW_S3_ENDPOINT', 'http://minio:9000'),
        'access_key': required('MINIO_ROOT_USER'),
        'secret_key': required('MINIO_ROOT_PASSWORD'),
        'path': os.environ.get('FLOUSFLOW_BRONZE_PATH', 's3a://flousflow/bronze/transactions'),
        'checkpoint': os.environ.get('FLOUSFLOW_BRONZE_CHECKPOINT', '/checkpoints/bronze-transactions-v1'),
        'brokers': os.environ.get('FLOUSFLOW_KAFKA_BOOTSTRAP_SERVERS', 'kafka:19092'),
        'topic': os.environ.get('FLOUSFLOW_KAFKA_TOPIC', 'flousflow.transactions.v1'),
        'starting_offsets': os.environ.get('FLOUSFLOW_KAFKA_STARTING_OFFSETS', 'earliest'),
    }
