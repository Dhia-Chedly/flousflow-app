"""Local MinIO S3A configuration. Credentials come from the environment."""


def configure_storage(builder, config):
    return (builder
            .config('spark.hadoop.fs.s3a.endpoint', config['endpoint'])
            .config('spark.hadoop.fs.s3a.access.key', config['access_key'])
            .config('spark.hadoop.fs.s3a.secret.key', config['secret_key'])
            .config('spark.hadoop.fs.s3a.aws.credentials.provider',
                    'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')
            .config('spark.hadoop.fs.s3a.path.style.access', 'true')
            .config('spark.hadoop.fs.s3a.connection.ssl.enabled',
                    str(config['endpoint'].startswith('https://')).lower()))
