"""Kafka source settings. Offsets in the checkpoint take precedence on restart."""


def read_kafka(spark, config):
    return (spark.readStream.format('kafka')
            .option('kafka.bootstrap.servers', config['brokers'])
            .option('subscribe', config['topic'])
            .option('startingOffsets', config['starting_offsets'])
            .option('failOnDataLoss', 'true')
            .option('maxOffsetsPerTrigger', '10000')
            .load())
