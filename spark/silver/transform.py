"""Parse Bronze events and retain invalid inputs for investigation."""
from pyspark.sql import functions as F, types as T
from silver.money import valid_money
from silver.validation import json_type_error

MONEY = {'amount': 'amount', 'oldbalanceOrg': 'origin_balance_before',
         'newbalanceOrig': 'origin_balance_after', 'oldbalanceDest': 'destination_balance_before',
         'newbalanceDest': 'destination_balance_after'}
PAYLOAD_FIELDS = ['step', 'type', 'nameOrig', 'nameDest', 'isFraud', 'isFlaggedFraud', *MONEY]
SCHEMA = T.StructType([T.StructField(name, T.StringType()) for name in
    ['source_dataset_id', 'source_row_number', 'transaction_id', 'replay_id',
     'event_id', 'event_time', 'produced_at']] + [
    T.StructField('payload', T.StructType([T.StructField(name, T.StringType()) for name in PAYLOAD_FIELDS])),
    T.StructField('_corrupt_record', T.StringType())])




def classify(bronze):
    parsed = bronze.withColumn('event', F.from_json(F.col('raw_value').cast('string'), SCHEMA,
                            {'columnNameOfCorruptRecord': '_corrupt_record'}))
    for name in ['source_dataset_id', 'source_row_number', 'transaction_id',
                 'replay_id', 'event_id', 'event_time', 'produced_at']:
        parsed = parsed.withColumn(name, F.col('event.' + name))
    for name in PAYLOAD_FIELDS:
        parsed = parsed.withColumn(name, F.col('event.payload.' + name))
    parsed = parsed.withColumn('_json_type_error', F.udf(json_type_error, T.StringType())(F.col('raw_value')))
    checks = []
    def check(condition, code):
        checks.append(F.when(F.coalesce(condition, F.lit(False)), F.lit(code)))
    check(F.col('event').isNull() | F.col('event._corrupt_record').isNotNull() | (F.col('_json_type_error') == 'MALFORMED_JSON'), 'MALFORMED_JSON')
    check(F.col('_json_type_error') == 'INVALID_FIELD_TYPE', 'INVALID_FIELD_TYPE')
    required = ['source_dataset_id', 'source_row_number', 'transaction_id',
                'replay_id', 'event_id', 'event_time', 'produced_at', *PAYLOAD_FIELDS]
    check(F.greatest(*[F.col(c).isNull() | (F.length(F.trim(F.col(c))) == 0) for c in required]), 'MISSING_FIELD')
    for col in ('source_dataset_id', 'transaction_id', 'event_id'):
        check(~F.col(col).rlike('^[0-9a-f]{64}$'), 'INVALID_ID')
    check(~F.col('source_row_number').rlike('^[1-9][0-9]*$') | F.expr('try_cast(source_row_number as bigint)').isNull(), 'INVALID_SOURCE_ROW')
    check(~F.col('step').rlike('^[1-9][0-9]*$') | F.expr('try_cast(step as int)').isNull(), 'INVALID_STEP')
    check(~F.col('type').isin('PAYMENT', 'TRANSFER', 'CASH_OUT', 'DEBIT', 'CASH_IN'), 'INVALID_TYPE')
    for col in ('event_time', 'produced_at'):
        check(~F.col(col).rlike(r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,6})?Z$') | F.expr(f'try_cast({col} as timestamp)').isNull(), 'INVALID_TIMESTAMP')
    for col in ('isFraud', 'isFlaggedFraud'):
        check(~F.col(col).isin('0', '1'), 'INVALID_LABEL')
    for col in MONEY:
        narrow = F.expr(f'try_cast(`{col}` as decimal(18,2))')
        check(~F.udf(valid_money, T.BooleanType())(F.col(col)) | narrow.isNull(), 'INVALID_AMOUNT')
    expected_transaction = F.sha2(F.concat(F.lit('flousflow:'), F.col('source_dataset_id'), F.lit(':'), F.col('source_row_number')), 256)
    expected_event = F.sha2(F.concat(F.lit('flousflow:'), F.col('transaction_id'), F.lit(':'), F.col('replay_id')), 256)
    check((F.col('transaction_id') != expected_transaction) | (F.col('event_id') != expected_event), 'INVALID_ID')
    parsed = parsed.withColumn('rejection_codes', F.array_distinct(F.filter(F.array(*checks), lambda x: x.isNotNull())))
    return parsed


def transactions(frame):
    columns = [F.col(c) for c in ['event_id', 'transaction_id', 'replay_id', 'source_dataset_id']]
    columns += [F.col('source_row_number').cast('long').alias('source_row_number'),
                F.col('event_time').cast('timestamp').alias('event_time'),
                F.col('produced_at').cast('timestamp').alias('produced_at'),
                F.col('step').cast('int').alias('step'), F.col('type').alias('transaction_type'),
                F.col('nameOrig').alias('origin_account_id'), F.col('nameDest').alias('destination_account_id')]
    columns += [F.col(src).cast('decimal(18,2)').alias(dst) for src, dst in MONEY.items()]
    columns += [(F.col(c) == '1').alias(out) for c, out in [('isFraud', 'is_fraud'), ('isFlaggedFraud', 'is_flagged_fraud')]]
    columns += [F.col(c) for c in ('kafka_topic', 'kafka_partition', 'kafka_offset', 'kafka_timestamp')]
    columns += [F.col('ingested_at').alias('bronze_ingested_at'), F.current_timestamp().alias('silver_processed_at')]
    result = frame.select(*columns)
    content = ['transaction_id', 'replay_id', 'source_dataset_id', 'source_row_number', 'event_time', 'step',
               'transaction_type', 'origin_account_id', 'destination_account_id', *MONEY.values(), 'is_fraud', 'is_flagged_fraud']
    return result.withColumn('content_hash', F.sha2(F.to_json(F.struct(*content)), 256))
