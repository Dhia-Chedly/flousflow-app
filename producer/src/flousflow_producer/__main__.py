"""Replay a PaySim CSV into Kafka, one acknowledged event at a time."""
import csv
import hashlib
import json
import math
import os
import re
import sys
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path

FIELDS = ('step', 'type', 'amount', 'nameOrig', 'oldbalanceOrg', 'newbalanceOrig',
          'nameDest', 'oldbalanceDest', 'newbalanceDest', 'isFraud', 'isFlaggedFraud')
MONEY = ('amount', 'oldbalanceOrg', 'newbalanceOrig', 'oldbalanceDest', 'newbalanceDest')
TYPES = {'PAYMENT', 'TRANSFER', 'CASH_OUT', 'DEBIT', 'CASH_IN'}


def digest(value):
    return hashlib.sha256(value.encode('utf-8')).hexdigest()


def dataset_hash(path):
    sha = hashlib.sha256()
    with open(path, 'rb') as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b''):
            sha.update(chunk)
    return sha.hexdigest()


def utc_string(value):
    return value.astimezone(timezone.utc).isoformat(timespec='microseconds').replace('+00:00', 'Z')


def make_event(row, dataset_id, number, replay_id, origin, now):
    try:
        if None in row or any(row.get(k) is None or not row[k].strip() for k in FIELDS):
            raise ValueError('missing or extra fields')
        if not re.fullmatch(r'[0-9]+', row['step']) or int(row['step']) < 1:
            raise ValueError('step must be a positive integer')
        step = int(row['step'])
        if row['type'] not in TYPES:
            raise ValueError('unknown transaction type')
        for key in MONEY:
            if not re.fullmatch(r'\d+(?:\.\d+)?(?:[Ee][+-]?\d+)?', row[key]):
                raise ValueError(f'{key} must be a nonnegative decimal with at most two decimal places')
            value = Decimal(row[key])
            if not value.is_finite() or value.as_tuple().exponent < -2 or value >= Decimal('10000000000000000'):
                raise ValueError(f'{key} exceeds DECIMAL(18,2)')
        for key in ('isFraud', 'isFlaggedFraud'):
            if row[key] not in ('0', '1'):
                raise ValueError(f'{key} must be 0 or 1')
        event_time = origin + timedelta(hours=step - 1)
    except (ValueError, InvalidOperation, OverflowError) as exc:
        raise ValueError(f'CSV data record {number}: {exc}') from exc
    transaction_id = digest(f'flousflow:{dataset_id}:{number}')
    event_id = digest(f'flousflow:{transaction_id}:{replay_id}')
    payload = dict(row)
    payload['step'] = step
    payload['isFraud'] = int(row['isFraud'])
    payload['isFlaggedFraud'] = int(row['isFlaggedFraud'])
    return {'source_dataset_id': dataset_id, 'source_row_number': number,
            'transaction_id': transaction_id, 'replay_id': replay_id, 'event_id': event_id,
            'event_time': utc_string(event_time), 'produced_at': utc_string(now), 'payload': payload}


def replay(path, topic, replay_id, origin, rate, producer, clock=time.monotonic, sleep=time.sleep):
    source_hash = dataset_hash(path)
    delivered = 0
    pending = 0
    errors = []
    next_send = clock()

    def acknowledged(error, message):
        nonlocal pending, delivered
        pending -= 1
        if error is not None:
            errors.append(str(error))
        else:
            delivered += 1

    def check_errors():
        if errors:
            raise RuntimeError(f'Delivery failed: {errors[0]}')

    with open(path, newline='', encoding='utf-8') as source:
        reader = csv.DictReader(source, strict=True)
        if reader.fieldnames != list(FIELDS):
            raise ValueError('CSV header does not match the expected PaySim columns')
        try:
            for number, row in enumerate(reader, 1):
                event = make_event(row, source_hash, number, replay_id, origin, datetime.now(timezone.utc))
                delay = next_send - clock()
                if delay > 0:
                    sleep(delay)
                producer.poll(0)
                check_errors()
                deadline = clock() + 30
                while pending >= 1000:
                    producer.poll(0.1)
                    check_errors()
                    if clock() >= deadline:
                        raise RuntimeError('Delivery queue timed out; outcome is unknown')
                while True:
                    try:
                        producer.produce(topic, key=event['event_id'].encode(),
                                         value=json.dumps(event, separators=(',', ':')).encode(),
                                         on_delivery=acknowledged)
                        pending += 1
                        break
                    except BufferError:
                        producer.poll(0.1)
                        check_errors()
                        if clock() >= deadline:
                            raise RuntimeError('Kafka queue remained full for 30 seconds')
                next_send = max(next_send + 1 / rate, clock())
        except csv.Error as exc:
            raise ValueError(f'CSV parse error near line {reader.line_num}: {exc}') from exc
    remaining = producer.flush(30)
    check_errors()
    if remaining or pending:
        raise RuntimeError('Final delivery timed out; outcome is unknown')
    return delivered


def main():
    try:
        path = Path(os.environ.get('FLOUSFLOW_SOURCE_PATH', 'data/sample/transactions.csv'))
        topic = os.environ.get('FLOUSFLOW_KAFKA_TOPIC', 'flousflow.transactions').strip()
        servers = os.environ.get('FLOUSFLOW_KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092').strip()
        replay_id = os.environ.get('FLOUSFLOW_REPLAY_ID', 'local-demo-001').strip()
        rate = float(os.environ.get('FLOUSFLOW_REPLAY_RATE_PER_SECOND', '10'))
        origin = datetime.fromisoformat(os.environ.get('FLOUSFLOW_EVENT_TIME_ORIGIN', '2020-01-01T00:00:00Z').replace('Z', '+00:00'))
        if not topic or not servers or not replay_id or not math.isfinite(rate) or rate <= 0:
            raise ValueError('topic, bootstrap servers and replay ID must be nonempty; rate must be finite and positive')
        if origin.tzinfo is None or origin.utcoffset() != timedelta(0):
            raise ValueError('event time origin must have a UTC offset')
        from confluent_kafka import Producer
        producer = Producer({'bootstrap.servers': servers, 'enable.idempotence': True,
                             'message.timeout.ms': 25000, 'acks': 'all', 'linger.ms': 5,
                             'queue.buffering.max.messages': 1000, 'queue.buffering.max.kbytes': 16384})
        count = replay(path, topic, replay_id, origin, rate, producer)
        print(f'Published and acknowledged {count} events to {topic}.')
    except (OSError, ValueError, RuntimeError, ImportError) as exc:
        print(f'Producer failed: {exc}', file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        print('Interrupted; restart with the same replay ID to preserve event IDs.', file=sys.stderr)
        return 130
    return 0


if __name__ == '__main__':
    sys.exit(main())
