import csv
import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from flousflow_producer.__main__ import FIELDS, make_event, replay

ORIGIN = datetime(2020, 1, 1, tzinfo=timezone.utc)
FIXTURE = Path('data/sample/transactions.csv')


class FakeProducer:
    def __init__(self, error=None, timeout=False):
        self.messages = []
        self.error = error
        self.timeout = timeout

    def produce(self, topic, key, value, on_delivery):
        self.messages.append((topic, key, json.loads(value)))
        self.callback = on_delivery

    def flush(self, timeout):
        if self.timeout:
            return 1
        self.callback(self.error, None)
        return 0


class ProducerTests(unittest.TestCase):
    def row(self):
        with FIXTURE.open(newline='') as f:
            return next(csv.DictReader(f))

    def test_identity_replay_and_decimal_values(self):
        row = self.row()
        row['amount'] = '0.00'
        a = make_event(row, 'abc', 1, 'run1', ORIGIN, ORIGIN)
        b = make_event(row, 'abc', 1, 'run1', ORIGIN, ORIGIN)
        c = make_event(row, 'abc', 1, 'run2', ORIGIN, ORIGIN)
        self.assertEqual(a['event_id'], b['event_id'])
        self.assertEqual(a['transaction_id'], c['transaction_id'])
        self.assertNotEqual(a['event_id'], c['event_id'])
        self.assertEqual(a['payload']['amount'], '0.00')
        self.assertEqual(a['event_time'], '2020-01-01T00:00:00.000000Z')

    def test_invalid_records_include_ordinal(self):
        for field, value in [('amount', '-1'), ('amount', '1.234'), ('step', '0'), ('isFraud', '2'), ('type', 'BAD')]:
            row = self.row(); row[field] = value
            with self.subTest(field=field, value=value), self.assertRaisesRegex(ValueError, 'record 7'):
                make_event(row, 'abc', 7, 'run', ORIGIN, ORIGIN)

    def test_replay_delivers_fixture_and_keys(self):
        producer = FakeProducer()
        self.assertEqual(replay(FIXTURE, 'topic', 'run', ORIGIN, 100, producer, clock=lambda: 0, sleep=lambda _: None), 3)
        for topic, key, event in producer.messages:
            self.assertEqual(key.decode(), event['event_id'])
            self.assertEqual(topic, 'topic')

    def test_failed_or_unknown_delivery_stops(self):
        for producer in (FakeProducer(error='failure'), FakeProducer(timeout=True)):
            with self.subTest(timeout=producer.timeout), self.assertRaises(RuntimeError):
                replay(FIXTURE, 'topic', 'run', ORIGIN, 1, producer)
            self.assertEqual(len(producer.messages), 1)

    def test_bad_header_stops_before_delivery(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'bad.csv'; path.write_text('wrong,header\n1,2\n')
            producer = FakeProducer()
            with self.assertRaisesRegex(ValueError, 'header'):
                replay(path, 'topic', 'run', ORIGIN, 1, producer)
            self.assertEqual(producer.messages, [])

if __name__ == '__main__':
    unittest.main()
