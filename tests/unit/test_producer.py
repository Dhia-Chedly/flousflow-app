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
        self.callbacks = []
        self.peak_pending = 0

    def produce(self, topic, key, value, on_delivery):
        self.messages.append((topic, key, json.loads(value)))
        self.callbacks.append(on_delivery)
        self.peak_pending = max(self.peak_pending, len(self.callbacks))

    def poll(self, timeout):
        if (self.error or timeout > 0) and self.callbacks:
            self.callbacks.pop(0)(self.error, None)

    def flush(self, timeout):
        if self.timeout:
            return 1
        for callback in self.callbacks:
            callback(self.error, None)
        self.callbacks.clear()
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

    def test_scientific_notation_from_paysim(self):
        row = self.row()
        row['newbalanceDest'] = '1.916920493E7'
        event = make_event(row, 'abc', 85, 'run', ORIGIN, ORIGIN)
        self.assertEqual(event['payload']['newbalanceDest'], '1.916920493E7')
        row['newbalanceDest'] = '1.234E-2'
        with self.assertRaisesRegex(ValueError, 'record 85'):
            make_event(row, 'abc', 85, 'run', ORIGIN, ORIGIN)

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
            self.assertEqual(len(producer.messages), 3 if producer.timeout else 1)

    def test_bounded_async_queue(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'many.csv'
            with path.open('w', newline='') as stream:
                writer = csv.DictWriter(stream, fieldnames=FIELDS)
                writer.writeheader()
                writer.writerows([self.row()] * 1200)
            producer = FakeProducer()
            count = replay(path, 'topic', 'run', ORIGIN, 10000, producer,
                           clock=lambda: 0, sleep=lambda _: None)
            self.assertEqual(count, 1200)
            self.assertEqual(producer.peak_pending, 1000)
            self.assertEqual(producer.callbacks, [])

    def test_queue_full_retries_without_losing_event(self):
        class FullOnce(FakeProducer):
            blocked = False

            def produce(self, *args, **kwargs):
                if not self.blocked:
                    self.blocked = True
                    raise BufferError('full')
                super().produce(*args, **kwargs)

        producer = FullOnce()
        self.assertEqual(replay(FIXTURE, 'topic', 'run', ORIGIN, 100, producer,
                                clock=lambda: 0, sleep=lambda _: None), 3)
        self.assertEqual(len(producer.messages), 3)

    def test_bad_header_stops_before_delivery(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'bad.csv'; path.write_text('wrong,header\n1,2\n')
            producer = FakeProducer()
            with self.assertRaisesRegex(ValueError, 'header'):
                replay(path, 'topic', 'run', ORIGIN, 1, producer)
            self.assertEqual(producer.messages, [])

if __name__ == '__main__':
    unittest.main()
