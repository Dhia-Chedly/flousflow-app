"""Live local acceptance checks. Requires Docker, Kafka, MinIO and uv sync.

Run: uv run --locked python tests/integration/bronze_pipeline.py
Uses isolated replay IDs in the existing topic/table. Do not run other publishers
or Spark writers during this test. Leaves test data and the streaming writer intact.
"""
import csv
import json
import os
from pathlib import Path
import subprocess
import tempfile
import time
import uuid
import atexit

ROOT = Path(__file__).resolve().parents[2]


def command(*args, env=None, timeout=120, stream=False):
    result = subprocess.run(args, cwd=ROOT, env=env, text=True, capture_output=not stream, timeout=timeout)
    if result.returncode:
        raise RuntimeError(f'{args}:\n{result.stdout}\n{result.stderr}')
    return result.stdout


def compose(*args, **kwargs):
    return command('docker', 'compose', *args, **kwargs)


def await_ready():
    deadline = time.monotonic() + 600
    while time.monotonic() < deadline:
        output = compose('ps', '-a', '--format', 'json', 'spark-bronze',
                         timeout=max(0.1, min(15, deadline - time.monotonic())))
        states = [json.loads(line) for line in output.splitlines() if line.strip()]
        states = [state for state in states if state.get('Name') != 'flousflow-test-inspector']
        if len(states) != 1:
            raise RuntimeError(f'Expected one Bronze writer, found {len(states)}')
        state = states[0]
        if state.get('Health') == 'healthy':
            return
        if state.get('State') == 'exited':
            raise AssertionError(compose('logs', '--tail', '80', 'spark-bronze'))
        time.sleep(5)
    raise AssertionError('Spark did not become ready within ten minutes:\n' + compose('logs', '--tail', '40', 'spark-bronze'))


def start_inspector():
    compose('run', '-d', '--no-deps', '--name', 'flousflow-test-inspector',
            'spark-bronze', '/opt/flousflow/spark/bronze/inspect_table.py', '--serve')
    atexit.register(stop_inspector)
    deadline = time.monotonic() + 120
    probe = "import socket; socket.create_connection(('127.0.0.1', 8765), timeout=2).close()"
    while time.monotonic() < deadline:
        try:
            command('docker', 'exec', 'flousflow-test-inspector', 'python3', '-c', probe, timeout=5)
            return
        except RuntimeError:
            time.sleep(2)
    raise TimeoutError('Inspection service failed to start')


def stop_inspector():
    command('docker', 'rm', '-f', 'flousflow-test-inspector', timeout=15)


def snapshot(replay_id, deadline=None, raw_value=''):
    deadline = deadline or time.monotonic() + 120
    remaining = max(0.1, min(120, deadline - time.monotonic()))
    client = ("import urllib.request,sys; "
              "r=urllib.request.Request('http://127.0.0.1:8765', data=sys.argv[1].encode()); "
              "print(urllib.request.urlopen(r, timeout=float(sys.argv[2])).read().decode())")
    output = command('docker', 'exec', 'flousflow-test-inspector', 'python3', '-c', client,
                     json.dumps({'replay_id': replay_id, 'raw_value': raw_value}),
                     str(remaining), timeout=remaining)
    result = json.loads(output)
    if 'error' in result:
        raise RuntimeError(result['error'])
    return result


def await_count(replay_id, count):
    deadline = time.monotonic() + 600
    last = None
    while time.monotonic() < deadline:
        try:
            last = snapshot(replay_id, deadline)
            if last['replay_count'] == count:
                return last
            if last['replay_count'] > count:
                raise AssertionError(f'Too many events: {last}')
        except RuntimeError as exc:
            last = str(exc)
        time.sleep(5)
    raise AssertionError(f'Bronze did not reach {count} for {replay_id}: {last}')


def publish(path, replay_id):
    print(f'Publishing {path.name} (replay {replay_id})...', flush=True)
    env = dict(os.environ, FLOUSFLOW_SOURCE_PATH=str(path), FLOUSFLOW_REPLAY_ID=replay_id,
               FLOUSFLOW_REPLAY_RATE_PER_SECOND='2000')
    started = time.monotonic()
    print(command('uv', 'run', '--locked', 'flousflow-producer', env=env), flush=True)
    print(f'Publication took {time.monotonic() - started:.2f}s', flush=True)


def main():
    source = ROOT / 'data/raw/PS_20174392719_1491204439457_log.csv'
    if not source.exists():
        raise FileNotFoundError('Provide the local PaySim CSV for the 10,000-record test')
    run = 'bronze-test-' + uuid.uuid4().hex
    print('Starting services and building images (first run may take several minutes)...', flush=True)
    compose('build', 'spark-bronze', stream=True, timeout=600)
    compose('up', '-d', 'spark-bronze', stream=True, timeout=600)
    print('Waiting for Spark to resolve dependencies and catch up...', flush=True)
    await_ready()
    start_inspector()
    # Seed a table even on a fresh empty topic; then allow all earlier offsets to drain.
    publish(ROOT / 'data/sample/transactions.csv', run + '-seed')
    await_count(run + '-seed', 3)
    from confluent_kafka import Consumer, TopicPartition
    topic = 'flousflow.transactions'
    consumer = Consumer({'bootstrap.servers': '127.0.0.1:9092', 'group.id': run,
                         'enable.auto.commit': False, 'broker.address.family': 'v4'})
    try:
        partitions = consumer.list_topics(topic, timeout=20).topics[topic].partitions
        ends = {p: consumer.get_watermark_offsets(TopicPartition(topic, p), timeout=20)[1]
                for p in partitions}
    finally:
        consumer.close()
    deadline = time.monotonic() + 600
    while True:
        baseline = snapshot(run, deadline)
        seen = {r['kafka_partition']: r['max_offset'] + 1 for r in baseline['offsets']
                if r['kafka_topic'] == topic}
        if all(seen.get(p, 0) >= end for p, end in ends.items()):
            break
        if time.monotonic() > deadline:
            raise AssertionError('Existing Kafka records did not drain')
        time.sleep(5)

    with tempfile.TemporaryDirectory() as directory:
        sample = Path(directory) / 'transactions.csv'
        with source.open(newline='') as stream, sample.open('w', newline='') as out:
            reader = csv.reader(stream); writer = csv.writer(out)
            header = next(reader); writer.writerow(header)
            rows = [next(reader) for _ in range(10000)]
            writer.writerows(rows)
        publish(sample, run)
        first = await_count(run, 10000)
        assert first['count'] == baseline['count'] + 10000, (baseline, first)
        assert first['unique_offsets'] == baseline['unique_offsets'] + 10000
        print('PASS A/B: 10,000 events committed; Delta log, query and offset counts verified.', flush=True)
        compose('stop', 'spark-bronze')
        compose('up', '-d', 'spark-bronze')
        await_ready()
        restarted = await_count(run, 10000)
        assert restarted['count'] == first['count'], (first, restarted)
        print('PASS C: checkpoint restart did not append committed offsets again.', flush=True)
        with sample.open('w', newline='') as out:
            writer = csv.writer(out); writer.writerow(header); writer.writerows(rows[:100])
        publish(sample, run + '-additional')
        added = await_count(run + '-additional', 100)
        assert added['count'] == first['count'] + 100
        print('PASS D: 100 new events committed after restart.', flush=True)
        compose('build', 'spark-bronze', stream=True, timeout=600)
        compose('up', '-d', '--force-recreate', '--no-deps', 'spark-bronze', stream=True)
        await_ready()
        rebuilt = await_count(run + '-additional', 100)
        assert rebuilt['count'] == added['count']
        assert rebuilt['unique_offsets'] == added['unique_offsets']
        print('PASS E: container rebuild/recreation preserved table and checkpoint.', flush=True)
        # Deliberately bypass the validating CSV producer to exercise raw Bronze behavior.
        from confluent_kafka import Producer
        raw_producer = Producer({'bootstrap.servers': '127.0.0.1:9092', 'broker.address.family': 'v4'})
        invalid = '{broken-json:' + run
        duplicate = json.dumps({'event_id': run + '-duplicate', 'replay_id': run + '-raw'})
        failures = []
        for value in (invalid, duplicate, duplicate):
            raw_producer.produce(topic, key=run.encode(), value=value.encode(),
                                 on_delivery=lambda err, _: failures.append(err) if err else None)
        assert raw_producer.flush(30) == 0 and not failures
        raw_result = await_count(run + '-raw', 2)
        raw_result = snapshot(run + '-raw', raw_value=invalid)
        assert raw_result['raw_count'] == 1, raw_result
        assert raw_result['count'] == rebuilt['count'] + 3, raw_result
        assert raw_result['unique_offsets'] == rebuilt['unique_offsets'] + 3, raw_result
        print('PASS F: malformed JSON preserved byte-for-byte; repeated event IDs retained.', flush=True)



if __name__ == '__main__':
    main()
