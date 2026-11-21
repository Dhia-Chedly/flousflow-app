"""Replay 10,000 source records and reconcile both Gold tables. Services must be running.

Requires a rebuilt silver image. Leaves replay data and refreshed Gold tables.
"""
import csv
import itertools
import os
from pathlib import Path
import subprocess
import tempfile
import uuid

ROOT = Path(__file__).resolve().parents[2]
replay = 'gold-check-' + uuid.uuid4().hex
source = ROOT / 'data/raw/PS_20174392719_1491204439457_log.csv'
with tempfile.TemporaryDirectory() as directory:
    sample = Path(directory) / 'transactions.csv'
    with source.open(newline='') as src, sample.open('w', newline='') as dst:
        reader = csv.reader(src)
        writer = csv.writer(dst)
        writer.writerow(next(reader))
        rows = list(itertools.islice(reader, 10000))
        if len(rows) != 10000:
            raise ValueError('Need at least 10,000 source records')
        writer.writerows(rows)
    subprocess.run(['uv', 'run', '--locked', 'flousflow-producer'], cwd=ROOT, check=True,
                   env=dict(os.environ, FLOUSFLOW_SOURCE_PATH=str(sample), FLOUSFLOW_REPLAY_ID=replay,
                            FLOUSFLOW_REPLAY_RATE_PER_SECOND='2000'))
subprocess.run(['docker', 'compose', '--profile', 'silver', 'run', '--rm', '--no-deps',
                '-e', 'FLOUSFLOW_VALIDATE_REPLAY_ID=' + replay, 'silver',
                '/opt/flousflow/spark/gold/validate.py'], cwd=ROOT, check=True)
