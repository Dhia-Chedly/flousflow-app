"""Run serving checks and verify PostgreSQL data survives a container restart."""
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def compose(*args):
    return subprocess.check_output(['docker', 'compose', '--profile', 'serving', *args],
                                   cwd=ROOT, text=True)


def snapshot():
    return compose('exec', '-T', 'postgres', 'psql', '-U', 'flousflow_admin', '-d', 'flousflow', '-Atc',
        "SELECT row_to_json(s) FROM serving.refresh_status s; "
        "SELECT count(*) FROM serving.hourly_transaction_metrics; "
        "SELECT count(*) FROM serving.account_activity;")


print(compose('run', '--rm', 'serving', '/opt/flousflow/spark/serving/selftest.py'), flush=True)
before = snapshot()
compose('restart', 'postgres')
compose('up', '-d', '--wait', 'postgres')
assert snapshot() == before, 'Serving data changed after PostgreSQL restart'
print('SERVING_PIPELINE_PASS: refresh checks and restart persistence', flush=True)
