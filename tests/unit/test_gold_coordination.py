import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'spark'))
from gold.locking import writer_lock
from gold.replay import replay_requirement


class GoldCoordinationTests(unittest.TestCase):
    def test_replay_requirement(self):
        with patch.dict(os.environ, {}, clear=True):
            self.assertEqual(replay_requirement(), ('', None, 600))
        for values in ({'FLOUSFLOW_REFRESH_REPLAY_ID': 'run'},
                       {'FLOUSFLOW_REFRESH_EXPECTED_COUNT': '10'},
                       {'FLOUSFLOW_REFRESH_REPLAY_ID': 'run', 'FLOUSFLOW_REFRESH_EXPECTED_COUNT': '0'},
                       {'FLOUSFLOW_REFRESH_WAIT_SECONDS': '-1'},
                       {'FLOUSFLOW_REFRESH_WAIT_SECONDS': 'nan'}):
            with patch.dict(os.environ, values, clear=True), self.assertRaises(ValueError):
                replay_requirement()
        with patch.dict(os.environ, {'FLOUSFLOW_REFRESH_REPLAY_ID': 'run',
                                      'FLOUSFLOW_REFRESH_EXPECTED_COUNT': '10'}, clear=True):
            self.assertEqual(replay_requirement(), ('run', 10, 600))

    def test_lock_excludes_other_process_and_releases_after_failure(self):
        with tempfile.TemporaryDirectory() as directory, patch.dict(os.environ, {
                'FLOUSFLOW_GOLD_LOCK': str(Path(directory) / 'writer.lock')}):
            code = 'from gold.locking import writer_lock\nwith writer_lock(): pass'
            env = dict(os.environ, PYTHONPATH=str(Path(__file__).resolve().parents[2] / 'spark'))
            with self.assertRaisesRegex(RuntimeError, 'injected'):
                with writer_lock():
                    attempt = subprocess.run([sys.executable, '-c', code], env=env, capture_output=True)
                    self.assertNotEqual(attempt.returncode, 0)
                    self.assertIn(b'Another Gold writer', attempt.stderr)
                    raise RuntimeError('injected')
            subprocess.run([sys.executable, '-c', code], env=env, check=True)
