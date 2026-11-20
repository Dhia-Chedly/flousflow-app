import importlib.util
import os
from pathlib import Path
import unittest
from unittest.mock import patch

PATH = Path(__file__).resolve().parents[2] / 'spark/common/settings.py'
spec = importlib.util.spec_from_file_location('bronze_settings', PATH)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


class BronzeConfigTests(unittest.TestCase):
    def test_credentials_required(self):
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaisesRegex(ValueError, 'MINIO_ROOT_USER'):
                module.settings()

    def test_defaults_and_overrides(self):
        with patch.dict(os.environ, {'MINIO_ROOT_USER': 'local-user', 'MINIO_ROOT_PASSWORD': 'test-secret',
                                    'FLOUSFLOW_BRONZE_PATH': 's3a://test/bronze'}, clear=True):
            config = module.settings()
        self.assertEqual(config['brokers'], 'kafka:19092')
        self.assertEqual(config['starting_offsets'], 'earliest')
        self.assertEqual(config['path'], 's3a://test/bronze')
        self.assertTrue(config['checkpoint'].startswith('/checkpoints/'))
