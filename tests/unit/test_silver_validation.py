import json
import sys
import unittest
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'spark'))
from silver.validation import json_type_error


class JsonTypeTests(unittest.TestCase):
    def test_types_and_corrupt_input(self):
        valid = {'source_row_number': 1, 'payload': {'step': 1, 'amount': '1.00'}}
        self.assertIsNone(json_type_error(json.dumps(valid).encode()))
        valid['source_row_number'] = '1'
        self.assertEqual(json_type_error(json.dumps(valid).encode()), 'INVALID_FIELD_TYPE')
        self.assertEqual(json_type_error(b'{bad'), 'MALFORMED_JSON')
        self.assertEqual(json_type_error(b'{"a":1,"a":2}'), 'MALFORMED_JSON')
        self.assertEqual(json_type_error(b'\xff'), 'MALFORMED_JSON')
        self.assertEqual(json_type_error(b'null'), 'INVALID_FIELD_TYPE')
