import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "spark"))
from silver.money import valid_money


class MoneyTests(unittest.TestCase):
    def test_exact_cents_and_scientific_notation(self):
        for value in ('0', '0.0000', '1.916920493E7', '1.2300'):
            self.assertTrue(valid_money(value), value)
        for value in ('0.00000000001', '1.234', '-1', '1e20', 'NaN'):
            self.assertFalse(valid_money(value), value)
