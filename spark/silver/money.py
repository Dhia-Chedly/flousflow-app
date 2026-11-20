from decimal import Decimal, InvalidOperation
import re


def valid_money(value):
    if value is None or not re.fullmatch(r'\d+(?:\.\d+)?(?:[Ee][+-]?\d+)?', value):
        return False
    try:
        number = Decimal(value)
        if not number.is_finite() or number < 0 or number >= Decimal('10000000000000000'):
            return False
        if number == 0:
            return True
        digits = number.as_tuple().digits
        trailing = 0
        for digit in reversed(digits):
            if digit != 0:
                break
            trailing += 1
        return number.as_tuple().exponent + trailing >= -2
    except (InvalidOperation, ValueError, OverflowError):
        return False

