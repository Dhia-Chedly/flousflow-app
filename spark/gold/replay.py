"""Optional valid-transaction requirement for a replay refresh."""
import os
import math


def replay_requirement():
    replay = os.getenv('FLOUSFLOW_REFRESH_REPLAY_ID', '').strip()
    count = os.getenv('FLOUSFLOW_REFRESH_EXPECTED_COUNT', '').strip()
    if bool(replay) != bool(count):
        raise ValueError('Set both FLOUSFLOW_REFRESH_REPLAY_ID and FLOUSFLOW_REFRESH_EXPECTED_COUNT')
    expected = int(count) if count else None
    timeout = float(os.getenv('FLOUSFLOW_REFRESH_WAIT_SECONDS', '600'))
    if (expected is not None and expected <= 0) or not math.isfinite(timeout) or timeout < 0:
        raise ValueError('Expected count must be positive and wait seconds nonnegative')
    return replay, expected, timeout
