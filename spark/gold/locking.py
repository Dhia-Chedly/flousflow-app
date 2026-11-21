"""Cross-process Gold writer lock shared by the local Compose containers."""
import fcntl
import os
from contextlib import contextmanager
from pathlib import Path


@contextmanager
def writer_lock():
    path = Path(os.getenv('FLOUSFLOW_GOLD_LOCK', '/locks/gold-refresh.lock'))
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('a') as handle:
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise RuntimeError('Another Gold writer is running') from None
        try:
            yield
        finally:
            fcntl.flock(handle, fcntl.LOCK_UN)
