"""Isolate server import side effects from the application's real data directory."""
import atexit
import os
import sys
import tempfile


DATA = tempfile.TemporaryDirectory(prefix='gold-pair-tests-')
os.environ['GOLD_PAIR_DATA_DIR'] = DATA.name


def cleanup():
    module = sys.modules.get('server')
    if module is not None and hasattr(module, 'TRADING'):
        module.TRADING.close()
    DATA.cleanup()


atexit.register(cleanup)
