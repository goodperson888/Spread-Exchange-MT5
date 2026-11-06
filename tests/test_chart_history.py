import copy
import json
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import Mock

import _test_runtime
from server import TradingRuntime
from trading_config import pair_key
from trading_store import Store
from test_trading_engine import config


class ChartHistoryTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.store=Store(Path(self.tmp.name)/'test.sqlite3');self.addCleanup(self.store.close)
        self.c=config();self.key=pair_key(self.c);self.at=int(time.time()*1000)-10000

    def sample(self, key, at, entry=1):
        self.store.sample(dict(key=key,time_ms=at,entry=entry,exit=entry+.1,valid=True))

    def test_terminal_path_change_recovers_history_but_other_accounts_never_mix(self):
        variant=copy.deepcopy(self.c);variant['mt5']['terminal_path']='C:/test/terminal64.exe'
        foreign=copy.deepcopy(self.c);foreign['mt5']['account']='OTHER'
        self.sample(pair_key(variant),self.at,2)
        self.sample(self.key,self.at+1,3)
        self.sample(pair_key(foreign),self.at+2,9)
        rows=self.store.read_samples(self.key,self.at-1)
        self.assertEqual([x['entry'] for x in rows],[2,3])

    def test_foreign_account_in_same_bucket_cannot_displace_current_market(self):
        foreign=copy.deepcopy(self.c);foreign['mt5']['server']='OTHER'
        self.sample(self.key,self.at,2)
        self.sample(pair_key(foreign),self.at+1,9)
        rows=self.store.read_samples(self.key,self.at-86400000,limit=100)
        self.assertEqual([x['entry'] for x in rows],[2])

    def test_historical_read_has_bounded_end_and_does_not_include_recent_points(self):
        old=self.at-2*86400000
        self.sample(self.key,old,2);self.sample(self.key,self.at,3)
        rows=self.store.read_samples(self.key,old-1000,until=old+1000)
        self.assertEqual([x['entry'] for x in rows],[2])

    def test_chart_read_uses_committed_wal_snapshot_without_writer_lock(self):
        self.sample(self.key,self.at,2)
        with self.store.lock:
            self.store.db.execute('BEGIN IMMEDIATE')
            self.store.db.execute('DELETE FROM quotes')
            rows=[];errors=[]
            def read():
                try: rows.extend(self.store.read_samples(self.key,self.at-1))
                except Exception as exc: errors.append(exc)
            thread=threading.Thread(target=read);thread.start();thread.join(2)
            self.store.db.rollback()
            self.assertFalse(thread.is_alive());self.assertFalse(errors)
            self.assertEqual([x['entry'] for x in rows],[2])

    def test_runtime_does_not_hold_execution_lock_during_chart_database_read(self):
        r=TradingRuntime.__new__(TradingRuntime);r.lock=threading.RLock();r.config=self.c
        entered=threading.Event();release=threading.Event();r.store=Mock()
        def read(*args):entered.set();release.wait(2);return []
        r.store.read_samples.side_effect=read
        worker=threading.Thread(target=lambda:r.chart(self.at));worker.start()
        try:
            self.assertTrue(entered.wait(1))
            acquired=r.lock.acquire(blocking=False)
            if acquired:r.lock.release()
            self.assertTrue(acquired)
        finally:release.set();worker.join(1)
        r.store.samples.assert_not_called()


if __name__=='__main__':unittest.main()
