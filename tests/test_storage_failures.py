"""Disk failures must not arm entries or strand a half-installed connection."""
import sqlite3
import threading
import unittest
from unittest.mock import Mock, patch

import _test_runtime
import test_entry_margin
from trading_store import storage_error_message, storage_snapshot


class StorageConnectionTests(unittest.TestCase):
    setUp=test_entry_margin.EntryMarginTests.setUp
    connect=test_entry_margin.EntryMarginTests.connect

    def full(self): return sqlite3.OperationalError('database or disk is full')

    def test_pause_write_failure_never_installs_connection_and_retry_recovers(self):
        with patch.object(self.store,'commit',side_effect=self.full()):
            with self.assertRaises(sqlite3.OperationalError): self.connect()
        self.assertFalse(self.r.connected)
        self.assertFalse(self.r.reconciled)
        self.assertIsNone(self.r.pump)
        self.assertFalse(self.r.engine.state['enabled'])
        self.terminal.close.assert_called_once()
        self.stream.close.assert_called_once()
        self.assertIn('磁盘',self.r.last_error)
        self.connect()
        self.assertTrue(self.r.connected)
        self.assertIsNotNone(self.r.pump)
        self.assertFalse(self.r.engine.state['enabled'])

    def test_sample_failure_preserves_previous_connection(self):
        old_terminal=Mock();old_pump=Mock()
        self.r.terminal=old_terminal;self.r.pump=old_pump
        self.r.connected=self.r.reconciled=True;self.r.config=self.c
        with patch.object(self.store,'sample',side_effect=self.full()):
            with self.assertRaises(sqlite3.OperationalError): self.connect()
        self.assertTrue(self.r.connected)
        self.assertIs(self.r.pump,old_pump)
        self.assertIs(self.r.terminal,old_terminal)
        old_pump.close.assert_not_called();old_terminal.close.assert_not_called()
        self.terminal.close.assert_called_once()
        self.assertFalse(self.r.engine.state['enabled'])

    def test_monitor_start_failure_disconnects_and_closes_new_resources(self):
        import server
        server.MaintenanceMonitor.return_value.start.side_effect=RuntimeError('worker start failed')
        with self.assertRaisesRegex(RuntimeError,'worker start failed'): self.connect()
        self.assertFalse(self.r.connected);self.assertFalse(self.r.reconciled)
        self.assertIsNone(self.r.pump);self.assertIsNone(self.r.terminal)
        self.terminal.close.assert_called_once();self.stream.close.assert_called_once()
        server.QuotePump.return_value.close.assert_called_once()
        self.assertFalse(self.r.engine.state['enabled'])

    def test_failed_start_journal_never_arms_engine(self):
        with patch.object(self.store,'commit',side_effect=self.full()):
            with self.assertRaises(sqlite3.OperationalError): self.r.engine.start(self.c)
        self.assertFalse(self.r.engine.state['enabled'])
        self.assertNotIn('last_start_ms',self.r.engine.state)
        self.assertIn('启动记录写入失败',self.r.engine.state['alarm'])
        self.assertEqual(self.r.engine.state['orders'],[])

    def test_worker_survives_failure_to_persist_pause(self):
        self.r.connected=True;self.r.config=self.c;self.r.stop_event=threading.Event()
        self.r.engine.state['enabled']=True
        def next_poll():
            self.assertFalse(self.r.engine.state['enabled'])
            self.r.stop_event.set()
        calls=[]
        def poll():
            calls.append(True)
            if len(calls)==1: raise self.full()
            next_poll()
        self.r._poll=poll
        with patch.object(self.store,'commit',side_effect=self.full()): self.r._loop()
        self.assertEqual(len(calls),2)
        self.assertFalse(self.r.engine.state['enabled'])

    def test_missing_intake_reports_incomplete_connection(self):
        self.r.config=self.c;self.r.connected=True
        with self.assertRaisesRegex(ValueError,'行情连接未完成'): self.r._poll()
        self.assertFalse(self.r.connected);self.assertFalse(self.r.reconciled)

    def test_diagnostics_read_space_only_on_demand_without_private_path(self):
        snapshot=storage_snapshot(self.store.path)
        self.assertGreater(snapshot['disk_total_bytes'],0)
        self.assertGreater(snapshot['database_bytes'],0)
        self.assertNotIn(str(self.store.path),str(snapshot))
        self.assertIn('迁移整个数据目录',storage_error_message(self.full()))
