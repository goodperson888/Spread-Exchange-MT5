"""Reconciliation exposes external positions without adopting or trading them."""
import json
import sys
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import Mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from server import TradingRuntime
from trading_engine import Engine
from trading_store import Store


class PositionReportTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        r = self.runtime = TradingRuntime.__new__(TradingRuntime)
        r.lock = threading.RLock()
        r.config = json.loads((ROOT / 'config.example.json').read_text())
        r.config['execution']['mode'] = 'live'
        r.connected = r.reconciled = True
        r.position_report = {'old': True}
        r.binance = Mock(position_mode='one_way')
        r.binance.positions.return_value = []
        r.binance.open_orders.return_value = []
        r.terminal = Mock()
        r.terminal.call.return_value = dict(account={'currency': 'USD'}, positions=[])
        r.engine = Engine(Store(Path(self.temp.name) / 'test.sqlite3'), Mock())
        self.addCleanup(r.engine.store.close)
        r.snapshot = lambda: {'position_report': r.position_report}

    def test_external_binance_position_visible_when_reconcile_fails(self):
        r = self.runtime
        r.binance.positions.return_value = [dict(symbol=r.config['symbol'],
            positionSide='BOTH', positionAmt='-2', entryPrice='4305.4', unRealizedProfit='3')]
        with self.assertRaisesRegex(ValueError, '日志不一致'):
            r.reconcile()
        self.assertFalse(r.reconciled)
        self.assertEqual(r.position_report['binance'][0]['entryPrice'], '4305.4')
        self.assertEqual(r.engine.state['groups'], [])
        self.assertEqual(r.engine.state['orders'], [])
        r.engine.broker.submit.assert_not_called()

    def test_manual_mt5_position_is_visible_but_unmanaged(self):
        r = self.runtime
        r.terminal.call.return_value['positions'] = [dict(ticket='123', lots=.02,
            magic=-1, side=0, comment='manual', price_open=4300.2, time_ms=123456)]
        r.reconcile()
        self.assertTrue(r.reconciled)
        p = r.position_report['mt5'][0]
        self.assertFalse(p['managed'])
        self.assertEqual(p['price_open'], 4300.2)
        self.assertEqual(r.engine.state['groups'], [])
        r.engine.broker.submit.assert_not_called()

    def test_read_failure_revokes_old_reconciliation_and_report(self):
        r = self.runtime
        r.binance.positions.side_effect = ValueError('timeout')
        with self.assertRaisesRegex(ValueError, 'timeout'):
            r.reconcile()
        self.assertFalse(r.reconciled)
        self.assertIsNone(r.position_report)

    def test_paper_does_not_read_live_account(self):
        r = self.runtime
        r.config['execution']['mode'] = 'paper'
        r.reconcile()
        self.assertIsNone(r.position_report)
        r.binance.positions.assert_not_called()
        r.terminal.call.assert_not_called()


if __name__ == '__main__':
    unittest.main()
