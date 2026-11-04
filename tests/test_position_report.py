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
import _test_runtime
from server import TradingRuntime
from trading_engine import Engine
from trading_store import Store
from trading_brokers import PaperBroker
from test_trading_engine import quote


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

    def two_groups(self):
        r=self.runtime;r.config['mt5']['adapter']='native';r.engine.broker=PaperBroker()
        plan=dict(qty=2,lots=.02,contract=100)
        r.engine.open(r.config,plan,quote(r.config));r.engine.open(r.config,plan,quote(r.config))
        opened=[o for o in r.engine.state['orders'] if o['leg']=='mt5' and o['action']=='open']
        positions=[dict(ticket=o['result']['position'],lots=.02,side=0,
                        magic=r.config['execution']['magic'],comment=o['id']) for o in opened]
        r.terminal.call.return_value['positions']=positions
        r.binance.positions.return_value=[dict(positionAmt='-4',positionSide='BOTH')]
        r.engine.state['enabled']=True
        return r,positions,opened

    def test_missing_group_ticket_cannot_be_hidden_by_equal_aggregate_lots(self):
        r,positions,opened=self.two_groups();positions[0]['lots']=.04;positions.pop()
        with self.assertRaisesRegex(ValueError,'MT5'):
            r.reconcile()
        self.assertFalse(r.reconciled);self.assertFalse(r.engine.state['enabled'])

    def test_each_ticket_quantity_must_match_even_when_all_tickets_exist(self):
        r,positions,opened=self.two_groups();positions[0]['lots']=.01;positions[1]['lots']=.03
        with self.assertRaisesRegex(ValueError,'MT5'):
            r.reconcile()
        self.assertFalse(r.reconciled)

    def test_ticket_and_opening_comment_must_belong_to_the_same_group(self):
        r,positions,opened=self.two_groups()
        positions[0]['comment'],positions[1]['comment']=positions[1]['comment'],positions[0]['comment']
        with self.assertRaisesRegex(ValueError,'MT5'):
            r.reconcile()

    def test_valid_partial_close_and_missing_result_ticket_can_be_reconciled(self):
        r,positions,opened=self.two_groups();group=r.engine.active()[0]
        r.engine.order(group,'mt5','close',1,quote(r.config))
        r.engine.order(group,'binance','close',1,quote(r.config))
        positions[0]['lots']=.01;r.binance.positions.return_value[0]['positionAmt']='-3'
        opened[1]['result'].pop('position')  # older journal resolves this by the exact comment
        r.reconcile()
        self.assertTrue(r.reconciled)
        self.assertEqual(opened[1]['result']['position'],positions[1]['ticket'])

    def test_replaced_ticket_does_not_override_a_known_journal_position(self):
        r,positions,opened=self.two_groups();positions[0]['ticket']='different-position'
        with self.assertRaisesRegex(ValueError,'MT5'):
            r.reconcile()


if __name__ == '__main__':
    unittest.main()
