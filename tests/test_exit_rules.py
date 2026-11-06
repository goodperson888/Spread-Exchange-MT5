import copy
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import _test_runtime
from server import TradingRuntime
from trading_brokers import PaperBroker
from trading_engine import Engine
from trading_store import Store
from exit_rules import FIELDS
from test_trading_engine import config, quote


class ExitRulesTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.store=Store(Path(self.tmp.name)/'test.sqlite3');self.addCleanup(self.store.close)
        self.c=config();self.original=copy.deepcopy(self.c)
        self.broker=Mock(wraps=PaperBroker());self.e=Engine(self.store,self.broker)
        self.e.start(self.c);self.plan=dict(qty=2,lots=.02,contract=100)
        self.e.tick(self.c,self.plan,quote(self.c));self.g=self.e.active()[0];self.broker.reset_mock()
        self.r=TradingRuntime.__new__(TradingRuntime);self.r.lock=threading.RLock()
        self.r.connected=True;self.r.reconciled=True;self.r.config=self.c;self.r.engine=self.e
        self.r.snapshot=Mock(return_value={});self.r.binance=Mock();self.r.terminal=Mock()

    def body(self, groups=None, **changes):
        groups=groups or [self.g]
        rules={key:self.g['parameters'][key] for key in FIELDS};rules.update(changes)
        return dict(groups=[g['id'] for g in groups],rules=rules,
                    expected_revisions={g['id']:g.get('exit_rules_revision',0) for g in groups})

    def test_apply_is_durable_audited_and_changes_no_entry_or_execution_fields(self):
        saved=copy.deepcopy(self.g['parameters']);opened=self.g['opened_ms']
        result=self.r.apply_exit_rules(self.body(take_contraction_usd='1.5'))
        self.assertEqual(self.g['parameters']['take_contraction_usd'],1.5)
        self.assertEqual(self.c,self.original);self.assertTrue(self.e.state['enabled'])
        for key in set(saved)-set(FIELDS):self.assertEqual(self.g['parameters'][key],saved[key])
        self.assertEqual(self.g['opened_ms'],opened)
        self.assertEqual(self.store.load()['groups'][0]['exit_rules_revision'],1)
        event=self.store.events(1)[0];self.assertEqual(event['kind'],'exit_rules_updated')
        change=event['data']['changes'][0]
        self.assertEqual(change['before']['take_contraction_usd'],2)
        self.assertEqual(change['after']['take_contraction_usd'],1.5)
        self.assertIn('下一次有效行情',result['message'])
        self.broker.submit.assert_not_called();self.r.binance.assert_not_called();self.r.terminal.assert_not_called()

    def test_new_conditions_take_effect_on_next_valid_tick_while_entry_is_paused(self):
        self.e.pause();self.r.apply_exit_rules(self.body(take_contraction_usd=1))
        self.assertEqual(self.g['status'],'open');self.broker.submit.assert_not_called()
        q=quote(self.c,bid=4304,ask=4304.1)
        invalid={**q,'valid':False};invalid['mt5']={**q['mt5'],'time_ms':0}
        self.e.tick(self.c,self.plan,invalid);self.assertEqual(self.g['status'],'open')
        self.e.tick(self.c,self.plan,q);self.assertEqual(self.g['status'],'closed')
        self.assertFalse(self.e.state['enabled'])
        self.e.start(self.c);self.e.state['last_open_ms']=0
        self.e.tick(self.c,self.plan,quote(self.c))
        self.assertEqual(self.e.active()[0]['parameters']['take_contraction_usd'],2)

    def test_pause_blocks_grid_adds_but_retains_ordinary_exits(self):
        self.c['strategy'].update(grid_enabled=True,grid_max_adds=2,max_groups=3,grid_spacing_usd=1)
        self.g['parameters'].update(grid_enabled=True)
        self.e.state['last_open_ms']=0;self.e.pause()
        self.e.tick(self.c,self.plan,quote(self.c,bid=4310,ask=4310.1))
        self.assertEqual(len(self.e.state['groups']),1);self.broker.submit.assert_not_called()
        self.e.tick(self.c,self.plan,quote(self.c,bid=4303,ask=4303.1))
        self.assertEqual(self.g['status'],'closed');self.assertEqual(len(self.e.state['groups']),1)

    def test_stale_revision_or_invalid_values_leave_all_parameters_unchanged(self):
        stale=self.body();self.r.apply_exit_rules(self.body(take_contraction_usd=1))
        before=copy.deepcopy(self.e.state)
        cases=[stale,self.body(target_mode='invalid'),self.body(require_net_profit='false'),
               self.body(max_hold_minutes=''),self.body(group_max_loss_usd=0),
               self.body(take_contraction_usd=float('nan')),self.body(min_net_profit_usd=float('inf')),
               self.body(max_hold_minutes=True),self.body(max_slippage_usd=999)]
        for body in cases:
            with self.subTest(body=body):
                with self.assertRaises(ValueError):self.r.apply_exit_rules(body)
                self.assertEqual(self.e.state,before)

    def test_nonopen_imported_wrong_market_and_unknown_fills_are_rejected(self):
        for status in ('opening','closing','unwinding','attention','closed'):
            self.g['status']=status
            with self.assertRaises(ValueError):self.r.apply_exit_rules(self.body())
        self.g['status']='open';self.g['imported']=True
        with self.assertRaises(ValueError):self.r.apply_exit_rules(self.body())
        self.g.pop('imported');old=self.g['key'];self.g['key']='another-account'
        with self.assertRaises(ValueError):self.r.apply_exit_rules(self.body())
        self.g['key']=old;self.e.state['orders'][0]['result']['status']='unknown'
        with self.assertRaises(ValueError):self.r.apply_exit_rules(self.body())

    def test_connection_reconciliation_and_restart_recovery_are_required(self):
        for attr in ('connected','reconciled'):
            setattr(self.r,attr,False)
            with self.assertRaises(ValueError):self.r.apply_exit_rules(self.body())
            setattr(self.r,attr,True)
        self.e.state['recovery']=True
        with self.assertRaises(ValueError):self.r.apply_exit_rules(self.body())

    def test_basket_requires_exact_current_members_and_updates_atomically(self):
        self.g['parameters']['exit_mode']='basket'
        other=copy.deepcopy(self.g);other['id']='second';self.e.state['groups'].append(other)
        with self.assertRaises(ValueError):self.r.apply_exit_rules(self.body())
        pending=self.body([self.g,other],take_contraction_usd=1)
        other['status']='closing'
        with self.assertRaises(ValueError):self.r.apply_exit_rules(pending)
        other['status']='open';self.r.apply_exit_rules(pending)
        self.assertEqual([g['parameters']['take_contraction_usd'] for g in (self.g,other)],[1,1])
        self.assertEqual([g['exit_rules_revision'] for g in (self.g,other)],[1,1])
        self.assertEqual(len(self.store.events(1)[0]['data']['changes']),2)

    def test_commit_failure_rolls_back_in_memory_rules_and_versions(self):
        before=copy.deepcopy(self.e.state)
        with patch.object(self.store,'commit',side_effect=OSError('disk full')):
            with self.assertRaises(OSError):self.r.apply_exit_rules(self.body(take_contraction_usd=1))
        self.assertEqual(self.e.state,before)
        self.assertEqual(self.store.load()['groups'][0]['parameters'],before['groups'][0]['parameters'])


if __name__=='__main__':unittest.main()
