"""Replay a late MT5 close followed by expired Binance IOC requests."""
import copy
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from execution_diagnostics import build_report
from trading_brokers import PaperBroker
from trading_engine import Engine
from trading_store import Store
from test_trading_engine import config, quote


class ResidualBroker(PaperBroker):
    def __init__(self, store):
        self.store=store;self.sent=[];self.queries=0;self.expired=0
        self.mt5_unknown=False;self.query_unknown=False;self.partial=False;self.native_ack=False

    def submit(self, o):
        self.sent.append(copy.deepcopy(o))
        saved=self.store.load()
        assert any(row['id']==o['id'] for row in saved['orders'])
        if o['action']=='close':
            group=next(g for g in saved['groups'] if g['id']==o['group'])
            assert group['close_attempts'][o['leg']]==o['close_attempt']
            if o['leg']=='mt5' and self.mt5_unknown:
                self.pending=copy.deepcopy(o)
                return dict(status='unknown',qty=0,price=0,ticket='500',retcode=10009,
                    **(dict(ack_volume=o['requested']/100,ack_price=0,
                            confirmation={'stage':'order_pending'}) if self.native_ack else {}))
            if o['leg']=='binance' and self.expired:
                self.expired-=1
                return dict(status='done',qty=0,price=0,raw_status='EXPIRED')
            result=super().submit(o)
            if self.partial:
                fills=[x for x in self.sent if x['action']=='close' and x['leg']==o['leg']]
                if len(fills)==1: result['qty']=.5 if o['leg']=='mt5' else .25
            return result
        return super().submit(o)

    def query(self, o):
        self.queries+=1
        if self.query_unknown: return dict(status='unknown',qty=0,price=0,ticket='500')
        return dict(status='done',qty=self.pending['requested'],price=self.pending['reference']-1.13,
                    ticket='500',position=self.pending['position'])


class ResidualCloseTests(unittest.TestCase):
    def setUp(self):
        temp=tempfile.TemporaryDirectory();self.addCleanup(temp.cleanup)
        self.store=Store(Path(temp.name)/'journal.db');self.addCleanup(self.store.close)
        self.c=config();self.c['execution']['close_retry_limit']=3
        self.p=dict(qty=1,lots=.01,contract=100)
        self.b=ResidualBroker(self.store);self.e=Engine(self.store,self.b);self.e.start(self.c)
        self.e.tick(self.c,self.p,quote(self.c));self.g=self.e.active()[0]
        self.signal=quote(self.c,bid=4302,ask=4302.1)

    def closes(self, leg=None):
        return [o for o in self.b.sent if o['action']=='close' and (leg is None or o['leg']==leg)]

    def test_late_mt5_confirmation_leaves_three_binance_attempts(self):
        self.b.mt5_unknown=True;self.b.expired=2
        self.e.tick(self.c,self.p,self.signal)
        self.assertEqual(self.g['close_attempts'],dict(mt5=1,binance=1))
        self.assertEqual(len(self.closes('binance')),1)
        pending=self.e.uncertain(self.g)[0];self.b.query_unknown=True
        for _ in range(3):
            pending['reconcile']['next_ms']=0;self.e.tick(self.c,self.p,self.signal)
        self.assertEqual(self.g['close_attempts'],dict(mt5=1,binance=1))
        self.assertEqual(len(self.closes()),2)
        self.b.query_unknown=False;pending['reconcile']['next_ms']=0
        for _ in range(3): self.e.tick(self.c,self.p,self.signal)
        self.assertEqual(self.g['status'],'closed')
        self.assertEqual(self.g['close_attempts'],dict(mt5=1,binance=3))
        self.assertEqual(len(self.closes('mt5')),1);self.assertEqual(len(self.closes('binance')),3)
        self.assertFalse(self.e.state['enabled'])
        self.assertEqual(self.e.amounts(self.g),dict(mt5=0,binance=0))
        self.assertEqual(self.closes('binance')[0]['slippage_mode'],'normal')
        self.assertTrue(all(o['slippage_mode']=='protective' for o in self.closes('binance')[1:]))
        self.assertTrue(all(o['slippage']==self.g['parameters']['unwind_slippage_usd'] for o in self.closes('binance')[1:]))

    def test_exact_limit_halts_with_platform_and_exposure_and_never_rearms(self):
        self.b.expired=10
        for _ in range(5): self.e.tick(self.c,self.p,self.signal)
        self.assertEqual(len(self.closes('binance')),3);self.assertEqual(len(self.closes('mt5')),1)
        self.assertEqual(self.g['status'],'attention');self.assertFalse(self.e.state['enabled'])
        self.assertEqual(self.g['close_halt']['leg'],'binance')
        self.assertEqual(self.g['close_halt']['remaining_binance'],1)
        self.assertEqual(self.g['close_halt']['remaining_mt5'],0)
        for _ in range(5): self.e.tick(self.c,self.p,self.signal)
        self.assertEqual(len(self.closes()),4)
        report=build_report(self.e.state,self.store.events())
        row=report['groups'][0]
        self.assertEqual(row['retry_limit'],3);self.assertEqual(row['close_attempts']['binance'],3)
        self.assertEqual(row['close_halt']['leg'],'binance')
        self.assertEqual(row['close_protection']['remaining_binance'],1)
        self.assertEqual(report['orders'][-1]['close_attempt'],3)
        self.assertEqual(report['orders'][-1]['slippage_mode'],'protective')
        intents=[e for e in report['events'] if e['kind']=='order_intent' and e['data'].get('leg')=='binance']
        self.assertEqual(intents[0]['data']['close_attempt'],3)  # Store.events is newest first.

    def test_explicit_manual_retry_grants_a_new_budget_without_reopening_mt5(self):
        self.b.expired=10
        for _ in range(5): self.e.tick(self.c,self.p,self.signal)
        self.b.expired=0;self.e.request_close(self.g['id'])
        self.assertEqual(self.g['close_attempts'],dict(mt5=0,binance=0))
        self.e.tick(self.c,self.p,self.signal)
        self.assertEqual(self.g['status'],'closed');self.assertEqual(len(self.closes('mt5')),1)
        self.assertEqual(self.closes('binance')[-1]['close_attempt'],1)
        self.assertFalse(self.e.state['enabled'])

    def test_restart_preserves_spent_budget_and_requires_account_reconciliation(self):
        self.b.expired=10
        for _ in range(2): self.e.tick(self.c,self.p,self.signal)
        restarted=Engine(self.store,self.b);g=restarted.active()[0]
        restarted.tick(self.c,self.p,self.signal)
        self.assertTrue(restarted.state['recovery']);self.assertEqual(len(self.closes('binance')),2)
        restarted.state['recovery']=False  # Simulate a successful exact account reconciliation.
        restarted.tick(self.c,self.p,self.signal);restarted.tick(self.c,self.p,self.signal)
        self.assertEqual(len(self.closes('binance')),3);self.assertEqual(g['status'],'attention')
        self.assertFalse(restarted.state['enabled'])

    def test_stale_quotes_spend_no_budget_and_send_no_orders(self):
        self.e.request_close(self.g['id']);q=copy.deepcopy(self.signal)
        for leg in ('binance','mt5'): q[leg]['time_ms']-=60000
        for _ in range(5): self.e.close_group(self.g,q)
        self.assertEqual(self.closes(),[]);self.assertNotIn('close_attempts',self.g)
        self.e.close_group(self.g,self.signal)
        self.assertEqual(self.g['status'],'closed')

    def test_partial_fills_reduce_only_the_confirmed_excess(self):
        self.b.partial=True;self.e.request_close(self.g['id'])
        self.e.close_group(self.g,self.signal)
        self.assertEqual(self.e.amounts(self.g),dict(mt5=.5,binance=.75))
        self.e.close_group(self.g,self.signal)
        self.assertEqual(self.g['status'],'closed')
        self.assertEqual([o['requested'] for o in self.closes('binance')],[1,.25,.5])
        self.assertEqual(self.g['close_attempts'],dict(mt5=2,binance=3))

    def test_legacy_exhausted_group_is_not_automatically_given_more_attempts(self):
        self.e.request_close(self.g['id']);self.g['attempts']=3
        self.e.close_group(self.g,self.signal)
        self.assertEqual(self.g['status'],'attention');self.assertEqual(self.closes(),[])

    def test_normal_paired_close_has_no_extra_journal_commit_or_pause(self):
        with patch.object(self.store,'commit',wraps=self.store.commit) as commits, \
                patch.object(self.store,'commit_many',wraps=self.store.commit_many) as batches:
            self.e.tick(self.c,self.p,self.signal)
        self.assertEqual(self.g['status'],'closed');self.assertTrue(self.e.state['enabled'])
        self.assertEqual(self.closes('mt5')[0]['slippage_mode'],'normal')
        self.assertEqual(self.closes('binance')[0]['slippage_mode'],'normal')
        self.assertEqual([c.args[1] for c in commits.call_args_list],
                         ['exit_signal','order_result','order_result','group_closed'])
        self.assertEqual(batches.call_count,1)
        self.assertEqual([event[0] for event in batches.call_args.args[1]],['order_intent','order_intent'])

    def test_brief_native_history_lag_does_not_latch_a_pause_or_repeat_the_close(self):
        self.b.mt5_unknown=self.b.native_ack=True
        with patch('trading_engine.time.monotonic',return_value=100):
            self.e.tick(self.c,self.p,self.signal)
            self.assertTrue(self.e.state['enabled']);self.assertEqual(self.g['status'],'closing')
            pending=self.e.uncertain(self.g)[0]
            self.assertEqual(self.e._close_confirmation_deadlines[pending['id']],100.3)
            pending['reconcile']['next_ms']=0
            self.e.tick(self.c,self.p,self.signal)
        self.assertEqual(self.g['status'],'closed');self.assertTrue(self.e.state['enabled'])
        self.assertEqual(len(self.closes('mt5')),1);self.assertEqual(len(self.closes('binance')),1)
        self.assertEqual(self.e._close_confirmation_deadlines,{})

    def test_native_grace_expiry_pauses_even_without_quotes_and_does_not_resubmit(self):
        self.b.mt5_unknown=self.b.native_ack=self.b.query_unknown=True
        with patch('trading_engine.time.monotonic',return_value=100):
            self.e.tick(self.c,self.p,self.signal)
        with patch('trading_engine.time.monotonic',return_value=100.301):
            for _ in range(3): self.e.reconcile_pending(self.c)
        self.assertFalse(self.e.state['enabled']);self.assertEqual(len(self.closes()),2)
        self.assertEqual(self.g['close_attempts'],dict(binance=1,mt5=1))

    def test_manual_pause_during_native_grace_cannot_be_automatically_rearmed(self):
        self.b.mt5_unknown=self.b.native_ack=True
        with patch('trading_engine.time.monotonic',return_value=100):
            self.e.tick(self.c,self.p,self.signal);self.e.pause()
            self.e.uncertain(self.g)[0]['reconcile']['next_ms']=0
            self.e.tick(self.c,self.p,self.signal)
        self.assertEqual(self.g['status'],'closed');self.assertFalse(self.e.state['enabled'])

    def test_unknown_partial_native_fill_does_not_keep_grace_permission(self):
        self.b.mt5_unknown=self.b.native_ack=True
        self.e.tick(self.c,self.p,self.signal)
        pending=self.e.uncertain(self.g)[0];pending['reconcile']['next_ms']=0
        self.b.query=lambda o:dict(status='unknown',qty=.5,price=4300,ticket='500')
        self.e.reconcile_pending(self.c)
        self.assertFalse(self.e.state['enabled']);self.assertEqual(len(self.closes()),2)


if __name__=='__main__': unittest.main()
