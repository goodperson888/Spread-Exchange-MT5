import sys
import tempfile
import time
import unittest
from unittest.mock import patch
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from trading_brokers import PaperBroker
from trading_engine import Engine
from trading_store import Store
from test_trading_engine import config, quote, ReconcileThenFillBroker


class SlippageBroker(PaperBroker):
    def submit(self, order):
        result=super().submit(order)
        if order['action']=='open':
            # Simulate 500 ms of movement between the signal and second-leg fill.
            result['price']=4300.3 if order['leg']=='mt5' else 4305.5
        return result


class ExecutionTimingTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.store=Store(Path(self.tmp.name)/'state.sqlite3');self.addCleanup(self.store.close)
        self.c=config();self.c['strategy']['entry_spread_usd']=5;self.c['execution']['entry_leg']='mt5'
        self.plan={'lots':.02,'qty':2,'contract':100}

    def test_duplicate_timestamps_are_immutable(self):
        q1=quote(self.c);q1['time_ms']=int(time.time()*1000);q1['entry']=5.7
        q2=dict(q1,entry=9.9,exit=8.8)
        self.store.sample(q1);self.store.sample(q2)
        rows=self.store.samples(q1['key'],q1['time_ms']-1,q1['time_ms']+1)
        self.assertEqual(len(rows),1);self.assertAlmostEqual(rows[0]['entry'],5.7)

    def test_actual_spread_is_recorded_after_both_fills(self):
        e=Engine(self.store,SlippageBroker());e.start(self.c)
        e.tick(self.c,self.plan,quote(self.c))
        g=e.active()[0];orders=[o for o in e.state['orders'] if o['action']=='open']
        self.assertAlmostEqual(g['entry_signal'],5.7)
        self.assertAlmostEqual(g['entry'],5.2)
        self.assertTrue(all(abs(o['signal_spread']-5.7)<1e-9 for o in orders))
        self.assertTrue(all(abs(o['actual_spread']-5.2)<1e-9 for o in orders))
        self.assertTrue(all(abs(o['spread_slippage']+.5)<1e-9 for o in orders))

    def test_second_leg_uses_fresh_quote_provider(self):
        e=Engine(self.store,PaperBroker());e.start(self.c)
        fresh=quote(self.c,bid=4305.5,ask=4305.6)
        calls=[]
        def provider(): calls.append(1);return fresh
        e.quote_provider=provider;e.tick(self.c,self.plan,quote(self.c))
        self.assertEqual(len(calls),1)
        b=next(o for o in e.state['orders'] if o['leg']=='binance' and o['action']=='open')
        self.assertEqual(b['reference'],fresh['binance']['bid'])
        self.assertEqual(b['quote_spread'],fresh['entry'])
        self.assertAlmostEqual(b['signal_spread'],5.7)
        self.assertEqual(b['quote_context']['mt5']['ask'],fresh['mt5']['ask'])

    def test_stage_timers_reuse_existing_commits_and_intent_precedes_submit(self):
        e=Engine(self.store,PaperBroker());e.start(self.c)
        with patch.object(self.store,'commit',wraps=self.store.commit) as commit:
            original=e.broker.submit
            def submit(order):
                # Durable recovery intent must exist before touching a platform.
                saved=self.store.load()['orders'][-1]
                self.assertEqual(saved['id'],order['id'])
                self.assertNotIn('dispatch_ms',saved)
                self.assertIn('dispatch_ms',order)
                return original(order)
            e.broker.submit=submit
            e.tick(self.c,self.plan,quote(self.c))
            self.assertEqual([c.args[1] for c in commit.call_args_list],
                ['group_opening','order_intent','order_result','order_intent','order_result','group_opened'])
        for o in e.state['orders']:
            self.assertGreaterEqual(o['intent_journal_ms'],0)
            self.assertGreaterEqual(o['result']['response_received_ms'],o['dispatch_ms'])
        self.assertGreaterEqual(e.active()[0]['execution_open']['elapsed_ms'],0)

    def test_manual_close_keeps_first_quote_and_times_pair_confirmation(self):
        e=Engine(self.store,PaperBroker());e.start(self.c);e.tick(self.c,self.plan,quote(self.c))
        g=e.active()[0];e.request_close(g['id'])
        q=quote(self.c,bid=4304,ask=4304.1)
        e.close_group(g,q)
        self.assertEqual(g['status'],'closed')
        self.assertEqual(g['exit_signal'],q['exit'])
        self.assertEqual(g['execution_close']['signal_spread'],q['exit'])
        self.assertGreaterEqual(g['execution_close']['elapsed_ms'],0)
        self.assertFalse(e._execution_timers)

    def test_restart_does_not_invent_pair_latency(self):
        e=Engine(self.store,PaperBroker());e.start(self.c);e.tick(self.c,self.plan,quote(self.c))
        g=e.active()[0];e.request_close(g['id'])
        restarted=Engine(self.store,PaperBroker())
        g=restarted.active()[0];restarted.close_group(g,quote(self.c))
        self.assertEqual(g['status'],'closed')
        self.assertIsNone(g['execution_close']['elapsed_ms'])

    def test_order_durations_use_monotonic_clock(self):
        e=Engine(self.store,PaperBroker());e.start(self.c);e.tick(self.c,self.plan,quote(self.c))
        with patch('trading_engine.time.monotonic',side_effect=[10,10.007,10.047]):
            o=e.order(e.active()[0],'binance','close',2,quote(self.c))
        self.assertAlmostEqual(o['intent_journal_ms'],7)
        self.assertEqual(o['result']['request_ms'],40)

    def test_late_query_confirmation_is_not_direct_fill_latency(self):
        self.c['execution']['entry_leg']='binance'
        e=Engine(self.store,ReconcileThenFillBroker());e.start(self.c)
        e.tick(self.c,self.plan,quote(self.c))
        o=e.state['orders'][0];direct=o['result']['response_received_ms']
        duration=o['result']['request_ms'];o['reconcile']['next_ms']=0
        self.assertTrue(e.resolve(e.active()[0]))
        self.assertEqual(o['result']['response_received_ms'],direct)
        self.assertEqual(o['result']['request_ms'],duration)
        self.assertIn('reconcile_time_ms',o['result'])
        self.assertNotIn('fill_time_ms',o['result'])

    def test_actual_entry_below_threshold_keeps_group_and_warns(self):
        class BadFill(SlippageBroker):
            def submit(self, order):
                r=super().submit(order)
                if order['leg']=='binance' and order['action']=='open':r['price']=4304.7
                return r
        e=Engine(self.store,BadFill());e.start(self.c);e.tick(self.c,self.plan,quote(self.c))
        active=e.active()
        self.assertEqual(len(active),1)
        g=active[0]
        self.assertEqual(g['status'],'open')
        self.assertIn('低于触发阈值',g.get('execution_warning',''))
        self.assertTrue(e.state['enabled'])
        self.assertFalse(any(o['action']=='close' for o in e.state['orders']))


if __name__=='__main__':unittest.main()
