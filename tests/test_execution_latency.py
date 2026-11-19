import json
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

from execution_diagnostics import export_report
from execution_latency import LatencyStats
from trading_brokers import PaperBroker, Terminal
from trading_engine import Engine
from trading_store import Store
from test_trading_engine import config, quote


class LatencyTests(unittest.TestCase):
    def test_bounded_samples_and_percentiles(self):
        stats = LatencyStats()
        for value in range(600): stats.record('decision_ms', value)
        phase = stats.snapshot()['phases']['decision_ms']
        self.assertEqual(phase['samples'], 512); self.assertEqual(phase['total_samples'], 600)
        self.assertEqual(phase['median_ms'], 343.5)
        self.assertEqual(phase['p95_ms'], 574); self.assertEqual(phase['max_ms'], 599)
        self.assertEqual(phase['p99_ms'], 594)
        self.assertFalse(phase['p99_low_sample'])

    def test_latest_is_not_max_and_small_p99_is_marked(self):
        stats=LatencyStats()
        stats.record_many((('decision_ms',100),('decision_ms',2),('not-allowed',1),('decision_ms',float('nan'))))
        phase=stats.snapshot()['phases']['decision_ms']
        self.assertEqual(phase['latest_ms'],2)
        self.assertEqual(phase['max_ms'],100)
        self.assertTrue(phase['p99_low_sample'])

    def test_invalid_values_and_arbitrary_fields_cannot_enter_export(self):
        stats = LatencyStats()
        for value in (-1, True, float('nan'), float('inf'), 'account-secret'):
            stats.record('decision_ms', value)
        stats.record('account-secret', 42)
        self.assertEqual(stats.snapshot()['phases'], {})
        self.assertNotIn('secret', json.dumps(stats.snapshot(), allow_nan=False))

    def test_reconnection_resets_mode_and_old_samples(self):
        stats = LatencyStats(); stats.reset('paper'); stats.record('decision_ms', 100)
        stats.reset('live')
        self.assertEqual(stats.snapshot()['mode'], 'live')
        self.assertEqual(stats.snapshot()['phases'], {})

    def test_first_pickup_only_and_local_received_times(self):
        stats = LatencyStats()
        q = dict(key='private-account', binance=dict(observed_ms=100, time_ms=10000000),
                 mt5=dict(observed_ms=103, time_ms=-10000000))
        stats.quote(q, 110); stats.quote(q, 150)
        phase = stats.snapshot()['phases']['quote_wait_ms']
        self.assertEqual(phase['samples'], 1); self.assertEqual(phase['median_ms'], 7)
        self.assertNotIn('private-account', json.dumps(stats.snapshot()))
        q['mt5']['observed_ms'] = 1000
        stats.quote(q, 200)
        self.assertEqual(stats.snapshot()['phases']['quote_wait_ms']['samples'], 1)

    def test_quote_pickup_uses_monotonic_and_pair_dedup_is_bounded(self):
        stats=LatencyStats()
        a=dict(key='private-a',binance=dict(observed_ms=100),mt5=dict(observed_ms=103),
               _published_monotonic=10,binance_transport='WebSocket bookTicker',mt5_transport='原生双品种报价轮询')
        b={**a,'key':'private-b'}
        with patch('execution_latency.time.monotonic',return_value=10.012):
            stats.quote(a,110);stats.quote(b,110);stats.quote(a,120)
        snapshot=stats.snapshot()
        self.assertEqual(snapshot['phases']['quote_pickup_ms']['samples'],2)
        self.assertEqual(snapshot['phases']['quote_pickup_ms']['median_ms'],12)
        self.assertEqual(snapshot['counters']['binance_quote_push'],2)
        self.assertEqual(snapshot['counters']['mt5_quote_poll'],2)
        self.assertNotIn('private',json.dumps(snapshot))
        for index in range(300):stats.quote({**a,'key':str(index)},110)
        self.assertLessEqual(len(stats.last_quotes),256)
        stats.reset('live')
        self.assertEqual(stats.snapshot()['counters'],{})
        self.assertEqual(len(stats.last_quotes),0)

    def test_receipt_counts_do_not_treat_unknown_or_partial_as_full_fill(self):
        stats=LatencyStats();order=dict(leg='mt5',action='open',requested=1)
        for result,valid in [(dict(status='done',qty=1),True),(dict(status='done',qty=.5),True),
                             (dict(status='done',qty=0),True),(dict(status='unknown',qty=0),True),(None,False)]:
            stats.outcome(order,result,valid)
        stats.outcome(dict(leg='private',action='open'),{},False)
        self.assertEqual(stats.snapshot()['counters'],dict(mt5_open_full_fill=1,mt5_open_partial_fill=1,
            mt5_open_zero_fill=1,mt5_open_unconfirmed=2))

    def test_strategy_lock_wait_and_cycle_are_separate(self):
        import _test_runtime
        from server import TradingRuntime
        clock=[100.]
        class DelayedLock:
            def __enter__(self):clock[0]+=.015
            def __exit__(self,*args):pass
        runtime=object.__new__(TradingRuntime)
        runtime.stop_event=threading.Event();runtime.market_wake=threading.Event();runtime.market_wake.set()
        runtime.lock=DelayedLock();runtime.latency=LatencyStats();runtime.connected=True
        runtime.config={'execution':{'poll_ms':0}}
        def poll():clock[0]+=.02;runtime.stop_event.set()
        runtime._poll=poll
        with patch('server.time.monotonic',side_effect=lambda:clock[0]):runtime._loop()
        phases=runtime.latency.snapshot()['phases']
        self.assertEqual(phases['strategy_lock_wait_ms']['median_ms'],15)
        self.assertEqual(phases['strategy_cycle_ms']['median_ms'],20)

    def test_publication_clock_is_not_stored_in_chart_history(self):
        with tempfile.TemporaryDirectory() as temp:
            store=Store(Path(temp)/'state.db')
            try:
                q=quote(config());LatencyStats.published(q)
                store.sample(q)
                for table in ('quotes','quote_minutes'):
                    raw=store.db.execute('SELECT data FROM '+table).fetchone()[0]
                    self.assertNotIn('_published_monotonic',raw)
                self.assertIn('_published_monotonic',q)
            finally:store.close()

    def test_native_quote_read_captures_queue_and_roundtrip_without_extra_calls(self):
        from unittest.mock import Mock
        for command in ('quote','pair_quotes'):
            stats=LatencyStats();clock=[100.]
            class DelayedLock:
                def __enter__(self):clock[0]+=.005
                def __exit__(self,*args):pass
            terminal=object.__new__(Terminal)
            terminal.lock=DelayedLock();terminal.process=Mock();terminal.process.poll.return_value=None
            def write(value):clock[0]+=.002
            terminal.process.stdin.write.side_effect=write
            def response(**kwargs):clock[0]+=.010;return dict(ok=True,data={'bid':4300})
            terminal.responses=Mock();terminal.responses.get.side_effect=response
            with patch('trading_brokers.PROCESS_LATENCY',stats), \
                 patch('trading_brokers.time.monotonic',side_effect=lambda:clock[0]):
                self.assertEqual(terminal.call(command),{'bid':4300})
            phases=stats.snapshot()['phases']
            self.assertEqual(phases['mt5_quote_queue_ms']['median_ms'],5)
            self.assertEqual(phases['mt5_quote_roundtrip_ms']['median_ms'],12)
            terminal.responses.get.assert_called_once()
            terminal.process.stdin.write.assert_called_once()
            self.assertEqual(json.loads(terminal.process.stdin.write.call_args.args[0]),{'command':command})

    def test_execution_quote_latency_links_to_orders_and_is_not_recreated_after_restart(self):
        with tempfile.TemporaryDirectory() as temp:
            store=Store(Path(temp)/'state.db')
            try:
                e=Engine(store,PaperBroker());c=config();e.start(c);e.latency=LatencyStats()
                clock=[100.];q=quote(c);q['_published_monotonic']=99.98
                original=e.broker.submit
                def submit(order):clock[0]+=.025;return original(order)
                with patch('trading_engine.time.monotonic',side_effect=lambda:clock[0]), \
                     patch.object(e.broker,'submit',side_effect=submit) as submits:
                    e.tick(c,dict(qty=2,lots=.02,contract=100),q)
                self.assertEqual(submits.call_count,2)
                g=e.active()[0];timing=g['execution_open']
                self.assertEqual(timing['quote_to_execution_ms'],20)
                self.assertEqual(timing['dispatch_gap_ms'],25)
                report=export_report(store.path)
                self.assertEqual(report['groups'][0]['execution_open']['quote_to_execution_ms'],20)
                self.assertEqual([o['execution_to_dispatch_ms'] for o in report['orders']],[0,25])
                restarted=Engine(store,PaperBroker())
                self.assertNotIn((g['id'],'open'),restarted._execution_timers)
                order=restarted.state['orders'][0];order.pop('execution_to_dispatch_ms')
                restarted._accept_order_result(restarted.active()[0],order,dict(result=dict(status='done',qty=2,price=4305),
                    dispatch_monotonic=200,received_monotonic=200.1,response_received_ms=1,dispatch_ms=1))
                self.assertNotIn('execution_to_dispatch_ms',order)
            finally:store.close()

    def test_decision_excludes_simulated_broker_and_journal_delays_without_extra_commits(self):
        with tempfile.TemporaryDirectory() as temp:
            store = Store(Path(temp)/'state.db')
            try:
                e = Engine(store, PaperBroker()); c = config(); e.start(c)
                stats = LatencyStats(); e.latency = stats
                clock = [100.]
                original_commit, original_submit = store.commit, e.broker.submit
                def commit(*args):
                    clock[0] += .01
                    return original_commit(*args)
                def submit(order):
                    clock[0] += .05
                    result=original_submit(order)
                    result['timings']={'order_send_ms':30,'terminal_queue_ms':5,'private':100}
                    return result
                with patch.object(store, 'commit', side_effect=commit) as commits, \
                     patch.object(e.broker, 'submit', side_effect=submit), \
                     patch('trading_engine.time.monotonic', side_effect=lambda:clock[0]):
                    e.tick(c, dict(qty=2, lots=.02, contract=100), quote(c))
                    self.assertEqual([call.args[1] for call in commits.call_args_list],
                        ['group_opening', 'order_intent', 'order_result', 'order_intent', 'order_result', 'group_opened'])
                phases = stats.snapshot()['phases']
                self.assertEqual(phases['decision_ms']['median_ms'], 0)
                self.assertEqual(phases['intent_journal_ms']['median_ms'], 10)
                self.assertEqual(phases['mt5_open_request_ms']['median_ms'], 50)
                self.assertEqual(phases['binance_open_request_ms']['median_ms'], 50)
                self.assertGreaterEqual(phases['pair_open_ms']['median_ms'], 140)
                self.assertEqual(phases['mt5_order_send_ms']['median_ms'],30)
                self.assertEqual(phases['mt5_terminal_queue_ms']['median_ms'],5)
                self.assertEqual(phases['result_journal_ms']['median_ms'],10)
                self.assertEqual(phases['open_dispatch_gap_ms']['samples'],1)
                self.assertNotIn('mt5_private',phases)
                with patch('execution_diagnostics.PROCESS_LATENCY', stats):
                    report = export_report(store.path)
                self.assertEqual(report['latency'], stats.snapshot())
                self.assertIn('quote_wait', ' '.join(report['notes']))
                self.assertIn('execution_to_dispatch_ms',report['orders'][0])
                self.assertIn('dispatch_gap_ms',report['groups'][0]['execution_open'])
            finally: store.close()


if __name__=='__main__': unittest.main()
