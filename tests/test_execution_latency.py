import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from execution_diagnostics import export_report
from execution_latency import LatencyStats
from trading_brokers import PaperBroker
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
                    return original_submit(order)
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
                with patch('execution_diagnostics.PROCESS_LATENCY', stats):
                    report = export_report(store.path)
                self.assertEqual(report['latency'], stats.snapshot())
                self.assertIn('quote_wait', report['notes'][-1])
            finally: store.close()


if __name__=='__main__': unittest.main()
