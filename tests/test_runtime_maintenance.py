import copy
import tempfile
import threading
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

import _test_runtime
from server import TradingRuntime
from execution_latency import LatencyStats
from runtime_maintenance import MaintenanceMonitor
from trading_brokers import PaperBroker
from trading_engine import Engine
from trading_store import Store
from test_trading_engine import config, quote


class MaintenanceTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.store = Store(Path(self.temp.name)/'state.db'); self.addCleanup(self.store.close)
        self.c = config(); self.c['execution']['mode'] = 'live'; self.c['mt5']['adapter'] = 'native'
        self.c['costs'].update(usdt_usd_auto=True, binance_taker_percent=.05)
        self.q = quote(self.c); self.plan = dict(qty=2, lots=.02, contract=100)
        self.e = Engine(self.store, PaperBroker()); self.e.start(self.c)
        self.e.tick(self.c, self.plan, self.q)
        self.g = self.e.active()[0]
        self.e.request_close(self.g['id']); self.e.tick(self.c, self.plan, self.q)
        self.assertEqual(self.g['status'], 'closed')
        self.e.state['enabled'] = False
        self.r = TradingRuntime.__new__(TradingRuntime)
        self.r.config = self.c; self.r.engine = self.e; self.r.quote = self.q; self.r.plan = self.plan
        self.r.lock = threading.RLock(); self.r.stop_event = threading.Event(); self.r.market_wake = threading.Event()
        self.r.connected = self.r.reconciled = True; self.r.market_meta = {}; self.r.entry_preflight = None
        self.r.latency = LatencyStats(); self.e.latency = self.r.latency
        self.r._last_mt5_snapshot_at = time.monotonic(); self.r._mt5_risk_error = ''
        self.r._last_mt5_snapshot = dict(allowed=True, positions=[])
        self.r.pump = SimpleNamespace(read=lambda:(quote(self.c), 1, ''), mt5_transport='test', binance_transport='test')
        self.r.binance = Mock(); self.r.terminal = Mock()
        self.client = Mock()
        self.client.trades.return_value = [dict(qty='2', commission='.1')]
        self.client.commissions_usdt.side_effect = lambda rows:sum(float(x['commission']) for x in rows)
        self.client.income.return_value = []
        self.client.usdt_usd.return_value = dict(value=.999)
        self.history = Mock()
        self.history.call.return_value = [dict(comment=o['id'], volume=.02, commission=-.12, fee=0, swap=0)
            for o in self.e.state['orders'] if o['leg']=='mt5']
        self.account = Mock(); self.account.call.return_value = dict(allowed=True, positions=[])
        self.factory = Mock(side_effect=[self.history, self.account])
        self.monitor = MaintenanceMonitor(self.r, self.client, self.factory)
        self.r.maintenance = self.monitor
        self.addCleanup(self.monitor.close)

    def run_blocked(self, kind, operation, continuation):
        entered, release = threading.Event(), threading.Event()
        original = operation.side_effect
        returned = operation.return_value
        def blocked(*args, **kwargs):
            entered.set()
            if not release.wait(3): raise TimeoutError('test release missing')
            return original(*args, **kwargs) if callable(original) else returned
        operation.side_effect = blocked
        thread = threading.Thread(target=self.monitor.step, args=(kind,))
        thread.start()
        try:
            self.assertTrue(entered.wait(1))
            continuation()
        finally:
            release.set(); thread.join(2)
        self.assertFalse(thread.is_alive())

    def assert_strategy_free(self):
        # The actual runtime poll can run while an unrelated query is waiting.
        self.assertTrue(self.r.lock.acquire(timeout=.2))
        try:
            with patch.object(self.e, 'tick', wraps=self.e.tick) as tick:
                self.r._poll()
                tick.assert_called_once()
        finally: self.r.lock.release()
        self.assertFalse(self.r.terminal.mock_calls)
        self.assertFalse(self.r.binance.mock_calls)

    def test_slow_fx_does_not_block_strategy_and_never_uses_execution_client(self):
        self.run_blocked('fx', self.client.usdt_usd, self.assert_strategy_free)
        self.assertEqual(self.c['costs']['usdt_usd'], .999)

    def test_slow_history_does_not_block_strategy_or_account_lane(self):
        def continuation():
            self.assert_strategy_free()
            self.assertTrue(self.monitor.step('account'))
            self.account.call.assert_called_once_with('snapshot')
            self.assertIsNot(self.account, self.history)
        self.run_blocked('costs', self.history.call, continuation)
        self.assertTrue(self.g['costs_verified'])
        self.assertAlmostEqual(self.g['valuation']['fees'], .44)

    def test_config_change_discards_old_fx(self):
        def change(): self.c['strategy']['cooldown_seconds'] += 1
        before = self.c['costs']['usdt_usd']
        self.run_blocked('fx', self.client.usdt_usd, change)
        self.assertEqual(self.c['costs']['usdt_usd'], before)

    def test_replaced_connection_discards_old_account_read(self):
        self.factory.side_effect = [self.account]
        def replace(): self.r.maintenance = object()
        before = self.r._last_mt5_snapshot
        self.run_blocked('account', self.account.call, replace)
        self.assertIs(self.r._last_mt5_snapshot, before)

    def test_changed_order_discards_completed_fee_query(self):
        def change(): self.e.state['orders'][0]['result']['ticket'] = 'different-order'
        before = copy.deepcopy(self.e.state['orders'][0]['result'])
        self.run_blocked('costs', self.history.call, change)
        self.assertFalse(self.g.get('costs_verified', False))
        self.assertEqual(self.e.state['orders'][0]['result'].get('fee'), before.get('fee'))

    def test_quote_valuation_changes_do_not_starve_fee_publication(self):
        self.run_blocked('costs', self.history.call, lambda:self.g.update(valuation={'net':12345}))
        self.assertTrue(self.g['costs_verified'])
        self.assertNotEqual(self.g['valuation']['net'], 12345)

    def test_history_failure_preserves_estimates_and_sets_retry_time(self):
        before = copy.deepcopy(self.e.state['orders']); valuation = copy.deepcopy(self.g['valuation'])
        self.history.call.return_value = []
        self.assertTrue(self.monitor.step('costs'))
        self.assertEqual(self.e.state['orders'], before)
        self.assertEqual(self.g['valuation'], valuation)
        self.assertIn('cost_verification_error', self.g)
        self.assertGreater(self.g['cost_check_ms'], 0)
        self.assertIsNone(self.monitor.capture('costs'))

    def test_storage_failure_rolls_back_fee_publication(self):
        before = copy.deepcopy(self.e.state)
        with patch.object(self.e, 'save', side_effect=OSError('disk error')):
            self.assertFalse(self.monitor.step('costs'))
        self.assertEqual(self.e.state, before)

    def test_one_closed_group_per_step(self):
        duplicate = copy.deepcopy(self.g); duplicate['id'] = 'second-closed-group'
        self.e.state['groups'].append(duplicate)
        orders = copy.deepcopy(self.e.state['orders'])
        for order in orders:
            order['group'] = duplicate['id']; order['id'] += 'second'
        self.e.state['orders'].extend(orders)
        self.assertEqual(len(self.monitor.capture('costs')['state']['groups']), 1)
        self.assertTrue(self.monitor.step('costs'))
        self.assertTrue(self.g['costs_verified']); self.assertFalse(duplicate.get('costs_verified', False))
        self.assertEqual(self.monitor.capture('costs')['ids'], {duplicate['id']})

    def test_pending_execution_defers_historical_work(self):
        self.e.state['groups'].append(dict(id='pending', status='opening'))
        for kind in ('costs', 'fx', 'funding'):
            self.assertFalse(self.monitor.step(kind))
        self.assertFalse(self.client.mock_calls); self.assertFalse(self.factory.mock_calls)

    def test_account_failure_pauses_entries_but_still_runs_exit_management(self):
        self.e.state['last_open_ms'] = 0
        self.e.start(self.c); self.e.tick(self.c, self.plan, quote(self.c))
        opened = self.e.active()[0]
        self.e.request_close(opened['id'])
        self.factory.side_effect = [self.account]
        self.account.call.side_effect = TimeoutError('private account info')
        self.assertTrue(self.monitor.step('account'))
        self.r._poll()
        self.assertEqual(opened['status'], 'closed')
        self.assertFalse(self.e.state['enabled'])
        self.assertNotIn('private account', self.r._mt5_risk_error)
        self.account.call.side_effect = None
        self.assertTrue(self.monitor.step('account'))
        self.assertFalse(self.e.state['enabled'])  # recovery never rearms trading

    def test_stale_account_gate_does_not_skip_tick(self):
        self.r._last_mt5_snapshot_at = time.monotonic()-16
        self.e.start(self.c)
        with patch.object(self.e, 'tick', wraps=self.e.tick) as tick:
            self.r._poll(); tick.assert_called_once()
        self.assertFalse(self.e.state['enabled'])
        self.assertIn('过期', self.e.state['alarm'])

    def test_fx_refresh_also_available_in_paper_mode_without_a_terminal(self):
        self.c['execution']['mode'] = 'paper'; self.c['mt5']['adapter'] = 'paper'
        self.assertFalse(self.monitor.step('account'))
        self.assertTrue(self.monitor.step('fx'))
        self.factory.assert_not_called()

    def test_funding_merge_keeps_newer_mt5_swap(self):
        self.e.state['last_open_ms'] = 0
        self.e.start(self.c); self.e.tick(self.c, self.plan, quote(self.c))
        opened = self.e.active()[0]
        self.g['closed_ms'] = opened['opened_ms']-1
        self.client.income.return_value = [dict(incomeType='FUNDING_FEE', time=opened['opened_ms'], income='1')]
        self.run_blocked('funding', self.client.income, lambda:opened.update(live_mt5_swap_usd=-.25))
        self.assertAlmostEqual(opened['live_binance_funding_usd'], 1)
        self.assertAlmostEqual(opened['live_carry_usd'], .75)

    def test_invalid_funding_retains_previous_value(self):
        self.e.state['last_open_ms'] = 0
        self.e.start(self.c); self.e.tick(self.c, self.plan, quote(self.c))
        opened = self.e.active()[0]
        opened.update(live_binance_funding_usd=.7, live_carry_usd=.5)
        self.g['closed_ms'] = opened['opened_ms']-1
        self.client.income.return_value = [dict(incomeType='FUNDING_FEE', time=opened['opened_ms'], income='nan')]
        self.assertTrue(self.monitor.step('funding'))
        self.assertEqual(opened['live_binance_funding_usd'], .7)
        self.assertEqual(opened['live_carry_usd'], .5)
        self.assertIn('carry_warning', self.r.market_meta)

    def test_invalid_account_swap_cannot_corrupt_live_state(self):
        self.factory.side_effect = [self.account]
        self.account.call.return_value = dict(allowed=True, positions=[dict(ticket='test', swap=float('nan'))])
        before = copy.deepcopy(self.r._last_mt5_snapshot)
        self.assertTrue(self.monitor.step('account'))
        self.assertEqual(self.r._last_mt5_snapshot, before)
        self.assertIn('读取失败', self.r._mt5_risk_error)

    def test_stopped_monitor_does_not_initialize_or_publish(self):
        self.monitor.close()
        for kind in ('account', 'fx', 'costs', 'funding'): self.assertFalse(self.monitor.step(kind))
        self.factory.assert_not_called(); self.client.usdt_usd.assert_not_called()

    def test_busy_execution_lock_defers_work_without_waiting(self):
        result = []
        with self.r.lock:
            thread = threading.Thread(target=lambda:result.append(self.monitor.step('costs')))
            thread.start(); thread.join(.5)
            self.assertFalse(thread.is_alive())
        self.assertEqual(result, [False]); self.factory.assert_not_called()

    def test_terminal_created_during_stop_is_closed_and_cannot_publish(self):
        def create():
            self.monitor.close()
            return self.account
        self.factory.side_effect = create
        before = self.r._last_mt5_snapshot
        self.assertFalse(self.monitor.step('account'))
        self.assertIs(self.r._last_mt5_snapshot, before)
        self.account.close.assert_called_once(); self.account.call.assert_not_called()

    def test_imported_fee_failure_cannot_publish_partial_fee_updates(self):
        self.g['imported'] = True
        before = copy.deepcopy(self.e.state['orders'])
        # The import path checks the MT5 close first, then the Binance close.
        # An empty Binance response after a matched MT5 deal must stay atomic.
        self.client.trades.return_value = []
        self.assertTrue(self.monitor.step('costs'))
        self.assertFalse(self.g.get('import_closing_costs_checked', False))
        self.assertEqual(self.e.state['orders'], before)
        self.assertGreater(self.g['import_cost_check_ms'], 0)

    def test_partial_imported_binance_fills_keep_costs_unverified(self):
        self.g['imported'] = True
        before = copy.deepcopy(self.e.state['orders'])
        self.client.trades.return_value = [dict(qty='1', commission='.1')]
        self.assertTrue(self.monitor.step('costs'))
        self.assertFalse(self.g.get('import_closing_costs_checked', False))
        self.assertEqual(self.e.state['orders'], before)


if __name__=='__main__': unittest.main()
