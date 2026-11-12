import copy
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from execution_diagnostics import export_report
from exit_observations import ExitObservations
from trading_brokers import PaperBroker
from trading_engine import Engine
from trading_store import Store
from test_trading_engine import config, quote


class ObservationTests(unittest.TestCase):
    def setUp(self):
        self.stats = ExitObservations(); self.stats.reset('live')
        self.c = config(); self.q = quote(self.c)
        self.now = self.q['time_ms']
        self.g = dict(id='private-group', key=self.q['key'], entry=6,
                      parameters={**self.c['strategy'], 'require_net_profit': True, 'exit_mode': 'group'})
        self.q['exit'] = 3.9
        self.aliases = {'private-group': 'G1'}

    def rows(self): return self.stats.snapshot(self.aliases)['episodes']

    def record(self, **kwargs):
        self.stats.observe(self.g, self.q, now=self.now, **kwargs)

    def test_repeated_net_veto_retains_first_latest_extrema_and_count(self):
        self.record(valuation=dict(net=-1))
        self.q['exit'] = 3.8; self.now += 10
        self.record(valuation=dict(net=-.2))
        rows = self.rows(); self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(row['code'], 'WAIT_NET_PROFIT')
        self.assertEqual(row['samples'], 2)
        self.assertEqual(row['first']['net'], -1); self.assertEqual(row['latest']['net'], -.2)
        self.assertEqual(row['min_exit_spread'], 3.8); self.assertEqual(row['max_net'], -.2)
        self.assertTrue(row['first']['target_reached']); self.assertFalse(row['first']['net_passed'])

    def test_reason_rule_and_interval_changes_start_new_episodes(self):
        self.record(valuation=dict(net=-1))
        self.q['exit'] = 5; self.record(valuation=dict(net=-1))
        self.g['exit_rules_revision'] = 1; self.record(valuation=dict(net=-1))
        self.now += self.stats.INTERVAL_MS; self.q = quote(self.c); self.q['exit'] = 5
        self.record(valuation=dict(net=-1))
        rows = self.rows(); self.assertEqual(len(rows), 4)
        self.assertEqual(rows[1]['code'], 'WAIT_TARGET')
        self.assertEqual(rows[2]['rules']['revision'], 1)

    def test_invalid_quote_has_no_stale_valuation_and_tracks_local_age(self):
        self.q['binance']['observed_ms'] = self.now-4000
        self.q['mt5']['observed_ms'] = self.now
        self.q['mt5']['time_ms'] = self.now+7200000
        self.record(code='QUOTE_UNAVAILABLE')
        first = self.rows()[0]['first']
        self.assertNotIn('net', first); self.assertFalse(first['valuation_evaluated'])
        self.assertFalse(first['quote_valid'])
        self.assertEqual(first['mt5_age_ms'], 0)
        self.assertEqual(first['local_skew_ms'], 4000)
        self.assertIn('STALE_QUOTE', first['quote_issues'])
        self.q['binance']['observed_ms'] = self.now
        self.q['binance']['bid'] = -1
        self.record(code='QUOTE_UNAVAILABLE')
        self.assertEqual(len(self.rows()), 2)
        self.assertIn('INVALID_PRICES', self.rows()[1]['first']['quote_issues'])

    def test_missing_and_future_quote_fields_are_explicit(self):
        self.stats.observe(self.g, None, 'QUOTE_UNAVAILABLE', now=self.now)
        self.assertIn('MISSING_QUOTE', self.rows()[0]['first']['quote_issues'])
        self.q['mt5']['observed_ms'] = self.now+2000
        self.record(code='QUOTE_UNAVAILABLE')
        self.assertIn('FUTURE_OBSERVED_TIME', self.rows()[1]['first']['quote_issues'])

    def test_basket_target_and_net_use_basket_instead_of_individual(self):
        self.g['parameters']['exit_mode'] = 'basket'
        basket = dict(entry=5, net=-2, members=2, target_blocked_members=1, net_blocked_members=2)
        self.record(valuation=dict(net=10), basket=basket)
        row = self.rows()[0]
        self.assertEqual(row['code'], 'WAIT_BASKET_TARGET')
        self.assertEqual(row['first']['target_exit_spread'], 3)
        self.assertEqual(row['first']['group_target_exit_spread'], 4)
        self.q['exit'] = 2.9; self.record(valuation=dict(net=10), basket=basket)
        self.assertEqual(self.rows()[-1]['code'], 'WAIT_BASKET_NET_PROFIT')
        basket.update(net=10, net_blocked_members=1)
        self.record(valuation=dict(net=10), basket=basket)
        self.assertEqual(self.rows()[-1]['code'], 'WAIT_BASKET_MEMBER')

    def test_absolute_target_and_import_require_net_even_if_flag_disabled(self):
        self.g.update(imported=True)
        self.g['parameters'].update(target_mode='absolute', exit_spread_usd=4, require_net_profit=False)
        self.record(valuation=dict(net=-1))
        self.assertEqual(self.rows()[0]['code'], 'WAIT_NET_PROFIT')
        self.assertEqual(self.rows()[0]['first']['target_exit_spread'], 4)

    def test_export_aliases_allowlists_and_nonfinite_fields(self):
        secret = 'DO-NOT-EXPORT'
        self.g['parameters'].update(api_key=secret, min_net_profit_usd=float('nan'))
        self.q['key'] = secret; self.q['binance'].update(ticket=secret, source_time_ms=float('inf'))
        self.record(code='QUOTE_UNAVAILABLE', valuation=dict(net=float('nan'), fees=float('inf'), api_key=secret))
        row = self.rows()[0]
        self.assertNotIn('net', row['first']); self.assertNotIn('min_net_profit_usd', row['rules'])
        serialized = json.dumps(self.stats.snapshot(self.aliases), allow_nan=False)
        self.assertNotIn(secret, serialized); self.assertNotIn('private-group', serialized)
        self.assertEqual(row['group'], 'G1')
        unmapped = self.stats.snapshot({})
        self.assertEqual(unmapped['episodes'], []); self.assertEqual(unmapped['omitted_unmapped_episodes'], 1)

    def test_capacity_eviction_cannot_hide_a_later_observation(self):
        self.stats.LIMIT = 3
        # deque limit is fixed on reset.
        self.stats.reset('live')
        self.record(code='WAIT_TARGET')
        for i in range(3):
            self.stats.observe(dict(self.g, id='other'), self.q,
                               'WAIT_TARGET' if i % 2 else 'WAIT_NET_PROFIT', now=self.now)
        self.record(code='WAIT_TARGET')
        rows = self.rows()
        self.assertEqual(len(rows), 1); self.assertEqual(rows[0]['sequence'], 5)
        self.assertLessEqual(len(self.stats.rows), 3); self.assertLessEqual(len(self.stats.latest), 3)

    def test_reset_discards_old_evidence_and_snapshots_remain_stable(self):
        self.record(code='WAIT_TARGET')
        before = self.rows()
        self.q['exit'] = 8; self.record(code='WAIT_TARGET')
        self.assertEqual(before[0]['latest']['exit_spread'], 3.9)
        self.stats.reset('paper')
        self.assertEqual(self.rows(), []); self.assertEqual(self.stats.snapshot({})['mode'], 'paper')


class EngineObservationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.store = Store(Path(self.temp.name)/'state.db'); self.addCleanup(self.store.close)
        self.e = Engine(self.store, PaperBroker()); self.c = config()
        self.c['strategy'].update(exit_mode='group', require_net_profit=True, min_net_profit_usd=0)
        self.p = dict(qty=1, lots=.01, contract=100)
        self.e.start(self.c); self.e.tick(self.c, self.p, quote(self.c))
        self.g = self.e.state['groups'][0]; self.e.pause()
        self.stats = ExitObservations(); self.e.exit_observations = self.stats
        self.aliases = {self.g['id']: 'G1'}
        self.q = quote(self.c, bid=4303, ask=4303.1)

    def rows(self): return self.stats.snapshot(self.aliases)['episodes']

    def test_net_veto_adds_no_commits_queries_or_orders(self):
        original = self.e.valuation
        def valuation(g, q): return dict(original(g, q), net=-1)
        with patch.object(self.e, 'valuation', side_effect=valuation), \
             patch.object(self.store, 'commit', wraps=self.store.commit) as commit, \
             patch.object(self.e.broker, 'submit', wraps=self.e.broker.submit) as submit, \
             patch.object(self.e.broker, 'query', wraps=self.e.broker.query) as query:
            for _ in range(20): self.e.tick(self.c, self.p, self.q)
        self.assertEqual(self.g['status'], 'open')
        self.assertEqual(self.rows()[0]['code'], 'WAIT_NET_PROFIT')
        self.assertEqual(self.rows()[0]['samples'], 20)
        commit.assert_not_called(); submit.assert_not_called(); query.assert_not_called()

    def test_pause_entries_still_exits_and_intent_is_committed_first(self):
        commit = self.store.commit; submit = self.e.broker.submit; order = []
        def committed(state, kind, data=None):
            order.append(kind); return commit(state, kind, data)
        def submitted(row):
            self.assertEqual(order[-1], 'order_intent'); return submit(row)
        with patch.object(self.store, 'commit', side_effect=committed), patch.object(self.e.broker, 'submit', side_effect=submitted):
            self.e.tick(self.c, self.p, self.q)
        self.assertEqual(self.g['status'], 'closed')
        self.assertFalse(self.e.state['enabled'])
        self.assertEqual(self.rows()[0]['code'], 'EXIT_TRIGGERED')
        self.assertEqual(len(self.e.state['orders']), 4)

    def test_stale_quote_and_recovery_never_reuse_old_net(self):
        self.g['valuation'] = dict(net=999)
        self.q['mt5']['time_ms'] -= 10000
        self.e.tick(self.c, self.p, self.q)
        self.assertEqual(self.rows()[0]['code'], 'QUOTE_UNAVAILABLE')
        self.assertNotIn('net', self.rows()[0]['first'])
        self.e.state['recovery'] = True
        self.e.tick(self.c, self.p, quote(self.c))
        self.assertEqual(self.rows()[-1]['code'], 'RECOVERY_PENDING')

    def test_first_leg_recheck_deferral_is_captured_without_submitting_close(self):
        self.e.close_quote_provider = lambda: quote(self.c)
        self.e.tick(self.c, self.p, self.q)
        self.assertEqual(self.g['status'], 'open')
        self.assertEqual([x['code'] for x in self.rows()], ['EXIT_TRIGGERED', 'RECHECK_TARGET'])
        self.assertEqual(len(self.e.state['orders']), 2)

    def test_first_leg_net_recheck_uses_new_valuation(self):
        original = self.e.valuation
        values = iter((10, -1))
        def valuation(g, q): return dict(original(g, q), net=next(values))
        with patch.object(self.e, 'valuation', side_effect=valuation):
            self.e.tick(self.c, self.p, self.q)
        self.assertEqual([x['code'] for x in self.rows()], ['EXIT_TRIGGERED', 'RECHECK_NET_PROFIT'])
        self.assertEqual(self.rows()[-1]['first']['net'], -1)
        self.assertEqual(len(self.e.state['orders']), 2)

    def test_basket_other_members_veto_is_recorded(self):
        self.g['parameters'].update(exit_mode='basket')
        other = copy.deepcopy(self.g); other.update(id='another-group')
        other['parameters']['min_net_profit_usd'] = 1000
        self.e.state['groups'].append(other)
        self.e.tick(self.c, self.p, self.q)
        row = self.rows()[0]
        self.assertEqual(row['code'], 'WAIT_BASKET_MEMBER')
        self.assertEqual(row['first']['basket_net_blocked_members'], 1)
        self.assertEqual(self.g['status'], 'open')

    def test_unmanaged_and_attention_groups_have_explicit_reasons(self):
        self.g.update(status='attention')
        self.e.tick(self.c, self.p, self.q)
        self.assertEqual(self.rows()[-1]['code'], 'ATTENTION_REQUIRED')
        self.g.update(status='open', imported=True, management_enabled=False, history_fees_usd=0, history_funding_usd=0)
        self.e.tick(self.c, self.p, self.q)
        self.assertEqual(self.rows()[-1]['code'], 'MANAGEMENT_DISABLED')

    def test_export_joins_memory_to_persisted_aliases_without_runtime_access(self):
        self.e.tick(self.c, self.p, quote(self.c))
        with patch('execution_diagnostics.PROCESS_EXIT_OBSERVATIONS', self.stats):
            report = export_report(self.store.path)
        self.assertEqual(report['groups'][0]['parameters']['exit_mode'], 'group')
        row = report['exit_observations']['episodes'][0]
        self.assertEqual(row['group'], report['groups'][0]['id'])
        self.assertNotIn(self.g['id'], json.dumps(report))


if __name__ == '__main__': unittest.main()
