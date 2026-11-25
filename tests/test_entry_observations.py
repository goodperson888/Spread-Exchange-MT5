import json
import unittest

from entry_observations import EntryObservations
from pair_instruments import direction_quote
from test_pair_combinations import native_config


class EntryObservationTests(unittest.TestCase):
    def setUp(self):
        self.c = native_config()
        self.c['strategy'].update(direction_mode='auto', entry_spread_usd=.6, entry_lower_usd=-.5)
        self.q = dict(key='PRIVATE-PAIR', entry=-.396, exit=.134, time_ms=1000,
                      binance=dict(bid=4000, ask=4000.2, observed_ms=1000),
                      mt5=dict(bid=4000.066, ask=4000.396, observed_ms=998))
        self.o = EntryObservations()

    def test_wrong_side_of_threshold_is_not_reached_and_reverse_stays_canonical(self):
        self.o.observe(self.c, self.q, 'WAIT_THRESHOLD', enabled=True, now=1002)
        f, r = self.o.snapshot()['episodes']
        self.assertFalse(f['latest']['threshold_reached'])
        self.assertAlmostEqual(f['latest']['distance'], .996)
        self.assertAlmostEqual(r['latest']['distance'], .634)
        projected = direction_quote(self.q, dict(pair=self.c['pair'], direction='reverse'))
        self.o.observe(self.c, projected, 'PREFLIGHT_BLOCKED', direction='reverse',
                       threshold=-.5, stage='preflight', now=1003)
        row = self.o.snapshot()['episodes'][-1]
        self.assertEqual(row['latest']['executable_spread'], .134)
        self.assertEqual(row['latest']['local_skew_ms'], 2)

    def test_reached_price_and_blocker_are_independent(self):
        q = dict(self.q, entry=.7)
        self.o.observe(self.c, q, 'COOLDOWN', direction='forward', enabled=True, now=1000)
        row = self.o.snapshot()['episodes'][0]
        self.assertEqual(row['code'], 'COOLDOWN')
        self.assertTrue(row['latest']['threshold_reached'])
        self.assertEqual(row['reached_samples'], 1)
        self.assertEqual(row['last_reached']['executable_spread'], .7)

    def test_repeats_keep_first_latest_extreme_and_split_on_reason_or_rules(self):
        for t, v in [(1000,.7),(1001,.9),(1002,.8)]:
            self.o.observe(self.c, dict(self.q, entry=v), 'COOLDOWN', direction='forward', now=t)
        rows = self.o.snapshot()['episodes']
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]['samples'], 3)
        self.assertEqual(rows[0]['first']['entry'], .7)
        self.assertEqual(rows[0]['latest']['entry'], .8)
        self.assertEqual(rows[0]['max_executable_spread'], .9)
        self.o.observe(self.c, self.q, 'WAIT_THRESHOLD', direction='forward', now=1003)
        self.c['strategy']['entry_spread_usd'] = 1
        self.o.observe(self.c, self.q, 'WAIT_THRESHOLD', direction='forward', now=1004)
        self.assertEqual(len(self.o.snapshot()['episodes']), 3)

    def test_dynamic_thresholds_aggregate_but_keep_each_sample_bounds(self):
        self.c['strategy'].update(dynamic_reference_enabled=True, entry_deviation_usd=1)
        for i in range(10):
            self.o.observe(self.c, dict(self.q, reference_ready=True, reference_entry=i,
                           reference_exit=i+.5), 'WAIT_THRESHOLD', direction='forward', now=1000+i)
        rows = self.o.snapshot()['episodes']
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]['first']['threshold'], 1)
        self.assertEqual(rows[0]['latest']['threshold'], 10)

    def test_bounded_memory_reset_and_snapshot_do_not_expose_private_config(self):
        self.c['api_secret'] = 'DO-NOT-EXPORT'
        self.c['mt5']['terminal_path'] = 'DO-NOT-EXPORT'
        for i in range(600):
            self.o.observe(self.c, self.q, 'WAIT_THRESHOLD', direction='forward', now=i*30000)
        report = self.o.snapshot()
        self.assertEqual(len(report['episodes']), self.o.LIMIT)
        self.assertLessEqual(len(self.o.latest), self.o.LIMIT)
        self.assertEqual(report['total_episodes'], 600)
        self.assertNotIn('PRIVATE', json.dumps(report))
        self.assertNotIn('DO-NOT-EXPORT', json.dumps(report))
        report['episodes'][-1]['latest']['threshold'] = 999
        self.assertEqual(self.o.snapshot()['episodes'][-1]['latest']['threshold'], .6)
        self.o.reset('live')
        self.assertEqual(self.o.snapshot()['episodes'], [])

    def test_chart_summary_keeps_recent_touch_when_current_price_recedes(self):
        from trading_config import pair_key
        self.o.observe(self.c,dict(self.q,entry=.7),'LIQUIDITY_SPREAD',enabled=True,
                       direction='forward',stage='preflight',now=1000,
                       liquidity=dict(code='LIQUIDITY_SPREAD',executable_spread=.5,threshold=.6))
        self.o.observe(self.c,self.q,'WAIT_THRESHOLD',direction='forward',enabled=True,now=1001)
        report=self.o.summary(pair_key(self.c))
        self.assertEqual(report['current'][0]['code'],'WAIT_THRESHOLD')
        self.assertEqual(report['recent_touches'][0]['code'],'LIQUIDITY_SPREAD')
        report['recent_touches'][0]['sample']['liquidity']['threshold']=999
        self.assertEqual(self.o.summary(pair_key(self.c))['recent_touches'][0]['sample']['liquidity']['threshold'],.6)
        self.assertFalse(self.o.summary('OTHER-PAIR')['current'])
        self.o.reset('live')
        self.assertFalse(self.o.summary(pair_key(self.c))['recent_touches'])

    def test_unknown_codes_and_stages_do_not_export_raw_error_text(self):
        self.o.observe(self.c, self.q, 'PRIVATE-ERROR')
        self.o.observe(self.c, self.q, 'WAIT_THRESHOLD', stage='PRIVATE-STAGE')
        self.assertFalse(self.o.snapshot()['episodes'])
