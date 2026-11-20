"""Consecutive source quotes gate real engine entries, never cached UI polls."""
import copy
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from entry_confirmation import EntryConfirmations, DEFAULTS, reached
from entry_observations import EntryObservations
from trading_config import validate, pair_key
from trading_engine import Engine
from trading_brokers import PaperBroker
from trading_store import Store
from pair_instruments import equal_lot_plan, entry_direction
from test_pair_combinations import native_config, specs
from test_trading_engine import quote
import pair_profiles


class ConfirmationTests(unittest.TestCase):
    def setUp(self):
        self.s=dict(DEFAULTS,max_quote_age_ms=3000,entry_forward_confirmation_ticks=3,
                    entry_reverse_confirmation_ticks=2)
        self.gate=EntryConfirmations()

    def q(self,n,value=1):
        return dict(entry=value,exit=value+.1,binance=dict(time_ms=n,bid=100+value,ask=100+value+.1),
                    mt5=dict(time_ms=1,bid=99.9,ask=100))

    def sample(self,n,value=1,**kw):
        return self.gate.observe('pair',self.s,self.q(n,value),'forward',1,now=n/1000,**kw)

    def test_better_prices_count_and_miss_resets(self):
        self.assertEqual(self.sample(1)['count'],1)
        self.assertEqual(self.sample(2,2)['count'],2)
        self.assertTrue(self.sample(3,3)['ready'])
        self.assertEqual(self.sample(4,.9)['count'],0)
        self.assertEqual(self.sample(5,1)['count'],1)

    def test_equal_mode_rejects_even_better_values_and_rounds_half_up(self):
        self.s.update(entry_forward_confirmation_mode='equal')
        self.assertEqual(self.sample(1,1.004)['count'],1)
        self.assertEqual(self.sample(2,1.005)['count'],0)
        for d in ('forward','reverse'):
            self.s[f'entry_{d}_confirmation_mode']='equal'
            self.assertTrue(reached(self.s,d,-.005,-.01))
            self.assertFalse(reached(self.s,d,-.004,-.01))
        self.assertFalse(reached(self.s,'forward',float('nan'),1))

    def test_repeated_tick_receipt_and_transport_changes_do_not_count(self):
        self.sample(1)
        q=self.q(1);q['binance'].update(observed_ms=999,transport='push',source_time_ms=1)
        q['sequence']=999;q['time_ms']=999
        for _ in range(6):
            r=self.gate.observe('pair',self.s,q,'forward',1,now=.5)
            self.assertEqual(r['count'],1)
        self.assertEqual(self.sample(2)['count'],2)

    def test_reset_stale_disabled_and_late_source_cannot_rearm_from_cache(self):
        self.sample(1);self.sample(2)
        self.gate.reset('pair')
        self.assertEqual(self.sample(2)['count'],0)
        self.assertEqual(self.sample(3)['count'],1)
        self.sample(4,valid=False)
        self.assertEqual(self.sample(4)['count'],0)
        self.assertEqual(self.sample(5)['count'],1)
        self.sample(6,enabled=False)
        self.assertEqual(self.sample(6)['count'],0)
        self.sample(7)
        old=self.gate.observe('pair',self.s,self.q(1),'forward',1,now=.1)
        self.assertFalse(old['ready']);self.assertEqual(old['count'],0)
        self.assertEqual(self.sample(7)['count'],0)
        self.assertEqual(self.sample(8)['count'],1)
        self.assertEqual(self.gate.observe('pair',self.s,self.q(8),'forward',1,now=9)['count'],0)
        self.assertEqual(self.sample(9001)['count'],1)

    def test_rules_direction_and_pairs_are_independent(self):
        self.sample(1);self.sample(2)
        other=self.gate.observe('other',self.s,self.q(3),'forward',1,now=.003)
        self.assertEqual(other['count'],1)
        reverse=self.gate.observe('pair',self.s,self.q(3,-2),'reverse',-1,now=.003)
        self.assertEqual(reverse['count'],1)
        self.s['entry_spread_usd']=2
        self.assertEqual(self.sample(2)['count'],0)
        self.assertEqual(self.sample(3)['count'],1)

    def test_readonly_snapshots_never_count_and_expire_progress(self):
        self.sample(1)
        for _ in range(10):self.assertEqual(self.gate.snapshot('pair',now=.1)['forward']['count'],1)
        self.assertEqual(self.gate.snapshot('pair',now=4)['forward']['count'],0)

    def test_new_connection_discards_old_clock_watermark_and_extreme_values_fail_closed(self):
        self.sample(10000)
        self.gate.clear()
        self.assertEqual(self.sample(1)['count'],1)
        self.s['entry_forward_confirmation_mode']='equal'
        self.assertFalse(reached(self.s,'forward',1e200,1))


class ConfirmationEngineTests(unittest.TestCase):
    def setUp(self):
        tmp=tempfile.TemporaryDirectory();self.addCleanup(tmp.cleanup)
        self.store=Store(Path(tmp.name)/'confirm.db');self.addCleanup(self.store.close)
        self.c=native_config();self.c['strategy'].update(direction_mode='auto',entry_lower_usd=-5,
            entry_forward_confirmation_ticks=3,entry_reverse_confirmation_ticks=2)
        self.p=equal_lot_plan(self.c,specs());self.e=Engine(self.store,PaperBroker())
        self.e.entry_observations=EntryObservations();self.e.start(self.c)
        self.base=quote(self.c)['time_ms']

    def q(self,n,reverse=False):
        q=quote(self.c,bid=90 if reverse else 110,ask=90.1 if reverse else 110.1,mt_bid=99.9,mt_ask=100)
        for leg in ('binance','mt5'):q[leg]['source_time_ms']=self.base+n
        return q

    def test_no_dispatch_until_three_distinct_quotes_and_diagnostics_capture_count(self):
        q=self.q(1)
        self.e.tick(self.c,self.p,q)
        for _ in range(5):self.e.tick(self.c,self.p,q)
        self.assertFalse(self.e.state['orders'])
        row=self.e.entry_observations.snapshot()['episodes'][-1]
        self.assertEqual(row['code'],'WAIT_CONFIRMATION')
        self.assertEqual(row['latest']['confirmation']['count'],1)
        self.e.tick(self.c,self.p,self.q(2));self.assertFalse(self.e.state['orders'])
        self.e.tick(self.c,self.p,self.q(3));self.assertEqual(len(self.e.state['orders']),2)
        self.assertEqual(self.e.active()[0]['direction'],'forward')
        self.assertEqual(self.e.entry_confirmations.snapshot(pair_key(self.c))['forward']['count'],0)

    def test_reverse_uses_lower_bound_and_its_own_required_count(self):
        self.e.tick(self.c,self.p,self.q(1,True));self.assertFalse(self.e.state['orders'])
        self.e.tick(self.c,self.p,self.q(2,True));self.assertEqual(self.e.active()[0]['direction'],'reverse')

    def test_pause_and_stale_ticks_reset_without_platform_calls(self):
        self.e.tick(self.c,self.p,self.q(1))
        self.e.pause();self.e.start(self.c)
        self.e.tick(self.c,self.p,self.q(1))
        self.assertEqual(self.e.entry_confirmations.snapshot(pair_key(self.c))['forward']['count'],0)
        self.e.tick(self.c,self.p,self.q(2))
        stale=self.q(3)
        for leg in ('binance','mt5'):stale[leg]['observed_ms']=1
        self.e.tick(self.c,self.p,stale)
        self.assertFalse(self.e.state['orders'])
        self.e.tick(self.c,self.p,self.q(3))
        self.assertEqual(self.e.entry_confirmations.snapshot(pair_key(self.c))['forward']['count'],1)

    def test_equal_mode_is_used_for_direction_selection_and_favorable_miss(self):
        self.c['strategy'].update(entry_spread_usd=10,entry_forward_confirmation_mode='equal',entry_forward_confirmation_ticks=1)
        q=self.q(1);q['entry']=10.004
        self.assertEqual(entry_direction(self.c,q),'forward')
        q['entry']=10.01;self.assertIsNone(entry_direction(self.c,q))
        self.e.tick(self.c,self.p,q);self.assertFalse(self.e.state['orders'])

    def test_existing_position_exit_does_not_wait_for_entry_confirmations(self):
        self.e.open(self.c,self.p,self.q(1))
        q=quote(self.c,bid=102,ask=102.1,mt_bid=99.9,mt_ask=100)
        self.e.tick(self.c,self.p,q)
        self.assertEqual(self.e.state['groups'][0]['status'],'closed')

    def test_exact_mode_grid_confirms_the_next_grid_threshold_instead_of_base(self):
        self.c['strategy'].update(entry_spread_usd=10,entry_forward_confirmation_mode='equal',
            entry_forward_confirmation_ticks=2,grid_enabled=True,grid_spacing_usd=2,
            grid_max_adds=1,max_groups=2)
        self.e.open(self.c,self.p,self.q(1))
        self.e.state['last_open_ms']=0
        for i in (2,3):
            q=quote(self.c,bid=112,ask=112.1,mt_bid=99.9,mt_ask=100)
            for leg in ('binance','mt5'):q[leg]['source_time_ms']=self.base+i
            self.e.tick(self.c,self.p,q)
            self.assertEqual(len(self.e.active()),1 if i==2 else 2)
        self.assertEqual(self.e.active()[1]['entry_threshold'],12)

    def test_invalid_settings_fail_and_older_profiles_use_defaults(self):
        for field,value in [('entry_forward_confirmation_ticks',True),('entry_reverse_confirmation_ticks',1.5),
                            ('entry_forward_confirmation_mode','bad'),('entry_confirmation_decimals',9)]:
            c=copy.deepcopy(self.c);c['strategy'][field]=value
            with self.assertRaises(ValueError):validate(c)
        old=pair_profiles.capture(self.c)
        for k in DEFAULTS:old['strategy'].pop(k,None)
        restored=pair_profiles.activate(self.c,old)
        for k,v in DEFAULTS.items():self.assertEqual(restored['strategy'][k],v)
