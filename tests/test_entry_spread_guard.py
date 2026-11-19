import copy
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from execution_diagnostics import build_report
from pair_instruments import equal_lot_plan
from trading_brokers import PaperBroker
from trading_config import validate
from trading_engine import Engine
from trading_store import Store
from test_trading_engine import config, quote
from test_pair_combinations import native_config, specs


class FillMovement(PaperBroker):
    def __init__(self, adverse):self.adverse=adverse
    def submit(self, order):
        r=super().submit(order)
        if order['action']=='open':
            # Worsen each opening leg by half the combined adverse movement.
            sell=order.get('native_position_side')==1 if order.get('platform')=='mt5' else order['leg']=='binance'
            r['price']+=(-1 if sell else 1)*self.adverse/2
        return r


class EntrySpreadGuardTests(unittest.TestCase):
    def setUp(self):
        tmp=tempfile.TemporaryDirectory();self.addCleanup(tmp.cleanup)
        self.store=Store(Path(tmp.name)/'guard.db');self.addCleanup(self.store.close)
        self.c=config();self.c['strategy'].update(entry_spread_guard_enabled=True,entry_spread_slippage_usd=.4)

    def run_open(self, movement=.6, c=None, q=None, plan=None):
        c=c or self.c;e=Engine(self.store,FillMovement(movement));e.start(c)
        e.tick(c,plan or dict(qty=2,lots=.02,contract=100),q or quote(c))
        return e,e.state['groups'][0]

    def test_excess_adverse_fill_pauses_and_closes_with_protective_budget(self):
        e,g=self.run_open()
        self.assertEqual(g['status'],'closed');self.assertFalse(e.state['enabled'])
        self.assertAlmostEqual(g['entry_spread_guard']['slippage'],-.6)
        closes=[o for o in e.state['orders'] if o['action']=='close']
        self.assertEqual(len(closes),2)
        self.assertTrue(all(o['slippage_mode']=='protective' for o in closes))
        self.assertTrue(all(o['slippage']==self.c['strategy']['unwind_slippage_usd'] for o in closes))
        report=build_report(e.state,self.store.events())
        self.assertEqual(report['groups'][0]['entry_spread_guard']['action'],'pause_and_protective_close')
        event=next(x for x in report['events'] if x['kind']=='entry_spread_guard_triggered')
        self.assertEqual(event['data']['reason_code'],'ENTRY_SPREAD_SLIPPAGE')
        self.assertEqual(event['data']['group'],'G1')

    def test_signal_is_baseline_even_when_well_above_configured_threshold(self):
        self.c['strategy']['entry_spread_usd']=1
        e,g=self.run_open()
        self.assertGreater(g['entry'],g['entry_threshold'])
        self.assertEqual(g['status'],'closed')
        self.assertAlmostEqual(g['entry_spread_guard']['signal'],5.7)

    def test_disabled_and_missing_switch_never_add_a_protective_close(self):
        for enabled in (False,None):
            with self.subTest(enabled=enabled):
                c=copy.deepcopy(self.c)
                if enabled is None:c['strategy'].pop('entry_spread_guard_enabled')
                else:c['strategy']['entry_spread_guard_enabled']=enabled
                e=Engine(self.store,FillMovement(.6));e.state['groups']=[];e.state['orders']=[];e.state['last_open_ms']=0
                e.state['recovery']=False;e.start(c);e.tick(c,dict(qty=2,lots=.02,contract=100),quote(c))
                self.assertEqual(e.active()[0]['status'],'open')
                self.assertTrue(e.state['enabled'])
                self.assertFalse(any(o['action']=='close' for o in e.state['orders']))

    def test_equal_limit_and_favorable_movement_are_allowed(self):
        for adverse in (.4,-2):
            with self.subTest(adverse=adverse):
                e=Engine(self.store,FillMovement(adverse));e.state['groups']=[];e.state['orders']=[];e.state['last_open_ms']=0
                e.state['recovery']=False;e.start(self.c);e.tick(self.c,dict(qty=2,lots=.02,contract=100),quote(self.c))
                self.assertEqual(e.active()[0]['status'],'open')
                self.assertNotIn('entry_spread_guard',e.active()[0])

    def test_reverse_normalizes_adverse_slippage_without_swapping_symbols(self):
        c=native_config();c['strategy'].update(self.c['strategy'])
        c['strategy'].update(direction_mode='auto',entry_lower_usd=-5,mt5_lots=.01)
        q=quote(c,bid=90,ask=90.1,mt_bid=99.9,mt_ask=100)
        e,g=self.run_open(c=c,q=q,plan=equal_lot_plan(c,specs()))
        self.assertEqual(g['direction'],'reverse');self.assertEqual(g['status'],'closed')
        self.assertAlmostEqual(g['entry_spread_guard']['slippage'],-.6)
        opening=[o for o in e.state['orders'] if o['action']=='open']
        self.assertEqual([o['native_position_side'] for o in opening],[0,1])

    def test_partial_first_leg_uses_existing_unwind_without_combined_fill_guard(self):
        class Partial(FillMovement):
            def submit(self,order):
                r=super().submit(order)
                if order['action']=='open':r['qty']/=2
                return r
        e=Engine(self.store,Partial(.6));e.start(self.c)
        e.tick(self.c,dict(qty=2,lots=.02,contract=100),quote(self.c))
        g=e.state['groups'][0]
        self.assertNotIn('entry_spread_guard',g)
        self.assertEqual(len([o for o in e.state['orders'] if o['action']=='open']),1)
        self.assertFalse(e.state['enabled'])

    def test_configuration_switch_and_limit_are_strict(self):
        for key,value in [('entry_spread_guard_enabled','true'),('entry_spread_slippage_usd',0),
                          ('entry_spread_slippage_usd',float('nan'))]:
            c=copy.deepcopy(self.c);c['strategy'][key]=value
            with self.assertRaises(ValueError):validate(c)

    def test_old_configuration_and_profile_do_not_inherit_an_enabled_guard(self):
        import _test_runtime
        import server
        import pair_profiles
        saved={'strategy':{'max_slippage_usd':1.2}}
        with patch.object(server,'read_json',return_value=saved):loaded=server.load_config()
        self.assertFalse(loaded['strategy']['entry_spread_guard_enabled'])
        self.assertEqual(loaded['strategy']['entry_spread_slippage_usd'],1.2)
        profile=pair_profiles.clean_profile(dict(name='legacy',type='exchange_mt5',
            short_symbol='XAUUSDT',long_symbol='XAUUSD'),self.c)
        profile['strategy'].pop('entry_spread_guard_enabled');profile['strategy'].pop('entry_spread_slippage_usd')
        activated=pair_profiles.activate(self.c,profile)
        self.assertFalse(activated['strategy']['entry_spread_guard_enabled'])
        self.assertEqual(activated['strategy']['entry_spread_slippage_usd'],self.c['strategy']['max_slippage_usd'])
