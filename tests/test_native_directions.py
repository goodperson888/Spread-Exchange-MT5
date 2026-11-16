import copy
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from trading_brokers import PaperBroker
from trading_config import validate, pair_key
from trading_engine import Engine
from trading_store import Store
from pair_instruments import equal_lot_plan, direction_quote, entry_direction
from execution_diagnostics import build_report
from exit_observations import ExitObservations
from test_trading_engine import config, quote
from test_pair_combinations import native_config, specs


class DirectionEngineTests(unittest.TestCase):
    def setUp(self):
        tmp=tempfile.TemporaryDirectory();self.addCleanup(tmp.cleanup)
        self.store=Store(Path(tmp.name)/'direction.db');self.addCleanup(self.store.close)
        self.c=native_config();self.c['strategy'].update(direction_mode='auto',entry_lower_usd=-5,mt5_lots=.01)
        self.p=equal_lot_plan(self.c,specs());self.b=PaperBroker();self.e=Engine(self.store,self.b)
        self.e.start(self.c)

    def market(self,a=90,b=100):
        return quote(self.c,bid=a,ask=a+.1,mt_bid=b-.1,mt_ask=b)

    def opened(self):
        self.e.tick(self.c,self.p,self.market());return self.e.active()[0]

    def test_reverse_opens_at_actual_ask_bid_and_owns_both_sides(self):
        q=self.market();before=copy.deepcopy(q);g=self.opened();orders=self.e.state['orders']
        self.assertEqual(g['direction'],'reverse');self.assertEqual(g['status'],'open')
        self.assertEqual([o['native_position_side'] for o in orders],[0,1])
        self.assertEqual([o['reference'] for o in orders],[90.1,99.9])
        self.assertEqual([o['requested'] for o in orders],[.01,.01])
        self.assertAlmostEqual(g['entry'],9.8);self.assertEqual(g['entry_threshold'],5)
        self.assertEqual(g['key'],pair_key(self.c));self.assertEqual(q,before)

    def test_reverse_pnl_respects_each_contract_and_real_closing_books(self):
        g=self.opened();v=self.e.valuation(g,self.market(93,98))
        self.assertAlmostEqual(v['gross'],3.09);self.assertAlmostEqual(v['fees'],.08)
        self.assertAlmostEqual(v['estimated_exit_fee'],.08);self.assertAlmostEqual(v['net'],2.96)

    def test_reverse_profit_closes_original_tickets_and_keeps_direction(self):
        g=self.opened();openings={o['leg']:o for o in self.e.state['orders']}
        self.e.tick(self.c,self.p,self.market(93,98))
        self.assertEqual(g['status'],'closed');self.assertEqual(g['direction'],'reverse')
        closes={o['leg']:o for o in self.e.state['orders'] if o['action']=='close'}
        self.assertEqual(closes['binance']['reference'],93)
        self.assertEqual(closes['mt5']['reference'],98)
        for leg,o in closes.items():
            self.assertEqual(o['opening_id'],openings[leg]['id'])
            self.assertEqual(o['native_position_side'],openings[leg]['native_position_side'])
        self.assertAlmostEqual(g['exit'],5);self.assertAlmostEqual(g['valuation']['gross'],3.09)

    def test_upper_selects_forward_and_neutral_selects_neither(self):
        self.assertIsNone(entry_direction(self.c,self.market(100,100)))
        self.e.tick(self.c,self.p,self.market(110,100));g=self.e.active()[0]
        self.assertEqual(g['direction'],'forward')
        self.assertEqual([o['native_position_side'] for o in self.e.state['orders']],[1,0])

    def test_active_reverse_blocks_opposite_signal_even_if_exit_never_passes(self):
        g=self.opened();g['parameters']['take_contraction_usd']=1000
        self.c['strategy'].update(max_groups=3,cooldown_seconds=1)
        self.e.state['last_open_ms']=0;self.e.tick(self.c,self.p,self.market(110,100))
        self.assertEqual(len(self.e.state['groups']),1);self.assertEqual(g['status'],'open')

    def test_flat_can_choose_forward_after_confirmed_reverse_close_and_cooldown(self):
        g=self.opened();self.e.request_close(g['id']);self.e.close_group(g,self.market())
        self.e.start(self.c);self.e.state['last_open_ms']=0;self.e.tick(self.c,self.p,self.market(110,100))
        self.assertEqual([g['direction'] for g in self.e.state['groups']],['reverse','forward'])

    def test_reverse_grid_adds_only_in_locked_direction_at_lower_levels(self):
        self.c['strategy'].update(grid_enabled=True,grid_max_adds=2,max_groups=3,grid_spacing_usd=2)
        g=self.opened();g['parameters']['take_contraction_usd']=1000
        self.e.state['last_open_ms']=0;self.e.tick(self.c,self.p,self.market(94,100))
        self.assertEqual(len(self.e.active()),1)
        self.e.tick(self.c,self.p,self.market(92,100))
        self.assertEqual(len(self.e.active()),2)
        self.assertEqual(self.e.active()[1]['entry_threshold'],7)
        self.assertTrue(all(g['direction']=='reverse' for g in self.e.active()))

    def test_second_leg_refresh_and_close_refresh_preserve_reverse(self):
        self.e.quote_provider=lambda:self.market(90.2,100)
        g=self.opened();self.assertAlmostEqual(self.e.state['orders'][1]['reference'],99.9)
        self.e.close_quote_provider=lambda:self.market(93,98)
        self.e.request_close(g['id']);self.e.close_group(g,self.market())
        self.assertEqual(g['status'],'closed');self.assertAlmostEqual(g['exit'],5)

    def test_partial_reverse_second_leg_unwinds_only_confirmed_quantities(self):
        calls=[]
        def submit(order):
            calls.append(copy.deepcopy(order));r=PaperBroker.submit(self.b,order)
            if order['action']=='open' and order['leg']=='mt5':r['qty']=.005
            return r
        self.b.submit=submit;self.e.tick(self.c,self.p,self.market());g=self.e.state['groups'][0]
        self.assertEqual(g['status'],'closed');self.assertFalse(self.e.state['enabled'])
        closes=[o for o in calls if o['action']=='close']
        self.assertAlmostEqual(sum(o['requested'] for o in closes if o['leg']=='binance'),.01)
        self.assertAlmostEqual(sum(o['requested'] for o in closes if o['leg']=='mt5'),.005)
        self.assertTrue(all(o['native_position_side']==(0 if o['leg']=='binance' else 1) for o in closes))

    def test_unknown_reverse_is_queried_without_duplicate_or_direction_change(self):
        self.b.submit=Mock(return_value=dict(status='unknown',qty=0,price=0))
        self.e.tick(self.c,self.p,self.market());g=self.e.active()[0]
        self.e.tick(self.c,self.p,self.market(110,100))
        self.assertEqual(self.b.submit.call_count,1);self.assertEqual(g['direction'],'reverse')

    def test_reverse_absolute_exit_uses_canonical_greater_than_target(self):
        self.c['strategy'].update(target_mode='absolute',exit_spread_usd=-4,require_net_profit=False)
        g=self.opened();self.e.tick(self.c,self.p,self.market(94,100));self.assertEqual(g['status'],'open')
        self.e.tick(self.c,self.p,self.market(96,100));self.assertEqual(g['status'],'closed')
        self.assertAlmostEqual(g['exit_decision']['signal_spread'],4)

    def test_observation_agrees_with_reverse_absolute_exit(self):
        self.c['strategy'].update(target_mode='absolute',exit_spread_usd=-4,require_net_profit=False)
        observer=ExitObservations();self.e.exit_observations=observer;g=self.opened()
        self.e.tick(self.c,self.p,self.market(94,100))
        rows=observer.snapshot({g['id']:'G1'})['episodes'];sample=rows[-1]['latest']
        self.assertFalse(sample['target_reached']);self.assertEqual(sample['target_exit_spread'],4)

    def test_reboot_preserves_original_direction_and_blocks_new_entries(self):
        self.opened();restored=Engine(self.store,self.b)
        self.assertTrue(restored.state['recovery']);self.assertFalse(restored.state['enabled'])
        self.assertEqual(restored.active()[0]['direction'],'reverse')
        restored.request_close(restored.active()[0]['id']);restored.close_group(restored.active()[0],self.market())
        self.assertEqual(restored.state['groups'][0]['status'],'closed')

    def test_missing_direction_on_old_groups_stays_forward(self):
        self.e.tick(self.c,self.p,self.market(110,100));g=self.e.active()[0];g.pop('direction')
        self.c['strategy']['direction_mode']='reverse'
        self.e.request_close(g['id']);self.e.close_group(g,self.market(110,100))
        self.assertEqual(g['status'],'closed')
        self.assertTrue(all(o['native_position_side']==(1 if o['leg']=='binance' else 0) for o in self.e.state['orders']))

    def test_positive_lower_bound_does_not_require_zero_center(self):
        self.c['strategy'].update(entry_spread_usd=10,entry_lower_usd=6)
        validate(self.c);self.e.tick(self.c,self.p,self.market(105,100));g=self.e.active()[0]
        self.assertEqual(g['direction'],'reverse');self.assertEqual(g['entry_threshold'],-6)
        self.assertAlmostEqual(g['entry'],-5.2)

    def test_validation_rejects_bad_modes_and_overlapping_auto_thresholds(self):
        for changes in (dict(direction_mode='x'),dict(entry_lower_usd=True),dict(entry_lower_usd=float('nan')),
                        dict(entry_lower_usd=5),dict(entry_lower_usd=10)):
            c=copy.deepcopy(self.c);c['strategy'].update(changes)
            with self.assertRaises(ValueError):validate(c)
        c=copy.deepcopy(self.c);c['strategy'].update(entry_spread_usd=-3,entry_lower_usd=-10);validate(c)

    def test_exchange_direction_stays_fixed_even_if_native_mode_was_saved(self):
        c=config();c['strategy']['direction_mode']='auto'
        self.assertEqual(entry_direction(c,quote(c,bid=10,ask=11,mt_bid=100,mt_ask=101)),'forward')
        with self.assertRaises(ValueError):self.e.open(c,self.p,quote(c),direction='reverse')

    def test_quote_projection_is_idempotent_and_never_swaps_books(self):
        g={'pair':self.c['pair'],'direction':'reverse'};q=self.market();r=direction_quote(q,g)
        self.assertEqual(r['entry'],-q['exit']);self.assertEqual(r['exit'],-q['entry'])
        self.assertIs(r['binance'],q['binance']);self.assertIs(direction_quote(r,g),r)
        self.assertIsNone(direction_quote({'key':q['key']},g)['entry'])

    def test_reverse_basket_closes_all_members_with_their_locked_sides(self):
        self.c['strategy'].update(exit_mode='basket',max_groups=2)
        self.opened();self.e.state['last_open_ms']=0;self.opened()
        self.assertEqual(len(self.e.active()),2)
        self.e.tick(self.c,self.p,self.market(93,98))
        self.assertFalse(self.e.active());self.assertTrue(all(g['reason']=='整篮子止盈' for g in self.e.state['groups']))

    def test_reverse_stale_and_skewed_quotes_cannot_open(self):
        q=self.market();q['binance']['time_ms']-=20000
        self.e.tick(self.c,self.p,q);self.assertFalse(self.e.state['orders'])

    def test_reverse_net_profit_gate_is_still_required(self):
        self.c['strategy']['min_net_profit_usd']=100
        g=self.opened();self.e.tick(self.c,self.p,self.market(93,98))
        self.assertEqual(g['status'],'open');self.assertEqual(len(self.e.state['orders']),2)

    def test_diagnostics_retains_direction_and_thresholds_without_account(self):
        g=self.opened();report=build_report(self.e.state,[]);row=report['groups'][0]
        self.assertEqual(row['direction'],'reverse');self.assertEqual(row['parameters']['entry_lower_usd'],-5)
        self.assertEqual(row['parameters']['direction_mode'],'auto')
        self.assertNotIn('account',row)


class DirectionRuntimeTests(unittest.TestCase):
    def setUp(self):
        import test_native_pair_runtime as runtime_tests
        runtime_tests.NativePairRuntimeTests.setUp(self)
        self.g['direction']='reverse';self.g['entry']=-self.g['entry']
        self.r.config['strategy'].update(direction_mode='auto',entry_lower_usd=-5)
        for leg,o in self.opens.items():
            side=0 if leg=='binance' else 1
            o['native_position_side']=side
            self.snaps[o['symbol']]['positions'][0]['side']=side
            self.deals[o['symbol']][0]['type']=side
        self.r.snapshot=Mock(return_value={'ok':True})

    def native_call(self,*args,**kwargs):
        import test_native_pair_runtime as runtime_tests
        return runtime_tests.NativePairRuntimeTests.native_call(self,*args,**kwargs)

    def manual_close(self,*args,**kwargs):
        import test_native_pair_runtime as runtime_tests
        return runtime_tests.NativePairRuntimeTests.manual_close(self,*args,**kwargs)

    def test_reverse_reconcile_verifies_the_actual_saved_position_sides(self):
        self.r._native_reconcile();self.assertTrue(self.r.reconciled)
        self.snaps[self.opens['binance']['symbol']]['positions'][0]['side']=1
        with self.assertRaises(ValueError):self.r._native_reconcile()
        self.assertFalse(self.r.reconciled)

    def test_reverse_account_observer_keeps_healthy_owned_positions(self):
        self.observer.step('account');self.assertTrue(self.r.engine.state['enabled'])

    def test_reverse_manual_flat_backfills_both_sides_once(self):
        self.manual_close('binance');self.manual_close('mt5');self.observer.step('history')
        self.assertEqual(self.g['status'],'closed')
        rows=[o for o in self.r.engine.state['orders'] if o.get('source')=='external']
        self.assertEqual({o['leg']:o['native_position_side'] for o in rows},dict(binance=0,mt5=1))
        self.observer.step('history')
        self.assertEqual(len([o for o in self.r.engine.state['orders'] if o.get('source')=='external']),2)

    def test_reverse_liquidation_closes_remaining_original_short_position(self):
        self.manual_close('binance',reason=6);self.observer.step('history')
        self.assertEqual(self.g['status'],'unwinding')
        self.r.engine.tick(self.r.config,self.r.plan,self.r.quote)
        closes=[o for o in self.r.engine.state['orders'] if o['action']=='close' and o.get('source')!='external']
        self.assertEqual(len(closes),1);self.assertEqual(closes[0]['leg'],'mt5')
        self.assertEqual(closes[0]['native_position_side'],1)
        self.assertEqual(closes[0]['reference'],self.r.quote['mt5']['ask'])
        self.assertEqual(self.g['status'],'closed')

    def test_auto_margin_checks_both_directions_and_uses_larger_total(self):
        self.r.engine.state['groups']=[]
        def margin(command,**args):
            short=args['symbol']==self.r.config['pair']['short_symbol']
            return dict(required=(10 if short else 40) if args['native_position_side']==1 else (20 if short else 30),available=55)
        self.r.terminal.call.side_effect=margin
        result=self.r._native_margin();self.assertEqual(result['required'],60);self.assertFalse(result['ready'])
        self.assertEqual(self.r.terminal.call.call_count,4)
