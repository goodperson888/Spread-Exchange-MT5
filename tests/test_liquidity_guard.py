import copy
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock

from liquidity_guard import assess, sweep, safe_evidence
from trading_engine import Engine, stamp
from trading_brokers import PaperBroker
from trading_store import Store
from trading_config import validate
from entry_observations import EntryObservations
from exit_observations import ExitObservations
from execution_diagnostics import build_report
from test_trading_engine import config, quote


def book(q,bids=None,asks=None):
    b=q['binance'];return dict(received_ms=stamp(),
        bids=bids or [(b['bid'],.4),(b['bid']-.2,10)],
        asks=asks or [(b['ask'],.4),(b['ask']+.2,10)])


class DepthMathTests(unittest.TestCase):
    def setUp(self):
        self.c=config();self.c['costs'].update(mt5_commission_per_lot_side=0,usdt_usd=1)
        self.q=quote(self.c,bid=106,ask=106.1,mt_bid=100,mt_ask=100.2)

    def check(self,**changes):
        args=dict(book=book(self.q),q=self.q,qty=1,strategy=self.c['strategy'],action='open',
                  now=stamp(),threshold=5,costs=self.c['costs'])
        args.update(changes);return assess(**args)

    def test_sweep_uses_weighted_prices_and_tightens_limit_to_last_consumed_level(self):
        r=self.check();self.assertTrue(r['ready'])
        self.assertAlmostEqual(r['vwap'],105.88);self.assertEqual(r['levels'],2)
        self.assertAlmostEqual(r['executable_spread'],5.68)
        self.assertEqual(r['limit'],105.8);self.assertAlmostEqual(r['exit_vwap'],106.22)

    def test_sufficient_raw_quantity_can_fail_reserved_depth_on_either_side(self):
        for side in ('bids','asks'):
            b=book(self.q);b[side]=[(106 if side=='bids' else 106.1,1.2)]
            self.assertEqual(self.check(book=b)['code'],'LIQUIDITY_DEPTH')

    def test_threshold_must_survive_sweeping_and_fees_must_fit_target(self):
        self.assertEqual(self.check(threshold=5.7)['code'],'LIQUIDITY_SPREAD')
        c=copy.deepcopy(self.c['costs']);c['binance_taker_percent']=1
        r=self.check(costs=c);self.assertEqual(r['code'],'LIQUIDITY_PROFIT')
        self.assertLess(r['expected_net'],0)
        c['binance_rebate_percent']=100
        self.assertTrue(self.check(costs=c)['ready'])

    def test_cost_basis_explains_profit_block_and_rejects_private_fields(self):
        c={**self.c['costs'],'binance_taker_percent':.05}
        r=self.check(costs=c,mt5_open_fee=4,mt5_close_fee=6)
        self.assertEqual(r['exchange_taker_percent'],.05)
        self.assertAlmostEqual(r['estimated_mt5_fees'],.1)
        self.assertAlmostEqual(r['estimated_round_trip_fees'],r['estimated_exchange_fees']+.1)
        r['fee_basis']=dict(state='fallback',exchange='lighter',taker_percent=.05,
                            error_code='UNITS_UNVERIFIED',account='PRIVATE',raw='PRIVATE')
        safe=safe_evidence(r)
        self.assertEqual(safe['fee_basis']['taker_percent'],.05)
        self.assertNotIn('PRIVATE',str(safe))
        self.assertAlmostEqual(safe['expected_net'],safe['expected_gross']-
                               safe['estimated_round_trip_fees']+safe['estimated_rebate']-safe['buffer_usd'])

    def test_absolute_target_fx_and_per_ounce_buffer_are_not_notional_profit(self):
        s={**self.c['strategy'],'target_mode':'absolute','exit_spread_usd':5,'liquidity_buffer_usd':.7}
        r=self.check(strategy=s);self.assertEqual(r['code'],'LIQUIDITY_PROFIT')
        self.assertAlmostEqual(r['expected_gross'],.68)
        q={**self.q,'usdt_usd':.99}
        self.assertEqual(self.check(q=q)['code'],'LIQUIDITY_SPREAD')

    def test_missing_stale_misaligned_or_corrupt_books_fail_closed(self):
        bad=[None,{**book(self.q),'received_ms':stamp()-4000},
             {**book(self.q),'bids':[(106,float('nan'))]}]
        for b in bad:self.assertFalse(self.check(book=b)['ready'])
        q=copy.deepcopy(self.q);q['mt5']['time_ms']-=2500
        self.assertEqual(self.check(q=q)['code'],'LIQUIDITY_UNAVAILABLE')

    def test_close_never_reads_entry_cost_forecast_and_export_drops_arbitrary_fields(self):
        r=self.check(action='close',costs=None,threshold=None)
        self.assertTrue(r['ready']);self.assertAlmostEqual(r['vwap'],106.22)
        self.assertNotIn('secret',safe_evidence({**r,'secret':'private'}))

    def test_config_rejects_invalid_reserve_and_buffer(self):
        for field,value in [('liquidity_max_usage_percent',0),('liquidity_max_usage_percent',101),
                            ('liquidity_buffer_usd',-1),('liquidity_buffer_usd',float('nan')),
                            ('liquidity_guard_enabled','true')]:
            c=copy.deepcopy(self.c);c['strategy'][field]=value
            with self.assertRaises(ValueError):validate(c)


class DepthEngineTests(unittest.TestCase):
    def setUp(self):
        tmp=tempfile.TemporaryDirectory();self.addCleanup(tmp.cleanup)
        self.store=Store(Path(tmp.name)/'orders.db');self.addCleanup(self.store.close)
        self.c=config();self.c['pair']={'type':'exchange_mt5','exchange':'lighter'};self.c['symbol']='XAU'
        self.c['costs'].update(mt5_commission_per_lot_side=0,usdt_usd=1)
        self.e=Engine(self.store,PaperBroker());self.e.entry_observations=EntryObservations();self.e.exit_observations=ExitObservations()
        self.plan=dict(qty=1,lots=.01,contract=100);self.q=quote(self.c)
        self.e.depth_provider=Mock(side_effect=lambda:book(self.q));self.e.start(self.c)

    def open(self):
        self.e.tick(self.c,self.plan,self.q)
        self.assertEqual(self.e.active()[0]['status'],'open');return self.e.active()[0]

    def test_missing_depth_skips_without_order_group_pause_or_rest(self):
        self.e.depth_provider.return_value=None;self.e.depth_provider.side_effect=None
        self.e.tick(self.c,self.plan,self.q)
        self.assertEqual(self.e.state['groups'],[]);self.assertEqual(self.e.state['orders'],[])
        self.assertTrue(self.e.state['enabled'])
        self.assertEqual(self.e.entry_observations.snapshot()['episodes'][-1]['code'],'LIQUIDITY_UNAVAILABLE')

    def test_open_exports_vwap_limits_and_cost_evidence(self):
        self.e.fee_reference_provider=lambda:dict(state='manual',exchange='lighter',taker_percent=self.c['costs']['binance_taker_percent'],secret='PRIVATE')
        g=self.open();o=self.e.state['orders'][0]
        self.assertEqual(o['limit'],4305.8);self.assertEqual(g['liquidity_open']['levels'],2)
        report=build_report(self.e.state,self.store.events())
        self.assertEqual(report['groups'][0]['liquidity_open']['code'],'LIQUIDITY_READY')
        self.assertIn('liquidity',report['orders'][0])
        self.assertEqual(report['groups'][0]['liquidity_open']['fee_basis']['state'],'manual')
        self.assertNotIn('PRIVATE',str(report))

    def blocked_profit(self):
        g=self.open();self.e.pause();self.q=quote(self.c,bid=4302,ask=4302.1)
        self.e.depth_provider.side_effect=lambda:book(self.q,asks=[(4302.1,.5)])
        self.e.tick(self.c,self.plan,self.q)
        self.assertEqual(g['status'],'open')
        self.assertFalse(any(o['action']=='close' for o in self.e.state['orders']))
        return g

    def test_manual_exit_continues_when_profit_exit_waits_for_depth(self):
        g=self.blocked_profit();self.e.request_close(g['id'])
        self.e.tick(self.c,self.plan,self.q);self.assertEqual(g['status'],'closed')

    def test_stop_exit_continues_when_profit_exit_waits_for_depth(self):
        g=self.blocked_profit();g['parameters'].update(group_loss_enabled=True,group_max_loss_usd=.1)
        self.q=quote(self.c,bid=4310,ask=4310.1)
        self.e.tick(self.c,self.plan,self.q);self.assertEqual(g['status'],'closed')

    def test_aggregate_sweep_cannot_reuse_same_liquidity_for_two_groups(self):
        g=self.open();other=copy.deepcopy(g);other['id']='other';self.e.state['groups'].append(other)
        for o in list(self.e.state['orders']):
            x=copy.deepcopy(o);x.update(id=x['id']+'x',group='other');self.e.state['orders'].append(x)
        self.q=quote(self.c,bid=4302,ask=4302.1)
        self.e.depth_provider.side_effect=lambda:book(self.q,asks=[(4302.1,2)])
        self.e.pause();self.e.tick(self.c,self.plan,self.q)
        self.assertEqual([x['status'] for x in (g,other)],['open','open'])
        self.assertEqual(g['liquidity_close']['aggregate_qty'],2)
        self.assertEqual(g['liquidity_close']['code'],'LIQUIDITY_DEPTH')

    def test_depth_vwap_can_block_profitable_top_quote_on_contraction(self):
        g=self.open();self.e.pause();self.q=quote(self.c,bid=4303.5,ask=4303.6)
        self.e.depth_provider.side_effect=lambda:book(self.q,asks=[(4303.6,.1),(4304,10)])
        self.e.tick(self.c,self.plan,self.q)
        self.assertEqual(g['status'],'open')
        self.assertFalse(any(o['action']=='close' for o in self.e.state['orders']))

    def test_group_not_at_profit_target_does_not_consume_another_groups_depth_budget(self):
        g=self.open();other=copy.deepcopy(g);other.update(id='waiting',entry=1)
        self.e.state['groups'].append(other)
        for o in list(self.e.state['orders']):
            x=copy.deepcopy(o);x.update(id=x['id']+'w',group='waiting');self.e.state['orders'].append(x)
        self.q=quote(self.c,bid=4302,ask=4302.1)
        self.e.depth_provider.side_effect=lambda:book(self.q,asks=[(4302.1,1.5)])
        self.e.pause();self.e.tick(self.c,self.plan,self.q)
        self.assertEqual(g['status'],'closed');self.assertEqual(other['status'],'open')
        self.assertEqual(g['liquidity_close']['aggregate_qty'],1)

    def test_partial_profit_close_keeps_reducing_remaining_leg_when_depth_disappears(self):
        class RejectOnce(PaperBroker):
            rejected=False
            def submit(broker,order):
                if order['leg']=='binance' and order['action']=='close' and not broker.rejected:
                    broker.rejected=True;return dict(status='done',qty=0,price=0,error='rejected')
                return super().submit(order)
        self.e.broker=RejectOnce();g=self.open();self.e.pause();self.q=quote(self.c,bid=4302,ask=4302.1)
        self.e.tick(self.c,self.plan,self.q)
        self.assertEqual(self.e.amounts(g)['mt5'],0);self.assertGreater(self.e.amounts(g)['binance'],0)
        self.e.depth_provider.side_effect=lambda:None
        self.e.tick(self.c,self.plan,self.q)
        self.assertEqual(g['status'],'closed')

    def test_basket_net_gate_uses_total_profit_instead_of_requiring_each_member_profit(self):
        g=self.open();g['parameters'].update(exit_mode='basket',take_contraction_usd=.1)
        other=copy.deepcopy(g);other.update(id='basket',entry=10)
        self.e.state['groups'].append(other)
        for o in list(self.e.state['orders']):
            x=copy.deepcopy(o);x.update(id=x['id']+'b',group='basket')
            if x['leg']=='binance':x['result']['price']+=5
            self.e.state['orders'].append(x)
        self.q=quote(self.c,bid=4306,ask=4306.1);self.e.pause()
        # The profitable member closes first; its realized profit must remain
        # in the basket gate when the losing, still-hedged member follows.
        self.e.state['groups'].reverse()
        self.e.tick(self.c,self.plan,self.q)
        self.assertEqual([x['status'] for x in (g,other)],['closed','closed'])

    def test_disabled_guard_leaves_existing_non_depth_execution_available(self):
        self.c['strategy']['liquidity_guard_enabled']=False
        self.e.depth_provider.side_effect=AssertionError('must not read depth')
        self.open();self.e.depth_provider.assert_not_called()

    def test_legacy_open_groups_keep_their_saved_exit_rules_without_a_new_guard(self):
        g=self.open();g['parameters'].pop('liquidity_guard_enabled')
        self.e.depth_provider.reset_mock();self.e.depth_provider.side_effect=AssertionError('legacy group')
        self.e.pause();self.q=quote(self.c,bid=4302,ask=4302.1)
        self.e.tick(self.c,self.plan,self.q)
        self.assertEqual(g['status'],'closed');self.e.depth_provider.assert_not_called()

    def test_normal_profit_exit_records_depth_and_uses_protected_limit(self):
        g=self.open();self.e.pause();self.q=quote(self.c,bid=4302,ask=4302.1)
        self.e.tick(self.c,self.plan,self.q)
        self.assertEqual(g['status'],'closed')
        close=next(o for o in self.e.state['orders'] if o['action']=='close' and o['leg']=='binance')
        self.assertAlmostEqual(close['limit'],4302.3)
        self.assertIn('liquidity',close)


if __name__=='__main__':unittest.main()
