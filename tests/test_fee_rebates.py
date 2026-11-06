import tempfile
import unittest
from pathlib import Path

from trading_engine import Engine
from trading_brokers import PaperBroker
from trading_store import Store
from trading_config import validate
from test_trading_engine import config, quote


class FeeRebateTests(unittest.TestCase):
    def fixture(self, rebate=0, **strategy):
        tmp=tempfile.TemporaryDirectory();self.addCleanup(tmp.cleanup)
        store=Store(Path(tmp.name)/'rebates.sqlite3');self.addCleanup(store.close)
        c=config();c['costs'].update(mt5_commission_per_lot_side=100,mt5_rebate_percent=rebate)
        c['strategy'].update(take_contraction_usd=1,require_net_profit=True,min_net_profit_usd=.1,**strategy)
        e=Engine(store,PaperBroker());e.start(c);plan=dict(qty=2,lots=.02,contract=100)
        e.tick(c,plan,quote(c));return c,e,plan,e.active()[0]

    def test_percentage_is_of_fees_and_both_entry_and_future_exit_are_counted(self):
        c,e,p,g=self.fixture();g['costs'].update(binance_taker_percent=.05,binance_rebate_percent=30,mt5_rebate_percent=50)
        for order in e.state['orders']:order['result']['fee']=10 if order['leg']=='binance' else 4
        v=e.valuation(g,quote(c))
        self.assertAlmostEqual(v['estimated_rebate_binance'],(10+v['estimated_exit_fee_binance'])*.3)
        self.assertAlmostEqual(v['estimated_rebate_mt5'],(4+v['estimated_exit_fee_mt5'])*.5)
        self.assertAlmostEqual(v['net'],v['net_before_rebate']+v['estimated_rebate'])
        self.assertAlmostEqual(v['net_before_rebate'],v['gross']-v['fees']-v['estimated_exit_fee']+v['carry'])

    def test_zero_and_legacy_missing_rates_preserve_the_old_result(self):
        c,e,p,g=self.fixture();q=quote(c);v=e.valuation(g,q)
        self.assertEqual(v['estimated_rebate'],0);self.assertEqual(v['net'],v['net_before_rebate'])
        g['costs'].pop('binance_rebate_percent');g['costs'].pop('mt5_rebate_percent')
        self.assertEqual(e.valuation(g,q),v)

    def test_order_fx_is_used_for_incurred_fees_and_quote_fx_for_future_fees(self):
        c,e,p,g=self.fixture();g['costs'].update(binance_rebate_percent=30,binance_taker_percent=.05)
        opening=next(o for o in e.state['orders'] if o['leg']=='binance')
        opening.update(fx=1.02);opening['result']['fee']=10
        q=quote(c);q['usdt_usd']=1.01;v=e.valuation(g,q)
        self.assertAlmostEqual(v['rebate_fee_base_binance'],10.2)
        self.assertAlmostEqual(v['estimated_rebate_binance'],(10.2+2*q['binance']['ask']*1.01*.0005)*.3)

    def test_negative_fee_and_carry_are_not_rebated(self):
        c,e,p,g=self.fixture(50);g['costs']['binance_rebate_percent']=100
        next(o for o in e.state['orders'] if o['leg']=='binance')['result']['fee']=-2
        g.update(live_mt5_swap_usd=-100,live_binance_funding_usd=50);g['mode']='live'
        v=e.valuation(g,quote(c));self.assertEqual(v['estimated_rebate_binance'],0)
        self.assertEqual(v['estimated_rebate_mt5'],2);self.assertEqual(v['carry'],-50)

    def test_actual_fee_replacement_recomputes_rebate_instead_of_accumulating_it(self):
        c,e,p,g=self.fixture(30);o=next(o for o in e.state['orders'] if o['leg']=='mt5')
        o['result']['fee']=2;first=e.valuation(g,quote(c))
        o['result']['fee']=3;second=e.valuation(g,quote(c))
        self.assertAlmostEqual(second['estimated_rebate']-first['estimated_rebate'],.3)
        self.assertAlmostEqual(second['net']-first['net'],-.7)
        self.assertEqual(second,e.valuation(g,quote(c)))

    def test_rebate_can_allow_profit_exit_that_is_blocked_without_it(self):
        for rate,closed in ((0,False),(50,True)):
            c,e,p,g=self.fixture(rate);e.pause();q=quote(c,bid=4304,ask=4304.1)
            v=e.valuation(g,q);self.assertLess(v['net_before_rebate'],0)
            if rate:self.assertGreater(v['net'],.1)
            e.tick(c,p,q);self.assertEqual(g['status']=='closed',closed)
            if closed:
                g['costs_verified']=True
                v=e.valuation(g,q);self.assertTrue(v['estimated'])
                self.assertEqual(v['estimated_exit_fee'],0);self.assertGreater(v['estimated_rebate'],0)

    def test_profit_exit_still_requires_price_spread_condition(self):
        c,e,p,g=self.fixture(50);g['parameters']['take_contraction_usd']=4;e.pause()
        q=quote(c,bid=4304,ask=4304.1);self.assertGreater(e.valuation(g,q)['net'],.1)
        e.tick(c,p,q);self.assertEqual(g['status'],'open')

    def test_basket_uses_combined_profit_including_each_groups_rebate(self):
        for rate,closed in ((0,False),(50,True)):
            c,e,p,g=self.fixture(rate,exit_mode='basket',max_groups=3)
            c['costs']['mt5_commission_per_lot_side']=150;g['costs']['mt5_commission_per_lot_side']=150
            e.state['last_open_ms']=0;e.tick(c,p,quote(c,bid=4307,ask=4307.1))
            self.assertEqual(len(e.active()),2);e.pause();q=quote(c,bid=4304,ask=4304.1)
            values=[e.valuation(x,q) for x in e.active()]
            self.assertLess(sum(v['net_before_rebate'] for v in values),0)
            if rate:self.assertGreater(sum(v['net'] for v in values),.1)
            e.tick(c,p,q);self.assertEqual(all(x['status']=='closed' for x in e.state['groups']),closed)

    def test_rates_cannot_leak_from_changed_main_config_into_existing_groups(self):
        c,e,p,g=self.fixture(max_groups=3);c['costs'].update(binance_rebate_percent=30,mt5_rebate_percent=50)
        self.assertEqual(e.valuation(g,quote(c))['estimated_rebate'],0)
        e.state['last_open_ms']=0;e.tick(c,p,quote(c,bid=4307,ask=4307.1))
        self.assertEqual(e.state['groups'][-1]['costs']['mt5_rebate_percent'],50)
        self.assertEqual(g['costs']['mt5_rebate_percent'],0)

    def test_validation_accepts_zero_to_one_hundred_only(self):
        for key in ('binance_rebate_percent','mt5_rebate_percent'):
            for bad in (-1,100.01,True,'30',float('nan'),float('inf')):
                c=config();c['costs'][key]=bad
                with self.subTest(key=key,value=bad),self.assertRaises(ValueError):validate(c)
            for good in (0,30.5,100):
                c=config();c['costs'][key]=good;validate(c)
