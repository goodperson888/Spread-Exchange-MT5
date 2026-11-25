import copy
import unittest
from unittest.mock import patch
from entry_economics import estimate
from test_trading_engine import config, quote
from test_pair_combinations import native_config


class EntryEconomicsTests(unittest.TestCase):
    def setUp(self):
        self.c=config()
        self.c['costs'].update(usdt_usd=1,binance_taker_percent=.05,mt5_fee_auto=False,
                               mt5_commission_per_lot_side=4,binance_rebate_percent=0,mt5_rebate_percent=0)
        self.c['strategy'].update(take_contraction_usd=1,target_mode='contraction')
        self.p=dict(lots=.01,qty=1,contract=100)
        self.q=quote(self.c,bid=3500,ask=3500.2,mt_bid=3497,mt_ask=3497.1)

    def preview(self,**kwargs):
        return estimate(self.c,self.p,self.q,exchange_reference=dict(state='fallback',taker_percent=.05),fresh=True,**kwargs)

    def test_one_spread_unit_and_each_platform_open_close_fees(self):
        r=self.preview();row=r['rows'][0]
        self.assertEqual(r['usd_per_spread'],1)
        self.assertAlmostEqual(row['fees']['binance']['open'],1.75)
        self.assertAlmostEqual(row['fees']['binance']['close'],1.7501)
        self.assertAlmostEqual(row['fees']['mt5']['round_trip'],.08)
        self.assertAlmostEqual(row['total_fees'],3.5801)
        self.assertEqual(row['target_gross'],1)
        self.assertAlmostEqual(row['target_net'],-2.5801)
        self.assertAlmostEqual(row['break_even_contraction'],3.5801)
        self.assertEqual(row['fees']['binance']['basis']['state'],'fallback')
        self.assertEqual(row['fees']['mt5']['basis']['open']['source'],'manual')

    def test_lighter_reserve_rebates_and_spread_are_not_double_counted(self):
        self.c['pair']=dict(type='exchange_mt5',exchange='lighter')
        self.c['costs'].update(binance_rebate_percent=20,mt5_rebate_percent=50)
        row=self.preview()['rows'][0]
        rebate=3.5001*.2+.08*.5
        self.assertAlmostEqual(row['estimated_rebate'],rebate)
        self.assertAlmostEqual(row['buffer_usd'],.1)
        self.assertAlmostEqual(row['spread_loss'],.3)
        self.assertAlmostEqual(row['break_even_contraction'],3.5801-rebate+.1)
        self.assertAlmostEqual(row['immediate_round_trip_loss'],.3+3.5801-rebate)
        self.assertAlmostEqual(row['target_net'],1-3.5801+rebate-.1)

    def test_quantity_and_fx_are_applied_once(self):
        self.p=dict(lots=.02,qty=2,contract=100)
        self.q['usdt_usd']=.99
        r=self.preview();row=r['rows'][0]
        self.assertEqual(r['usd_per_spread'],2)
        self.assertEqual(row['target_gross'],2)
        self.assertAlmostEqual(row['fees']['binance']['round_trip'],3.5001*2*.99)
        self.assertAlmostEqual(row['fees']['mt5']['round_trip'],.16)

    def test_missing_quote_does_not_turn_unknown_exchange_costs_into_zero(self):
        r=estimate(self.c,self.p,None)
        self.assertFalse(r['quote_fresh'])
        self.assertIsNone(r['rows'][0]['total_fees'])
        self.assertIsNone(r['rows'][0]['target_net'])
        self.assertEqual(r['rows'][0]['fees']['mt5']['round_trip'],.08)

    def test_native_unequal_contracts_never_claim_fixed_spread_profit(self):
        c=native_config();p=dict(lots=.01,leg_contracts=dict(binance=100,mt5=10))
        r=estimate(c,p,quote(c),fresh=True)
        self.assertFalse(r['balanced'])
        self.assertEqual(r['exposures'],dict(binance=1,mt5=.1))
        self.assertIsNone(r['usd_per_spread'])
        self.assertIsNone(r['rows'][0]['target_gross'])
        self.assertIsNone(r['rows'][0]['break_even_contraction'])
        self.assertAlmostEqual(r['rows'][0]['fees']['binance']['round_trip'],.06)
        self.assertAlmostEqual(r['rows'][0]['fees']['mt5']['round_trip'],.1)

    def test_native_auto_uses_side_specific_historical_commissions(self):
        c=native_config();c['strategy']['direction_mode']='auto'
        c['costs'].update(mt5_fee_auto=True,mt5_short_fee_auto=True)
        p=dict(lots=.01,leg_contracts=dict(binance=100,mt5=100))
        refs={}
        for leg,symbol in [('binance',c['symbol']),('mt5',c['mt5']['symbol'])]:
            refs[leg]=dict(symbol=symbol,checked_ms=1000,profiles={
                key:dict(rate=rate,state='ready',samples=3,lots=.01)
                for key,rate in [('open_0',3),('close_1',4),('open_1',7),('close_0',8)]})
        r=estimate(c,p,quote(c),refs,now=1001,fresh=True)
        f,b=r['rows']
        self.assertEqual(f['direction'],'forward');self.assertEqual(b['direction'],'reverse')
        self.assertAlmostEqual(f['fees']['binance']['round_trip'],.15)
        self.assertAlmostEqual(f['fees']['mt5']['round_trip'],.07)
        self.assertAlmostEqual(b['fees']['binance']['round_trip'],.07)
        self.assertAlmostEqual(b['fees']['mt5']['round_trip'],.15)
        self.assertEqual(f['fees']['binance']['basis']['open']['source'],'history')
        stale=estimate(c,p,quote(c),refs,now=1001+86400000)
        self.assertEqual(stale['rows'][0]['fees']['binance']['basis']['open']['source'],'fallback')

    def test_absolute_target_uses_current_executable_entry_not_entry_threshold(self):
        self.c['strategy'].update(target_mode='absolute',exit_spread_usd=1)
        row=self.preview()['rows'][0]
        self.assertAlmostEqual(row['target_gross'],self.q['entry']-1)

    def test_preview_is_read_only_and_never_reads_files_or_platforms(self):
        before=copy.deepcopy((self.c,self.p,self.q))
        with patch('builtins.open',side_effect=AssertionError('unexpected I/O')):
            self.preview()
        self.assertEqual(before,(self.c,self.p,self.q))
        self.assertIsNone(estimate(self.c,None,self.q))
