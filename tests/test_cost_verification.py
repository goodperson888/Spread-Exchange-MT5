import copy
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import _test_runtime
from server import TradingRuntime
from trading_brokers import PaperBroker
from trading_engine import Engine
from trading_store import Store
from test_trading_engine import config, quote


class CostVerificationTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.store=Store(Path(self.temp.name)/'test.sqlite3');self.addCleanup(self.store.close)
        self.c=config();self.c['execution']['mode']='live';self.c['mt5']['adapter']='native'
        self.c['costs'].update(binance_taker_percent=.05,mt5_commission_per_lot_side=3)
        self.e=Engine(self.store,PaperBroker());self.e.start(self.c)
        p=dict(qty=2,lots=.02,contract=100);q=quote(self.c)
        self.e.tick(self.c,p,q);self.g=self.e.active()[0]
        self.e.request_close(self.g['id']);self.e.tick(self.c,p,q)
        self.assertEqual(self.g['status'],'closed')
        self.original_results=copy.deepcopy([o['result'] for o in self.e.state['orders']])
        self.original_net=self.g['valuation']['net']
        self.r=TradingRuntime.__new__(TradingRuntime)
        self.r.config=self.c;self.r.engine=self.e;self.r.quote=q
        self.r.binance=Mock();self.r.binance.trades.return_value=[dict(qty='2',commission='.1')]
        self.r.binance.commissions_usdt.side_effect=lambda fills:sum(float(x['commission']) for x in fills)
        self.r.binance.income.return_value=[]
        self.deals=[dict(comment=o['id'],volume=o['result']['qty']/100,commission=-.12,fee=0,
                         swap=-.2 if o['action']=='close' else 0)
                    for o in self.e.state['orders'] if o['leg']=='mt5']
        self.r.terminal=Mock();self.r.terminal.call.return_value=self.deals

    def assert_pending_without_overwriting_results(self):
        self.r._verify_closed_costs()
        self.assertFalse(self.g.get('costs_verified',False))
        self.assertIn('cost_verification_error',self.g)
        self.assertEqual([o['result'] for o in self.e.state['orders']],self.original_results)
        self.assertEqual(self.g['valuation']['net'],self.original_net)

    def test_empty_binance_history_keeps_estimates_and_retries_after_delay(self):
        self.r.binance.trades.return_value=[]
        self.assert_pending_without_overwriting_results()
        requests=self.r.binance.trades.call_count
        self.r._verify_closed_costs()
        self.assertEqual(self.r.binance.trades.call_count,requests)
        self.r.binance.trades.return_value=[dict(qty='1',commission='.05')]*2
        self.g['cost_check_ms']=0
        self.r._verify_closed_costs()
        self.assertTrue(self.g['costs_verified'])
        self.assertNotIn('cost_verification_error',self.g)
        self.assertAlmostEqual(self.g['valuation']['fees'],.44)
        self.assertAlmostEqual(self.g['mt5_swap_usd'],-.2)
        self.assertFalse(self.g['valuation']['estimated'])

    def test_partial_binance_history_is_not_verified(self):
        self.r.binance.trades.return_value=[dict(qty='1',commission='.05')]
        self.assert_pending_without_overwriting_results()

    def test_empty_mt5_history_does_not_partially_replace_binance_fees(self):
        self.r.terminal.call.return_value=[]
        self.assert_pending_without_overwriting_results()

    def test_partial_mt5_history_is_not_verified(self):
        self.deals[0]['volume']=.01
        self.assert_pending_without_overwriting_results()

    def test_funding_failure_leaves_all_commissions_unchanged(self):
        self.r.binance.income.side_effect=TimeoutError('funding history delayed')
        self.assert_pending_without_overwriting_results()

    def test_complete_zero_fee_records_are_valid(self):
        self.r.binance.trades.return_value=[dict(qty='2',commission='0')]
        for deal in self.deals:deal.update(commission=0,swap=0)
        self.r._verify_closed_costs()
        self.assertTrue(self.g['costs_verified'])
        self.assertEqual(self.g['valuation']['fees'],0)

    def test_another_account_or_paper_group_is_not_verified_using_current_connection(self):
        self.g['key']='another-account'
        self.r._verify_closed_costs()
        self.r.binance.trades.assert_not_called();self.r.terminal.call.assert_not_called()
        self.assertFalse(self.g.get('costs_verified',False))

    def test_old_group_fee_history_waits_until_other_paired_execution_settles(self):
        self.e.state['groups'].append(dict(id='pending-pair',status='opening'))
        self.r._verify_closed_costs()
        self.r.binance.trades.assert_not_called();self.r.terminal.call.assert_not_called()
        self.assertNotIn('cost_check_ms',self.g)

    def native_query(self):
        def call(command, **args):
            if command=='history': return [{**d,'comment':'broker-rewritten'} for d in self.deals]
            order=args['order'];saved=order['result']
            return dict(status='done',qty=saved['qty'],price=saved['price'],ticket=saved['ticket'],
                        fee_verified=True,fee=.04,swap=-.37 if order['action']=='close' else 0)
        self.r.terminal.call.side_effect=call

    def test_rewritten_comments_use_exact_original_order_for_fees_and_swap(self):
        self.native_query();self.r._verify_closed_costs()
        self.assertTrue(self.g['costs_verified']);self.assertEqual(self.g['cost_verification_stage'],'verified')
        self.assertAlmostEqual(self.g['mt5_swap_usd'],-.37)
        self.assertAlmostEqual(self.g['valuation']['fees'],.28)
        closes=[o for o in self.e.state['orders'] if o['leg']=='mt5' and o['action']=='close']
        self.assertEqual(closes[0]['result']['swap'],-.37)
        self.assertEqual(sum(c.args[0]=='query' for c in self.r.terminal.call.call_args_list),2)

    def test_exact_native_fallback_rejects_wrong_identity_quantity_price_and_unknown(self):
        base=dict(status='done',qty=2,price=1,ticket='foreign',fee_verified=True,fee=.04,swap=-.37)
        for key,value in (('ticket','foreign'),('qty',1),('price',0),('status','unknown'),
                          ('fee_verified',False),('swap',float('nan')),('fee',True)):
            with self.subTest(key=key):
                def call(command,**args):
                    if command=='history': return []
                    saved=args['order']['result']
                    return {**base,'ticket':saved['ticket'],'qty':saved['qty'],'price':saved['price'],key:value}
                self.g['cost_check_ms']=0;self.r.terminal.call.side_effect=call
                self.assert_pending_without_overwriting_results()
                self.assertEqual(self.g['cost_verification_stage'],'mt5_fees')

    def test_native_fallback_never_partially_applies_before_funding_is_verified(self):
        self.native_query();self.r.binance.income.side_effect=TimeoutError('private upstream')
        self.assert_pending_without_overwriting_results()
        self.assertEqual(self.g['cost_verification_stage'],'binance_funding')
        from execution_diagnostics import build_report
        report=build_report(self.e.state,self.store.events())
        self.assertEqual(report['groups'][0]['cost_verification_stage'],'binance_funding')
        self.assertEqual(report['events'][0]['kind'],'cost_verification_pending')
        self.assertNotIn('private upstream',str(report))


if __name__=='__main__':unittest.main()
