"""Regression coverage for incomplete close receipts and linear futures PnL."""
import copy
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import _test_runtime
from server import TradingRuntime
from execution_diagnostics import build_report
from market_push import TickFreshness
from trading_brokers import PaperBroker
from trading_engine import Engine, valid_group_quote
from trading_store import Store
from test_trading_engine import config, quote


class CloseConfirmationTests(unittest.TestCase):
    def setUp(self):
        tmp=tempfile.TemporaryDirectory();self.addCleanup(tmp.cleanup)
        self.store=Store(Path(tmp.name)/'state.db');self.addCleanup(self.store.close)
        self.c=config();self.p=dict(lots=.01,qty=1,contract=100)
        self.b=PaperBroker();self.e=Engine(self.store,self.b);self.e.start(self.c)
        self.e.tick(self.c,self.p,quote(self.c));self.g=self.e.active()[0]
        self.g['status']='closing';self.submitted=[]
        def submit(o):
            self.submitted.append(copy.deepcopy(o))
            if o['leg']=='binance':
                return dict(status='done',raw_status='FILLED',qty=1,price=0,ticket='500')
            return PaperBroker.submit(self.b,o)
        self.b.submit=submit
        self.b.query=Mock(return_value=dict(status='done',qty=1,price=4306.1,ticket='500'))

    def test_missing_price_is_confirmed_before_pause_without_resubmission(self):
        self.e.close_group(self.g,quote(self.c))
        self.assertEqual(self.g['status'],'closed');self.assertTrue(self.e.state['enabled'])
        self.assertCountEqual([(o['leg'],o['requested']) for o in self.submitted],[('mt5',1),('binance',1)])
        self.assertEqual(self.b.query.call_count,1)
        self.assertEqual(self.b.query.call_args.args[0]['query_timeout_ms'],1000)
        order=self.e.state['orders'][-1]
        self.assertEqual(order['initial_result']['price'],0)
        self.assertFalse(order['initial_result']['valid'])
        self.assertEqual(order['reconcile']['last_query']['price'],4306.1)
        self.assertEqual(order['result']['qty'],1)
        self.assertFalse(any(e['kind']=='pause' for e in self.store.events()))

    def test_failed_query_pauses_and_later_confirmation_never_auto_resumes(self):
        self.b.query.side_effect=TimeoutError('offline')
        self.e.close_group(self.g,quote(self.c))
        self.assertFalse(self.e.state['enabled']);self.assertEqual(self.g['status'],'closing')
        self.assertEqual(self.e.state['last_pause']['reason_code'],'ORDER_UNCONFIRMED')
        self.b.query.side_effect=None
        self.e.state['orders'][-1]['reconcile']['next_ms']=0
        self.e.close_group(self.g,quote(self.c))
        self.assertEqual(self.g['status'],'closed');self.assertFalse(self.e.state['enabled'])
        self.assertEqual(len(self.submitted),2)

    def test_regressed_quantity_or_different_ticket_cannot_become_zero_fill(self):
        for reply in (dict(status='done',qty=0,price=0,ticket='500'),
                      dict(status='done',qty=1,price=4306,ticket='other')):
            with self.subTest(reply=reply):
                self.b.query.return_value=reply
                if not self.submitted:self.e.close_group(self.g,quote(self.c))
                else:
                    self.e.state['orders'][-1]['reconcile']['next_ms']=0
                    self.e.close_group(self.g,quote(self.c))
                self.assertTrue(self.e.uncertain(self.g));self.assertFalse(self.e.state['enabled'])
                self.assertEqual(len(self.submitted),2)
                self.assertFalse(self.e.state['orders'][-1]['reconcile']['last_query']['valid'])

    def test_valid_close_issues_no_additional_query(self):
        self.b.submit=lambda o:PaperBroker.submit(self.b,o)
        self.e.close_group(self.g,quote(self.c))
        self.assertEqual(self.g['status'],'closed');self.b.query.assert_not_called()

    def test_partial_or_nonterminal_receipt_does_not_use_fast_path(self):
        for raw,qty in (('PARTIALLY_FILLED',1),('FILLED',.5)):
            with self.subTest(raw=raw,qty=qty):
                self.b.submit=lambda o:dict(status='done',qty=qty,price=0,ticket='500',raw_status=raw)
                self.e.order(self.g,'binance','close',1,quote(self.c))
                self.b.query.assert_not_called()

    def test_successful_fast_confirmation_keeps_manual_pause(self):
        self.e.pause();self.e.close_group(self.g,quote(self.c))
        self.assertEqual(self.g['status'],'closed');self.assertFalse(self.e.state['enabled'])

    def test_parallel_close_uses_one_latest_cache_before_both_dispatches(self):
        initial=quote(self.c);fresh=quote(self.c,ask=4307.5)
        calls=[]
        def cached():
            calls.append(1)
            return fresh
        self.e.close_quote_provider=cached
        self.e.close_group(self.g,initial)
        binance=next(o for o in self.submitted if o['leg']=='binance')
        self.assertEqual(binance['reference'],4307.5)
        self.assertEqual(binance['quote_context']['binance']['ask'],4307.5)
        self.assertEqual(len(calls),1)
        self.assertTrue(all(o['quote_context']==binance['quote_context'] for o in self.submitted))
        self.assertEqual(self.g['execution_close']['signal_spread'],initial['exit'])

    def test_expired_cache_blocks_residual_retry_without_resending_mt5(self):
        initial=quote(self.c);stale=copy.deepcopy(initial)
        stale['binance']['time_ms']-=10000
        def submit(o):
            self.submitted.append(copy.deepcopy(o))
            if o['leg']=='binance' and len([x for x in self.submitted if x['leg']=='binance'])==1:
                return dict(status='done',qty=0,price=0,raw_status='EXPIRED')
            return PaperBroker.submit(self.b,o)
        self.b.submit=submit
        self.e.close_quote_provider=lambda:stale if self.submitted else initial
        self.e.close_group(self.g,initial)
        self.assertCountEqual([o['leg'] for o in self.submitted],['mt5','binance'])
        self.e.close_group(self.g,initial)
        self.assertEqual(len(self.submitted),2)
        self.e.close_quote_provider=lambda:initial
        self.e.close_group(self.g,initial)
        self.assertEqual(len(self.submitted),3)
        self.assertEqual(self.submitted[-1]['leg'],'binance')
        self.assertEqual(self.submitted[-1]['dispatch_mode'],'serial')
        self.assertEqual(self.g['status'],'closed')


class LinearPnlTests(unittest.TestCase):
    def setUp(self):
        tmp=tempfile.TemporaryDirectory();self.addCleanup(tmp.cleanup)
        self.store=Store(Path(tmp.name)/'state.db');self.addCleanup(self.store.close)
        self.c=config();self.c['costs']['usdt_usd']=1
        self.e=Engine(self.store,PaperBroker());self.e.start(self.c)
        self.p=dict(qty=1,lots=.01,contract=100)
        self.e.tick(self.c,self.p,quote(self.c));self.g=self.e.active()[0]

    def test_changing_fx_converts_profit_without_revaluing_full_notional(self):
        for o in self.e.state['orders']:o['result']['price']=4000
        q=quote(self.c,bid=3989.9,ask=3990,mt_bid=3991,mt_ask=3991.1)
        q['usdt_usd']=.99
        v=self.e.valuation(self.g,q)
        self.assertAlmostEqual(v['binance_pnl_usdt'],10)
        self.assertAlmostEqual(v['mt5_pnl_usd'],-9)
        self.assertAlmostEqual(v['gross'],.9)

    def test_fx_alone_cannot_pass_minimum_profit_check(self):
        for o in self.e.state['orders']:o['result']['price']=4000
        self.g['parameters'].update(target_mode='absolute',exit_spread_usd=10,min_net_profit_usd=.1,require_net_profit=True)
        q=quote(self.c,bid=3999.9,ask=4000,mt_bid=4000,mt_ask=4000.1)
        q.update(usdt_usd=.99,exit=-40)
        self.e.tick(self.c,self.p,q)
        self.assertEqual(self.g['status'],'open')
        self.assertEqual(self.g['valuation']['gross'],0)

    def test_reported_live_case_with_fx_drift_does_not_pass_net_profit_gate(self):
        self.g['costs'].update(binance_taker_percent=.04,binance_rebate_percent=40,mt5_commission_per_lot_side=4)
        for o in self.e.state['orders']:
            o['fx']=.99911908
            o['result'].update(price=4048.28 if o['leg']=='binance' else 4042,
                               fee=1.619312 if o['leg']=='binance' else .04)
        self.g['entry']=4048.28*.99911908-4042
        self.g['parameters'].update(take_contraction_usd=1.8,require_net_profit=True,min_net_profit_usd=0)
        q=quote(self.c,bid=4031.67,ask=4031.68,mt_bid=4026.92,mt_ask=4027.02)
        q.update(usdt_usd=.99895458,entry=4031.67*.99895458-4027.02,exit=4031.68*.99895458-4026.92)
        v=self.e.valuation(self.g,q)
        self.assertTrue(self.e.target(self.g,self.g['entry'],q['exit']))
        self.assertAlmostEqual(v['gross'],1.502646028,places=7)
        self.assertLess(v['net'],0)
        self.e.tick(self.c,self.p,q)
        self.assertEqual(self.g['status'],'open');self.assertEqual(len(self.e.state['orders']),2)

    def test_closed_profit_stays_fixed_during_later_cost_verification_and_restart(self):
        opens={'binance':4048.28,'mt5':4042}
        closes={'binance':4032.88,'mt5':4025.91}
        for o in self.e.state['orders']:
            o['result']['price']=opens[o['leg']];o['fx']=.99911908
        self.e.request_close(self.g['id'])
        self.e.broker.submit=lambda o:dict(status='done',qty=1,price=closes[o['leg']],ticket=o['id'])
        q=quote(self.c);q['usdt_usd']=.99895458
        self.e.close_group(self.g,q)
        v=self.g['valuation'];self.assertAlmostEqual(v['gross'],15.4*.99895458-16.09,places=7)
        q['usdt_usd']=1.05
        self.assertEqual(self.e.valuation(self.g,q)['gross'],v['gross'])
        # Old flat records migrate using their closing-order FX, with no reads.
        self.g.pop('valuation_fx');self.g['valuation']={'gross':999}
        self.e.save('test_legacy')
        e=Engine(self.store,PaperBroker())
        self.assertAlmostEqual(e.state['groups'][0]['valuation']['gross'],v['gross'])
        self.assertFalse(e.state['enabled']);self.assertEqual(len(e.state['orders']),4)

    def test_profit_exit_waits_if_latest_cache_no_longer_meets_minimum(self):
        signal=quote(self.c,bid=4302,ask=4302.1)
        worse=quote(self.c,bid=4306,ask=4306.1)
        self.e.close_quote_provider=lambda:worse
        self.e.tick(self.c,self.p,signal)
        self.assertEqual(self.g['status'],'open')
        self.assertEqual(len(self.e.state['orders']),2)
        self.assertNotIn('execution_close',self.g)
        self.assertTrue(self.e.state['enabled'])
        self.assertEqual(next(e for e in build_report(self.e.state,self.store.events())['events']
                              if e['kind']=='exit_deferred')['data']['reason_code'],'TARGET_DEFERRED')
        self.e.close_quote_provider=lambda:signal
        self.e.tick(self.c,self.p,signal)
        self.assertEqual(self.g['status'],'closed')

    def test_profit_decline_after_first_close_never_strands_remaining_leg(self):
        signal=quote(self.c,bid=4302,ask=4302.1)
        worse=quote(self.c,bid=4310,ask=4310.1)
        def cached():
            return worse if any(o['action']=='close' for o in self.e.state['orders']) else signal
        self.e.close_quote_provider=cached
        sent=[]
        def submit(o):
            sent.append(copy.deepcopy(o))
            if o['leg']=='binance' and len([x for x in sent if x['leg']=='binance'])==1:
                return dict(status='done',qty=0,price=0,raw_status='EXPIRED')
            return PaperBroker().submit(o)
        self.e.broker.submit=submit
        self.e.tick(self.c,self.p,signal)
        self.assertEqual(self.g['status'],'closing')
        self.e.tick(self.c,self.p,signal)
        self.assertEqual(self.g['status'],'closed')
        self.assertLess(self.g['valuation']['net'],0)
        self.assertEqual(len(self.e.state['orders']),5)
        last=self.e.state['orders'][-1]
        self.assertEqual(last['reference'],4310.1)

    def test_slippage_over_budget_remains_completed_and_is_exported(self):
        self.e.request_close(self.g['id'])
        def slipped(o):
            result=PaperBroker.submit(self.e.broker,o)
            if o['leg']=='mt5':result['price']-=1
            return result
        self.e.broker.submit=slipped
        self.e.close_group(self.g,quote(self.c))
        self.assertEqual(self.g['status'],'closed')
        report=build_report(self.e.state,self.store.events())
        mt5=next(o for o in report['orders'] if o['leg']=='mt5' and o['action']=='close')
        self.assertEqual(mt5['adverse_slippage'],1)
        self.assertTrue(mt5['slippage_exceeded'])
        self.assertEqual(mt5['result']['status'],'done')

    def test_exit_audit_records_trigger_profit_and_preserves_estimated_rebate_label(self):
        q=quote(self.c,bid=4302,ask=4302.1)
        self.e.tick(self.c,self.p,q)
        self.assertEqual(self.g['status'],'closed')
        report=build_report(self.e.state,self.store.events())
        row=report['groups'][0]
        self.assertEqual(row['reason_code'],'TARGET_REACHED')
        self.assertGreaterEqual(row['exit_decision']['estimated_net'],row['exit_decision']['min_net_profit'])
        self.assertEqual(row['exit_decision']['contraction'],self.g['entry']-q['exit'])
        self.assertEqual(row['valuation']['pnl_schema_version'],2)


class CachedRuntimeQuoteTests(unittest.TestCase):
    def test_cache_read_never_invokes_terminal_or_rest_and_preserves_tick_age(self):
        c=config();r=TradingRuntime.__new__(TradingRuntime);r.config=c
        r.quote=quote(c);r.pump=Mock();r.pump.read.return_value=(r.quote,1,'')
        r.push_bridge=None;r.market_stream=Mock();r.market_stream.quote.return_value=None
        r.terminal=Mock();r.binance=Mock();r.mt5_freshness=TickFreshness()
        r.quote['mt5']['time_ms']-=10000
        q=r._cached_close_quote()
        self.assertEqual(q['mt5']['time_ms'],r.quote['mt5']['time_ms'])
        self.assertFalse(valid_group_quote(q,q['key'],c['strategy']))
        r.terminal.call.assert_not_called();r.binance.quote.assert_not_called()
        r.market_stream.quote.assert_called_once_with(max_age_ms=c['strategy']['max_quote_age_ms'])
        r.quote['key']='different-terminal'
        self.assertIsNone(r._cached_close_quote())
