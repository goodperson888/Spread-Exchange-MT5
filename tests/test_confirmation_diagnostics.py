"""Deterministic execution recovery tests; no live accounts or network."""
import copy
import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch
from urllib.error import URLError

from execution_diagnostics import build_report
from execution_evidence import result_evidence
from trading_brokers import ApiError, Binance, PaperBroker
from trading_engine import Engine, stamp
from trading_store import Store
from test_trading_engine import config, quote


class Clock:
    def __init__(self): self.now=100.;self.base=stamp()
    def wall(self): return self.base+round((self.now-100)*1000)
    def advance(self,seconds): self.now+=seconds


class DelayedBroker(PaperBroker):
    def __init__(self,clock): self.clock=clock;self.submitted=[];self.budgets=[];self.unknown=False
    def submit(self,o):
        self.submitted.append(copy.deepcopy(o))
        if o['leg']=='binance' and o['action']=='open':
            self.clock.advance(.359)
            return dict(status='unknown',qty=0,price=0,error='private upstream error',
                        error_code='TIMEOUT',error_kind='timeout')
        return super().submit(o)
    def query(self,o):
        budget=o['query_timeout_ms'];self.budgets.append(budget)
        self.clock.advance(.01 if self.unknown else min(.380,budget/1000))
        if self.unknown or budget<380:
            return dict(status='unknown',qty=0,price=0,error_code='TIMEOUT',error_kind='timeout')
        return dict(status='done',qty=o['requested'],price=4306,raw_status='FILLED')


class ConfirmationTests(unittest.TestCase):
    def setUp(self):
        temp=tempfile.TemporaryDirectory();self.addCleanup(temp.cleanup)
        self.store=Store(Path(temp.name)/'state.db');self.addCleanup(self.store.close)
        self.clock=Clock();self.b=DelayedBroker(self.clock)
        self.c=config();self.c['execution']['entry_leg']='binance'
        self.c['strategy']['max_unhedged_ms']=1500
        self.p=dict(lots=.02,qty=2,contract=100)
        self.addCleanup(patch.stopall)
        patch('trading_engine.time.monotonic',lambda:self.clock.now).start()
        patch('trading_engine.stamp',self.clock.wall).start()
        self.e=Engine(self.store,self.b);self.e.start(self.c)
        self.e.tick(self.c,self.p,quote(self.c))
        self.g=self.e.active()[0];self.o=self.e.state['orders'][0]

    def test_normal_network_latency_confirms_within_original_window(self):
        deadline=self.e._entry_deadlines[self.g['id']]
        self.clock.advance(.1)
        self.e.tick(self.c,self.p,quote(self.c))
        self.assertEqual(self.b.budgets,[1000])
        self.assertLess(self.clock.now,deadline)
        self.assertEqual(self.g['status'],'open')
        self.assertEqual([(o['leg'],o['action']) for o in self.b.submitted],
                         [('binance','open'),('mt5','open')])
        self.assertFalse(self.e.state['enabled'])
        self.assertEqual(self.o['initial_result']['error_code'],'TIMEOUT')
        self.assertNotIn('error_code',self.o['result'])
        self.assertEqual(self.o['reconcile']['last_query']['raw_status'],'FILLED')
        report=build_report(self.e.state,self.store.events())
        self.assertNotIn('private upstream error',json.dumps(report))
        self.assertEqual(report['orders'][0]['initial_result']['error_kind'],'timeout')

    def test_late_confirmation_only_unwinds_and_never_resumes(self):
        self.clock.advance(1.6)
        self.e.tick(self.c,self.p,quote(self.c))
        self.assertEqual(self.g['status'],'closed')
        self.assertEqual([(o['leg'],o['action']) for o in self.b.submitted],
                         [('binance','open'),('binance','close')])
        self.assertFalse(self.e.state['enabled'])
        self.assertEqual(self.e.state['last_pause']['reason_code'],'ENTRY_WINDOW_EXPIRED')
        self.e.tick(self.c,self.p,quote(self.c))
        self.assertEqual(len(self.b.submitted),2)

    def test_attempt_count_keeps_original_deadline_and_recent_retry_is_bounded(self):
        self.b.unknown=True;deadline=self.e._entry_deadlines[self.g['id']]
        for _ in range(4):
            self.o['reconcile']['next_ms']=0
            self.assertFalse(self.e.resolve(self.g))
        self.assertEqual(self.e._entry_deadlines[self.g['id']],deadline)
        self.assertLess(self.clock.now,deadline)
        self.assertEqual(self.o['reconcile']['next_ms']-self.clock.wall(),500)
        self.assertEqual(len(self.b.submitted),1)
        self.clock.advance(11);self.o['reconcile']['next_ms']=0
        self.e.resolve(self.g)
        self.assertEqual(self.o['reconcile']['next_ms']-self.clock.wall(),2000)
        self.assertEqual(len(self.b.submitted),1)

    def test_query_budget_never_exceeds_remaining_entry_window(self):
        self.b.unknown=True
        self.clock.now=self.e._entry_deadlines[self.g['id']]-.075
        self.o['reconcile']['next_ms']=0;self.e.resolve(self.g)
        self.assertLessEqual(self.b.budgets[-1],75)
        self.assertGreater(self.b.budgets[-1],0)

    def test_invalid_ack_keeps_direct_fields_without_counting_them_as_fills(self):
        self.b.submit=Mock(return_value=dict(status='done',qty=2,price=0,retcode=10009))
        o=self.e.order(self.g,'mt5','open',2,quote(self.c))
        self.assertEqual(o['result']['status'],'unknown')
        self.assertEqual(o['result']['qty'],0)
        self.assertEqual(o['initial_result']['qty'],2)
        self.assertEqual(o['initial_result']['price'],0)
        self.assertFalse(o['initial_result']['valid'])
        self.assertEqual(o['initial_result']['error_code'],'INVALID_RESPONSE')
        report=build_report(self.e.state,[])
        self.assertFalse(report['orders'][-1]['initial_result']['valid'])

    def test_failed_query_after_partial_fill_keeps_timeout_and_known_quantity(self):
        self.o['result'].update(qty=.5,price=4306)
        self.b.unknown=True;self.o['reconcile']['next_ms']=0
        self.e.resolve(self.g)
        self.assertEqual(self.o['result']['qty'],.5)
        evidence=self.o['reconcile']['last_query']
        self.assertEqual(evidence['error_code'],'TIMEOUT')
        self.assertFalse(evidence['valid'])

    def test_repeated_unknown_reads_preserve_pause_time_and_throttle_events(self):
        self.b.unknown=True;pause=dict(self.e.state['last_pause'])
        for _ in range(20):
            self.o['reconcile']['next_ms']=0;self.e.state['alarm']='后台正在核对原订单'
            self.e.resolve(self.g);self.clock.advance(1)
        events=self.store.events(100)
        self.assertEqual(self.e.state['last_pause'],pause)
        self.assertEqual(sum(e['kind']=='pause' for e in events),1)
        retries=[e for e in events if e['kind'].startswith('order_reconcile_')]
        self.assertEqual(len(retries),2)  # First failed read and fast phase exhausted.
        self.assertEqual(self.o['reconcile']['attempts'],20)
        first=dict(self.o['reconcile']['first_unconfirmed_query'])
        self.clock.advance(31);self.o['reconcile']['next_ms']=0
        self.e.resolve(self.g)
        self.assertEqual(self.o['reconcile']['first_unconfirmed_query'],first)
        self.assertEqual(len(self.b.submitted),1)
        self.assertEqual(self.store.events(1)[0]['kind'],'order_reconcile_exhausted')

    def test_partial_fill_changes_are_durable_without_waiting_for_heartbeat(self):
        self.b.query=Mock(side_effect=[dict(status='pending',qty=.5,price=4306),
                                      dict(status='pending',qty=.6,price=4306)])
        for qty in (.5,.6):
            self.o['reconcile']['next_ms']=0
            self.e.resolve(self.g)
            saved=self.store.load()['orders'][0]
            self.assertEqual(saved['result']['qty'],qty)
        self.assertEqual(len(self.b.submitted),1)


class FilledPriceConfirmationTests(unittest.TestCase):
    def setUp(self):
        temp=tempfile.TemporaryDirectory();self.addCleanup(temp.cleanup)
        self.store=Store(Path(temp.name)/'state.db');self.addCleanup(self.store.close)
        self.clock=Clock();self.c=config();self.c['strategy']['max_unhedged_ms']=1500
        self.p=dict(lots=.02,qty=2,contract=100)
        self.addCleanup(patch.stopall)
        patch('trading_engine.time.monotonic',lambda:self.clock.now).start()
        patch('trading_engine.stamp',self.clock.wall).start()
        self.b=DelayedBroker(self.clock)
        def submit(order):
            self.b.submitted.append(copy.deepcopy(order))
            if order['leg']=='binance' and order['action']=='open':
                self.clock.advance(.375)
                return dict(status='done',qty=order['requested'],price=0,ticket='500',raw_status='FILLED')
            self.clock.advance(.328)
            return PaperBroker.submit(self.b,order)
        self.b.submit=submit
        self.e=Engine(self.store,self.b);self.e.start(self.c)

    def test_one_bounded_read_confirms_filled_price_before_pausing(self):
        self.e.tick(self.c,self.p,quote(self.c))
        self.assertEqual(self.e.active()[0]['status'],'open')
        self.assertTrue(self.e.state['enabled'])
        self.assertEqual(self.b.budgets,[1000])
        self.assertEqual([(o['leg'],o['action']) for o in self.b.submitted],[('binance','open'),('mt5','open')])
        self.assertFalse(any(e['kind']=='pause' for e in self.store.events(30)))

    def test_failed_confirmation_still_pauses_without_hedging_or_resubmitting(self):
        self.b.unknown=True
        self.e.tick(self.c,self.p,quote(self.c))
        self.assertFalse(self.e.state['enabled']);self.assertEqual(self.e.active()[0]['status'],'opening')
        self.assertEqual(len(self.b.submitted),1)
        self.assertEqual(self.e.state['last_pause']['reason_code'],'ORDER_UNCONFIRMED')

    def test_confirmation_past_original_deadline_never_opens_second_leg(self):
        def query(order):
            self.clock.advance(1.2)
            return dict(status='done',qty=2,price=4306,ticket='500')
        self.b.query=query
        self.e.tick(self.c,self.p,quote(self.c))
        self.assertFalse(self.e.state['enabled'])
        self.assertFalse(any(o['leg']=='mt5' and o['action']=='open' for o in self.b.submitted))
        self.assertEqual(sum(o['leg']=='binance' and o['action']=='open' for o in self.b.submitted),1)


class EvidenceTests(unittest.TestCase):
    def test_confirmation_filters_private_nested_fields_and_keeps_first_failure(self):
        secret='PRIVATE-DO-NOT-EXPORT'
        result=dict(status='unknown',qty=0,price=0,ticket=secret,deal=secret,
                    confirmation=dict(stage='fill_volume_mismatch',filled_lots=.02,deal_lots=.01,
                    account=secret,rows=[secret],reads=[dict(api='deals_ticket',status='unavailable',rows=0,
                    error_code=-4,error=secret),dict(api=secret,status='error',rows=0)]))
        state=dict(groups=[dict(id='g')],orders=[dict(id='o',group='g',result=result,
            reconcile=dict(first_unconfirmed_query=dict(time_ms=123,query_ms=16,**result)))])
        r=build_report(state,[]);e=r['orders'][0]['result']
        self.assertNotIn(secret,json.dumps(r))
        self.assertTrue(e['has_ticket']);self.assertTrue(e['has_deal'])
        self.assertEqual(e['confirmation']['reads'],[dict(api='deals_ticket',status='unavailable',rows=0,error_code=-4)])
        self.assertEqual(r['orders'][0]['reconcile']['first_unconfirmed_query']['time_ms'],123)

    def test_pause_audit_and_runtime_are_allowlisted_even_without_groups(self):
        secret='PRIVATE-DO-NOT-EXPORT'
        state=dict(enabled=False,recovery=False,alarm='订单状态未知 '+secret,
                   last_pause=dict(time_ms=5,reason_code='ORDER_UNCONFIRMED',reason_label=secret,
                                   enabled_before=True,keep_entry=True,account=secret))
        events=[dict(kind='pause',time=5,data=dict(reason='订单状态未知 '+secret)),
                dict(kind='start',time=1,data=dict(mode='live',key=secret)),dict(kind='boot',time=0,data=None)]
        r=build_report(state,events)
        self.assertEqual(r['schema_version'],2)
        self.assertEqual(r['runtime']['reason_code'],'ORDER_UNCONFIRMED')
        self.assertTrue(r['runtime']['last_pause']['enabled_before'])
        self.assertEqual([e['kind'] for e in r['events']],['pause','start','boot'])
        self.assertNotIn(secret,json.dumps(r))

    def test_malformed_and_private_error_fields_cannot_escape(self):
        secret='PRIVATE-DO-NOT-EXPORT'
        for code in (secret,{},[],True):
            r=result_evidence(dict(status={},raw_status={},error_code=code,error_kind=[],error=secret))
            self.assertEqual(r,{'error_kind':'unknown'})
            self.assertNotIn(secret,json.dumps(r))
        self.assertEqual(result_evidence({'error_code':-2013})['error_code'],-2013)

    def test_retry_export_keeps_safe_query_failure_before_success(self):
        s=dict(groups=[dict(id='private-group')],orders=[dict(id='private-order',group='private-group')])
        events=[dict(kind='order_reconcile_retry',time=10,data=dict(id='private-order',attempt=1,
                     query_ms=391,query_timeout_ms=1000,result=dict(status='unknown',error_code=-2013,
                     error_kind='exchange',error='PRIVATE-DO-NOT-EXPORT',ticket='PRIVATE-DO-NOT-EXPORT')))]
        r=build_report(s,events);d=r['events'][0]['data']
        self.assertEqual(d['result'],dict(status='unknown',error_code=-2013,error_kind='exchange',has_ticket=True))
        self.assertEqual(d['query_timeout_ms'],1000)
        self.assertNotIn('PRIVATE-DO-NOT-EXPORT',json.dumps(r))


class BinanceEvidenceTests(unittest.TestCase):
    def test_timeout_network_and_invalid_json_are_distinct(self):
        b=Binance()
        for cause,code in [(TimeoutError(),'TIMEOUT'),(URLError(TimeoutError()),'TIMEOUT'),
                           (URLError('private address'),'NETWORK')]:
            b.opener=Mock();b.opener.open.side_effect=cause
            with self.assertRaises(ApiError) as ctx: b.request('/test')
            self.assertEqual(ctx.exception.code,code)
        b.opener=Mock();b.opener.open.return_value.__enter__=Mock(return_value=io.StringIO('invalid'))
        b.opener.open.return_value.__exit__=Mock(return_value=False)
        with self.assertRaises(ApiError) as ctx: b.request('/test')
        self.assertEqual(ctx.exception.code,'INVALID_RESPONSE')

    def test_query_preserves_exchange_code_and_passes_timeout(self):
        b=Binance();b.request=Mock(side_effect=ApiError(-2013,'private upstream text'))
        r=b.query(dict(id='private-order',symbol='XAUUSDT',query_timeout_ms=950))
        self.assertEqual(r['status'],'unknown')
        self.assertEqual(result_evidence(r)['error_code'],-2013)
        self.assertEqual(r['error_kind'],'exchange')
        self.assertEqual(b.request.call_args.kwargs['timeout'],.95)
        self.assertFalse(b.request.call_args.kwargs['_time_retry'])

    def test_submit_keeps_rejection_distinct_from_uncertain_network_response(self):
        b=Binance();o=dict(action='open',symbol='XAUUSDT',requested=1,limit=4000)
        spec={'filters':{'PRICE_FILTER':{'tickSize':'0.01'}}}
        for code,uncertain,status in [(-2015,False,'done'),('TIMEOUT',True,'unknown')]:
            b.request=Mock(side_effect=ApiError(code,'private upstream text',uncertain))
            r=b.submit(o,spec)
            self.assertEqual(r['status'],status)
            self.assertEqual(result_evidence(r)['error_code'],code)
