"""Platform reply evidence stays bounded, private, and outside extra I/O."""
import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch
from urllib.error import HTTPError

from execution_evidence import capture_receipt, result_evidence
from execution_diagnostics import build_report
from trading_brokers import ApiError, Binance
from trading_engine import Engine
from trading_store import Store
from test_confirmation_diagnostics import Clock, DelayedBroker
from test_trading_engine import config, quote


class ReceiptTests(unittest.TestCase):
    def test_zero_price_missing_quote_and_query_fill_remain_distinct(self):
        raw=dict(status='FILLED',orderId=123,clientOrderId='private',executedQty='1',avgPrice='0')
        initial=Binance.normalize(raw)
        self.assertEqual(initial['price'],0)
        e=initial['receipt']
        self.assertEqual(e['field_types']['cumQuote'],'missing')
        self.assertEqual(e['values']['avgPrice'],0)
        self.assertEqual(e['price_source'],'unavailable')
        query=Binance.normalize({**raw,'avgPrice':'4000','cumQuote':'4000','updateTime':123456})
        self.assertEqual(query['receipt']['values']['updateTime'],123456)
        self.assertEqual(initial['receipt']['values']['avgPrice'],0)
        self.assertEqual(query['receipt']['price_source'],'avgPrice')

    def test_quote_derived_price_keeps_original_fields_and_source(self):
        r=Binance.normalize(dict(status='FILLED',executedQty='2',avgPrice='0',cumQuote='8000'))
        self.assertEqual(r['price'],4000)
        self.assertEqual(r['receipt']['price_source'],'cumQuote')
        self.assertEqual(r['receipt']['values']['avgPrice'],0)
        self.assertEqual(r['receipt']['values']['cumQuote'],8000)

    def test_parse_failure_keeps_field_types_and_identity_for_original_query(self):
        for invalid,kind in [(None,'null'),({},'object'),([], 'array'),('private','string')]:
            r=Binance.normalize(dict(status='FILLED',orderId=123,executedQty='1',avgPrice=invalid))
            self.assertEqual(r['status'],'unknown')
            self.assertEqual(r['ticket'],'123')
            self.assertEqual(r['receipt']['field_types']['avgPrice'],kind)
            self.assertNotIn('avgPrice',r['receipt']['values'])
            self.assertNotIn('private',json.dumps(result_evidence(r)))
        for payload in (None,[],True):
            self.assertEqual(Binance.normalize(payload)['status'],'unknown')

    def test_nested_export_rechecks_allowlists_and_does_not_leak_payload(self):
        secret='PRIVATE-DO-NOT-EXPORT'
        receipt=capture_receipt('binance',dict(status='FILLED',executedQty='1',avgPrice='NaN',
            cumQuote='Infinity',orderId=secret,clientOrderId=secret,account=secret,msg=secret))
        receipt['values'].update(account=secret,side=secret,price=float('nan'))
        receipt['field_types'].update(api_key=secret,avgPrice=secret)
        receipt.update(request=secret,platform_id=secret,payload_type={})
        r=dict(status='done',qty=1,price=4000,receipt=receipt,precheck_receipt=receipt)
        state=dict(groups=[dict(id=secret)],orders=[dict(id=secret,group=secret,result=r,initial_result=r,
            reconcile=dict(last_query=r,first_unconfirmed_query=r,
                           first_cancel=dict(result=r),last_cancel=dict(result=r)))])
        report=build_report(state,[dict(kind='order_result',data=dict(id=secret,result=r))])
        encoded=json.dumps(report,allow_nan=False)
        self.assertNotIn(secret,encoded)
        exported=report['orders'][0]['result']['receipt']
        self.assertNotIn('avgPrice',exported['values'])
        self.assertNotIn('cumQuote',exported['values'])
        self.assertTrue(exported['has_orderId'])

    def test_http_code_preserved_without_body_text(self):
        b=Binance(key='private',secret='private')
        b.opener=Mock()
        b.opener.open.side_effect=HTTPError('private-url',503,'private',{},
            io.BytesIO(b'{"code":-1007,"msg":"PRIVATE-DO-NOT-EXPORT"}'))
        r=b.query(dict(id='private',symbol='XAUUSDT'))
        e=result_evidence(r)
        self.assertEqual(e['http_status'],503)
        self.assertEqual(e['error_code'],-1007)
        self.assertNotIn('PRIVATE-DO-NOT-EXPORT',json.dumps(e))

    def test_submit_and_cancel_capture_existing_replies_without_another_request(self):
        b=Binance();b.request=Mock(return_value=dict(status='FILLED',orderId=123,executedQty='1',avgPrice='4000'))
        o=dict(id='private',symbol='XAUUSDT',action='open',requested=1,limit=4000)
        b.submit(o,{'filters':{'PRICE_FILTER':{'tickSize':'0.01'}}})
        self.assertEqual(b.request.call_count,1)
        b.request.return_value=dict(status='CANCELED',executedQty='.5',avgPrice='4000')
        r=b.cancel(o)
        self.assertEqual(b.request.call_count,2)
        self.assertEqual(r['receipt']['values']['status'],'CANCELED')


class ReceiptRecoveryTests(unittest.TestCase):
    def setUp(self):
        tmp=tempfile.TemporaryDirectory();self.addCleanup(tmp.cleanup)
        self.store=Store(Path(tmp.name)/'state.db');self.addCleanup(self.store.close)
        self.clock=Clock();self.b=DelayedBroker(self.clock)
        self.addCleanup(patch.stopall)
        patch('trading_engine.time.monotonic',lambda:self.clock.now).start()
        patch('trading_engine.stamp',self.clock.wall).start()
        self.c=config();self.e=Engine(self.store,self.b);self.e.start(self.c)
        self.e.tick(self.c,dict(lots=.02,qty=2,contract=100),quote(self.c))
        self.g=self.e.active()[0];self.o=self.e.state['orders'][0]

    def test_invalid_initial_receipt_survives_normalization_persistence_and_export(self):
        r=Binance.normalize(dict(status='FILLED',executedQty='2',avgPrice='0',orderId=123))
        self.b.submit=Mock(return_value=r)
        o=self.e.order(self.g,'binance','open',2,quote(self.c))
        self.assertEqual(o['initial_result']['validation_issues'],['nonpositive_fill_price'])
        self.assertEqual(o['result']['receipt']['values']['avgPrice'],0)
        stored=self.store.load()['orders'][-1]
        self.assertEqual(stored['initial_result']['receipt'],r['receipt'])
        exported=build_report(self.e.state,[])['orders'][-1]
        self.assertEqual(exported['initial_result']['receipt'],r['receipt'])
        self.b.submit.assert_called_once()

    def test_cancel_failure_then_reply_retained_and_not_used_as_fill_confirmation(self):
        self.o['result']=dict(status='pending',qty=.5,price=4306)
        self.b.cancel=Mock(side_effect=[ApiError(-2011,'private',http_status=400),
            Binance.normalize(dict(status='CANCELED',executedQty='.5',avgPrice='4306'))])
        self.b.query=Mock(return_value=dict(status='pending',qty=.5,price=4306))
        for _ in range(2):
            self.o['reconcile']['next_ms']=0
            self.assertFalse(self.e.resolve(self.g))
        rec=build_report(self.e.state,[])['orders'][0]['reconcile']
        self.assertEqual(rec['cancel_attempts'],2)
        self.assertFalse(rec['first_cancel']['returned'])
        self.assertEqual(rec['first_cancel']['result']['http_status'],400)
        self.assertEqual(rec['first_cancel']['result']['error_code'],-2011)
        self.assertTrue(rec['last_cancel']['returned'])
        self.assertEqual(rec['last_cancel']['result']['receipt']['values']['status'],'CANCELED')
        self.assertEqual(self.o['result']['status'],'pending')
        self.assertEqual(len(self.b.submitted),1)
        self.assertEqual(self.b.query.call_count,2)
        self.assertIn('time_ms',rec['last_query'])
        self.assertTrue(self.store.load()['orders'][0]['reconcile']['last_cancel']['returned'])
        self.assertNotIn('private',json.dumps(rec))

    def test_regressed_query_quantity_is_explained_and_never_replaces_known_fill(self):
        self.o['result'].update(qty=.5,price=4306)
        self.b.query=Mock(return_value=dict(status='done',qty=.4,price=4306))
        self.o['reconcile']['next_ms']=0
        self.e.resolve(self.g)
        self.assertEqual(self.o['result']['qty'],.5)
        self.assertEqual(self.o['reconcile']['last_query']['validation_issues'],['quantity_regressed'])


if __name__=='__main__': unittest.main()
