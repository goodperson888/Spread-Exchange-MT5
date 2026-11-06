import copy
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import Mock

import _test_runtime
from server import TradingRuntime
from trading_brokers import PaperBroker
from trading_engine import Engine, stamp
from trading_store import Store
from test_trading_engine import config, quote


class ExecutionRecoveryTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.store=Store(Path(self.tmp.name)/'journal.sqlite3');self.addCleanup(self.store.close)
        self.c=config();self.c['execution']['entry_leg']='binance'
        self.plan=dict(qty=1,lots=.01,contract=100)
        self.b=Mock(wraps=PaperBroker())
        self.e=Engine(self.store,self.b)

    def unknown_second(self, result=None):
        def submit(o):
            if o['leg']=='mt5' and o['action']=='open':
                return copy.deepcopy(result) if result is not None else dict(
                    status='done',qty=1,price=0,ticket='500',deal='600',retcode=10009)
            return PaperBroker().submit(o)
        self.b.submit.side_effect=submit
        self.b.query.return_value=dict(status='unknown',qty=0,price=0)
        self.e.start(self.c);self.e.tick(self.c,self.plan,quote(self.c))
        g=self.e.active()[0];o=self.e.uncertain(g)[0]
        o['reconcile']['next_ms']=0
        return g,o

    def recovered(self, o):
        self.assertEqual(o['result']['ticket'],'500')
        return dict(status='done',qty=1,price=4300.3,ticket='500',position='900',position_identifier='700')

    def test_invalid_ack_keeps_order_identity_durably(self):
        g,o=self.unknown_second()
        self.assertEqual(g['status'],'unwinding');self.assertFalse(self.e.state['enabled'])
        saved=self.store.load()['orders'][-1]['result']
        self.assertEqual((saved['ticket'],saved['deal'],saved['retcode']),('500','600',10009))
        self.assertEqual(saved['qty'],0)
        self.assertEqual(self.b.submit.call_count,2)

    def test_malformed_non_mapping_results_are_unknown_without_crashing(self):
        for value in (None,[],1,'bad'):
            self.assertFalse(self.e.result_valid(value,1))
        g,o=self.unknown_second(['malformed'])
        self.assertEqual(o['result']['status'],'unknown')
        self.assertEqual(self.b.submit.call_count,2)

    def test_stale_quotes_still_resolve_but_do_not_close_or_reopen(self):
        g,o=self.unknown_second();self.b.query.side_effect=self.recovered
        q=quote(self.c)
        for leg in ('binance','mt5'):q[leg]['time_ms']-=60000
        self.e.tick(self.c,self.plan,q)
        self.assertFalse(self.e.uncertain(g));self.assertEqual(self.b.submit.call_count,2)
        self.assertEqual(self.e.amounts(g),dict(binance=1,mt5=1))
        self.e.tick(self.c,self.plan,quote(self.c))
        self.assertEqual(g['status'],'closed');self.assertEqual(self.b.submit.call_count,4)
        sent=[x.args[0] for x in self.b.submit.call_args_list]
        self.assertEqual([(x['leg'],x['action']) for x in sent],
                         [('binance','open'),('mt5','open'),('mt5','close'),('binance','close')])
        self.assertEqual(sent[2]['position'],'900');self.assertEqual(sent[2]['opening_id'],o['id'])
        self.assertFalse(self.e.state['enabled'])

    def test_recovery_queries_only_and_never_submits_until_reconciled(self):
        g,o=self.unknown_second();self.e.state['recovery']=True
        self.b.query.side_effect=self.recovered
        self.e.tick(self.c,self.plan,quote(self.c))
        self.assertFalse(self.e.uncertain(g));self.assertEqual(self.b.submit.call_count,2)
        self.assertTrue(self.e.state['recovery'])

    def test_wrong_connection_does_not_query_unknown_order(self):
        g,o=self.unknown_second();other=copy.deepcopy(self.c);other['mt5']['account']='OTHER'
        calls=self.b.query.call_count
        with self.assertRaisesRegex(ValueError,'账户或品种不一致'):
            self.e.tick(other,self.plan,quote(other))
        self.assertEqual(self.b.query.call_count,calls)

    def test_no_quote_runtime_continues_read_only_confirmation(self):
        g,o=self.unknown_second();self.b.query.side_effect=self.recovered
        r=TradingRuntime.__new__(TradingRuntime);r.config=self.c;r.engine=self.e
        r.pump=Mock();r.pump.read.return_value=(None,0,None)
        r._poll()
        self.assertFalse(self.e.uncertain(g));self.assertEqual(self.b.submit.call_count,2)

    def test_unreconciled_runtime_also_queries_interrupted_opening_before_close_only_guard(self):
        self.c['execution']['mode']='live'
        g,o=self.unknown_second();g['status']='opening';self.e.state['recovery']=True
        self.b.query.side_effect=self.recovered
        r=TradingRuntime.__new__(TradingRuntime);r.config=self.c;r.engine=self.e;r.reconciled=False
        r.pump=Mock();r.pump.read.return_value=(quote(self.c),0,None)
        r.market_meta={};r.entry_preflight=None
        r._auto_reconcile_flat=Mock(side_effect=RuntimeError('after read-only confirmation'))
        with self.assertRaisesRegex(RuntimeError,'after read-only confirmation'):r._poll()
        self.assertFalse(self.e.uncertain(g));self.assertEqual(self.b.submit.call_count,2)

    def test_close_with_stale_quote_cannot_bypass_tick_guard(self):
        self.e.start(self.c);self.e.tick(self.c,self.plan,quote(self.c));g=self.e.active()[0]
        self.e.request_close(g['id'])
        q=quote(self.c)
        for leg in ('binance','mt5'):q[leg]['time_ms']-=60000
        self.e.close_group(g,q)
        self.assertEqual(self.b.submit.call_count,2);self.assertEqual(g['status'],'closing')

    def test_second_leg_expired_quote_does_not_send_a_protective_close_with_it(self):
        q=quote(self.c)
        stale=copy.deepcopy(q)
        for leg in ('binance','mt5'):stale[leg]['time_ms']-=60000
        self.e.quote_provider=lambda:stale
        self.e.start(self.c);self.e.tick(self.c,self.plan,q)
        self.assertEqual(self.b.submit.call_count,1)
        self.assertEqual(self.e.active()[0]['status'],'unwinding')
        self.e.tick(self.c,self.plan,quote(self.c))
        self.assertEqual(self.b.submit.call_count,2)
        self.assertEqual(self.e.state['groups'][0]['status'],'closed')

    def test_close_response_explains_pending_confirmation(self):
        g,o=self.unknown_second();o['reconcile']['next_ms']=stamp()+60000
        r=TradingRuntime.__new__(TradingRuntime);r.config=self.c;r.engine=self.e
        r.quote=quote(self.c);r.plan=self.plan;r.lock=threading.RLock();r.snapshot=lambda:{}
        result=r.close_group(g['id'])
        self.assertIn('平仓请求已记录',result['message']);self.assertIn('尚未双平',result['message'])
        self.assertEqual(self.b.submit.call_count,2)
        self.assertEqual(g['status'],'unwinding')
        o['reconcile']['next_ms']=0;self.b.query.side_effect=self.recovered
        self.e.tick(self.c,self.plan,quote(self.c))
        mt5_close=self.b.submit.call_args_list[2].args[0]
        self.assertEqual(mt5_close['slippage'],g['parameters']['unwind_slippage_usd'])

    def ordinary_recovery_runtime(self, mismatch=False):
        self.c['execution']['mode']='live';self.c['mt5']['adapter']='native'
        g,o=self.unknown_second();self.e.state['recovery']=True
        self.b.query.side_effect=self.recovered
        r=TradingRuntime.__new__(TradingRuntime);r.config=self.c;r.engine=self.e
        r.quote=quote(self.c);r.plan=self.plan;r.lock=threading.RLock();r.snapshot=lambda:{}
        r.connected=True;r.reconciled=False;r._last_auto_reconcile_at=-1e9
        r.binance=Mock(position_mode='one_way')
        r.binance.positions.return_value=[dict(positionAmt='-1',positionSide='BOTH')]
        r.binance.open_orders.return_value=[]
        r.terminal=Mock();r.terminal.call.return_value=dict(orders=[],positions=[
            dict(ticket='900',identifier='700',magic=self.c['execution']['magic'],side=0,
                 lots=.02 if mismatch else .01,comment=o['id'])])
        return r,g

    def test_ordinary_interrupted_close_resumes_only_after_exact_account_reconciliation(self):
        r,g=self.ordinary_recovery_runtime()
        self.assertTrue(r._auto_reconcile_flat())
        self.assertTrue(r.reconciled);self.assertFalse(self.e.state['recovery'])
        self.assertEqual(self.b.submit.call_count,2);self.assertFalse(self.e.state['enabled'])
        self.e.tick(self.c,self.plan,quote(self.c))
        self.assertEqual(g['status'],'closed');self.assertEqual(self.b.submit.call_count,4)

    def test_ordinary_recovery_mismatch_still_blocks_every_close(self):
        r,g=self.ordinary_recovery_runtime(mismatch=True)
        self.assertFalse(r._auto_reconcile_flat())
        self.assertFalse(r.reconciled);self.assertTrue(self.e.state['recovery'])
        self.e.tick(self.c,self.plan,quote(self.c))
        self.assertEqual(self.b.submit.call_count,2)


if __name__=='__main__':unittest.main()
