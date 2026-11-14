"""Concurrency, durability, and recovery at the two-platform close boundary."""
import copy
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

from execution_diagnostics import build_report
from execution_latency import LatencyStats
from trading_brokers import PaperBroker
from trading_engine import Engine
from trading_store import Store
from test_trading_engine import config, quote


class ParallelCloseTests(unittest.TestCase):
    def setUp(self):
        tmp=tempfile.TemporaryDirectory();self.addCleanup(tmp.cleanup)
        self.store=Store(Path(tmp.name)/'state.db');self.addCleanup(self.store.close)
        self.c=config();self.c['execution']['close_retry_limit']=3
        self.p=dict(qty=1,lots=.01,contract=100)
        self.b=PaperBroker();self.e=Engine(self.store,self.b);self.e.start(self.c)
        self.e.tick(self.c,self.p,quote(self.c));self.g=self.e.active()[0]
        self.e.request_close(self.g['id']);self.e.latency=LatencyStats()
        self.sent=[];self.receipts={};self.owner=threading.get_ident()

    def close(self): self.e.close_group(self.g,quote(self.c))

    def install(self, replies=None, barrier=False):
        replies=replies or {};gate=threading.Barrier(2) if barrier else None
        lock=threading.Lock();original=PaperBroker.submit
        def submit(o):
            saved=self.store.load()
            closes=[row for row in saved['orders'] if row['action']=='close']
            assert {row['leg'] for row in closes}=={'mt5','binance'}
            assert threading.get_ident()!=self.owner
            with lock: self.sent.append(copy.deepcopy(o))
            if gate: gate.wait(timeout=3)
            reply=replies.get(o['leg'])
            if isinstance(reply,BaseException): raise reply
            result=copy.deepcopy(reply) if reply is not None else original(self.b,o)
            with lock: self.receipts[o['id']]=copy.deepcopy(result)
            return result
        self.b.submit=submit

    def test_both_requests_overlap_and_all_state_commits_stay_on_owner(self):
        self.install(barrier=True)
        original=self.store.commit;threads=[]
        def commit(*args):
            threads.append(threading.get_ident());return original(*args)
        original_batch=self.store.commit_many
        def batch(*args):
            threads.append(threading.get_ident());return original_batch(*args)
        with patch.object(self.store,'commit',side_effect=commit), \
                patch.object(self.store,'commit_many',side_effect=batch): self.close()
        self.assertEqual(self.g['status'],'closed')
        self.assertEqual(set(threads),{self.owner})
        self.assertEqual(len(self.sent),2)
        self.assertEqual({o['dispatch_mode'] for o in self.sent},{'parallel'})
        self.assertEqual({o['close_batch'] for o in self.sent},{1})
        self.assertEqual({o['slippage_mode'] for o in self.sent},{'normal'})
        self.assertEqual(self.g['close_attempts'],dict(mt5=1,binance=1))
        self.assertEqual(self.e.amounts(self.g),dict(mt5=0,binance=0))
        report=build_report(self.e.state,self.store.events())
        timing=report['groups'][0]['execution_close']
        self.assertEqual(timing['parallel_batches'],1)
        self.assertGreaterEqual(timing['last_dispatch_gap_ms'],0)
        self.assertIn('close_dispatch_gap_ms',self.e.latency.snapshot()['phases'])
        rows=[o for o in report['orders'] if o['action']=='close']
        for row in rows:
            self.assertEqual(row['dispatch_mode'],'parallel');self.assertEqual(row['close_batch'],1)
            self.assertGreaterEqual(row['dispatch_queue_ms'],0)
            self.assertGreaterEqual(row['result']['response_received_ms'],row['dispatch_ms'])

    def test_fast_receipt_is_durable_while_peer_submit_is_still_running(self):
        both=threading.Barrier(2);committed=threading.Event();original=self.store.commit
        def commit(state,kind,data=None):
            result=original(state,kind,data)
            if kind=='order_result' and data['id']==self.fast_id: committed.set()
            return result
        def submit(o):
            if o['leg']=='binance': self.fast_id=o['id']
            both.wait(timeout=3)
            if o['leg']=='mt5':
                assert committed.wait(timeout=3), 'peer receipt was held until slow submit finished'
                saved=self.store.load()
                assert next(row for row in saved['orders'] if row['id']==self.fast_id)['result']['qty']==1
            return PaperBroker.submit(self.b,o)
        self.b.submit=submit
        with patch.object(self.store,'commit',side_effect=commit): self.close()
        self.assertEqual(self.g['status'],'closed')

    def test_rejected_or_partial_leg_only_reduces_confirmed_residual_next(self):
        for leg,qty in (('mt5',0),('binance',0),('mt5',.4),('binance',.4)):
            with self.subTest(leg=leg,qty=qty):
                # Restore the same fully hedged group between scenarios.
                self.e.state['orders']=self.e.state['orders'][:2]
                for key in ('close_attempts','execution_close','closed_ms','close_protection'):
                    self.g.pop(key,None)
                self.g.update(status='closing',attempts=0);self.e.state['enabled']=True
                self.sent=[]
                self.install({leg:dict(status='done',qty=qty,price=4300 if qty else 0)})
                self.close()
                self.assertEqual(len(self.sent),2)
                self.assertFalse(self.e.state['enabled'])
                remaining=self.e.amounts(self.g)
                self.assertAlmostEqual(remaining[leg],1-qty)
                self.assertEqual(remaining['binance' if leg=='mt5' else 'mt5'],0)
                def reduce(o):
                    self.sent.append(copy.deepcopy(o));return PaperBroker.submit(self.b,o)
                self.b.submit=reduce;self.close()
                self.assertEqual(self.g['status'],'closed')
                self.assertEqual(len(self.sent),3)
                self.assertEqual(self.sent[-1]['leg'],leg)
                self.assertEqual(self.sent[-1]['dispatch_mode'],'serial')
                self.assertEqual(self.sent[-1]['slippage_mode'],'protective')
                self.assertAlmostEqual(self.sent[-1]['requested'],1-qty)

    def test_timeout_preserves_original_and_never_repeats_other_filled_leg(self):
        self.install({'mt5':TimeoutError('uncertain write')},barrier=True)
        self.close();self.assertEqual(len(self.sent),2)
        pending=self.e.uncertain(self.g)[0]
        self.assertEqual(pending['leg'],'mt5');original_id=pending['id']
        self.b.query=lambda o:dict(status='unknown',qty=0,price=0)
        for _ in range(3):
            pending['reconcile']['next_ms']=0;self.close()
        self.assertEqual(len(self.sent),2)
        self.b.query=lambda o:dict(status='done',qty=o['requested'],price=o['reference'],ticket='original')
        pending['reconcile']['next_ms']=0;self.close()
        self.assertEqual(pending['id'],original_id)
        self.assertEqual(self.g['status'],'closed');self.assertEqual(len(self.sent),2)
        self.assertFalse(self.e.state['enabled'])

    def test_restart_between_receipts_queries_original_without_new_writes(self):
        class Interrupted(BaseException): pass
        self.install(barrier=True);original=self.store.commit
        def commit(state,kind,data=None):
            original(state,kind,data)
            if kind=='order_result': raise Interrupted()
        with patch.object(self.store,'commit',side_effect=commit):
            with self.assertRaises(Interrupted): self.close()
        persisted=self.store.load()
        self.assertEqual(len([o for o in persisted['orders'] if o['action']=='close']),2)
        self.assertEqual(sum(o['result']['status']=='unknown' for o in persisted['orders']),1)
        recovered=Engine(self.store,self.b);g=recovered.active()[0]
        self.assertTrue(recovered.state['recovery']);self.assertFalse(recovered.state['enabled'])
        ids={o['id'] for o in recovered.uncertain(g)}
        self.b.query=lambda o:copy.deepcopy(self.receipts[o['id']])
        for o in recovered.uncertain(g):
            if 'reconcile' in o: o['reconcile']['next_ms']=0
        recovered.reconcile_pending(self.c)
        self.assertFalse(recovered.uncertain(g));self.assertTrue(ids.issubset(self.receipts))
        recovered.state['recovery']=False  # Account ownership reconciliation succeeds.
        recovered.tick(self.c,self.p,quote(self.c))
        self.assertEqual(g['status'],'closed');self.assertEqual(len(self.sent),2)
        self.assertFalse(recovered.state['enabled'])

    def test_second_intent_event_failure_rolls_back_whole_batch_before_dispatch(self):
        import sqlite3
        self.install();before=self.store.load();events=self.store.events()
        self.store.db.execute("""CREATE TRIGGER fail_close_intent BEFORE INSERT ON events
            WHEN NEW.kind='order_intent' AND NEW.data LIKE '%"leg": "binance"%'
            BEGIN SELECT RAISE(ABORT, 'journal unavailable'); END""")
        with self.assertRaises(sqlite3.IntegrityError): self.close()
        self.assertEqual(self.sent,[])
        self.assertFalse(self.e.uncertain(self.g))
        self.assertEqual(self.store.load(),before);self.assertEqual(self.store.events(),events)
        self.assertEqual(len(self.e.state['orders']),2)
        self.assertEqual(self.g['close_attempts'],dict(mt5=0,binance=0))
        self.store.db.execute('DROP TRIGGER fail_close_intent')
        self.close();self.assertEqual(self.g['status'],'closed')

    def test_quote_expiring_during_journal_sends_neither_and_spends_no_budget(self):
        self.install()
        with patch('trading_engine.valid_group_quote',side_effect=[True,False]): self.close()
        self.assertEqual(self.sent,[])
        self.assertEqual(self.g['close_attempts'],dict(mt5=0,binance=0))
        rows=[o for o in self.e.state['orders'] if o['action']=='close']
        self.assertTrue(all(o['result']['submitted'] is False for o in rows))
        self.assertTrue(all('dispatch_ms' not in o for o in rows))
        self.close();self.assertEqual(self.g['status'],'closed')
        self.assertEqual(self.g['close_attempts'],dict(mt5=1,binance=1))

    def test_second_worker_creation_failure_aborts_both_before_network(self):
        from concurrent.futures import ThreadPoolExecutor
        self.install();original=ThreadPoolExecutor.submit;count=0
        def submit(pool,*args,**kwargs):
            nonlocal count
            count+=1
            if count==2: raise RuntimeError('thread limit')
            return original(pool,*args,**kwargs)
        with patch('execution_dispatch.ThreadPoolExecutor.submit',new=submit): self.close()
        self.assertEqual(self.sent,[]);self.assertEqual(self.g['status'],'attention')
        self.assertEqual(self.e.state['last_pause']['reason_code'],'CLOSE_DISPATCH_FAILED')
        self.assertFalse(self.e.uncertain(self.g))
        for _ in range(3): self.e.tick(self.c,self.p,quote(self.c))
        self.assertEqual(self.sent,[])
        self.e.request_close(self.g['id']);self.close()
        self.assertEqual(self.g['status'],'closed');self.assertFalse(self.e.state['enabled'])

    def test_exhausted_platform_cannot_launch_only_its_peer(self):
        self.install();self.g['close_attempts']=dict(mt5=0,binance=3)
        self.close();self.assertEqual(self.sent,[])
        self.assertEqual(self.g['status'],'attention');self.assertEqual(self.g['close_halt']['leg'],'binance')

    def test_equal_partial_fills_close_only_the_remaining_matched_quantity(self):
        self.install({leg:dict(status='done',qty=.5,price=4300) for leg in ('mt5','binance')},barrier=True)
        self.close();self.assertEqual(self.e.amounts(self.g),dict(mt5=.5,binance=.5))
        self.install(barrier=True);self.close()
        self.assertEqual(self.g['status'],'closed')
        self.assertEqual(self.g['close_attempts'],dict(mt5=2,binance=2))
        for o in self.sent[-2:]:
            self.assertEqual(o['requested'],.5);self.assertEqual(o['close_batch'],2)

    def test_both_zero_fills_keep_separate_bounded_budgets(self):
        self.install({leg:dict(status='done',qty=0,price=0,raw_status='EXPIRED') for leg in ('mt5','binance')})
        for _ in range(5): self.close()
        self.assertEqual(self.g['status'],'attention');self.assertEqual(len(self.sent),6)
        self.assertEqual(self.g['close_attempts'],dict(mt5=3,binance=3))
        self.assertEqual(self.e.amounts(self.g),dict(mt5=1,binance=1))

    def test_quote_expiring_while_workers_start_aborts_both_without_budget(self):
        self.install()
        with patch('trading_engine.valid_group_quote',side_effect=[True,True,False]): self.close()
        self.assertEqual(self.sent,[])
        self.assertEqual(self.g['close_attempts'],dict(mt5=0,binance=0))
        self.assertFalse(self.e.uncertain(self.g))
        self.close();self.assertEqual(self.g['status'],'closed')

    def test_pending_peer_is_never_queried_or_cancelled_during_submit(self):
        self.install({leg:dict(status='pending',qty=0,price=0,ticket=leg) for leg in ('mt5','binance')},barrier=True)
        queried=[];cancelled=[]
        def query(o):
            assert len(self.receipts)==2
            queried.append(o['id'])
            return dict(status='done',qty=o['requested'],price=o['reference'],ticket=o['leg'])
        self.b.query=query
        self.b.cancel=lambda o:cancelled.append(o['id'])
        self.close();self.assertFalse(self.e.state['enabled'])
        for o in self.e.uncertain(self.g): o['reconcile']['next_ms']=0
        self.close();self.assertEqual(self.g['status'],'closed')
        self.assertEqual(len(self.sent),2)
        self.assertCountEqual(queried,[o['id'] for o in self.sent])
        self.assertCountEqual(cancelled,[o['id'] for o in self.sent])


if __name__=='__main__': unittest.main()
