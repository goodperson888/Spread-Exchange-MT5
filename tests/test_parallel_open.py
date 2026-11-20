"""Paired opening durability, independent writes and uncertain-fill recovery."""
import copy
import sqlite3
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from execution_diagnostics import build_report
from execution_latency import LatencyStats
from mt5_pool_config import candidates
from pair_instruments import equal_lot_plan
from trading_brokers import PaperBroker
from trading_config import validate
from trading_engine import Engine, stamp
from trading_store import Store
from test_mt5_pool import config as pool_config, spec
from test_trading_engine import config, quote


class ParallelOpenTests(unittest.TestCase):
    def setUp(self):
        tmp=tempfile.TemporaryDirectory();self.addCleanup(tmp.cleanup)
        self.store=Store(Path(tmp.name)/'state.db');self.addCleanup(self.store.close)
        self.c=candidates(pool_config(2))['t1~t2'];validate(self.c)
        self.p=equal_lot_plan(self.c,{leg:spec() for leg in ('binance','mt5')})
        self.b=PaperBroker();self.e=Engine(self.store,self.b);self.e.latency=LatencyStats()
        self.e.start(self.c);self.sent=[];self.lock=threading.Lock();self.owner=threading.get_ident()

    def open(self, direction='forward'):
        self.e.open(self.c,self.p,quote(self.c),direction=direction)
        return self.e.state['groups'][-1]

    def install(self, replies=None, overlap=False):
        replies=replies or {};gate=threading.Barrier(2) if overlap else None
        def submit(o):
            if o['action']=='open':
                durable=self.store.load()
                rows=[x for x in durable['orders'] if x['group']==o['group'] and x['action']=='open']
                assert len(rows)==2 and {x['leg'] for x in rows}=={'binance','mt5'}
                assert next(x for x in rows if x['id']==o['id'])['result']['status']=='unknown'
                assert threading.get_ident()!=self.owner
            with self.lock:self.sent.append(copy.deepcopy(o))
            if gate and o['action']=='open':gate.wait(timeout=3)
            reply=replies.get(o['leg']) if o['action']=='open' else None
            if isinstance(reply,BaseException):raise reply
            return copy.deepcopy(reply) if reply is not None else PaperBroker.submit(self.b,o)
        self.b.submit=submit

    def test_both_open_calls_overlap_after_one_atomic_journal_and_owner_handles_receipts(self):
        self.install(overlap=True);threads=[];batches=[]
        original=self.store.commit;batch_original=self.store.commit_many
        def commit(*args):threads.append(threading.get_ident());return original(*args)
        def batch(state,events):
            threads.append(threading.get_ident());batches.append(copy.deepcopy(events))
            return batch_original(state,events)
        with patch.object(self.store,'commit',side_effect=commit),patch.object(self.store,'commit_many',side_effect=batch):
            g=self.open()
        self.assertEqual(g['status'],'open');self.assertEqual(g['entry_dispatch'],'parallel')
        self.assertEqual(set(threads),{self.owner});self.assertEqual(len(batches),1)
        self.assertEqual([kind for kind,_ in batches[0]],['group_opening','order_intent','order_intent'])
        self.assertEqual(len(self.sent),2);self.assertEqual({o['dispatch_mode'] for o in self.sent},{'parallel'})
        report=build_report(self.e.state,self.store.events())
        self.assertEqual(report['groups'][0]['entry_dispatch'],'parallel')
        self.assertGreaterEqual(report['groups'][0]['execution_open']['dispatch_gap_ms'],0)
        self.assertIn('open_dispatch_gap_ms',self.e.latency.snapshot()['phases'])

    def test_fast_receipt_is_committed_before_slow_peer_returns(self):
        barrier=threading.Barrier(2);saved=threading.Event();original=self.store.commit
        def commit(state,kind,data=None):
            result=original(state,kind,data)
            if kind=='order_result' and data['id']==self.fast_id:saved.set()
            return result
        def submit(o):
            if o['leg']=='binance':self.fast_id=o['id']
            barrier.wait(timeout=3)
            if o['leg']=='mt5':assert saved.wait(3),'fast receipt was held behind slow peer'
            return PaperBroker.submit(self.b,o)
        self.b.submit=submit
        with patch.object(self.store,'commit',side_effect=commit):g=self.open()
        self.assertEqual(g['status'],'open')

    def test_transaction_failure_sends_neither_and_rolls_back_group_and_intents(self):
        self.install();before=self.store.load();events=self.store.events()
        self.store.db.execute("""CREATE TRIGGER fail_open_intent BEFORE INSERT ON events
            WHEN NEW.kind='order_intent' AND NEW.data LIKE '%"leg": "mt5"%'
            BEGIN SELECT RAISE(ABORT, 'journal unavailable'); END""")
        with self.assertRaises(sqlite3.IntegrityError):self.open()
        self.assertEqual(self.sent,[]);self.assertEqual(self.e.state['groups'],[])
        self.assertEqual(self.e.state['orders'],[]);self.assertEqual(self.store.load(),before)
        self.assertEqual(self.store.events(),events);self.assertFalse(self.e.state['enabled'])

    def test_second_worker_failure_never_releases_first_worker(self):
        from concurrent.futures import ThreadPoolExecutor
        self.install();original=ThreadPoolExecutor.submit;count=0
        def submit(pool,*args,**kwargs):
            nonlocal count
            count+=1
            if count==2:raise RuntimeError('no second worker')
            return original(pool,*args,**kwargs)
        with patch('execution_dispatch.ThreadPoolExecutor.submit',new=submit):g=self.open()
        self.assertEqual(self.sent,[]);self.assertEqual(g['status'],'closed')
        self.assertFalse(self.e.state['enabled']);self.assertFalse(self.e.uncertain(g))
        self.assertEqual(self.e.state['last_pause']['reason_code'],'OPEN_DISPATCH_FAILED')

    def test_quote_expired_before_release_sends_neither_and_keeps_entry_armed(self):
        self.install()
        with patch('trading_engine.valid_group_quote',return_value=False):g=self.open()
        self.assertEqual(self.sent,[]);self.assertEqual(g['status'],'closed')
        self.assertTrue(self.e.state['enabled']);self.assertTrue(g['entry_skipped'])
        self.assertTrue(all(o['result']['submitted'] is False for o in self.e.state['orders']))

    def test_single_reject_or_partial_fill_unwinds_only_confirmed_quantities(self):
        for leg,qty in (('mt5',0),('binance',0),('mt5',.004),('binance',.004)):
            with self.subTest(leg=leg,qty=qty):
                self.e.state.update(groups=[],orders=[],enabled=True)
                self.sent=[];self.install({leg:dict(status='done',qty=qty,price=4300 if qty else 0)},overlap=True)
                g=self.open()
                self.assertFalse(self.e.state['enabled']);self.assertEqual(g['status'],'closed')
                self.assertFalse(self.e.uncertain(g));self.assertEqual(max(self.e.amounts(g).values()),0)
                opens=[o for o in self.sent if o['action']=='open'];closes=[o for o in self.sent if o['action']=='close']
                self.assertEqual(len(opens),2)
                for side in ('binance','mt5'):
                    self.assertAlmostEqual(sum(o['requested'] for o in closes if o['leg']==side),qty if side==leg else .01)
                self.assertTrue(all(o['slippage_mode']=='protective' for o in closes))

    def test_unknown_leg_keeps_original_ids_and_never_repeats_or_closes_uncertain_exposure(self):
        self.install({'binance':TimeoutError('write outcome unknown')},overlap=True)
        g=self.open();ids=[o['id'] for o in self.e.state['orders']]
        self.assertEqual(g['status'],'opening');self.assertFalse(self.e.state['enabled'])
        self.b.query=lambda o:dict(status='unknown',qty=0,price=0)
        for _ in range(3):self.e.tick(self.c,self.p,quote(self.c))
        self.assertEqual(len(self.sent),2);self.assertEqual([o['id'] for o in self.e.state['orders']],ids)
        unknown=self.e.uncertain(g)[0];unknown['reconcile']['next_ms']=0
        self.b.query=lambda o:dict(status='done',qty=.01,price=o['reference'],ticket=o['id'],position=o['id'])
        self.e.tick(self.c,self.p,quote(self.c))
        self.assertEqual(g['status'],'open');self.assertEqual(g['reason'],'')
        self.assertFalse(self.e.state['enabled']);self.assertEqual(len(self.sent),2)

    def test_expired_confirmation_unwinds_after_unknown_order_is_resolved(self):
        self.install({'mt5':TimeoutError('timeout')},overlap=True);g=self.open()
        self.e._entry_deadlines[g['id']]=time.monotonic()-1
        self.b.query=lambda o:dict(status='done',qty=.01,price=o['reference'],ticket=o['id'],position=o['id'])
        self.e.uncertain(g)[0]['reconcile']['next_ms']=0
        self.e.tick(self.c,self.p,quote(self.c))
        self.assertEqual(g['status'],'closed');self.assertEqual(len([o for o in self.sent if o['action']=='open']),2)
        self.assertFalse(self.e.state['enabled'])

    def test_verified_native_history_race_gets_original_confirmation_window_on_either_leg(self):
        for pending_legs in (('binance',),('mt5',),('binance','mt5')):
            with self.subTest(pending_legs=pending_legs):
                self.e.state.update(groups=[],orders=[],enabled=True)
                replies={leg:dict(status='unknown',qty=0,price=0,ticket=leg,retcode=10009,
                    ack_volume=.01,ack_price=0,confirmation=dict(stage='order_pending',position_matches=1,
                        deal_matches=1,ticket_matches=1,symbol_matches=1,magic_matches=1,side_matches=1,entry_matches=1))
                    for leg in pending_legs}
                self.sent=[];self.install(replies,overlap=True);g=self.open()
                self.assertEqual(g['status'],'opening');self.assertTrue(self.e.state['enabled'])
                self.assertEqual(len(self.sent),2)
                for o in self.e.uncertain(g):o['reconcile']['next_ms']=0
                self.b.query=lambda o:dict(status='done',qty=.01,price=o['reference'],ticket=o['result']['ticket'],position=o['id'])
                self.e.tick(self.c,self.p,quote(self.c))
                self.assertEqual(g['status'],'open');self.assertTrue(self.e.state['enabled'])
                self.assertEqual(len(self.sent),2)

    def test_both_full_receipts_do_not_churn_a_hedge_just_because_calls_were_slow(self):
        self.install(overlap=True)
        original=self.e._accept_order_result
        def accept(g,o,r):
            original(g,o,r);self.e._entry_deadlines[g['id']]=time.monotonic()-1
        with patch.object(self.e,'_accept_order_result',side_effect=accept):g=self.open()
        self.assertEqual(g['status'],'open');self.assertTrue(self.e.state['enabled'])
        self.assertEqual(len(self.sent),2)

    def test_dispatch_reports_local_quote_age_and_alignment_without_platform_clock(self):
        now=stamp();q=quote(self.c)
        q['binance'].update(observed_ms=now-20,time_ms=now+7200000)
        q['mt5'].update(observed_ms=now-50,time_ms=now+7200000)
        with patch('trading_engine.stamp',return_value=now):self.e.open(self.c,self.p,q)
        report=build_report(self.e.state,[])
        rows={o['leg']:o for o in report['orders']}
        self.assertEqual(rows['binance']['dispatch_quote_age_ms'],20)
        self.assertEqual(rows['mt5']['dispatch_quote_age_ms'],50)
        self.assertTrue(all(o['dispatch_quote_alignment_ms']==30 for o in rows.values()))
        self.assertIn('open_dispatch_quote_age_ms',self.e.latency.snapshot()['phases'])

    def test_reverse_direction_keeps_terminal_ownership_and_opposite_sides(self):
        g=self.open('reverse');rows={o['leg']:o for o in self.e.state['orders']}
        self.assertEqual(g['status'],'open');self.assertEqual(g['direction'],'reverse')
        self.assertEqual(rows['binance']['native_position_side'],0)
        self.assertEqual(rows['mt5']['native_position_side'],1)
        for leg in rows:self.assertEqual(rows[leg]['endpoint'],g['pair']['endpoints'][leg])

    def test_broker_precheck_rejections_are_not_mislabelled_as_stale_quote_skips(self):
        self.install({leg:dict(status='done',qty=0,price=0,submitted=False,error='margin rejected')
                      for leg in ('binance','mt5')},overlap=True)
        g=self.open()
        self.assertEqual(g['status'],'closed');self.assertFalse(self.e.state['enabled'])
        self.assertFalse(g.get('entry_skipped',False));self.assertIn('拒单',g['reason'])

    def test_restart_never_replays_persisted_open_intents(self):
        self.install({'mt5':TimeoutError('timeout')},overlap=True);g=self.open()
        self.b.query=lambda o:dict(status='unknown',qty=0,price=0)
        restarted=Engine(self.store,self.b)
        restarted.tick(self.c,self.p,quote(self.c))
        self.assertTrue(restarted.state['recovery']);self.assertFalse(restarted.state['enabled'])
        self.assertEqual(len(self.sent),2)
        self.assertEqual([o['id'] for o in restarted.state['orders']],[o['id'] for o in self.e.state['orders']])

    def test_execution_controls_validate_and_old_profiles_get_their_own_defaults(self):
        import pair_profiles
        for field,bad in (('entry_dispatch','parallel'),('entry_dispatch',True),
                          ('mt5_quote_poll_ms',True),('mt5_quote_poll_ms',9),
                          ('mt5_quote_poll_ms',1001),('mt5_quote_poll_ms',20.5)):
            with self.subTest(field=field,value=bad):
                changed=copy.deepcopy(self.c);changed['execution'][field]=bad
                with self.assertRaises(ValueError):validate(changed)
        for interval in (10,20,50,1000):
            changed=copy.deepcopy(self.c);changed['execution']['mt5_quote_poll_ms']=interval;validate(changed)
        saved=pair_profiles.capture(self.c)
        self.assertEqual(saved['execution']['entry_dispatch'],'auto')
        self.c['execution'].update(entry_dispatch='serial',mt5_quote_poll_ms=20)
        self.assertEqual(pair_profiles.activate(self.c,saved)['execution']['entry_dispatch'],'auto')
        saved['execution'].pop('entry_dispatch');saved['execution'].pop('mt5_quote_poll_ms')
        restored=pair_profiles.activate(self.c,saved)
        self.assertEqual(restored['execution']['entry_dispatch'],'auto')
        self.assertEqual(restored['execution']['mt5_quote_poll_ms'],50)

    def test_same_terminal_exchange_and_explicit_serial_keep_original_path(self):
        self.assertTrue(self.e._parallel_entry(self.c))
        changed=copy.deepcopy(self.c);changed['execution']['entry_dispatch']='serial'
        self.assertFalse(self.e._parallel_entry(changed))
        changed=copy.deepcopy(self.c);changed['pair'].pop('endpoints')
        self.assertFalse(self.e._parallel_entry(changed));self.assertFalse(self.e._parallel_entry(config()))
        changed=copy.deepcopy(self.c)
        changed['pair']['endpoints']['mt5']['terminal_path']='c:/mt5-1/terminal64.exe'
        self.assertFalse(self.e._parallel_entry(changed))
        self.c['execution']['entry_dispatch']='serial';sent=[]
        def submit(o):sent.append(threading.get_ident());return PaperBroker.submit(self.b,o)
        self.b.submit=submit;g=self.open()
        self.assertEqual(g['status'],'open');self.assertEqual(g['entry_dispatch'],'serial')
        self.assertEqual(sent,[self.owner,self.owner])


if __name__=='__main__':unittest.main()
