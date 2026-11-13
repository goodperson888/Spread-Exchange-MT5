"""A native MT5 acknowledgement race must not extend the entry deadline."""
import copy
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace as NS
from unittest.mock import Mock, patch

from trading_brokers import NativeMT5Fills, PaperBroker
from trading_engine import Engine
from trading_store import Store
from test_confirmation_diagnostics import Clock
from test_trading_engine import config, quote


class NativeRaceBroker(PaperBroker):
    def __init__(self, clock):
        self.clock=clock;self.submitted=[];self.queries=[]
        self.terminal_at=101.44;self.query_delay=.006;self.on_query=None
        self.position=NS(ticket=900,identifier=700,magic=42,type=0,symbol='XAUUSD',volume=.01)
        self.deal=NS(ticket=600,order=500,position_id=700,symbol='XAUUSD',magic=42,
                     type=0,entry=0,volume=.01,price=4300.3,commission=-.04,fee=0,swap=0)
        self.history=NS(ticket=500,symbol='XAUUSD',magic=42,type=0,state=4,
                        volume_initial=.01,volume_current=0)
        self.mt=Mock(ORDER_STATE_FILLED=4,ORDER_STATE_CANCELED=2,
                     ORDER_STATE_REJECTED=5,ORDER_STATE_EXPIRED=6)
        self.mt.symbol_info.return_value=NS(name='XAUUSD',trade_contract_size=100)
        self.mt.positions_get.return_value=(self.position,)
        self.mt.history_deals_get.return_value=(self.deal,)
        self.mt.orders_get.side_effect=lambda **kw: (NS(ticket=500),) if clock.now<self.terminal_at else ()
        self.mt.history_orders_get.return_value=(self.history,)
        self.fills=NativeMT5Fills(self.mt,'XAUUSD',42)

    def submit(self, o):
        self.submitted.append(copy.deepcopy(o))
        if o['action']=='open' and o['leg']=='binance':
            self.clock.advance(1.057)
        if o['action']=='open' and o['leg']=='mt5':
            self.position.comment=self.deal.comment=o['id']
            self.clock.advance(.281)
            # This broker returns no deal ID and no price in its success ack.
            return self.fills.submitted(o,NS(retcode=10009,order=500,deal=0,volume=.01,price=0),100)
        return super().submit(o)

    def query(self, o):
        self.queries.append(copy.deepcopy(o));self.clock.advance(self.query_delay)
        if self.on_query: self.on_query()
        return self.fills.query(o)


class SecondConfirmationTests(unittest.TestCase):
    def setUp(self):
        temp=tempfile.TemporaryDirectory();self.addCleanup(temp.cleanup)
        self.store=Store(Path(temp.name)/'journal.db');self.addCleanup(self.store.close)
        self.clock=Clock();self.c=config();self.c['execution']['entry_leg']='binance'
        self.c['strategy']['max_unhedged_ms']=1500
        self.p=dict(qty=1,lots=.01,contract=100);self.b=NativeRaceBroker(self.clock)
        self.addCleanup(patch.stopall)
        patch('trading_engine.time.monotonic',lambda:self.clock.now).start()
        patch('trading_engine.stamp',self.clock.wall).start()
        self.e=Engine(self.store,self.b);self.e.start(self.c)

    def open_pending(self):
        self.e.tick(self.c,self.p,quote(self.c))
        self.g=self.e.state['groups'][0];self.o=self.e.state['orders'][1]

    def test_matching_native_fill_confirms_without_rollback_or_duplicate_submission(self):
        self.open_pending();deadline=self.e._entry_deadlines[self.g['id']]
        self.assertEqual(deadline,101.5)
        self.assertEqual(self.g['status'],'opening');self.assertTrue(self.e.state['enabled'])
        self.assertEqual(self.o['initial_result']['confirmation']['stage'],'order_pending')
        self.assertEqual(self.o['result']['qty'],0);self.assertNotIn('deal',self.o['result'])
        self.e.tick(self.c,self.p,quote(self.c));self.assertEqual(self.b.queries,[])
        self.clock.advance(.102);self.e.tick(self.c,self.p,quote(self.c))
        self.assertEqual(self.g['status'],'open');self.assertTrue(self.e.state['enabled'])
        self.assertEqual(len(self.b.submitted),2);self.assertEqual(len(self.b.queries),1)
        self.assertLessEqual(self.b.queries[0]['query_timeout_ms'],60)
        self.assertEqual(self.o['result']['price'],4300.3)
        self.assertEqual(self.o['result']['position'],'900')
        self.assertLess(self.clock.now,deadline);self.assertNotIn(self.g['id'],self.e._entry_deadlines)
        self.assertEqual(self.store.load()['groups'][0]['status'],'open')
        self.assertFalse(any(e['kind']=='pause' for e in self.store.events()))
        self.assertLess(self.g['execution_open']['elapsed_ms'],1500)

    def test_persistent_pending_reaches_original_deadline_and_pauses(self):
        self.b.terminal_at=1000;self.open_pending()
        self.clock.advance(.102);self.e.tick(self.c,self.p,quote(self.c))
        self.assertTrue(self.e.state['enabled']);self.assertEqual(self.g['status'],'opening')
        self.clock.now=101.501;self.e.tick(self.c,self.p,quote(self.c))
        self.assertFalse(self.e.state['enabled']);self.assertEqual(self.g['status'],'unwinding')
        self.assertEqual(self.e.state['last_pause']['reason_code'],'ENTRY_WINDOW_EXPIRED')
        self.assertEqual(len(self.b.submitted),2)

    def test_query_crossing_deadline_does_not_keep_pair_open(self):
        self.open_pending();self.b.query_delay=.08
        self.clock.advance(.102);self.e.tick(self.c,self.p,quote(self.c))
        self.assertEqual(self.g['status'],'closed');self.assertFalse(self.e.state['enabled'])
        self.assertEqual(len(self.b.submitted),4)
        self.assertEqual(self.e.state['last_pause']['reason_code'],'ENTRY_WINDOW_EXPIRED')

    def test_missing_quotes_cannot_leave_expired_confirmation_armed(self):
        self.open_pending();self.b.terminal_at=1000;self.clock.now=101.501
        self.e.reconcile_pending(self.c)
        self.assertFalse(self.e.state['enabled']);self.assertEqual(self.g['status'],'unwinding')
        self.assertEqual(len(self.b.submitted),2)
        self.assertEqual(self.store.load()['groups'][0]['status'],'unwinding')

    def test_late_fill_with_stale_quotes_is_paused_before_any_close(self):
        self.open_pending();self.clock.now=101.501
        q=quote(self.c)
        for leg in ('binance','mt5'): q[leg]['time_ms']-=60000
        self.e.tick(self.c,self.p,q)
        self.assertFalse(self.e.state['enabled']);self.assertEqual(self.g['status'],'unwinding')
        self.assertEqual(self.o['result']['status'],'done');self.assertEqual(len(self.b.submitted),2)
        self.e.tick(self.c,self.p,quote(self.c))
        self.assertEqual(self.g['status'],'closed');self.assertEqual(len(self.b.submitted),4)

    def test_terminal_partial_fill_still_protectively_unwinds(self):
        self.open_pending();self.b.deal.volume=.005
        self.b.history.state=2;self.b.history.volume_current=.005
        self.clock.advance(.102);self.e.tick(self.c,self.p,quote(self.c))
        self.assertFalse(self.e.state['enabled']);self.assertEqual(self.g['status'],'closed')
        self.assertEqual(self.o['result']['qty'],.5)
        self.assertEqual(self.e.state['last_pause']['reason_code'],'SECOND_LEG_INCOMPLETE')

    def test_manual_pause_invalidates_window_and_is_never_rearmed(self):
        self.open_pending();self.e.pause()
        self.clock.advance(.102);self.e.tick(self.c,self.p,quote(self.c))
        self.assertFalse(self.e.state['enabled']);self.assertEqual(self.g['status'],'closed')
        self.assertEqual(len(self.b.submitted),4)

    def test_restart_requires_reconciliation_and_cannot_resume_entry(self):
        self.open_pending();self.clock.advance(.102)
        restarted=Engine(self.store,self.b)
        restarted.tick(self.c,self.p,quote(self.c))
        self.assertTrue(restarted.state['recovery']);self.assertFalse(restarted.state['enabled'])
        self.assertEqual(len(self.b.submitted),2);self.assertEqual(restarted._entry_deadlines,{})

    def test_ack_without_matching_owned_deal_does_not_wait(self):
        self.b.deal.magic=99;self.open_pending()
        self.assertFalse(self.e.state['enabled']);self.assertEqual(self.g['status'],'unwinding')
        self.assertEqual(len(self.b.submitted),2)

    def test_unknown_partial_quantity_cannot_suppress_protection(self):
        self.open_pending();self.o['result'].update(qty=.5,price=4300.3)
        self.e.tick(self.c,self.p,quote(self.c))
        self.assertFalse(self.e.state['enabled']);self.assertEqual(self.g['status'],'unwinding')

    def test_normal_ack_path_has_no_confirmation_queries_or_extra_commits(self):
        self.b.fills.submitted=lambda o,ack,contract: dict(status='done',qty=1,price=4300.3,position='900')
        with patch.object(self.store,'commit',wraps=self.store.commit) as commits:
            self.open_pending()
        self.assertEqual(self.g['status'],'open');self.assertEqual(self.b.queries,[])
        self.assertEqual([c.args[1] for c in commits.call_args_list],
                         ['group_opening','order_intent','order_result','order_intent','order_result','group_opened'])


if __name__=='__main__': unittest.main()
