import copy
import io
import json
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import Mock, patch
from types import SimpleNamespace
from collections import namedtuple

import _test_runtime
from server import TradingRuntime
from trading_brokers import Binance, PaperBroker, worker_main, native_external_history
from trading_engine import Engine
from trading_store import Store
from trading_config import pair_key
from external_closes import ExternalCloseMonitor, fingerprint
from execution_diagnostics import build_report
from test_trading_engine import config, quote


class ExternalCloseTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.store=Store(Path(self.tmp.name)/'records.sqlite3');self.addCleanup(self.store.close)
        self.c=config();self.c['execution']['mode']='live';self.c['mt5']['adapter']='native'
        self.broker=Mock(wraps=PaperBroker());self.e=Engine(self.store,self.broker)
        self.e.start(self.c);self.plan=dict(qty=2,lots=.02,contract=100);self.q=quote(self.c)
        self.e.tick(self.c,self.plan,self.q);self.g=self.e.active()[0]
        self.g['opened_ms']=int(time.time()*1000)-10000
        self.at=self.g['opened_ms']+8000
        self.bo=next(o for o in self.e.state['orders'] if o['leg']=='binance')
        self.mo=next(o for o in self.e.state['orders'] if o['leg']=='mt5')
        self.bo['result']['ticket']='101'
        self.mo['result'].update(ticket='1001',position='800',position_identifier='900')
        for o in self.e.state['orders']:o['created_ms']=self.g['opened_ms']
        self.broker.reset_mock()
        self.r=TradingRuntime.__new__(TradingRuntime);self.r.lock=threading.RLock();self.r.stop_event=threading.Event()
        self.r.engine=self.e;self.r.config=self.c;self.r.connected=True;self.r.reconciled=True;self.r.quote=self.q
        self.r.external_close_status=None
        self.client=Mock();self.client.refresh_position_mode.return_value='one_way'
        self.client.positions.return_value=[];self.client.open_orders.return_value=[]
        self.client.query.side_effect=lambda o:copy.deepcopy(o['result'])
        self.client.commissions_usdt.side_effect=lambda rows:sum(float(f['commission']) for f in rows)
        self.fills=[dict(id='1',orderId='101',symbol=self.g['symbol'],side='SELL',positionSide='BOTH',
                         qty='2',price=str(self.bo['result']['price']),commission='.1',commissionAsset='USDT',time=self.g['opened_ms']+1),
                    dict(id='2',orderId='102',symbol=self.g['symbol'],side='BUY',positionSide='BOTH',
                         qty='2',price='4303',commission='.2',commissionAsset='USDT',time=self.at)]
        self.deals=[dict(ticket=11,order=1001,position_id=900,entry=0,type=0,magic=self.c['execution']['magic'],
                         comment=self.mo['id'],volume=.02,price=self.mo['result']['price'],commission=-.12,fee=0,swap=0,time_msc=self.g['opened_ms']+1),
                    dict(ticket=12,order=1002,position_id=900,entry=1,type=1,magic=0,
                         comment='manual close',volume=.02,price=4301,commission=-.13,fee=-.01,swap=-.4,time_msc=self.at)]
        self.mt5=dict(account=dict(account=self.c['mt5']['account'],server=self.c['mt5']['server']),positions=[],orders=[])
        self.terminal=Mock();self.factory=Mock(return_value=self.terminal)
        self.terminal.call.side_effect=lambda command,**kwargs:copy.deepcopy(self.mt5 if command=='snapshot' else self.deals)
        self.client.trade_history.side_effect=lambda *args:copy.deepcopy(self.fills)
        self.monitor=ExternalCloseMonitor(self.r,self.client,self.factory,start=False);self.r.external_monitor=self.monitor

    def mt_position(self,lots=.02):
        return dict(ticket='800',identifier='900',symbol=self.g['mt5_symbol'],lots=lots,side=0,
                    magic=self.c['execution']['magic'],comment=self.mo['id'],swap=-.2)

    def b_position(self,qty=-2,side='BOTH'):
        return dict(symbol=self.g['symbol'],positionSide=side,positionAmt=str(qty))

    def external(self):return [o for o in self.e.state['orders'] if o.get('source')=='external']

    def test_manual_both_flat_is_durable_actual_and_never_submits(self):
        self.monitor.step()
        self.assertEqual(self.g['status'],'closed');self.assertEqual(len(self.external()),2)
        self.assertFalse(self.e.state['enabled']);self.assertFalse(self.e.state['recovery']);self.assertTrue(self.r.reconciled)
        self.assertGreaterEqual(self.g['closed_ms'],self.at);self.assertAlmostEqual(self.g['exit'],2)
        self.assertEqual(self.e.amounts(self.g),dict(mt5=0,binance=0))
        self.assertEqual(self.store.load()['groups'][0]['status'],'closed')
        self.assertAlmostEqual(sum(o['result']['fee'] for o in self.external()),.34)
        for o in self.external():
            self.assertEqual(o['result']['platform_time_ms'],self.at)
            self.assertNotIn('dispatch_ms',o);self.assertNotIn('request_ms',o['result']);self.assertNotIn('signal_spread',o)
        self.assertNotIn('execution_close',self.g)
        self.broker.submit.assert_not_called();self.client.submit.assert_not_called()
        self.assertEqual([x[0][0] for x in self.terminal.call.call_args_list],['snapshot','external_history','snapshot'])

    def test_matching_positions_do_not_fetch_histories_or_pause_trading(self):
        self.client.positions.return_value=[self.b_position()];self.mt5['positions']=[self.mt_position()]
        self.monitor.step()
        self.client.trade_history.assert_not_called();self.client.query.assert_not_called()
        self.assertTrue(self.e.state['enabled']);self.assertFalse(self.e.state['recovery'])
        self.assertEqual(self.terminal.call.call_args.args,('snapshot',))

    def test_hedge_short_manual_close_and_split_fills_preserve_actual_price_and_fee(self):
        self.client.refresh_position_mode.return_value='hedge'
        for f in self.fills:f['positionSide']='SHORT'
        self.fills[1].update(qty='1',price='4302',commission='.1')
        self.fills.append(dict(self.fills[1],id='3',price='4304'))
        self.monitor.step();self.assertEqual(self.g['status'],'closed')
        b=next(o for o in self.external() if o['leg']=='binance')
        self.assertEqual(b['result']['price'],4303);self.assertEqual(b['external_deal_ids'],['2','3'])
        self.assertEqual(b['result']['fee'],.2)

    def test_broker_clock_offsets_preserve_raw_times_without_changing_ownership_or_confirmation_time(self):
        local=int(time.time()*1000)
        for sign in (-1,1):
            with self.subTest(offset=sign*7200000):
                from external_closes import propose
                deals=copy.deepcopy(self.deals)
                for d in deals:d['time_msc']+=sign*7200000
                proposals,errors=propose(self.e.state,self.c,'one_way',[],self.mt5,self.fills,deals,self.client.commissions_usdt)
                self.assertEqual(errors,[])
                mt=next(o for o in proposals if o['leg']=='mt5')
                self.assertEqual(mt['result']['platform_time_ms'],self.at+sign*7200000)
        self.deals[1]['time_msc']+=7200000
        self.monitor.step();self.assertLess(self.g['closed_ms'],local+10000)

    def test_opposite_direction_or_extra_program_position_blocks_backfill(self):
        self.client.positions.return_value=[self.b_position(2)]
        self.monitor.step();self.assertEqual(self.external(),[]);self.assertFalse(self.r.reconciled)
        self.client.positions.return_value=[]
        self.mt5['positions']=[dict(self.mt_position(),ticket='foreign',identifier='foreign')]
        self.monitor.step();self.assertEqual(self.external(),[])

    def test_original_zero_fill_order_is_never_relabeled_as_manual(self):
        self.e.state['orders'].append(dict(id='prior-program-close',group=self.g['id'],leg='binance',action='close',
                                           result=dict(status='done',qty=0,price=0,ticket='102')))
        self.monitor.step();self.assertFalse(any(o['leg']=='binance' for o in self.external()))

    def test_one_leg_manual_close_keeps_residual_and_blocks_automatic_trading(self):
        self.client.positions.return_value=[self.b_position()]
        self.fills=self.fills[:1]
        self.monitor.step()
        self.assertEqual(self.g['status'],'attention');self.assertEqual(len(self.external()),1)
        self.assertEqual(self.e.amounts(self.g),dict(mt5=0,binance=2))
        self.assertTrue(self.e.state['recovery']);self.assertFalse(self.r.reconciled)
        self.e.tick(self.c,self.plan,quote(self.c,bid=4303,ask=4303.1))
        self.broker.submit.assert_not_called();self.assertIn('剩余仓位',self.r.external_close_status['message'])

    def test_explicit_reconciliation_then_user_close_sends_only_the_remaining_leg(self):
        self.client.positions.return_value=[self.b_position()];self.monitor.step()
        self.r.binance=self.client;self.client.position_mode='one_way';self.r.terminal=self.terminal
        self.r.snapshot=Mock(return_value={})
        self.r.reconcile();self.assertTrue(self.r.reconciled);self.assertFalse(self.e.state['recovery'])
        self.e.request_close(self.g['id']);self.e.tick(self.c,self.plan,quote(self.c))
        self.assertEqual(self.g['status'],'closed')
        self.assertEqual([c.args[0]['leg'] for c in self.broker.submit.call_args_list],['binance'])

    def test_binance_partial_manual_close_keeps_mt5_and_remaining_swap(self):
        self.client.positions.return_value=[self.b_position(-1)];self.mt5['positions']=[self.mt_position()]
        self.deals=self.deals[:1];self.fills[1].update(qty='1',commission='.1')
        self.monitor.step()
        self.assertEqual(self.e.amounts(self.g),dict(binance=1,mt5=2));self.assertEqual(self.g['status'],'attention')
        self.assertEqual([o['leg'] for o in self.external()],['binance'])
        self.assertAlmostEqual(self.g['live_mt5_swap_usd'],-.2)

    def test_partial_mt5_then_remaining_platform_closes_is_idempotent(self):
        self.client.positions.return_value=[self.b_position()];self.mt5['positions']=[self.mt_position(.01)]
        self.deals[1]['volume']=.01;self.fills=self.fills[:1]
        self.monitor.step();self.assertEqual(self.e.amounts(self.g),dict(mt5=1,binance=2))
        self.monitor.step();self.assertEqual(len(self.external()),1)
        self.client.positions.return_value=[];self.mt5['positions']=[]
        self.deals.append(dict(self.deals[1],ticket=13,order=1003,time_msc=self.at+100))
        self.fills.append(dict(id='2',orderId='102',symbol=self.g['symbol'],side='BUY',positionSide='BOTH',qty='2',price='4303',commission='.2',commissionAsset='USDT',time=self.at+100))
        self.monitor.step();self.assertEqual(self.g['status'],'closed');self.assertEqual(len(self.external()),3)
        count=len(self.e.state['orders']);self.monitor.step();self.assertEqual(len(self.e.state['orders']),count)

    def test_delayed_history_never_invents_fills_then_recovers(self):
        deals,fills=self.deals,self.fills;self.deals=[];self.fills=[]
        self.monitor.step();self.assertEqual(self.external(),[]);self.assertTrue(self.e.state['recovery'])
        self.assertEqual(self.e.amounts(self.g),dict(mt5=2,binance=2))
        self.deals=deals;self.fills=fills;self.monitor.history_retry_at=0
        self.monitor.step();self.assertEqual(self.g['status'],'closed')

    def test_missing_history_uses_backoff_without_repeating_expensive_queries(self):
        self.deals=[];self.fills=[];self.monitor.step()
        count=self.client.trade_history.call_count
        native_count=sum(c.args[0]=='external_history' for c in self.terminal.call.call_args_list)
        self.monitor.step()
        self.assertEqual(self.client.trade_history.call_count,count)
        self.assertEqual(sum(c.args[0]=='external_history' for c in self.terminal.call.call_args_list),native_count)
        self.assertIn('稍后重试',self.r.external_close_status['message'])

    def test_wrong_mt5_account_or_open_order_never_backfills(self):
        self.mt5['account']['account']='different'
        self.monitor.step();self.assertEqual(self.external(),[]);self.assertFalse(self.e.state['enabled'])
        self.mt5['account']['account']=self.c['mt5']['account'];self.client.open_orders.return_value=[dict(orderId='pending')]
        self.monitor.step();self.assertEqual(self.external(),[]);self.client.trade_history.assert_not_called()

    def test_unknown_binance_client_id_cannot_use_another_accounts_fills(self):
        self.client.query.return_value=dict(status='unknown',qty=0,price=0);self.client.query.side_effect=None
        self.monitor.step()
        self.assertEqual([o['leg'] for o in self.external()],['mt5']);self.assertEqual(self.g['status'],'attention')
        self.assertFalse(self.r.reconciled);self.assertIn('归属',self.r.external_close_status['message'])

    def test_foreign_mt5_history_with_same_manual_comment_is_ignored(self):
        self.deals[1]['position_id']=999
        self.monitor.step();self.assertFalse(any(o['leg']=='mt5' for o in self.external()))
        self.assertEqual(self.e.amounts(self.g)['mt5'],2)

    def test_foreign_or_duplicate_history_does_not_get_fabricated_into_profit(self):
        self.fills.append(dict(self.fills[1],id='3',side='SELL'))
        self.deals.append(copy.deepcopy(self.deals[1]))
        self.monitor.step();self.assertEqual(self.external(),[]);self.assertEqual(self.g['status'],'open')

    def test_multi_group_binance_manual_close_is_never_arbitrarily_distributed(self):
        other=copy.deepcopy(self.g);other.update(id='other',status='open');self.e.state['groups'].append(other)
        bo=copy.deepcopy(self.bo);bo.update(id='other-b',group='other');bo['result']['ticket']='103'
        mo=copy.deepcopy(self.mo);mo.update(id='other-m',group='other');mo['result'].update(ticket='2001',position='1800',position_identifier='1900')
        self.e.state['orders'].extend([bo,mo])
        self.mt5['positions']=[dict(self.mt_position(),ticket='1800',identifier='1900',comment='other-m')]
        self.deals.append(dict(self.deals[0],ticket=21,order=2001,position_id=1900,comment='other-m'))
        self.monitor.step()
        self.assertEqual([o['leg'] for o in self.external()],['mt5'])
        self.assertIn('无法唯一归属',self.r.external_close_status['message']);self.assertTrue(self.e.state['recovery'])

    def test_restart_reuses_deal_ids_and_recovers_remaining_records(self):
        self.client.positions.return_value=[self.b_position()];self.monitor.step()
        self.e=Engine(self.store,self.broker);self.r.engine=self.e;self.g=self.e.active()[0]
        self.client.positions.return_value=[]
        self.monitor.step();self.assertEqual(self.g['status'],'closed');self.assertEqual(len(self.external()),2)

    def test_racing_local_execution_discards_observation(self):
        calls=0
        def positions(*args):
            nonlocal calls
            calls+=1
            if calls==2:self.bo['result']['qty']=1.9
            return []
        self.client.positions.side_effect=positions
        self.monitor.step();self.assertEqual(self.external(),[]);self.assertTrue(self.e.state['recovery'])

    def test_racing_platform_execution_waits_for_stable_snapshot(self):
        self.client.positions.side_effect=[[],[self.b_position(-1)]]
        self.monitor.step();self.assertEqual(self.external(),[]);self.assertIn('仍在变化',self.r.external_close_status['message'])

    def test_execution_lock_skips_all_network_reads(self):
        acquired=threading.Event();release=threading.Event()
        def hold():
            with self.r.lock:acquired.set();release.wait(2)
        thread=threading.Thread(target=hold);thread.start();acquired.wait(1)
        try:self.monitor.step();self.factory.assert_not_called();self.client.positions.assert_not_called()
        finally:release.set();thread.join()

    def test_unknown_original_order_uses_only_read_queries_and_keeps_recovery(self):
        self.bo['result']['status']='unknown'
        self.monitor.step()
        self.client.query.assert_called_once();self.client.positions.assert_not_called()
        self.client.trade_history.assert_not_called();self.terminal.call.assert_not_called()
        self.assertEqual(self.external(),[]);self.assertFalse(self.r.reconciled)
        self.assertTrue(self.g['external_attention']);self.assertTrue(self.e.state['recovery'])
        self.broker.submit.assert_not_called();self.client.submit.assert_not_called();self.client.cancel.assert_not_called()

    def test_restarted_unknown_open_resolves_before_manual_flat_history_backfill(self):
        self.g['status']='unwinding';self.e.state.update(recovery=True,enabled=False);self.r.reconciled=False
        known=copy.deepcopy(self.mo['result']);known.pop('position')
        self.mo['result']=dict(status='unknown',qty=0,price=0,ticket='1001')
        self.terminal.call.side_effect=lambda command,**kwargs:copy.deepcopy(
            known if command=='query' else self.mt5 if command=='snapshot' else self.deals)
        self.monitor.step()
        self.assertEqual(self.mo['result']['status'],'done');self.assertEqual(self.mo['result']['qty'],2)
        self.assertEqual(self.external(),[]);self.assertFalse(self.e.state['enabled'])
        self.monitor.step()
        self.assertEqual(self.g['status'],'closed');self.assertEqual(len(self.external()),2)
        self.assertEqual(self.e.amounts(self.g),dict(mt5=0,binance=0))
        self.assertTrue(self.r.reconciled);self.assertFalse(self.e.state['recovery']);self.assertFalse(self.e.state['enabled'])
        self.assertEqual(self.mo['result']['position_identifier'],'900');self.assertNotIn('position',self.mo['result'])
        self.broker.submit.assert_not_called();self.client.submit.assert_not_called();self.client.cancel.assert_not_called()
        self.assertEqual([x.args[0] for x in self.terminal.call.call_args_list],['query','snapshot','external_history','snapshot'])

    def test_recovery_statuses_include_settled_unwinding_and_closing(self):
        self.g['status']='closing';self.e.state['recovery']=True;self.r.reconciled=False
        self.monitor.step()
        self.assertEqual(self.g['status'],'closed');self.assertTrue(self.r.reconciled)
        self.broker.submit.assert_not_called()

    def test_closed_original_position_without_ticket_uses_proven_identifier(self):
        self.mo['result'].pop('position');self.mo['result'].pop('position_identifier')
        self.g['status']='unwinding';self.monitor.step()
        self.assertEqual(self.g['status'],'closed');self.assertEqual(self.mo['result']['position_identifier'],'900')
        self.assertEqual(self.e.amounts(self.g),dict(mt5=0,binance=0))

    def test_missing_mt5_order_ticket_uses_proven_position_identifier(self):
        # Some MT5 acknowledgements persist the immutable position identifier
        # but omit the original order ticket.  Manual close history is still
        # attributable to that position and must unblock a flat account.
        self.mo['result'].pop('ticket')
        self.monitor.step()
        self.assertEqual(self.g['status'],'closed')
        self.assertEqual([o['leg'] for o in self.external()],['mt5','binance'])
        self.assertTrue(self.r.reconciled)

    def test_missing_original_ticket_can_find_unique_owned_partial_position(self):
        self.mo['result'].pop('position');self.mo['result'].pop('position_identifier')
        self.mt5['positions']=[self.mt_position(.01)];self.deals[1]['volume']=.01
        self.monitor.step()
        self.assertEqual(self.mo['result']['position'],'800');self.assertEqual(self.g['status'],'attention')
        self.assertEqual(self.e.mt5_tickets(self.g),{'800':1});self.assertFalse(self.r.reconciled)
        self.broker.submit.assert_not_called()

    def test_rejected_mt5_leg_does_not_block_binance_only_unwind_backfill(self):
        self.mo['result'].update(qty=0,price=0);self.deals=[];self.g['status']='unwinding'
        self.monitor.step()
        self.assertEqual(self.g['status'],'closed');self.assertEqual([o['leg'] for o in self.external()],['binance'])
        self.assertEqual(self.e.amounts(self.g),dict(mt5=0,binance=0))

    def test_unknown_original_closes_resolve_flat_without_duplicate_external_fills(self):
        self.g['status']='closing';self.e.state.update(enabled=False,recovery=True);self.r.reconciled=False
        for opening in (self.bo,self.mo):
            self.e.state['orders'].append(dict(id='close-'+opening['id'],group=self.g['id'],leg=opening['leg'],
                action='close',requested=2,position=opening['result'].get('position'),
                result=dict(status='unknown',qty=0,price=0)))
        self.client.query.side_effect=lambda o:dict(status='done',qty=2,price=4303,ticket='102')
        self.terminal.call.side_effect=lambda command,**kwargs:copy.deepcopy(
            dict(status='done',qty=2,price=4301,ticket='1002',position='800') if command=='query' else self.mt5)
        self.monitor.step();self.assertEqual(self.g['status'],'closing')
        self.monitor.step()
        self.assertEqual(self.g['status'],'closed');self.assertEqual(self.external(),[])
        self.assertTrue(self.r.reconciled);self.assertFalse(self.e.state['enabled'])
        self.client.trade_history.assert_not_called();self.broker.submit.assert_not_called()
        report=build_report(self.e.state,self.store.events())
        events=[e for e in report['events'] if e['kind'] in ('external_orders_resolved','recovery_flat_confirmed')]
        self.assertEqual(len(events),2)
        self.assertNotIn(self.g['id'],json.dumps(events));self.assertNotIn(self.mo['id'],json.dumps(events))

    def test_retry_bookkeeping_does_not_discard_observation_but_fill_changes_do(self):
        token=fingerprint(self.e.state)
        self.mo['reconcile']=dict(attempts=10,next_ms=1234);self.mo['result']['error']='waiting'
        self.assertEqual(token,fingerprint(self.e.state))
        self.mo['result']['qty']=1
        self.assertFalse(self.r._external_publish(self.monitor,token,'checked',[],[]))

    def test_failed_original_query_never_turns_platform_absence_into_a_fill(self):
        self.bo['result']['status']='unknown';self.client.query.side_effect=ValueError('unavailable')
        self.monitor.step();self.monitor.step()
        self.assertEqual(self.bo['result']['status'],'unknown');self.assertEqual(self.external(),[])
        self.assertNotEqual(self.g['status'],'closed');self.assertFalse(self.r.reconciled)
        self.client.trade_history.assert_not_called();self.broker.submit.assert_not_called()

    def test_recovery_result_commit_failure_rolls_back_confirmed_order(self):
        self.bo['result']['status']='unknown';token=fingerprint(self.e.state)
        result=dict(self.bo['result'],status='done')
        with patch.object(self.store,'commit',side_effect=OSError('disk full')):
            with self.assertRaises(OSError):
                self.r._external_publish(self.monitor,token,'resolved',[dict(id=self.bo['id'],result=result)],[])
        self.assertEqual(self.e.state['orders'][0]['result']['status'],'unknown')
        self.assertTrue(self.e.state['recovery']);self.assertFalse(self.e.state['enabled']);self.assertFalse(self.r.reconciled)

    def test_stopped_or_disconnected_observer_rejects_late_results(self):
        token=fingerprint(self.e.state);self.r.connected=False
        self.assertFalse(self.r._external_publish(self.monitor,token,'checked',[],[]))
        self.r.connected=True;self.monitor.close()
        self.assertFalse(self.r._external_publish(self.monitor,token,'checked',[],[]))

    def test_explicit_reconcile_flat_zero_rows_requests_history_retry(self):
        self.g['status']='unwinding';self.r.binance=self.client;self.client.position_mode='one_way'
        self.r.terminal=self.terminal;self.r.snapshot=Mock(return_value={})
        self.client.positions.return_value=[self.b_position(0)]
        self.monitor.history_retry_at=time.monotonic()+60
        with self.assertRaisesRegex(ValueError,'双边平台当前空仓'):
            self.r.reconcile()
        self.assertTrue(self.monitor.retry_requested.is_set())
        self.monitor.step();self.assertEqual(self.g['status'],'closed')
        self.client.trade_history.assert_called_once();self.broker.submit.assert_not_called()

    def test_snapshot_freezes_unverified_group_but_projects_verified_open_group(self):
        self.r.store=self.store;self.r.plan=self.plan;self.r.last_error='';self.r.market_meta={}
        self.r.position_report=None;self.r.entry_preflight=None;self.r.binance=self.client
        self.g['valuation']=self.e.valuation(self.g,self.q)
        previous=copy.deepcopy(self.g['valuation']);self.r.quote=quote(self.c,bid=4310,ask=4310.1)
        self.r.reconciled=False
        with patch('server.load_config',return_value=copy.deepcopy(self.c)):
            pending=self.r.snapshot()['state']['groups'][0]
            self.assertEqual(pending['valuation_status'],'unverified');self.assertEqual(pending['valuation'],previous)
            self.r.reconciled=True
            current=self.r.snapshot()['state']['groups'][0]
            self.assertEqual(current['valuation_status'],'estimate');self.assertNotEqual(current['valuation']['net'],previous['net'])
            for q in (None,dict(self.q,key='other-market')):
                self.r.quote=q
                row=self.r.snapshot()['state']['groups'][0]
                self.assertEqual(row['valuation_status'],'unverified');self.assertEqual(row['valuation'],previous)
        self.assertEqual(self.g['valuation'],previous);self.client.positions.assert_not_called();self.broker.submit.assert_not_called()

    def test_append_commit_failure_rolls_back_fills_but_keeps_safety_pause(self):
        token=fingerprint(self.e.state);self.r._external_publish(self.monitor,token,'detected',[],[])
        from external_closes import propose
        proposals,errors=propose(self.e.state,self.c,'one_way',[],self.mt5,self.fills,self.deals,self.client.commissions_usdt)
        with patch.object(self.store,'commit',side_effect=OSError('disk full')):
            with self.assertRaises(OSError):self.r._external_publish(self.monitor,token,'backfill',proposals,errors)
        self.assertEqual(self.external(),[]);self.assertTrue(self.e.state['recovery']);self.assertFalse(self.e.state['enabled'])

    def test_fee_verification_uses_exact_external_fees_and_swap_without_new_external_history(self):
        self.monitor.step()
        self.r.binance=Mock();self.r.binance.trades.return_value=[dict(qty='2',commission='.1')]
        self.r.binance.commissions_usdt.side_effect=self.client.commissions_usdt.side_effect
        self.r.binance.income.return_value=[];self.r.terminal=Mock();self.r.terminal.call.return_value=[self.deals[0]]
        self.r._verify_closed_costs()
        self.assertTrue(self.g['costs_verified']);self.assertAlmostEqual(self.g['valuation']['fees'],.56)
        self.assertAlmostEqual(self.g['mt5_swap_usd'],-.4)
        self.r.binance.trades.assert_called_once_with(self.g['symbol'],'101')
        self.assertEqual(self.r.terminal.call.call_args.args,('history',))

    def test_diagnostics_mark_external_source_without_leaking_deal_or_position_ids(self):
        self.monitor.step();report=build_report(self.e.state,self.store.events())
        rows=[o for o in report['orders'] if o['source']=='external'];self.assertEqual(len(rows),2)
        for o in rows:
            self.assertEqual(o['result']['platform_time_ms'],self.at)
            self.assertNotIn('external_deal_ids',o);self.assertNotIn('ticket',o['result']);self.assertNotIn('position',o)
            self.assertNotIn('dispatch_ms',o)
        self.assertTrue(any(e['kind']=='external_close_backfilled' for e in report['events']))


class TradeHistoryTests(unittest.TestCase):
    def test_native_external_history_uses_order_and_position_ids_not_local_dates_or_manual_magic(self):
        Deal=namedtuple('Deal','ticket order position_id symbol magic')
        opening=Deal(11,1001,900,'XAUUSD',123);manual=Deal(12,1002,900,'XAUUSD',0)
        mt=SimpleNamespace(history_deals_get=Mock(side_effect=[(opening,),(opening,manual)]))
        result=native_external_history(mt,'XAUUSD',[dict(order_ticket='1001',identifier='900')])
        self.assertEqual([d['magic'] for d in result],[123,0])
        self.assertEqual(mt.history_deals_get.call_args_list[0].kwargs,dict(ticket=1001))
        self.assertEqual(mt.history_deals_get.call_args_list[1].kwargs,dict(position=900))

    def test_native_external_history_rejects_wrong_position_identifier(self):
        Deal=namedtuple('Deal','order position_id symbol')
        mt=SimpleNamespace(history_deals_get=Mock(return_value=(Deal(1001,999,'XAUUSD'),)))
        with self.assertRaises(ValueError):native_external_history(mt,'XAUUSD',[dict(order_ticket=1001,identifier=900)])

    def test_native_external_history_can_query_position_when_order_ticket_missing(self):
        Deal=namedtuple('Deal','ticket order position_id symbol magic entry type comment')
        opening=Deal(11,1001,900,'XAUUSD',123,0,0,'strategy-open')
        closing=Deal(12,1002,900,'XAUUSD',0,1,1,'manual close')
        mt=SimpleNamespace(history_deals_get=Mock(side_effect=[(opening,closing)]))
        result=native_external_history(mt,'XAUUSD',[dict(order_ticket=None,identifier=900)])
        self.assertEqual([d['ticket'] for d in result],[11,12])
        self.assertEqual(mt.history_deals_get.call_args_list[0].kwargs,dict(position=900))

    def test_native_external_history_falls_back_when_ticket_filter_is_unavailable(self):
        Deal=namedtuple('Deal','ticket order position_id symbol magic entry type comment')
        opening=Deal(11,1001,900,'XAUUSD',123,0,0,'strategy-open')
        closing=Deal(12,1002,900,'XAUUSD',0,1,1,'manual close')
        mt=SimpleNamespace(history_deals_get=Mock(side_effect=[None,(opening,closing),(opening,closing)]))
        result=native_external_history(mt,'XAUUSD',[dict(order_ticket=1001,identifier=900)])
        self.assertEqual([d['ticket'] for d in result],[11,12])
        self.assertEqual(mt.history_deals_get.call_args_list[1].kwargs,dict(position=900))

    def test_read_only_native_worker_rejects_order_submission_before_native_calls(self):
        mt=SimpleNamespace(initialize=Mock(return_value=True),shutdown=Mock(),symbol_select=Mock(),order_send=Mock(),
                           symbol_info=Mock(return_value=SimpleNamespace(name='XAUUSD')),
                           account_info=lambda:SimpleNamespace(login=42,server='test'),
                           terminal_info=lambda:SimpleNamespace(connected=True))
        commands=[dict(command='init',settings=dict(account='42',server='test',symbol='XAUUSD'),magic=1,read_only=True),
                  dict(command='query',order={'id':'original-intent'}),dict(command='submit',order={}),dict(command='cancel',order={})]
        output=io.StringIO()
        with patch.dict('sys.modules',MetaTrader5=mt),patch('sys.stdin',io.StringIO('\n'.join(json.dumps(c) for c in commands))),patch('sys.stdout',output),patch('trading_brokers.NativeMT5Fills.query',return_value=dict(status='unknown',qty=0,price=0)) as query:
            worker_main()
        results=[json.loads(line) for line in output.getvalue().splitlines()]
        self.assertTrue(results[0]['ok']);self.assertTrue(results[1]['ok']);query.assert_called_once_with({'id':'original-intent'})
        for row in results[2:]:self.assertFalse(row['ok']);self.assertIn('只允许读取',row['error'])
        mt.order_send.assert_not_called()

    def test_full_pages_are_split_with_no_truncation(self):
        b=Binance.__new__(Binance)
        b.request=Mock(side_effect=[[dict(id=i) for i in range(1000)],[dict(id=1)],[dict(id=2)]])
        self.assertEqual(b.trade_history('XAUUSDT',0,10),[dict(id=1),dict(id=2)])
        self.assertEqual([(c.args[1]['startTime'],c.args[1]['endTime']) for c in b.request.call_args_list],[(0,10),(0,5),(6,10)])

    def test_full_same_millisecond_page_is_explicitly_incomplete(self):
        b=Binance.__new__(Binance);b.request=Mock(return_value=[{}]*1000)
        with self.assertRaises(ValueError):b.trade_history('XAUUSDT',10,10)

    def test_history_range_is_bounded_and_never_calls_order_endpoints(self):
        b=Binance.__new__(Binance);b.request=Mock(return_value=[])
        b.trade_history('XAUUSDT',0,8*86400000)
        self.assertEqual(b.request.call_count,2)
        self.assertTrue(all(c.args[0]=='/fapi/v1/userTrades' for c in b.request.call_args_list))
        with self.assertRaises(ValueError):b.trade_history('XAUUSDT',0,181*86400000)


if __name__=='__main__':unittest.main()
