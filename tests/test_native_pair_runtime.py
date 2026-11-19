import copy
import tempfile
import threading
import unittest
from pathlib import Path
from types import SimpleNamespace as NS
from unittest.mock import Mock, patch

import _test_runtime
from server import TradingRuntime
from native_pair import NativePairObserver, NativePairPump
from trading_engine import Engine
from trading_brokers import PaperBroker
from trading_store import Store
from execution_latency import LatencyStats
from market_push import QuoteEvents, TickFreshness
from test_pair_combinations import native_config, specs
from pair_instruments import equal_lot_plan
from test_trading_engine import quote


class NativePairRuntimeTests(unittest.TestCase):
    def setUp(self):
        tmp=tempfile.TemporaryDirectory();self.addCleanup(tmp.cleanup)
        self.store=Store(Path(tmp.name)/'test.db');self.addCleanup(self.store.close)
        r=self.r=TradingRuntime.__new__(TradingRuntime)
        r.lock=threading.RLock();r.config=native_config();r.config['strategy']['mt5_lots']=.01
        r.config['execution']['mode']='live'
        r.plan=equal_lot_plan(r.config,specs());r.engine=Engine(self.store,PaperBroker());r.store=self.store
        r.engine.start(r.config);r.quote=quote(r.config)
        r.engine.tick(r.config,r.plan,r.quote);self.g=r.engine.active()[0]
        self.g['mode']='live';r.config['execution']['mode']='live'
        # Bind actual immutable platform identities, as native broker acknowledgements do.
        self.opens={o['leg']:o for o in r.engine.state['orders']}
        self.snaps={};self.deals={}
        for i,leg in enumerate(('binance','mt5')):
            o=self.opens[leg];side=1 if leg=='binance' else 0;name=o['symbol']
            o['result'].update(ticket=str(500+i),position=str(900+i),position_identifier=str(700+i))
            p=dict(ticket=str(900+i),identifier=str(700+i),symbol=name,magic=r.config['execution']['magic'],
                   lots=.01,side=side,comment=o['id'],swap=0,profit=0)
            self.snaps[name]=dict(positions=[p],orders=[],spec={'name':name},allowed=True,
                                  account=dict(account=r.config['mt5'].get('account'),server=r.config['mt5'].get('server'),
                                               currency='USD',margin_mode=2))
            self.deals[name]=[dict(ticket=str(600+i),order=str(500+i),position_id=str(700+i),symbol=name,
                                  magic=r.config['execution']['magic'],entry=0,type=side,comment=o['id'],
                                  volume=.01,price=o['result']['price'],commission=-.03,fee=0,swap=0,
                                  time_msc=self.g['opened_ms'],reason=3)]
        self.store.commit(r.engine.state,'test_native_identity')
        r.config['pair']['id']='same-terminal';r.connected=True;r.reconciled=True
        r.market_wake=threading.Event();r.market_meta={};r.external_close_status=None
        r.terminal=Mock();r.terminal.call.side_effect=self.native_call
        r.position_report=None;r.stop_event=threading.Event();r.latency=LatencyStats()
        self.observer=NativePairObserver(r);r.maintenance=self.observer
        self.observer.terminals={'history':r.terminal,'account':r.terminal}

    def native_call(self,command,**args):
        name=args.get('symbol',self.r.config['mt5']['symbol'])
        if command=='snapshot':return copy.deepcopy(self.snaps[name])
        if command=='external_history':return copy.deepcopy(self.deals[name])
        if command=='query':
            o=args['order'];return dict(status='done',qty=o['requested'],price=o['result']['price'],fee=.03,fee_verified=True,
                                       position=o['result']['position'],position_identifier=o['result']['position_identifier'])
        raise AssertionError(command)

    def manual_close(self,leg,reason=0):
        o=self.opens[leg];name=o['symbol'];original=self.deals[name][0]
        self.deals[name].append({**original,'ticket':str(800+(0 if leg=='binance' else 1)),
                                'order':str(1000+(0 if leg=='binance' else 1)),'entry':1,'type':1-original['type'],
                                'magic':0,'comment':'manual','time_msc':self.g['opened_ms']+100,'reason':reason})
        self.snaps[name]['positions']=[]

    def test_external_both_flat_backfills_two_legs_and_freezes_group(self):
        self.manual_close('binance');self.manual_close('mt5')
        self.observer.step('history')
        self.assertEqual(self.g['status'],'closed');self.assertFalse(self.r.engine.state['enabled'])
        rows=[o for o in self.r.engine.state['orders'] if o.get('source')=='external']
        self.assertEqual({o['leg'] for o in rows},{'binance','mt5'})
        self.assertTrue(all(o['quantity_unit']=='lot' and o['result']['qty']==.01 for o in rows))
        self.assertTrue(self.g['costs_verified'])
        self.r.terminal.call.reset_mock();self.observer.step('history')
        self.assertEqual(len([o for o in self.r.engine.state['orders'] if o.get('source')=='external']),2)

    def test_short_leg_stop_out_arms_protective_long_close_only_after_history_proof(self):
        self.manual_close('binance',reason=6);self.observer.step('history')
        self.assertEqual(self.g['status'],'unwinding')
        self.assertIn('强平',self.g['reason']);self.assertFalse(self.r.engine.state['enabled'])
        self.assertEqual(self.r.engine.amounts(self.g),dict(binance=0,mt5=.01))

    def test_reboot_stop_out_can_protect_only_after_full_remaining_ownership_proof(self):
        self.r.reconciled=False;self.r.engine.state['recovery']=True
        self.manual_close('binance',reason=6);self.observer.step('history')
        self.assertTrue(self.r.reconciled);self.assertFalse(self.r.engine.state['recovery'])
        self.assertEqual(self.g['status'],'unwinding');self.assertIn('liquidation_recovery',self.g)

    def test_proven_stop_out_closes_only_the_remaining_original_leg(self):
        self.manual_close('binance',reason=6);self.observer.step('history')
        self.r.engine.tick(self.r.config,self.r.plan,self.r.quote)
        submitted=[o for o in self.r.engine.state['orders'] if o['action']=='close' and o.get('source')!='external']
        self.assertEqual(len(submitted),1);self.assertEqual(submitted[0]['leg'],'mt5')
        self.assertEqual(submitted[0]['opening_id'],self.opens['mt5']['id'])
        self.assertEqual(submitted[0]['slippage_mode'],'protective');self.assertEqual(self.g['status'],'closed')

    def test_stop_out_does_not_authorize_unknown_or_wrong_remaining_ownership(self):
        self.r.reconciled=False;self.r.engine.state['recovery']=True
        self.manual_close('binance',reason=6)
        self.snaps[self.opens['mt5']['symbol']]['positions'][0]['magic']=1
        self.observer.step('history')
        self.assertFalse(self.r.reconciled);self.assertTrue(self.r.engine.state['recovery'])
        self.assertNotEqual(self.g['status'],'unwinding')

    def test_manual_one_leg_close_is_not_falsely_reported_as_liquidation(self):
        self.manual_close('mt5',reason=0);self.observer.step('history')
        self.assertEqual(self.g['status'],'attention');self.assertNotIn('强平',self.g['reason'])

    def test_empty_history_does_not_erase_positions_or_mark_flat(self):
        self.manual_close('mt5');self.deals[self.opens['mt5']['symbol']]=[]
        self.observer.step('history')
        self.assertEqual(self.r.engine.amounts(self.g),dict(binance=.01,mt5=.01))
        self.assertNotEqual(self.g['status'],'closed')

    def test_missing_history_preserves_previously_verified_carry(self):
        self.g['native_swap_usd']=-.4
        self.deals[self.opens['mt5']['symbol']]=[]
        self.observer.step('history')
        self.assertEqual(self.g['native_swap_usd'],-.4)

    def test_position_change_pauses_entries_while_history_is_missing(self):
        self.manual_close('mt5');self.deals[self.opens['mt5']['symbol']]=[]
        self.observer.step('account');self.observer.step('history')
        self.assertFalse(self.r.engine.state['enabled'])
        self.assertNotEqual(self.g['status'],'closed')
        self.assertEqual(self.r.engine.amounts(self.g),dict(binance=.01,mt5=.01))

    def test_unowned_position_also_stops_new_entries(self):
        rows=self.snaps[self.opens['mt5']['symbol']]['positions']
        rows.append({**rows[0],'ticket':'other-ticket','magic':0,'comment':'manual'})
        self.observer.step('account')
        self.assertFalse(self.r.engine.state['enabled'])

    def test_account_observer_is_read_only_and_separate_from_history(self):
        self.observer.step('account')
        self.assertEqual([call.args[0] for call in self.r.terminal.call.call_args_list],['snapshot','snapshot'])
        self.assertIsNotNone(self.r._last_mt5_snapshot_at)

    def test_history_publish_rejects_changed_order_evidence(self):
        self.manual_close('mt5');original=self.native_call
        def changed(command,**args):
            if command=='external_history':self.opens['binance']['result']['price']+=1
            return original(command,**args)
        self.r.terminal.call.side_effect=changed;self.observer.step('history')
        self.assertFalse(any(o.get('source')=='external' for o in self.r.engine.state['orders']))

    def test_external_history_rechecks_platform_exposure_before_backfill(self):
        self.manual_close('mt5');original=self.native_call;scans=0
        def changed(command,**args):
            nonlocal scans
            if command=='snapshot':
                scans+=1
                if scans>2:self.snaps[self.opens['mt5']['symbol']]['orders']=[{'ticket':'new-pending'}]
            return original(command,**args)
        self.r.terminal.call.side_effect=changed
        with self.assertRaisesRegex(ValueError,'核对期间变化'):self.observer.step('history')
        self.assertFalse(any(o.get('source')=='external' for o in self.r.engine.state['orders']))

    def test_unknown_original_is_not_replaced_by_external_close(self):
        self.manual_close('mt5');self.opens['mt5']['result']['status']='unknown'
        self.observer.step('history')
        self.assertFalse(any(o.get('source')=='external' for o in self.r.engine.state['orders']))

    def test_margin_adds_both_legs_and_does_not_assume_broker_hedge_discount(self):
        self.r.engine.state['groups']=[]
        self.r.terminal.call.side_effect=lambda command,**args:dict(required=10 if args['native_position_side']==1 else 20,available=25)
        margin=self.r._native_margin()
        self.assertEqual(margin['required'],30);self.assertFalse(margin['ready'])

    def test_quote_outage_still_queries_pending_originals(self):
        self.r.pump=Mock();self.r.pump.read.return_value=(None,0,'quote unavailable')
        self.r.engine.reconcile_pending=Mock()
        with self.assertRaisesRegex(ValueError,'quote unavailable'):self.r._native_poll()
        self.r.engine.reconcile_pending.assert_called_once_with(self.r.config)

    def test_order_quote_refresh_does_not_write_chart_or_query_account(self):
        self.r.config['execution']['mode']='paper';self.r.mt5_freshness=TickFreshness();self.r.push_bridge=None
        self.r.quote_events=QuoteEvents();reader=Mock();now=self.r.quote['time_ms']
        reader.call.return_value={name:dict(bid=4300,ask=4300.2,time_ms=now) for name in self.snaps}
        pump=NativePairPump(self.r,reader)
        with patch.object(self.store,'sample') as sample:
            q=pump.refresh(publish=False)
        sample.assert_not_called();reader.call.assert_called_once_with('pair_quotes')
        self.assertEqual(q['pair']['type'],'mt5_mt5')

    def test_quote_publish_wakes_strategy_without_chart_disk_io_and_coalesces(self):
        self.r.push_bridge=None;self.r.quote_events=QuoteEvents();self.r.market_wake=threading.Event()
        pump=NativePairPump(self.r,Mock())
        q=copy.deepcopy(self.r.quote)
        with patch.object(self.store,'sample',side_effect=AssertionError('disk on quote thread')):
            pump.publish(q)
            q2=copy.deepcopy(q);q2['binance']['bid']+=.1
            pump.publish(q2)
        self.assertTrue(self.r.market_wake.is_set())
        self.assertEqual(pump.read()[1],2)
        self.assertEqual(pump.pending_sample,q2)

    def test_chart_write_error_is_visible_and_wakes_runtime(self):
        self.r.push_bridge=None;self.r.market_wake=threading.Event()
        pump=NativePairPump(self.r,Mock());pump.pending_sample=self.r.quote
        def fail(q):pump.stopped.set();raise OSError('disk full')
        with patch.object(self.store,'sample',side_effect=fail):pump._samples()
        self.assertIn('行情历史写入失败',pump.read()[2])
        self.assertTrue(self.r.market_wake.is_set())


class NativePairConnectionTests(unittest.TestCase):
    def setUp(self):
        tmp=tempfile.TemporaryDirectory();self.addCleanup(tmp.cleanup)
        self.store=Store(Path(tmp.name)/'connect.db');self.addCleanup(self.store.close)
        r=self.r=TradingRuntime.__new__(TradingRuntime)
        r.lock=threading.RLock();r.store=self.store;r.engine=Engine(self.store,PaperBroker())
        r.connected=False;r.reconciled=False;r.config=None;r.quote=None;r.plan=None;r.last_error=''
        for key in ('terminal','pump','maintenance','external_monitor','market_stream','entry_preflight','risk_monitor',
                    'push_bridge','binance','position_report','adoption_preview','external_close_status'):
            setattr(r,key,None)
        r.market_meta={};r.stop_event=threading.Event();r.market_wake=threading.Event()
        r.mt5_freshness=TickFreshness();r.quote_events=QuoteEvents();r.latency=LatencyStats()
        r._restore_push_bridge=Mock()
        self.c=native_config();self.c['strategy']['mt5_lots']=.01;self.c['execution']['mode']='live'
        self.created=[];self.commands=[];self.clock=0
        self.factory=self.patch('native_pair.Terminal',side_effect=self.make_terminal)
        self.patch('native_pair.NativePairPump.start')
        self.patch('native_pair.NativePairObserver.start')
        self.patch('server.load_config',return_value=self.c)
        self.addCleanup(r._disconnect_pair_resources)

    def patch(self,name,**kw):
        p=patch(name,**kw);result=p.start();self.addCleanup(p.stop);return result

    def make_terminal(self,*args,**kw):
        terminal=Mock();terminal.call.side_effect=self.call;self.created.append(terminal);return terminal

    def call(self,command,**kw):
        self.commands.append((command,kw))
        if command=='pair_spec':return next(s for s in specs().values() if s['name']==kw['symbol'])
        if command=='snapshot':return dict(account=dict(currency='USD',margin_mode=2),allowed=True,positions=[],orders=[])
        if command=='pair_quotes':return {s['name']:dict(bid=100,ask=101,time_ms=self.clock+1000) for s in specs().values()}
        raise AssertionError('Unexpected native operation: '+command)

    def test_pending_quote_keeps_status_responsive_and_blocks_start_or_duplicate_connect(self):
        entered=threading.Event();release=threading.Event();result=[];errors=[];original=self.call
        def call(command,**kw):
            if command=='pair_quotes':
                entered.set()
                if not release.wait(3):raise ValueError('test quote timed out')
            return original(command,**kw)
        self.call=call
        def connect():
            try:result.append(self.r._native_connect(self.c))
            except Exception as exc:errors.append(exc)
        t=threading.Thread(target=connect);t.start()
        try:
            self.assertTrue(entered.wait(1))
            read=[];reader=threading.Thread(target=lambda:read.append(self.r.snapshot()));reader.start();reader.join(.5)
            self.assertFalse(reader.is_alive(),'status must not wait for native quote retrieval')
            self.assertFalse(read[0]['connected'])
            self.assertEqual(read[0]['connection_attempt']['stage'],'读取双品种首轮报价')
            self.assertNotIn('双品种已连接',read[0]['state']['alarm'])
            with self.assertRaisesRegex(ValueError,'仍在连接'):self.r._native_connect(self.c)
            with self.assertRaisesRegex(ValueError,'仍在连接'):self.r.start(self.c)
        finally:release.set();t.join(3)
        self.assertFalse(errors);self.assertTrue(result[0]['connected']);self.assertFalse(result[0]['capabilities']['reconciled'])
        self.assertEqual(result[0]['connection_attempt']['state'],'connected')
        self.assertEqual([x[0] for x in self.commands],['pair_spec','pair_spec','snapshot','pair_quotes'])
        self.assertFalse(self.r.engine.state['enabled'])

    def test_first_quote_failure_releases_both_workers_and_keeps_truthful_status(self):
        original=self.call
        def call(command,**kw):
            if command=='pair_quotes':raise ValueError('quote timeout')
            return original(command,**kw)
        self.call=call
        with self.assertRaisesRegex(ValueError,'读取双品种首轮报价.*quote timeout'):self.r._native_connect(self.c)
        status=self.r.snapshot()
        self.assertFalse(status['connected']);self.assertEqual(status['connection_attempt']['state'],'failed')
        self.assertNotIn('双品种已连接',status['state']['alarm'])
        self.assertTrue(all(t.close.called for t in self.created));self.assertIsNone(self.r.pump)
        self.call=original
        self.assertTrue(self.r._native_connect(self.c)['connected'])

    def test_failed_replacement_keeps_existing_owned_position_management(self):
        old_terminal=Mock();old_pump=Mock();old_broker=self.r.engine.broker
        self.r.terminal=old_terminal;self.r.pump=old_pump;self.r.config=copy.deepcopy(self.c);self.r.connected=True
        self.call=lambda command,**kw:(_ for _ in ()).throw(ValueError('spec unavailable'))
        with self.assertRaisesRegex(ValueError,'核验品种 A 规格'):self.r._native_connect(self.c)
        self.assertTrue(self.r.connected);self.assertIs(self.r.terminal,old_terminal);self.assertIs(self.r.pump,old_pump)
        self.assertIs(self.r.engine.broker,old_broker);old_terminal.close.assert_not_called();old_pump.close.assert_not_called()
        self.created[0].close.assert_called_once()

    def test_initialization_calls_share_budget_without_changing_order_timeouts(self):
        original=self.call
        def make(*args,**kw):
            self.clock+=9;return self.make_terminal(*args,**kw)
        def call(command,**kw):
            self.clock+=9;return original(command,**kw)
        self.factory.side_effect=make;self.call=call
        self.patch('native_pair.time',new=NS(monotonic=lambda:self.clock))
        with self.assertRaisesRegex(ValueError,'读取账户与交易权限.*25 秒'):self.r._native_connect(self.c)
        self.assertEqual(len(self.created),1);self.created[0].close.assert_called_once()
        self.assertEqual([kw['timeout_seconds'] for _,kw in self.commands],[12,7])
        self.assertEqual(self.r.connection_attempt['elapsed_ms'],27000)
        self.assertFalse(self.r.connected)

    def test_failure_during_publish_does_not_leave_success_alarm_or_workers(self):
        with patch.object(self.store,'sample',side_effect=ValueError('disk unavailable')):
            with self.assertRaisesRegex(ValueError,'启用行情与后台观察'):self.r._native_connect(self.c)
        self.assertFalse(self.r.connected);self.assertEqual(self.r.connection_attempt['state'],'failed')
        self.assertNotIn('双品种已连接',self.r.engine.state['alarm'])
        self.assertTrue(all(t.close.called for t in self.created));self.assertIsNone(self.r.pump)

    def test_full_disk_during_pause_still_releases_both_prepared_workers(self):
        import sqlite3
        with patch.object(self.store,'commit',side_effect=sqlite3.OperationalError('database or disk is full')):
            with self.assertRaisesRegex(ValueError,'disk is full'):self.r._native_connect(self.c)
        self.assertFalse(self.r.connected);self.assertIsNone(self.r.pump)
        self.assertFalse(self.r.engine.state['enabled'])
        self.assertEqual(self.r.connection_attempt['state'],'failed')
        self.assertTrue(all(t.close.called for t in self.created))

    def test_prepared_quote_reader_is_closed_if_bridge_setup_fails_before_install(self):
        self.r._restore_push_bridge.side_effect=ValueError('bridge unavailable')
        with self.assertRaisesRegex(ValueError,'启用行情与后台观察'):self.r._native_connect(self.c)
        self.assertTrue(all(t.close.called for t in self.created));self.assertFalse(self.r.connected)

    def test_owned_group_contracts_are_checked_again_before_replacing_runtime(self):
        from trading_config import pair_key
        self.r.engine.active=Mock(side_effect=[[],[dict(key=pair_key(self.c),leg_contracts={'binance':1,'mt5':1})]])
        with self.assertRaisesRegex(ValueError,'合约大小不符'):self.r._native_connect(self.c)
        self.assertTrue(all(t.close.called for t in self.created));self.assertFalse(self.r.connected)
        self.assertIsNone(self.r.pump)


if __name__=='__main__':unittest.main()
