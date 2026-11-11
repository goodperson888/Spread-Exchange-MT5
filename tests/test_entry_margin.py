"""Insufficient opening funds must never strand recovery or position management."""
import copy
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import _test_runtime
import server
from execution_diagnostics import build_report
from market_push import TickFreshness
from trading_brokers import PaperBroker
from trading_engine import Engine, stamp
from trading_store import Store
from test_trading_engine import config, quote


class EntryMarginTests(unittest.TestCase):
    def setUp(self):
        tmp=tempfile.TemporaryDirectory();self.addCleanup(tmp.cleanup)
        self.store=Store(Path(tmp.name)/'state.db');self.addCleanup(self.store.close)
        self.c=config();self.c['execution']['mode']='live';self.c['mt5']['adapter']='native'
        self.c['costs'].update(usdt_usd_auto=False,binance_fee_auto=False)
        self.r=server.TradingRuntime.__new__(server.TradingRuntime)
        self.r.lock=threading.RLock();self.r.engine=Engine(self.store,PaperBroker());self.r.store=self.store
        self.r.connected=False;self.r.reconciled=False;self.r.config=None
        for key in ('pump','terminal','market_stream','entry_preflight','risk_monitor','external_monitor','push_bridge'):
            setattr(self.r,key,None)
        self.r.quote_events=Mock();self.r.mt5_freshness=TickFreshness();self.r.market_wake=threading.Event()
        self.r.snapshot=lambda:dict(connected=self.r.connected,state=self.r.engine.state,
            auto_values=self.r.market_meta,capabilities=dict(reconciled=self.r.reconciled))
        self.exchange=Mock(position_mode='hedge')
        self.exchange.refresh_position_mode.return_value='hedge'
        self.exchange.spec.return_value=dict(status='TRADING',baseAsset='XAU',quoteAsset='USDT',filters={
            k:dict(minQty='.001',maxQty='1000',stepSize='.001') for k in ('LOT_SIZE','MARKET_LOT_SIZE')})
        self.terminal=Mock();self.margin=dict(required=100.,available=1.)
        self.platform=dict(allowed=True,account=dict(currency='USD',margin_mode=2),orders=[],positions=[],
            spec=dict(name=self.c['mt5']['symbol'],contract_size_oz=100,volume_min=.01,volume_max=100,volume_step=.01),
            quote=dict(bid=4300,ask=4300.3,time_ms=stamp()))
        def call(command,**kwargs):
            if command=='snapshot':return copy.deepcopy(self.platform)
            if command=='margin':
                if isinstance(self.margin,Exception): raise self.margin
                return self.margin
            raise AssertionError('Unexpected terminal command: '+command)
        self.terminal.call.side_effect=call
        self.stream=Mock();self.stream.quote.return_value=dict(bid=4306,ask=4306.1,time_ms=stamp())
        patches=dict(Binance=Mock(return_value=self.exchange),Terminal=Mock(return_value=self.terminal),
            BinanceBookTicker=Mock(return_value=Mock(start=Mock(return_value=self.stream))),
            EntryPreflight=Mock(),BinanceRiskMonitor=Mock(),QuotePump=Mock(),checked_spec=Mock())
        for name,value in patches.items():
            p=patch.object(server,name,value);p.start();self.addCleanup(p.stop)
        p=patch.object(server.external_closes,'ExternalCloseMonitor',Mock());p.start();self.addCleanup(p.stop)

    def connect(self):return self.r.connect(self.c,'test-key','test-secret')

    def existing_group(self):
        self.r.engine.start(self.c)
        self.r.engine.tick(self.c,dict(lots=.02,qty=2,contract=100),quote(self.c))
        self.r.engine.pause()
        return self.r.engine.active()[0]

    def test_insufficient_margin_allows_connection_and_retains_original_group(self):
        group=self.existing_group()
        result=self.connect()
        self.assertTrue(result['connected']);self.assertFalse(self.r.reconciled)
        self.assertFalse(self.r.engine.state['enabled'])
        self.assertEqual(group['status'],'open')
        report=result['auto_values']['mt5_margin_check']
        self.assertEqual(report['state'],'insufficient')
        self.assertAlmostEqual(report['lots'],.08)
        self.assertAlmostEqual(report['managed_lots'],.02)
        self.assertIn('已有仓位',report['message'])
        self.terminal.close.assert_not_called()
        self.assertEqual([c.args[0] for c in self.terminal.call.call_args_list],['snapshot','margin'])
        self.exchange.test_order.assert_not_called()
        self.assertEqual(self.store.load()['entry_margin']['phase'],'connect')

    def test_unavailable_or_malformed_margin_does_not_disconnect(self):
        for margin in (dict(required=None,available=100),{},dict(required=float('nan'),available=100),
                       dict(required=True,available=100),dict(required=0,available=100)):
            self.margin=margin
            result=self.connect()
            self.assertTrue(result['connected'])
            self.assertEqual(result['auto_values']['mt5_margin_check']['state'],'unavailable')
            self.assertFalse(self.r.engine.state['enabled'])

    def test_margin_transport_failure_still_rejects_connection(self):
        self.margin=TimeoutError('MT5 会话已断开')
        with self.assertRaisesRegex(TimeoutError,'会话已断开'): self.connect()
        self.assertFalse(self.r.connected)
        self.terminal.close.assert_called_once()

    def test_account_compatibility_failure_still_rejects_connection(self):
        self.platform['account']['currency']='EUR'
        with self.assertRaisesRegex(ValueError,'币种不是 USD'):self.connect()
        self.assertFalse(self.r.connected)
        self.terminal.close.assert_called_once()
        self.assertEqual([c.args[0] for c in self.terminal.call.call_args_list],['snapshot'])

    def test_start_rechecks_funds_and_only_enables_after_success(self):
        self.connect()
        with self.assertRaisesRegex(ValueError,'持仓对账'):self.r.start()
        self.r.reconciled=True
        with self.assertRaisesRegex(ValueError,'新开仓保证金不足'):self.r.start()
        self.assertTrue(self.r.connected);self.assertFalse(self.r.engine.state['enabled'])
        report=build_report(self.store.load(),[])
        self.assertEqual(report['runtime']['last_pause']['reason_code'],'ENTRY_MARGIN')
        self.assertEqual(report['runtime']['entry_margin']['phase'],'start')
        self.assertEqual(report['runtime']['entry_margin']['available'],1)
        self.margin=dict(required=100,available=200)
        self.r.start()
        self.assertTrue(self.r.engine.state['enabled'])
        self.assertEqual(self.r.market_meta['mt5_margin_check']['state'],'ready')
        self.assertEqual(sum(c.args[0]=='margin' for c in self.terminal.call.call_args_list),3)
        self.exchange.test_order.assert_not_called()

    def test_full_existing_capacity_requires_no_additional_margin_or_order(self):
        self.existing_group();self.c['strategy']['max_total_lots']=.02
        # This limit is not part of account identity; groups retain their own rules.
        result=self.connect();self.r.reconciled=True
        self.r.start()
        self.assertEqual(result['auto_values']['mt5_margin_check']['state'],'capacity_full')
        self.assertFalse(any(c.args[0]=='margin' for c in self.terminal.call.call_args_list))
        self.r.engine.tick(self.c,dict(lots=.02,qty=2,contract=100),quote(self.c))
        self.assertEqual(len(self.r.engine.active()),1)
        self.assertFalse(any(c.args[0]=='submit' for c in self.terminal.call.call_args_list))

    def test_unadopted_platform_positions_do_not_reduce_strategy_margin_budget(self):
        self.platform['positions']=[dict(lots=.05,side=0)]
        result=self.connect()
        self.assertEqual(result['auto_values']['mt5_margin_check']['managed_lots'],0)
        self.assertAlmostEqual(result['auto_values']['mt5_margin_check']['lots'],.1)
        self.assertFalse(self.r.reconciled)

    def test_margin_failure_does_not_gate_existing_group_close(self):
        g=self.existing_group();self.connect();self.r.reconciled=True
        with self.assertRaisesRegex(ValueError,'新开仓保证金不足'):self.r.start()
        # An offline broker confirms both close legs. The margin guard must
        # never run from the engine's close path, including while entry-paused.
        self.r.engine.broker=PaperBroker()
        self.r.pump.read.return_value=(self.r.quote,1,'')
        self.r.engine.request_close(g['id'])
        before=self.terminal.call.call_count
        self.r.engine.close_group(g,quote(self.c))
        self.assertEqual(g['status'],'closed')
        self.assertEqual(self.terminal.call.call_count,before)


if __name__=='__main__':unittest.main()
