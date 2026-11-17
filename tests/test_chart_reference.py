import copy
import io
import json
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path
from types import SimpleNamespace as NS
from unittest.mock import Mock,patch

from chart_reference import ReferenceHistory,PERIODS,align_bars,fetch_bars,HistoryRows,HistoryError,native_clock
from trading_brokers import native_chart_rates,worker_main
from test_trading_engine import config
from test_pair_combinations import native_config


class ReferenceTests(unittest.TestCase):
    def setUp(self):
        tmp=tempfile.TemporaryDirectory();self.addCleanup(tmp.cleanup)
        self.directory=Path(tmp.name)/'reference';self.c=config()
        self.c['mt5'].update(adapter='native',account='PRIVATE-ACCOUNT')
        self.c['costs']['usdt_usd']=1.
        self.now=int(time.time()*1000)//900000*900000+30000
        p=patch('chart_reference.time.time',return_value=self.now/1000);p.start();self.addCleanup(p.stop)
        self.fetch=Mock(side_effect=self.rows)
        self.svc=ReferenceHistory(self.directory,self.fetch);self.addCleanup(self.svc.close)

    def rows(self,c,period,start,end,cancel):
        step=PERIODS[period]
        return [dict(time_ms=at,short_close=4005+i,long_close=4000)
                for i,at in enumerate(range(end-step*3,end,step))]

    def result(self,minutes=60,local=None,connected=True,svc=None):
        svc=svc or self.svc
        svc.request(self.c,minutes,local or [],connected)
        worker=svc.thread
        if worker: worker.join(2);self.assertFalse(worker.is_alive())
        return svc.request(self.c,minutes,local or [],connected)

    def test_ready_points_and_statistics_are_separate_from_executable_quotes(self):
        r=self.result()
        self.assertEqual(r['state'],'ready');self.assertTrue(r['reference_only'])
        self.assertEqual(r['stats']['median'],6)
        self.assertEqual(r['stats']['minimum'],5);self.assertEqual(r['stats']['maximum'],7)
        self.assertAlmostEqual(r['stats']['p10'],5.2)
        self.assertFalse(any('entry' in x or 'exit' in x or 'binance' in x for x in r['samples']))
        local=[dict(time_ms=r['samples'][0]['time_ms']+1000,entry=6,exit=7)]
        r=self.result(local=local)
        self.assertEqual(len(r['samples']),2);self.assertEqual(r['stats']['count'],3)
        self.assertEqual(self.fetch.call_count,1)

    def test_full_local_window_does_not_fetch(self):
        end=self.now-self.now%60000
        r=self.result(local=[dict(time_ms=at,valid=True) for at in range(end-59*60000,end+1,60000)])
        self.assertEqual(r['state'],'local');self.fetch.assert_not_called()

    def test_unsupported_adapter_keeps_local_history_without_synthetic_reference(self):
        self.c['mt5']['adapter']='paper'
        r=self.result()
        self.assertEqual(r['state'],'unavailable');self.assertEqual(r['samples'],[])
        self.fetch.assert_not_called()

    def test_background_history_does_not_hold_the_execution_lock(self):
        import _test_runtime
        from server import TradingRuntime
        entered=threading.Event();release=threading.Event()
        def fetch(*args):entered.set();release.wait(2);return []
        self.svc.fetch=fetch
        runtime=TradingRuntime.__new__(TradingRuntime)
        runtime.lock=threading.RLock();runtime.config=self.c;runtime.connected=True;runtime.reference_history=self.svc
        runtime.chart_reference(60,[])
        try:
            self.assertTrue(entered.wait(1))
            acquired=[]
            def take_lock():
                ok=runtime.lock.acquire(blocking=False);acquired.append(ok)
                if ok:runtime.lock.release()
            probe=threading.Thread(target=take_lock);probe.start();probe.join(1)
            self.assertEqual(acquired,[True])
        finally:
            worker=self.svc.thread;release.set()
            if worker: worker.join(2)

    def test_disconnected_loads_disk_cache_without_platform_requests(self):
        self.assertEqual(self.result(connected=False)['state'],'idle')
        self.fetch.assert_not_called()
        self.result()
        svc=ReferenceHistory(self.directory,Mock(side_effect=AssertionError('network forbidden')))
        self.addCleanup(svc.close)
        r=self.result(connected=False,svc=svc)
        self.assertEqual(r['state'],'ready');self.assertEqual(len(r['samples']),3)
        svc.fetch.assert_not_called()

    def test_cache_contains_no_identity_or_credentials_and_fx_is_recomputed(self):
        self.c['binance'].update(api_key='SECRET-KEY',api_secret='SECRET-VALUE')
        self.c['mt5']['mcp_token']='SECRET-TOKEN'
        self.result()
        content=next(self.directory.glob('*.json')).read_text()
        for secret in ('PRIVATE-ACCOUNT','SECRET-KEY','SECRET-VALUE','SECRET-TOKEN'): self.assertNotIn(secret,content)
        incoming=self.fetch.call_args.args[0]
        self.assertNotIn('api_key',incoming['binance']);self.assertNotIn('mcp_token',incoming['mt5'])
        self.c['costs']['usdt_usd']=.99
        r=self.result();self.assertEqual(self.fetch.call_count,1)
        self.assertAlmostEqual(r['samples'][0]['spread'],4005*.99-4000)

    def test_different_accounts_and_windows_never_reuse_another_market_cache(self):
        self.result();self.c['mt5']['account']='OTHER'
        self.result();self.result(10080)
        self.assertEqual(self.fetch.call_count,3)
        self.assertEqual(self.fetch.call_args.args[1],'M5')
        self.result(43200);self.assertEqual(self.fetch.call_args.args[1],'M15')
        self.assertLessEqual((self.fetch.call_args.args[3]-self.fetch.call_args.args[2])/900000,2881)

    def test_disk_full_cannot_abort_display_or_pause_trading(self):
        with patch.object(self.svc,'_write_cache',side_effect=OSError(28,'No space left on device')):
            r=self.result()
        self.assertEqual(r['state'],'ready');self.assertEqual(len(r['samples']),3)
        self.assertIn('未写入',r['cache_warning'])

    def test_background_failure_is_reported_and_backed_off(self):
        self.fetch.side_effect=ValueError('history unavailable')
        r=self.result();self.assertEqual(r['state'],'error')
        self.assertIn('history unavailable',r['message'])
        self.result();self.assertEqual(self.fetch.call_count,1)

    def test_latest_window_cancels_old_work_without_waiting_for_history(self):
        entered=threading.Event();release=threading.Event();cancelled=[]
        def fetch(c,period,start,end,cancel):
            if period=='M1':
                entered.set();release.wait(2);cancelled.append(cancel.is_set())
            return self.rows(c,period,start,end,cancel)
        self.svc.fetch=fetch
        self.svc.request(self.c,60,[])
        self.assertTrue(entered.wait(1))
        r=self.svc.request(self.c,43200,[])
        self.assertEqual(r['state'],'queued')
        worker=self.svc.thread;release.set();worker.join(2)
        self.assertFalse(worker.is_alive());self.assertEqual(cancelled,[True])
        self.assertEqual(self.svc.request(self.c,43200,[])['state'],'ready')

    def test_suspend_cancels_pending_history_work(self):
        entered=threading.Event();release=threading.Event()
        def fetch(*args):entered.set();release.wait(2);return []
        self.svc.fetch=fetch;self.svc.request(self.c,60,[])
        self.assertTrue(entered.wait(1));worker=self.svc.thread
        self.svc.suspend();self.assertTrue(self.svc.cancel.is_set())
        release.set();worker.join(2);self.assertFalse(worker.is_alive())

    def test_memory_and_disk_caches_are_bounded(self):
        for minutes in range(60,70): self.result(minutes)
        self.assertLessEqual(len(self.svc.entries),8)
        self.assertLessEqual(len(list(self.directory.glob('*.json'))),8)

    def test_corrupt_cache_is_refetched(self):
        self.result();next(self.directory.glob('*.json')).write_text('{broken')
        svc=ReferenceHistory(self.directory,self.fetch);self.addCleanup(svc.close)
        self.assertEqual(self.result(svc=svc)['state'],'ready');self.assertEqual(self.fetch.call_count,2)


class SourceTests(unittest.TestCase):
    def test_pairing_requires_same_complete_bar_and_never_uses_nearest_time(self):
        at=1794801600000
        a=[dict(t=at,close=100),dict(t=at+60000,close=103),dict(t=at+180000,close=105)]
        b=[dict(t=at,close=99),dict(t=at+120000,close=101),dict(t=at+180000,close=102)]
        rows=align_bars(a,b,at,at+240000,60000)
        self.assertEqual([r['time_ms'] for r in rows],[at,at+180000])
        self.assertEqual(rows[0]['short_close']-rows[0]['long_close'],1)
        for bad in (a+[a[0]],[dict(t=at+1,close=100)],[dict(t=at,close=float('nan'))]):
            with self.assertRaises(ValueError):align_bars(bad,b,at,at+240000,60000)

    def run_fetch(self,c,drift=0,request=None):
        end=int(time.time()*1000)//60000*60000;start=end-60000*600
        terminal=Mock()
        def call(command,**kw):
            if command=='chart_clock':
                now=int(time.time()*1000)
                return dict(observed_ms=now,ticks={name:now+drift for name in (c['mt5']['symbol'],c['pair']['short_symbol'])})
            self.assertEqual(command,'chart_rates')
            return dict(symbol=kw['symbol'],bars=[dict(t=start,close=4000)],
                        tick_time_ms=int(time.time()*1000)+drift,observed_ms=int(time.time()*1000))
        terminal.call.side_effect=call
        client=Mock();client.request.side_effect=request or [[
            [start+i*60000,0,0,0,'4005'] for i in range(499)],
            [[start+i*60000,0,0,0,'4006'] for i in range(499,600)]]
        with patch('trading_brokers.Terminal',return_value=terminal) as factory, \
             patch('trading_brokers.Binance',return_value=client) as exchange:
            try:r=fetch_bars(c,'M1',start,end,threading.Event())
            finally:self.terminal=terminal;self.factory=factory;self.exchange=exchange;self.client=client
        return r

    def test_exchange_history_uses_independent_read_only_worker_and_unsigned_pagination(self):
        c=config();c['mt5']['adapter']='native'
        r=self.run_fetch(c)
        self.assertEqual(len(r),1);self.assertEqual(r[0]['short_close'],4005)
        self.assertTrue(self.factory.call_args.kwargs['read_only'])
        self.assertNotIn('key',self.exchange.call_args.kwargs)
        self.assertEqual(self.client.request.call_count,2)
        self.assertEqual(self.client.request.call_args.args[0],'/fapi/v1/klines')
        self.terminal.close.assert_called_once()

    def test_native_pair_fetches_both_selected_symbols_without_exchange_requests(self):
        c=native_config();r=self.run_fetch(c)
        self.assertEqual(len(r),1);self.exchange.assert_not_called()
        self.assertEqual([x.kwargs['symbol'] for x in self.terminal.call.call_args_list if x.args[0]=='chart_rates'],
                         [c['mt5']['symbol'],c['pair']['short_symbol']])
        self.assertEqual(len(self.factory.call_args.args[0]['pair_symbols']),2)

    def test_clock_mismatch_is_not_silently_corrected_or_joined(self):
        with self.assertRaisesRegex(ValueError,'历史时基尚未确认'):self.run_fetch(config(),drift=7200000)
        self.client.request.assert_not_called();self.terminal.close.assert_called_once()

    def test_pagination_failure_closes_reader_and_does_not_return_partial_range(self):
        with self.assertRaisesRegex(ValueError,'请求区间之外'):self.run_fetch(config(),request=[[[1,0,0,0,'4005']]])
        self.terminal.close.assert_called_once()

    def test_native_history_request_is_bounded_and_excludes_unfinished_bar(self):
        at=int(time.time()*1000)//60000*60000-600000
        mt=Mock();mt.copy_rates_range.return_value=[dict(time=at//1000,close=4000),dict(time=(at+60000)//1000,close=4001)]
        mt.symbol_info_tick.return_value=NS(time_msc=int(time.time()*1000))
        r=native_chart_rates(mt,'GOLD','M1',at,at+60000)
        self.assertEqual(r['bars'],[dict(t=at,close=4000)])
        self.assertEqual(mt.copy_rates_range.call_args.args[2].tzinfo.utcoffset(None).total_seconds(),0)
        with self.assertRaises(ValueError):native_chart_rates(mt,'GOLD','M1',at-60000*5000,at)
        mt.order_send.assert_not_called()

    def test_actual_worker_accepts_history_in_read_only_mode_without_any_order(self):
        mt=Mock();now=int(time.time()*1000)//60000*60000
        mt.account_info.return_value=NS(login=123,server='TEST')
        mt.terminal_info.return_value=NS(connected=True)
        mt.symbol_info.return_value=NS(name='GOLD')
        mt.symbol_info_tick.return_value=NS(time_msc=now)
        mt.copy_rates_range.return_value=[dict(time=(now-60000)//1000,close=4000)]
        messages=[dict(command='init',settings=dict(account='123',server='TEST',symbol='GOLD'),magic=42,read_only=True),
                  dict(command='chart_rates',period='M1',start_ms=now-60000,end_ms=now)]
        output=io.StringIO()
        with patch.dict(sys.modules,{'MetaTrader5':mt}),patch('sys.stdin',io.StringIO('\n'.join(map(json.dumps,messages)))),patch('sys.stdout',output):
            worker_main()
        reply=json.loads(output.getvalue().splitlines()[-1]);self.assertTrue(reply['ok'])
        self.assertEqual(reply['data']['bars'][0]['close'],4000)
        mt.order_send.assert_not_called();mt.order_check.assert_not_called()


class ClockTests(unittest.TestCase):
    def probes(self, offset=7200000, advancing=True, other_offset=None):
        now=int(time.time()*1000)
        return [dict(observed_ms=now+i*350,ticks={'B':now+offset+(i*350 if advancing else 0),
                    'A':now+(offset if other_offset is None else other_offset)+(i*350 if advancing else 0)}) for i in range(9)]

    def verify(self, probes):
        terminal=Mock();terminal.call.side_effect=probes;cancel=Mock();cancel.wait.return_value=False
        diagnostics={}
        result=native_clock(terminal,['B','A'],cancel,lambda:10,diagnostics)
        return result,diagnostics

    def test_common_advancing_broker_clock_is_measured_without_timezone_assumption(self):
        offset,diag=self.verify(self.probes())
        self.assertEqual(offset,7200000);self.assertEqual(diag['clock_state'],'shared_advancing')
        self.assertEqual(len(diag['clock_samples']),4)
        self.assertEqual(self.verify(self.probes(offset=-7200000))[0],-7200000)

    def test_stale_common_ticks_and_different_leg_clocks_remain_blocked(self):
        with self.assertRaisesRegex(HistoryError,'未同时确认报价更新'):self.verify(self.probes(advancing=False))
        with self.assertRaisesRegex(HistoryError,'两腿时间不一致'):self.verify(self.probes(other_offset=3600000))

    def test_missing_and_jumping_clock_cannot_create_history(self):
        probes=self.probes();probes[0]['ticks']['A']=None
        with self.assertRaisesRegex(HistoryError,'字段缺失'):self.verify(probes)
        probes=self.probes();probes[1]['ticks']['A']+=3600000
        with self.assertRaisesRegex(HistoryError,'时间跳变'):self.verify(probes)

    def test_shifted_native_bars_are_joined_before_display_conversion(self):
        c=native_config();offset=7200000;end=int(time.time()*1000)//900000*900000;start=end-900000*2
        t=Mock();probes=iter(self.probes(offset));cancel=Mock();cancel.wait.return_value=False;cancel.is_set.return_value=False
        wrong_bar=False
        def call(command,**kw):
            if command=='chart_clock':
                probe=next(probes)
                probe['ticks']={c['mt5']['symbol']:probe['ticks']['B'],c['symbol']:probe['ticks']['A']}
                return probe
            self.assertEqual(kw['start_ms'],start+offset);self.assertEqual(kw['end_ms'],end+offset)
            now=int(time.time()*1000)
            return dict(symbol=kw['symbol'],observed_ms=now,tick_time_ms=now+offset,
                        current_bar_ms=now//900000*900000+(0 if wrong_bar else offset),
                        bars=[dict(t=start+offset,close=4000 if kw['symbol']==c['mt5']['symbol'] else 4005)])
        t.call.side_effect=call
        with patch('trading_brokers.Terminal',return_value=t),patch('trading_brokers.Binance') as exchange:
            rows=fetch_bars(c,'M15',start,end,cancel)
        self.assertEqual(rows[0]['time_ms'],start);self.assertEqual(rows[0]['source_time_ms'],start+offset)
        self.assertEqual(rows[0]['short_close']-rows[0]['long_close'],5);exchange.assert_not_called()
        self.assertEqual(rows.diagnostics['paired_count'],1);t.close.assert_called_once()
        wrong_bar=True;probes=iter(self.probes(offset))
        with patch('trading_brokers.Terminal',return_value=t):
            with self.assertRaisesRegex(HistoryError,'K 线与报价的原始时基不一致'):
                fetch_bars(c,'M15',start,end,cancel)

    def test_shifted_history_metadata_survives_cache_and_is_exportable_on_failure(self):
        with tempfile.TemporaryDirectory() as folder:
            svc=ReferenceHistory(folder);now=int(time.time()*1000)//60000*60000
            rows=HistoryRows([dict(time_ms=now-60000,short_close=5,long_close=4)],
                             dict(display_offset_ms=7200000,clock_state='shared_advancing'))
            svc._write_cache('test',rows,now-120000,now)
            restored=svc._read_cache('test',now-120000,now,60000)
            self.assertEqual(restored.diagnostics,rows.diagnostics)
            svc.entries['test']=dict(state='error',diagnostics=dict(clock_samples=[dict(leg='mt5',offset_ms=7200000)]))
            self.assertEqual(svc.diagnostics()[0]['clock_samples'][0]['offset_ms'],7200000)
            svc.close()
