"""Regress the stable +2h MT5 clock reported with Lighter build 89."""
import copy
import tempfile
import unittest
from types import SimpleNamespace as NS
from unittest.mock import Mock,patch

from chart_reference import exchange_clock,fetch_bars,HistoryError,Cancelled,ReferenceHistory
from reference_diagnostics import ReferenceDiagnostics
from test_trading_engine import config


NOW=1795177963643
OFFSET=7200000


def cancel():
    value=Mock();value.wait.return_value=False;value.is_set.return_value=False
    return value


def probes(offset=OFFSET,steps=None):
    # Matches the user's raw offset: 2h minus roughly 5.5 seconds of tick age.
    return [dict(observed_ms=NOW+i*350,ticks={'XAUUSD':NOW+offset-5500+step})
            for i,step in enumerate(steps if steps is not None else [i*350 for i in range(9)])]


class ExchangeClockTests(unittest.TestCase):
    def verify(self,values,token=None):
        terminal=Mock();terminal.call.side_effect=values;diagnostics={}
        offset=exchange_clock(terminal,'XAUUSD',token or cancel(),lambda:10,diagnostics)
        return offset,diagnostics,terminal

    def test_offset_requires_two_distinct_advances_for_zero_positive_and_negative_clocks(self):
        for expected in (0,OFFSET,-OFFSET,19800000):
            with self.subTest(offset=expected):
                offset,diag,terminal=self.verify(probes(expected))
                self.assertEqual(offset,expected)
                self.assertEqual(terminal.call.call_count,3)
                self.assertEqual(diag['clock_state'],'mt5_advancing')
                self.assertEqual(diag['alignment'],'mt5_to_exchange_utc')
                self.assertEqual(len(diag['clock_samples']),3)
                self.assertEqual(diag['clock_samples'][0]['offset_ms'],expected-5500)

    def test_frozen_tick_and_only_one_new_tick_are_not_a_timezone_proof(self):
        for steps in ([0]*9,[0]+[350]*8):
            with self.subTest(steps=steps):
                with self.assertRaises(HistoryError) as caught:self.verify(probes(steps=steps))
                self.assertEqual(caught.exception.diagnostics['error_code'],'ticks_not_advancing')

    def test_missing_jump_and_unstable_offset_fail_closed(self):
        missing=probes();missing[0]['ticks']['XAUUSD']=None
        jumped=probes();jumped[1]['ticks']['XAUUSD']+=3600000
        unstable=probes();unstable[1]['ticks']['XAUUSD']-=3001
        for values,code in [(missing,'missing_time'),(jumped,'clock_jump'),(unstable,'clock_jump')]:
            with self.subTest(code=code):
                with self.assertRaises(HistoryError) as caught:self.verify(values)
                self.assertEqual(caught.exception.diagnostics['error_code'],code)
        values=probes();values[1]['observed_ms']+=4000
        with self.assertRaises(HistoryError) as caught:self.verify(values)
        self.assertEqual(caught.exception.diagnostics['error_code'],'unstable_offset')

    def test_arbitrary_lag_is_not_rounded_into_a_server_timezone(self):
        with self.assertRaises(HistoryError) as caught:self.verify(probes(OFFSET+45000))
        self.assertEqual(caught.exception.diagnostics['error_code'],'inconsistent_clocks')

    def test_window_cancellation_stops_the_probe(self):
        token=cancel();token.wait.return_value=True
        with self.assertRaises(Cancelled):self.verify(probes(),token)


class ExchangeHistoryTests(unittest.TestCase):
    def run_fetch(self,venue='lighter',offset=OFFSET,period='M1',bar_offset=None,rate_drift=0,frozen=False):
        interval={'M1':60000,'M5':300000,'M15':900000}[period]
        end=NOW//interval*interval;start=end-interval*3
        c=config();c['mt5'].update(adapter='native',symbol='XAUUSD')
        c['pair']=dict(type='exchange_mt5',exchange=venue)
        c['symbol']='XAU' if venue=='lighter' else 'XAUUSDT'
        c['lighter']=dict(proxy_url='http://127.0.0.1:7891')
        original=copy.deepcopy(c)
        terminal=Mock();series=iter(probes(offset,[0]*9 if frozen else None))
        def call(command,**kwargs):
            if command=='chart_clock':return next(series)
            self.assertEqual(command,'chart_rates')
            self.assertEqual(kwargs['start_ms'],start+offset)
            self.assertEqual(kwargs['end_ms'],end+offset)
            self.assertEqual(kwargs['clock_offset_ms'],offset)
            observed=NOW+1050;tick=observed+offset-5500+rate_drift
            current=(observed+offset-5500)//interval*interval
            if bar_offset is not None:current=current-offset+bar_offset
            # Missing middle MT5 bar must remain a hole, never nearest-joined.
            return dict(symbol='XAUUSD',observed_ms=observed,tick_time_ms=tick,current_bar_ms=current,
                        bars=[dict(t=start+offset,close=4000),dict(t=start+offset+2*interval,close=4002)])
        terminal.call.side_effect=call
        exchange=Mock()
        exchange.chart_bars.return_value=[dict(t=start+i*interval,close=4005+i) for i in range(3)]
        exchange.request.return_value=[[start+i*interval,0,0,0,str(4005+i)] for i in range(3)]
        self.terminal=terminal;self.exchange=exchange;self.config=c;self.bounds=(start,end,interval)
        with patch('trading_brokers.Terminal',return_value=terminal) as factory, \
             patch('trading_brokers.Binance',return_value=exchange) as binance, \
             patch('lighter_broker.Lighter',return_value=exchange) as lighter:
            try:rows=fetch_bars(c,period,start,end,cancel())
            finally:
                self.factory=factory;self.binance=binance;self.lighter=lighter
                self.assertEqual(c,original)
        return rows

    def test_both_exchanges_join_only_mt5_converted_times_at_every_observation_period(self):
        for venue in ('lighter','binance'):
            for period in ('M1','M5','M15'):
                with self.subTest(venue=venue,period=period):
                    rows=self.run_fetch(venue=venue,period=period)
                    start,end,interval=self.bounds
                    self.assertEqual([r['time_ms'] for r in rows],[start,start+2*interval])
                    self.assertEqual([r['short_close']-r['long_close'] for r in rows],[5,5])
                    self.assertEqual(rows[0]['mt5_source_time_ms'],start+OFFSET)
                    self.assertEqual(rows[0]['exchange_source_time_ms'],start)
                    self.assertEqual(rows.diagnostics['paired_count'],2)
                    self.assertEqual(rows.diagnostics['legs'][0]['current_bar_ms']%interval,0)
                    self.assertTrue(self.factory.call_args.kwargs['read_only'])
                    self.terminal.close.assert_called_once();self.exchange.close.assert_called_once()
                    if venue=='lighter':
                        self.binance.assert_not_called()
                        self.assertEqual(self.exchange.chart_bars.call_args.args[2:4],(start,end))
                    else:
                        self.lighter.assert_not_called()
                        self.assertNotIn('key',self.binance.call_args.kwargs)
                        self.assertEqual(self.exchange.request.call_args.args[1]['startTime'],start)

    def test_zero_and_negative_offsets_also_validate_and_pair_both_clocks(self):
        for offset in (0,-OFFSET):
            with self.subTest(offset=offset):
                rows=self.run_fetch(offset=offset)
                self.assertEqual(rows[0]['time_ms'],self.bounds[0])
                self.assertEqual(rows.diagnostics['display_offset_ms'],offset)

    def test_inconsistent_bar_clock_cannot_return_cross_platform_prices_even_at_zero_offset(self):
        for offset,bar_offset in ((OFFSET,0),(0,OFFSET)):
            with self.subTest(offset=offset):
                with self.assertRaises(HistoryError) as caught:self.run_fetch(offset=offset,bar_offset=bar_offset)
                self.assertEqual(caught.exception.diagnostics['error_code'],'bar_clock_mismatch')
                self.terminal.close.assert_called_once();self.lighter.assert_not_called()

    def test_clock_change_during_history_read_is_rejected(self):
        with self.assertRaises(HistoryError) as caught:self.run_fetch(rate_drift=3600000)
        self.assertEqual(caught.exception.diagnostics['error_code'],'unconfirmed_clock')
        self.terminal.close.assert_called_once();self.lighter.assert_not_called()

    def test_frozen_quote_never_fetches_rates_or_exchange_history(self):
        with self.assertRaises(HistoryError) as caught:self.run_fetch(frozen=True)
        self.assertEqual(caught.exception.diagnostics['error_code'],'ticks_not_advancing')
        self.assertTrue(all(call.args[0]=='chart_clock' for call in self.terminal.call.call_args_list))
        self.terminal.close.assert_called_once();self.lighter.assert_not_called()

    def test_safe_diagnostics_and_cached_display_explain_which_leg_was_converted(self):
        rows=self.run_fetch()
        diagnostics=ReferenceDiagnostics()
        diagnostics.set_chart([dict(rows.diagnostics,state='ready',account='PRIVATE',error='PRIVATE')])
        result=diagnostics.snapshot()['chart_reference'][0]
        self.assertEqual(result['alignment'],'mt5_to_exchange_utc')
        self.assertEqual(result['clock_state'],'mt5_advancing')
        self.assertEqual(result['clock_tolerance_ms'],10000)
        self.assertNotIn('PRIVATE',str(result))
        with tempfile.TemporaryDirectory() as folder:
            svc=ReferenceHistory(folder,Mock(return_value=rows))
            try:
                c=self.config;c['costs']['usdt_usd']=1
                with patch('chart_reference.time',NS(time=lambda:NOW/1000,monotonic=lambda:1)):
                    svc.request(c,60,[])
                    worker=svc.thread
                    if worker:worker.join(2)
                    output=svc.request(c,60,[])
                self.assertEqual(output['state'],'ready')
                self.assertIn('MT5 时间偏移 +2 小时已验证',output['message'])
                self.assertIn('历史夏令时',output['message'])
                self.assertNotIn('两腿原始 K 线按同一服务器',output['message'])
                self.assertTrue(output['reference_only'])
            finally:svc.close()


if __name__=='__main__':unittest.main()
