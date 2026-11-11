"""Fresh background quotes avoid redundant I/O between opening legs."""
import copy
import unittest
from unittest.mock import Mock, patch

import _test_runtime
from execution_diagnostics import build_report
from market_push import TickFreshness
from server import TradingRuntime
from trading_config import pair_key
from trading_brokers import PaperBroker
from trading_engine import Engine, valid_quote
from test_trading_engine import config, quote


class ExecutionQuoteCacheTests(unittest.TestCase):
    def setUp(self):
        self.at=1800000000000
        self.c=config();self.c['mt5']['adapter']='native'
        self.r=TradingRuntime.__new__(TradingRuntime);self.r.config=self.c
        self.cached=quote(self.c)
        self.cached.update(time_ms=self.at,usdt_usd=self.c['costs']['usdt_usd'])
        for leg in ('binance','mt5'):
            self.cached[leg].update(time_ms=self.at,observed_ms=self.at)
        self.r.quote=copy.deepcopy(self.cached)
        self.r.pump=Mock();self.r.pump.read.side_effect=lambda:(self.cached,1,'')
        self.r.push_bridge=None;self.r.market_stream=Mock()
        self.r.market_stream.quote.return_value=dict(self.cached['binance'],ask=4306.4)
        self.r.terminal=Mock();self.r.terminal.call.return_value={'quote':dict(self.cached['mt5'])}
        self.r.binance=Mock();self.r.mt5_freshness=TickFreshness()
        for target in ('server.now_ms','trading_engine.stamp','market_push.milliseconds'):
            p=patch(target,return_value=self.at);p.start();self.addCleanup(p.stop)

    def test_recent_cache_uses_latest_stream_price_without_terminal_or_rest(self):
        original=copy.deepcopy(self.cached)
        q=self.r._execution_quote()
        self.assertEqual(q['execution_quote_source'],'cache')
        self.assertEqual(q['binance']['ask'],4306.4)
        self.assertEqual(q['mt5']['observed_ms'],self.at)
        self.assertEqual(self.cached,original)
        self.assertTrue(valid_quote(q,self.c,now=self.at))
        self.r.terminal.call.assert_not_called();self.r.binance.quote.assert_not_called()

    def test_real_second_leg_path_uses_cache_and_persists_quote_source(self):
        store=Mock();store.load.return_value=None
        engine=Engine(store,PaperBroker());engine.start(self.c)
        engine.quote_provider=self.r._execution_quote
        engine.tick(self.c,dict(lots=.01,qty=1,contract=100),copy.deepcopy(self.cached))
        self.assertEqual(engine.active()[0]['status'],'open')
        self.assertEqual(engine.state['orders'][1]['quote_source'],'cache')
        self.assertEqual(build_report(engine.state,[])['orders'][1]['quote_source'],'cache')
        self.r.terminal.call.assert_not_called();self.r.binance.quote.assert_not_called()

    def test_cache_older_than_250ms_uses_original_native_read(self):
        self.cached['mt5']['observed_ms']=self.at-251
        q=self.r._execution_quote()
        self.assertEqual(q['execution_quote_source'],'refresh')
        self.r.terminal.call.assert_called_once_with('quote')
        self.r.binance.quote.assert_not_called()

    def test_user_stricter_age_and_skew_limits_still_apply(self):
        self.c['strategy']['max_quote_age_ms']=100
        self.cached['mt5']['observed_ms']=self.at-101
        self.assertEqual(self.r._execution_quote()['execution_quote_source'],'refresh')
        self.r.terminal.call.assert_called_once()
        self.r.terminal.call.reset_mock()
        self.c['strategy'].update(max_quote_age_ms=3000,max_clock_skew_ms=10)
        self.cached['mt5']['observed_ms']=self.at-20
        self.assertEqual(self.r._execution_quote()['execution_quote_source'],'refresh')
        self.r.terminal.call.assert_called_once()

    def test_wrong_pair_or_background_error_never_uses_fast_path(self):
        self.cached['key']='different-account'
        q=self.r._execution_quote()
        self.assertEqual(q['execution_quote_source'],'refresh')
        self.assertEqual(q['key'],pair_key(self.c))
        self.r.terminal.call.reset_mock();self.cached['key']=pair_key(self.c)
        self.r.pump.read.side_effect=None;self.r.pump.read.return_value=(self.cached,1,'channel unavailable')
        self.assertEqual(self.r._execution_quote()['execution_quote_source'],'refresh')
        self.r.terminal.call.assert_called_once()

    def test_no_background_sample_keeps_original_fallback(self):
        self.r.pump=None
        q=self.r._execution_quote()
        self.assertEqual(q['execution_quote_source'],'refresh')
        self.r.terminal.call.assert_called_once_with('quote')

    def test_malformed_cache_uses_original_native_read(self):
        self.cached['mt5']['bid']='malformed'
        q=self.r._execution_quote()
        self.assertEqual(q['execution_quote_source'],'refresh')
        self.r.terminal.call.assert_called_once_with('quote')

    def test_bad_cache_cannot_hide_a_failed_native_read(self):
        self.cached['mt5']['observed_ms']=self.at-10000
        self.r.terminal.call.side_effect=TimeoutError('native unavailable')
        with self.assertRaises(TimeoutError):self.r._execution_quote()

    def test_unchanged_tick_cannot_be_made_fresh_by_cache_or_fallback(self):
        self.r.mt5_freshness.observe(dict(self.cached['mt5'],observed_ms=self.at-10000),self.c['mt5'])
        self.cached['mt5']['observed_ms']=self.at-10000
        self.r.terminal.call.return_value={'quote':dict(self.cached['mt5'],observed_ms=self.at)}
        q=self.r._execution_quote()
        self.assertEqual(q['execution_quote_source'],'refresh')
        self.assertEqual(q['mt5']['observed_ms'],self.at-10000)
        self.assertFalse(valid_quote(q,self.c,now=self.at))

    def test_quote_source_export_is_an_enum_and_contains_no_private_data(self):
        for source in ('cache','refresh','PRIVATE-URL'):
            report=build_report(dict(groups=[dict(id='g')],orders=[dict(id='o',group='g',quote_source=source)]),[])
            if source in ('cache','refresh'):self.assertEqual(report['orders'][0]['quote_source'],source)
            else:self.assertNotIn('quote_source',report['orders'][0])
