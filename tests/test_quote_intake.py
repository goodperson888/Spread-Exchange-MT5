import threading
import time
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

import _test_runtime
from market_push import QuotePump, QuoteEvents, TickFreshness, milliseconds
from native_pair import NativePairPump
from mt5_pool import PoolPump
from mt5_pool_config import candidates
from quote_fallback import BackgroundRead
from execution_latency import LatencyStats
from test_trading_engine import config
from test_pair_combinations import native_config
from test_mt5_pool import config as pool_config


def eventually(check, timeout=2):
    deadline=time.monotonic()+timeout
    while time.monotonic()<deadline:
        if check():return True
        threading.Event().wait(.002)
    return False


class BackgroundReadTests(unittest.TestCase):
    def test_slow_read_is_single_flight_and_close_discards_late_result(self):
        entered=threading.Event();release=threading.Event();finished=threading.Event();wake=threading.Event()
        def load():
            entered.set();release.wait(2);finished.set();return 'late'
        loader=Mock(side_effect=load);source=BackgroundRead(loader,wake,'test-fallback')
        self.addCleanup(source.close);self.addCleanup(release.set)
        self.assertEqual(source.read(0),(None,None));self.assertTrue(entered.wait(1))
        for _ in range(100):self.assertEqual(source.read(0),(None,None))
        loader.assert_called_once()
        source.close();release.set();self.assertTrue(finished.wait(1))
        self.assertTrue(eventually(lambda:not source.thread.is_alive()))
        self.assertIsNone(source.value);self.assertFalse(wake.is_set())

    def test_error_retry_is_bounded_and_recovers_without_replacing_source(self):
        wake=threading.Event();loader=Mock(side_effect=[ValueError('offline'),{'bid':1}])
        source=BackgroundRead(loader,wake,'test-retry');self.addCleanup(source.close)
        with patch('quote_fallback.time.monotonic',return_value=100):
            source.read(.05);self.assertTrue(wake.wait(1));wake.clear()
            value,error=source.read(.05)
            self.assertIsNone(value);self.assertIsInstance(error,ValueError)
            loader.assert_called_once()
        with patch('quote_fallback.time.monotonic',return_value=101):
            source.read(.05);self.assertTrue(wake.wait(1))
            self.assertEqual(source.read(.05),({'bid':1},None))
        self.assertEqual(loader.call_count,2)


class IntakeTests(unittest.TestCase):
    def exchange_pump(self):
        c=config();c['mt5']['adapter']='native';c['execution']['poll_ms']=5000
        at=milliseconds();bridge=Mock();bridge.quote.return_value=None
        stream=Mock();stream.quote.return_value=dict(bid=4306,ask=4306.2,time_ms=at)
        terminal=Mock();wake=threading.Event();events=QuoteEvents();history=Mock()
        pump=QuotePump(c,Mock(),stream,terminal,bridge,Mock(),history,wake,quote_events=events)
        self.addCleanup(pump.close)
        return pump,bridge,stream,terminal,wake,events,history

    def block(self,mock,result):
        entered=threading.Event();release=threading.Event()
        def delayed(*args,**kwargs):entered.set();release.wait(3);return result
        mock.side_effect=delayed;self.addCleanup(release.set)
        return entered,release

    def test_exchange_push_bypasses_slow_terminal_read_and_late_poll_cannot_overwrite(self):
        pump,bridge,stream,terminal,wake,events,history=self.exchange_pump()
        entered,release=self.block(terminal.call,{'quote':dict(bid=4200,ask=4200.2,time_ms=milliseconds()-100)})
        pump.start();self.assertTrue(entered.wait(1))
        at=milliseconds();bridge.quote.return_value=dict(bid=4300,ask=4300.2,time_ms=at,observed_ms=at,
                                                        transport='EA Socket 推送')
        pump.changed.set();self.assertTrue(wake.wait(1))
        self.assertFalse(release.is_set());self.assertEqual(pump.read()[0]['mt5']['bid'],4300)
        self.assertGreater(events.sequence,0);terminal.call.assert_called_once()
        release.set();self.assertTrue(eventually(lambda:not pump.mt5_fallback.busy))
        self.assertEqual(pump.read()[0]['mt5']['bid'],4300)

    def test_websocket_recovery_bypasses_slow_rest_read(self):
        pump,bridge,stream,terminal,wake,events,history=self.exchange_pump()
        at=milliseconds();bridge.quote.return_value=dict(bid=4300,ask=4300.2,time_ms=at,observed_ms=at)
        stream.quote.return_value=None
        entered,release=self.block(pump.exchange.quote,dict(bid=4206,ask=4206.2,time_ms=at))
        pump.start();self.assertTrue(entered.wait(1))
        stream.quote.return_value=dict(bid=4306,ask=4306.2,time_ms=milliseconds())
        pump.changed.set();self.assertTrue(wake.wait(1))
        self.assertEqual(pump.read()[0]['binance']['bid'],4306)
        self.assertFalse(release.is_set());pump.exchange.quote.assert_called_once()
        release.set();self.assertTrue(eventually(lambda:not pump.rest_fallback.busy))
        self.assertEqual(pump.read()[0]['binance']['bid'],4306)

    def test_slow_history_does_not_block_next_quote_or_chart_event(self):
        pump,bridge,stream,terminal,wake,events,history=self.exchange_pump()
        at=milliseconds();bridge.quote.return_value=dict(bid=4300,ask=4300.2,time_ms=at,observed_ms=at)
        entered,release=self.block(history,None)
        pump.start();self.assertTrue(entered.wait(1));first=pump.read()[1];wake.clear()
        stream.quote.return_value=dict(bid=4307,ask=4307.2,time_ms=milliseconds())
        stream.on_quote();self.assertTrue(wake.wait(1))
        self.assertGreater(pump.read()[1],first);self.assertEqual(pump.read()[0]['binance']['bid'],4307)
        self.assertGreaterEqual(events.sequence,2);self.assertFalse(release.is_set())

    def test_async_poll_completion_does_not_refresh_unchanged_quote(self):
        pump,bridge,stream,terminal,wake,events,history=self.exchange_pump()
        at=milliseconds();terminal.call.return_value={'quote':dict(bid=4300,ask=4300.2,time_ms=at-7200000)}
        pump.collect(background=True);self.assertTrue(pump.changed.wait(1))
        pump.collect(background=True);first=pump.read()[0]['mt5']['observed_ms']
        pump.changed.clear();pump.mt5_fallback.completed=None
        pump.collect(background=True);self.assertTrue(pump.changed.wait(1))
        stream.quote.return_value=dict(bid=4307,ask=4307.2,time_ms=milliseconds())
        pump.collect(background=True)
        self.assertEqual(pump.read()[0]['mt5']['observed_ms'],first)
        self.assertEqual(terminal.call.call_count,2)

    def test_late_poll_superseded_by_push_is_discarded_without_channel_error(self):
        pump,bridge,stream,terminal,wake,events,history=self.exchange_pump()
        at=milliseconds()
        entered,release=self.block(terminal.call,{'quote':dict(bid=4200,ask=4200.2,time_ms=at-100)})
        pump.start();self.assertTrue(entered.wait(1))
        bridge.quote.return_value=dict(bid=4300,ask=4300.2,time_ms=at,observed_ms=at,transport='EA Socket 推送')
        pump.changed.set();self.assertTrue(wake.wait(1))
        release.set();self.assertTrue(eventually(lambda:not pump.mt5_fallback.busy))
        self.assertIsNone(pump.mt5_fallback.error);self.assertIsNone(pump.mt5_fallback.value)
        bridge.quote.return_value=None
        pump.collect(background=True)
        self.assertEqual(pump.read()[0]['mt5']['bid'],4300);self.assertEqual(pump.read()[2],'')

    def test_previously_cached_poll_cannot_regress_a_newer_execution_observation(self):
        pump,bridge,stream,terminal,wake,events,history=self.exchange_pump()
        at=milliseconds();old=dict(bid=4200,ask=4200.2,time_ms=at-100)
        terminal.call.return_value={'quote':old}
        pump.collect(background=True);self.assertTrue(pump.changed.wait(1))
        pump.collect(background=True);first=pump.read()[1]
        pump.mt5_freshness.observe(dict(bid=4300,ask=4300.2,time_ms=at),pump.config['mt5'])
        stream.quote.return_value=dict(bid=4307,ask=4307.2,time_ms=at)
        pump.collect(background=True)
        self.assertEqual(pump.read()[1],first);self.assertEqual(pump.read()[2],'')

    def test_native_pair_push_bypasses_slow_pair_poll_and_requires_both_books(self):
        c=native_config();c['execution']['mt5_quote_poll_ms']=500
        bridge=Mock();books={};bridge.quote.side_effect=lambda settings,age:books.get(settings['symbol'])
        r=SimpleNamespace(config=c,push_bridge=bridge,mt5_freshness=TickFreshness(),store=Mock(),
                          quote_events=QuoteEvents(),market_wake=threading.Event())
        terminal=Mock();pump=NativePairPump(r,terminal);self.addCleanup(pump.close)
        names=(c['pair']['short_symbol'],c['mt5']['symbol']);at=milliseconds()
        old={name:dict(bid=4200,ask=4200.2,time_ms=at-100) for name in names}
        entered,release=self.block(terminal.call,old)
        pump.start();self.assertTrue(entered.wait(1))
        books[names[0]]=dict(bid=4306,ask=4306.2,time_ms=at,observed_ms=at,transport='EA Socket 推送')
        pump.refresh(background=True)
        self.assertIsNone(pump.read()[0])  # Never invent the missing second book.
        books[names[1]]=dict(bid=4300,ask=4300.2,time_ms=at,observed_ms=at,transport='EA Socket 推送')
        pump.changed.set();self.assertTrue(r.market_wake.wait(1))
        self.assertEqual(pump.read()[0]['mt5']['bid'],4300);self.assertFalse(release.is_set())
        terminal.call.assert_called_once()
        release.set();self.assertTrue(eventually(lambda:not pump.fallback.busy))
        self.assertEqual(pump.read()[0]['mt5']['bid'],4300)

    def test_pool_push_bypasses_each_inflight_poll_without_changing_pair_identity(self):
        c=pool_config(2);bridge=Mock();books={}
        bridge.quote.side_effect=lambda settings,age:books.get(settings['account'])
        r=SimpleNamespace(push_bridge=bridge,market_wake=threading.Event(),quote_events=QuoteEvents(),store=Mock())
        contexts={edge:SimpleNamespace(config=fixed) for edge,fixed in candidates(c).items()}
        owner=SimpleNamespace(runtime=r,config=c,endpoints=c['mt5_pool']['endpoints'],contexts=contexts,
                              edges_by_key={},chart_key='t1~t2')
        pump=PoolPump(owner);self.addCleanup(pump.close);reader=Mock();at=milliseconds()
        entered,release=self.block(reader.call,{'quote':dict(bid=4200,ask=4200.2,time_ms=at-100)})
        with patch('mt5_pool.Terminal',return_value=reader):
            pump.start();self.assertTrue(entered.wait(1))
            for i,e in enumerate(owner.endpoints):
                books[e['account']]=dict(bid=4306-i*6,ask=4306.2-i*6,time_ms=at,observed_ms=at,
                                        transport='EA Socket 推送')
                pump.quote_wakes[e['id']].set()
            self.assertTrue(eventually(lambda:pump.read()[0] is not None));q=pump.read()[0]
            self.assertEqual(q['mt5']['bid'],4300);self.assertEqual(q['binance']['bid'],4306)
            self.assertFalse(release.is_set());release.set()
            self.assertTrue(eventually(lambda:all(not f.busy for f in pump.fallbacks.values())))
            self.assertEqual(pump.read()[0]['mt5']['bid'],4300)
            pump.close()


if __name__=='__main__':unittest.main()
