import copy
import json
import socket
import tempfile
import threading
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch
import sys
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import _test_runtime
from market_push import PushBridge, QuoteEvents, QuotePump, TickFreshness, milliseconds
from trading_config import pair_key
from trading_engine import Engine, valid_quote
from trading_store import Store
from trading_brokers import PaperBroker
from test_trading_engine import config


class PushTests(unittest.TestCase):
    def setUp(self):
        self.arrived=threading.Event()
        self.bridge=PushBridge('a'*64,0,self.arrived.set).start()
        self.addCleanup(self.bridge.close)
        self.identity={'account':'123','server':'Demo','symbol':'XAUUSD'}

    def client(self,token=None,identity=None):
        sock=socket.create_connection(('127.0.0.1',self.bridge.port),timeout=1)
        self.addCleanup(sock.close)
        sock.sendall((json.dumps(dict(identity or self.identity,token=token or self.bridge.token))+'\n').encode())
        return sock

    def quote(self,**extra):
        return dict(self.identity,bid=4300,ask=4300.2,time_ms=milliseconds(),seq=1,**extra)

    def test_auth_and_bound_identity(self):
        bad=self.client('wrong');self.assertEqual(bad.recv(100),b'')
        sock=self.client();self.assertEqual(sock.recv(100),b'OK\n');self.arrived.clear()
        q=self.quote();sock.sendall((json.dumps(q)+'\n').encode());self.assertTrue(self.arrived.wait(1))
        self.assertEqual(self.bridge.quote(self.identity,5000)['bid'],4300)
        self.assertIsNone(self.bridge.quote(dict(self.identity,account='456'),5000))
        sock.sendall((json.dumps(dict(q,symbol='OTHER',seq=2))+'\n').encode())
        self.assertEqual(sock.recv(100),b'')

    def test_duplicate_writer_cannot_replace_or_disconnect_owner(self):
        first=self.client();self.assertEqual(first.recv(100),b'OK\n')
        second=self.client();self.assertEqual(second.recv(100),b'')
        self.arrived.clear();first.sendall((json.dumps(self.quote())+'\n').encode())
        self.assertTrue(self.arrived.wait(1));self.assertIsNotNone(self.bridge.quote(self.identity,5000))

    def test_accept_server_timezone_offset_but_reject_invalid_and_backwards_ticks(self):
        identity=self.bridge.identity(self.identity);owner=object();self.bridge.owners[identity]=owner
        q=self.quote()
        # MT5 broker/server timestamps may be hours away from local/Binance time;
        # receipt time, not the raw server clock, controls freshness.
        self.assertTrue(self.bridge.accept(identity,owner,dict(q,time_ms=milliseconds()-20000)))
        self.assertFalse(self.bridge.accept(identity,owner,dict(q,bid=float('nan'))))
        self.assertFalse(self.bridge.accept(identity,owner,dict(q,bid=4400)))
        self.assertTrue(self.bridge.accept(identity,owner,q))
        self.assertFalse(self.bridge.accept(identity,owner,dict(q,time_ms=q['time_ms']-1)))
        self.assertEqual(self.bridge.quote(self.identity,5000)['source_time_ms'],q['time_ms'])


class PumpTests(unittest.TestCase):
    def fixture(self):
        c=config();c['mt5']['adapter']='native';c['execution']['poll_ms']=5000
        bridge=Mock();bridge.quote.return_value={'bid':4300,'ask':4300.2,'time_ms':milliseconds()}
        stream=Mock();stream.quote.return_value={'bid':4306,'ask':4306.2,'time_ms':milliseconds()}
        terminal=Mock();publish=Mock();wake=threading.Event()
        pump=QuotePump(c,Mock(),stream,terminal,bridge,Mock(),publish,wake)
        return c,bridge,stream,terminal,publish,wake,pump

    def test_push_avoids_terminal_requests_and_validates_pair(self):
        c,b,s,t,p,w,pump=self.fixture();pump.collect()
        self.assertTrue(w.is_set());t.call.assert_not_called()
        q,version,error=pump.read();self.assertEqual(version,1);self.assertTrue(q['valid'])
        self.assertAlmostEqual(q['entry'],5.8);self.assertEqual(q['key'],pair_key(c))
        pump.collect();self.assertEqual(p.call_count,1)
        s.quote.return_value=dict(s.quote.return_value,bid=4307)
        pump.collect();self.assertEqual(p.call_count,2)
        self.assertGreater(p.call_args.args[0]['time_ms'],q['time_ms'])

    def test_events_arrive_while_trade_execution_is_blocked(self):
        c,b,s,t,p,w,pump=self.fixture()
        trade_started=threading.Event();release_trade=threading.Event()
        def trading_worker(): trade_started.set();release_trade.wait(2)
        trade=threading.Thread(target=trading_worker);trade.start();self.addCleanup(release_trade.set)
        pump.start();self.addCleanup(pump.close)
        self.assertTrue(trade_started.wait(1));self.assertTrue(w.wait(1));w.clear()
        s.quote.return_value=dict(s.quote.return_value,bid=4307)
        s.on_quote()
        self.assertTrue(w.wait(1))  # event wakes a 5s compatibility interval immediately
        self.assertTrue(trade.is_alive());self.assertGreaterEqual(p.call_count,2)
        release_trade.set();trade.join(1)

    def test_stopped_old_connection_cannot_publish(self):
        c,b,s,t,p,w,pump=self.fixture();pump.close();pump.collect();p.assert_not_called()

    def test_strategy_sees_latest_before_slow_history_publication(self):
        c,b,s,t,p,w,pump=self.fixture()
        publication_started=threading.Event();release=threading.Event()
        def blocked_publication(q):
            publication_started.set();release.wait(2)
        p.side_effect=blocked_publication
        worker=threading.Thread(target=pump.collect);worker.start()
        try:
            self.assertTrue(publication_started.wait(1))
            self.assertTrue(w.is_set())
            self.assertEqual(pump.read()[1],1)
            self.assertEqual(pump.read()[0]['binance']['bid'],s.quote.return_value['bid'])
            self.assertTrue(worker.is_alive())
        finally:
            release.set();worker.join(1)

    def test_server_clock_offset_does_not_make_fresh_native_quote_invalid(self):
        c,b,s,t,p,w,pump=self.fixture();b.quote.return_value=None
        t.call.return_value={'quote':{'bid':4300,'ask':4300.2,'time_ms':milliseconds()-15000}}
        pump.collect();t.call.assert_called_once_with('quote');q,_,_=pump.read()
        # The quote was read now, so an old broker server timestamp is only a
        # display field. Make the explicit source/receipt distinction visible.
        self.assertTrue(q['valid'])
        self.assertNotEqual(q['mt5']['source_time_ms'],q['mt5']['observed_ms'])

    def test_unchanged_polled_tick_expires_while_binance_keeps_updating(self):
        c,b,s,t,p,w,pump=self.fixture();b.quote.return_value=None
        at=milliseconds()
        tick={'bid':4300,'ask':4300.2,'time_ms':at-15000}
        t.call.return_value={'quote':tick}
        with patch('market_push.milliseconds',return_value=at), patch('trading_engine.stamp',return_value=at):
            pump.collect()
        self.assertTrue(pump.read()[0]['valid'])
        s.quote.return_value=dict(s.quote.return_value,time_ms=at+10000)
        pump.mt5_read_at=0
        with patch('market_push.milliseconds',return_value=at+10000), patch('trading_engine.stamp',return_value=at+10000):
            pump.collect()
            stale=pump.read()[0]
            self.assertFalse(stale['valid'])
            self.assertEqual(stale['mt5']['observed_ms'],at)
            with tempfile.TemporaryDirectory() as directory:
                store=Store(Path(directory)/'test.sqlite3')
                try:
                    broker=Mock(wraps=PaperBroker());engine=Engine(store,broker);engine.start(c)
                    engine.tick(c,dict(lots=.02,qty=2,contract=100),stale)
                    broker.submit.assert_not_called()
                finally:store.close()
            # A genuinely new source tick restores freshness despite the broker clock offset.
            t.call.return_value={'quote':dict(tick,time_ms=tick['time_ms']+10000)}
            pump.mt5_read_at=0;pump.collect()
            self.assertTrue(pump.read()[0]['valid'])

    def test_push_poll_and_execution_reads_share_the_same_tick_age(self):
        import server
        c,b,s,t,p,w,pump=self.fixture();at=milliseconds()
        tick={'bid':4300,'ask':4300.2,'time_ms':at-15000}
        b.quote.return_value=dict(tick,observed_ms=at)
        with patch('market_push.milliseconds',return_value=at):pump.collect()
        b.quote.return_value=None;t.call.return_value={'quote':tick}
        s.quote.return_value=dict(s.quote.return_value,time_ms=at+10000)
        r=server.TradingRuntime.__new__(server.TradingRuntime)
        r.config=c;r.mt5_freshness=pump.mt5_freshness
        r.push_bridge=b;r.terminal=t;r.market_stream=s
        with patch('market_push.milliseconds',return_value=at+10000), patch('server.now_ms',return_value=at+10000):
            pump.collect()
            self.assertEqual(pump.read()[0]['mt5']['observed_ms'],at)
            execution=r._execution_quote()
        self.assertFalse(valid_quote(execution,c,now=at+10000))
        self.assertEqual(execution['mt5']['observed_ms'],at)

    def test_quote_tracking_is_scoped_to_account_and_rejects_backwards_ticks(self):
        tracker=TickFreshness();settings={'account':'1','server':'Demo','symbol':'XAUUSD'}
        tick={'time_ms':1000,'bid':4300,'ask':4300.2}
        tracker.observe(tick,settings,10000)
        self.assertEqual(tracker.observe(tick,settings,20000)['observed_ms'],10000)
        other=tracker.observe(tick,dict(settings,account='2'),20000)
        self.assertEqual(other['observed_ms'],20000)
        with self.assertRaisesRegex(ValueError,'倒退'):
            tracker.observe(dict(tick,time_ms=999),settings,30000)

    def test_slow_rest_fallback_cannot_refresh_the_mt5_receipt_time(self):
        import server
        c,b,s,t,p,w,pump=self.fixture();b.quote.return_value=None;s.quote.return_value=None
        at=milliseconds();clock=[at]
        t.call.return_value={'quote':dict(bid=4300,ask=4300.2,time_ms=at-15000)}
        def delayed_market(symbol):
            clock[0]+=5000
            return dict(bid=4306,ask=4306.2,time_ms=clock[0])
        pump.exchange.quote.side_effect=delayed_market
        with patch('market_push.milliseconds',side_effect=lambda:clock[0]), patch('trading_engine.stamp',side_effect=lambda:clock[0]):
            pump.collect()
        q=pump.read()[0]
        self.assertEqual(q['mt5']['observed_ms'],at)
        self.assertFalse(q['valid'])
        # The between-leg read uses the same receipt-before-REST rule.
        r=server.TradingRuntime.__new__(server.TradingRuntime)
        r.config=c;r.mt5_freshness=TickFreshness();r.push_bridge=None
        r.terminal=t;r.market_stream=s;r.binance=pump.exchange;clock[0]=at
        with patch('market_push.milliseconds',side_effect=lambda:clock[0]), patch('server.now_ms',side_effect=lambda:clock[0]):
            q=r._execution_quote()
        self.assertEqual(q['mt5']['observed_ms'],at)
        self.assertFalse(valid_quote(q,c,now=clock[0]))

    def test_strategy_wakes_without_waiting_poll_interval(self):
        import server
        r=server.TradingRuntime.__new__(server.TradingRuntime)
        r.lock=threading.RLock();r.stop_event=threading.Event();r.market_wake=threading.Event()
        r.connected=True;r.config={'execution':{'poll_ms':5000}};r._last_poll=time.monotonic()
        called=threading.Event();r._poll=lambda:called.set()
        thread=threading.Thread(target=r._loop);thread.start()
        try:r.market_wake.set();self.assertTrue(called.wait(1))
        finally:r.stop_event.set();r.market_wake.set();thread.join(1)


class EventTests(unittest.TestCase):
    def test_replay_and_gap_reset(self):
        events=QuoteEvents(2)
        for i in range(3):events.publish({'time_ms':i})
        seq,reset,rows=events.read(0,0);self.assertTrue(reset);self.assertEqual(seq,3)
        seq,reset,rows=events.read(2,0);self.assertFalse(reset);self.assertEqual([i for i,q in rows],[3])
        self.assertTrue(events.read(99,0)[1])

    def test_http_sse_replay_and_origin(self):
        import server
        import http.client
        events=QuoteEvents();events.publish({'time_ms':1,'entry':2,'exit':3})
        httpd=server.ThreadingHTTPServer(('127.0.0.1',0),server.Handler)
        threading.Thread(target=httpd.serve_forever,daemon=True).start()
        try:
            with patch.object(server,'TRADING',SimpleNamespace(quote_events=events)):
                conn=http.client.HTTPConnection('127.0.0.1',httpd.server_port,timeout=1)
                conn.request('GET','/api/trading/stream',headers={'Last-Event-ID':events.epoch+':0'})
                response=conn.getresponse();self.assertEqual(response.status,200)
                self.assertIn('text/event-stream',response.headers['Content-Type'])
                self.assertEqual(response.readline().decode().strip(),'id: '+events.epoch+':1')
                self.assertEqual(response.readline().decode().strip(),'event: quotes')
                payload=json.loads(response.readline().decode()[6:]);self.assertFalse(payload['reset'])
                self.assertEqual(payload['samples'][0]['entry'],2);conn.close()
                conn=http.client.HTTPConnection('127.0.0.1',httpd.server_port,timeout=1)
                conn.request('GET','/api/trading/stream',headers={'Origin':'https://example.com'})
                response=conn.getresponse();self.assertEqual(response.status,403);response.read();conn.close()
        finally:httpd.shutdown();httpd.server_close()


if __name__=='__main__':unittest.main()
