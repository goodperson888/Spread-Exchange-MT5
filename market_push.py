"""Local quote push transport. No trading commands or account credentials on this channel."""
import collections
import hmac
import json
import math
import secrets
import socket
import socketserver
import threading
import time
from execution_latency import LatencyStats, PROCESS_LATENCY
from quote_fallback import BackgroundRead


def milliseconds():
    return int(time.time() * 1000)


def observed_quote(value, observed_ms=None):
    """Keep broker/server tick time for display, but use local receipt time for freshness."""
    result = dict(value)
    result['source_time_ms'] = int(value.get('source_time_ms', value.get('time_ms', 0)))
    result['observed_ms'] = int(value.get('observed_ms', value.get('received_ms', observed_ms or milliseconds())))
    return result


class TickFreshness:
    """Keep the first local observation of an MT5 tick across transports."""
    def __init__(self):
        self.lock = threading.Lock()
        self.ticks = {}

    def observe(self, value, settings, observed_ms=None, *, discard_older=False):
        quote = observed_quote(value, observed_ms)
        identity = tuple(str(settings.get(k, '')) for k in ('account', 'server', 'symbol'))
        signature = (quote['source_time_ms'], quote['bid'], quote['ask'])
        with self.lock:
            previous = self.ticks.get(identity)
            if previous:
                old_signature, first_seen = previous
                if signature[0] < old_signature[0]:
                    if discard_older:return None
                    raise ValueError('MT5 报价时间倒退，等待新 tick')
                if signature == old_signature:
                    quote['observed_ms'] = min(first_seen, quote['observed_ms'])
            self.ticks[identity] = (signature, quote['observed_ms'])
        return quote


class QuoteEvents:
    """Bounded replay buffer shared by SSE clients; slow clients never block producers."""
    def __init__(self, capacity=10000):
        self.condition = threading.Condition()
        self.rows = collections.deque(maxlen=capacity)
        self.sequence = 0
        self.epoch = secrets.token_hex(8)

    def publish(self, quote):
        with self.condition:
            self.sequence += 1
            self.rows.append((self.sequence, quote))
            self.condition.notify_all()

    def read(self, cursor, timeout=10):
        with self.condition:
            if self.sequence == cursor:
                self.condition.wait_for(lambda: self.sequence > cursor, timeout)
            reset = cursor > self.sequence or bool(self.rows and cursor < self.rows[0][0] - 1)
            rows = list(self.rows) if reset else [(i, q) for i, q in self.rows if i > cursor]
            return self.sequence, reset, rows


class PushBridge:
    """Authenticated, loopback-only MQL5 TCP receiver with one writer per identity."""
    def __init__(self, token, port=8767, notify=None, latency=None):
        self.token, self.port, self.notify = token, port, notify
        self.lock = threading.Lock()
        self.values, self.owners = {}, {}
        self.server = None
        self.latency = latency if latency is not None else PROCESS_LATENCY

    def start(self):
        bridge = self
        class Handler(socketserver.StreamRequestHandler):
            def handle(self):
                self.connection.settimeout(5)
                self.connection.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                identity = None
                owner = object()
                try:
                    line = self.rfile.readline(4097)
                    if len(line) > 4096: return
                    hello = json.loads(line)
                    if not isinstance(hello, dict): return
                    if not hmac.compare_digest(str(hello.get('token', '')), bridge.token): return
                    identity = bridge.identity(hello)
                    if not all(identity): return
                    with bridge.lock:
                        if identity in bridge.owners: return
                        bridge.owners[identity] = owner
                        bridge.values.pop(identity, None)
                    self.wfile.write(b'OK\n'); self.wfile.flush()
                    last_seq = -1
                    while True:
                        line = self.rfile.readline(4097)
                        received_counter=time.perf_counter();received_ms=milliseconds()
                        if not line or len(line) > 4096: break
                        data = json.loads(line)
                        if not isinstance(data, dict): break
                        if data.get('type') == 'ping': continue
                        if bridge.identity(data) != identity: break
                        seq = int(data['seq'])
                        if seq <= last_seq: continue
                        parsed=time.perf_counter()
                        if bridge.accept(identity, owner, data, received_ms, received_counter):
                            last_seq = seq
                            bridge.latency.record('mt5_push_parse_ms',(parsed-received_counter)*1000)
                except (OSError, ValueError, TypeError, KeyError, OverflowError):
                    pass
                finally:
                    with bridge.lock:
                        if bridge.owners.get(identity) is owner:
                            bridge.owners.pop(identity, None)
                            bridge.values.pop(identity, None)
                    if bridge.notify: bridge.notify()
        class Server(socketserver.ThreadingTCPServer):
            allow_reuse_address = True
            daemon_threads = True
        self.server = Server(('127.0.0.1', self.port), Handler)
        self.port = self.server.server_address[1]
        threading.Thread(target=self.server.serve_forever, name='mt5-ea-push', daemon=True).start()
        return self

    @staticmethod
    def identity(data):
        return tuple(str(data.get(k, '')) for k in ('account', 'server', 'symbol'))

    def accept(self, identity, owner, data, received_ms=None, received_counter=None):
        started=time.perf_counter()
        bid, ask, at = float(data['bid']), float(data['ask']), int(data['time_ms'])
        if not (math.isfinite(bid) and math.isfinite(ask) and 0 < bid <= ask): return False
        with self.lock:
            old = self.values.get(identity)
            if self.owners.get(identity) is not owner or (old and at < old['source_time_ms']): return False
            quote=observed_quote(dict(bid=bid, ask=ask, time_ms=at,
                transport='EA Socket 推送', _push_received_perf_counter=(
                    received_counter if received_counter is not None else started)),received_ms)
            if old and (at,bid,ask)==(old['source_time_ms'],old['bid'],old['ask']):
                quote['observed_ms']=old['observed_ms']
                quote['_push_received_perf_counter']=old['_push_received_perf_counter']
            self.values[identity]=quote
        accepted=time.perf_counter()
        if self.notify:self.notify()
        timings=[('mt5_push_accept_ms',(accepted-started)*1000),
                 ('mt5_push_notify_ms',(time.perf_counter()-accepted)*1000)]
        # The EA reports its previous successful send, measured on its own
        # monotonic clock. Never subtract an EA clock from the Python clock.
        for field,phase in (('prev_prepare_us','mt5_ea_prepare_ms'),('prev_send_us','mt5_ea_send_ms')):
            value=data.get(field)
            if isinstance(value,(int,float)) and not isinstance(value,bool) and math.isfinite(value) and 0<=value<=60000000:
                timings.append((phase,value/1000))
        self.latency.record_many(timings)
        return True

    def quote(self, settings, max_age_ms):
        with self.lock:
            value = self.values.get(self.identity(settings))
            if not value or milliseconds() - value['observed_ms'] > max_age_ms: return None
            return dict(value)

    def close(self):
        if self.server:
            self.server.shutdown(); self.server.server_close()
            self.server = None


class QuotePump:
    """Quote intake and persistence independent of the serialized trading worker."""
    def __init__(self, config, exchange, stream, terminal, bridge, paper_quote, publish, wake_strategy,
                 mt5_freshness=None, quote_events=None):
        self.config, self.exchange, self.stream, self.terminal = config, exchange, stream, terminal
        self.bridge, self.paper_quote, self.publish, self.wake_strategy = bridge, paper_quote, publish, wake_strategy
        self.changed, self.stopped = threading.Event(), threading.Event()
        self.lock = threading.Lock()
        self.latest = None
        self.error = ''
        self.sampling_error = ''
        self.version = 0
        self.mt5_transport = '等待行情'
        self.binance_transport = '等待行情'
        self.last_signature = None
        self.last_stamp = 0
        self.mt5_cache = self.rest_cache = None
        self.mt5_read_at = self.rest_read_at = 0
        self.mt5_freshness = mt5_freshness or TickFreshness()
        self.quote_events = quote_events
        self.pending_sample = None
        self.sample_wake = threading.Event()
        self.mt5_fallback = BackgroundRead(lambda:self._read_mt5(discard_older=True),self.changed,'pair-mt5-fallback')
        self.rest_fallback = BackgroundRead(lambda: observed_quote(self.exchange.quote(self.config['symbol'])),
                                            self.changed,'pair-rest-fallback')

    def start(self):
        self.stream.on_quote = self.changed.set
        self.sample_thread = threading.Thread(target=self._samples,name='pair-quote-history',daemon=True)
        self.sample_thread.start()
        self.thread = threading.Thread(target=self.run, name='pair-quote-intake', daemon=True)
        self.thread.start()
        return self

    def read(self):
        with self.lock: return self.latest, self.version, self.error or self.sampling_error

    def _read_mt5(self,discard_older=False):
        c=self.config
        value=(self.paper_quote(c['mt5']) if c['mt5']['adapter']=='paper' else
               self.terminal.call('quote' if c['mt5']['adapter']=='native' else 'snapshot'))['quote']
        return self.mt5_freshness.observe(value,c['mt5'],discard_older=discard_older)

    def collect(self,background=False):
        c = self.config
        age = c['strategy']['max_quote_age_ms']
        poll = c['execution'].get('mt5_quote_poll_ms', 50) / 1000 if c['mt5']['adapter']=='native' else c['execution']['poll_ms'] / 1000
        now = time.monotonic()
        pushed = self.bridge.quote(c['mt5'], age) if self.bridge and c['mt5']['adapter'] != 'paper' else None
        if pushed:
            mt5 = pushed
            self.mt5_transport = 'EA Socket 推送'
        else:
            if background:
                mt5,error=self.mt5_fallback.read(poll)
                if error:raise error
                if mt5 is None:return
            else:
                if self.mt5_cache is None or not self.mt5_read_at or now - self.mt5_read_at >= poll:
                    self.mt5_cache = self._read_mt5()
                    self.mt5_read_at = time.monotonic()
                mt5 = self.mt5_cache
            self.mt5_transport = ('MCP' if c['mt5']['adapter']=='mcp' else '原生') + '轮询兼容（EA 未接入或报价过期）'
        # Record receipt before a potentially slow REST fallback on the other
        # platform. Pair construction must not hide that elapsed time.
        mt5 = self.mt5_freshness.observe(mt5, c['mt5'],discard_older=background)
        if mt5 is None:return
        # Read exchange AFTER any blocking terminal read, never pair a fresh MT5 tick with an old cached exchange tick.
        market = self.stream.quote(max_age_ms=age)
        if market is None:
            rest_interval=getattr(self.exchange,'quote_rest_interval',1.)
            if background:
                market,error=self.rest_fallback.read(rest_interval)
                if error:raise error
                if market is None:return
            else:
                if self.rest_cache is None or not self.rest_read_at or time.monotonic() - self.rest_read_at >= rest_interval:
                    self.rest_cache = observed_quote(self.exchange.quote(c['symbol']))
                    self.rest_read_at = time.monotonic()
                market = self.rest_cache
            self.binance_transport = 'REST 回退'
        else: self.binance_transport = 'WebSocket 盘口' if getattr(self.exchange,'name','')=='Lighter' else 'WebSocket bookTicker'
        fx = c['costs']['usdt_usd']
        observed_at = milliseconds()
        market = observed_quote(market, observed_at)
        # Transport changes are observable state too.  A pushed tick can have
        # the same broker timestamp and prices as the compatibility poll; keep
        # one fresh event so the UI changes from polling to EA push immediately.
        signature = tuple((q['source_time_ms'], q.get('source_sequence'), q['bid'], q['ask']) for q in (mt5, market)) + (
            fx, self.mt5_transport, self.binance_transport)
        if signature == self.last_signature: return
        from trading_config import pair_key
        from pair_instruments import pair_info
        from trading_engine import valid_quote
        q = dict(key=pair_key(c), pair=pair_info(c), time_ms=milliseconds(), mt5=dict(mt5), binance=dict(market), usdt_usd=fx,
                 entry=market['bid']*fx-mt5['ask'], exit=market['ask']*fx-mt5['bid'],
                 mt5_transport=self.mt5_transport, binance_transport=self.binance_transport,
                 exchange_stream_status=self.stream.status() if hasattr(self.stream,'status') else {})
        q['valid'] = valid_quote(q, c)
        # Keep raw sample IDs unique without rounding the display/strategy to seconds.
        q['time_ms'] = max(q['time_ms'], self.last_stamp + 1)
        self.last_stamp = q['time_ms']; self.last_signature = signature
        if self.stopped.is_set(): return
        with self.lock:
            LatencyStats.published(q)
            self.latest, self.version, self.error = q, self.version + 1, ''
        self.wake_strategy.set()
        if self.quote_events:self.quote_events.publish(q)
        if background:
            with self.lock:self.pending_sample=q
            self.sample_wake.set()
        else:self.publish(q)

    def _samples(self):
        while not self.stopped.is_set():
            self.sample_wake.wait(.1);self.sample_wake.clear()
            with self.lock:pending=self.pending_sample;self.pending_sample=None
            if pending is not None:
                try:self.publish(pending)
                except Exception as exc:
                    with self.lock:self.sampling_error='行情历史写入失败：'+str(exc)
                    self.wake_strategy.set()
            self.stopped.wait(.1)

    def run(self):
        while not self.stopped.is_set():
            self.changed.clear()
            try: self.collect(background=True)
            except Exception as exc:
                with self.lock: self.error = str(exc)
                self.wake_strategy.set()
            self.changed.wait(min(self.config['execution']['poll_ms'],self.config['execution'].get('mt5_quote_poll_ms',50)
                                  if self.config['mt5']['adapter']=='native' else self.config['execution']['poll_ms']) / 1000)

    def close(self):
        self.stopped.set(); self.changed.set();self.sample_wake.set()
        self.mt5_fallback.close();self.rest_fallback.close()
        if hasattr(self, 'thread'): self.thread.join(timeout=.5)
        if hasattr(self,'sample_thread'):self.sample_thread.join(timeout=.5)
