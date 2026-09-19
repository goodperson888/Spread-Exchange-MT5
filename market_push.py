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


def milliseconds():
    return int(time.time() * 1000)


def observed_quote(value, observed_ms=None):
    """Keep broker/server tick time for display, but use local receipt time for freshness."""
    result = dict(value)
    result['source_time_ms'] = int(value.get('source_time_ms', value.get('time_ms', 0)))
    result['observed_ms'] = int(value.get('observed_ms', value.get('received_ms', observed_ms or milliseconds())))
    return result


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
    def __init__(self, token, port=8767, notify=None):
        self.token, self.port, self.notify = token, port, notify
        self.lock = threading.Lock()
        self.values, self.owners = {}, {}
        self.server = None

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
                        if not line or len(line) > 4096: break
                        data = json.loads(line)
                        if not isinstance(data, dict): break
                        if data.get('type') == 'ping': continue
                        if bridge.identity(data) != identity: break
                        seq = int(data['seq'])
                        if seq <= last_seq: continue
                        if bridge.accept(identity, owner, data): last_seq = seq
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

    def accept(self, identity, owner, data):
        bid, ask, at = float(data['bid']), float(data['ask']), int(data['time_ms'])
        if not (math.isfinite(bid) and math.isfinite(ask) and 0 < bid <= ask): return False
        with self.lock:
            old = self.values.get(identity)
            if self.owners.get(identity) is not owner or (old and at < old['source_time_ms']): return False
            self.values[identity] = observed_quote(dict(bid=bid, ask=ask, time_ms=at,
                                                        transport='EA Socket 推送'))
        if self.notify: self.notify()
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
    def __init__(self, config, exchange, stream, terminal, bridge, paper_quote, publish, wake_strategy):
        self.config, self.exchange, self.stream, self.terminal = config, exchange, stream, terminal
        self.bridge, self.paper_quote, self.publish, self.wake_strategy = bridge, paper_quote, publish, wake_strategy
        self.changed, self.stopped = threading.Event(), threading.Event()
        self.lock = threading.Lock()
        self.latest = None
        self.error = ''
        self.version = 0
        self.mt5_transport = '等待行情'
        self.binance_transport = '等待行情'
        self.last_signature = None
        self.last_stamp = 0
        self.mt5_cache = self.rest_cache = None
        self.mt5_read_at = self.rest_read_at = 0

    def start(self):
        self.stream.on_quote = self.changed.set
        self.thread = threading.Thread(target=self.run, name='pair-quote-intake', daemon=True)
        self.thread.start()
        return self

    def read(self):
        with self.lock: return self.latest, self.version, self.error

    def collect(self):
        c = self.config
        age = c['strategy']['max_quote_age_ms']
        poll = c['execution']['poll_ms'] / 1000
        now = time.monotonic()
        pushed = self.bridge.quote(c['mt5'], age) if self.bridge and c['mt5']['adapter'] != 'paper' else None
        if pushed:
            mt5 = pushed
            self.mt5_transport = 'EA Socket 推送'
        else:
            if self.mt5_cache is None or now - self.mt5_read_at >= poll:
                if c['mt5']['adapter'] == 'paper': self.mt5_cache = self.paper_quote(c['mt5'])['quote']
                else: self.mt5_cache = self.terminal.call('quote' if c['mt5']['adapter'] == 'native' else 'snapshot')['quote']
                self.mt5_read_at = time.monotonic()
            mt5 = self.mt5_cache
            self.mt5_transport = ('MCP' if c['mt5']['adapter']=='mcp' else '原生') + '轮询兼容（EA 未接入或报价过期）'
        # Read exchange AFTER any blocking terminal read, never pair a fresh MT5 tick with an old cached exchange tick.
        market = self.stream.quote(max_age_ms=age)
        if market is None:
            if self.rest_cache is None or time.monotonic() - self.rest_read_at >= 1:
                self.rest_cache = self.exchange.quote(c['symbol']); self.rest_read_at = time.monotonic()
            market = self.rest_cache
            self.binance_transport = 'REST 回退'
        else: self.binance_transport = 'WebSocket bookTicker'
        fx = c['costs']['usdt_usd']
        observed_at = milliseconds()
        mt5 = observed_quote(mt5, observed_at)
        market = observed_quote(market, observed_at)
        # Transport changes are observable state too.  A pushed tick can have
        # the same broker timestamp and prices as the compatibility poll; keep
        # one fresh event so the UI changes from polling to EA push immediately.
        signature = tuple((q['source_time_ms'], q['bid'], q['ask']) for q in (mt5, market)) + (
            fx, self.mt5_transport, self.binance_transport)
        if signature == self.last_signature: return
        from trading_config import pair_key
        from trading_engine import valid_quote
        q = dict(key=pair_key(c), time_ms=milliseconds(), mt5=dict(mt5), binance=dict(market), usdt_usd=fx,
                 entry=market['bid']*fx-mt5['ask'], exit=market['ask']*fx-mt5['bid'],
                 mt5_transport=self.mt5_transport, binance_transport=self.binance_transport)
        q['valid'] = valid_quote(q, c)
        # Keep raw sample IDs unique without rounding the display/strategy to seconds.
        q['time_ms'] = max(q['time_ms'], self.last_stamp + 1)
        self.last_stamp = q['time_ms']; self.last_signature = signature
        if self.stopped.is_set(): return
        self.publish(q)
        with self.lock:
            self.latest, self.version, self.error = q, self.version + 1, ''
        self.wake_strategy.set()

    def run(self):
        while not self.stopped.is_set():
            self.changed.clear()
            try: self.collect()
            except Exception as exc:
                with self.lock: self.error = str(exc)
                self.wake_strategy.set()
            self.changed.wait(self.config['execution']['poll_ms'] / 1000)

    def close(self):
        self.stopped.set(); self.changed.set()
        if hasattr(self, 'thread'): self.thread.join(timeout=.5)
