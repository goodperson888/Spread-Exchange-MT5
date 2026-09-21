"""Background, non-executing Binance checks. No trading-runtime lock is held.

Readiness is short-lived and scoped to one connection and opening context.
It never reserves liquidity and never guarantees either leg will fill.
"""
import copy
import threading
import time


class EntryPreflight:
    TTL = 10.0
    REFRESH = 5.0
    SYNC_INTERVAL = 30.0
    NEAR_USD = 0.5

    def __init__(self, client, trading_client, spec, clock=time.monotonic, start=True):
        self.client, self.trading_client, self.spec = client, trading_client, spec
        self.clock = clock
        self.lock = threading.Lock()
        self.wake = threading.Event()
        self.stopped = threading.Event()
        self.context = None
        self.revision = 0
        self.result = None
        self.running = False
        self.last_attempt = -float('inf')
        self.last_sync = -float('inf')
        self.sync_error = ''
        if start:
            self.thread = threading.Thread(target=self._loop, name='gold-pair-entry-preflight', daemon=True)
            self.thread.start()

    def close(self):
        self.stopped.set()
        self.wake.set()

    def update(self, context):
        with self.lock:
            old_key = self.context.get('key') if self.context else None
            new_key = context.get('key') if context else None
            if old_key != new_key:
                self.revision += 1
                self.result = None
            self.context = copy.deepcopy(context)
        self.wake.set()

    def invalidate(self):
        with self.lock:
            self.revision += 1
            self.result = None

    def status(self):
        with self.lock:
            now = self.clock()
            result = self.result
            age = now-result['started'] if result else None
            ready = bool(self.context and result and result['ok'] and age < self.TTL and not self.stopped.is_set())
            state = ('ready' if ready else 'checking' if self.running else
                     'failed' if result and not result['ok'] else 'expired' if result else 'waiting')
            return dict(ready=ready, state=state, checking=self.running,
                        age_ms=round(age*1000) if age is not None else None,
                        ttl_ms=int(self.TTL*1000), refresh_ms=int(self.REFRESH*1000),
                        near_usd=self.NEAR_USD,
                        message=result['message'] if result else self.sync_error or '等待接近下一档开仓阈值',
                        clock_offset_ms=getattr(self.client, 'offset', None),
                        sync_rtt_ms=getattr(self.client, 'sync_rtt_ms', None))

    def _sync(self):
        self.client.sync()
        if self.stopped.is_set(): return
        # Publish an immutable clock anchor, never change credentials or mode.
        self.trading_client.offset = self.client.offset
        self.trading_client._clock_anchor = self.client._clock_anchor
        self.trading_client.sync_rtt_ms = self.client.sync_rtt_ms

    def _step(self):
        with self.lock:
            now = self.clock()
            context, revision = copy.deepcopy(self.context), self.revision
            check = bool(context and now-self.last_attempt >= self.REFRESH)
            calibrate = now-self.last_sync >= self.SYNC_INTERVAL
            if not check and not calibrate: return
            if check:
                self.last_attempt = now
                self.running = True
            self.last_sync = now
        try:
            self._sync()
            self.sync_error = ''
            if check:
                report = self.client.preflight(sync_clock=False)
                if report['position_mode'] != context['position_mode']:
                    raise ValueError('币安持仓模式变化，请重新连接并对账')
                self.client.test_order(dict(action='open', symbol=context['symbol'],
                    requested=context['qty'], limit=context['price'], position_side='SHORT'), self.spec)
                message = '币安校时、账户及测试订单通过；预检不保证实际成交'
            else:
                message = '币安时钟已同步'
            ok = True
        except Exception as exc:
            ok, message = False, str(exc)
            self.sync_error = message
        finally:
            with self.lock:
                self.running = False
                if revision == self.revision and not self.stopped.is_set():
                    if check:
                        # Age starts before network calls, never at response time.
                        self.result = dict(ok=ok, message=message, started=now)
                    elif not ok:
                        self.result = None

    def _loop(self):
        while not self.stopped.is_set():
            self.wake.wait(1)
            self.wake.clear()
            if not self.stopped.is_set(): self._step()
