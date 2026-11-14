"""Bounded, process-local timings. No disk writes, platform reads or trading locks."""
from collections import deque
import math
import threading
import time


class LatencyStats:
    LIMIT = 512
    PHASES = frozenset(('quote_wait_ms', 'binance_quote_age_ms', 'mt5_quote_age_ms', 'decision_ms', 'strategy_pass_ms',
        'intent_journal_ms', 'binance_open_request_ms', 'binance_close_request_ms',
        'mt5_open_request_ms', 'mt5_close_request_ms', 'confirmation_query_ms',
        'pair_open_ms', 'pair_close_ms', 'close_dispatch_gap_ms', 'maintenance_account_ms',
        'maintenance_fx_ms', 'maintenance_funding_ms', 'maintenance_costs_ms'))

    def __init__(self):
        self.lock = threading.Lock()
        self.samples = {}
        self.counts = {}
        self.last_quote = None
        self.started_ms = int(time.time()*1000)
        self.mode = 'unknown'

    def reset(self, mode):
        with self.lock:
            self.samples.clear(); self.counts.clear()
            self.last_quote = None
            self.started_ms = int(time.time()*1000)
            self.mode = mode if mode in ('live', 'paper') else 'unknown'

    def record(self, phase, value):
        if phase not in self.PHASES or not isinstance(value, (int, float)) or isinstance(value, bool): return
        if not math.isfinite(value) or value < 0: return
        with self.lock:
            self.samples.setdefault(phase, deque(maxlen=self.LIMIT)).append(float(value))
            self.counts[phase] = self.counts.get(phase, 0)+1

    def quote(self, quote, now_ms):
        times = tuple(quote.get(leg, {}).get('observed_ms') for leg in ('binance', 'mt5'))
        if any(not isinstance(x, (int, float)) or isinstance(x, bool) or not math.isfinite(x)
               or x <= 0 or x > now_ms for x in times): return
        identity = (quote.get('key'), *times)
        # Record first strategy pickup only; an unchanged cached quote isn't a new event.
        if identity == self.last_quote: return
        self.last_quote = identity
        self.record('quote_wait_ms', now_ms-max(times))
        for leg, received in zip(('binance', 'mt5'), times):
            self.record(leg+'_quote_age_ms', now_ms-received)

    def snapshot(self):
        with self.lock:
            samples = {key:list(values) for key, values in self.samples.items()}
            counts = dict(self.counts)
            started_ms, mode = self.started_ms, self.mode
        phases = {}
        for key, values in samples.items():
            values.sort()
            n = len(values)
            phases[key] = dict(samples=n, total_samples=counts[key],
                median_ms=round((values[(n-1)//2]+values[n//2])/2, 3),
                p95_ms=round(values[math.ceil(n*.95)-1], 3), max_ms=round(values[-1], 3))
        return dict(schema_version=1, started_ms=started_ms, mode=mode, sample_limit=self.LIMIT, phases=phases)


PROCESS_LATENCY = LatencyStats()
