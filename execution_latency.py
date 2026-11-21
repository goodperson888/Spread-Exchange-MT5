"""Bounded, process-local timings. No disk writes, platform reads or trading locks."""
from collections import deque, OrderedDict
import math
import threading
import time


class LatencyStats:
    LIMIT = 512
    PHASES = frozenset(('quote_wait_ms', 'binance_quote_age_ms', 'mt5_quote_age_ms', 'decision_ms', 'strategy_pass_ms',
        'quote_pickup_ms', 'strategy_lock_wait_ms', 'strategy_cycle_ms', 'pool_prepare_ms', 'pool_rank_ms',
        'mt5_push_parse_ms', 'mt5_push_accept_ms', 'mt5_push_notify_ms',
        'mt5_ea_prepare_ms', 'mt5_ea_send_ms',
        'binance_push_to_publish_ms', 'mt5_push_to_publish_ms',
        'binance_push_to_strategy_ms', 'mt5_push_to_strategy_ms',
        'result_journal_ms', 'open_dispatch_gap_ms', 'open_execution_to_dispatch_ms', 'close_execution_to_dispatch_ms',
        'receipt_handling_wait_ms', 'second_leg_quote_ms', 'liquidity_check_ms',
        'open_dispatch_quote_age_ms', 'close_dispatch_quote_age_ms',
        'open_dispatch_quote_alignment_ms', 'close_dispatch_quote_alignment_ms',
        'open_quote_to_execution_ms', 'close_quote_to_execution_ms', 'mt5_quote_queue_ms', 'mt5_quote_roundtrip_ms',
        'mt5_terminal_queue_ms', 'mt5_terminal_roundtrip_ms', 'mt5_worker_preflight_ms',
        'mt5_order_check_ms', 'mt5_order_send_ms', 'mt5_fill_normalize_ms', 'mt5_worker_submit_ms',
        'intent_journal_ms', 'binance_open_request_ms', 'binance_close_request_ms',
        'mt5_open_request_ms', 'mt5_close_request_ms', 'confirmation_query_ms',
        'pair_open_ms', 'pair_close_ms', 'close_dispatch_gap_ms', 'maintenance_account_ms',
        'maintenance_fx_ms', 'maintenance_funding_ms', 'maintenance_costs_ms'))

    def __init__(self):
        self.lock = threading.Lock()
        self.samples = {}
        self.counts = {}
        self.last_quotes = OrderedDict()
        self.counters = {}
        self.started_ms = int(time.time()*1000)
        self.mode = 'unknown'

    def reset(self, mode):
        with self.lock:
            self.samples.clear(); self.counts.clear()
            self.last_quotes.clear(); self.counters.clear()
            self.started_ms = int(time.time()*1000)
            self.mode = mode if mode in ('live', 'paper') else 'unknown'

    def record(self, phase, value):
        self.record_many(((phase, value),))

    def record_many(self, values):
        # Batch a receipt's fixed stages under one very short memory-only lock.
        with self.lock:
            for phase, value in values:
                if phase not in self.PHASES or not isinstance(value, (int, float)) or isinstance(value, bool): continue
                if not math.isfinite(value) or value < 0: continue
                self.samples.setdefault(phase, deque(maxlen=self.LIMIT)).append(float(value))
                self.counts[phase] = self.counts.get(phase, 0)+1

    @staticmethod
    def published(quote):
        # Local publication only; not broker time and never used for trade eligibility.
        quote['_published_monotonic'] = time.monotonic()
        quote['_published_perf_counter'] = time.perf_counter()

    def outcome(self, order, result, valid):
        leg, action = order.get('leg'), order.get('action')
        if leg not in ('binance', 'mt5') or action not in ('open', 'close'): return
        qty, requested = (result.get('qty') if isinstance(result, dict) else None), order.get('requested')
        if not valid or result.get('status') not in ('done',): status = 'unconfirmed'
        elif qty == 0: status = 'zero_fill'
        elif isinstance(qty, (int, float)) and isinstance(requested, (int, float)) and abs(qty-requested)<1e-8: status = 'full_fill'
        else: status = 'partial_fill'
        with self.lock:
            key = leg+'_'+action+'_'+status
            self.counters[key] = self.counters.get(key, 0)+1

    def quote(self, quote, now_ms):
        times = tuple(quote.get(leg, {}).get('observed_ms') for leg in ('binance', 'mt5'))
        if any(not isinstance(x, (int, float)) or isinstance(x, bool) or not math.isfinite(x)
               or x <= 0 or x > now_ms for x in times): return
        key = quote.get('key')
        identity = (*times, quote.get('_published_monotonic'))
        # Record first strategy pickup only; an unchanged cached quote isn't a new event.
        with self.lock:
            if identity == self.last_quotes.get(key): return
            self.last_quotes[key] = identity; self.last_quotes.move_to_end(key)
            if len(self.last_quotes)>256: self.last_quotes.popitem(last=False)
            for leg in ('binance', 'mt5'):
                transport = quote.get(leg+'_transport', quote.get(leg, {}).get('transport'))
                # Only fixed labels leave the process, never arbitrary upstream text.
                source = ('push' if transport in ('EA Socket 推送', 'WebSocket bookTicker') else
                          'poll' if isinstance(transport, str) and ('轮询' in transport or transport=='REST 回退') else 'unknown')
                name = leg+'_quote_'+source
                self.counters[name] = self.counters.get(name, 0)+1
        values = [('quote_wait_ms', now_ms-max(times))]
        for leg, received in zip(('binance', 'mt5'), times):
            values.append((leg+'_quote_age_ms', now_ms-received))
            pushed=quote.get(leg,{}).get('_push_received_perf_counter')
            published=quote.get('_published_perf_counter')
            if isinstance(pushed,(int,float)) and not isinstance(pushed,bool) and math.isfinite(pushed):
                values.append((leg+'_push_to_strategy_ms',(time.perf_counter()-pushed)*1000))
                if isinstance(published,(int,float)) and not isinstance(published,bool):
                    values.append((leg+'_push_to_publish_ms',(published-pushed)*1000))
        published = quote.get('_published_monotonic')
        if isinstance(published, (int, float)) and not isinstance(published, bool):
            values.append(('quote_pickup_ms', (time.monotonic()-published)*1000))
        self.record_many(values)

    def snapshot(self):
        with self.lock:
            samples = {key:list(values) for key, values in self.samples.items()}
            counts = dict(self.counts)
            counters = dict(self.counters)
            started_ms, mode = self.started_ms, self.mode
        phases = {}
        for key, values in samples.items():
            latest = values[-1]
            values.sort()
            n = len(values)
            phases[key] = dict(samples=n, total_samples=counts[key],
                median_ms=round((values[(n-1)//2]+values[n//2])/2, 3),
                p95_ms=round(values[math.ceil(n*.95)-1], 3), p99_ms=round(values[math.ceil(n*.99)-1], 3),
                max_ms=round(values[-1], 3), latest_ms=round(latest, 3),
                p99_low_sample=n<100)
        return dict(schema_version=2, started_ms=started_ms, mode=mode, sample_limit=self.LIMIT,
                    phases=phases, counters=counters)


PROCESS_LATENCY = LatencyStats()
