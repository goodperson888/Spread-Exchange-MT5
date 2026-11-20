"""New-source-quote entry confirmation. Memory only; no broker or disk calls."""
from collections import OrderedDict
from decimal import Decimal, ROUND_HALF_UP, InvalidOperation
import math
import threading
import time

DEFAULTS = {'entry_confirmation_decimals': 2, 'exit_confirmation_ticks': 1}
for _direction in ('forward', 'reverse'):
    DEFAULTS.update({f'entry_{_direction}_confirmation_mode': 'at_or_better',
                     f'entry_{_direction}_confirmation_ticks': 1})


def settings(strategy, direction):
    return (strategy.get(f'entry_{direction}_confirmation_mode', 'at_or_better'),
            strategy.get(f'entry_{direction}_confirmation_ticks', 1),
            strategy.get('entry_confirmation_decimals', 2))


def reached(strategy, direction, value, threshold):
    if not all(isinstance(x, (int, float)) and not isinstance(x, bool) and math.isfinite(x)
               for x in (value, threshold)):
        return False
    mode, _, decimals = settings(strategy, direction)
    if mode == 'equal':
        unit = Decimal(1).scaleb(-decimals)
        try:
            return (Decimal(str(value)).quantize(unit, rounding=ROUND_HALF_UP) ==
                    Decimal(str(threshold)).quantize(unit, rounding=ROUND_HALF_UP))
        except InvalidOperation:
            return False
    return value <= threshold if direction == 'reverse' else value >= threshold


def source_token(quote):
    # Receipt time, heartbeat, transport and local publication sequence must
    # never turn a repeated platform tick into an extra confirmation.
    return tuple((('sequence',book['source_sequence']) if book.get('source_sequence') is not None else
                  None if book.get('time_basis')=='receipt_only' else book.get('source_time_ms', book.get('time_ms', 0)),
                  book.get('bid'), book.get('ask'))
                 for book in (quote.get('binance', {}), quote.get('mt5', {})))


class EntryConfirmations:
    LIMIT = 2048

    def __init__(self):
        self.rows = OrderedDict()
        self.lock = threading.RLock()

    def reset(self, key=None):
        # Keep source watermarks so an old cached tick cannot immediately
        # re-arm after pause, a submission, or the end of a stale interval.
        with self.lock:
            rows = self.rows.values() if key is None else (self.rows.get((key,d)) for d in ('forward','reverse'))
            for row in rows:
                if row is not None: row.update(count=0, ready=False, matched=False)

    def clear(self):
        """A newly installed connection has a new source-clock lifetime."""
        with self.lock:
            self.rows.clear()

    def observe(self, key, strategy, quote, direction, threshold, *, enabled=True,
                valid=True, level=0, now=None, reuse_single=False):
        now = time.monotonic() if now is None else now
        mode, required, decimals = settings(strategy, direction)
        signature = (mode, required, decimals, level, tuple(sorted(strategy.items())))
        token = source_token(quote) if quote else None
        index = (key, direction)
        with self.lock:
            row = self.rows.get(index)
            if row is None or row['signature'] != signature:
                old_token = row['token'] if row else None
                row = dict(signature=signature, token=old_token, count=0, ready=False,
                           required=required, mode=mode, decimals=decimals, updated=now)
                self.rows[index] = row
                while len(self.rows) > self.LIMIT:
                    self.rows.popitem(last=False)
            self.rows.move_to_end(index)
            value = quote.get('exit' if direction == 'reverse' else 'entry') if quote else None
            hit = valid and enabled and reached(strategy, direction, value, threshold)
            row.update(matched=bool(hit), threshold=threshold)
            if now-row['updated'] > strategy.get('max_quote_age_ms', 3000)/1000:
                row.update(count=0, ready=False)
            if not hit:
                row.update(count=0, ready=False)
            # A one-sample profit exit preserves the original immediate
            # reevaluation after fee updates or a deferred preflight. Entry
            # confirmations and multi-sample exits always require new ticks.
            if token is not None and (token != row['token'] or reuse_single and required == 1):
                previous = row['token']
                # A late fallback result must not replay a superseded quote.
                if previous and any(a[0] and b[0] and a[0] < b[0] for a,b in zip(token,previous)):
                    row.update(count=0, ready=False)
                else:
                    row.update(token=token, updated=now)
                    if hit:
                        row['count'] = min(required, row['count']+1)
                        row['ready'] = row['count'] >= required
            return self._public(row)

    @staticmethod
    def _public(row):
        return {k:row[k] for k in ('count','required','ready','mode','decimals','matched','threshold')
                if k in row}

    def snapshot(self, key, *, now=None, max_age_ms=3000):
        now = time.monotonic() if now is None else now
        with self.lock:
            result = {}
            for direction in ('forward','reverse'):
                row = self.rows.get((key,direction))
                if row is None: continue
                public = self._public(row)
                if now-row['updated'] > max_age_ms/1000:
                    public.update(count=0, ready=False)
                result[direction] = public
            return result
