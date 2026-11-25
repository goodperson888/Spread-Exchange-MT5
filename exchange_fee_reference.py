"""Allowlisted fee provenance; no credentials, raw errors or I/O in snapshots."""
import math
import time

_CURRENT = {}


def safe(value):
    if not isinstance(value, dict): return {}
    result = {}
    for key in ('taker_percent', 'checked_ms', 'attempted_ms'):
        n = value.get(key)
        if isinstance(n, (int, float)) and not isinstance(n, bool) and math.isfinite(n):
            if n >= 0 and (key != 'taker_percent' or n <= 5): result[key] = n
    for key, allowed in (('state', ('verified', 'manual', 'fallback', 'estimate')),
                         ('exchange', ('binance', 'lighter')),
                         ('error_code', ('NONE', 'TIMEOUT', 'UNITS_UNVERIFIED', 'READ_FAILED'))):
        if value.get(key) in allowed: result[key] = value[key]
    return result


def initial(config):
    return safe(dict(exchange=config.get('pair', {}).get('exchange', 'binance'),
        state=('estimate' if config['execution']['mode'] != 'live' else
               'fallback' if config['costs']['binance_fee_auto'] else 'manual'),
        taker_percent=config['costs']['binance_taker_percent'], error_code='NONE'))


def verified(previous, fees):
    rate = fees.get('taker')
    if isinstance(rate, bool) or not isinstance(rate, (int, float)) or not math.isfinite(rate) or not 0 <= rate <= 5:
        raise ValueError('invalid account fee rate')
    now = int(time.time()*1000)
    return safe(dict(previous, state='verified', taker_percent=rate,
                     checked_ms=now, attempted_ms=now, error_code='NONE'))


def failed(previous, error):
    code = ('UNITS_UNVERIFIED' if '计量单位' in str(error) else
            'TIMEOUT' if isinstance(error, TimeoutError) or 'timeout' in str(error).lower() or '超时' in str(error)
            else 'READ_FAILED')
    return safe(dict(previous, attempted_ms=int(time.time()*1000), error_code=code))


def register(value):
    global _CURRENT
    _CURRENT = safe(value)
    return dict(_CURRENT)


def snapshot():
    return dict(_CURRENT)
