"""Read-only per-group cost previews from already cached quotes and fee samples."""
import math
import time
from commission_reference import fee_rate, MAX_AGE_MS, MIN_SAMPLES
from exchange_fee_reference import safe as safe_exchange_fee
from pair_instruments import native_pair, pair_info, position_side, direction_quote


def finite(n):
    return isinstance(n, (int, float)) and not isinstance(n, bool) and math.isfinite(n)


def commission(group, leg, action, now):
    rate = fee_rate(group, leg, action)
    a = leg == 'binance'
    auto = group['costs'].get('mt5_short_fee_auto' if a else 'mt5_fee_auto', False)
    source = 'fallback' if auto else 'manual'
    ref = group.get('commission_reference', {}).get(leg, {})
    side = position_side(group, leg) if native_pair(group) else 0
    if action == 'close': side = 1-side
    row = ref.get('profiles', {}).get(action+'_'+str(side), {})
    checked = ref.get('checked_ms')
    expected = group['symbol'] if a else group['mt5_symbol']
    if (auto and ref.get('symbol') == expected and finite(checked) and 0 <= now-checked <= MAX_AGE_MS
            and row.get('state') == 'ready' and finite(row.get('rate')) and 0 <= row['rate'] <= 10000
            and finite(row.get('lots')) and abs(row['lots']-group['lots']) <= max(1e-8, group['lots']*.01)
            and row.get('samples', 0) >= MIN_SAMPLES): source = 'history'
    return dict(rate_per_lot_side=rate, source=source)


def estimate(config, plan, quote, reference=None, exchange_reference=None, *, fresh=False, now=None):
    if not config or not plan: return None
    now = int(time.time()*1000) if now is None else now
    native = native_pair(config)
    c, s = config['costs'], config['strategy']
    lots = plan.get('lots')
    if not finite(lots) or lots <= 0: return None
    contracts = plan.get('leg_contracts', {}) if native else {}
    exposures = ({leg: lots*contracts.get(leg, 0) for leg in ('binance', 'mt5')}
                 if native else dict(binance=plan.get('qty', 0), mt5=lots*plan.get('contract', 0)))
    if not all(finite(v) and v > 0 for v in exposures.values()): return None
    balanced = math.isclose(exposures['binance'], exposures['mt5'], rel_tol=1e-9, abs_tol=1e-10)
    scale = exposures['binance'] if balanced else None
    q = quote or {}
    books = {leg:q.get(leg, {}) for leg in exposures}
    prices_ok = all(finite(b.get('bid')) and finite(b.get('ask')) and 0 < b['bid'] <= b['ask'] for b in books.values())
    fx = 1. if native else q.get('usdt_usd', c['usdt_usd'])
    if not finite(fx) or fx <= 0: prices_ok = False
    fx = fx if finite(fx) and fx > 0 else c['usdt_usd']
    directions = ('forward', 'reverse') if native and s.get('direction_mode') == 'auto' else (
        s.get('direction_mode', 'forward') if native else 'forward',)
    rows = []
    for direction in directions:
        group = dict(pair=pair_info(config), direction=direction, costs=c, lots=lots, opened_ms=now,
                     symbol=config['symbol'], mt5_symbol=config['mt5']['symbol'], commission_reference=reference or {})
        fees = {}
        for leg in ('binance', 'mt5'):
            if native or leg == 'mt5':
                actions = {action:commission(group, leg, action, now) for action in ('open', 'close')}
                fees[leg] = dict(open=lots*actions['open']['rate_per_lot_side'],
                                 close=lots*actions['close']['rate_per_lot_side'], basis=actions)
            else:
                rate = c['binance_taker_percent']
                fees[leg] = dict(open=books[leg]['bid']*exposures[leg]*fx*rate/100 if prices_ok else None,
                                 close=books[leg]['ask']*exposures[leg]*fx*rate/100 if prices_ok else None,
                                 taker_percent=rate, basis=safe_exchange_fee(exchange_reference))
            f = fees[leg]
            f['round_trip'] = f['open']+f['close'] if all(finite(f[k]) for k in ('open', 'close')) else None
            rebate_key = ('mt5_short_rebate_percent' if native else 'binance_rebate_percent') if leg == 'binance' else 'mt5_rebate_percent'
            f['estimated_rebate'] = f['round_trip']*c.get(rebate_key, 0)/100 if finite(f['round_trip']) else None
            f['spread_loss'] = (books[leg]['ask']-books[leg]['bid'])*exposures[leg]*(fx if not native and leg == 'binance' else 1) if prices_ok else None
        total = sum(f['round_trip'] for f in fees.values()) if all(finite(f['round_trip']) for f in fees.values()) else None
        rebate = sum(f['estimated_rebate'] for f in fees.values()) if total is not None else None
        spread_loss = sum(f['spread_loss'] for f in fees.values()) if prices_ok else None
        # Match the existing Lighter entry forecast; this is a reserve, not a paid fee.
        buffer = s.get('liquidity_buffer_usd', .1)*exposures['binance'] if not native and config.get('pair', {}).get('exchange') == 'lighter' and s.get('liquidity_guard_enabled', True) else 0.
        projected = direction_quote(q, group) if q else {}
        entry = projected.get('entry')
        target = (s['take_contraction_usd'] if s['target_mode'] == 'contraction' else
                  entry-s['exit_spread_usd'] if finite(entry) else None)
        gross = target*scale if scale is not None and finite(target) else None
        net = gross-total+rebate-buffer if finite(gross) and total is not None else None
        rows.append(dict(direction=direction, fees=fees, total_fees=total, estimated_rebate=rebate,
            buffer_usd=buffer, spread_loss=spread_loss, target_contraction=target,
            target_gross=gross, target_net=net, min_net_profit=s.get('min_net_profit_usd', 0),
            break_even_contraction=max(0., (total-rebate+buffer)/scale) if total is not None and scale else None,
            required_contraction=max(0., (total-rebate+buffer+s.get('min_net_profit_usd', 0))/scale) if total is not None and scale else None,
            immediate_round_trip_loss=spread_loss+total-rebate if spread_loss is not None and total is not None else None))
    return dict(checked_ms=now, quote_time_ms=q.get('time_ms'), quote_fresh=bool(fresh),
                lots=lots, exposures=exposures, usd_per_spread=scale, balanced=balanced,
                mode=config['execution']['mode'], rows=rows)
