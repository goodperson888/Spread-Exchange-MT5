"""Read-only commission samples; never query a platform from the order path."""
import copy
import math
import statistics
import time

from pair_instruments import native_pair, short_symbol, position_side
from trading_config import pair_key
from reference_diagnostics import PROCESS_REFERENCE_DIAGNOSTICS

MAX_AGE_MS = 86400000
MIN_SAMPLES = 3


def finite(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)


def summarize(rows, symbol, lots, now):
    """Keep entry/exit and buy/sell separate; missing charges never mean zero."""
    orders = {}
    for row in rows:
        if row.get('symbol') != symbol or row.get('type') not in (0, 1) or row.get('entry') not in (0, 1, 3):
            continue
        if row.get('order') is None: continue
        key = (str(row['order']), 'open' if row['entry'] == 0 else 'close', row['type'])
        item = orders.setdefault(key, dict(volume=0., charge=0., at=0, valid=True))
        values = [row.get(k) for k in ('volume', 'commission', 'fee', 'time_msc')]
        if not all(finite(x) for x in values) or values[0] <= 0 or not now-30*86400000 <= values[3] <= now+14*3600000:
            item['valid'] = False
            continue
        item['volume'] += values[0]
        item['charge'] -= values[1]+values[2]
        item['at'] = max(item['at'], values[3])
        # Credits/rebates mixed into a deal are not a prospective fee schedule.
        if values[1] > 0 or values[2] > 0: item['valid'] = False
    profiles = {}
    for action in ('open', 'close'):
        for side in (0, 1):
            samples = sorted((r for (_, a, s), r in orders.items() if a == action and s == side
                and r['valid'] and abs(r['volume']-lots) <= max(1e-8, lots*.01)), key=lambda r:r['at'], reverse=True)[:20]
            rates = [r['charge']/r['volume'] for r in samples]
            item = dict(samples=len(rates), state='insufficient', lots=lots)
            if samples: item['latest_deal_ms'] = samples[0]['at']
            if rates:
                middle = statistics.median(rates)
                if min(rates) < 0 or max(rates) > 10000 or max(rates)-min(rates) > max(.05, abs(middle)*.25):
                    item['state'] = 'variable'
                elif len(rates) >= MIN_SAMPLES:
                    item.update(state='ready', rate=round(max(rates), 8))
            profiles[action+'_'+str(side)] = item
    # All-zero trade fields cannot prove a commission-free account: some
    # brokers post charges separately. Zero exits need evidence of upfront fees.
    for key,item in profiles.items():
        if item.get('rate') != 0 or item['state'] != 'ready': continue
        action,side=key.split('_')
        counterpart=profiles[('close' if action=='open' else 'open')+'_'+str(1-int(side))]
        if counterpart.get('state')!='ready' or counterpart.get('rate',0)<=0:
            item.pop('rate',None);item['state']='zero_unconfirmed'
    return dict(symbol=symbol, checked_ms=now, lookback_days=30, minimum_samples=MIN_SAMPLES,
                source='mt5_deal_commission_fee', profiles=profiles)


def native_commission_reference(mt, symbol, lots):
    from datetime import datetime, timezone
    now = int(time.time()*1000)
    if not finite(lots) or lots <= 0: raise ValueError('佣金参考手数无效')
    account = mt.account_info()
    if not account or account.currency != 'USD': raise ValueError('佣金参考暂只支持 USD 账户')
    # Account history is intentionally independent of strategy magic/comments.
    rows = mt.history_deals_get(datetime.fromtimestamp((now-30*86400000)/1000, timezone.utc),
                               datetime.fromtimestamp((now+14*3600000)/1000, timezone.utc), group=symbol)
    if rows is None: raise ValueError('MT5 佣金历史读取失败')
    if len(rows) > 20000: raise ValueError('佣金历史超过读取上限，保留手动估算')
    return summarize([dict(r._asdict()) for r in rows], symbol, lots, now)


def refresh(monitor, terminal):
    """Capture/publish without waiting for the execution lock or writing its journal."""
    r = monitor.runtime
    if not r.lock.acquire(blocking=False): return False
    try:
        if monitor.stopped.is_set() or monitor is not r.maintenance or not r.connected: return False
        c = copy.deepcopy(r.config)
        if c['mt5']['adapter'] != 'native': return False
        names = {'mt5':c['mt5']['symbol']}
        if native_pair(c): names['binance'] = short_symbol(c)
        enabled = {leg:name for leg,name in names.items() if c['costs'].get(
            'mt5_short_fee_auto' if leg == 'binance' else 'mt5_fee_auto', False)}
        if not enabled: return True
    finally: r.lock.release()
    legs = {}
    for leg, name in enabled.items():
        try:
            reader=monitor._terminal('commission',leg) if c.get('pair',{}).get('endpoints') else terminal()
            legs[leg] = reader.call('commission_reference', symbol=name, lots=c['strategy']['mt5_lots'])
        except Exception:
            legs[leg] = dict(symbol=name, checked_ms=int(time.time()*1000), state='unavailable',
                             message='佣金历史读取失败，保留手填估算')
            lane_lock=getattr(monitor,'session_lock',None) or getattr(monitor,'lock',None)
            if lane_lock is not None:
                with lane_lock:
                    slot=('commission',leg) if c.get('pair',{}).get('endpoints') else 'commission'
                    reader=monitor.terminals.get(slot)
                    monitor.terminals[slot]=None
                if reader:
                    try: reader.close()
                    except Exception: pass
    if not r.lock.acquire(blocking=False): return False
    try:
        if monitor.stopped.is_set() or monitor is not r.maintenance or not r.connected or r.config != c: return False
        r.engine.commission_reference = dict(key=pair_key(c), legs=copy.deepcopy(legs))
        r.market_meta['mt5_commission_reference'] = copy.deepcopy(legs)
        PROCESS_REFERENCE_DIAGNOSTICS.commissions=safe_references(legs)
        return True
    finally: r.lock.release()


def attach(engine, config, group):
    reference = getattr(engine,'commission_references',{}).get(pair_key(config),getattr(engine, 'commission_reference', {}))
    if reference.get('key') == pair_key(config):
        group['commission_reference'] = copy.deepcopy(reference.get('legs', {}))
        legs=('binance','mt5') if native_pair(config) else ('mt5',)
        group['commission_estimates']={leg+'_'+action:fee_rate(group,leg,action)
                                       for leg in legs for action in ('open','close')}


def fee_rate(group, leg, action):
    costs = group['costs']
    a = leg == 'binance'
    fallback = costs.get('mt5_short_commission_per_lot_side' if a else 'mt5_commission_per_lot_side', 0)
    locked=group.get('commission_estimates',{}).get(leg+'_'+action)
    if finite(locked) and 0<=locked<=10000: return locked
    if not costs.get('mt5_short_fee_auto' if a else 'mt5_fee_auto', False): return fallback
    ref = group.get('commission_reference', {}).get(leg, {})
    checked = ref.get('checked_ms')
    expected = group['symbol'] if a else group['mt5_symbol']
    basis=group['opened_ms'] if 'opened_ms' in group else int(time.time()*1000)
    if ref.get('symbol') != expected or not finite(checked) or not 0 <= basis-checked <= MAX_AGE_MS:
        return fallback
    side = position_side(group, leg) if native_pair({'pair':group.get('pair', {})}) else 0
    if action == 'close': side = 1-side
    row = ref.get('profiles', {}).get(action+'_'+str(side), {})
    rate = row.get('rate')
    if (row.get('state') == 'ready' and finite(rate) and 0 <= rate <= 10000
            and finite(row.get('lots')) and abs(row['lots']-group['lots'])<=max(1e-8,group['lots']*.01)
            and row.get('samples', 0) >= MIN_SAMPLES):
        return rate
    return fallback


def safe_references(refs):
    """Allowlist for on-demand exports, excluding account identity and raw deals."""
    result={}
    if not isinstance(refs,dict): return result
    for leg in ('binance','mt5'):
        ref=refs.get(leg)
        if not isinstance(ref,dict): continue
        item={k:ref[k] for k in ('checked_ms','lookback_days','minimum_samples') if finite(ref.get(k))}
        item['profiles']={}
        profiles=ref.get('profiles',{})
        if not isinstance(profiles,dict): profiles={}
        for key in ('open_0','open_1','close_0','close_1'):
            row=profiles.get(key)
            if not isinstance(row,dict): continue
            clean={k:row[k] for k in ('samples','lots','rate','latest_deal_ms') if finite(row.get(k))}
            if row.get('state') in ('ready','variable','insufficient','zero_unconfirmed'): clean['state']=row['state']
            item['profiles'][key]=clean
        if ref.get('state')=='unavailable': item['state']='unavailable'
        result[leg]=item
    return result
