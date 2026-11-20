"""Pair definitions. Legacy exchange pairs keep their original key and quantity unit."""
import copy
import math
from decimal import Decimal


def native_pair(config):
    return config.get('pair', {}).get('type') == 'mt5_mt5'


def pool_pair(config):
    return native_pair(config) and config.get('pair', {}).get('mode') == 'pool'


def short_symbol(config):
    return config.get('pair', {}).get('short_symbol', '')


def reverse_pair(group):
    return native_pair(group) and group.get('direction')=='reverse'


def native_thresholds(config, quote):
    """Return effective forward/reverse entry thresholds for a native pair."""
    s=config.get('strategy', {})
    upper=s.get('entry_spread_usd', 0)
    lower=s.get('entry_lower_usd', -5)
    if not native_pair(config) or not s.get('dynamic_reference_enabled') or not quote:
        return upper, lower
    if not quote.get('reference_ready'):
        return upper, lower
    deviation=s.get('entry_deviation_usd', 0)
    ref_entry=quote.get('reference_entry');ref_exit=quote.get('reference_exit')
    if isinstance(ref_entry,(int,float)) and math.isfinite(ref_entry): upper=ref_entry+deviation
    if isinstance(ref_exit,(int,float)) and math.isfinite(ref_exit): lower=ref_exit-deviation
    return upper, lower


def position_side(group,leg):
    return (1 if leg=='binance' else 0) ^ int(reverse_pair(group))


def order_sells(group,leg,action):
    return (position_side(group,leg)==1)==(action=='open')


def direction_quote(quote,group):
    """Project spread scalars only; A/B books and account identity never swap."""
    if not quote: return quote
    desired='reverse' if reverse_pair(group) else 'forward'
    current=quote.get('_spread_direction','forward')
    if desired==current: return quote
    negate=lambda x:-x if isinstance(x,(int,float)) and math.isfinite(x) else None
    return {**quote,'entry':negate(quote.get('exit')),'exit':negate(quote.get('entry')),'_spread_direction':desired}


def entry_ambiguous(config,quote,active=(),*,thresholds=None):
    """Flat auto pairs must not arbitrarily choose between two reached sides."""
    if not native_pair(config) or config['strategy'].get('direction_mode','forward')!='auto' or active or not quote:
        return False
    upper,lower=thresholds if thresholds is not None else native_thresholds(config,quote)
    from entry_confirmation import reached
    return reached(config['strategy'],'forward',quote['entry'],upper) and reached(config['strategy'],'reverse',quote['exit'],lower)


def entry_direction(config,quote,active=(),*,thresholds=None):
    """Choose only an allowed, reached direction; open groups lock the pair."""
    if not native_pair(config): return 'forward'
    if entry_ambiguous(config,quote,active,thresholds=thresholds): return None
    s=config['strategy'];mode=s.get('direction_mode','forward')
    upper, lower=thresholds if thresholds is not None else native_thresholds(config,quote)
    directions={'forward','reverse'} if mode=='auto' else {mode}
    held={g.get('direction','forward') for g in active}
    if held:
        if len(held)!=1: return None
        directions &= held
    from entry_confirmation import reached
    if 'forward' in directions and reached(s,'forward',quote['entry'],upper): return 'forward'
    if 'reverse' in directions and reached(s,'reverse',quote['exit'],lower): return 'reverse'
    return None


def pair_info(config):
    p = config.get('pair', {})
    native = native_pair(config)
    result = dict(id=p.get('id', 'legacy'), name=p.get('name') or f"{config['symbol']} × {config['mt5']['symbol']}",
                type='mt5_mt5' if native else 'exchange_mt5',
                short_symbol=short_symbol(config) if native else config['symbol'],
                long_symbol=config['mt5']['symbol'], quantity_unit='lot' if native else 'oz')
    if p.get('mode'): result['mode']=p['mode']
    if p.get('endpoints'):
        from mt5_pool_config import endpoint_ref
        result['endpoints']={leg:endpoint_ref(e) for leg,e in p['endpoints'].items()}
    return result


def equal_lot_plan(config, specs):
    lots = Decimal(str(config['strategy']['mt5_lots']))
    if not lots.is_finite() or lots <= 0: raise ValueError('每腿手数必须大于 0')
    for leg in ('binance', 'mt5'):
        s = specs[leg]
        minimum, maximum, step = (Decimal(str(s[k])) for k in ('volume_min', 'volume_max', 'volume_step'))
        contract = float(s['contract_size_oz'])
        if (not all(x.is_finite() and x > 0 for x in (minimum, maximum, step)) or
                not minimum <= lots <= maximum or lots % step):
            raise ValueError(f"{s['name']} 不支持 {lots} 手，请使用同时符合两个品种步长的手数")
        if not math.isfinite(contract) or contract <= 0 or s.get('currency_profit') != 'USD':
            raise ValueError(f"{s['name']} 合约规格无效或盈亏币种不是 USD")
        if int(s.get('trade_mode', -1)) != 4:
            raise ValueError(f"{s['name']} 当前不是完整可交易模式")
    if specs['binance']['name'] == specs['mt5']['name'] and not config.get('pair',{}).get('endpoints'):
        raise ValueError('两条腿必须选择不同品种')
    if config.get('pair',{}).get('endpoints') and specs['binance']['contract_size_oz']!=specs['mt5']['contract_size_oz']:
        raise ValueError('多终端同品种相同手数需要相同合约大小，请核对两个黄金合约规格')
    return dict(lots=float(lots), qty=float(lots), contract=1., quantity_unit='lot',
                leg_contracts={k:float(v['contract_size_oz']) for k,v in specs.items()},
                specs=copy.deepcopy(specs), pair=pair_info(config))


def native_valuation(engine, group, quote):
    """Equal lots can have different notionals: value each leg with its OWN multiplier."""
    g = group; c = g['costs']; remaining = engine.amounts(g)
    pnl = {'binance':0., 'mt5':0.}; fees = {'binance':0., 'mt5':0.}; flags = []
    from commission_reference import fee_rate
    estimates = {(leg,action):fee_rate(g,leg,action) for leg in ('binance','mt5') for action in ('open','close')}
    rates = {leg:estimates[leg,'close'] for leg in ('binance','mt5')}
    for o in engine.state['orders']:
        if o['group'] != g['id']: continue
        r=o.get('result',{}); lots=r.get('qty',0); leg=o['leg']
        sign=1 if order_sells(g,leg,o['action']) else -1
        pnl[leg] += sign*lots*r.get('price',0)*g['leg_contracts'][leg]
        fees[leg] += r.get('fee',lots*estimates[leg,o['action']])
        if lots>0: flags.append(r.get('fee_verified') is True)
    for leg in pnl:
        if not remaining[leg]: continue
        short=position_side(g,leg)==1
        pnl[leg] += (-1 if short else 1)*remaining[leg]*quote[leg]['ask' if short else 'bid']*g['leg_contracts'][leg]
    exits={leg:remaining[leg]*rates[leg] for leg in rates}
    rebate_rates={'binance':c.get('mt5_short_rebate_percent',0), 'mt5':c.get('mt5_rebate_percent',0)}
    # Cashback applies only to positive commission, never to swap.
    bases={leg:sum(max(0,o.get('result',{}).get('fee',o.get('result',{}).get('qty',0)*estimates[leg,o['action']]))
                  for o in engine.state['orders'] if o['group']==g['id'] and o['leg']==leg) for leg in rates}
    rebates={leg:(bases[leg]+max(0,exits[leg]))*rebate_rates[leg]/100 for leg in rates}
    swap=g.get('native_swap_usd',0)
    if g['mode']=='paper':
        from trading_engine import stamp
        days=max(0,(g.get('closed_ms',stamp())-g['opened_ms'])/86400000)
        swap=-g['lots']*(c.get('mt5_short_swap_per_lot_day',0)+c['mt5_swap_per_lot_day'])*days
    gross=sum(pnl.values()); before=gross-sum(fees.values())-sum(exits.values())+swap
    verified=bool(g.get('costs_verified')); flat=max(remaining.values())<1e-8
    rounded=lambda x:round(x,8)
    return dict(pnl_schema_version=2, quantity_unit='lot', valuation_fx=1.,
                binance_pnl_usdt=rounded(pnl['binance']),mt5_pnl_usd=rounded(pnl['mt5']),
                gross=rounded(gross),fees=rounded(sum(fees.values())),estimated_exit_fee=rounded(sum(exits.values())),
                estimated_exit_fee_binance=rounded(exits['binance']),estimated_exit_fee_mt5=rounded(exits['mt5']),
                mt5_swap=rounded(swap),binance_funding=0.,carry=rounded(swap),net_before_rebate=rounded(before),
                estimated_rebate_binance=rounded(rebates['binance']),estimated_rebate_mt5=rounded(rebates['mt5']),
                estimated_rebate=rounded(sum(rebates.values())),net=rounded(before+sum(rebates.values())),
                remaining=remaining,costs_verified=verified,
                fees_verified=verified or (g['mode']=='live' and bool(flags) and all(flags)),
                estimated=not flat or sum(rebates.values())>0 or (g['mode']=='live' and not verified))
