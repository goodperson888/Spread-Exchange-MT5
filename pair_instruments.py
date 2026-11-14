"""Pair definitions. Legacy exchange pairs keep their original key and quantity unit."""
import copy
import math
from decimal import Decimal


def native_pair(config):
    return config.get('pair', {}).get('type') == 'mt5_mt5'


def short_symbol(config):
    return config.get('pair', {}).get('short_symbol', '')


def pair_info(config):
    p = config.get('pair', {})
    native = native_pair(config)
    return dict(id=p.get('id', 'legacy'), name=p.get('name') or f"{config['symbol']} × {config['mt5']['symbol']}",
                type='mt5_mt5' if native else 'exchange_mt5',
                short_symbol=short_symbol(config) if native else config['symbol'],
                long_symbol=config['mt5']['symbol'], quantity_unit='lot' if native else 'oz')


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
    if specs['binance']['name'] == specs['mt5']['name']: raise ValueError('两条腿必须选择不同品种')
    return dict(lots=float(lots), qty=float(lots), contract=1., quantity_unit='lot',
                leg_contracts={k:float(v['contract_size_oz']) for k,v in specs.items()},
                specs=copy.deepcopy(specs), pair=pair_info(config))


def native_valuation(engine, group, quote):
    """Equal lots can have different notionals: value each leg with its OWN multiplier."""
    g = group; c = g['costs']; remaining = engine.amounts(g)
    pnl = {'binance':0., 'mt5':0.}; fees = {'binance':0., 'mt5':0.}; flags = []
    rates = {'binance':c.get('mt5_short_commission_per_lot_side',0), 'mt5':c['mt5_commission_per_lot_side']}
    for o in engine.state['orders']:
        if o['group'] != g['id']: continue
        r=o.get('result',{}); lots=r.get('qty',0); leg=o['leg']
        sign=1 if (leg=='binance')==(o['action']=='open') else -1
        pnl[leg] += sign*lots*r.get('price',0)*g['leg_contracts'][leg]
        fees[leg] += r.get('fee',lots*rates[leg])
        if lots>0: flags.append(r.get('fee_verified') is True)
    pnl['binance'] -= remaining['binance']*quote['binance']['ask']*g['leg_contracts']['binance']
    pnl['mt5'] += remaining['mt5']*quote['mt5']['bid']*g['leg_contracts']['mt5']
    exits={leg:remaining[leg]*rates[leg] for leg in rates}
    rebate_rates={'binance':c.get('mt5_short_rebate_percent',0), 'mt5':c.get('mt5_rebate_percent',0)}
    # Cashback applies only to positive commission, never to swap.
    bases={leg:sum(max(0,o.get('result',{}).get('fee',o.get('result',{}).get('qty',0)*rates[leg]))
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
