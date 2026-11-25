"""Pure, bounded depth decisions. No network, disk, account reads or writes."""
import math
from exchange_fee_reference import safe as safe_fee_reference


DEFAULTS = dict(liquidity_guard_enabled=True, liquidity_max_usage_percent=80,
                liquidity_buffer_usd=0.1)
LABELS = dict(LIQUIDITY_UNAVAILABLE='等待新鲜、连续的 Lighter 盘口',
              LIQUIDITY_DEPTH='保护价内深度不足或超过盘口占用上限',
              LIQUIDITY_SPREAD='吃单均价计算后的入场价差不足',
              LIQUIDITY_PROFIT='扣除预计开平成本及缓冲后收益空间不足',
              LIQUIDITY_READY='盘口深度及成交均价检查通过')


def finite(value):
    return isinstance(value,(int,float)) and not isinstance(value,bool) and math.isfinite(value)


def safe_evidence(value):
    """A fixed numerical allowlist, also used for diagnostic exports."""
    if not isinstance(value,dict): return {}
    fields=('checked_ms book_received_ms book_age_ms book_sequence_offset requested available '
            'max_usage_percent usage_percent levels vwap worst limit top_price impact_usd '
            'exit_vwap exit_available exit_levels executable_spread threshold estimated_round_trip_fees '
            'estimated_rebate expected_gross expected_net buffer_usd required_net aggregate_qty '
            'estimated_net_after_depth exchange_taker_percent mt5_open_fee_per_lot mt5_close_fee_per_lot '
            'estimated_exchange_fees estimated_mt5_fees valuation_fx').split()
    result={k:value[k] for k in fields if finite(value.get(k))}
    if isinstance(value.get('fee_basis'),dict): result['fee_basis']=safe_fee_reference(value['fee_basis'])
    if value.get('code') in LABELS: result['code']=value['code']
    if type(value.get('ready')) is bool: result['ready']=value['ready']
    if value.get('action') in ('open','close'): result['action']=value['action']
    return result


def sweep(book, side, qty, limit, usage):
    rows=book.get('bids' if side=='sell' else 'asks',())
    levels={}
    for price,size in rows:
        if not finite(price) or not finite(size) or price<=0 or size<0:
            raise ValueError('invalid depth')
        if (price>=limit if side=='sell' else price<=limit):
            levels[price]=levels.get(price,0)+size
    rows=sorted(levels.items(),reverse=side=='sell')
    available=sum(size for _,size in rows)
    left=qty;notional=0.;count=0;worst=None
    for price,size in rows:
        amount=min(left,size)
        if amount<=0: continue
        left-=amount;notional+=amount*price;count+=1;worst=price
        if left<=1e-10: break
    ready=left<=1e-10 and qty<=available*usage/100+1e-10
    return dict(ready=ready,requested=qty,available=available,levels=count,
                usage_percent=qty/available*100 if available else 0,
                **(dict(vwap=notional/qty,worst=worst) if left<=1e-10 else {}))


def assess(book,q,qty,strategy,action,now,*,threshold=None,costs=None,contract=100,
           mt5_open_fee=0,mt5_close_fee=0):
    out=dict(action=action,ready=False,code='LIQUIDITY_UNAVAILABLE',checked_ms=now,requested=qty)
    try:
        received=book['received_ms'];age=now-received
        if not finite(received) or not 0<=age<=strategy['max_quote_age_ms']: return out
        # A fresh exchange snapshot must still align with the MT5 source quote.
        mt=q['mt5'];mt_at=mt.get('observed_ms',mt.get('time_ms',0))
        if not 0<=now-mt_at<=strategy['max_quote_age_ms'] or abs(received-mt_at)>strategy['max_clock_skew_ms']: return out
        fx=q.get('usdt_usd',(costs or {}).get('usdt_usd',1))
        usage=strategy.get('liquidity_max_usage_percent',80)
        if not finite(qty) or qty<=0 or not finite(fx) or fx<=0: return out
        bid=max(p for p,n in book['bids'] if n>0);ask=min(p for p,n in book['asks'] if n>0)
        if bid>ask: return out
        out.update(book_received_ms=received,book_age_ms=age,max_usage_percent=usage)
        if finite(book.get('offset')):out['book_sequence_offset']=book['offset']
        slip=strategy['max_slippage_usd']/fx
        sell=action=='open';top=bid if sell else ask;limit=top-slip if sell else top+slip
        fill=sweep(book,'sell' if sell else 'buy',qty,limit,usage)
        out.update(fill,top_price=top,limit=limit,code='LIQUIDITY_DEPTH')
        out['ready']=False
        if not fill['ready']: return out
        # The actual IOC gets the last consumed price, never the whole budget.
        out['limit']=fill['worst']
        buffer=strategy.get('liquidity_buffer_usd',.1)*qty
        out.update(buffer_usd=buffer,impact_usd=max(0,(top-fill['vwap'] if sell else fill['vwap']-top)*fx*qty))
        if not sell:
            out.update(ready=True,code='LIQUIDITY_READY');return out
        out.update(executable_spread=fill['vwap']*fx-mt['ask'],threshold=threshold)
        if threshold is None or out['executable_spread']+1e-10<threshold:
            out['code']='LIQUIDITY_SPREAD';return out
        exit_fill=sweep(book,'buy',qty,ask+slip,usage)
        out.update(exit_available=exit_fill['available'],exit_levels=exit_fill['levels'])
        if not exit_fill['ready']: return out
        out['exit_vwap']=exit_fill['vwap']
        c=costs or {};lots=qty/contract
        out.update(exchange_taker_percent=c.get('binance_taker_percent',0),
                   mt5_open_fee_per_lot=mt5_open_fee, mt5_close_fee_per_lot=mt5_close_fee, valuation_fx=fx)
        exchange_fee=(fill['vwap']+exit_fill['vwap'])*qty*fx*c.get('binance_taker_percent',0)/100
        mt_fee=lots*(mt5_open_fee+mt5_close_fee)
        rebate=exchange_fee*c.get('binance_rebate_percent',0)/100+mt_fee*c.get('mt5_rebate_percent',0)/100
        target=(out['executable_spread']-strategy['exit_spread_usd'] if strategy['target_mode']=='absolute'
                else strategy['take_contraction_usd'])
        gross=target*qty;net=gross-exchange_fee-mt_fee+rebate-buffer
        out.update(expected_gross=gross,estimated_round_trip_fees=exchange_fee+mt_fee,
                   estimated_exchange_fees=exchange_fee,estimated_mt5_fees=mt_fee,
                   estimated_rebate=rebate,expected_net=net,required_net=strategy.get('min_net_profit_usd',0))
        if net+1e-10<out['required_net']:
            out['code']='LIQUIDITY_PROFIT';return out
        out.update(ready=True,code='LIQUIDITY_READY');return out
    except (KeyError,TypeError,ValueError,OverflowError,ZeroDivisionError):
        out.update(ready=False,code='LIQUIDITY_UNAVAILABLE');return out
