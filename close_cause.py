"""Platform-confirmed close causes; no comment/price/position-delta guesses."""
import math


MT5_REASONS = {0:'桌面端订单', 1:'手机端订单', 2:'网页端订单', 3:'EA 或脚本订单',
               4:'止损', 5:'止盈', 6:'强平 Stop Out', 7:'展期', 8:'变动保证金', 9:'拆分', 10:'公司行动'}


def finite(value):
    return isinstance(value,(int,float)) and not isinstance(value,bool) and math.isfinite(value)


def evidence(value):
    if not isinstance(value,dict) or type(value.get('schema_version')) is not int or value['schema_version']!=1: return {}
    if value.get('source') not in ('mt5_deal_reason','binance_force_orders'): return {}
    if value.get('kind') not in ('liquidation','other','unknown'): return {}
    clean=dict(schema_version=1,source=value['source'],kind=value['kind'])
    if value['source']=='mt5_deal_reason':
        codes=value.get('mt5_reasons',[])
        clean['mt5_reasons']=sorted({x for x in codes if type(x) is int and 0<=x<=255})[:16] if isinstance(codes,list) else []
        clean['labels']=[MT5_REASONS.get(x,'未知平台原因') for x in clean['mt5_reasons']]
        # A caller cannot turn arbitrary metadata into a liquidation proof.
        clean['kind']='liquidation' if 6 in clean['mt5_reasons'] else 'other' if clean['mt5_reasons'] else 'unknown'
    return clean


def mt5_cause(rows):
    codes=sorted({d['reason'] for d in rows if type(d.get('reason')) is int and 0<=d['reason']<=255})
    return evidence(dict(schema_version=1,source='mt5_deal_reason',kind='unknown',mt5_reasons=codes))


def binance_cause(order, rows, mode):
    """rows came from signed forceOrders filtered to LIQUIDATION, not userTrades."""
    unknown=dict(schema_version=1,source='binance_force_orders',kind='unknown')
    matches=[r for r in rows if isinstance(r,dict) and str(r.get('orderId'))==str(order['result'].get('ticket'))]
    if len(matches)!=1: return unknown
    row=matches[0]
    if (row.get('symbol')!=order['symbol'] or row.get('side')!='BUY'
            or row.get('positionSide')!=('SHORT' if mode=='hedge' else 'BOTH') or row.get('status')!='FILLED'):
        return unknown
    try: qty=float(row['executedQty']); price=float(row['avgPrice'])
    except (KeyError,TypeError,ValueError,OverflowError): return unknown
    if (not finite(qty) or not finite(price) or qty<=0 or price<=0
            or abs(qty-order['result']['qty'])>1e-7 or abs(price-order['result']['price'])>1e-7): return unknown
    return dict(schema_version=1,source='binance_force_orders',kind='liquidation')


def liquidated(order):
    cause=evidence(order.get('result',{}).get('close_cause'))
    return (order.get('source')=='external' and order.get('action')=='close'
            and order.get('result',{}).get('status')=='done' and order['result'].get('qty',0)>0
            and cause.get('kind')=='liquidation'
            and cause['source']==('mt5_deal_reason' if order.get('leg')=='mt5' or order.get('platform')=='mt5' else 'binance_force_orders'))


def needs_history(state):
    active={g['id'] for g in state['groups'] if g['status']!='closed'}
    waiting={g['id'] for g in state['groups'] if state.get('recovery') or g.get('external_attention') or g['status']=='attention'}
    for order in state['orders']:
        if order['group'] not in active or order.get('source')!='external' or order['action']!='close': continue
        if order['group'] in waiting and not evidence(order.get('result',{}).get('close_cause')): return True
        if liquidated(order) and state.get('recovery'):
            # Flat groups use the existing flat-confirmation path.
            remaining={'mt5':0.,'binance':0.}
            for o in state['orders']:
                if o['group']==order['group']:
                    remaining[o['leg']]+=(1 if o['action']=='open' else -1)*o.get('result',{}).get('qty',0)
            if max(remaining.values())>1e-8: return True
    return False
