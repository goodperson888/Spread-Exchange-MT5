"""Verify already-filled opening fees through the independent read-only observer."""
import math


def number(value):
    if isinstance(value,bool): raise ValueError('费用字段无效')
    value=float(value)
    if not math.isfinite(value): raise ValueError('费用字段无效')
    return value


def verify(order, client, terminal):
    saved=order['result'];ticket=str(saved.get('ticket') or '')
    if order['action']!='open' or saved.get('status')!='done' or not ticket or number(saved['qty'])<=0:
        raise ValueError('原开仓成交尚未确认')
    # Binance.query proves the original random client ID on the connected
    # account. Native query proves exact symbol/magic/order/terminal volume.
    result=client.query(order) if order['leg']=='binance' else terminal.call('query',order=order)
    if (result.get('status')!='done' or str(result.get('ticket'))!=ticket
            or abs(number(result['qty'])-number(saved['qty']))>1e-7
            or abs(number(result['price'])-number(saved['price']))>1e-7):
        raise ValueError('原开仓成交归属或数量待核验')
    if order['leg']=='mt5': return number(result['fee'])
    fills=client.trades(order['symbol'],ticket)
    if not isinstance(fills,list) or not fills or len(fills)>=1000:
        raise ValueError('成交手续费记录尚未完整返回')
    ids=set();qty=0.
    for row in fills:
        if (str(row.get('orderId'))!=ticket or row.get('symbol')!=order['symbol']
                or row.get('side')!='SELL' or row.get('positionSide') not in ('BOTH','SHORT')
                or row.get('id') is None or not row.get('commissionAsset')):
            raise ValueError('成交手续费归属尚未确认')
        key=str(row['id'])
        if key in ids: raise ValueError('成交手续费记录重复')
        ids.add(key)
        amount=number(row['qty']);price=number(row['price']);number(row['commission'])
        if amount<=0 or price<=0: raise ValueError('成交手续费记录无效')
        qty+=amount
    if abs(qty-number(saved['qty']))>1e-7: raise ValueError('成交手续费数量尚未完整返回')
    return number(client.commissions_usdt(fills))
