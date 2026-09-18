"""Read-only adoption planning. No broker writes occur in this module."""
import copy
import math
import uuid
from trading_config import pair_key, plan


def number(data, key, low, high):
    raw = data.get(key)
    if raw is None or raw == '' or isinstance(raw, bool):
        raise ValueError(key + ' 必须明确填写，不能把未知费用当作零')
    try:
        value = float(raw)
    except (TypeError, ValueError):
        raise ValueError(key + ' 必须为数字') from None
    if not math.isfinite(value) or not low <= value <= high:
        raise ValueError(key + ' 超出允许范围')
    return value


def fingerprint(positions, mt5, pending):
    # Ignore mark prices, floating profit and accrued swap: these change normally.
    return {
        'binance': sorted((x.get('positionSide'), str(x.get('positionAmt')),
                           str(x.get('entryPrice'))) for x in positions if float(x.get('positionAmt', 0))),
        'mt5': sorted((str(x['ticket']), x['lots'], x['side'], x['magic'], x.get('comment'),
                       x.get('identifier'), x.get('time_ms'), x.get('price_open')) for x in mt5['positions']),
        'account': {k: mt5.get('account', {}).get(k) for k in ('account', 'server', 'currency', 'margin_mode')},
        'orders': pending, 'mt5_orders': mt5.get('orders', []),
        'spec': mt5['spec'],
    }


def preview(config, mt5, positions, pending, tickets, inputs, spec, at):
    if pending or mt5.get('orders'):
        raise ValueError('当前品种存在挂单，请先在平台处理挂单后重新预览；程序不会自动撤单')
    if mt5.get('orders') is None:
        raise ValueError('MT5 未返回挂单检查结果，请更新并重新连接')
    if not mt5.get('allowed') or mt5['account'].get('currency') != 'USD' or mt5['account'].get('margin_mode') != 2:
        raise ValueError('接管需要允许自动交易的 USD MT5 对冲账户')
    if not tickets or len(tickets) != len(set(tickets)):
        raise ValueError('请选择不重复的 MT5 持仓票据')
    selected = [p for p in mt5['positions'] if str(p['ticket']) in tickets]
    if len(selected) != len(tickets):
        raise ValueError('所选 MT5 持仓已变化，请重新读取')
    if any(p['magic']==config['execution']['magic'] and str(p['ticket']) not in tickets for p in mt5['positions']):
        raise ValueError('还有带本策略标识的 MT5 仓位未选择，请先核对全部仓位')
    if any(p['side'] != 0 for p in selected):
        raise ValueError('当前策略只接管 MT5 多头＋币安空头')
    b = [x for x in positions if float(x.get('positionAmt', 0))]
    if len(b) != 1 or float(b[0]['positionAmt']) >= 0 or b[0].get('positionSide') not in ('BOTH', 'SHORT'):
        raise ValueError('需要当前品种只有币安空头，不支持同时存在多头或反向接管')
    b = b[0]
    qty = abs(float(b['positionAmt'])); price = float(b['entryPrice'])
    if not math.isfinite(price) or price <= 0:
        raise ValueError('币安开仓均价无效')
    contract = float(mt5['spec']['contract_size_oz'])
    total = sum(float(p['lots']) * contract for p in selected)
    if not math.isfinite(total) or abs(total - qty) > 1e-7:
        raise ValueError(f'尚未配平：所选 MT5 {total:g} 盎司，币安全部空头 {qty:g} XAU；不会自动补单')
    for p in selected:
        if not p.get('time_ms') or not p.get('identifier') or not math.isfinite(float(p.get('price_open', 0))) or p['price_open'] <= 0:
            raise ValueError('MT5 持仓缺少有效开仓价、时间或身份标识')
        check = copy.deepcopy(config); check['strategy']['mt5_lots'] = p['lots']
        plan(check, mt5['spec'], spec, price)  # Each ticket must be executable on both legs.
    fx = number(inputs, 'entry_fx', .5, 1.5)
    fees = number(inputs, 'history_fees_usd', 0, 1e9)
    funding = number(inputs, 'history_funding_usd', -1e9, 1e9)
    contraction = number(inputs, 'take_contraction_usd', .000001, 1e5)
    profit = number(inputs, 'min_net_profit_usd', 0, 1e9)
    if inputs.get('costs_confirmed') is not True:
        raise ValueError('请核对历史汇率和费用，并确认接受费用估算；未知项目不能默认为零')
    rules = copy.deepcopy(config['strategy'])
    # Adoption is an isolated basket. It never inherits new-entry grid or loss rules.
    rules.update(exit_mode='basket', target_mode='contraction', take_contraction_usd=contraction,
                 require_net_profit=True, min_net_profit_usd=profit, grid_enabled=False,
                 grid_max_adds=0, group_loss_enabled=False, total_loss_enabled=False, max_hold_minutes=0)
    avg = sum(p['lots'] * contract * p['price_open'] for p in selected) / qty
    return dict(id=uuid.uuid4().hex[:12], created_ms=at, key=pair_key(config),
                positions=copy.deepcopy(selected), qty=qty, lots=total/contract, contract=contract,
                binance_entry=price, mt5_entry=avg, entry=price*fx-avg, entry_fx=fx,
                history_fees_usd=fees, history_funding_usd=funding, parameters=rules,
                fingerprint=fingerprint(positions, mt5, pending))


def register(engine, config, proposal, at):
    """Persist one basket and its opening basis in one store transaction; never submit."""
    p = proposal
    g = dict(id=p['id'], status='open', opened_ms=at, original_opened_ms=min(x['time_ms'] for x in p['positions']),
             qty=p['qty'], lots=p['lots'], contract=p['contract'], symbol=config['symbol'],
             mt5_symbol=config['mt5']['symbol'], mode='live', parameters=p['parameters'],
             costs=copy.deepcopy(config['costs']), attempts=0, retry_limit=config['execution']['close_retry_limit'],
             entry=p['entry'], open_binance=p['binance_entry'], reason='', key=p['key'], grid_index=0,
             imported=True, management_enabled=False, binance_key_fingerprint=p['binance_key_fingerprint'],
             history_fees_usd=p['history_fees_usd'],
             history_funding_usd=p['history_funding_usd'], costs_verified=False,
             live_mt5_swap_usd=sum(x.get('swap', 0) for x in p['positions']))
    orders = []
    for pos in p['positions']:
        orders.append(dict(id='adopt'+uuid.uuid4().hex[:20], group=g['id'], leg='mt5', action='open',
                           symbol=g['mt5_symbol'], requested=pos['lots']*p['contract'], created_ms=pos['time_ms'],
                           imported=True, original_position=copy.deepcopy(pos), fx=1,
                           result=dict(status='done', qty=pos['lots']*p['contract'], price=pos['price_open'],
                                       position=str(pos['ticket']), ticket=str(pos['ticket']), fee=0)))
    orders.append(dict(id='adopt'+uuid.uuid4().hex[:20], group=g['id'], leg='binance', action='open',
                       symbol=g['symbol'], requested=p['qty'], created_ms=at, imported=True, fx=p['entry_fx'],
                       result=dict(status='done', qty=p['qty'], price=p['binance_entry'], fee=0)))
    before = copy.deepcopy(engine.state)
    try:
        engine.state['groups'].append(g); engine.state['orders'].extend(orders)
        engine.state.update(enabled=False, recovery=True)
        engine.save('positions_adopted', {'group': g['id'], 'tickets': [str(x['ticket']) for x in p['positions']]})
    except Exception:
        engine.state = before
        raise
    return g
