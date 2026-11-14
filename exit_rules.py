"""Explicit, revision-checked edits to existing ordinary groups' exit rules."""
import copy
import math

from trading_config import pair_key, rebate_rates, REBATE_FIELDS


FIELDS=('target_mode','take_contraction_usd','exit_spread_usd','require_net_profit',
        'min_net_profit_usd','group_loss_enabled','group_max_loss_usd','max_hold_minutes')
BOUNDS={'take_contraction_usd':(.000001,100000),'exit_spread_usd':(-100000,100000),
        'min_net_profit_usd':(0,1e9),'group_max_loss_usd':(.01,1e9),'max_hold_minutes':(0,525600)}


def normalize(raw):
    if not isinstance(raw,dict) or set(raw)!=set(FIELDS):
        raise ValueError('请完整填写退出规则；不允许修改手数、退出分组方式或执行保护参数')
    rules={}
    if raw['target_mode'] not in ('contraction','absolute'):raise ValueError('止盈方式无效')
    rules['target_mode']=raw['target_mode']
    for key in ('require_net_profit','group_loss_enabled'):
        if type(raw[key]) is not bool:raise ValueError('退出条件开关无效')
        rules[key]=raw[key]
    for key,(lo,hi) in BOUNDS.items():
        value=raw[key]
        if isinstance(value,bool) or not isinstance(value,(int,float,str)) or not str(value).strip():
            raise ValueError(key+' 必须填写有效数值')
        try:value=float(value)
        except (ValueError,OverflowError):raise ValueError(key+' 必须填写有效数值') from None
        if not math.isfinite(value) or not lo<=value<=hi:raise ValueError(f'{key} 必须在 {lo}～{hi} 范围内')
        rules[key]=value
    return rules


def apply(engine, config, body):
    # The caller holds TradingRuntime.lock. No broker calls or trade dispatch.
    ids=body.get('groups');versions=body.get('expected_revisions')
    if (not isinstance(ids,list) or not 1<=len(ids)<=50 or
            any(not isinstance(x,str) or not x for x in ids) or len(set(ids))!=len(ids)):
        raise ValueError('请选择明确的交易组')
    if not isinstance(versions,dict) or set(versions)!=set(ids):raise ValueError('缺少规则版本，请重新载入')
    active=engine.active();by_id={g['id']:g for g in active};selected=[]
    if engine.state['recovery'] or any(engine.uncertain(g) for g in active):
        raise ValueError('请先完成持仓对账并确认未知成交')
    for gid in ids:
        g=by_id.get(gid)
        if not g or g['status']!='open':raise ValueError('交易组状态已变化，请重新载入；平仓中或异常组不能修改')
        if g.get('imported'):raise ValueError('接管旧仓请在专用管理区调整')
        if g.get('key')!=pair_key(config):raise ValueError('交易组与当前账户或品种不一致')
        if type(versions[gid]) is not int or versions[gid]!=g.get('exit_rules_revision',0):
            raise ValueError('规则已被修改，请重新载入后核对')
        selected.append(g)
    basket=[g for g in selected if g['parameters'].get('exit_mode')=='basket']
    if basket:
        members={g['id'] for g in active if not g.get('imported') and g['parameters'].get('exit_mode')=='basket'}
        if set(ids)!=members:raise ValueError('整篮子必须统一修改全部当前成员；成员已变化请重新载入')
    elif len(selected)!=1 or selected[0]['parameters'].get('exit_mode')!='group':
        raise ValueError('逐组退出请单独选择一组')
    rules=normalize(body.get('rules'))
    rates=None
    if 'rebates' in body:
        if not isinstance(body['rebates'],dict) or set(body['rebates'])!=set(REBATE_FIELDS):
            raise ValueError('请完整填写币安和 MT5 手续费返佣比例')
        rates=rebate_rates(body['rebates'])
    before=[(g,copy.deepcopy(g['parameters']),g.get('exit_rules_revision'),copy.deepcopy(g['costs'])) for g in selected]
    changes=[]
    try:
        for g,old,revision,costs in before:
            g['parameters'].update(rules);g['exit_rules_revision']=(revision or 0)+1
            if rates is not None:g['costs'].update(rates)
            if rates is not None and g.get('pair',{}).get('type')=='mt5_mt5':
                g['costs']['mt5_short_rebate_percent']=rates['binance_rebate_percent']
            old_rates=rebate_rates(costs);new_rates=rebate_rates(g['costs'])
            if g.get('pair',{}).get('type')=='mt5_mt5':
                old_rates['binance_rebate_percent']=costs.get('mt5_short_rebate_percent',0)
                new_rates['binance_rebate_percent']=g['costs'].get('mt5_short_rebate_percent',0)
            changes.append({'group':g['id'],'revision':g['exit_rules_revision'],
                            'before':{key:old.get(key) for key in FIELDS},'after':dict(rules),
                            'rebates_before':old_rates,'rebates_after':new_rates})
        engine.save('exit_rules_updated',{'changes':changes})
    except Exception:
        for g,old,revision,costs in before:
            g['parameters']=old;g['costs']=costs
            if revision is None:g.pop('exit_rules_revision',None)
            else:g['exit_rules_revision']=revision
        raise
    return ids
