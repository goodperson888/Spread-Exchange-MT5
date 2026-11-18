"""Credential-free combination presets. Activation is gated by the runtime, not this file."""
import copy
import re
import uuid
from pair_instruments import pair_info,pool_pair


def capture(config):
    info=pair_info(config)
    return {**info, **({'mt5_pool':copy.deepcopy(config['mt5_pool'])} if pool_pair(config) else {}), 'symbol':config['symbol'],
            'strategy':copy.deepcopy(config['strategy']), 'costs':copy.deepcopy(config['costs']),
            'risk_alerts':copy.deepcopy(config.get('risk_alerts',{})),
            'execution':{k:config['execution'][k] for k in ('mode','entry_leg','poll_ms','magic','close_retry_limit')}}


def catalogue(config, saved):
    rows=copy.deepcopy(saved.get('profiles',[]))
    active=capture(config)
    rows=[active if x.get('id')==active['id'] else x for x in rows]
    if not any(x.get('id')==active['id'] for x in rows): rows.insert(0,active)
    return dict(active_id=active['id'],profiles=rows)


def clean_profile(body, config):
    p=capture(config)
    p['id']=body.get('id') or uuid.uuid4().hex[:12]
    if not isinstance(p['id'],str) or not re.fullmatch(r'[a-zA-Z0-9_-]{1,40}',p['id']):
        raise ValueError('组合标识无效')
    p['name']=str(body.get('name','')).strip()
    if not 1<=len(p['name'])<=60: raise ValueError('组合名称需为 1～60 个字符')
    p['type']=body.get('type','exchange_mt5')
    if p['type'] not in ('exchange_mt5','mt5_mt5'): raise ValueError('组合类型无效')
    for key in ('short_symbol','long_symbol'):
        value=str(body.get(key,'')).strip()
        if not re.fullmatch(r'[\w.#+/-]{1,40}',value): raise ValueError('请填写终端中的完整品种名')
        p[key]=value
    is_pool=p['type']=='mt5_mt5' and body.get('mode')=='pool'
    p.pop('mode',None);p.pop('mt5_pool',None)
    if is_pool:
        from mt5_pool_config import clean_pool
        p['mode']='pool'
        inherited=config.get('mt5_pool') if body.get('id')==pair_info(config)['id'] else None
        p['mt5_pool']=clean_pool(body.get('mt5_pool') or inherited or dict(endpoints=[
            dict(id='t1',name='MT5 A',symbol='XAUUSD'),dict(id='t2',name='MT5 B',symbol='XAUUSD')]))
        upper=p['strategy']['entry_spread_usd']
        p['strategy'].update(direction_mode='auto',entry_lower_usd=min(-max(1,abs(upper)),upper-1),grid_enabled=False,exit_mode='group')
        from mt5_pool_config import seed_rules
        p['mt5_pool']=seed_rules(p['mt5_pool'],p['strategy'])
    if p['short_symbol']==p['long_symbol'] and not is_pool: raise ValueError('两条腿不能是同一品种')
    if p['type']=='exchange_mt5' and not re.fullmatch(r'[A-Z0-9]{5,24}',p['short_symbol']):
        raise ValueError('请填写完整的 USDT 黄金合约名，连接时核验实际标的与数量规则')
    p['symbol']=p['short_symbol'];p['quantity_unit']='lot' if p['type']=='mt5_mt5' else 'oz'
    # New profiles copy reviewed parameters; they never inherit a running/armed state.
    if p['id']!=pair_info(config)['id']:
        p['execution']['magic']=9120000+int(uuid.uuid4().hex[:6],16)%10000000
        if p['type']=='mt5_mt5' and not config.get('pair',{}).get('type')=='mt5_mt5':
            p['strategy']['direction_mode']='auto'
            p['strategy']['entry_lower_usd']=-max(1,abs(p['strategy']['entry_spread_usd']))
    return p


def activate(config, profile):
    c=copy.deepcopy(config)
    c['pair']={k:profile[k] for k in ('id','name','type','short_symbol')}
    c.pop('mt5_pool',None)
    if profile.get('mode')=='pool':
        c['pair']['mode']='pool';c['mt5_pool']=copy.deepcopy(profile['mt5_pool'])
    c['symbol']=profile['short_symbol'];c['mt5']['symbol']=profile['long_symbol']
    # Older saved profiles may predate newly added strategy controls.
    c['strategy']=copy.deepcopy(config.get('strategy',{}));c['strategy'].update(copy.deepcopy(profile.get('strategy',{})))
    c['costs']=copy.deepcopy(profile.get('costs',config.get('costs',{})))
    c['risk_alerts']=copy.deepcopy(profile.get('risk_alerts',{}))
    c['execution'].update(copy.deepcopy(profile['execution']))
    if profile['type']=='mt5_mt5':
        c['costs'].update(usdt_usd=1.,usdt_usd_auto=False,binance_fee_auto=False)
        if c['mt5']['adapter']!='paper': c['mt5']['adapter']='native'
    return c
