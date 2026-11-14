"""Credential-free combination presets. Activation is gated by the runtime, not this file."""
import copy
import re
import uuid
from pair_instruments import pair_info


def capture(config):
    info=pair_info(config)
    return {**info, 'symbol':config['symbol'],
            'strategy':copy.deepcopy(config['strategy']), 'costs':copy.deepcopy(config['costs']),
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
    if p['short_symbol']==p['long_symbol']: raise ValueError('两条腿不能是同一品种')
    if p['type']=='exchange_mt5' and p['short_symbol'] not in ('XAUUSDT','PAXGUSDT'):
        raise ValueError('请选择 XAUUSDT 或 PAXGUSDT 永续')
    p['symbol']=p['short_symbol'];p['quantity_unit']='lot' if p['type']=='mt5_mt5' else 'oz'
    # New profiles copy reviewed parameters; they never inherit a running/armed state.
    if p['id']!=pair_info(config)['id']:
        p['execution']['magic']=9120000+int(uuid.uuid4().hex[:6],16)%10000000
    return p


def activate(config, profile):
    c=copy.deepcopy(config)
    c['pair']={k:profile[k] for k in ('id','name','type','short_symbol')}
    c['symbol']=profile['short_symbol'];c['mt5']['symbol']=profile['long_symbol']
    for key in ('strategy','costs'): c[key]=copy.deepcopy(profile[key])
    c['execution'].update(copy.deepcopy(profile['execution']))
    if profile['type']=='mt5_mt5':
        c['costs'].update(usdt_usd=1.,usdt_usd_auto=False,binance_fee_auto=False)
        if c['mt5']['adapter']!='paper': c['mt5']['adapter']='native'
    return c
