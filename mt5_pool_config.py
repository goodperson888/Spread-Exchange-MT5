"""Local terminal identities and fixed, independently configured pool edges."""
import copy
import hashlib
import json
import math
import ntpath
import re
from itertools import combinations

RULE_FIELDS = ('entry_spread_usd', 'entry_lower_usd', 'direction_mode', 'take_contraction_usd',
               'target_mode', 'exit_spread_usd', 'require_net_profit', 'min_net_profit_usd',
               'group_loss_enabled', 'group_max_loss_usd', 'max_hold_minutes')


def fingerprint(e):
    fields=[ntpath.normcase(ntpath.normpath(str(e.get('terminal_path','')).strip())),
            str(e.get('account','')).strip(),str(e.get('server','')).strip(),str(e.get('symbol','')).strip()]
    return hashlib.sha256(json.dumps(fields,ensure_ascii=False).encode()).hexdigest()[:24]


def endpoint_ref(e):
    # Immutable ownership evidence, without account number, server or filesystem path.
    return dict(id=e['id'],name=e['name'],symbol=e['symbol'],identity=fingerprint(e))


def edge_id(a,b):
    return '~'.join(sorted((a,b)))


def clean_pool(value, complete=False):
    if not isinstance(value,dict): raise ValueError('请先配置 MT5 终端池')
    rows=value.get('endpoints',[])
    if not isinstance(rows,list) or len(rows)<2: raise ValueError('终端池至少需要 2 个 MT5')
    endpoints=[];ids=set();paths=set();accounts=set()
    for row in rows:
        if not isinstance(row,dict): raise ValueError('MT5 终端配置格式无效')
        e={k:str(row.get(k,'')).strip() for k in ('id','name','terminal_path','account','server','symbol')}
        if not re.fullmatch(r'[a-zA-Z0-9_-]{1,24}',e['id']) or e['id'] in ids: raise ValueError('终端标识必须唯一')
        if not 1<=len(e['name'])<=40: raise ValueError('请为每个终端填写名称')
        if not re.fullmatch(r'[\w.#+/-]{1,40}',e['symbol']): raise ValueError('请填写每个终端的完整黄金品种名')
        if complete and (not e['terminal_path'] or not e['account'] or not e['server']):
            raise ValueError(e['name']+'：请填写终端路径、目标账号及服务器；可先检查读取身份')
        if e['account'] and not re.fullmatch(r'[0-9]{1,20}',e['account']): raise ValueError('MT5 账号应为数字')
        path=ntpath.normcase(ntpath.normpath(e['terminal_path'])) if e['terminal_path'] else ''
        account=(e['account'],e['server'])
        if path and path in paths: raise ValueError('不同 MT5 必须使用独立安装路径，不能重复连接同一终端')
        if all(account) and account in accounts: raise ValueError('不同终端不能使用同一账号及服务器')
        ids.add(e['id']);paths.add(path) if path else None
        if all(account): accounts.add(account)
        for key,default,lo,hi in (('max_lots',1.,.000001,10000),('commission',0.,0,10000),
                                   ('rebate',0.,0,100),('swap',0.,-100000,100000)):
            v=row.get(key,default)
            if isinstance(v,bool) or not isinstance(v,(int,float)) or not math.isfinite(v) or not lo<=v<=hi:
                raise ValueError(e['name']+'：手数额度、佣金或返佣无效')
            e[key]=float(v)
        if type(row.get('fee_auto',True)) is not bool: raise ValueError('自动佣金设置无效')
        e['fee_auto']=row.get('fee_auto',True);endpoints.append(e)
    endpoints.sort(key=lambda e:e['id'])
    pairs={edge_id(a['id'],b['id']) for a,b in combinations(endpoints,2)}
    rules=value.get('rules',{})
    if not isinstance(rules,dict) or any(k not in pairs for k in rules): raise ValueError('组合规则引用了不存在的终端')
    cleaned={}
    for key,rule in rules.items():
        if not isinstance(rule,dict) or set(rule)-set(RULE_FIELDS): raise ValueError('组合独立参数无效')
        cleaned[key]=copy.deepcopy(rule)
    chart=value.get('chart_pair')
    return dict(endpoints=endpoints,rules=cleaned,chart_pair=chart if chart in pairs else sorted(pairs)[0])


def endpoint_settings(config, e):
    return {**config['mt5'],**{k:e[k] for k in ('terminal_path','account','server','symbol')},
            'adapter':'native','pair_symbols':[e['symbol']]}


def endpoint_probe_settings(config, e):
    # Workers use pair_symbols to restrict their clock/identity scope. The
    # connection probe instead validates exactly one endpoint and its own symbol.
    settings=endpoint_settings(config,e)
    settings.pop('pair_symbols')
    settings.update(pool_endpoint_check=True,pair_lots=config['strategy']['mt5_lots'],
                    pair_execution_mode=config['execution']['mode'])
    return settings


def candidates(config):
    pool=clean_pool(config['mt5_pool'])
    result={};base={k:v for k,v in config.items() if k!='mt5_pool'}
    for a,b in combinations(pool['endpoints'],2):
        key=edge_id(a['id'],b['id']);c=copy.deepcopy(base)
        c['pair'].update(mode='terminals',short_symbol=a['symbol'],
                         name=f"{a['name']} × {b['name']}",endpoints={'binance':a,'mt5':b},pool_edge=key)
        c['symbol']=a['symbol'];c['mt5']=endpoint_settings(config,b)
        c['strategy'].update(pool['rules'].get(key,{}))
        c['strategy']['grid_enabled']=False
        c['costs'].update(usdt_usd=1.,usdt_usd_auto=False,binance_fee_auto=False,
                         mt5_short_commission_per_lot_side=a['commission'],mt5_commission_per_lot_side=b['commission'],
                         mt5_short_rebate_percent=a['rebate'],mt5_rebate_percent=b['rebate'],
                         mt5_short_fee_auto=a['fee_auto'],mt5_fee_auto=b['fee_auto'],
                         mt5_short_swap_per_lot_day=a['swap'],mt5_swap_per_lot_day=b['swap'])
        result[key]=c
    return result


def seed_rules(pool,strategy):
    """Freeze defaults when a fixed pair is created; later edits stay independent."""
    result=clean_pool(pool)
    for a,b in combinations(result['endpoints'],2):
        key=edge_id(a['id'],b['id'])
        result['rules'][key]={**{k:copy.deepcopy(strategy[k]) for k in RULE_FIELDS if k in strategy},
                              **result['rules'].get(key,{})}
    return result
