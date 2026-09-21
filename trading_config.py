"""Validated configuration and executable equal-ounce order sizes."""
import math
import re
from decimal import Decimal
from urllib.parse import urlsplit


def validate_proxy_url(value):
    """Accept an optional credential-free HTTP proxy URL."""
    value = str(value or '').strip()
    if not value:
        return ''
    try:
        parsed = urlsplit(value)
        port = parsed.port
    except ValueError:
        raise ValueError('币安代理地址无效') from None
    if (parsed.scheme.lower() != 'http' or not parsed.hostname or not port or
            parsed.path not in ('', '/') or parsed.query or parsed.fragment or
            parsed.username is not None or parsed.password is not None):
        raise ValueError('币安代理请填写无账号密码的 HTTP 地址，例如 http://127.0.0.1:7890')
    return value


def validate(c):
    s, e, costs = c['strategy'], c['execution'], c['costs']
    validate_proxy_url(c.get('binance', {}).get('proxy_url', ''))
    if not re.fullmatch(r'[A-Z0-9]{5,24}', c['symbol']):
        raise ValueError('币安合约名称无效，请填写平台实际名称，例如 XAUUSDT')
    if e['mode'] not in ('paper', 'live') or e['quote_source'] != 'market':
        raise ValueError('运行模式或行情来源无效')
    if c['mt5']['adapter'] not in ('paper', 'mcp', 'native'):
        raise ValueError('MT5 适配器无效')
    if e['mode'] == 'live' and (e['quote_source'] != 'market' or c['mt5']['adapter'] != 'native'):
        raise ValueError('实盘必须使用真实行情及 Windows MT5 终端')
    if s['exit_mode'] not in ('group', 'basket') or s['target_mode'] not in ('contraction', 'absolute'):
        raise ValueError('退出方式无效')
    for k in ('require_net_profit', 'group_loss_enabled', 'total_loss_enabled', 'grid_enabled'):
        if type(s[k]) is not bool:
            raise ValueError(k+' 必须是开关')
    bounds = {
        'mt5_lots': (0.000001, 10000), 'max_groups': (1, 50), 'cooldown_seconds': (1, 86400),
        'max_total_lots': (0.000001, 10000), 'entry_spread_usd': (0, 100000),
        'grid_spacing_usd': (0.000001, 100000), 'grid_max_adds': (0, 49),
        'take_contraction_usd': (0.000001, 100000), 'exit_spread_usd': (-100000, 100000),
        'min_net_profit_usd': (0, 1e9), 'group_max_loss_usd': (0.01, 1e9), 'total_max_loss_usd': (0.01, 1e9),
        'max_hold_minutes': (0, 525600), 'max_quote_age_ms': (100, 10000),
        'max_clock_skew_ms': (0, 5000), 'max_unhedged_ms': (100, 30000), 'max_slippage_usd': (0.01, 1000),
        'unwind_slippage_usd': (0.01, 1000),
    }
    for k, (lo, hi) in bounds.items():
        v = s[k]
        if isinstance(v, bool) or not isinstance(v, (int, float)) or not math.isfinite(v) or not lo <= v <= hi:
            raise ValueError(f'{k} 必须在 {lo}～{hi} 范围内')
    if (int(s['max_groups']) != s['max_groups'] or int(s['grid_max_adds']) != s['grid_max_adds']
            or s['mt5_lots'] > s['max_total_lots']):
        raise ValueError('组数必须是整数，每组手数不能超过总手数上限')
    if s['unwind_slippage_usd'] < s['max_slippage_usd']:
        raise ValueError('保护性平仓滑点不能小于普通最大滑点')
    if s['grid_enabled'] and int(s['max_groups']) < 1 + int(s['grid_max_adds']):
        raise ValueError('启用网格时，最大持仓组数必须至少为 1 + 网格补仓次数')
    for k, lo, hi in [('poll_ms', 250, 5000), ('magic', 1, 2147483647), ('close_retry_limit', 1, 5)]:
        v=e[k]
        if type(v) is not int or not lo <= v <= hi:
            raise ValueError(k+' 不在有效整数范围')
    for k in ('usdt_usd_auto','binance_fee_auto'):
        if type(costs[k]) is not bool:
            raise ValueError(k+' 必须是开关')
    for k, v in costs.items():
        if k in ('usdt_usd_auto','binance_fee_auto'): continue
        if isinstance(v, bool) or not isinstance(v, (int, float)) or not math.isfinite(v):
            raise ValueError('费用参数必须为有限数字')
    if not .5 <= costs['usdt_usd'] <= 1.5 or not 0 <= costs['binance_taker_percent'] <= 5:
        raise ValueError('汇率或币安手续费率无效')
    if not 0 <= costs['mt5_commission_per_lot_side'] <= 10000:
        raise ValueError('MT5 佣金参数无效')


def pair_key(c):
    return '|'.join(str(x) for x in (c['execution']['mode'], c['execution']['quote_source'], c['symbol'],
        c['mt5']['adapter'], c['mt5'].get('terminal_path'), c['mt5'].get('account'), c['mt5'].get('server'),
        c['mt5']['symbol'], c['execution']['magic']))


def plan(c, mt5, exchange, price):
    lots = Decimal(str(c['strategy']['mt5_lots']))
    def fits(n, minimum, maximum, step):
        lo, hi, step = map(lambda x: Decimal(str(x)), (minimum, maximum, step))
        return step > 0 and lo <= n <= hi and n % step == 0
    if not fits(lots, mt5['volume_min'], mt5['volume_max'], mt5['volume_step']):
        raise ValueError('MT5 手数不符合实际合约规格')
    qty = lots * Decimal(str(mt5['contract_size_oz']))
    if exchange['status'] != 'TRADING' or exchange.get('baseAsset') not in ('XAU', 'PAXG') or exchange.get('quoteAsset') != 'USDT':
        raise ValueError('请选择可交易的 USDT 黄金合约；不自动替换其他品种')
    for key in ('LOT_SIZE', 'MARKET_LOT_SIZE'):
        f=exchange['filters'].get(key)
        if not f or not fits(qty, f['minQty'], f['maxQty'], f['stepSize']):
            raise ValueError('等黄金数量不符合币安数量规则，请调整 MT5 手数')
    f=exchange['filters'].get('MIN_NOTIONAL', {})
    if qty*Decimal(str(price)) < Decimal(str(f.get('notional', f.get('minNotional', 0)))):
        raise ValueError('币安名义金额低于最小下单金额')
    return {'lots':float(lots), 'qty':float(qty), 'contract':mt5['contract_size_oz']}
