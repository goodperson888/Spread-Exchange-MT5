"""Built-in, read-only MT5 diagnostics. Never imports or calls order_send.

Each probe runs in a bounded child process so a stalled terminal cannot block
the web service. A future execution engine must keep its own persistent session.
"""
import importlib
import json
import math
import re
import subprocess
import sys
import time
from decimal import Decimal, ROUND_CEILING
from pathlib import Path


def failure(code, message):
    return dict(connected=False, code=code, message=message,
                real_order_enabled=False, checked_at_ms=int(time.time() * 1000))


def symbol_diagnostics(info):
    """Read only already-fetched symbol metadata; never guess contract units."""
    fields = ('name', 'description', 'currency_base', 'currency_profit', 'currency_margin')
    result = {key: getattr(info, key, None) for key in fields}
    result['custom'] = getattr(info, 'custom', None)
    for key in ('trade_contract_size', 'trade_calc_mode', 'trade_mode',
                'volume_min', 'volume_step', 'volume_max'):
        value = getattr(info, key, None)
        result[key] = value if isinstance(value, (int, float)) and math.isfinite(value) else None
    reasons = []
    for key, expected, label in (('currency_base', 'XAU', '基础货币'), ('currency_profit', 'USD', '盈利货币')):
        original = getattr(info, key, None)
        value = '' if original is None else str(original).upper()
        if value != expected:
            if original is None:
                reasons.append(f'{label}字段 {key} 未返回，无法确认 {expected}')
            elif not value:
                reasons.append(f'{label}字段 {key} 为空值，无法确认 {expected}')
            else:
                reasons.append(f'{label}字段 {key} 不是 {expected}')
    if getattr(info, 'custom', False):
        reasons.append('custom 为 true：自定义品种不能自动按盎司配平')
    result['reasons'] = reasons
    return result


def symbol_scan_diagnostics(symbols, gold):
    """Name/description matches are diagnostic hints, never executable candidates."""
    hints = []
    for info in symbols:
        text = (str(getattr(info, 'name', ''))+' '+str(getattr(info, 'description', ''))).upper()
        if (str(getattr(info, 'currency_base', '')).upper() == 'XAU' or
                any(token in text for token in ('XAU', 'GOLD', '黄金'))):
            hints.append(info)
    return dict(symbol_scan={'total_symbols':len(symbols), 'validated_candidates':len(gold),
                             'diagnostic_matches':len(hints), 'shown':min(5,len(hints))},
                symbol_diagnostic_candidates=[symbol_diagnostics(info) for info in hints[:5]])


def usd_gold_cfd(info):
    """Narrow broker-metadata exception; still requires a separate profit check."""
    name = str(getattr(info, 'name', '')).upper()
    description = str(getattr(info, 'description', '')).upper()
    return (re.fullmatch(r'XAUUSD[A-Z0-9._#-]*', name) is not None
            and (re.search(r'\bGOLD\b', description) is not None or '黄金' in description)
            and (re.search(r'\bUSD\b|\bUS\s+DOLLAR\b', description) is not None or '美元' in description)
            and str(getattr(info, 'currency_base', '')).upper() == 'USD'
            and str(getattr(info, 'currency_profit', '')).upper() == 'USD'
            and getattr(info, 'custom', None) is False
            and getattr(info, 'trade_calc_mode', None) in (3, 4))  # CFD / CFDLEVERAGE


def quote_diagnostics(tick):
    """Keep invalid quote fields JSON-safe, including on an early probe failure."""
    def finite(key):
        value = getattr(tick, key, None)
        return value if isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value) else None
    bid, ask, source = finite('bid'), finite('ask'), finite('time_msc')
    local = int(time.time() * 1000)
    valid_time = source is not None and source > 0
    delta = local-int(source) if valid_time else None
    hours = round(-delta/3600000) if delta is not None else 0
    possible_offset = hours if hours and abs(-delta-hours*3600000) <= 30000 else None
    return dict(bid=bid, ask=ask, time_ms=source, local_time_ms=local,
                age_ms=delta, raw_time_delta_ms=delta, possible_offset_hours=possible_offset,
                freshness_status='needs_live_updates',
                valid_time=valid_time, valid_price=bid is not None and ask is not None and 0 < bid <= ask)


def verify_cfd_profit(api, info, account, tick, spec):
    """Connection-time read-only check; never used by the execution worker."""
    result = dict(method='gold_cfd_profit', passed=False,
                  account_currency=str(getattr(account, 'currency', '')), checks=[])
    def reject(reason):
        result['reason'] = reason
        return result
    if result['account_currency'].upper() != 'USD':
        return reject('黄金 CFD 兼容校验目前要求美元账户，避免把账户币种换算误当成黄金数量。')
    try:
        reference = quote_diagnostics(tick)
        bid, ask = reference['bid'], reference['ask']
        if reference['valid_price'] and bid > 1:
            result['price_source'] = 'reference_quote'
        else:
            # CFD profit is linear in the explicitly supplied price difference.
            # These hypothetical prices validate units, never form a market quote.
            bid = ask = 1000.0
            result['price_source'] = 'hypothetical_prices'
        step = Decimal(str(spec['volume_step']))
        lots = float((Decimal(str(spec['volume_min'])) / step).to_integral_value(rounding=ROUND_CEILING) * step)
        if lots > spec['volume_max']:
            return reject('合约手数规则无法用于盈亏预估校验。')
        result.update(lots=lots, contract_size_oz=spec['contract_size_oz'], price_change=1.0)
        for side, action, opening, sign in (('buy', api.ORDER_TYPE_BUY, ask, 1),
                                            ('sell', api.ORDER_TYPE_SELL, bid, -1)):
            for move in (1.0, -1.0):
                closing = opening + move
                expected = sign * lots * spec['contract_size_oz'] * (closing - opening)
                if not math.isfinite(expected) or expected == 0:
                    return reject('合约量或报价超出盈亏预估校验的有效数值范围。')
                actual = api.order_calc_profit(action, info.name, lots, opening, closing)
                if isinstance(actual, bool) or not isinstance(actual, (int, float)) or not math.isfinite(actual):
                    return reject('MT5 未返回有效的盈亏预估；请检查终端连接后重试。')
                result['checks'].append(dict(side=side, price_open=opening, price_close=closing,
                                             expected_usd=expected, actual_usd=actual))
                if not math.isclose(actual, expected, rel_tol=1e-4, abs_tol=min(.005, abs(expected) * .005)):
                    return reject('MT5 盈亏预估与每手合约量的黄金换算不一致，不能自动配平。')
    except Exception:
        return reject('无法完成 MT5 盈亏预估校验；请检查终端连接及组件版本后重试。')
    result['passed'] = True
    return result


def inspect_terminal(settings, api=None, platform=None):
    platform = platform or sys.platform
    if api is None:
        if platform != 'win32':
            return failure('WINDOWS_REQUIRED', '内置 MT5 直连支持 Windows 64 位；这台电脑可查看配置页面。请在安装 MT5 的 Windows 电脑运行应用，无需 EA 或独立桥接器。')
        try:
            api = importlib.import_module('MetaTrader5')
        except ImportError:
            return failure('DEPENDENCY_MISSING', '当前运行包缺少内置 MT5 组件。请使用包含组件的 Windows 构建包；源码开发者先安装 requirements-windows.txt。')
    path = str(settings.get('terminal_path', '')).strip().strip('"')
    if path and (not Path(path).is_file() or Path(path).suffix.lower() != '.exe'):
        return failure('TERMINAL_PATH', '终端路径无效，请选择本机 MT5 的 terminal64.exe；只有一个终端时可留空自动查找。')
    try:
        # Attach to the last logged-in session. Do not switch accounts/login.
        args = [path] if path else []
        if not api.initialize(*args, timeout=10000):
            code = api.last_error()[0]
            return failure('INITIALIZE_FAILED', f'无法连接 MT5（错误码 {code}）。请先打开并登录 MT5；有多个终端时填写具体路径。')
        terminal, account = api.terminal_info(), api.account_info()
        if terminal is None or not getattr(terminal, 'connected', False) or account is None or not getattr(account, 'login', 0):
            return failure('NOT_LOGGED_IN', 'MT5 尚未连接交易服务器，请在终端中登录并检查网络。')
        identity = dict(account=str(account.login), server=str(account.server),
                        currency=str(account.currency), margin_mode=int(account.margin_mode))
        result = dict(connected=True, real_order_enabled=False,
                      checked_at_ms=int(time.time() * 1000), identity=identity,
                      terminal_path=str(getattr(terminal, 'path', '')), candidates=[],
                      symbol=None, quote=None, blockers=[], warnings=[])
        expected_account = str(settings.get('account', '')).strip()
        expected_server = str(settings.get('server', '')).strip()
        if ((expected_account and expected_account != identity['account']) or
                (expected_server and expected_server != identity['server'])):
            result.update(code='ACCOUNT_MISMATCH', identity_matches=False,
                          message='当前 MT5 登录账户/服务器与配置不一致，请在 MT5 切换到目标账户后重试。')
            result['blockers'].append(result['message'])
            return result
        result['identity_matches'] = True
        symbols = api.symbols_get()
        if symbols is None:
            return failure('SYMBOLS_UNAVAILABLE', '无法读取 MT5 品种列表，请检查终端连接后重试。')
        gold = [x for x in symbols if str(getattr(x, 'currency_base', '')).upper() == 'XAU'
                and str(getattr(x, 'currency_profit', '')).upper() == 'USD'
                and not getattr(x, 'custom', False)]
        result['candidates'] = [str(x.name) for x in gold][:100]
        possible = gold + [x for x in symbols if usd_gold_cfd(x)]
        selected = str(settings.get('symbol', '')).strip()
        exact_names = {str(x.name) for x in symbols}
        matches = [name for name in exact_names if selected and name.casefold() == selected.casefold()]
        ambiguous_case = selected not in exact_names and len(matches) > 1
        if selected and selected not in exact_names and len(matches) == 1:
            result['symbol_resolution'] = dict(requested=selected, resolved=matches[0])
            selected = matches[0]
        if not selected and len(possible) == 1:
            selected = possible[0].name
        info = api.symbol_info(selected) if selected and not ambiguous_case else None
        if info is None:
            result.update(symbol_scan_diagnostics(symbols, gold))
            if ambiguous_case:
                message = '已连接 MT5，但有多个品种仅大小写不同；请按终端中的完整名称及大小写选定，再重新检查。'
            elif selected:
                message = '已连接 MT5，但无法读取所填品种的详细信息；请核对终端中的完整品种名后重新检查。'
            elif possible:
                message = '已连接 MT5，发现多个黄金候选品种；请选择具体品种，再检查连接；兼容候选还需完成盈亏预估校验。'
            else:
                message = '已连接 MT5，但自动识别未找到通过校验的黄金品种；请查看下方候选诊断，并手动填写终端中的完整品种名后重新检查。'
            result.update(code='SELECT_SYMBOL', message=message)
            return result
        result['symbol_diagnostics'] = symbol_diagnostics(info)
        compatibility = usd_gold_cfd(info)
        if result['symbol_diagnostics']['reasons'] and not compatibility:
            result.update(code='UNSUPPORTED_SYMBOL', message='已连接 MT5，但所选品种未通过黄金规格校验；具体原因见下方品种诊断。')
            return result
        spec = dict(name=str(info.name), contract_size_oz=float(info.trade_contract_size),
                    volume_min=float(info.volume_min), volume_step=float(info.volume_step),
                    volume_max=float(info.volume_max), digits=int(info.digits),
                    trade_mode=int(info.trade_mode), currency_base=str(info.currency_base), currency_profit='USD')
        numbers = [spec[k] for k in ('contract_size_oz', 'volume_min', 'volume_step', 'volume_max')]
        if not all(math.isfinite(x) and x > 0 for x in numbers) or spec['volume_min'] > spec['volume_max']:
            result.update(code='INVALID_SPEC', message='终端返回了无效合约规格，不能计算配平数量。')
            return result
        selected_ok = api.symbol_select(info.name, True)
        if not selected_ok:
            result['blockers'].append('无法把黄金品种加入市场报价，请在 MT5 中手动显示该品种。')
        tick = api.symbol_info_tick(info.name)
        result['quote_diagnostics'] = quote_diagnostics(tick)
        if compatibility:
            # USD base currency is the known metadata variant, not the failure reason.
            result['symbol_diagnostics']['reasons'] = []
            verification = (verify_cfd_profit(api, info, account, tick, spec) if selected_ok else
                            dict(method='gold_cfd_profit', passed=False, reason='请先在 MT5 市场报价中显示该品种，再重新检查。'))
            result['symbol_verification'] = verification
            if not verification['passed']:
                result['symbol_diagnostics']['reasons'].append(verification['reason'])
                result.update(code='UNSUPPORTED_SYMBOL', message='已连接 MT5，但黄金 CFD 兼容校验未通过；具体原因见下方品种诊断。')
                return result
            spec.update(quantity_unit='troy_ounce', verification='gold_cfd_profit')
            result['symbol_diagnostics']['reasons'] = []
            if spec['name'] not in result['candidates']:
                result['candidates'].append(spec['name'])
        result['symbol'] = spec
        diagnostic = result['quote_diagnostics']
        if diagnostic['valid_time'] and diagnostic['valid_price']:
            stamp, bid, ask, age = (diagnostic[key] for key in ('time_ms', 'bid', 'ask', 'age_ms'))
            result['quote'] = dict(bid=bid, ask=ask, time_ms=stamp, age_ms=age)
            if age > 5000 or age < -1000:
                result['warnings'].append('原始时间与本机有偏差，单次读取无法区分时间基准差异与报价未更新；此差值不作为行情延迟。连接实时行情后，仍需通过 tick 持续更新检查，不能据此认定可立即成交。')
        elif not diagnostic['valid_price']:
            result['blockers'].append('尚无有效 Bid/Ask，请在 MT5 市场报价中显示该品种，等待报价或检查休市时间。')
        else:
            result['blockers'].append('已读取 Bid/Ask，但报价时间戳缺失或无效；请等待终端更新报价，当前不能视为可立即成交。')
        if not getattr(terminal, 'trade_allowed', False):
            result['blockers'].append('MT5 工具栏的 Algo Trading / 自动交易未启用。')
        if getattr(terminal, 'tradeapi_disabled', True):
            result['blockers'].append('MT5「工具 → 选项 → EA交易」中需取消「禁止通过外部 Python API 自动交易」。')
        if not getattr(account, 'trade_allowed', False) or not getattr(account, 'trade_expert', False):
            result['blockers'].append('账户未允许程序交易；请确认使用交易密码登录，而非只读投资者密码。')
        if spec['trade_mode'] != getattr(api, 'SYMBOL_TRADE_MODE_FULL', 4):
            result['blockers'].append('品种未开放完整双向交易权限。')
        if identity['margin_mode'] != getattr(api, 'ACCOUNT_MARGIN_MODE_RETAIL_HEDGING', 2):
            result['blockers'].append('该账户为净额或交易所模式；逐组持仓隔离尚未适配。')
        result.update(code='CONNECTED', message='已读取本机 MT5 账户及黄金规格；此次检查不下单。')
        return result
    except Exception:
        return failure('PROBE_ERROR', '读取 MT5 失败，请关闭残留终端窗口、重新登录后检查。')
    finally:
        try:
            api.shutdown()
        except Exception:
            pass


def run_probe(settings):
    if sys.platform != 'win32':
        return inspect_terminal(settings)
    command = [sys.executable, '--mt5-probe'] if getattr(sys, 'frozen', False) else [sys.executable, str(Path(__file__).resolve()), '--mt5-probe']
    try:
        child = subprocess.run(command, input=json.dumps(settings), capture_output=True,
                               text=True, encoding='utf-8', timeout=18,
                               creationflags=subprocess.CREATE_NO_WINDOW)
        if child.returncode:
            return failure('PROBE_PROCESS_FAILED', '内置 MT5 组件启动失败，请检查 Windows 构建包是否完整。')
        return json.loads(child.stdout)
    except subprocess.TimeoutExpired:
        return failure('PROBE_TIMEOUT', 'MT5 检查超过 18 秒已停止等待；请检查终端是否卡住后重试。')
    except (ValueError, OSError):
        return failure('PROBE_PROCESS_FAILED', '无法读取内置 MT5 组件的结果，请检查应用安装。')


def probe_main():
    settings = json.loads(sys.stdin.read(16000))
    print(json.dumps(inspect_terminal(settings), ensure_ascii=True))


if __name__ == '__main__':
    probe_main()
