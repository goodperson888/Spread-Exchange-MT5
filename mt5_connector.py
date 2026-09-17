"""Built-in, read-only MT5 diagnostics. Never imports or calls order_send.

Each probe runs in a bounded child process so a stalled terminal cannot block
the web service. A future execution engine must keep its own persistent session.
"""
import importlib
import json
import math
import subprocess
import sys
import time
from pathlib import Path


def failure(code, message):
    return dict(connected=False, code=code, message=message,
                real_order_enabled=False, checked_at_ms=int(time.time() * 1000))


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
                      symbol=None, quote=None, blockers=[])
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
        selected = str(settings.get('symbol', '')).strip()
        if not selected and len(gold) == 1:
            selected = gold[0].name
        info = api.symbol_info(selected) if selected else None
        if info is None:
            result.update(code='SELECT_SYMBOL', message='请选择本账户的黄金品种，再检查连接。' if gold else '未发现基础货币 XAU、盈亏货币 USD 的黄金品种，请核对经纪商合约。')
            return result
        if (str(getattr(info, 'currency_base', '')).upper() != 'XAU' or
                str(getattr(info, 'currency_profit', '')).upper() != 'USD' or getattr(info, 'custom', False)):
            result.update(code='UNSUPPORTED_SYMBOL', message='所选品种不是标准 XAU/USD 黄金品种，不能自动按盎司配平。')
            return result
        spec = dict(name=str(info.name), contract_size_oz=float(info.trade_contract_size),
                    volume_min=float(info.volume_min), volume_step=float(info.volume_step),
                    volume_max=float(info.volume_max), digits=int(info.digits),
                    trade_mode=int(info.trade_mode), currency_base='XAU', currency_profit='USD')
        numbers = [spec[k] for k in ('contract_size_oz', 'volume_min', 'volume_step', 'volume_max')]
        if not all(math.isfinite(x) and x > 0 for x in numbers) or spec['volume_min'] > spec['volume_max']:
            result.update(code='INVALID_SPEC', message='终端返回了无效合约规格，不能计算配平数量。')
            return result
        result['symbol'] = spec
        if not api.symbol_select(info.name, True):
            result['blockers'].append('无法把黄金品种加入市场报价，请在 MT5 中手动显示该品种。')
        tick = api.symbol_info_tick(info.name)
        stamp = int(getattr(tick, 'time_msc', 0) or 0)
        bid, ask = float(getattr(tick, 'bid', 0)), float(getattr(tick, 'ask', 0))
        if stamp > 0 and math.isfinite(bid) and math.isfinite(ask) and 0 < bid <= ask:
            age = int(time.time() * 1000) - stamp
            result['quote'] = dict(bid=bid, ask=ask, time_ms=stamp, age_ms=age)
            if age > 5000 or age < -1000:
                result['blockers'].append('报价时间过旧或时钟异常；可能休市，不能视为可立即成交。')
        else:
            result['blockers'].append('尚无有效 Bid/Ask，请等待报价或检查休市时间。')
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
