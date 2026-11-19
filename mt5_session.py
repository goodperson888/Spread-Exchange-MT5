"""Pure session binding checks; no terminal calls or account switching."""
import ntpath


def terminal_path(settings):
    return str(settings.get('terminal_path', '')).strip().strip('"').strip()


def normalize_path(path):
    return ntpath.normcase(ntpath.normpath(str(path).strip().strip('"').strip()))


def initialize_options(settings):
    # Only opt in explicitly: portable mode can use a different login profile.
    return {'timeout': 10000, **({'portable': True} if settings.get('portable') is True else {})}


def binding_info(settings, terminal):
    requested = terminal_path(settings)
    actual = str(getattr(terminal, 'path', '') or '')
    actual_directory = ntpath.dirname(actual) if actual.lower().endswith('.exe') else actual
    return dict(requested_path=requested, actual_path=actual,
                data_path=str(getattr(terminal, 'data_path', '') or ''),
                portable=settings.get('portable') is True,
                path_matches=bool(requested and actual and
                    normalize_path(ntpath.dirname(requested)) == normalize_path(actual_directory)))


def binding_error(settings, info):
    if not settings.get('require_terminal_path'):
        return ''
    if not info['requested_path']:
        return '终端池的每个 MT5 必须填写独立的 terminal64.exe 完整路径，不能自动查找。'
    if not info['actual_path']:
        return '无法核验实际连接的 MT5 安装目录，已阻止应用此终端；请重启目标 MT5 后检查。'
    if not info['path_matches']:
        return '实际连接的 MT5 目录与所填路径不一致，已阻止使用此会话。请确认各终端独立安装并已分别登录；复制目录的终端可在确认数据目录后尝试便携模式。'
    if info['portable'] and (not info['data_path'] or
            normalize_path(info['data_path']) != normalize_path(ntpath.dirname(info['requested_path']))):
        return '已选择便携模式，但实际数据目录不在所填安装目录。请先关闭该目标终端，以 terminal64.exe /portable 打开并登录后重试。'
    return ''


def initialize_error(code):
    detail = {-10001: '本机通信发送失败', -10002: '本机通信接收失败',
              -10003: '本机通信初始化失败', -10005: '本机通信超时'}.get(code, '初始化失败')
    return (f'无法连接 MT5（错误码 {code}，{detail}）。请先手动打开所填路径的终端并登录；'
            '多个终端需独立安装及数据目录。若打开了其他终端，请核对快捷方式目标和安装目录；'
            '复制目录时可考虑便携模式，并先在该模式下手动登录。')
