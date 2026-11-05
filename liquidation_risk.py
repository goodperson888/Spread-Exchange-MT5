"""Read-only liquidation indicators. Never used to make trading decisions."""
import copy
import math
import threading
import time


DEFAULT_ALERTS = {
    'binance_warning_distance_percent': 5,
    'binance_danger_distance_percent': 2,
    'mt5_warning_margin_level_percent': 200,
    'mt5_danger_margin_level_percent': 100,
}
STALE_MS = 20000


def mt5_risk_fields(account):
    return {key: number(getattr(account, key, None)) for key in
            ('equity', 'margin', 'margin_level', 'margin_so_mode', 'margin_so_call', 'margin_so_so')}


def number(value):
    if value is None or isinstance(value, bool): return None
    try: value = float(value)
    except (TypeError, ValueError, OverflowError): return None
    return value if math.isfinite(value) else None


def validate_alerts(values):
    if not isinstance(values, dict): raise ValueError('风险提醒参数必须为对象')
    settings = {**DEFAULT_ALERTS, **values}
    for key in DEFAULT_ALERTS:
        value = number(settings[key])
        maximum = 100 if key.startswith('binance_') else 1000000
        if type(settings[key]) not in (int, float) or value is None or not 0 < value <= maximum:
            raise ValueError('风险提醒阈值必须为有效正数，币安距离不超过 100%，MT5 保证金水平不超过 1000000%')
    for prefix, suffix in (('binance', 'distance_percent'), ('mt5', 'margin_level_percent')):
        if number(settings[f'{prefix}_danger_{suffix}']) >= number(settings[f'{prefix}_warning_{suffix}']):
            raise ValueError('风险提醒的危险线必须低于预警线')


def binance_risk(account, positions, symbol, alerts):
    result = dict(level='unknown', message='等待币安账户与持仓风险数据', positions=[])
    if isinstance(account, dict):
        result.update(equity=number(account.get('totalMarginBalance')),
                      maintenance_margin=number(account.get('totalMaintMargin')),
                      available=number(account.get('availableBalance')))
    if not isinstance(positions, list): return result
    incomplete = False
    for item in positions:
        if not isinstance(item, dict):
            incomplete = True
            continue
        if item.get('symbol') != symbol: continue
        quantity = number(item.get('positionAmt'))
        if quantity is None:
            incomplete = True
            continue
        if quantity == 0: continue
        mark, liquidation = number(item.get('markPrice')), number(item.get('liquidationPrice'))
        distance = None
        if mark is not None and mark > 0 and liquidation is not None and liquidation > 0:
            distance = number(((liquidation-mark) if quantity < 0 else (mark-liquidation))/mark*100)
        if distance is None:
            incomplete = True
        result['positions'].append(dict(side='short' if quantity < 0 else 'long', qty=abs(quantity),
                                        mark_price=mark, liquidation_price=liquidation,
                                        distance_percent=distance))
    distances = [x['distance_percent'] for x in result['positions'] if x['distance_percent'] is not None]
    nearest = min(distances) if distances else None
    result['distance_percent'] = nearest
    equity, maintenance = result.get('equity'), result.get('maintenance_margin')
    account_complete = equity is not None and maintenance is not None and maintenance >= 0
    if account_complete and (equity < 0 or maintenance > 0 and equity <= maintenance):
        result.update(level='danger', message='账户保证金余额已不高于维持保证金，请立即核查币安账户')
    elif nearest is not None and nearest <= alerts['binance_danger_distance_percent']:
        result.update(level='danger', message='已进入自设强平距离危险区间，请核查敞口和保证金')
    elif nearest is not None and nearest <= alerts['binance_warning_distance_percent']:
        result.update(level='warning', message='已进入自设强平距离预警区间')
    elif incomplete or not account_complete:
        result.update(level='unknown', message='部分风险字段缺失或强平价为 0，无法完整判定')
    elif not result['positions']:
        result.update(level='idle', message='当前品种无持仓；账户指标仍包含其他品种')
    else:
        result.update(level='normal', message='未触发自设预警；强平价随保证金和持仓变化')
    if incomplete and result['level'] in ('warning', 'danger'):
        result['message'] += '；另有风险字段缺失'
    return result


def mt5_risk(account, alerts):
    if not isinstance(account, dict):
        return dict(level='unknown', message='等待 MT5 账户风险数据')
    result = {key: number(account.get(key)) for key in
              ('equity', 'margin', 'free_margin', 'margin_level', 'margin_so_mode', 'margin_so_call', 'margin_so_so')}
    result.update(currency=str(account.get('currency') or ''), level='unknown', message='MT5 风险字段不完整')
    equity, margin = result['equity'], result['margin']
    mode, call, stop = result['margin_so_mode'], result['margin_so_call'], result['margin_so_so']
    if equity is None or margin is None or margin < 0: return result
    if margin == 0:
        if equity < 0:
            result.update(level='danger', message='MT5 账户净值为负，请立即核查账户')
        else:
            result.update(level='idle', message='账户当前无保证金占用')
        return result
    if mode not in (0, 1) or call is None or stop is None or call < 0 or stop < 0: return result
    if mode == 1:
        result.update(message='经纪商采用金额强平模式；仅展示原始阈值，风险等级请在 MT5 终端核实')
        return result
    # The margin level is account equity / used margin, expressed in percent.
    level = number(equity / margin * 100)
    if level is None: return result
    result['margin_level'] = level
    if level <= stop:
        result.update(level='danger', message='已达到经纪商强平线，请立即核查 MT5 账户')
    elif level <= alerts['mt5_danger_margin_level_percent']:
        result.update(level='danger', message='保证金水平已达到自设危险线')
    elif level <= max(call, alerts['mt5_warning_margin_level_percent']):
        result.update(level='warning', message='保证金水平已达到预警线或经纪商追保线')
    else:
        result.update(level='normal', message='未触发预警；经纪商强平规则按账户整体执行')
    result['buffer_percentage_points'] = level - stop
    return result


def freshness(result, age_ms, updated_ms=None, error=''):
    result = dict(result, age_ms=age_ms, updated_ms=updated_ms, stale_after_ms=STALE_MS)
    if age_ms is not None and age_ms >= STALE_MS:
        result.update(level='stale', message='风险数据已过期，当前风险状态未知；请检查连接')
    elif error:
        result.update(level='unknown', message='风险数据读取失败，当前风险状态未知：'+error)
    return result


class BinanceRiskMonitor:
    """Independent REST client; no runtime/terminal lock and no order endpoints."""
    INTERVAL = 5.0

    def __init__(self, client, symbol, clock=time.monotonic, start=True):
        self.client, self.symbol, self.clock = client, symbol, clock
        self.lock = threading.Lock()
        self.stopped = threading.Event()
        self.samples = {}
        self.last_sync = -float('inf')
        if start:
            self.thread = threading.Thread(target=self._loop, name='gold-pair-risk', daemon=True)
            self.thread.start()

    def close(self): self.stopped.set()

    def _step(self):
        for key, path, params in (('account', '/fapi/v3/account', None),
                                  ('positions', '/fapi/v3/positionRisk', {'symbol': self.symbol})):
            if self.stopped.is_set(): return
            started = self.clock()
            try:
                if started-self.last_sync >= 30:
                    self.client.sync()
                    self.last_sync = self.clock()
                data = self.client.request(path, params, signed=True)
                sample = dict(data=data, started=started, updated_ms=int(time.time()*1000), error='')
            except Exception as exc:
                # Only publish known adapter errors; arbitrary exception text can
                # contain a request URL or credentials in third-party clients.
                code = getattr(exc, 'code', None)
                sample = dict(error=f'币安接口 {code}' if type(code) is int or code == 'NETWORK' else '币安风险接口不可用')
            with self.lock:
                if self.stopped.is_set(): return
                previous = self.samples.get(key, {})
                self.samples[key] = {**previous, **sample}

    def status(self, alerts):
        with self.lock: samples = copy.deepcopy(self.samples)
        account, positions = samples.get('account', {}), samples.get('positions', {})
        result = binance_risk(account.get('data'), positions.get('data'), self.symbol, alerts)
        stamps = [x['started'] for x in (account, positions) if 'started' in x]
        age = round(max(0, self.clock()-min(stamps))*1000) if stamps else None
        updated = min((x['updated_ms'] for x in (account, positions) if 'updated_ms' in x), default=None)
        error = '；'.join(x['error'] for x in (account, positions) if x.get('error'))
        return freshness(result, age, updated, error)

    def _loop(self):
        while not self.stopped.is_set():
            self._step()
            self.stopped.wait(self.INTERVAL)


def risk_snapshot(mode, connected, monitor, mt5_snapshot, mt5_started, alerts, clock=time.monotonic, mt5_error=''):
    settings = {**DEFAULT_ALERTS, **alerts}
    if not connected or mode != 'live':
        state = 'disconnected' if not connected else 'paper'
        message = '未连接，风险状态未知' if not connected else '纸面模式：不读取真实账户强平风险'
        return dict(binance=dict(level=state, message=message), mt5=dict(level=state, message=message), alerts=settings)
    binance = monitor.status(settings) if monitor else dict(level='unknown', message='等待币安风险采集')
    account = mt5_snapshot.get('account') if isinstance(mt5_snapshot, dict) else None
    age = round(max(0, clock()-mt5_started)*1000) if account is not None and mt5_started is not None else None
    mt5 = freshness(mt5_risk(account, settings), age, error=mt5_error)
    return dict(binance=binance, mt5=mt5, alerts=settings)
