"""Bounded, memory-only evidence for actual entry evaluations; no broker/store I/O."""
from collections import OrderedDict, deque
import copy
import math
import threading
import time
from pair_instruments import direction_quote, native_thresholds, native_pair
from entry_confirmation import DEFAULTS as CONFIRMATION_DEFAULTS, reached
from liquidity_guard import LABELS as LIQUIDITY_LABELS, safe_evidence as liquidity_evidence

LABELS = {
    **LIQUIDITY_LABELS,
    'WAIT_CONFIRMATION': '等待连续新报价确认',
    'WAIT_THRESHOLD': '对应方向的可成交价差未到阈值',
    'AMBIGUOUS_THRESHOLD': '两种买卖组合同时到价，等待报价区分',
    'DIRECTION_DISABLED': '此买卖方向未启用',
    'QUOTE_INVALID': '等待新鲜同步报价',
    'OPPOSITE_POSITION': '此终端对已有相反方向持仓',
    'TERMINAL_QUOTA': '终端手数额度不足',
    'ACCOUNT_UNVERIFIED': '等待有效账户及持仓核验',
    'PERMISSION': '账户或自动交易权限不符',
    'MARGIN': '保证金不足或尚未核验',
    'RECOVERY_PENDING': '等待恢复持仓对账',
    'ORDER_PENDING': '等待未完成或异常交易组核对',
    'GROUP_LIMIT': '最大交易组数已用满',
    'LOTS_LIMIT': '总手数额度不足',
    'COOLDOWN': '开仓冷却中',
    'ENTRY_PAUSED': '自动新开仓尚未启用或已暂停',
    'OTHER_CANDIDATE': '本轮优先评估其他候选方向',
    'EXIT_PROCESSING': '本轮优先处理平仓或其他已有订单',
    'GRID_LIMIT': '网格加仓条件未满足',
    'ROLLOVER_GUARD': '隔夜或时段保护阻止新开仓',
    'DIRECTION_OR_THRESHOLD': '方向锁定或可成交价差条件未满足',
    'PREFLIGHT_BLOCKED': '下单前复查未通过',
    'EXECUTION_QUOTE_STALE': '下单前报价超过执行新鲜度限制',
    'ENTRY_SUBMIT': '下单前复查通过，进入开仓流程',
}
RULES = ('entry_spread_usd', 'entry_lower_usd', 'max_quote_age_ms', 'max_clock_skew_ms',
         'max_groups', 'max_total_lots', 'cooldown_seconds', 'entry_deviation_usd', *CONFIRMATION_DEFAULTS)


def finite(x):
    return isinstance(x, (int, float)) and not isinstance(x, bool) and math.isfinite(x)


class EntryObservations:
    LIMIT = 512
    INTERVAL_MS = 30000

    def __init__(self):
        self.lock = threading.Lock()
        self.reset('unknown')

    def reset(self, mode):
        with self.lock:
            self.rows = deque(maxlen=self.LIMIT)
            self.latest = OrderedDict()
            self.sequence = 0
            self.mode = mode if mode in ('paper', 'live') else 'unknown'
            self.started_ms = int(time.time()*1000)

    def observe(self, c, quote, code, *, direction=None, stage='engine', enabled=False,
                threshold=None, now=None, confirmation=None, thresholds=None, liquidity=None):
        if code not in LABELS or stage not in ('engine', 'pool_rank', 'preflight'): return
        now = int(time.time()*1000) if now is None else now
        q = direction_quote(quote, {'pair': c.get('pair', {}), 'direction': 'forward'}) or {}
        s = c.get('strategy', {})
        upper, lower = thresholds if thresholds is not None else native_thresholds(c, q)
        rules = {k:s[k] for k in RULES if finite(s.get(k))}
        for d in ('forward','reverse'):
            rules[f'entry_{d}_confirmation_mode']=s.get(f'entry_{d}_confirmation_mode','at_or_better')
        mode = s.get('direction_mode', 'forward') if native_pair(c) else 'forward'
        rules['direction_mode'] = mode if mode in ('forward', 'reverse', 'auto') else 'unknown'
        rules['dynamic_reference_enabled'] = s.get('dynamic_reference_enabled') is True
        sample = {k:q[k] for k in ('time_ms', 'entry', 'exit') if finite(q.get(k))}
        sample.update(evaluated_ms=now, automatic_enabled=bool(enabled))
        if liquidity is not None: sample['liquidity']=liquidity_evidence(liquidity)
        ages = []
        for side, leg in (('a', 'binance'), ('b', 'mt5')):
            book = q.get(leg, {})
            for field in ('bid', 'ask'):
                if finite(book.get(field)): sample[side+'_'+field] = book[field]
            observed = book.get('observed_ms', book.get('time_ms'))
            if finite(observed): sample[side+'_age_ms'] = now-observed; ages.append(now-observed)
        if len(ages) == 2: sample['local_skew_ms'] = abs(ages[0]-ages[1])
        endpoints = c.get('pair', {}).get('endpoints', {})
        if endpoints:
            from mt5_pool_config import fingerprint
            refs = tuple(fingerprint(endpoints[leg]) for leg in ('binance', 'mt5'))
        else: refs = ()
        # Internal keys identify the pair but never appear in exports.
        from trading_config import pair_key
        key = pair_key(c)
        if q.get('key') is not None: sample['pair_matches'] = q['key'] == key
        directions = [direction] if direction in ('forward', 'reverse') else (('forward', 'reverse') if native_pair(c) else ('forward',))
        for d in directions:
            evidence = dict(sample)
            if confirmation and d in confirmation:
                evidence['confirmation'] = dict(confirmation[d])
            evidence['direction_enabled'] = mode in ('auto', d)
            if type(q.get('reference_ready')) is bool: evidence['reference_ready'] = q['reference_ready']
            bound = threshold if finite(threshold) else lower if d == 'reverse' else upper
            value = q.get('exit' if d == 'reverse' else 'entry')
            if finite(bound): evidence['threshold'] = bound
            if finite(value): evidence['executable_spread'] = value
            if finite(value) and finite(bound):
                evidence['threshold_reached'] = reached(s,d,value,bound)
                evidence['distance'] = (0 if evidence['threshold_reached'] else abs(value-bound)) if s.get(f'entry_{d}_confirmation_mode')=='equal' else max(0, value-bound if d == 'reverse' else bound-value)
            signature = (code, stage, tuple(rules.items()), bound if not rules['dynamic_reference_enabled'] else None, bool(enabled))
            index = (key, d, stage)
            with self.lock:
                row = self.latest.get(index)
                if row is None or row['_signature'] != signature or not 0 <= now-row['first_ms'] < self.INTERVAL_MS:
                    if len(self.rows) == self.LIMIT:
                        old = self.rows[0]
                        if self.latest.get(old['_index']) is old: self.latest.pop(old['_index'], None)
                    self.sequence += 1
                    row = dict(_index=index, _signature=signature, _pair=key, _endpoints=refs,
                               sequence=self.sequence, code=code, label=LABELS[code], stage=stage,
                               direction=d, rules=rules, first_ms=now, first=evidence, samples=0)
                    self.rows.append(row); self.latest[index] = row
                    while len(self.latest) > self.LIMIT: self.latest.popitem(last=False)
                row.update(latest=evidence, last_ms=now, samples=row['samples']+1)
                if finite(value):
                    choose, field = (min, 'min_executable_spread') if d == 'reverse' else (max, 'max_executable_spread')
                    row[field] = choose(row.get(field, value), value)
                if evidence.get('threshold_reached'):
                    row['reached_samples'] = row.get('reached_samples', 0)+1
                    row['last_reached'] = evidence

    def summary(self, key):
        """Scan bounded metadata; copy at most four selected samples per poll."""
        current, touches = {}, {}
        def select(target, row, sample):
            direction=row['direction']
            rank=(sample['evaluated_ms'], row['stage']=='preflight')
            if direction not in target or rank >= target[direction][0]:
                target[direction]=(rank,row,sample)
        def views(selected):
            return [dict(code=row['code'],label=row['label'],direction=row['direction'],
                         stage=row['stage'],sample=copy.deepcopy(sample))
                    for _,row,sample in selected.values()]
        with self.lock:
            for row in self.rows:
                if row['_pair'] != key: continue
                select(current,row,row['latest'])
                sample=row.get('last_reached')
                if (sample and sample.get('automatic_enabled') and sample.get('direction_enabled')
                        and row['code'] != 'ENTRY_SUBMIT'):
                    select(touches,row,sample)
            return dict(current=views(current), recent_touches=views(touches))

    def snapshot(self, terminal_alias=None):
        with self.lock:
            rows = [dict(r) for r in self.rows]
            meta = dict(schema_version=1, mode=self.mode, started_ms=self.started_ms,
                        episode_limit=self.LIMIT, interval_ms=self.INTERVAL_MS, total_episodes=self.sequence)
        pairs = {}; terminals = {}; result = []
        for row in rows:
            key = row.pop('_pair'); refs = row.pop('_endpoints')
            row.pop('_index'); row.pop('_signature')
            row['pair'] = pairs.setdefault(key, 'P'+str(len(pairs)+1))
            if refs:
                aliases = []
                for ref in refs:
                    aliases.append(terminal_alias({'identity':ref}) if terminal_alias else terminals.setdefault(ref, 'T'+str(len(terminals)+1)))
                row['endpoints'] = dict(a=aliases[0], b=aliases[1])
            row['rules'] = dict(row['rules']); row['first'] = dict(row['first']); row['latest'] = dict(row['latest'])
            if 'last_reached' in row: row['last_reached'] = dict(row['last_reached'])
            result.append(row)
        return dict(**meta, episodes=result)


PROCESS_ENTRY_OBSERVATIONS = EntryObservations()
