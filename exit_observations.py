"""Bounded exit-decision evidence. Memory only; never reads brokers or the store."""
from collections import OrderedDict, deque
import math
import threading
import time


LABELS = {
    'WAIT_TARGET': '退出价差尚未达标',
    'WAIT_NET_PROFIT': '退出价差达标，预计净收益不足',
    'WAIT_BASKET_TARGET': '整篮子退出价差尚未达标',
    'WAIT_BASKET_NET_PROFIT': '整篮子预计净收益不足',
    'WAIT_BASKET_MEMBER': '本组条件满足，整篮子其他成员条件未满足',
    'EXIT_TRIGGERED': '退出条件触发，进入平仓流程',
    'RECHECK_TARGET': '第一腿前复查：退出价差已不满足',
    'RECHECK_NET_PROFIT': '第一腿前复查：预计净收益不足',
    'QUOTE_UNAVAILABLE': '策略等待有效报价',
    'CLOSE_QUOTE_UNAVAILABLE': '平仓流程等待有效报价',
    'RECOVERY_PENDING': '等待原账户连接及持仓对账',
    'MANAGEMENT_DISABLED': '接管持仓尚未启用管理',
    'OPENING_PENDING': '开仓流程尚未结束',
    'CLOSE_PENDING': '正在平仓或保护性撤回',
    'ATTENTION_REQUIRED': '交易组异常，等待核对处理',
}
RULE_NUMBERS = ('take_contraction_usd', 'exit_spread_usd', 'min_net_profit_usd',
                'max_quote_age_ms', 'max_clock_skew_ms', 'group_max_loss_usd', 'max_hold_minutes')
VALUE_NUMBERS = ('gross', 'fees', 'estimated_exit_fee', 'net_before_rebate',
                 'estimated_rebate', 'net', 'valuation_fx', 'carry')


def finite(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)


def numeric(row, keys):
    return {key: row[key] for key in keys if finite(row.get(key))}


class ExitObservations:
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
            self.started_ms = int(time.time()*1000)
            self.mode = mode if mode in ('live', 'paper') else 'unknown'

    def observe(self, group, quote, code=None, *, valuation=None, basket=None, limits=None,
                expected_key=None, trigger=None, now=None):
        """Record evaluated quotes only. Missing valuation stays missing on early returns.

        First evidence is immutable. Repeats update latest evidence and extrema;
        a reason/rule change or 30-second interval starts a new bounded episode.
        """
        now = int(time.time()*1000) if now is None else now
        q = quote or {}; s = group.get('parameters', {}); v = valuation or {}
        rules = numeric(s, RULE_NUMBERS)
        for key in ('require_net_profit', 'group_loss_enabled'):
            if type(s.get(key)) is bool: rules[key] = s[key]
        for key, allowed in (('target_mode', ('contraction', 'absolute')), ('exit_mode', ('group', 'basket'))):
            if s.get(key) in allowed: rules[key] = s[key]
        revision = group.get('exit_rules_revision', 0)
        if finite(revision): rules['revision'] = revision
        sample = numeric(q, ('time_ms', 'entry', 'exit'))
        sample = {('quote_time_ms' if k == 'time_ms' else 'entry_spread' if k == 'entry' else 'exit_spread'): x
                  for k, x in sample.items()}
        sample['evaluated_ms'] = now
        sample['valuation_evaluated'] = valuation is not None
        sample.update(numeric(v, VALUE_NUMBERS))
        for key in ('estimated', 'fees_verified', 'costs_verified'):
            if type(v.get(key)) is bool: sample[key] = v[key]
        entry = group.get('entry'); exit_spread = q.get('exit')
        in_basket = basket is not None and s.get('exit_mode') == 'basket' and not group.get('imported')
        evaluated_entry = basket['entry'] if in_basket else entry
        if finite(entry): sample['group_entry_spread'] = entry
        if finite(evaluated_entry):
            if s.get('target_mode') == 'contraction' and finite(s.get('take_contraction_usd')):
                sample['target_exit_spread'] = evaluated_entry-s['take_contraction_usd']
                if finite(entry): sample['group_target_exit_spread'] = entry-s['take_contraction_usd']
                if finite(exit_spread): sample['target_reached'] = evaluated_entry-exit_spread >= s['take_contraction_usd']
            elif s.get('target_mode') == 'absolute' and finite(s.get('exit_spread_usd')):
                sample['target_exit_spread'] = sample['group_target_exit_spread'] = s['exit_spread_usd']
                if finite(exit_spread): sample['target_reached'] = exit_spread <= s['exit_spread_usd']
        if in_basket:
            sample.update({'basket_'+k: x for k, x in numeric(basket,
                ('entry', 'net', 'members', 'target_blocked_members', 'net_blocked_members')).items()})
        net = basket['net'] if in_basket else v.get('net')
        require_net = bool(group.get('imported')) or bool(s.get('require_net_profit'))
        if not require_net: sample['net_passed'] = True
        elif finite(net) and finite(s.get('min_net_profit_usd')):
            sample['net_passed'] = net >= s['min_net_profit_usd']
        bounds = limits or s
        sample.update({'checked_'+k: x for k, x in numeric(bounds,
            ('max_quote_age_ms', 'max_clock_skew_ms')).items()})
        sample['key_matches'] = bool(q) and q.get('key') == (group.get('key') if expected_key is None else expected_key)
        sample['group_key_matches'] = bool(q) and q.get('key') == group.get('key')
        prices_valid = True; ages = []
        for leg in ('binance', 'mt5'):
            side = q.get(leg, {})
            sample.update({leg+'_'+k: x for k, x in numeric(side,
                ('bid', 'ask', 'observed_ms', 'time_ms', 'source_time_ms')).items()})
            bid, ask = side.get('bid'), side.get('ask')
            prices_valid &= finite(bid) and finite(ask) and 0 < bid <= ask
            observed = side.get('observed_ms', side.get('time_ms'))
            if finite(observed):
                sample[leg+'_age_ms'] = now-observed; ages.append(now-observed)
        sample['prices_valid'] = bool(prices_valid)
        issues = []
        if not q: issues.append('MISSING_QUOTE')
        if not sample['key_matches']: issues.append('KEY_MISMATCH')
        if not prices_valid: issues.append('INVALID_PRICES')
        sample['quote_valid'] = False
        if len(ages) == 2:
            sample['local_skew_ms'] = abs(ages[0]-ages[1])
            age_limit, skew_limit = bounds.get('max_quote_age_ms'), bounds.get('max_clock_skew_ms')
            if finite(age_limit) and finite(skew_limit):
                sample['quote_valid'] = (sample['key_matches'] and prices_valid
                    and all(-1000 <= age <= age_limit for age in ages) and abs(ages[0]-ages[1]) <= skew_limit)
                if any(age < -1000 for age in ages): issues.append('FUTURE_OBSERVED_TIME')
                if any(age > age_limit for age in ages): issues.append('STALE_QUOTE')
                if abs(ages[0]-ages[1]) > skew_limit: issues.append('LOCAL_SKEW')
        else: issues.append('MISSING_OBSERVED_TIME')
        sample['quote_issues'] = tuple(issues)
        if trigger in ('逐组止盈', '整篮子止盈', '接管篮子收窄并盈利', '本策略总浮动亏损上限', '单组亏损上限', '持仓超时'):
            sample['trigger_label'] = trigger
        if code is None:
            if trigger: code = 'EXIT_TRIGGERED'
            elif not sample.get('target_reached'): code = 'WAIT_BASKET_TARGET' if in_basket else 'WAIT_TARGET'
            elif not sample.get('net_passed'): code = 'WAIT_BASKET_NET_PROFIT' if in_basket else 'WAIT_NET_PROFIT'
            elif in_basket: code = 'WAIT_BASKET_MEMBER'
        if code not in LABELS: return
        signature = (code, tuple(rules.items()), tuple(issues),
                     tuple(numeric(bounds, ('max_quote_age_ms', 'max_clock_skew_ms')).items()))
        gid = group.get('id')
        if not isinstance(gid, str): return
        with self.lock:
            old = self.latest.get(gid)
            if (old is None or old['_signature'] != signature or now < old['first_ms']
                    or now-old['first_ms'] >= self.INTERVAL_MS):
                self.sequence += 1
                old = dict(_group=gid, _signature=signature, sequence=self.sequence, code=code,
                           label=LABELS[code], rules=rules, first_ms=now, first=sample, samples=0)
                if len(self.rows) == self.LIMIT:
                    evicted = self.rows[0]
                    if self.latest.get(evicted['_group']) is evicted: self.latest.pop(evicted['_group'])
                self.rows.append(old)
                self.latest[gid] = old
                self.latest.move_to_end(gid)
                while len(self.latest) > self.LIMIT: self.latest.popitem(last=False)
            old['latest'] = sample; old['last_ms'] = now; old['samples'] += 1
            for field, extreme, choose in (('exit_spread', 'min_exit_spread', min), ('net', 'max_net', max)):
                if field in sample: old[extreme] = choose(old.get(extreme, sample[field]), sample[field])

    def snapshot(self, group_ids):
        # Export never takes the runtime/store lock. Evidence dictionaries are
        # replaced, not mutated, so shallow row copies suffice under this lock.
        with self.lock:
            rows = [dict(row) for row in self.rows]
            meta = dict(schema_version=1, mode=self.mode, started_ms=self.started_ms,
                        episode_limit=self.LIMIT, interval_ms=self.INTERVAL_MS, total_episodes=self.sequence)
        result = []; omitted = 0
        for row in rows:
            gid = row.pop('_group'); row.pop('_signature')
            if gid not in group_ids: omitted += 1; continue
            row['first'] = dict(row['first']); row['latest'] = dict(row['latest']); row['rules'] = dict(row['rules'])
            row['group'] = group_ids[gid]; result.append(row)
        return dict(**meta, omitted_unmapped_episodes=omitted, episodes=result)


PROCESS_EXIT_OBSERVATIONS = ExitObservations()
