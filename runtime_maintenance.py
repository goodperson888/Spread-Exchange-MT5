"""Independent read-only maintenance lanes; publish only against unchanged evidence."""
import copy
import math
import threading
import time
from maintenance_work import MaintenanceWork
from trading_config import pair_key
from trading_engine import Engine


def peers(state):
    return [{k:g[k] for k in ('id', 'key', 'mode', 'symbol', 'opened_ms', 'closed_ms', 'qty', 'open_binance') if k in g}
            for g in state['groups']]


def evidence(state, ids):
    # Valuation changes on each quote. It is derived, not an execution identity.
    return dict(groups=[{k:v for k,v in g.items() if k!='valuation' and not k.startswith('live_')}
                        for g in state['groups'] if g['id'] in ids],
                orders=[o for o in state['orders'] if o['group'] in ids])


class MaintenanceMonitor:
    def __init__(self, runtime, client, terminal_factory):
        self.runtime, self.client, self.terminal_factory = runtime, client, terminal_factory
        self.stopped = threading.Event()
        # Account snapshots must not queue behind large history reads either.
        self.terminals = {'account':None, 'costs':None}
        self.session_lock = threading.Lock()
        self.retry_at = {}
        self.threads = []

    def start(self):
        for lane in ('account', 'history'):
            thread = threading.Thread(target=self._loop, args=(lane,), name='gold-pair-maintenance-'+lane, daemon=True)
            self.threads.append(thread)
            thread.start()

    def close(self):
        self.stopped.set()
        with self.session_lock:
            terminals = tuple(self.terminals.values())
            self.terminals = {'account':None, 'costs':None}
        for terminal in terminals:
            if terminal:
                try: terminal.close()
                except Exception: pass

    def capture(self, kind):
        r = self.runtime
        if not r.lock.acquire(blocking=False): return None
        try:
            if (self.stopped.is_set() or r.stop_event.is_set() or self is not r.maintenance
                    or not r.connected): return None
            if kind=='account' and r.config['mt5']['adapter']!='native': return None
            state = r.engine.state
            active = r.engine.active()
            if kind!='account' and any(g['status']!='open' or r.engine.uncertain(g) for g in active): return None
            eligible = []
            if kind=='costs':
                eligible = [g for g in state['groups'] if g.get('mode')=='live'
                    and g.get('key')==pair_key(r.config) and g['status']=='closed'
                    and not g.get('import_closing_costs_checked' if g.get('imported') else 'costs_verified')
                    and int(time.time()*1000)-g.get('import_cost_check_ms' if g.get('imported') else 'cost_check_ms', 0)>=60000]
            target = min(eligible, key=lambda g:g.get('import_cost_check_ms' if g.get('imported') else 'cost_check_ms', 0)) if eligible else None
            if kind=='costs' and (r.config['execution']['mode']!='live' or target is None): return None
            if kind=='fx' and not r.config['costs']['usdt_usd_auto']: return None
            if kind=='funding' and (r.config['execution']['mode']!='live' or not active): return None
            selected = [target] if kind=='costs' else active if kind in ('account', 'funding') else []
            ids = {g['id'] for g in selected}
            token = copy.deepcopy(evidence(state, ids))
            return dict(config=copy.deepcopy(r.config), ids=ids, evidence=token,
                active_ids={g['id'] for g in active}, peers=peers(state) if kind in ('costs', 'funding') else [],
                state=copy.deepcopy(dict(groups=selected, orders=token['orders'])),
                quote=copy.deepcopy(r.quote), positions=copy.deepcopy((r._last_mt5_snapshot or {}).get('positions', [])))
        finally: r.lock.release()

    def _terminal(self, kind):
        with self.session_lock: terminal = self.terminals[kind]
        if terminal is None:
            terminal = self.terminal_factory()
            with self.session_lock:
                stopped = self.stopped.is_set()
                if not stopped: self.terminals[kind] = terminal
            if stopped:
                terminal.close()
                raise RuntimeError('maintenance stopped')
        return terminal

    def step(self, kind):
        capture = self.capture(kind)
        if capture is None: return False
        started = time.monotonic()
        result = {}
        try:
            if kind=='account':
                result['snapshot'] = self._terminal('account').call('snapshot')
                if type(result['snapshot'].get('allowed')) is not bool or not isinstance(result['snapshot'].get('positions'), list):
                    raise ValueError('invalid snapshot')
                for position in result['snapshot']['positions']:
                    if (not isinstance(position, dict) or 'ticket' not in position
                            or isinstance(position.get('swap', 0), bool)
                            or not math.isfinite(float(position.get('swap', 0)))):
                        raise ValueError('invalid position carry')
            elif kind=='fx':
                fx = self.client.usdt_usd()
                if not isinstance(fx.get('value'), (int,float)) or isinstance(fx['value'],bool) or not math.isfinite(fx['value']) or fx['value']<=0:
                    raise ValueError('invalid FX')
                result['fx'] = fx
            else:
                # All calculations operate on copies, using read-only clients.
                work = MaintenanceWork()
                work.config, work.quote = capture['config'], capture['quote']
                work.binance = self.client
                work.terminal = self._terminal('costs') if kind=='costs' else None
                work.market_meta = {}
                work._last_carry_refresh = -float('inf')
                work.funding_peers = capture['peers']
                work.engine = Engine.__new__(Engine)
                work.engine.state = capture['state']
                work.engine.save = lambda event, data=None: result.update(event=event, data=data)
                if kind=='costs': work._verify_closed_costs()
                else: work._update_live_carry(capture['positions'])
                result.update(state=work.engine.state, warnings=work.market_meta)
        except Exception:
            result = {'error':True}
            terminal_kind = 'account' if kind=='account' else 'costs' if kind=='costs' else None
            if terminal_kind:
                with self.session_lock:
                    terminal = self.terminals[terminal_kind]
                    self.terminals[terminal_kind] = None
                if terminal:
                    try: terminal.close()
                    except Exception: pass
        latency = getattr(self.runtime, 'latency', None)
        if latency and not self.stopped.is_set() and self is self.runtime.maintenance:
            latency.record('maintenance_'+kind+'_ms', (time.monotonic()-started)*1000)
        return self.publish(kind, capture, result, started)

    def publish(self, kind, capture, result, started):
        r = self.runtime
        if not r.lock.acquire(blocking=False): return False
        try:
            if (self.stopped.is_set() or r.stop_event.is_set() or self is not r.maintenance
                    or not r.connected or r.config!=capture['config']): return False
            state = r.engine.state
            if kind!='fx':
                if (evidence(state, capture['ids'])!=capture['evidence']
                        or {g['id'] for g in r.engine.active()}!=capture['active_ids']): return False
                if kind in ('costs', 'funding') and peers(state)!=capture['peers']: return False
                if kind!='account' and any(g['status']!='open' or r.engine.uncertain(g) for g in r.engine.active()): return False
            if result.get('error'):
                if kind=='account': r._mt5_risk_error = 'MT5 账户快照读取失败，暂停新开仓；已有仓位继续管理'
                else: r.market_meta[kind+'_warning'] = '后台'+dict(fx='汇率', funding='资金费', costs='成交费用')[kind]+'核对失败，保留原数值稍后重试'
                r.market_wake.set()
                return True
            if kind=='account':
                r._last_mt5_snapshot = result['snapshot']
                r._last_mt5_snapshot_at = started
                r._mt5_risk_error = '' if result['snapshot']['allowed'] else 'MT5 自动交易权限已关闭；已暂停新开仓'
                r._update_live_carry(result['snapshot']['positions'], refresh_funding=False)
            elif kind=='fx':
                r.config['costs']['usdt_usd'] = result['fx']['value']
                r.market_meta['fx'] = result['fx']
                r._last_fx_refresh = time.monotonic()
            elif kind=='funding':
                warning = result['warnings'].get('carry_warning')
                values = {g['id']:g.get('live_binance_funding_usd', 0) for g in result['state']['groups']}
                if not all(isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)
                           for value in values.values()): warning = True
                if warning:
                    r.market_meta['carry_warning'] = '实盘资金费更新待重试；暂用上次核对值'
                else:
                    by_id = {g['id']:g for g in state['groups']}
                    for gid, value in values.items():
                        group = by_id[gid]
                        group['live_binance_funding_usd'] = value
                        group['live_carry_usd'] = group.get('live_mt5_swap_usd', 0)+value
                    r.market_meta.pop('carry_warning', None)
                    r._last_carry_refresh = time.monotonic()
            else:
                if 'event' not in result: return False
                source = result['state']['groups'][0]
                group = next(g for g in state['groups'] if g['id']==source['id'])
                orders = [o for o in state['orders'] if o['group']==group['id']]
                before_group, before_orders = copy.deepcopy(group), copy.deepcopy(orders)
                success = result['event'] in ('costs_verified', 'adoption_closing_costs_checked')
                fields = ('cost_check_ms', 'import_cost_check_ms', 'cost_verification_error', 'cost_verification_stage')
                if success: fields += ('costs_verified', 'mt5_swap_usd', 'binance_funding_usd', 'carry_usd',
                    'live_mt5_swap_usd', 'live_binance_funding_usd', 'import_closing_costs_checked')
                try:
                    for key in fields:
                        if key in source: group[key] = source[key]
                        else: group.pop(key, None)
                    if success:
                        group.pop('cost_verification_error', None)
                        by_id = {o['id']:o for o in result['state']['orders']}
                        for order in orders:
                            for key in ('fee', 'fee_verified', 'swap'):
                                if key in by_id[order['id']]['result']: order['result'][key] = by_id[order['id']]['result'][key]
                        group['valuation'] = r.engine.valuation(group, r.quote)
                    r.engine.save(result['event'], result['data'])
                except Exception:
                    group.clear(); group.update(before_group)
                    for order, before in zip(orders, before_orders): order.clear(); order.update(before)
                    return False
            r.market_meta.pop(kind+'_warning', None)
            r.market_wake.set()
            return True
        finally: r.lock.release()

    def _loop(self, lane):
        while not self.stopped.is_set():
            kinds = ('account',) if lane=='account' else ('fx', 'funding', 'costs')
            for kind in kinds:
                if self.stopped.is_set(): break
                if time.monotonic()<self.retry_at.get(kind, 0): continue
                try: published = self.step(kind)
                except Exception: published = False  # Never stop the trading worker.
                delay = dict(account=2, fx=30, funding=60, costs=2)[kind] if published else .5
                self.retry_at[kind] = time.monotonic()+delay
            self.stopped.wait(.25)
