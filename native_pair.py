"""Same-terminal equal-lot pairs, reusing durable execution and strict native fill proofs.

The persisted engine quantity is LOTS for these groups. Conversion to native
contract quantity happens only at the broker boundary; prices stay unscaled.
"""
import copy
import math
import threading
import time
from trading_brokers import Terminal, PaperBroker
from trading_config import pair_key
from trading_engine import valid_group_quote, stamp
from pair_instruments import short_symbol, equal_lot_plan, native_thresholds, pair_info, position_side
from market_push import TickFreshness
from execution_latency import LatencyStats
from entry_observations import PROCESS_ENTRY_OBSERVATIONS
from runtime_maintenance import evidence
import external_closes
import close_cause
import commission_reference


def settings_for(config,leg=None):
    if leg and config.get('pair',{}).get('endpoints'):
        from mt5_pool_config import endpoint_settings
        return endpoint_settings(config,config['pair']['endpoints'][leg])
    return {**config['mt5'],'pair_symbols':[short_symbol(config),config['mt5']['symbol']]}


class NativePairBroker:
    def __init__(self, terminal, contracts):
        self.terminal=terminal;self.contracts=contracts

    def terminal_for(self,leg):
        return self.terminal[leg] if isinstance(self.terminal,dict) else self.terminal

    def wire(self, order):
        o=copy.deepcopy(order);factor=self.contracts[o['leg']]
        o['requested']*=factor
        if isinstance(o.get('result'),dict):
            o['result']['qty']=o['result'].get('qty',0)*factor
        return o

    def read_result(self, result, leg):
        r=copy.deepcopy(result)
        # Invalid fields stay invalid for Engine.result_valid; never coerce to zero.
        if isinstance(r,dict) and isinstance(r.get('qty'),(int,float)) and not isinstance(r['qty'],bool):
            r['qty']/=self.contracts[leg]
        return r

    def submit(self, order):
        o=self.wire(order)
        return self.read_result(self.terminal_for(o['leg']).call('submit',symbol=o['symbol'],order=o),o['leg'])

    def query(self, order):
        o=self.wire(order)
        return self.read_result(self.terminal_for(o['leg']).call('query',symbol=o['symbol'],order=o),o['leg'])

    def cancel(self, order):
        # Native IOC/FOK original orders must be verified, never blindly replaced.
        return None


class NativePairPump:
    def __init__(self, runtime, terminal, config=None):
        self.runtime=runtime;self.terminal=terminal;self.bridge=runtime.push_bridge
        self.config=copy.deepcopy(config if config is not None else runtime.config)
        self.changed=threading.Event();self.stopped=threading.Event();self.lock=threading.Lock()
        self.latest=None;self.version=0;self.error='';self.last_stamp=0
        self.pending_sample=None;self.sample_wake=threading.Event();self.sampling_error=''
        self.reference_entry=None;self.reference_exit=None;self.reference_samples=0;self.reference_time_ms=None
        self.mt5_transport='MT5 双品种报价轮询';self.binance_transport='MT5 品种 A'
        self.thread=threading.Thread(target=self._loop,name='mt5-pair-quotes',daemon=True)
        self.sample_thread=threading.Thread(target=self._samples,name='mt5-pair-history',daemon=True)

    def start(self): self.sample_thread.start();self.thread.start()
    def close(self):
        self.stopped.set();self.changed.set();self.sample_wake.set();self.terminal.close()
        if self.thread.is_alive() and threading.current_thread() is not self.thread:self.thread.join(timeout=.5)
        if self.sample_thread.is_alive() and threading.current_thread() is not self.sample_thread:self.sample_thread.join(timeout=.5)
    def read(self):
        with self.lock: return self.latest,self.version,self.error or self.sampling_error

    def refresh(self,publish=True,timeout_seconds=None):
        r=self.runtime;c=self.config
        names={'binance':short_symbol(c),'mt5':c['mt5']['symbol']}
        quotes={leg:self.bridge.quote({**c['mt5'],'symbol':name},c['strategy']['max_quote_age_ms'])
                if self.bridge else None for leg,name in names.items()}
        self.binance_transport='EA Socket 推送' if quotes['binance'] else '原生双品种报价轮询'
        self.mt5_transport='EA Socket 推送' if quotes['mt5'] else '原生双品种报价轮询'
        if not all(quotes.values()):
            rows=self.terminal.call('pair_quotes',**({'timeout_seconds':timeout_seconds} if timeout_seconds is not None else {}))
            for leg,name in names.items():
                if quotes[leg] is None:
                    quotes[leg]=r.mt5_freshness.observe(rows[name],{**c['mt5'],'symbol':name})
        q=dict(key=pair_key(c),time_ms=stamp(),usdt_usd=1.,**quotes,
               binance_transport=self.binance_transport,mt5_transport=self.mt5_transport,
               pair=pair_info(c),entry=quotes['binance']['bid']-quotes['mt5']['ask'],
               exit=quotes['binance']['ask']-quotes['mt5']['bid'])
        self._add_reference(q)
        return self.publish(q) if publish else q

    def _add_reference(self,q):
        """Attach a time-based EMA without blocking the quote path."""
        now=int(q['time_ms']);previous=self.reference_time_ms
        dt=max(1,now-previous) if previous is not None else 1
        window=max(1,int(self.config.get('strategy',{}).get('reference_window_minutes',240)))*60000
        alpha=1-math.exp(-dt/window)
        if self.reference_entry is None:
            self.reference_entry=float(q['entry']);self.reference_exit=float(q['exit'])
        else:
            self.reference_entry+=alpha*(float(q['entry'])-self.reference_entry)
            self.reference_exit+=alpha*(float(q['exit'])-self.reference_exit)
        self.reference_time_ms=now;self.reference_samples+=1
        ready=self.reference_samples>=20
        q.update(reference_entry=self.reference_entry,reference_exit=self.reference_exit,
                 reference_samples=self.reference_samples,reference_ready=ready,
                 dynamic_reference_enabled=bool(self.config.get('strategy',{}).get('dynamic_reference_enabled')))
        upper,lower=native_thresholds(self.config,q)
        q.update(entry_threshold=upper,entry_lower_threshold=lower)

    def publish(self,q):
        if self.stopped.is_set(): return q
        r=self.runtime
        with self.lock:
            previous=self.latest
            changed=not previous or any(previous[leg]!=q[leg] for leg in ('binance','mt5'))
            self.error=''
            if not changed: return self.latest
            q['time_ms']=max(q['time_ms'],self.last_stamp+1);self.last_stamp=q['time_ms']
            LatencyStats.published(q)
            self.latest=q;self.version+=1
            self.pending_sample=q
        r.quote_events.publish(q);r.market_wake.set();self.sample_wake.set()
        return q

    def _samples(self):
        # Bound chart history to the latest sample per 100 ms. Live decisions
        # use the latest quote without waiting for the history write.
        while not self.stopped.is_set():
            self.sample_wake.wait(.1);self.sample_wake.clear()
            with self.lock:pending=self.pending_sample;self.pending_sample=None
            if pending is not None:
                try:self.runtime.store.sample(pending)
                except Exception as exc:
                    self.sampling_error='行情历史写入失败：'+str(exc);self.runtime.market_wake.set()
            self.stopped.wait(.1)

    def _loop(self):
        while not self.stopped.is_set():
            self.changed.clear()
            try: self.refresh()
            except Exception as exc:
                with self.lock: self.error=str(exc)
                self.runtime.market_wake.set()
            self.changed.wait(.1)


def leg_view(state, group, leg):
    """Reuse the native external-close verifier with one ticket-owned leg at a time."""
    g=copy.deepcopy(group);g['mt5_symbol']=group['symbol'] if leg=='binance' else group['mt5_symbol']
    if group.get('pair',{}).get('endpoints'):
        g['_endpoint_identity']=group['pair']['endpoints'][leg]['identity']
    rows=[copy.deepcopy(o) for o in state['orders'] if o['group']==g['id'] and o['leg']==leg]
    for o in rows: o['leg']='mt5'
    return dict(groups=[g],orders=rows),g


def exposure_matches(engine, config, snapshots):
    """A complete in-memory ownership proof against a stable platform snapshot."""
    if config.get('pair',{}).get('endpoints'):
        from mt5_pool import pool_exposure_matches
        return pool_exposure_matches(engine,config,snapshots)
    for leg,snapshot in snapshots.items():
        if snapshot['orders']: return False
        current={str(p['ticket']):p for p in snapshot['positions']};expected=set()
        if len(current)!=len(snapshot['positions']): return False
        for g in engine.active():
            if g['key']!=pair_key(config) or engine.uncertain(g): return False
            qty=engine.amounts(g)[leg]
            if qty<1e-8: continue
            openings=[o for o in engine.state['orders'] if o['group']==g['id'] and o['leg']==leg
                      and o['action']=='open' and o['result'].get('qty',0)>0]
            if len(openings)!=1: return False
            o=openings[0];ticket=external_closes.opening_position(o,current,config['execution']['magic'])
            p=current.get(ticket)
            if (not p or ticket in expected or p.get('symbol')!=o['symbol']
                    or p.get('side')!=o.get('native_position_side',0) or p.get('magic')!=config['execution']['magic']
                    or p.get('comment')!=o['id'] or abs(float(p['lots'])-qty)>1e-8
                    or (o['result'].get('position_identifier') and
                        str(p.get('identifier'))!=str(o['result']['position_identifier']))): return False
            expected.add(ticket)
        if set(current)!=expected: return False
    return True


class NativePairObserver:
    def __init__(self,runtime):
        self.runtime=runtime;self.stopped=threading.Event();self.lock=threading.Lock();self.terminals={}
        self.threads=[]
    def start(self):
        for lane in ('account','history','commission'):
            t=threading.Thread(target=self._loop,args=(lane,),name='mt5-pair-'+lane,daemon=True)
            self.threads.append(t);t.start()
    def close(self):
        self.stopped.set()
        with self.lock: terminals=list(self.terminals.values());self.terminals.clear()
        for terminal in terminals:
            if terminal is not None:
                try: terminal.close()
                except Exception: pass
    def _terminal(self,lane,leg=None):
        if self.runtime.config.get('pair',{}).get('endpoints'):
            leg=leg or 'mt5';slot=(lane,leg)
        else: slot=lane
        with self.lock: terminal=self.terminals.get(slot)
        if terminal is None:
            owner=getattr(self.runtime,'pool_owner',None)
            if owner:
                return owner.read_terminal(lane,self.runtime.config['pair']['endpoints'][leg])
            r=self.runtime;terminal=Terminal(settings_for(r.config,leg),r.config['execution']['magic'],read_only=True)
            with self.lock:
                if self.stopped.is_set(): terminal.close();raise ValueError('观察会话已停止')
                self.terminals[slot]=terminal
        return terminal
    def step(self,lane):
        r=self.runtime
        if not r.lock.acquire(blocking=False): return
        try:
            if self is not r.maintenance or not r.connected: return
            c=copy.deepcopy(r.config);key=pair_key(c)
            groups=[g for g in r.engine.state['groups'] if g.get('key')==key and g.get('mode')=='live'
                    and (g['status']!='closed' or not g.get('costs_verified'))]
            # Bounded history work; active groups always take precedence.
            groups=sorted(groups,key=lambda g:g['status']=='closed')[:50]
            ids={g['id'] for g in groups};token=copy.deepcopy(evidence(r.engine.state,ids))
            state=copy.deepcopy(dict(groups=groups,orders=token['orders']))
        finally: r.lock.release()
        terminal=self._terminal(lane)
        snapshots={leg:self._terminal(lane,leg).call('snapshot',symbol=name) for leg,name in
                   (('binance',short_symbol(c)),('mt5',c['mt5']['symbol']))}
        if lane=='account':
            with r.lock:
                if self is not r.maintenance or pair_key(r.config)!=key: return
                r._native_snapshots=snapshots
                r._last_mt5_snapshot=snapshots['mt5'];r._last_mt5_snapshot_at=time.monotonic();r._mt5_risk_error=''
                if not all(s.get('allowed') for s in snapshots.values()):
                    r._mt5_risk_error='MT5 自动交易权限已关闭，暂停新开仓'
                if token==evidence(r.engine.state,ids):
                    if any(s['orders'] for s in snapshots.values()):
                        r._mt5_risk_error='MT5 组合品种存在未完成委托，暂停新开仓'
                    for g in groups:
                        if g['status']!='open' or r.engine.uncertain(g): continue
                        for leg,snapshot in snapshots.items():
                            view,vg=leg_view(state,g,leg)
                            current={str(p['ticket']):p for p in snapshot['positions']}
                            opening=next((o for o in view['orders'] if o['action']=='open' and o['result'].get('qty',0)>0),None)
                            if not opening: continue
                            ticket=external_closes.opening_position(opening,current,c['execution']['magic'])
                            position=current.get(ticket)
                            if position and (position.get('side')!=opening.get('native_position_side',0)
                                    or position.get('magic')!=c['execution']['magic'] or position.get('comment')!=opening['id']
                                    or (opening['result'].get('position_identifier') and
                                        str(position.get('identifier'))!=str(opening['result']['position_identifier']))):
                                r._mt5_risk_error='MT5 持仓归属或方向变化，暂停新开仓并等待核对'
                            actual=float(position['lots']) if position else 0.
                            if abs(actual-r.engine.amounts(g)[leg])>1e-8:
                                r._mt5_risk_error='MT5 平台持仓变化，暂停新开仓并等待成交历史核对'
                    active=r.engine.active()
                    if all(g['status']=='open' and not r.engine.uncertain(g) for g in active) and not exposure_matches(r.engine,c,snapshots):
                        r._mt5_risk_error='MT5 平台持仓或委托与本机归属不一致，暂停新开仓并等待核对'
                if r._mt5_risk_error and r.engine.state.get('enabled'): r.engine.pause(r._mt5_risk_error)
            return
        updates=[];fees=[];swaps={};errors=[]
        if any(s['orders'] for s in snapshots.values()):
            raise ValueError('MT5 组合品种存在未完成委托，等待完整核对')
        broker=NativePairBroker({leg:self._terminal(lane,leg) for leg in ('binance','mt5')},r.plan['leg_contracts'])
        for g in state['groups']:
            if any(o['group']==g['id'] and o['result'].get('status')!='done' for o in state['orders']): continue
            group_swap=0.;verified=True;carry_valid=True
            for leg in ('binance','mt5'):
                view,vg=leg_view(state,g,leg);snapshot=snapshots[leg]
                opening=next((o for o in view['orders'] if o['action']=='open' and o['result'].get('qty',0)>0),None)
                if not opening: continue
                try:
                    result=opening['result']
                    anchor=dict(identifier=result.get('position_identifier'),order_ticket=result.get('ticket'),
                                created_ms=opening['created_ms'],comment=opening['id'],magic=c['execution']['magic'],
                                native_position_side=opening.get('native_position_side',0))
                    deals=self._terminal(lane,leg).call('external_history',symbol=vg['mt5_symbol'],start_ms=g['opened_ms'],openings=[anchor])
                    current={str(p['ticket']):p for p in snapshot['positions']}
                    rows=external_closes.mt5_backfill(view,c,vg,current,deals)
                    for o in rows:
                        o.update(leg=leg,platform='mt5',quantity_unit='lot',
                                 native_position_side=opening.get('native_position_side',0),contract_size=g['leg_contracts'][leg])
                        if opening.get('endpoint'): o['endpoint']=copy.deepcopy(opening['endpoint'])
                        updates.append(o)
                    for o in state['orders']:
                        if o['group']!=g['id'] or o['leg']!=leg or o.get('source')=='external' or not o['result'].get('qty'): continue
                        found=broker.query(o)
                        if (found.get('status')=='done' and found.get('fee_verified') is True and
                            abs(found.get('qty',-1)-o['result']['qty'])<1e-8): fees.append((o['id'],found))
                        else: verified=False
                    group_swap+=sum(float(d.get('swap',0)) for d in deals)
                    position=external_closes.opening_position(opening,current,c['execution']['magic'])
                    if position in current: group_swap+=float(current[position].get('swap',0))
                    if not math.isfinite(group_swap): raise ValueError('MT5 Swap 字段无效')
                except Exception as exc: errors.append(str(exc));verified=False;carry_valid=False
            swaps[g['id']]=(group_swap,verified,carry_valid)
        needs_protection=any(close_cause.liquidated(o) and any(g['id']==o['group'] and g['status']!='closed' for g in state['groups'])
                             for o in state['orders'])
        if updates or needs_protection:
            # External history and live positions must describe one stable state.
            # Quotes/profit/swap can move; compare only ownership and exposure.
            def exposure(s):
                return (sorted((str(p.get('ticket')),str(p.get('identifier')),p.get('symbol'),
                                p.get('magic'),p.get('side'),p.get('lots'),p.get('comment')) for p in s['positions']),
                        s['orders'])
            for leg in ('binance','mt5'):
                after=self._terminal(lane,leg).call('snapshot',symbol=short_symbol(c) if leg=='binance' else c['mt5']['symbol'])
                if exposure(after)!=exposure(snapshots[leg]):
                    raise ValueError('MT5 仓位在成交核对期间变化，稍后重查；不会推算平仓')
        with r.lock:
            if (self is not r.maintenance or pair_key(r.config)!=key or
                    token!=evidence(r.engine.state,ids)): return
            changed=False
            for oid,found in fees:
                o=next(o for o in r.engine.state['orders'] if o['id']==oid)
                for field in ('fee','swap','fee_verified','position','position_identifier'):
                    if field in found and o['result'].get(field)!=found[field]: o['result'][field]=found[field];changed=True
            for o in updates:
                if o.pop('_evidence_only',False):
                    existing=next((x for x in r.engine.state['orders'] if x['id']==o['id']),None)
                    if existing and existing['result'].get('close_cause')!=o['result'].get('close_cause'):
                        existing['result']['close_cause']=o['result']['close_cause'];changed=True
                    continue
                o.pop('_opening_identity',None)
                if any(x['id']==o['id'] for x in r.engine.state['orders']): continue
                o['result']['fee_verified']=True
                r.engine.state['orders'].append(o);changed=True
            for g in r.engine.state['groups']:
                if g['id'] not in swaps: continue
                value,verified,carry_valid=swaps[g['id']]
                if carry_valid and g.get('native_swap_usd')!=value: g['native_swap_usd']=value;changed=True
                if any(o['group']==g['id'] for o in updates):
                    r.engine.pause('发现 MT5 平台外部平仓，已回补记录；新开仓暂停')
                    g['external_close']=True
                    amounts=r.engine.amounts(g)
                    if max(amounts.values())<1e-8 and not r.engine.uncertain(g):
                        g.update(status='closing',reason='平台外部平仓，双腿成交历史已核对')
                        r.engine.finalize_flat(g,r.quote)
                    elif abs(amounts['binance']-amounts['mt5'])>1e-8:
                        g.update(status='attention',reason='平台外部平仓后双腿手数不一致，请对账后平仓')
                if g['status']=='closed' and verified and not g.get('costs_verified'):
                    g['costs_verified']=True;g['valuation']=r.engine.valuation(g,r.quote);changed=True
            if not errors and exposure_matches(r.engine,c,snapshots):
                armed=[]
                for g in r.engine.active():
                    if g.get('liquidation_recovery') and (g['status']=='attention' or
                            (r.reconciled and not r.engine.state.get('recovery'))): continue
                    if any(close_cause.liquidated(o) for o in r.engine.state['orders'] if o['group']==g['id']):
                        g.setdefault('liquidation_recovery',dict(detected_ms=stamp(),armed_ms=stamp(),
                                     **{'remaining_'+leg:qty for leg,qty in r.engine.amounts(g).items()}))
                        g.update(status='unwinding',reason='MT5 已确认强平，保护性平掉剩余敞口')
                        armed.append(g['id'])
                if armed:
                    r.engine.pause('MT5 已确认强平，保护性平掉剩余敞口')
                    if not getattr(r,'pool_owner',None): r.engine.state['recovery']=False
                    r.reconciled=True;changed=True
                    r.engine.save('liquidation_protection_armed',{'groups':armed})
                elif not r.engine.active():
                    if not getattr(r,'pool_owner',None): r.engine.state['recovery']=False
                    r.reconciled=True
            if changed: r.engine.save('native_pair_history_updated')
            r.external_close_status=dict(state='waiting' if errors else 'ready',checked_ms=stamp(),
                                         message=errors[0] if errors else 'MT5 双腿持仓与成交后台核对中')
            r.market_wake.set()
            if getattr(r,'pool_owner',None): r.pool_owner.recovery_updated()
    def _loop(self,lane):
        if lane=='commission':
            while not self.stopped.is_set():
                try: commission_reference.refresh(self,lambda:self._terminal('commission'))
                except Exception: pass
                self.stopped.wait(60)
            return
        while not self.stopped.is_set():
            try: self.step(lane)
            except Exception as exc:
                r=self.runtime
                with r.lock:
                    if self is r.maintenance:
                        if lane=='account': r._mt5_risk_error='MT5 账户读取失败：'+str(exc)
                        else: r.external_close_status=dict(state='failed',checked_ms=stamp(),message=str(exc))
            self.stopped.wait(2 if lane=='account' else 3)


class NativePairRuntime:
    def _native_connection_pending(self):
        return (getattr(self,'connection_attempt',None) or {}).get('state')=='connecting'

    def _connection_status(self):
        with self.lock:
            result=copy.deepcopy(getattr(self,'connection_attempt',None))
            if result and result['state']=='connecting':
                result['elapsed_ms']=round((time.monotonic()-self._connection_started)*1000)
            return result

    def _native_connect(self,config):
        c=copy.deepcopy(config);c['symbol']=short_symbol(c)
        c['costs'].update(usdt_usd=1.,usdt_usd_auto=False,binance_fee_auto=False)
        mode=c['execution']['mode'];terminal=None;reader=None;installed=False;pump_installed=False
        with self.lock:
            if self._native_connection_pending(): raise ValueError('双品种行情仍在连接，请等待当前检查完成')
            if self.engine.state.get('enabled'): raise ValueError('先暂停新开仓，再重新连接')
            if any(g['key']!=pair_key(c) for g in self.engine.active()): raise ValueError('仍有其他组合未平仓，禁止切换连接')
            if c['mt5']['adapter']!='native': raise ValueError('同一 MT5 双品种需要 Windows 原生终端；可使用真实行情进行纸面验证')
            self._connection_started=time.monotonic()
            self.connection_attempt=dict(state='connecting',stage='准备连接',started_ms=stamp(),elapsed_ms=0,timings=[])
        deadline=self._connection_started+25
        def remaining():
            seconds=deadline-time.monotonic()
            if seconds<=0: raise ValueError('双品种连接检查累计超过 25 秒')
            return min(12,seconds)
        def stage(label,work):
            with self.lock: self.connection_attempt['stage']=label
            remaining();started=time.monotonic()
            try: return work()
            finally:
                with self.lock:
                    self.connection_attempt['timings'].append(dict(stage=label,elapsed_ms=round((time.monotonic()-started)*1000)))
        try:
            # Slow native reads must not hold the trading/status lock. Keep any
            # existing connection managing its owned positions until all new
            # sessions, both specs and the first pair quote have been prepared.
            terminal=stage('初始化 MT5 执行会话',lambda:Terminal(settings_for(c),c['execution']['magic'],timeout_seconds=remaining()))
            specs={leg:stage(label,lambda name=name:terminal.call('pair_spec',symbol=name,timeout_seconds=remaining()))
                   for leg,name,label in (('binance',short_symbol(c),'核验品种 A 规格'),('mt5',c['mt5']['symbol'],'核验品种 B 规格'))}
            plan=equal_lot_plan(c,specs)
            snap=stage('读取账户与交易权限',lambda:terminal.call('snapshot',timeout_seconds=remaining()))
            if snap['account']['currency']!='USD' or snap['account']['margin_mode']!=2:
                raise ValueError('当前仅支持 USD 对冲账户，无法按票据安全管理其他账户模式')
            if mode=='live' and not snap['allowed']: raise ValueError('MT5 自动交易权限未开启')
            reader=stage('初始化 MT5 行情会话',lambda:Terminal(settings_for(c),c['execution']['magic'],read_only=True,timeout_seconds=remaining()))
            pump=NativePairPump(self,reader,c)
            quote=stage('读取双品种首轮报价',lambda:pump.refresh(publish=False,timeout_seconds=remaining()))
            remaining()
            with self.lock:
                if self.stop_event.is_set(): raise ValueError('程序正在退出，连接已取消')
                if self.engine.state.get('enabled'): raise ValueError('自动开仓状态发生变化，请先暂停后重新连接')
                if any(g['key']!=pair_key(c) or g.get('leg_contracts')!=plan['leg_contracts'] for g in self.engine.active()):
                    raise ValueError('未平交易组与当前组合或合约大小不符，禁止重新绑定')
                self.connection_attempt['stage']='启用行情与后台观察'
                # Keep the previous connection intact if storage is unavailable.
                self.engine.pause('双品种已连接；启动新开仓前请核对持仓')
                self.store.sample(quote)
                self._disconnect_pair_resources();installed=True
                self.config=c;self.plan=plan;self.terminal=terminal;self.spec=None;self.binance=None
                self._native_snapshots={};self._last_mt5_snapshot=snap;self._last_mt5_snapshot_at=time.monotonic();self._mt5_risk_error=''
                self.engine.broker=PaperBroker() if mode=='paper' else NativePairBroker(terminal,plan['leg_contracts'])
                self.engine.quote_provider=self._native_execution_quote
                self.engine.close_quote_provider=self._native_cached_quote
                self.engine.entry_preflight=self._native_entry_guard if mode=='live' else None
                self.quote=quote;self.last_error=''
                self.engine.state.pop('entry_margin',None)
                self.position_report=None;self.adoption_preview=None;self.external_close_status=None
                self.market_meta=dict(pair=pair_info(c),strategy_trigger='双品种报价驱动；同一 MT5 请求由持久会话有序发送')
                self.latency.reset(mode);PROCESS_ENTRY_OBSERVATIONS.reset(mode);self._restore_push_bridge()
                self.pump=pump;pump_installed=True;self.pump.bridge=self.push_bridge
                self.pump.publish(quote);self.pump.start()
                if mode=='live':
                    self.maintenance=NativePairObserver(self);self.maintenance.start()
                self.connected=True;self.reconciled=mode=='paper'
                self.connection_attempt.update(state='connected',stage='双品种行情已连接',elapsed_ms=round((time.monotonic()-self._connection_started)*1000))
                self.engine.save('native_pair_connect',self._connection_status())
                return self.snapshot()
        except Exception as exc:
            with self.lock:
                message='双品种连接失败（'+self.connection_attempt['stage']+'）：'+str(exc)
                self.connection_attempt.update(state='failed',message=message,elapsed_ms=round((time.monotonic()-self._connection_started)*1000))
                if installed:
                    self._disconnect_pair_resources()
                    try: self.engine.pause(message)
                    except Exception: self.engine.state.update(enabled=False,alarm=message)
                self.last_error=message
                if not self.engine.state.get('enabled'): self.engine.state['alarm']=message
                try: self.engine.save('native_pair_connect',self._connection_status())
                except Exception: pass  # Never skip worker cleanup if the disk is full.
            # Prepared, uninstalled workers have no trading side effects.
            for resource,uninstalled in ((reader,not pump_installed),(terminal,not installed)):
                if resource and uninstalled:
                    try: resource.close()
                    except Exception: pass
            raise ValueError(message) from exc

    def _disconnect_pair_resources(self):
        self.connected=False;self.reconciled=False
        for name in ('maintenance','external_monitor','pump','terminal','market_stream','entry_preflight','risk_monitor'):
            resource=getattr(self,name,None)
            setattr(self,name,None)
            if resource:
                try: resource.close()
                except Exception: pass
        if getattr(self,'pool',None):
            self.pool=None
            self.engine.multi_pair=False
            self.engine.__dict__.pop('entry_candidate_key',None)

    def _native_cached_quote(self):
        q=self.pump.read()[0] if self.pump else self.quote
        return {**q,'execution_quote_source':'cache'} if q else None

    def _native_execution_quote(self):
        q=self._native_cached_quote()
        limits={**self.config['strategy'],'max_quote_age_ms':min(250,self.config['strategy']['max_quote_age_ms'])}
        return q if valid_group_quote(q,pair_key(self.config),limits) else {**self.pump.refresh(publish=False),'execution_quote_source':'refresh'}

    def _native_margin(self):
        c=self.config;lots=max(0,c['strategy']['max_total_lots']-sum(g['lots'] for g in self.engine.active()))
        if lots<=1e-9: return dict(ready=True,state='capacity_full',required=0.,checked_ms=stamp(),message='总手数额度已用满，已有持仓继续管理')
        mode=c['strategy'].get('direction_mode','forward')
        directions=('forward','reverse') if mode=='auto' else (mode,)
        batches=[[self.terminal.call('margin',symbol=name,lots=lots,
                  native_position_side=position_side({'pair':c['pair'],'direction':direction},leg))
                  for leg,name in (('binance',short_symbol(c)),('mt5',c['mt5']['symbol']))]
                  for direction in directions]
        try:
            required=max(sum(float(x['required']) for x in rows) for rows in batches)
            available=min(float(x['available']) for rows in batches for x in rows)
            if not math.isfinite(required) or required<=0 or not math.isfinite(available): raise ValueError()
            return dict(ready=required<=available,state='ready' if required<=available else 'insufficient',lots=lots,
                        required=required,available=available,checked_ms=stamp(),
                        message=f'按双腿剩余额度各 {lots:g} 手估算保证金 {required:.2f} USD，可用 {available:.2f} USD')
        except (TypeError,ValueError,KeyError):
            return dict(ready=False,state='unavailable',checked_ms=stamp(),message='双品种新开仓保证金未能核验；已有持仓仍可管理')

    def _native_entry_guard(self,q,threshold):
        error=self._account_health_error()
        self.engine.entry_preflight_detail=dict(state='waiting' if error else 'ready',reason=error,reason_code='ACCOUNT_UNVERIFIED' if error else '')
        if error: self.engine.pause(error);return False
        return True

    def _native_start(self,current_config):
        with self.lock:
            if self._native_connection_pending(): raise ValueError('双品种行情仍在连接，暂不能启动新开仓')
            if not self.connected or (current_config is not None and self.config_changed(current_config)):
                raise ValueError('请先保存并连接当前组合')
            if not self.reconciled: raise ValueError('请先完成双品种持仓对账')
            if self.config['execution']['mode']=='live':
                self._native_reconcile()
                margin=self._native_margin();self.engine.state['entry_margin']=margin
                if not margin['ready']: raise ValueError(margin['message'])
            self.engine.start(self.config)
            return self.snapshot()

    def _native_reconcile(self):
        with self.lock:
            if not self.connected: raise ValueError('请先连接双品种行情')
            if self.config['execution']['mode']=='paper':
                for g in self.engine.active(): self.engine.resolve(g,force=True)
                self.engine.state['recovery']=any(self.engine.uncertain(g) for g in self.engine.active())
                self.reconciled=not self.engine.state['recovery'];self.engine.save('paper_reconciled')
                return self.snapshot()
            self.reconciled=False;c=self.config
            if self.engine.state.get('enabled'): self.engine.pause('正在重新核对 MT5 双品种持仓，新开仓暂停')
            for g in self.engine.active(): self.engine.resolve(g,force=True)
            snapshots={leg:self.terminal.call('snapshot',symbol=name) for leg,name in
                       (('binance',short_symbol(c)),('mt5',c['mt5']['symbol']))}
            self.position_report=dict(time_ms=stamp(),status='已读取，等待核对',pair=pair_info(c),
                                      native_legs=snapshots,binance=[],binance_orders=[],mt5=[],
                                      symbol=short_symbol(c),mt5_symbol=c['mt5']['symbol'])
            for leg,snap in snapshots.items():
                if snap['orders']: raise ValueError('组合品种存在未完成委托，禁止启动')
                expected={};identities={}
                for g in self.engine.active():
                    if self.engine.uncertain(g): raise ValueError('原订单成交尚未确认，继续核对原订单')
                    view,vg=leg_view(self.engine.state,g,leg)
                    for o in view['orders']:
                        p=str(o.get('position') or o['result'].get('position') or '')
                        if not p and o['action']=='open' and o['result'].get('qty',0)>0:
                            matches=[x for x in snap['positions'] if x['comment']==o['id'] and x['magic']==c['execution']['magic']]
                            if len(matches)==1: p=str(matches[0]['ticket'])
                        if o['result'].get('qty',0)>0 and not p: raise ValueError('MT5 持仓票据尚未唯一确认')
                        expected[p]=expected.get(p,0)+(1 if o['action']=='open' else -1)*o['result'].get('qty',0)
                        if o['action']=='open' and o['result'].get('qty',0)>0:
                            identities[p]=(o['id'],o['result'].get('position_identifier'),o.get('native_position_side',0))
                current={str(p['ticket']):p for p in snap['positions']}
                for p in snap['positions']:
                    opening,identifier,side=identities.get(str(p['ticket']),('',None,None))
                    if p['magic']!=c['execution']['magic'] or p['side']!=side:
                        raise ValueError('组合品种存在未归属或其他方向持仓；不会自动接管或平仓')
                    if not opening or p.get('comment')!=opening or (identifier and str(p.get('identifier'))!=str(identifier)):
                        raise ValueError('MT5 持仓开仓归属或不可变标识不符')
                    if abs(expected.get(str(p['ticket']),0)-p['lots'])>1e-8: raise ValueError('平台手数与本机记录不一致，等待后台成交回补')
                if any(value>1e-8 and ticket not in current for ticket,value in expected.items()):
                    raise ValueError('平台仓位减少，等待后台成交历史核对；不会直接抹平记录')
            self._native_snapshots=snapshots;self._last_mt5_snapshot=snapshots['mt5'];self._last_mt5_snapshot_at=time.monotonic()
            self.engine.state['recovery']=False;self.reconciled=True;self.position_report['status']='双品种持仓对账通过'
            for g in self.engine.active():
                if max(self.engine.amounts(g).values())<1e-8: self.engine.finalize_flat(g,self.quote)
            self.engine.save('reconciled');return self.snapshot()

    def _native_poll(self):
        q,version,error=self.pump.read()
        self.engine.reconcile_pending(self.config)
        if error: raise ValueError(error)
        if not q: return
        self.quote=q;c=self.config
        if c['execution']['mode']=='live' and not self.reconciled:
            self.engine.observe_exit_wait(c,q,'RECOVERY_PENDING');return
        error=self._account_health_error() if c['execution']['mode']=='live' else ''
        if error and self.engine.state.get('enabled'): self.engine.pause(error)
        self.latency.quote(q,stamp());started=time.monotonic()
        try: self.engine.tick(c,self.plan,q)
        finally: self.latency.record('strategy_pass_ms',(time.monotonic()-started)*1000)
