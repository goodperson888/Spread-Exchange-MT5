"""Multi-terminal gold pool: cached selection, immutable two-leg ownership.

Execution sessions never share read-only quote/account/history work. One engine
and journal own all groups; pool selection is permission for NEW groups only.
"""
import copy
import math
import threading
import time
from types import SimpleNamespace

from mt5_pool_config import (candidates, clean_pool, endpoint_ref, endpoint_settings, fingerprint)
from pair_instruments import equal_lot_plan, pair_info, position_side, direction_quote
from trading_config import pair_key
from trading_engine import stamp, valid_group_quote
from trading_brokers import Terminal, PaperBroker
from market_push import TickFreshness
from native_pair import NativePairBroker, NativePairObserver
import external_closes

LEGS=('binance','mt5')


def owned_on(engine, endpoint):
    identity=endpoint_ref(endpoint)
    for g in engine.state['groups']:
        if g['status']=='closed': continue
        for leg,ref in g.get('pair',{}).get('endpoints',{}).items():
            if ref['id']==identity['id'] and ref['identity']==identity['identity']:
                yield g,leg


def endpoint_exposure_matches(engine, endpoint, snapshot):
    if snapshot.get('orders'): return False
    current={str(p['ticket']):p for p in snapshot['positions']}
    if len(current)!=len(snapshot['positions']): return False
    expected=set()
    for g,leg in owned_on(engine,endpoint):
        if engine.uncertain(g): return False
        qty=engine.amounts(g)[leg]
        if qty<1e-8: continue
        openings=[o for o in engine.state['orders'] if o['group']==g['id'] and o['leg']==leg
                  and o['action']=='open' and o['result'].get('qty',0)>0]
        if len(openings)!=1: return False
        o=openings[0];ticket=external_closes.opening_position(o,current,g['magic'])
        p=current.get(ticket)
        if (not p or ticket in expected or p.get('symbol')!=o['symbol']
                or p.get('side')!=o['native_position_side'] or p.get('magic')!=g['magic']
                or p.get('comment')!=o['id'] or abs(float(p['lots'])-qty)>1e-8
                or (o['result'].get('position_identifier') and
                    str(p.get('identifier'))!=str(o['result']['position_identifier']))): return False
        expected.add(ticket)
    return set(current)==expected


def pool_exposure_matches(engine, config, snapshots):
    return all(endpoint_exposure_matches(engine,e,snapshots[leg])
               for leg,e in config['pair']['endpoints'].items())


class PoolBroker:
    def __init__(self, sessions, endpoints):
        self.sessions=sessions;self.endpoints={e['id']:endpoint_ref(e) for e in endpoints}
    def _call(self, command, order):
        ref=order.get('endpoint',{});eid=ref.get('id')
        if eid not in self.endpoints or ref!=self.endpoints[eid] or order['symbol']!=ref['symbol']:
            raise ValueError('订单的原终端身份不符，禁止转发到其他账户')
        factor=order['contract_size']
        if not isinstance(factor,(int,float)) or not math.isfinite(factor) or factor<=0:
            raise ValueError('订单原合约大小无效')
        broker=NativePairBroker(self.sessions[eid],{order['leg']:factor})
        return getattr(broker,command)(order)
    def submit(self,order): return self._call('submit',order)
    def query(self,order): return self._call('query',order)
    def cancel(self,order): return None


class EngineScope:
    """Observers see only their own active pair; all writes use the one journal."""
    def __init__(self,engine,key): self.base=engine;self.key=key
    def __getattr__(self,name):
        if name=='commission_reference': return getattr(self.base,'commission_references',{}).get(self.key,{})
        return getattr(self.base,name)
    @property
    def commission_reference(self): return getattr(self.base,'commission_references',{}).get(self.key,{})
    @commission_reference.setter
    def commission_reference(self,value):
        if not hasattr(self.base,'commission_references'): self.base.commission_references={}
        self.base.commission_references[self.key]=value
    def active(self): return [g for g in self.base.active() if g['key']==self.key]


class PoolPump:
    def __init__(self,owner):
        self.owner=owner;self.runtime=owner.runtime;self.stopped=threading.Event();self.lock=threading.RLock()
        self.books={};self.errors={};self.quotes={};self.version=0;self.threads=[];self.readers={}
        self.pending_samples={};self.sample_wake=threading.Event();self.sampling_error=''
        self.quote_wakes={e['id']:threading.Event() for e in owner.endpoints}
        self.mt5_transport='独立 MT5 报价会话';self.binance_transport='独立 MT5 报价会话'
    def start(self):
        writer=threading.Thread(target=self._samples,name='pool-chart-history',daemon=True)
        self.threads.append(writer);writer.start()
        for e in self.owner.endpoints:
            t=threading.Thread(target=self._loop,args=(e,),name='pool-quotes-'+e['id'],daemon=True)
            self.threads.append(t);t.start()
    def close(self):
        self.stopped.set();self.sample_wake.set()
        for wake in self.quote_wakes.values():wake.set()
        with self.lock: readers=list(self.readers.values());self.readers.clear()
        for r in readers: r.close()
        for thread in self.threads:
            if thread is not threading.current_thread(): thread.join(timeout=1)
    def all(self):
        with self.lock: return copy.deepcopy(self.quotes)
    def read(self):
        with self.lock: return self.quotes.get(self.owner.chart_key),self.version,''
    def ingest(self,e,book):
        chart_quote=None
        with self.lock:
            if self.stopped.is_set(): return
            self.errors.pop(e['id'],None)
            if self.books.get(e['id'])==book: return
            self.books[e['id']]=book
            for edge,ctx in self.owner.contexts.items():
                endpoints=ctx.config['pair']['endpoints']
                if e['id'] not in {x['id'] for x in endpoints.values()}: continue
                if not all(x['id'] in self.books for x in endpoints.values()): continue
                books={leg:copy.deepcopy(self.books[x['id']]) for leg,x in endpoints.items()}
                q=dict(key=pair_key(ctx.config),time_ms=stamp(),usdt_usd=1.,pair=pair_info(ctx.config),**books,
                       entry=books['binance']['bid']-books['mt5']['ask'],exit=books['binance']['ask']-books['mt5']['bid'])
                previous=self.quotes.get(edge)
                if previous: q['time_ms']=max(q['time_ms'],previous['time_ms']+1)
                self.quotes[edge]=q;ctx.quote=q;self.version+=1
                self.pending_samples[edge]=q
                if edge==self.owner.chart_key:chart_quote=q
        if chart_quote:self.runtime.quote_events.publish(chart_quote)
        self.runtime.market_wake.set()
        self.sample_wake.set()
    def _samples(self):
        while not self.stopped.is_set():
            self.sample_wake.wait(.1);self.sample_wake.clear()
            with self.lock:pending=self.pending_samples;self.pending_samples={}
            for q in pending.values():
                try:self.runtime.store.sample(q)
                except Exception as exc:self.sampling_error=str(exc);self.runtime.market_wake.set()
            self.stopped.wait(.1)
    def _loop(self,e):
        freshness=TickFreshness();settings=endpoint_settings(self.owner.config,e)
        wake=self.quote_wakes[e['id']]
        while not self.stopped.is_set():
            wake.clear()
            try:
                bridge=getattr(self.runtime,'push_bridge',None)
                book=bridge.quote(settings,self.owner.config['strategy']['max_quote_age_ms']) if bridge else None
                if book is None:
                    reader=self.readers.get(e['id'])
                    if reader is None:
                        reader=Terminal(settings,self.owner.config['execution']['magic'],read_only=True)
                        with self.lock:
                            if self.stopped.is_set(): reader.close();return
                            self.readers[e['id']]=reader
                    book=freshness.observe(reader.call('quote',symbol=e['symbol'])['quote'],settings)
                    book['transport']='独立原生轮询'
                self.ingest(e,book)
            except Exception as exc:
                with self.lock:
                    self.errors[e['id']]=str(exc);reader=self.readers.pop(e['id'],None)
                if reader: reader.close()
                self.runtime.market_wake.set();self.stopped.wait(1)
            wake.wait(.05)


class PoolOwner:
    def __init__(self,runtime,config,sessions,specs,snapshots):
        self.runtime=runtime;self.config=config;self.endpoints=clean_pool(config['mt5_pool'])['endpoints']
        self.sessions=sessions;self.specs=specs;self.health={};self.contexts={};self.stopped=threading.Event()
        self.health_readers={};self.resource_lock=threading.Lock();self.health_threads=[]
        self.chart_key=config['mt5_pool']['chart_pair'];self.ranking=[]
        for edge,c in candidates(config).items():
            plan=equal_lot_plan(c,{leg:specs[e['id']] for leg,e in c['pair']['endpoints'].items()})
            ctx=SimpleNamespace(config=c,plan=plan,engine=EngineScope(runtime.engine,pair_key(c)),
                lock=runtime.lock,connected=True,reconciled=config['execution']['mode']=='paper',
                quote=None,market_meta={},market_wake=runtime.market_wake,_native_snapshots={},
                external_close_status=None,pool_owner=self)
            ctx.maintenance=NativePairObserver(ctx);self.contexts[edge]=ctx
        self.pump=PoolPump(self)
    def start(self):
        self.pump.start()
        if self.config['execution']['mode']=='live':
            for ctx in self.contexts.values():
                # Account/margin snapshots are shared once per terminal, separate
                # from pair-specific exact-ticket history and fee verification.
                for lane in ('history','commission'):
                    t=threading.Thread(target=ctx.maintenance._loop,args=(lane,),name='pool-'+lane,daemon=True)
                    ctx.maintenance.threads.append(t);t.start()
            for e in self.endpoints:
                t=threading.Thread(target=self._health_loop,args=(e,),name='pool-health-'+e['id'],daemon=True)
                self.health_threads.append(t);t.start()
    def close(self):
        self.stopped.set();self.pump.close()
        for ctx in self.contexts.values(): ctx.connected=False;ctx.maintenance.close()
        with self.resource_lock: readers=list(self.health_readers.values());self.health_readers.clear()
        for r in readers:r.close()
        for r in self.sessions.values():r.close()
    def _health_loop(self,e):
        r=self.runtime
        while not self.stopped.is_set():
            reader=None
            try:
                with r.lock: token=self._token(e)
                reader=self.health_readers.get(e['id'])
                if reader is None:
                    reader=Terminal(endpoint_settings(self.config,e),self.config['execution']['magic'],read_only=True)
                    with self.resource_lock:
                        if self.stopped.is_set():reader.close();return
                        self.health_readers[e['id']]=reader
                snap=reader.call('snapshot',symbol=e['symbol'])
                margins={side:reader.call('margin',symbol=e['symbol'],lots=self.config['strategy']['mt5_lots'],native_position_side=side)
                         for side in (0,1)}
                with r.lock:
                    if self.stopped.is_set() or r.pool is not self: return
                    if token==self._token(e):
                        self.health[e['id']]=dict(snapshot=snap,margins=margins,checked_ms=stamp(),error='')
                        if not endpoint_exposure_matches(r.engine,e,snap) and r.engine.state.get('enabled'):
                            r.engine.pause(e['name']+'：平台持仓变化，暂停新开仓并等待历史核对')
                        self.recovery_updated()
            except Exception as exc:
                with r.lock:
                    if self.stopped.is_set(): return
                    self.health[e['id']]=dict(checked_ms=stamp(),error=str(exc))
                with self.resource_lock: reader=self.health_readers.pop(e['id'],None)
                if reader:reader.close()
            self.runtime.market_wake.set();self.stopped.wait(2)
    def _token(self,e):
        return [(g['id'],leg,copy.deepcopy([o for o in self.runtime.engine.state['orders']
                 if o['group']==g['id'] and o['leg']==leg])) for g,leg in owned_on(self.runtime.engine,e)]
    def recovery_updated(self):
        r=self.runtime
        if r.pool is not self: return
        if not r.engine.state.get('recovery'): return
        ready=all(self._health_matches(e) for e in self.endpoints)
        if ready:
            r.engine.state['recovery']=False;r.reconciled=True
            for ctx in self.contexts.values():ctx.reconciled=True
            r.engine.save('pool_reconciled')
    def _health_matches(self,e):
        h=self.health.get(e['id'],{})
        return (not h.get('error') and 0<=stamp()-h.get('checked_ms',0)<=5000 and h.get('snapshot')
                and endpoint_exposure_matches(self.runtime.engine,e,h['snapshot']))
    def block(self,ctx,direction):
        r=self.runtime;c=ctx.config;lots=c['strategy']['mt5_lots']
        for leg,e in c['pair']['endpoints'].items():
            held=sum(r.engine.amounts(g)[l] for g,l in owned_on(r.engine,e))
            if held+lots>e['max_lots']+1e-8:return e['name']+' 手数额度不足'
            if self.config['execution']['mode']=='paper':continue
            if not self._health_matches(e):return e['name']+' 等待有效账户/持仓核验'
            h=self.health[e['id']];snap=h['snapshot'];account=snap.get('account',{})
            if account.get('currency')!='USD' or account.get('margin_mode')!=2 or not snap.get('allowed'):
                return e['name']+' 账户或自动交易权限不符'
            side=position_side({'pair':c['pair'],'direction':direction},leg)
            margin=h['margins'].get(side,{})
            required=margin.get('required');available=margin.get('available')
            if (not all(isinstance(v,(int,float)) and not isinstance(v,bool) and math.isfinite(v) for v in (required,available))
                    or required<=0 or required>available):return e['name']+' 保证金不足或尚未核验'
        return ''
    def rank(self,quotes):
        rows=[]
        for edge,ctx in self.contexts.items():
            c=ctx.config;q=quotes.get(edge);s=c['strategy']
            for direction in ('forward','reverse'):
                projected=direction_quote(q,dict(pair=c['pair'],direction=direction)) if q else None
                threshold=s['entry_spread_usd'] if direction=='forward' else -s.get('entry_lower_usd',-5)
                enabled=s.get('direction_mode','auto') in ('auto',direction)
                reason=''
                if not enabled:reason='此方向已关闭'
                elif not valid_group_quote(q,pair_key(c),s):reason='等待新鲜同步报价'
                elif any(g.get('direction','forward')!=direction for g in ctx.engine.active()):reason='此终端对已有相反方向持仓'
                elif projected['entry']<threshold:reason='未到独立开仓阈值'
                else:reason=self.block(ctx,direction)
                if not reason:
                    active=self.runtime.engine.active()
                    if self.runtime.engine.state.get('recovery'):reason='等待恢复对账'
                    elif any(g['status']!='open' for g in active):reason='等待未完成或异常交易组核对'
                    elif len(active)>=s['max_groups']:reason='全池最大组数已用满'
                    elif sum(g['lots'] for g in active)+ctx.plan['lots']>s['max_total_lots']+1e-9:reason='全池总手数额度不足'
                    elif stamp()-self.runtime.engine.state['last_open_ms']<s['cooldown_seconds']*1000:reason='开仓冷却中'
                gross=projected['entry'] if projected else None
                # Prospective round trip commissions; rebate remains an estimate.
                from commission_reference import fee_rate
                basis=dict(pair=c['pair'],direction=direction,symbol=c['symbol'],mt5_symbol=c['mt5']['symbol'],
                           costs=c['costs'],lots=ctx.plan['lots'],opened_ms=stamp(),
                           commission_reference=ctx.engine.commission_reference.get('legs',{}))
                fee=sum(fee_rate(basis,leg,action)*(1-c['costs'].get('mt5_short_rebate_percent' if leg=='binance' else 'mt5_rebate_percent',0)/100)
                        for leg in LEGS for action in ('open','close'))/ctx.plan['leg_contracts']['mt5']
                endpoints=c['pair']['endpoints'];sell='binance' if direction=='forward' else 'mt5';buy='mt5' if sell=='binance' else 'binance'
                rows.append(dict(edge=edge,key=pair_key(c),direction=direction,buy=endpoints[buy]['name'],sell=endpoints[sell]['name'],
                    spread=gross,threshold=threshold,estimated_roundtrip_cost=fee,score=gross-fee if gross is not None else None,
                    ready=not reason,reason=reason or '报价及账户条件满足',selected=False))
        rows.sort(key=lambda x:(not x['ready'],-(x['score'] if x['score'] is not None else -1e20),x['edge'],x['direction']))
        winner=next((x for x in rows if x['ready']),None)
        if winner:winner['selected']=True
        self.ranking=rows
        return winner


class PoolRuntime:
    def _pool_connect(self,config):
        c=copy.deepcopy(config);c['mt5_pool']=clean_pool(c['mt5_pool'],complete=True)
        from trading_config import validate
        validate(c);sessions={};owner=None;installed=False
        with self.lock:
            if self._native_connection_pending():raise ValueError('终端池正在连接，请稍候')
            if self.engine.state.get('enabled'):raise ValueError('请先暂停新开仓')
            expected={pair_key(x) for x in candidates(c).values()}
            if any(g['key'] not in expected for g in self.engine.active()):raise ValueError('已有组绑定其他终端身份，禁止切换')
            self._connection_started=time.monotonic()
            self.connection_attempt=dict(state='connecting',stage='核验终端池',started_ms=stamp(),elapsed_ms=0,timings=[])
        try:
            specs={};snapshots={};initial={};identities=set()
            for e in c['mt5_pool']['endpoints']:
                with self.lock:self.connection_attempt['stage']='核验 '+e['name']
                terminal=Terminal(endpoint_settings(c,e),c['execution']['magic']);sessions[e['id']]=terminal
                snap=terminal.call('snapshot',symbol=e['symbol']);spec=terminal.call('pair_spec',symbol=e['symbol'])
                account=snap['account']
                if account['currency']!='USD' or account['margin_mode']!=2:raise ValueError(e['name']+' 需要 USD 对冲账户')
                if c['execution']['mode']=='live' and not snap['allowed']:raise ValueError(e['name']+' 未开启自动交易')
                if spec.get('currency_base')!='XAU' and not any(x in spec['name'].upper() for x in ('XAU','GOLD')):
                    raise ValueError(e['name']+' 首版终端池仅支持已核验的黄金品种')
                # Worker also checks configured identity on EVERY call.
                identity=(account.get('login',e['account']),account.get('server',e['server']))
                if identity in identities:raise ValueError('终端池发现重复的实际账户，不能作为独立两腿')
                identities.add(identity);specs[e['id']]=spec;snapshots[e['id']]=snap
                book=terminal.call('quote',symbol=e['symbol'])['quote']
                initial[e['id']]=TickFreshness().observe(book,endpoint_settings(c,e))
            owner=PoolOwner(self,c,sessions,specs,snapshots)
            with self.lock:
                if self.stop_event.is_set() or self.engine.state.get('enabled'):raise ValueError('连接状态已变化，请重试')
                if any(g.get('leg_contracts')!=owner.contexts[next(k for k,v in owner.contexts.items() if pair_key(v.config)==g['key'])].plan['leg_contracts']
                       for g in self.engine.active()):raise ValueError('原交易组的合约大小发生变化，禁止重新绑定')
                self.engine.pause('终端池已连接；核对原账户持仓后可启动')
                self._disconnect_pair_resources()
                self.pool=owner;self.config=c;self.pump=owner.pump;self.terminal=None;self.binance=None
                self.maintenance=owner;self.plan=owner.contexts[owner.chart_key].plan
                self.engine.multi_pair=True
                self.engine.broker=PaperBroker() if c['execution']['mode']=='paper' else PoolBroker(sessions,owner.endpoints)
                self.engine.quote_provider=self._pool_execution_quote;self.engine.close_quote_provider=self._pool_close_quote
                self.engine.entry_preflight=self._pool_entry_guard
                self.connected=True;self.reconciled=c['execution']['mode']=='paper';installed=True
                self.market_meta=dict(pair=pair_info(c),strategy_trigger='独立 MT5 报价驱动；从已核验候选中择优，已有组绑定原终端')
                self._restore_push_bridge()
                self.position_report=None;self.external_close_status=None;self.last_error=''
                self.latency.reset(c['execution']['mode'])
                for e in owner.endpoints:owner.pump.ingest(e,initial[e['id']])
                self.quote=owner.pump.read()[0]
                owner.start();self.connection_attempt.update(state='connected',stage='终端池已连接',elapsed_ms=round((time.monotonic()-self._connection_started)*1000))
                self.engine.save('pool_connected',dict(terminals=len(owner.endpoints),pairs=len(owner.contexts)))
                return self.snapshot()
        except Exception as exc:
            if installed:self._disconnect_pair_resources()
            elif owner:owner.close()
            else:
                for session in sessions.values():session.close()
            with self.lock:
                self.last_error=str(exc);self.connection_attempt.update(state='failed',message=str(exc),stage='终端池连接失败')
            raise
    def _pool_execution_quote(self,group):
        q=self._pool_group_quote(group)
        limits={**group['parameters'],'max_quote_age_ms':min(250,group['parameters']['max_quote_age_ms'])}
        if not valid_group_quote(q,group['key'],limits):return None
        return {**q,'execution_quote_source':'cache'} if q else None
    def _pool_group_quote(self,group):
        return next((q for q in self.pool.pump.all().values() if q['key']==group['key']),None)
    def _pool_close_quote(self,q):
        book=next((x for x in self.pool.pump.all().values() if q and x['key']==q['key']),None)
        return direction_quote(book,dict(pair=self.config['pair'],direction=q.get('_spread_direction','forward'))) if book else None
    def _pool_entry_guard(self,q,threshold):
        ctx=next((c for c in self.pool.contexts.values() if pair_key(c.config)==q['key']),None)
        if not ctx:return False
        direction=q.get('_spread_direction','forward')
        limits={**ctx.config['strategy'],'max_quote_age_ms':min(250,ctx.config['strategy']['max_quote_age_ms'])}
        reason='' if valid_group_quote(q,pair_key(ctx.config),limits) else '等待更新的双边执行报价'
        reason=reason or self.pool.block(ctx,direction)
        self.engine.entry_preflight_detail=dict(state='waiting' if reason else 'ready',reason=reason)
        return not reason
    def _pool_poll(self):
        if self.pool.pump.sampling_error:raise ValueError('行情历史写入失败：'+self.pool.pump.sampling_error)
        quotes=self.pool.pump.all();owner=self.pool
        for edge,ctx in owner.contexts.items():
            self.engine.reconcile_pending(ctx.config)
            q=quotes.get(edge)
            if q:
                for g in ctx.engine.active():
                    if valid_group_quote(q,g['key'],g['parameters']):g['valuation']=self.engine.valuation(g,q)
        self.quote=quotes.get(owner.chart_key)
        if self.config['execution']['mode']=='live' and not self.reconciled:
            for edge,ctx in owner.contexts.items():self.engine.observe_exit_wait(ctx.config,quotes.get(edge),'RECOVERY_PENDING')
            return
        winner=owner.rank(quotes)
        self.engine.entry_candidate_key=winner['key'] if winner else None
        # Manage every fixed pair first. Only then admit at most one ranked entry.
        orders_before=len(self.engine.state['orders'])
        for edge,ctx in owner.contexts.items():
            q=quotes.get(edge)
            if not q:continue
            self.engine.entry_candidate_key=None
            self.engine.tick(ctx.config,ctx.plan,q)
        self.engine.entry_candidate_key=winner['key'] if winner else None
        if winner and len(self.engine.state['orders'])==orders_before:
            ctx=owner.contexts[winner['edge']]
            self.engine.tick(ctx.config,ctx.plan,quotes[winner['edge']])
    def _pool_reconcile(self):
        owner=self.pool;mode=self.config['execution']['mode']
        if self.engine.state.get('enabled'):self.engine.pause('正在核对所有原终端持仓')
        for g in self.engine.active():self.engine.resolve(g,force=True)
        snapshots=[]
        if mode=='live':
            for e in owner.endpoints:
                snap=owner.sessions[e['id']].call('snapshot',symbol=e['symbol'])
                snapshots.append(dict(id=e['id'],name=e['name'],symbol=e['symbol'],snapshot=snap))
                if not endpoint_exposure_matches(self.engine,e,snap):raise ValueError(e['name']+' 仓位或成交尚未对应，等待后台历史核对；不会清除原记录')
        if any(self.engine.uncertain(g) for g in self.engine.active()):raise ValueError('仍有未知成交，继续查询原订单')
        self.engine.state['recovery']=False;self.reconciled=True
        self.position_report=dict(time_ms=stamp(),status='终端池原账户持仓核对通过',pool_endpoints=snapshots)
        for ctx in owner.contexts.values():ctx.reconciled=True
        self.engine.save('pool_reconciled');return self.snapshot()
    def _pool_start(self,current_config):
        with self.lock:
            if not self.connected or not self.pool or (current_config and self.config_changed(current_config)):
                raise ValueError('请先保存并连接当前终端池')
            self._pool_reconcile()
            if self.config['execution']['mode']=='live' and not all(self.pool._health_matches(e) for e in self.pool.endpoints):
                raise ValueError('后台仍在核验账户与保证金，请等待各终端状态更新')
            self.engine.start(self.config);self.market_wake.set();return self.snapshot()
    def _pool_status(self):
        owner=self.pool
        if not owner:return None
        quotes=owner.pump.all();owner.rank(quotes)
        endpoints=[]
        for e in owner.endpoints:
            h=owner.health.get(e['id'],{});s=h.get('snapshot',{});book=owner.pump.books.get(e['id'])
            from liquidation_risk import mt5_risk,DEFAULT_ALERTS,freshness
            risk=mt5_risk(s.get('account'),{**DEFAULT_ALERTS,**self.config.get('risk_alerts',{})})
            risk=freshness(risk,stamp()-h['checked_ms'] if h.get('checked_ms') else None,error=h.get('error',''))
            endpoints.append(dict(id=e['id'],name=e['name'],symbol=e['symbol'],quote=copy.deepcopy(book),risk=risk,
                error=h.get('error') or owner.pump.errors.get(e['id'],''),checked_ms=h.get('checked_ms'),
                account=copy.deepcopy(s.get('account')),allowed=s.get('allowed'),
                remaining_lots=max(0,e['max_lots']-sum(self.engine.amounts(g)[leg] for g,leg in owned_on(self.engine,e)))))
        return dict(endpoints=endpoints,candidates=copy.deepcopy(owner.ranking),chart_pair=owner.chart_key,
                    chart_strategy=copy.deepcopy(owner.contexts[owner.chart_key].config['strategy']),
                    pairs=[dict(id=edge,name=ctx.config['pair']['name']) for edge,ctx in owner.contexts.items()])
