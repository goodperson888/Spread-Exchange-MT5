"""Multi-terminal gold pool: cached selection, immutable two-leg ownership.

Execution sessions never share read-only quote/account/history work. One engine
and journal own all groups; pool selection is permission for NEW groups only.
"""
import copy
import math
import threading
import time
from types import SimpleNamespace

from mt5_pool_config import (candidates, clean_pool, endpoint_ref, endpoint_settings, fingerprint, RULE_FIELDS)
from pair_instruments import equal_lot_plan, pair_info, position_side, direction_quote, entry_ambiguous
from trading_config import pair_key
from trading_engine import stamp, valid_group_quote
from trading_brokers import Terminal, PaperBroker
from market_push import TickFreshness
from quote_fallback import BackgroundRead
from execution_latency import LatencyStats
from entry_observations import PROCESS_ENTRY_OBSERVATIONS
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
        self.fallbacks={}
        self.pending_samples={};self.sample_wake=threading.Event();self.sampling_error=''
        self.quote_wakes={e['id']:threading.Event() for e in owner.endpoints}
        self.edges_by_endpoint={e['id']:[(edge,ctx) for edge,ctx in owner.contexts.items()
            if e['id'] in {x['id'] for x in ctx.config['pair']['endpoints'].values()}] for e in owner.endpoints}
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
        with self.lock:
            readers=list(self.readers.values());self.readers.clear()
            fallbacks=list(self.fallbacks.values())
        for fallback in fallbacks:fallback.close()
        for r in readers: r.close()
        for thread in self.threads:
            if thread is not threading.current_thread(): thread.join(timeout=1)
    def all(self):
        with self.lock: return copy.deepcopy(self.quotes)
    def for_key(self,key):
        with self.lock:return copy.deepcopy(self.quotes.get(self.owner.edges_by_key.get(key)))
    def read(self):
        with self.lock: return self.quotes.get(self.owner.chart_key),self.version,''
    def ingest(self,e,book):
        chart_quote=None
        with self.lock:
            if self.stopped.is_set(): return
            previous_book=self.books.get(e['id'])
            if previous_book:
                old_at=previous_book.get('source_time_ms',previous_book.get('time_ms',0))
                at=book.get('source_time_ms',book.get('time_ms',0))
                if at<old_at:return
            self.errors.pop(e['id'],None)
            if self.books.get(e['id'])==book: return
            self.books[e['id']]=book
            for edge,ctx in self.edges_by_endpoint[e['id']]:
                endpoints=ctx.config['pair']['endpoints']
                if not all(x['id'] in self.books for x in endpoints.values()): continue
                books={leg:copy.deepcopy(self.books[x['id']]) for leg,x in endpoints.items()}
                q=dict(key=pair_key(ctx.config),time_ms=stamp(),usdt_usd=1.,pair=pair_info(ctx.config),**books,
                       entry=books['binance']['bid']-books['mt5']['ask'],exit=books['binance']['ask']-books['mt5']['bid'])
                previous=self.quotes.get(edge)
                if previous: q['time_ms']=max(q['time_ms'],previous['time_ms']+1)
                LatencyStats.published(q)
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
        def read_quote():
            reader=self.readers.get(e['id'])
            if reader is None:
                reader=Terminal(settings,self.owner.config['execution']['magic'],read_only=True)
                with self.lock:
                    if self.stopped.is_set():reader.close();return None
                    self.readers[e['id']]=reader
            try:
                book=freshness.observe(reader.call('quote',symbol=e['symbol'])['quote'],settings,discard_older=True)
                if book is None:return None
                book['transport']='独立原生轮询'
                return book
            except Exception:
                with self.lock:
                    if self.readers.get(e['id']) is reader:self.readers.pop(e['id'],None)
                reader.close()
                raise
        fallback=BackgroundRead(read_quote,wake,'pool-fallback-'+e['id'])
        with self.lock:
            if self.stopped.is_set():fallback.close();return
            self.fallbacks[e['id']]=fallback
        while not self.stopped.is_set():
            wake.clear()
            try:
                bridge=getattr(self.runtime,'push_bridge',None)
                book=bridge.quote(settings,self.owner.config['strategy']['max_quote_age_ms']) if bridge else None
                if book is None:
                    book,error=fallback.read(self.owner.config['execution'].get('mt5_quote_poll_ms',50)/1000)
                    if error:raise error
                if book is not None:
                    book=freshness.observe(book,settings,discard_older=True)
                    if book is not None:self.ingest(e,book)
            except Exception as exc:
                with self.lock:
                    self.errors[e['id']]=str(exc)
                self.runtime.market_wake.set()
            wake.wait(self.owner.config['execution'].get('mt5_quote_poll_ms',50)/1000)


class PoolOwner:
    def __init__(self,runtime,config,sessions,specs,snapshots):
        self.runtime=runtime;self.config=config;self.endpoints=clean_pool(config['mt5_pool'])['endpoints']
        self.sessions=sessions;self.specs=specs;self.health={};self.contexts={};self.stopped=threading.Event()
        self.health_readers={};self.readers={};self.resource_lock=threading.Lock();self.health_threads=[]
        self.chart_key=config['mt5_pool']['chart_pair'];self.ranking=[]
        for edge,c in candidates(config).items():
            plan=equal_lot_plan(c,{leg:specs[e['id']] for leg,e in c['pair']['endpoints'].items()})
            ctx=SimpleNamespace(config=c,plan=plan,engine=EngineScope(runtime.engine,pair_key(c)),
                lock=runtime.lock,connected=True,reconciled=config['execution']['mode']=='paper',
                quote=None,market_meta={},market_wake=runtime.market_wake,_native_snapshots={},
                external_close_status=None,pool_owner=self)
            ctx.maintenance=NativePairObserver(ctx);self.contexts[edge]=ctx
        self.edges_by_key={pair_key(ctx.config):edge for edge,ctx in self.contexts.items()}
        self.pump=PoolPump(self)
    def start(self):
        self.pump.start()
        if self.config['execution']['mode']=='live':
            for lane in ('history','commission'):
                target=self._history_loop if lane=='history' else self._commission_loop
                t=threading.Thread(target=target,name='pool-'+lane,daemon=True)
                self.health_threads.append(t);t.start()
            for e in self.endpoints:
                t=threading.Thread(target=self._health_loop,args=(e,),name='pool-health-'+e['id'],daemon=True)
                self.health_threads.append(t);t.start()
    def close(self):
        self.stopped.set();self.pump.close()
        for ctx in self.contexts.values(): ctx.connected=False;ctx.maintenance.close()
        with self.resource_lock:
            readers=list(self.health_readers.values())+list(self.readers.values())
            self.health_readers.clear();self.readers.clear()
        for r in readers:r.close()
        for r in self.sessions.values():r.close()
    def read_terminal(self,lane,e):
        """History/commission workers scale by endpoint, never by pair count."""
        key=(lane,e['id'])
        with self.resource_lock:
            if self.stopped.is_set():raise ValueError('终端池已断开')
            reader=self.readers.get(key)
        if reader is not None:return reader
        candidate=Terminal(endpoint_settings(self.config,e),self.config['execution']['magic'],read_only=True)
        with self.resource_lock:
            if self.stopped.is_set():candidate.close();raise ValueError('终端池已断开')
            reader=self.readers.setdefault(key,candidate)
        if reader is not candidate:candidate.close()
        return reader
    def discard_reader(self,lane,e):
        with self.resource_lock:reader=self.readers.pop((lane,e['id']),None)
        if reader:
            try:reader.close()
            except Exception:pass
    def _history_loop(self):
        r=self.runtime
        while not self.stopped.is_set():
            with r.lock:
                relevant={g['key'] for g in r.engine.state['groups'] if g.get('mode')=='live'
                    and (g['status']!='closed' or not g.get('costs_verified'))}
                contexts=[c for c in self.contexts.values() if c.engine.key in relevant]
                contexts.sort(key=lambda c:not bool(c.engine.active()))
            for ctx in contexts:
                if self.stopped.is_set():return
                try:ctx.maintenance.step('history')
                except Exception as exc:
                    for e in ctx.config['pair']['endpoints'].values():self.discard_reader('history',e)
                    with r.lock:
                        ctx.external_close_status=dict(state='failed',checked_ms=stamp(),message=str(exc))
            self.stopped.wait(3)
    def _commission_loop(self):
        from commission_reference import safe_references
        from reference_diagnostics import PROCESS_REFERENCE_DIAGNOSTICS
        r=self.runtime
        while not self.stopped.is_set():
            references={}
            for e in self.endpoints:
                if self.stopped.is_set():return
                if not e['fee_auto']:continue
                try:
                    references[e['id']]=self.read_terminal('commission',e).call(
                        'commission_reference',symbol=e['symbol'],lots=self.config['strategy']['mt5_lots'])
                except Exception:
                    self.discard_reader('commission',e)
                    references[e['id']]=dict(symbol=e['symbol'],checked_ms=stamp(),state='unavailable',
                        message='佣金历史读取失败，保留手填估算')
            with r.lock:
                if self.stopped.is_set() or r.pool is not self:return
                for edge,ctx in self.contexts.items():
                    legs={leg:copy.deepcopy(references[e['id']]) for leg,e in ctx.config['pair']['endpoints'].items()
                          if e['id'] in references}
                    ctx.engine.commission_reference=dict(key=ctx.engine.key,legs=legs)
                    ctx.market_meta['mt5_commission_reference']=legs
                    if edge==self.chart_key:PROCESS_REFERENCE_DIAGNOSTICS.commissions=safe_references(legs)
            r.market_wake.set();self.stopped.wait(60)
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
        return self.block_info(ctx,direction)[1]
    def block_info(self,ctx,direction):
        r=self.runtime;c=ctx.config;lots=c['strategy']['mt5_lots']
        for leg,e in c['pair']['endpoints'].items():
            held=sum(r.engine.amounts(g)[l] for g,l in owned_on(r.engine,e))
            if held+lots>e['max_lots']+1e-8:return 'TERMINAL_QUOTA',e['name']+' 手数额度不足'
            if self.config['execution']['mode']=='paper':continue
            if not self._health_matches(e):return 'ACCOUNT_UNVERIFIED',e['name']+' 等待有效账户/持仓核验'
            h=self.health[e['id']];snap=h['snapshot'];account=snap.get('account',{})
            if account.get('currency')!='USD' or account.get('margin_mode')!=2 or not snap.get('allowed'):
                return 'PERMISSION',e['name']+' 账户或自动交易权限不符'
            side=position_side({'pair':c['pair'],'direction':direction},leg)
            margin=h['margins'].get(side,{})
            required=margin.get('required');available=margin.get('available')
            if (not all(isinstance(v,(int,float)) and not isinstance(v,bool) and math.isfinite(v) for v in (required,available))
                    or required<=0 or required>available):return 'MARGIN',e['name']+' 保证金不足或尚未核验'
        return '', ''
    def rank(self,quotes,*,observe=False,recovery=False):
        from entry_confirmation import reached
        rows=[]
        for edge,ctx in self.contexts.items():
            c=ctx.config;q=quotes.get(edge);s=c['strategy'];key=ctx.engine.key
            for direction in ('forward','reverse'):
                projected=direction_quote(q,dict(pair=c['pair'],direction=direction)) if q else None
                threshold=s['entry_spread_usd'] if direction=='forward' else -s.get('entry_lower_usd',-5)
                enabled=s.get('direction_mode','auto') in ('auto',direction)
                confirmations=self.runtime.engine.entry_confirmations
                valid=valid_group_quote(q,key,s)
                if observe:
                    progress=confirmations.observe(key,s,q,direction,
                        -threshold if direction=='reverse' else threshold,valid=valid,
                        enabled=enabled and not recovery and self.runtime.engine.state.get('enabled',False))
                else:
                    progress=confirmations.snapshot(key,max_age_ms=s['max_quote_age_ms']).get(direction,{})
                code='';reason=''
                if not enabled:code,reason='DIRECTION_DISABLED','此方向已关闭'
                elif not valid:code,reason='QUOTE_INVALID','等待新鲜同步报价'
                elif any(g.get('direction','forward')!=direction for g in ctx.engine.active()):code,reason='OPPOSITE_POSITION','此终端对已有相反方向持仓'
                elif entry_ambiguous(c,q,ctx.engine.active()):code,reason='AMBIGUOUS_THRESHOLD','两种买卖组合同时到价，等待报价区分'
                elif not reached(s,direction,q['exit'] if direction=='reverse' else q['entry'],
                                 -threshold if direction=='reverse' else threshold):code,reason='WAIT_THRESHOLD','未到独立开仓阈值'
                else:code,reason=self.block_info(ctx,direction)
                if not reason:
                    active=self.runtime.engine.active()
                    if self.runtime.engine.state.get('recovery'):code,reason='RECOVERY_PENDING','等待恢复对账'
                    elif any(g['status']!='open' for g in active):code,reason='ORDER_PENDING','等待未完成或异常交易组核对'
                    elif len(active)>=s['max_groups']:code,reason='GROUP_LIMIT','全池最大组数已用满'
                    elif sum(g['lots'] for g in active)+ctx.plan['lots']>s['max_total_lots']+1e-9:code,reason='LOTS_LIMIT','全池总手数额度不足'
                    elif stamp()-self.runtime.engine.state['last_open_ms']<s['cooldown_seconds']*1000:code,reason='COOLDOWN','开仓冷却中'
                if not reason and not progress.get('ready',s.get(f'entry_{direction}_confirmation_ticks',1)==1):
                    code,reason='WAIT_CONFIRMATION',f"连续新报价确认 {progress.get('count',0)}/{s.get(f'entry_{direction}_confirmation_ticks',1)}"
                gross=projected['entry'] if projected else None
                # Prospective round trip commissions; rebate remains an estimate.
                fee=self._roundtrip_cost(ctx,direction)
                endpoints=c['pair']['endpoints'];sell='binance' if direction=='forward' else 'mt5';buy='mt5' if sell=='binance' else 'binance'
                rows.append(dict(edge=edge,key=key,direction=direction,buy=endpoints[buy]['name'],sell=endpoints[sell]['name'],
                    spread=gross,threshold=threshold,estimated_roundtrip_cost=fee,score=gross-fee if gross is not None else None,
                    confirmation=progress,ready=not reason,reason=reason or '报价及账户条件满足',reason_code=code or 'CANDIDATE_READY',selected=False))
        rows.sort(key=lambda x:(not x['ready'],-(x['score'] if x['score'] is not None else -1e20),x['edge'],x['direction']))
        winner=next((x for x in rows if x['ready']),None)
        if winner:winner['selected']=True
        self.ranking=rows
        if observe:
            for row in rows:
                code=('RECOVERY_PENDING' if recovery else 'ENTRY_PAUSED' if not self.runtime.engine.state.get('enabled') else
                      row['reason_code'] if not row['ready'] else 'OTHER_CANDIDATE' if not row['selected'] else None)
                if code:
                    self.runtime.engine.observe_entry(self.contexts[row['edge']].config,quotes.get(row['edge']),code,
                                                      direction=row['direction'],stage='pool_rank')
        return winner

    @staticmethod
    def _roundtrip_cost(ctx,direction):
        from commission_reference import fee_rate, MAX_AGE_MS, finite
        c=ctx.config;now=stamp();refs=ctx.engine.commission_reference.get('legs',{})
        # Reference eligibility can change at its future/expiry boundary even
        # without a replacement snapshot. Preserve those exact fee gates.
        eligible=tuple(finite(refs.get(leg,{}).get('checked_ms')) and
                       0<=now-refs[leg]['checked_ms']<=MAX_AGE_MS for leg in LEGS)
        key=(c['symbol'],c['mt5']['symbol'],ctx.plan['lots'],ctx.plan['leg_contracts']['mt5'],
             c['costs'],refs,eligible)
        cache=getattr(ctx,'_fee_cache',{})
        previous=cache.get(direction)
        if previous and previous[0]==key:return previous[1]
        basis=dict(pair=c['pair'],direction=direction,symbol=c['symbol'],mt5_symbol=c['mt5']['symbol'],
                   costs=c['costs'],lots=ctx.plan['lots'],opened_ms=now,commission_reference=refs)
        fee=sum(fee_rate(basis,leg,action)*(1-c['costs'].get(
            'mt5_short_rebate_percent' if leg=='binance' else 'mt5_rebate_percent',0)/100)
                for leg in LEGS for action in ('open','close'))/ctx.plan['leg_contracts']['mt5']
        cache[direction]=(copy.deepcopy(key),fee);ctx._fee_cache=cache
        return fee


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
                # Broker-specific symbols such as GC are allowed after the
                # worker's USD-linear P/L, contract and trading checks pass.
                base=str(spec.get('currency_base','')).upper(); name=str(spec.get('name',e['symbol'])).upper()
                if base not in ('XAU','USD') and not any(token in name for token in ('XAU','GOLD','GC')):
                    raise ValueError(e['name']+'：终端池品种需是可核验的黄金现货或黄金期货（例如 XAUUSD、GC）')
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
                PROCESS_ENTRY_OBSERVATIONS.reset(c['execution']['mode'])
                self.engine.entry_confirmations.clear()
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
        return self.pool.pump.for_key(group['key'])
    def _pool_close_quote(self,q):
        book=self.pool.pump.for_key(q['key']) if q else None
        return direction_quote(book,dict(pair=self.config['pair'],direction=q.get('_spread_direction','forward'))) if book else None
    def _pool_entry_guard(self,q,threshold):
        ctx=next((c for c in self.pool.contexts.values() if pair_key(c.config)==q['key']),None)
        if not ctx:
            self.engine.entry_preflight_detail=dict(state='waiting',reason='等待有效终端对',reason_code='ACCOUNT_UNVERIFIED')
            return False
        direction=q.get('_spread_direction','forward')
        limits={**ctx.config['strategy'],'max_quote_age_ms':min(250,ctx.config['strategy']['max_quote_age_ms'])}
        code,reason=('', '') if valid_group_quote(q,pair_key(ctx.config),limits) else ('EXECUTION_QUOTE_STALE', '等待更新的双边执行报价')
        if not reason:code,reason=self.pool.block_info(ctx,direction)
        self.engine.entry_preflight_detail=dict(state='waiting' if reason else 'ready',reason=reason,reason_code=code)
        return not reason
    def _pool_poll(self):
        prepared=time.monotonic();latency=getattr(self,'latency',None)
        if self.pool.pump.sampling_error:raise ValueError('行情历史写入失败：'+self.pool.pump.sampling_error)
        quotes=self.pool.pump.all();owner=self.pool
        active_keys={g['key'] for g in self.engine.active()}
        managed=[(edge,ctx) for edge,ctx in owner.contexts.items() if ctx.engine.key in active_keys]
        for edge,ctx in managed:
            self.engine.reconcile_pending(ctx.config)
            q=quotes.get(edge)
            if q:
                for g in ctx.engine.active():
                    if valid_group_quote(q,g['key'],g['parameters']):g['valuation']=self.engine.valuation(g,q)
        self.quote=quotes.get(owner.chart_key)
        if latency:latency.record('pool_prepare_ms',(time.monotonic()-prepared)*1000)
        if self.config['execution']['mode']=='live' and not self.reconciled:
            owner.rank(quotes,observe=True,recovery=True)
            for edge,ctx in owner.contexts.items():self.engine.observe_exit_wait(ctx.config,quotes.get(edge),'RECOVERY_PENDING')
            return
        ranked=time.monotonic()
        try:winner=owner.rank(quotes,observe=True)
        finally:
            if latency:latency.record('pool_rank_ms',(time.monotonic()-ranked)*1000)
        self.engine.entry_candidate_key=winner['key'] if winner else None
        # Manage every fixed pair first. Only then admit at most one ranked entry.
        orders_before=len(self.engine.state['orders'])
        for edge,ctx in managed:
            q=quotes.get(edge)
            if not q:continue
            self.engine.entry_candidate_key=None
            self._pool_tick(ctx,q)
        self.engine.entry_candidate_key=winner['key'] if winner else None
        if winner and len(self.engine.state['orders'])==orders_before:
            ctx=owner.contexts[winner['edge']]
            self._pool_tick(ctx,quotes[winner['edge']])
        elif winner:
            self.engine.observe_entry(owner.contexts[winner['edge']].config,quotes.get(winner['edge']),'EXIT_PROCESSING',direction=winner['direction'])
    def _pool_tick(self,ctx,q):
        latency=getattr(self,'latency',None)
        if latency:latency.quote(q,stamp())
        started=time.monotonic()
        try:self.engine.tick(ctx.config,ctx.plan,q)
        finally:
            if latency:latency.record('strategy_pass_ms',(time.monotonic()-started)*1000)
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
                    pairs=[dict(id=edge,name=ctx.config['pair']['name'],rules={k:ctx.config['strategy'][k] for k in RULE_FIELDS if k in ctx.config['strategy']}) for edge,ctx in owner.contexts.items()])
