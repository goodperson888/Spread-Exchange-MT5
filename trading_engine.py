"""Durable paired execution. Order intent is committed before every broker write."""
import copy
import time
import uuid
from trading_config import pair_key
from execution_evidence import reason_code, REASON_LABELS, result_evidence, validation_issues


def stamp(): return int(time.time()*1000)


def valid_group_quote(q, key, s, now=None):
    now=now or stamp()
    if not q or q.get('key')!=key: return False
    for side in ('binance','mt5'):
        x=q.get(side,{})
        if not 0<float(x.get('bid',0))<=float(x.get('ask',0)): return False
        observed=int(x.get('observed_ms',x.get('time_ms',0)))
        if not -1000<=now-observed<=s['max_quote_age_ms']: return False
    binance_observed=int(q['binance'].get('observed_ms',q['binance'].get('time_ms',0)))
    mt5_observed=int(q['mt5'].get('observed_ms',q['mt5'].get('time_ms',0)))
    return abs(binance_observed-mt5_observed)<=s['max_clock_skew_ms']


def valid_quote(q, c, now=None):
    return valid_group_quote(q,pair_key(c),c['strategy'],now)


class Engine:
    # A lost HTTP response does not mean that the exchange rejected the order.
    # Retry status reads with a short, bounded backoff instead of querying on
    # every quote tick (which can both waste REST quota and still race the
    # exchange's order index).
    RECONCILE_DELAYS_MS = (100, 300, 700, 1200)
    QUERY_TIMEOUT_MS = 1000
    RECENT_RECONCILE_MS = 10000
    RECONCILE_LOG_INTERVAL_MS = 30000

    def __init__(self, store, broker):
        self.store,self.broker=store,broker
        # Deliberately not persisted: a restart or manual pause must never
        # resume an interrupted entry, even if the wall clock moved backwards.
        self._entry_deadlines={}
        self._execution_timers={}
        self._reconcile_notices={}
        self.state=store.load() or dict(groups=[],orders=[],last_open_ms=0,enabled=False,mode='paper',key=None,alarm='',recovery=False)
        self.state['enabled']=False
        self.state['boot_ms']=stamp()
        for g in self.state['groups']:
            if g.get('imported'): g['management_enabled']=False
            if (g.get('status')=='closed' and g.get('valuation',{}).get('pnl_schema_version')!=2
                    and not self.uncertain(g) and max(self.amounts(g).values())<1e-8):
                g['valuation']=self.valuation(g,dict(binance=dict(ask=0),mt5=dict(bid=0)))
        self.state['recovery']=any(g['status']!='closed' for g in self.state['groups'])
        if self.state['recovery']: self.state['alarm']='程序重启，须连接原账户并完成持仓对账后恢复管理'
        self.save('boot')

    def save(self, kind, data=None): self.store.commit(self.state,kind,data)

    def _latency(self, phase, value):
        latency = getattr(self, 'latency', None)
        if latency is not None: latency.record(phase, value)

    def _observe_exit(self, g, q, code=None, **evidence):
        observer = getattr(self, 'exit_observations', None)
        if observer is not None: observer.observe(g, q, code, **evidence)

    def observe_exit_wait(self, c, q, code):
        if getattr(self, 'exit_observations', None) is None: return
        for g in self.active():
            self._observe_exit(g, q, code, limits=c['strategy'], expected_key=pair_key(c))

    def active(self): return [g for g in self.state['groups'] if g['status']!='closed']

    def pause(self, reason='已暂停新开仓；已有仓位继续管理', keep_entry=False):
        if not keep_entry: self._entry_deadlines.clear()
        code=reason_code(reason)
        evidence={'time_ms':stamp(),'reason_code':code,'reason_label':REASON_LABELS[code],
                  'enabled_before':bool(self.state.get('enabled')),'keep_entry':bool(keep_entry)}
        self.state.update(enabled=False,alarm=reason,last_pause=evidence)
        self.save('pause',{'reason':reason,**evidence})

    def start(self, c):
        if self.state['recovery']: raise ValueError('请先完成持仓对账')
        if any(g['status'] in ('opening','unwinding','closing','attention') for g in self.active()):
            raise ValueError('尚有未完成或异常交易组')
        self.state.update(enabled=True,key=pair_key(c),mode=c['execution']['mode'],alarm='')
        self.state['last_start_ms']=stamp()
        self.save('start',{'mode':self.state['mode']})

    def amounts(self, g):
        amounts={'binance':0.,'mt5':0.}
        for o in self.state['orders']:
            if o['group']==g['id']:
                amounts[o['leg']]+=(1 if o['action']=='open' else -1)*o.get('result',{}).get('qty',0)
        return {k:max(0,v) for k,v in amounts.items()}

    def uncertain(self, g):
        return [o for o in self.state['orders'] if o['group']==g['id'] and o.get('result',{}).get('status')!='done']

    def _waiting_second_confirmation(self, g):
        """Allow one native MT5 acknowledgement race within the original window.

        A successful acknowledgement and visible matching deal are permission
        to keep reading the original order, never proof of a complete fill.
        Manual pause/restart invalidates this process-local permission.
        """
        deadline=self._entry_deadlines.get(g['id'])
        if (g.get('status')!='opening' or not self.state['enabled'] or self.state['recovery']
                or deadline is None or time.monotonic()>=deadline
                or stamp()-g['opened_ms']>=g['parameters']['max_unhedged_ms']
                or g.get('second_leg')!='mt5'):
            return False
        orders=[o for o in self.state['orders'] if o['group']==g['id'] and o['action']=='open']
        first=next((o for o in orders if o['leg']==g.get('first_leg')),None)
        second=next((o for o in orders if o['leg']=='mt5'),None)
        if not first or not second: return False
        r=second['result'];ack=second.get('initial_result',{});proof=ack.get('confirmation',{})
        return (first['result'].get('status')=='done' and first['result'].get('qty')==g['qty']
                and r.get('status')=='unknown' and r.get('qty')==0
                and ack.get('valid') is True and ack.get('retcode')==10009
                and ack.get('ack_volume')==g['lots'] and ack.get('ack_price')==0
                and bool(r.get('ticket'))
                and proof.get('stage')=='order_pending' and proof.get('position_matches')==1
                and proof.get('deal_matches')==1
                and all(proof.get(key)==1 for key in
                        ('ticket_matches','symbol_matches','magic_matches','side_matches','entry_matches')))

    def order(self, g, leg, action, qty, q):
        if qty<=1e-9: return
        side=q[leg]; sell=(leg=='binance')==(action=='open')
        reference=side['bid' if sell else 'ask']
        if g.get('status')=='unwinding':
            # Groups created by an older build do not have the dedicated
            # unwind field; fall back to their saved normal budget safely.
            budget=g['parameters'].get('unwind_slippage_usd', g['parameters']['max_slippage_usd'])
        else:
            budget=g['parameters']['max_slippage_usd']
        slip=budget/(g['costs']['usdt_usd'] if leg=='binance' else 1)
        o=dict(id='gp'+uuid.uuid4().hex[:24],group=g['id'],leg=leg,action=action,
               requested=qty,symbol=g['symbol'] if leg=='binance' else g['mt5_symbol'],
               reference=reference,limit=reference+(-slip if sell else slip),slippage=slip,
               fx=q.get('usdt_usd',g['costs']['usdt_usd']),
               created_ms=stamp(),result={'status':'unknown','qty':0,'price':0})
        if leg=='binance':
            # bookTicker carries the current top-of-book quantity.  The live
            # broker uses it as a zero-latency fast path and only requests
            # REST depth when the top level cannot cover this order.
            top_key='bid_qty' if sell else 'ask_qty'
            o['top_qty']=q.get('binance',{}).get(top_key)
        if q.get('execution_quote_source') in ('cache','refresh'): o['quote_source']=q['execution_quote_source']
        o['signal_spread']=q.get('entry' if action=='open' else 'exit')
        o['signal_time_ms']=q.get('time_ms')
        # Preserve each leg's quote even when the paired signal is filled in later.
        o['quote_spread']=o['signal_spread']
        o['quote_context']={name:{key:q.get(name,{}).get(key) for key in
            ('bid','ask','bid_qty','ask_qty','time_ms','observed_ms','source_time_ms')} for name in ('binance','mt5')}
        if leg=='binance':
            # The strategy always opens and closes the Binance SHORT leg.
            # The broker adds this only when the account is in Hedge Mode.
            o['position_side']='SHORT'
        if action=='close' and leg=='mt5':
            candidates=[x for x in self.state['orders'] if x['group']==g['id'] and x['leg']=='mt5' and x['action']=='open' and x['result'].get('qty',0)>0]
            if g.get('imported'):
                # Close each adopted ticket separately; never reuse the first ticket for the whole basket.
                remaining=self.mt5_tickets(g)
                opened=next((x for x in candidates if remaining.get(str(x['result'].get('position')),0)>1e-8),None)
                if opened is None: raise ValueError('接管票据剩余数量不一致，请重新对账')
                qty=min(qty,remaining[str(opened['result']['position'])]);o['requested']=qty
                o['adopted_position']=copy.deepcopy(opened['original_position'])
            else:
                opened=candidates[0]
            o['position']=opened['result'].get('position')
            o['opening_id']=opened['id']
            o['position_identifier']=opened['result'].get('position_identifier')
        journal_started=time.monotonic()
        self.state['orders'].append(o);self.save('order_intent',{'id':o['id'],'group':g['id'],'leg':leg,'action':action,'qty':qty})
        request_started=time.monotonic()
        o['intent_journal_ms']=round((request_started-journal_started)*1000,3)
        self._latency('intent_journal_ms', o['intent_journal_ms'])
        o['dispatch_ms']=stamp()
        try: result=self.broker.submit(copy.deepcopy(o))
        except Exception as exc: result=dict(status='unknown',qty=0,price=0,error=str(exc))
        initial_evidence=result_evidence(result)
        valid=self.result_valid(result,qty)
        # Never let an invalid result look like a completed zero-volume fill.
        if not valid:
            initial_evidence['validation_issues']=validation_issues(result,qty)
            # Keep broker identity for subsequent reads even when its fill
            # fields are unusable. Never treat a malformed acknowledgement as flat.
            metadata={k:result[k] for k in ('ticket','deal','position','position_identifier','retcode','ack_volume','ack_price')
                      if isinstance(result,dict) and k in result}
            metadata.update({k:initial_evidence[k] for k in ('receipt','precheck_receipt','http_status','native_error_code','ack_received_ms') if k in initial_evidence})
            result=dict(metadata,status='unknown',qty=0,price=0,error='成交回报无效，等待对账',
                        error_code='INVALID_RESPONSE',error_kind='invalid_response')
            initial_evidence.update(error_code='INVALID_RESPONSE',error_kind='invalid_response')
        result['request_ms']=round((time.monotonic()-request_started)*1000)
        self._latency(leg+'_'+action+'_request_ms', result['request_ms'])
        result['response_received_ms']=stamp()
        if result.get('status')=='done' and float(result.get('qty',0))>0:
            result['fill_time_ms']=stamp()
        if result.get('status')!='done':
            now_ms=stamp()
            o['reconcile']={'attempts':0,'next_ms':now_ms+100,'started_ms':now_ms,
                            'cancel_sent':False}
        o['initial_result']={**initial_evidence,'request_ms':result['request_ms'],
                             'response_received_ms':result['response_received_ms'],'valid':valid}
        o['result']=result;self.save('order_result',{'id':o['id'],'result':result})
        if action=='close' and self._price_only_full_fill(o):
            # One bounded read, never another order write. Keep the direct
            # acknowledgement intact and do not re-enable a paused engine.
            o['reconcile']['next_ms']=stamp()
            self.resolve(g)
        return o

    @staticmethod
    def _price_only_full_fill(o):
        initial=o.get('initial_result',{})
        return (o.get('leg')=='binance' and initial.get('raw_status')=='FILLED'
                and initial.get('status')=='done' and initial.get('valid') is False
                and initial.get('validation_issues')==['nonpositive_fill_price']
                and initial.get('qty')==o.get('requested') and initial.get('price')==0
                and bool(o.get('result',{}).get('ticket')))

    def _begin_execution(self, g, action, q=None, started=None, started_ms=None):
        key='execution_'+action
        if key in g: return
        g[key]={'started_ms':stamp() if started_ms is None else started_ms}
        if q:
            g[key].update(signal_time_ms=q.get('time_ms'),signal_spread=q.get('entry' if action=='open' else 'exit'))
        self._execution_timers[(g['id'],action)]=time.monotonic() if started is None else started

    def _complete_execution(self, g, action):
        timing=g.get('execution_'+action)
        started=self._execution_timers.pop((g['id'],action),None)
        if timing is not None:
            timing['confirmed_ms']=stamp()
            # A restart cannot reconstruct a monotonic timer. Never invent latency.
            timing['elapsed_ms']=round((time.monotonic()-started)*1000,3) if started is not None else None
            if timing['elapsed_ms'] is not None: self._latency('pair_'+action+'_ms', timing['elapsed_ms'])

    def _record_spread(self, g, action, signal=None):
        rows=[o for o in self.state['orders'] if o['group']==g['id'] and o['action']==action
              and o.get('result',{}).get('qty',0)>0]
        b=[o for o in rows if o['leg']=='binance'];m=[o for o in rows if o['leg']=='mt5']
        if not b or not m: return None
        bqty=sum(float(o['result']['qty']) for o in b);mqty=sum(float(o['result']['qty']) for o in m)
        if bqty<=0 or mqty<=0: return None
        bprice=sum(float(o['result']['qty'])*float(o['result']['price']) for o in b)/bqty
        mprice=sum(float(o['result']['qty'])*float(o['result']['price']) for o in m)/mqty
        fx=sum(float(o.get('fx',g['costs']['usdt_usd']))*float(o['result']['qty']) for o in b)/bqty
        actual=bprice*fx-mprice
        if signal is None:
            signal=next((o.get('signal_spread') for o in rows if o.get('signal_spread') is not None),None)
        for o in rows:
            if 'reference' in o and 'slippage' in o:
                sell=(o['leg']=='binance')==(o['action']=='open')
                adverse=(o['reference']-o['result']['price']) if sell else (o['result']['price']-o['reference'])
                o['adverse_slippage']=adverse
                o['slippage_exceeded']=adverse>o['slippage']+1e-8
            if signal is not None: o['signal_spread']=signal
            o['actual_spread']=actual
            o['spread_slippage']=actual-signal if signal is not None else None
        return actual

    def mt5_tickets(self, g):
        result={}
        for o in self.state['orders']:
            if o['group']!=g['id'] or o['leg']!='mt5': continue
            ticket=str(o.get('position') or o.get('result',{}).get('position') or '')
            result[ticket]=result.get(ticket,0)+(1 if o['action']=='open' else -1)*o.get('result',{}).get('qty',0)
        return result

    @staticmethod
    def result_valid(r, requested):
        import math
        if not isinstance(r,dict): return False
        try:
            qty=float(r.get('qty',0));price=float(r.get('price',0))
            return (isinstance(r,dict) and r.get('status') in ('done','pending','unknown') and
                    math.isfinite(qty) and 0<=qty<=requested+1e-7 and
                    math.isfinite(price) and (qty==0 or price>0))
        except (ValueError,TypeError): return False

    def resolve(self, g, force=False):
        now_ms=stamp()
        for o in self.uncertain(g):
            reconcile=o.setdefault('reconcile',{'attempts':0,'next_ms':now_ms,
                                                 'started_ms':now_ms,'cancel_sent':False})
            if force:
                reconcile['exhausted']=False
                reconcile['attempts']=0
                reconcile['next_ms']=now_ms
            if now_ms < int(reconcile.get('next_ms',0)):
                continue
            if o['result'].get('status')=='pending':
                if not reconcile.get('cancel_sent'):
                    cancel_started=time.monotonic()
                    cancel={'time_ms':stamp()}
                    reconcile['cancel_attempts']=int(reconcile.get('cancel_attempts',0))+1
                    try:
                        reply=self.broker.cancel(o)
                        cancel.update(returned=True,result=result_evidence(reply) if isinstance(reply,dict) else {})
                        reconcile['cancel_sent']=True
                    except Exception as exc:
                        cancel.update(returned=False,result=result_evidence(dict(error=True,
                            error_code=getattr(exc,'code',None),error_kind=getattr(exc,'kind',None),
                            http_status=getattr(exc,'http_status',None))))
                    cancel['elapsed_ms']=round((time.monotonic()-cancel_started)*1000)
                    reconcile['last_cancel']=cancel
                    reconcile.setdefault('first_cancel',copy.deepcopy(cancel))
            reconcile['attempts']=int(reconcile.get('attempts',0))+1
            query_order=copy.deepcopy(o)
            deadline=self._entry_deadlines.get(g['id'])
            query_order['query_timeout_ms']=self.QUERY_TIMEOUT_MS
            if deadline is not None:
                remaining=deadline-time.monotonic()
                if remaining>0:
                    query_order['query_timeout_ms']=max(1,min(self.QUERY_TIMEOUT_MS,int(remaining*1000)))
            query_budget=query_order['query_timeout_ms']
            query_started=time.monotonic()
            try:
                r=self.broker.query(query_order)
            except Exception as exc:
                r={'status':'unknown','qty':0,'price':0,'error':str(exc)}
            shape_valid=self.result_valid(r,o['requested'])
            minimum=o['result'].get('qty',0)
            if self._price_only_full_fill(o): minimum=max(minimum,o['initial_result']['qty'])
            valid=shape_valid and r.get('qty',0)>=minimum
            ticket_mismatch=(isinstance(r,dict) and o['leg']=='binance'
                             and o['result'].get('ticket') and r.get('ticket')
                             and str(o['result']['ticket'])!=str(r['ticket']))
            valid=valid and not ticket_mismatch
            query_ms=round((time.monotonic()-query_started)*1000)
            self._latency('confirmation_query_ms', query_ms)
            evidence=result_evidence(r)
            evidence['valid']=valid
            if not shape_valid:
                evidence.update(error_code='INVALID_RESPONSE',error_kind='invalid_response')
                evidence['validation_issues']=validation_issues(r,o['requested'])
            elif not valid: evidence['validation_issues']=['identity_mismatch' if ticket_mismatch else 'quantity_regressed']
            reconcile['last_query']={**evidence,'time_ms':stamp(),'query_ms':query_ms,'query_timeout_ms':query_budget}
            if valid:
                merged={**o['result'],**r}
                if r.get('status')=='done' and not r.get('error'):
                    for key in ('error','error_code','error_kind'): merged.pop(key,None)
                # A later query confirms the fill but cannot reconstruct the
                # broker's original execution timestamp. Keep this separate
                # from fill_time_ms so the UI never presents it as latency.
                if r.get('status')=='done' and float(r.get('qty',0))>0 and not merged.get('fill_time_ms'):
                    merged['reconcile_time_ms']=stamp()
                o['result']=merged
                if r.get('status')=='done':
                    self._reconcile_notices.pop(o['id'],None)
                    self.save('order_reconciled',{'id':o['id'],'result':merged,
                              'attempt':reconcile['attempts'],'query_ms':query_ms,'query_timeout_ms':query_budget})
                    continue
            # Keep pending/unknown results unresolved.  In particular, a
            # transient "order not found" immediately after a timed-out POST
            # is not proof that the order was rejected.
            data={'id':o['id'],'attempt':reconcile['attempts'],
                  'query_ms':query_ms,
                  'query_timeout_ms':query_budget,'result':evidence,
                  'status':r.get('status','unknown') if isinstance(r,dict) else 'unknown'}
            if isinstance(r,dict) and r.get('error'): data['error']=str(r['error'])
            reconcile.setdefault('first_unconfirmed_query',{'time_ms':stamp(),**reconcile['last_query']})
            first_exhausted=not reconcile.get('exhausted') and reconcile['attempts']>=len(self.RECONCILE_DELAYS_MS)
            if reconcile['attempts']>=len(self.RECONCILE_DELAYS_MS):
                reconcile['exhausted']=True
                # Attempt count must never invalidate an unexpired entry
                # deadline. Late confirmation still requires a protective
                # unwind; recent unknown orders need prompt bounded reads.
                recent=stamp()-reconcile['started_ms']<self.RECENT_RECONCILE_MS
                reconcile['next_ms']=stamp()+(500 if recent else 2000)
                kind='order_reconcile_exhausted'
            else:
                reconcile['next_ms']=max(stamp()+100,reconcile['started_ms']+
                    self.RECONCILE_DELAYS_MS[reconcile['attempts']])
                kind='order_reconcile_retry'
            # Stable unknown reads need no synchronous journal commit every
            # two seconds. Persist all fill/identity/stage changes immediately,
            # plus a periodic heartbeat; intent/result durability is unchanged.
            identity=tuple(str(o['result'].get(k,'')) for k in ('ticket','deal','position','position_identifier'))
            cancel=reconcile.get('last_cancel',{})
            signature=(evidence,identity,cancel.get('returned'),cancel.get('result'))
            previous=self._reconcile_notices.get(o['id'])
            checked=time.monotonic()
            if (force or first_exhausted or previous is None or previous[0]!=signature or
                    (checked-previous[1])*1000>=self.RECONCILE_LOG_INTERVAL_MS):
                self._reconcile_notices[o['id']]=(copy.deepcopy(signature),checked)
                self.save(kind,data)
        if self.uncertain(g):
            if self._waiting_second_confirmation(g): return False
            reason='订单状态未知，禁止重发；正在分层查询成交和持仓'
            # Background position checks may update alarm text every scan.
            # Do not turn that into a new pause/time stamp on every scan.
            if self.state.get('enabled') or not self.state.get('last_pause'): self.pause(reason,keep_entry=True)
            return False
        return True

    def valuation(self, g, q):
        c=g['costs'];fx=q.get('usdt_usd',c['usdt_usd']);fees=0
        if g.get('status')=='closed':
            # Freeze historical PnL conversion, including when fees arrive later.
            closes=[o for o in self.state['orders'] if o['group']==g['id'] and o['leg']=='binance'
                    and o['action']=='close' and o.get('result',{}).get('qty',0)>0]
            total=sum(o['result']['qty'] for o in closes)
            historical=sum(o['result']['qty']*o.get('fx',c['usdt_usd']) for o in closes)/total if total else c['usdt_usd']
            fx=g.get('valuation_fx',historical)
        pnl=dict(binance=0.,mt5=0.)
        fee_base=dict(binance=0.,mt5=0.);fee_flags=[]
        owned=self.amounts(g)
        for o in self.state['orders']:
            if o['group']!=g['id']: continue
            r=o.get('result',{});qty=r.get('qty',0);price=r.get('price',0)
            sign=1 if (o['leg']=='binance')==(o['action']=='open') else -1
            order_fx=o.get('fx',g['costs']['usdt_usd'])
            pnl[o['leg']]+=sign*qty*price
            fee=r.get('fee', qty*price*c['binance_taker_percent']/100 if o['leg']=='binance' else qty/g['contract']*c['mt5_commission_per_lot_side'])*(order_fx if o['leg']=='binance' else 1)
            fees+=fee
            fee_base[o['leg']]+=max(0,fee)
            if qty>0: fee_flags.append(r.get('fee_verified') is True)
        pnl['binance']-=owned['binance']*q['binance']['ask']
        pnl['mt5']+=owned['mt5']*q['mt5']['bid']
        # Linear USDT futures settle the price difference in USDT. Convert
        # that PnL once, rather than converting both full contract notionals.
        gross=pnl['binance']*fx+pnl['mt5']
        exit_fee_binance=owned['binance']*q['binance']['ask']*fx*c['binance_taker_percent']/100
        exit_fee_mt5=owned['mt5']/g['contract']*c['mt5_commission_per_lot_side']
        exit_fee=exit_fee_binance+exit_fee_mt5
        days=max(0,(g.get('closed_ms',stamp())-g['opened_ms'])/86400000)
        if g['mode']!='paper':
            verified=g.get('costs_verified',False)
            mt5_swap=g.get('mt5_swap_usd',0) if verified else g.get('live_mt5_swap_usd',0)
            funding=g.get('binance_funding_usd',0) if verified else g.get('live_binance_funding_usd',0)
        else:
            verified=False
            mt5_swap=-g['lots']*c['mt5_swap_per_lot_day']*days
            funding=g['qty']*g.get('open_binance',0)*fx*c['paper_funding_percent_day']/100*days
        carry=mt5_swap+funding
        if g.get('imported'):
            fees+=g['history_fees_usd']
            funding+=g['history_funding_usd']
            carry=mt5_swap+funding
        # User-declared cashback is an estimate, independent of verified
        # platform commissions. Never rebate funding, Swap or negative fees.
        rebate_binance=(fee_base['binance']+max(0,exit_fee_binance))*c.get('binance_rebate_percent',0)/100
        rebate_mt5=(fee_base['mt5']+max(0,exit_fee_mt5))*c.get('mt5_rebate_percent',0)/100
        rebate=rebate_binance+rebate_mt5
        before_rebate=gross-fees-exit_fee+carry
        flat=max(owned.values())<1e-8
        return dict(pnl_schema_version=2,binance_pnl_usdt=round(pnl['binance'],8),mt5_pnl_usd=round(pnl['mt5'],8),valuation_fx=fx,
                    gross=round(gross,8),fees=round(fees,8),estimated_exit_fee=round(exit_fee,8),
                    estimated_exit_fee_binance=round(exit_fee_binance,8),estimated_exit_fee_mt5=round(exit_fee_mt5,8),
                    mt5_swap=round(mt5_swap,8),binance_funding=round(funding,8),carry=round(carry,8),
                    net_before_rebate=round(before_rebate,8),estimated_rebate=round(rebate,8),
                    estimated_rebate_binance=round(rebate_binance,8),estimated_rebate_mt5=round(rebate_mt5,8),
                    rebate_fee_base_binance=round(fee_base['binance'],8),rebate_fee_base_mt5=round(fee_base['mt5'],8),
                    net=round(before_rebate+rebate,8),remaining=owned,
                    costs_verified=verified,
                    fees_verified=verified or (g['mode']=='live' and not g.get('imported') and bool(fee_flags) and all(fee_flags)),
                    estimated=not flat or rebate>0 or (g['mode']!='paper' and not verified))

    def _finish_open(self, c, p, g, q, threshold=None):
        """Continue the second leg after the first leg is confirmed.

        This is also used when a timed-out first-leg response is reconciled a
        moment later.  It deliberately keeps the original group timestamp so
        the configured one-sided exposure window still bounds the recovery.
        """
        first_leg=g.get('first_leg') or ('binance' if c.get('execution',{}).get('entry_leg','binance')=='binance' else 'mt5')
        second_leg=g.get('second_leg') or ('mt5' if first_leg=='binance' else 'binance')
        first_label='币安' if first_leg=='binance' else 'MT5'
        first=next((o for o in self.state['orders']
                    if o['group']==g['id'] and o['leg']==first_leg and o['action']=='open'),None)
        if first is None: return False
        result=first['result']
        if result.get('status')!='done': return False
        if abs(result.get('qty',0)-g['qty'])>1e-8:
            g['status']='unwinding';g['reason']=f'{first_label} 开仓拒单或部分成交，正在保护性平仓';self.pause(g['reason'])
            self.close_group(g,q);return True
        deadline=self._entry_deadlines.get(g['id'])
        if (stamp()-g['opened_ms']>=g['parameters']['max_unhedged_ms'] or
                (deadline is not None and time.monotonic()>=deadline)):
            g['status']='unwinding';g['reason']='第一腿成交后超出敞口期限或报价过期，正在保护性平仓';self.pause(g['reason'])
            self.close_group(g,q);return True
        # Submit the second leg only once. A later pass may finalize its
        # verified original fill, but must never create a duplicate order.
        existing=next((o for o in self.state['orders']
                       if o['group']==g['id'] and o['leg']==second_leg and o['action']=='open'),None)
        second=existing
        if second is None:
            second_q=q
            provider=getattr(self,'quote_provider',None)
            if provider:
                try: second_q=provider()
                except Exception as exc:
                    g['status']='unwinding';g['reason']='第二腿前无法取得新报价，正在保护性平仓：'+str(exc);self.pause(g['reason']);self.close_group(g,q);return True
            if (stamp()-g['opened_ms']>=g['parameters']['max_unhedged_ms'] or
                (deadline is not None and time.monotonic()>=deadline) or not valid_quote(second_q,c)):
                g['status']='unwinding';g['reason']='第一腿成交后报价过期或超出敞口期限，正在保护性平仓';self.pause(g['reason']);self.close_group(g,second_q);return True
            second=self.order(g,second_leg,'open',g['qty'],second_q)
        # Keep the normal 100 ms reconciliation backoff and original deadline.
        # No sleep, resubmission, additional write or relaxed fill validation.
        if second['result']['status']!='done' and self._waiting_second_confirmation(g): return True
        self._entry_deadlines.pop(g['id'],None)
        if second['result']['status']=='done' and abs(second['result']['qty']-g['qty'])<1e-8:
            actual=self._record_spread(g,'open',g.get('entry_signal',g['entry']))
            if actual is None:
                # Both legs reported full fills, but the pair basis could not
                # be reconstructed. Keep the hedge untouched and require a
                # reconciliation instead of churning fees with a blind close.
                g['status']='attention';g['reason']='两边已成交但无法计算实际价差，请对账核验';self.pause(g['reason']);return True
            g['status']='open';g['open_binance']=(first if first_leg=='binance' else second)['result']['price']
            g['entry_signal']=g.get('entry_signal',g['entry']);g['entry']=actual
            self._complete_execution(g,'open')
            if threshold is None: threshold=g.get('entry_threshold',g['parameters']['entry_spread_usd'])
            if actual < threshold:
                g['execution_warning']=f'实际成交价差 {actual:.6f} 低于触发阈值 {threshold:.6f}，按实际开仓价差管理；未反向平仓'
            self.save('group_opened',{'group':g['id'],'entry':g['entry']})
            if not self.state['enabled']:
                self.state['alarm']='双边开仓已确认；新开仓仍暂停，核对后可手动启动'
                self.save('entry_recovered',{'group':g['id'],'reason':self.state['alarm']})
        else:
            error=str(second.get('result',{}).get('error') or '')
            suffix='：'+error if error else ''
            second_label='币安' if second_leg=='binance' else 'MT5'
            g['status']='unwinding';g['reason']=f'{second_label} 拒单、部分成交或状态未知，正在保护性平仓'+suffix;self.pause(g['reason'])
            self.close_group(g,q)
        return True

    def open(self, c, p, q, grid_index=0, min_entry_spread=None):
        now=stamp()
        entry_started=time.monotonic()
        threshold=min_entry_spread if min_entry_spread is not None else c['strategy']['entry_spread_usd']
        guard=getattr(self, 'entry_preflight', None)
        if guard and not guard(q, threshold):
            detail=getattr(self, 'entry_preflight_detail', {})
            data={'threshold':threshold, 'reason':detail.get('reason') or self.state.get('alarm') or '等待开仓预检通过',
                  'state':detail.get('state','waiting')}
            previous=getattr(self, '_preflight_log', None)
            checked=time.monotonic()
            if previous is None or previous[0]!=data or checked-previous[1]>=15:
                self.save('entry_preflight_blocked', data)
                self._preflight_log=(data,checked)
            return
        self._preflight_log=None
        first_leg='binance' if c.get('execution',{}).get('entry_leg','binance')=='binance' else 'mt5'
        second_leg='mt5' if first_leg=='binance' else 'binance'
        g=dict(id=uuid.uuid4().hex[:12],status='opening',opened_ms=now,qty=p['qty'],lots=p['lots'],contract=p['contract'],
            symbol=c['symbol'],mt5_symbol=c['mt5']['symbol'],mode=c['execution']['mode'],
            parameters=copy.deepcopy(c['strategy']),costs=copy.deepcopy(c['costs']),attempts=0,
            retry_limit=c['execution']['close_retry_limit'],entry=q['entry'],entry_signal=q['entry'],reason='',key=pair_key(c),
            first_leg=first_leg,second_leg=second_leg,entry_threshold=threshold,grid_index=int(grid_index))
        self._begin_execution(g,'open',q,entry_started,now)
        self.state['groups'].append(g);self.state['last_open_ms']=now;self.save('group_opening',{'group':g['id']})
        self._entry_deadlines[g['id']]=entry_started+c['strategy']['max_unhedged_ms']/1000
        first=self.order(g,first_leg,'open',p['qty'],q)
        result=first['result']
        # Only a local pre-submit block on the first leg can be skipped.
        # Exchange rejections, fills and uncertain submissions still pause.
        if (first_leg=='binance' and result.get('submitted') is False
            and result.get('status')=='done' and result.get('qty')==0
            and result.get('error_code') in ('DEPTH_INSUFFICIENT','DEPTH_CHECK_FAILED')
            and not self.uncertain(g) and max(self.amounts(g).values())<1e-8):
            g.update(status='closed',closed_ms=stamp(),entry_skipped=True,
                     reason=result.get('error','盘口预检未通过')+'；本次未下单，冷却后自动重新检查')
            g['valuation']=self.valuation(g,q)
            self.save('entry_skipped',{'group':g['id'],'reason':g['reason'],
                                      'code':result['error_code']})
            self._entry_deadlines.pop(g['id'],None)
            self._execution_timers.pop((g['id'],'open'),None)
            return
        if (result.get('status')!='done' and self.state['enabled']
                and self._price_only_full_fill(first)
                and time.monotonic()<self._entry_deadlines[g['id']]):
            # A terminal FILLED acknowledgement without a price is incomplete,
            # not permission to hedge. Try one read within the original entry
            # budget before pausing; never re-arm an already paused engine.
            first['reconcile']['next_ms']=stamp()
            if not self.resolve(g): return
            result=first['result']
        if result.get('status')!='done':
            self.pause(f'{"币安" if first_leg=="binance" else "MT5"} 开仓状态未知，进入分层对账',keep_entry=True);return
        self._finish_open(c,p,g,q,threshold)

    def request_close(self, group_id=None, reason='用户平仓'):
        self._entry_deadlines.clear()
        code=reason_code(reason)
        self.state['last_pause']={'time_ms':stamp(),'reason_code':code,'reason_label':REASON_LABELS[code],
                                  'enabled_before':bool(self.state.get('enabled')),'keep_entry':False}
        self.state['enabled']=False
        found=False
        for g in self.active():
            if group_id is None or g['id']==group_id:
                self._begin_execution(g,'close')
                if g['status']=='attention': g['attempts']=0
                # A manual close must not downgrade an ongoing protective
                # unwind to the tighter normal slippage budget.
                if g['status']!='unwinding': g.update(status='closing',reason=reason)
                found=True
        if group_id and not found: raise ValueError('未找到活动交易组')
        self.save('close_requested',{'group':group_id,'reason':reason})

    def finalize_flat(self, g, q):
        """Record a completed close once both broker legs report zero exposure."""
        if not q or g.get('status') not in ('closing','unwinding') or self.uncertain(g): return False
        if max(self.amounts(g).values())>=1e-8: return False
        actual=self._record_spread(g,'close',g.get('exit_signal',q.get('exit')))
        g.update(status='closed',closed_ms=stamp(),exit=actual if actual is not None else q.get('exit'))
        self._complete_execution(g,'close')
        self._execution_timers.pop((g['id'],'open'),None)
        g['valuation']=self.valuation(g,q)
        g['valuation_fx']=g['valuation']['valuation_fx']
        self.save('group_closed',{'group':g['id'],'net_estimate':g['valuation']['net']})
        return True

    def _close_quote(self, q):
        provider=getattr(self,'close_quote_provider',None)
        if provider:
            try: q=provider()
            except Exception: q=None
        return q

    def _close_order(self, g, leg, qty, q, refresh=True):
        if refresh: q=self._close_quote(q)
        if not valid_group_quote(q,g['key'],g['parameters']):
            self.state['alarm']='平仓等待有效新报价；成交查询继续，当前尚未双平'
            return
        return self.order(g,leg,'close',qty,q)

    def close_group(self, g, q):
        self._begin_execution(g,'close',q)
        if q and 'exit_signal' not in g: g['exit_signal']=q.get('exit')
        timing=g['execution_close']
        if 'signal_spread' not in timing and q:
            timing.update(signal_time_ms=q.get('time_ms'),signal_spread=q.get('exit'))
        # Status reads must precede account ownership checks: a delayed fill
        # changes the expected exposure that those checks compare against.
        if not self.resolve(g):
            self._observe_exit(g,q,'CLOSE_PENDING')
            return
        q=self._close_quote(q)
        if not valid_group_quote(q,g['key'],g['parameters']):
            self.state['alarm']='平仓等待有效新报价；成交查询继续，当前尚未双平'
            self._observe_exit(g,q,'CLOSE_QUOTE_UNAVAILABLE')
            return
        if g.get('imported') and hasattr(self,'before_import_close'):
            if self.before_import_close() is False: return
            # Account reads can take longer than the quote budget. Wait for another tick.
            if any(stamp()-int(q[leg].get('observed_ms',q[leg].get('time_ms',0)))>g['parameters']['max_quote_age_ms'] for leg in ('binance','mt5')):
                self.state['alarm']='接管平仓核验完成时行情已过期，等待新报价'
                self._observe_exit(g,q,'CLOSE_QUOTE_UNAVAILABLE')
                return
        amounts=self.amounts(g)
        if max(amounts.values())<1e-8:
            self.finalize_flat(g,q);return
        # A normal profit exit can be deferred only before either leg closes.
        # Once any close fills, always finish reducing the other leg instead
        # of leaving exposure while waiting for a more profitable price.
        if (g.get('reason')=='逐组止盈' and g['parameters'].get('exit_mode')=='group'
                and not any(o['group']==g['id'] and o['action']=='close'
                            and o.get('result',{}).get('qty',0)>0 for o in self.state['orders'])):
            v=self.valuation(g,q);s=g['parameters']
            if (not self.target(g,g['entry'],q['exit']) or
                    (s['require_net_profit'] and v['net']<s['min_net_profit_usd'])):
                self._observe_exit(g,q,'RECHECK_TARGET' if not self.target(g,g['entry'],q['exit'])
                                   else 'RECHECK_NET_PROFIT',valuation=v)
                g.update(status='open',reason='止盈报价已变化，继续持仓等待条件')
                g.pop('exit_signal',None);g.pop('exit_decision',None);g.pop('execution_close',None)
                self._execution_timers.pop((g['id'],'close'),None)
                self.save('exit_deferred',dict(group=g['id'],reason=g['reason'],
                          signal_spread=q['exit'],estimated_net=v['net'],min_net_profit=s['min_net_profit_usd']))
                return
        if g['attempts']>=g['retry_limit']:
            g['status']='attention';self.pause('减仓重试达到上限，仍有敞口；请检查账户后使用“重试平仓”');return
        g['attempts']+=1;self.save('close_attempt',{'group':g['id'],'attempt':g['attempts']})
        # Remove a pre-existing unmatched excess first. Do not touch the
        # hedged portion until that reduction is confirmed.
        before_m, before_b = amounts['mt5'], amounts['binance']
        if before_m-before_b>1e-8:
            self._close_order(g,'mt5',before_m-before_b,q)
        elif before_b-before_m>1e-8:
            self._close_order(g,'binance',before_b-before_m,q)
        if self.uncertain(g): return
        amounts=self.amounts(g); before_m, before_b=amounts['mt5'],amounts['binance']
        if abs(before_m-before_b)>1e-8:
            # Excess close was rejected/partial. Retry it before the pair.
            return

        # For the matched portion, close MT5 first and buy back no more Binance
        # than MT5 actually closed. This prevents a rejected/partial MT5 close
        # from creating a naked MT5 long.
        if before_m>1e-8:
            self._close_order(g,'mt5',before_m,q,refresh=False)
            after_m=self.amounts(g)['mt5']
            mt5_closed=max(0,before_m-after_m)
            if mt5_closed>1e-8:
                self._close_order(g,'binance',min(before_b,mt5_closed),q)
        if g.get('imported') and not self.uncertain(g):
            after=self.amounts(g)
            if after['mt5']<before_m-1e-8 and abs(after['mt5']-after['binance'])<1e-8:
                g['attempts']=0
        if not self.uncertain(g) and max(self.amounts(g).values())<1e-8:
            self.finalize_flat(g,q)

    def reconcile_pending(self, c):
        # Read-only confirmation must continue even while quotes are stale or
        # restart recovery blocks all writes. resolve() already applies backoff.
        for g in self.active():
            if g['key']!=pair_key(c): continue
            self._expire_second_confirmation(g)
            if self.uncertain(g): self.resolve(g)
            self._expire_second_confirmation(g)

    def _expire_second_confirmation(self, g):
        # Even without a usable quote, commit the expired/interrupted state.
        # Actual closing still needs confirmed fills and valid execution quotes.
        if g.get('status')!='opening' or not any(
                o['group']==g['id'] and o['leg']==g.get('second_leg') and o['action']=='open'
                for o in self.state['orders']): return
        deadline=self._entry_deadlines.get(g['id'])
        if (deadline is None or time.monotonic()>=deadline or
                stamp()-g['opened_ms']>=g['parameters']['max_unhedged_ms']):
            g['status']='unwinding';g['reason']='开仓确认窗口已结束或流程中断，正在保护性平仓'
            self.pause(g['reason'])

    def tick(self, c, p, q):
        self.reconcile_pending(c)
        if self.state['recovery']:
            self.observe_exit_wait(c,q,'RECOVERY_PENDING')
            return
        if not valid_quote(q,c):
            if self.active(): self.state['alarm']='报价过期、时钟偏差或品种不符，等待有效行情；已有仓位仍未平仓'
            self.observe_exit_wait(c,q,'QUOTE_UNAVAILABLE')
            return
        measure = getattr(self, 'latency', None) is not None
        decision_elapsed = 0.
        exit_event=False
        for g in self.active():
            if g['key']!=q['key']: raise ValueError('当前行情与持仓账户或品种不一致')
            if g['status']=='opening':
                confirmed=self.resolve(g)
                deadline=self._entry_deadlines.get(g['id'])
                if confirmed or not self._waiting_second_confirmation(g):
                    # If the first leg was only late to acknowledge, continue
                    # the paired entry while the original exposure window is
                    # still valid. Otherwise _finish_open performs the normal
                    # protective unwind.
                    if deadline is not None and time.monotonic()<deadline:
                        self._finish_open(c,p,g,q)
                    elif confirmed or any(o['group']==g['id'] and o['leg']==g.get('second_leg')
                                          and o['action']=='open' for o in self.state['orders']):
                        self._entry_deadlines.pop(g['id'],None)
                        g['status']='unwinding';g['reason']='开仓确认窗口已结束或流程中断，正在保护性平仓'
                        self.pause(g['reason'])
            started = time.monotonic() if measure else 0
            g['valuation']=self.valuation(g,q)
            if g['status']!='open' or (g.get('imported') and not g.get('management_enabled')):
                self._observe_exit(g,q,{'opening':'OPENING_PENDING','closing':'CLOSE_PENDING',
                    'unwinding':'CLOSE_PENDING','attention':'ATTENTION_REQUIRED'}.get(g['status'],'MANAGEMENT_DISABLED'),
                    valuation=g['valuation'],limits=c['strategy'])
            if measure: decision_elapsed += time.monotonic()-started
        started = time.monotonic() if measure else 0
        opened=[g for g in self.active() if g['status']=='open' and (not g.get('imported') or g.get('management_enabled'))]
        total=sum(g.get('valuation',{}).get('net',0) for g in self.active() if not g.get('imported'))
        loss=c['strategy']['total_loss_enabled'] and total<=-c['strategy']['total_max_loss_usd']
        basket=[g for g in opened if g['parameters']['exit_mode']=='basket' and not g.get('imported')]
        basket_exit=set()
        basket_evidence=None
        if basket:
            qty=sum(g['qty'] for g in basket); entry=sum(g['entry']*g['qty'] for g in basket)/qty
            net=sum(g['valuation']['net'] for g in basket)
            # Every current member's saved condition must agree. Explicit
            # existing-basket edits update all current members atomically.
            if all(self.target(g,entry,q['exit']) and (not g['parameters']['require_net_profit'] or net>=g['parameters']['min_net_profit_usd']) for g in basket):
                basket_exit={g['id'] for g in basket}
            if getattr(self,'exit_observations',None) is not None:
                basket_evidence=dict(entry=entry,net=net,members=len(basket),
                    target_blocked_members=sum(not self.target(g,entry,q['exit']) for g in basket),
                    net_blocked_members=sum(g['parameters']['require_net_profit'] and net<g['parameters']['min_net_profit_usd'] for g in basket))
        for g in opened:
            s=g['parameters'];v=g['valuation'];reason=''
            if g.get('imported'):
                if self.target(g,g['entry'],q['exit']) and v['net']>=s['min_net_profit_usd']: reason='接管篮子收窄并盈利'
            elif loss: reason='本策略总浮动亏损上限'
            elif s['group_loss_enabled'] and v['net']<=-s['group_max_loss_usd']: reason='单组亏损上限'
            elif s['max_hold_minutes'] and stamp()-g['opened_ms']>=s['max_hold_minutes']*60000: reason='持仓超时'
            elif g['id'] in basket_exit: reason='整篮子止盈'
            elif s['exit_mode']=='group' and self.target(g,g['entry'],q['exit']) and (not s['require_net_profit'] or v['net']>=s['min_net_profit_usd']): reason='逐组止盈'
            self._observe_exit(g,q,valuation=v,basket=basket_evidence,trigger=reason,limits=c['strategy'])
            if reason:
                self._begin_execution(g,'close',q)
                decision=dict(signal_spread=q['exit'],entry_spread=g['entry'],
                    contraction=g['entry']-q['exit'],target_contraction=s['take_contraction_usd'],
                    target_spread=s['exit_spread_usd'],min_net_profit=s['min_net_profit_usd'],
                    estimated_net=v['net'],estimated_net_before_rebate=v['net_before_rebate'],
                    estimated_gross=v['gross'],estimated_exit_fee=v['estimated_exit_fee'],
                    estimated_rebate=v['estimated_rebate'],valuation_fx=v['valuation_fx'],
                    require_net_profit=s['require_net_profit'],target_mode=s['target_mode'])
                if g['id'] in basket_exit: decision['basket_estimated_net']=sum(x['valuation']['net'] for x in basket)
                g.update(status='closing',reason=reason,exit_signal=q['exit'],exit_decision=decision)
                if measure: decision_elapsed += time.monotonic()-started
                exit_event=True;self.save('exit_signal',{'group':g['id'],'reason':reason,**decision})
                if reason in ('本策略总浮动亏损上限','单组亏损上限'): self.pause(reason)
                started = time.monotonic() if measure else 0
        if measure: decision_elapsed += time.monotonic()-started
        for g in self.active():
            if g['status'] in ('closing','unwinding'):
                exit_event=True;self.close_group(g,q)
        started = time.monotonic() if measure else 0
        s=c['strategy'];all_active=self.active();active=[g for g in all_active if not g.get('imported')];grid_index=0;open_threshold=s['entry_spread_usd'];grid_allowed=True
        if s.get('grid_enabled') and active:
            # Grid additions are separate paired groups. They only continue
            # a homogeneous grid chain; unrelated/manual groups block adds.
            if all(g.get('parameters',{}).get('grid_enabled') for g in active):
                grid_index=max(int(g.get('grid_index',0)) for g in active)+1
                open_threshold=s['entry_spread_usd']+grid_index*s['grid_spacing_usd']
                grid_allowed=grid_index<=int(s['grid_max_adds'])
            else:
                grid_allowed=False
        should_open = (self.state['enabled'] and not exit_event and all(g['status']=='open' for g in all_active)
            and grid_allowed and len(active)<s['max_groups']
            and sum(g['lots'] for g in all_active)+p['lots']<=s['max_total_lots']+1e-9
            and stamp()-self.state['last_open_ms']>=s['cooldown_seconds']*1000 and q['entry']>=open_threshold)
        if measure: self._latency('decision_ms', (decision_elapsed+time.monotonic()-started)*1000)
        if should_open:
            self.open(c,p,q,grid_index=grid_index,min_entry_spread=open_threshold)

    @staticmethod
    def target(g, entry, exit_spread):
        s=g['parameters']
        return entry-exit_spread>=s['take_contraction_usd'] if s['target_mode']=='contraction' else exit_spread<=s['exit_spread_usd']
