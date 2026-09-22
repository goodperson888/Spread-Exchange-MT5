"""Durable paired execution. Order intent is committed before every broker write."""
import copy
import time
import uuid
from trading_config import pair_key


def stamp(): return int(time.time()*1000)


def valid_quote(q, c, now=None):
    now=now or stamp(); s=c['strategy']
    if q.get('key')!=pair_key(c): return False
    for side in ('binance','mt5'):
        x=q.get(side,{})
        if not 0<float(x.get('bid',0))<=float(x.get('ask',0)): return False
        observed=int(x.get('observed_ms',x.get('time_ms',0)))
        if not -1000<=now-observed<=s['max_quote_age_ms']: return False
    binance_observed=int(q['binance'].get('observed_ms',q['binance'].get('time_ms',0)))
    mt5_observed=int(q['mt5'].get('observed_ms',q['mt5'].get('time_ms',0)))
    return abs(binance_observed-mt5_observed)<=s['max_clock_skew_ms']


class Engine:
    def __init__(self, store, broker):
        self.store,self.broker=store,broker
        self.state=store.load() or dict(groups=[],orders=[],last_open_ms=0,enabled=False,mode='paper',key=None,alarm='',recovery=False)
        self.state['enabled']=False
        for g in self.state['groups']:
            if g.get('imported'): g['management_enabled']=False
        self.state['recovery']=any(g['status']!='closed' for g in self.state['groups'])
        if self.state['recovery']: self.state['alarm']='程序重启，须连接原账户并完成持仓对账后恢复管理'
        self.save('boot')

    def save(self, kind, data=None): self.store.commit(self.state,kind,data)

    def active(self): return [g for g in self.state['groups'] if g['status']!='closed']

    def pause(self, reason='已暂停新开仓；已有仓位继续管理'):
        self.state['enabled']=False;self.state['alarm']=reason;self.save('pause',{'reason':reason})

    def start(self, c):
        if self.state['recovery']: raise ValueError('请先完成持仓对账')
        if any(g['status'] in ('opening','unwinding','closing','attention') for g in self.active()):
            raise ValueError('尚有未完成或异常交易组')
        self.state.update(enabled=True,key=pair_key(c),mode=c['execution']['mode'],alarm='')
        self.save('start',{'mode':self.state['mode']})

    def amounts(self, g):
        amounts={'binance':0.,'mt5':0.}
        for o in self.state['orders']:
            if o['group']==g['id']:
                amounts[o['leg']]+=(1 if o['action']=='open' else -1)*o.get('result',{}).get('qty',0)
        return {k:max(0,v) for k,v in amounts.items()}

    def uncertain(self, g):
        return [o for o in self.state['orders'] if o['group']==g['id'] and o.get('result',{}).get('status')!='done']

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
        o['signal_spread']=q.get('entry' if action=='open' else 'exit')
        o['signal_time_ms']=q.get('time_ms')
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
            o['position']=opened['result'].get('position') or opened['result'].get('ticket')
            if not o['position']:
                raise ValueError('MT5 持仓票据未确定，必须先完成对账')
        self.state['orders'].append(o);self.save('order_intent',{'id':o['id'],'group':g['id'],'leg':leg,'action':action,'qty':qty})
        try: result=self.broker.submit(copy.deepcopy(o))
        except Exception as exc: result=dict(status='unknown',qty=0,price=0,error=str(exc))
        # Never let an invalid result look like a completed zero-volume fill.
        if not self.result_valid(result,qty): result=dict(status='unknown',qty=0,price=0,error='成交回报无效，等待对账')
        if result.get('status')=='done' and float(result.get('qty',0))>0:
            result['fill_time_ms']=stamp()
        o['result']=result;self.save('order_result',{'id':o['id'],'result':result})
        return o

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
        return (r.get('status') in ('done','pending','unknown') and
                math.isfinite(float(r.get('qty',0))) and 0<=float(r.get('qty',0))<=requested+1e-7 and
                math.isfinite(float(r.get('price',0))) and (r.get('qty',0)==0 or r.get('price',0)>0))

    def resolve(self, g):
        for o in self.uncertain(g):
            if o['result'].get('status')=='pending':
                try: self.broker.cancel(o)
                except Exception: pass
            try: r=self.broker.query(copy.deepcopy(o))
            except Exception: continue
            if self.result_valid(r,o['requested']) and r.get('qty',0)>=o['result'].get('qty',0):
                merged={**o['result'],**r}
                if r.get('status')=='done': merged.pop('error',None)
                # A later query confirms the fill but cannot reconstruct the
                # broker's original execution timestamp. Keep this separate
                # from fill_time_ms so the UI never presents it as latency.
                if r.get('status')=='done' and float(r.get('qty',0))>0 and not merged.get('fill_time_ms'):
                    merged['reconcile_time_ms']=stamp()
                o['result']=merged;self.save('order_reconciled',{'id':o['id'],'result':merged})
        if self.uncertain(g):
            self.pause('订单状态未知，禁止重发；正在查询成交和持仓');return False
        return True

    def valuation(self, g, q):
        fx=q.get('usdt_usd',g['costs']['usdt_usd']);c=g['costs'];gross=0;fees=0
        owned=self.amounts(g)
        for o in self.state['orders']:
            if o['group']!=g['id']: continue
            r=o.get('result',{});qty=r.get('qty',0);price=r.get('price',0)
            sign=1 if (o['leg']=='binance')==(o['action']=='open') else -1
            order_fx=o.get('fx',g['costs']['usdt_usd'])
            gross+=sign*qty*price*(order_fx if o['leg']=='binance' else 1)
            fees+=r.get('fee', qty*price*c['binance_taker_percent']/100 if o['leg']=='binance' else qty/g['contract']*c['mt5_commission_per_lot_side'])*(order_fx if o['leg']=='binance' else 1)
        gross-=owned['binance']*q['binance']['ask']*fx
        gross+=owned['mt5']*q['mt5']['bid']
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
        flat=max(owned.values())<1e-8
        return dict(gross=round(gross,8),fees=round(fees,8),estimated_exit_fee=round(exit_fee,8),
                    estimated_exit_fee_binance=round(exit_fee_binance,8),estimated_exit_fee_mt5=round(exit_fee_mt5,8),
                    mt5_swap=round(mt5_swap,8),binance_funding=round(funding,8),carry=round(carry,8),
                    net=round(gross-fees-exit_fee+carry,8),remaining=owned,
                    costs_verified=verified,estimated=not flat or (g['mode']!='paper' and not verified))

    def open(self, c, p, q, grid_index=0, min_entry_spread=None):
        now=stamp()
        threshold=min_entry_spread if min_entry_spread is not None else c['strategy']['entry_spread_usd']
        guard=getattr(self, 'entry_preflight', None)
        if guard and not guard(q, threshold):
            self.save('entry_preflight_blocked', {'threshold':threshold})
            return
        g=dict(id=uuid.uuid4().hex[:12],status='opening',opened_ms=now,qty=p['qty'],lots=p['lots'],contract=p['contract'],
            symbol=c['symbol'],mt5_symbol=c['mt5']['symbol'],mode=c['execution']['mode'],
            parameters=copy.deepcopy(c['strategy']),costs=copy.deepcopy(c['costs']),attempts=0,
            retry_limit=c['execution']['close_retry_limit'],entry=q['entry'],reason='',key=pair_key(c),
            grid_index=int(grid_index))
        self.state['groups'].append(g);self.state['last_open_ms']=now;self.save('group_opening',{'group':g['id']})
        # Choose the first leg explicitly. Binance-first avoids creating an
        # MT5 position when the exchange rejects a signed order; MT5-first is
        # retained as a fallback for users whose exchange route is reliable.
        first_leg='binance' if c.get('execution',{}).get('entry_leg','binance')=='binance' else 'mt5'
        second_leg='mt5' if first_leg=='binance' else 'binance'
        first_label='币安' if first_leg=='binance' else 'MT5'
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
            return
        if first['result']['status']!='done':
            self.pause(f'{first_label} 开仓状态未知，等待对账');return
        if abs(first['result']['qty']-p['qty'])>1e-8:
            g['status']='unwinding';g['reason']=f'{first_label} 开仓拒单或部分成交，正在保护性平仓';self.pause(g['reason'])
            self.close_group(g,q);return
        if stamp()-now>c['strategy']['max_unhedged_ms'] or not valid_quote(q,c):
            g['status']='unwinding';g['reason']='第一腿成交后超出敞口期限或报价过期，正在保护性平仓';self.pause(g['reason'])
            self.close_group(g,q);return
        second_q=q
        provider=getattr(self,'quote_provider',None)
        if provider:
            try: second_q=provider()
            except Exception as exc:
                g['status']='unwinding';g['reason']='第二腿前无法取得新报价，正在保护性平仓：'+str(exc);self.pause(g['reason']);self.close_group(g,q);return
            if stamp()-now>c['strategy']['max_unhedged_ms'] or not valid_quote(second_q,c):
                g['status']='unwinding';g['reason']='第一腿成交后报价过期或超出敞口期限，正在保护性平仓';self.pause(g['reason']);self.close_group(g,second_q);return
        second=self.order(g,second_leg,'open',p['qty'],second_q)
        if second['result']['status']=='done' and abs(second['result']['qty']-p['qty'])<1e-8:
            actual=self._record_spread(g,'open',q['entry'])
            if actual is None:
                # Both legs reported full fills, but the pair basis could not
                # be reconstructed. Keep the hedge untouched and require a
                # reconciliation instead of churning fees with a blind close.
                g['status']='attention';g['reason']='两边已成交但无法计算实际价差，请对账核验';self.pause(g['reason']);return
            g['status']='open';g['open_binance']=second['result']['price']
            g['entry_signal']=q['entry'];g['entry']=actual
            if actual < threshold:
                g['execution_warning']=f'实际成交价差 {actual:.6f} 低于触发阈值 {threshold:.6f}，按实际开仓价差管理；未反向平仓'
            self.save('group_opened',{'group':g['id'],'entry':g['entry']})
        else:
            error=str(second.get('error') or '')
            suffix='：'+error if error else ''
            second_label='币安' if second_leg=='binance' else 'MT5'
            g['status']='unwinding';g['reason']=f'{second_label} 拒单、部分成交或状态未知，正在保护性平仓'+suffix;self.pause(g['reason'])
            self.close_group(g,q)

    def request_close(self, group_id=None, reason='用户平仓'):
        self.state['enabled']=False
        found=False
        for g in self.active():
            if group_id is None or g['id']==group_id:
                if g['status']=='attention': g['attempts']=0
                g['status']='closing';g['reason']=reason;found=True
        if group_id and not found: raise ValueError('未找到活动交易组')
        self.save('close_requested',{'group':group_id,'reason':reason})

    def finalize_flat(self, g, q):
        """Record a completed close once both broker legs report zero exposure."""
        if not q or g.get('status') not in ('closing','unwinding') or self.uncertain(g): return False
        if max(self.amounts(g).values())>=1e-8: return False
        actual=self._record_spread(g,'close',g.get('exit_signal',q.get('exit')))
        g.update(status='closed',closed_ms=stamp(),exit=actual if actual is not None else q.get('exit'))
        g['valuation']=self.valuation(g,q)
        self.save('group_closed',{'group':g['id'],'net_estimate':g['valuation']['net']})
        return True

    def close_group(self, g, q):
        if g.get('imported') and hasattr(self,'before_import_close'):
            self.before_import_close()
            # Account reads can take longer than the quote budget. Wait for another tick.
            if any(stamp()-int(q[leg].get('observed_ms',q[leg].get('time_ms',0)))>g['parameters']['max_quote_age_ms'] for leg in ('binance','mt5')):
                self.state['alarm']='接管平仓核验完成时行情已过期，等待新报价'
                return
        if not self.resolve(g): return
        amounts=self.amounts(g)
        if max(amounts.values())<1e-8:
            self.finalize_flat(g,q);return
        if g['attempts']>=g['retry_limit']:
            g['status']='attention';self.pause('减仓重试达到上限，仍有敞口；请检查账户后使用“重试平仓”');return
        g['attempts']+=1;self.save('close_attempt',{'group':g['id'],'attempt':g['attempts']})
        # Remove a pre-existing unmatched excess first. Do not touch the
        # hedged portion until that reduction is confirmed.
        before_m, before_b = amounts['mt5'], amounts['binance']
        if before_m-before_b>1e-8:
            self.order(g,'mt5','close',before_m-before_b,q)
        elif before_b-before_m>1e-8:
            self.order(g,'binance','close',before_b-before_m,q)
        if self.uncertain(g): return
        amounts=self.amounts(g); before_m, before_b=amounts['mt5'],amounts['binance']
        if abs(before_m-before_b)>1e-8:
            # Excess close was rejected/partial. Retry it before the pair.
            return

        # For the matched portion, close MT5 first and buy back no more Binance
        # than MT5 actually closed. This prevents a rejected/partial MT5 close
        # from creating a naked MT5 long.
        if before_m>1e-8:
            self.order(g,'mt5','close',before_m,q)
            after_m=self.amounts(g)['mt5']
            mt5_closed=max(0,before_m-after_m)
            if mt5_closed>1e-8:
                self.order(g,'binance','close',min(before_b,mt5_closed),q)
        if g.get('imported') and not self.uncertain(g):
            after=self.amounts(g)
            if after['mt5']<before_m-1e-8 and abs(after['mt5']-after['binance'])<1e-8:
                g['attempts']=0
        if not self.uncertain(g) and max(self.amounts(g).values())<1e-8:
            self.finalize_flat(g,q)

    def tick(self, c, p, q):
        if self.state['recovery']: return
        if not valid_quote(q,c):
            if self.active(): self.state['alarm']='报价过期、时钟偏差或品种不符，等待有效行情；已有仓位仍未平仓'
            return
        exit_event=False
        for g in self.active():
            if g['key']!=q['key']: raise ValueError('当前行情与持仓账户或品种不一致')
            if g['status']=='opening':
                if self.resolve(g): g['status']='unwinding';g['reason']='中断后的开仓组撤销'
            g['valuation']=self.valuation(g,q)
        opened=[g for g in self.active() if g['status']=='open' and (not g.get('imported') or g.get('management_enabled'))]
        total=sum(g.get('valuation',{}).get('net',0) for g in self.active() if not g.get('imported'))
        loss=c['strategy']['total_loss_enabled'] and total<=-c['strategy']['total_max_loss_usd']
        basket=[g for g in opened if g['parameters']['exit_mode']=='basket' and not g.get('imported')]
        basket_exit=set()
        if basket:
            qty=sum(g['qty'] for g in basket); entry=sum(g['entry']*g['qty'] for g in basket)/qty
            net=sum(g['valuation']['net'] for g in basket)
            # Every member's saved condition must agree; edits affect only future groups.
            if all(self.target(g,entry,q['exit']) and (not g['parameters']['require_net_profit'] or net>=g['parameters']['min_net_profit_usd']) for g in basket):
                basket_exit={g['id'] for g in basket}
        for g in opened:
            s=g['parameters'];v=g['valuation'];reason=''
            if g.get('imported'):
                if self.target(g,g['entry'],q['exit']) and v['net']>=s['min_net_profit_usd']: reason='接管篮子收窄并盈利'
            elif loss: reason='本策略总浮动亏损上限'
            elif s['group_loss_enabled'] and v['net']<=-s['group_max_loss_usd']: reason='单组亏损上限'
            elif s['max_hold_minutes'] and stamp()-g['opened_ms']>=s['max_hold_minutes']*60000: reason='持仓超时'
            elif g['id'] in basket_exit: reason='整篮子止盈'
            elif s['exit_mode']=='group' and self.target(g,g['entry'],q['exit']) and (not s['require_net_profit'] or v['net']>=s['min_net_profit_usd']): reason='逐组止盈'
            if reason:
                g.update(status='closing',reason=reason,exit_signal=q['exit']);exit_event=True;self.save('exit_signal',{'group':g['id'],'reason':reason,'signal_spread':q['exit']})
                if reason in ('本策略总浮动亏损上限','单组亏损上限'): self.pause(reason)
        for g in self.active():
            if g['status'] in ('closing','unwinding'):
                exit_event=True;self.close_group(g,q)
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
        if (self.state['enabled'] and not exit_event and all(g['status']=='open' for g in all_active)
            and grid_allowed and len(active)<s['max_groups']
            and sum(g['lots'] for g in all_active)+p['lots']<=s['max_total_lots']+1e-9
            and stamp()-self.state['last_open_ms']>=s['cooldown_seconds']*1000 and q['entry']>=open_threshold):
            self.open(c,p,q,grid_index=grid_index,min_entry_spread=open_threshold)

    @staticmethod
    def target(g, entry, exit_spread):
        s=g['parameters']
        return entry-exit_spread>=s['take_contraction_usd'] if s['target_mode']=='contraction' else exit_spread<=s['exit_spread_usd']
