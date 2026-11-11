"""Cost calculations on an isolated observation; no execution or connection lifecycle."""
import math
import time
from trading_config import pair_key

def now_ms(): return int(time.time()*1000)


class MaintenanceWork:
    def _update_live_carry(self, positions, refresh_funding=True):
        if self.config['execution']['mode']!='live': return
        active=self.engine.active();orders=self.engine.state['orders']
        for group in active:
            if group.get('imported'):
                tickets=self.engine.mt5_tickets(group)
                current=sum(float(p.get('swap',0)) for p in positions if str(p['ticket']) in tickets)
                realized=sum(float(o.get('result',{}).get('swap',0)) for o in orders
                             if o['group']==group['id'] and o['leg']=='mt5' and o['action']=='close')
                group['live_mt5_swap_usd']=current+realized
                continue
            opened=next((o for o in orders if o['group']==group['id'] and o['leg']=='mt5'
                         and o['action']=='open' and o.get('result',{}).get('qty',0)>0),None)
            ticket=str(opened.get('result',{}).get('position','')) if opened else ''
            comment=opened['id'] if opened else ''
            swap=sum(float(p.get('swap',0)) for p in positions
                     if str(p.get('ticket',''))==ticket or p.get('comment')==comment)
            swap+=sum(float(o.get('result',{}).get('swap',0)) for o in orders
                      if o['group']==group['id'] and o.get('source')=='external' and o['leg']=='mt5')
            group['live_mt5_swap_usd']=swap
            group['live_carry_usd']=swap+group.get('live_binance_funding_usd',0)
        if not refresh_funding or not active or time.monotonic()-self._last_carry_refresh<60: return
        try:
            start=min(g['opened_ms'] for g in active);end=now_ms()
            rows=self.binance.income(self.config['symbol'],start,end)
            for group in active: group['live_binance_funding_usd']=0
            for item in rows:
                if item.get('incomeType')!='FUNDING_FEE': continue
                at=int(item.get('time',0));peers=[g for g in getattr(self, 'funding_peers', self.engine.state['groups'])
                    if g.get('mode')=='live' and g.get('symbol')==self.config['symbol']
                    and g.get('open_binance') is not None and g['opened_ms']<=at<=g.get('closed_ms',at)]
                total=sum(float(g.get('qty',0)) for g in peers)
                if not total: continue
                for group in active:
                    if any(peer['id']==group['id'] for peer in peers):
                        group['live_binance_funding_usd']+=float(item.get('income',0))*group['qty']/total*self.config['costs']['usdt_usd']
            for group in active:
                group['live_carry_usd']=group.get('live_mt5_swap_usd',0)+group.get('live_binance_funding_usd',0)
            self._last_carry_refresh=time.monotonic();self.market_meta.pop('carry_warning',None)
        except Exception as exc:
            self.market_meta['carry_warning']='实盘持仓费更新待重试：'+str(exc)
            self._last_carry_refresh=time.monotonic()

    def _verify_closed_costs(self):
        """Replace estimates with broker deal records after a completed live group.

        Binance commissions are matched to the exact order IDs. MT5 deals are
        matched to our unique comments and magic number. Funding has no order
        ID in Binance's income feed, so it is only attributed while this app
        is the sole managed position for this symbol (enforced by reconcile).

        Funding fees are calculated by exact time window to avoid double counting
        when multiple groups have overlapping holding periods.
        """
        if self.config['execution']['mode'] != 'live':
            return
        if any(g['status']!='open' or self.engine.uncertain(g) for g in self.engine.active()):
            # Even a different group's fee history must not delay a pending
            # hedge confirmation or protective close.
            return
        for group in self.engine.state['groups']:
            if group.get('mode') != 'live' or group.get('key') != pair_key(self.config):
                continue
            if group.get('imported'):
                if (group['status']=='closed' and not group.get('import_closing_costs_checked')
                        and now_ms()-group.get('import_cost_check_ms',0)>=60000):
                    group['import_cost_check_ms']=now_ms()
                    try:
                        self._verify_import_costs(group)
                    except Exception as exc:
                        group['cost_verification_error']=str(exc)
                        self.engine.save('cost_verification_pending', {'group': group['id']})
                continue
            if group['status'] != 'closed' or group.get('costs_verified'):
                continue
            if now_ms()-group.get('cost_check_ms',0)<60000:
                continue
            group['cost_check_ms']=now_ms()
            try:
                by_id = {o['id']: o for o in self.engine.state['orders'] if o['group'] == group['id']}
                fees = {}
                for order in by_id.values():
                    if order['leg'] != 'binance' or order['result'].get('qty',0)<=0:
                        continue
                    if order.get('source')=='external':
                        # Already read and position-validated by the independent
                        # observer; no additional history on the execution lane.
                        if not order.get('external_deal_ids') or not math.isfinite(order['result']['fee']):
                            raise ValueError('外部成交费用记录待核验')
                        fees[order['id']] = order['result']['fee']
                        continue
                    if not order['result'].get('ticket'):
                        raise ValueError('币安成交订单号尚未确认')
                    fills = self.binance.trades(order['symbol'], order['result']['ticket'])
                    if not fills or abs(sum(float(x.get('qty',0)) for x in fills)-order['result']['qty'])>1e-7:
                        raise ValueError('币安成交费用记录尚未完整返回')
                    fees[order['id']] = self.binance.commissions_usdt(fills)
                mt5_orders = [o for o in by_id.values() if o['leg']=='mt5' and o['result'].get('qty',0)>0]
                deals = self.terminal.call('history', start_ms=group['opened_ms'] - 60000) if mt5_orders else []
                mt_swap = 0
                for order in mt5_orders:
                    if order.get('source')=='external':
                        if not order.get('external_deal_ids') or not math.isfinite(order['result']['fee']):
                            raise ValueError('外部成交费用记录待核验')
                        fees[order['id']] = order['result']['fee']
                        mt_swap += order['result'].get('swap',0)
                        continue
                    rows = [x for x in deals if x.get('comment') == order['id']]
                    if not rows or abs(sum(float(x.get('volume',0))*group['contract'] for x in rows)-order['result']['qty'])>1e-7:
                        raise ValueError('MT5 成交费用记录尚未完整返回')
                    fees[order['id']] = -sum(float(x.get('commission', 0)) + float(x.get('fee', 0)) for x in rows)
                    mt_swap += sum(float(x.get('swap', 0)) for x in rows)

                # Funding is reported for the whole symbol position. Allocate
                # each event by group quantity among all groups alive then, so
                # overlapping groups do not each claim the full account fee.
                window_start = group['opened_ms']
                window_end = group['closed_ms'] + 1000
                income = self.binance.income(group['symbol'], window_start, window_end)
                funding = 0
                for item in income:
                    if item.get('incomeType') == 'FUNDING_FEE':
                        fee_time = item.get('time', 0)
                        if window_start <= fee_time <= window_end:
                            peers = [x for x in getattr(self, 'funding_peers', self.engine.state['groups'])
                                     if x.get('mode') == 'live' and x.get('symbol') == group['symbol']
                                     and x.get('open_binance') is not None
                                     and x['opened_ms'] <= fee_time <= x.get('closed_ms', fee_time)]
                            total_qty = sum(float(x.get('qty', 0)) for x in peers)
                            share = float(group['qty']) / total_qty if total_qty else 1
                            funding += float(item.get('income', 0)) * share

                if not all(math.isfinite(x) for x in (*fees.values(),mt_swap,funding)):
                    raise ValueError('成交费用记录包含无效数字')
                # Publish only a complete set. A delayed MT5/funding record
                # must not partially overwrite already known commissions.
                for order_id, fee in fees.items():
                    by_id[order_id]['result']['fee'] = fee
                    by_id[order_id]['result']['fee_verified'] = True
                group['mt5_swap_usd'] = mt_swap
                group['binance_funding_usd'] = funding * group['costs']['usdt_usd']
                group['carry_usd'] = group['mt5_swap_usd'] + group['binance_funding_usd']
                group['costs_verified'] = True
                group.pop('cost_verification_error',None)
                group['valuation'] = self.engine.valuation(group, self.quote)
                self.engine.save('costs_verified', {'group': group['id'], 'net': group['valuation']['net']})
            except Exception as exc:
                # Position is already flat; retain estimates and surface the
                # verification failure without treating it as a trade failure.
                group['cost_verification_error'] = str(exc)
                self.engine.save('cost_verification_pending', {'group': group['id']})

    def _verify_import_costs(self, group):
        """Verify trades sent after adoption, while retaining the explicit original cost estimates."""
        orders=[o for o in self.engine.state['orders'] if o['group']==group['id'] and o['action']=='close']
        deals=self.terminal.call('history',start_ms=group['opened_ms']-60000)
        for o in orders:
            if o['result'].get('qty',0)<=0: continue
            if o['leg']=='binance':
                fills=self.binance.trades(o['symbol'],o['result']['ticket'])
                if not fills or abs(sum(float(x.get('qty',0)) for x in fills)-o['result']['qty'])>1e-7:
                    raise ValueError('币安接管平仓成交费用尚未完整返回')
                o['result']['fee']=self.binance.commissions_usdt(fills)
            else:
                rows=[d for d in deals if d.get('comment')==o['id']]
                if abs(sum(float(d['volume'])*group['contract'] for d in rows)-o['result']['qty'])>1e-7:
                    raise ValueError('MT5 接管平仓成交记录尚未完整返回')
                o['result']['fee']=-sum(float(d.get('commission',0))+float(d.get('fee',0)) for d in rows)
                o['result']['swap']=sum(float(d.get('swap',0)) for d in rows)
        group['live_mt5_swap_usd']=sum(o['result'].get('swap',0) for o in orders if o['leg']=='mt5')
        funding=0
        for item in self.binance.income(group['symbol'],group['opened_ms'],group['closed_ms']+1000):
            at=int(item.get('time',0))
            if item.get('incomeType')!='FUNDING_FEE' or not group['opened_ms']<=at<=group['closed_ms']: continue
            peers=[g for g in getattr(self, 'funding_peers', self.engine.state['groups']) if g.get('mode')=='live' and g['symbol']==group['symbol']
                   and g['opened_ms']<=at<=g.get('closed_ms',at)]
            total=sum(g['qty'] for g in peers)
            if total: funding+=float(item['income'])*group['qty']/total*group['costs']['usdt_usd']
        group['live_binance_funding_usd']=funding
        group['import_closing_costs_checked']=True
        group['costs_verified']=False
        group.pop('cost_verification_error',None)
        group['valuation']=self.engine.valuation(group,self.quote)
        self.engine.save('adoption_closing_costs_checked',{'group':group['id']})
