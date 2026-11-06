"""Read-only detection and exact-history backfill of platform-side closes.

No position delta is ever converted into a synthetic fill. Binance attribution
is intentionally limited to an unambiguous single group in the history window.
"""
import hashlib
import json
import math
import threading
import time

from trading_engine import Engine


def number(value):
    if isinstance(value, bool): raise ValueError('无效成交数值')
    try: value = float(value)
    except (ValueError,TypeError,OverflowError): raise ValueError('无效成交数值') from None
    if not math.isfinite(value): raise ValueError('无效成交数值')
    return value


def amounts(state, group):
    result = dict(binance=0., mt5=0.)
    for o in state['orders']:
        if o['group'] == group['id']:
            result[o['leg']] += (1 if o['action']=='open' else -1)*number(o['result'].get('qty',0))
    return result


def observation_state(state):
    active = [g for g in state['groups'] if g['status']!='closed']
    ids = {g['id'] for g in active}
    start = min((g['opened_ms'] for g in active),default=float('inf'))
    peers = [{k:g.get(k) for k in ('id','key','status','opened_ms','closed_ms','exit_rules_revision')}
             for g in state['groups'] if g['id'] not in ids and g.get('closed_ms',0)>=start]
    return dict(groups=active+peers,orders=[o for o in state['orders'] if o['group'] in ids])


def fingerprint(state):
    # Quotes, valuations, monitor messages and funding updates do not change
    # ownership. An intent, result, close request or rule edit invalidates a scan.
    state = observation_state(state)
    groups = sorted([{k:g.get(k) for k in ('id','key','status','opened_ms','closed_ms','exit_rules_revision')}
                     for g in state['groups']],key=lambda g:g['id'])
    orders=[]
    for o in state['orders']:
        row={k:o.get(k) for k in ('id','group','leg','action','requested','position','opening_id',
                                 'position_identifier','source','external_deal_ids')}
        row['result']={k:o.get('result',{}).get(k) for k in
                       ('status','qty','price','fee','swap','ticket','deal','position','position_identifier','retcode','ack_volume','ack_price')}
        orders.append(row)
    return hashlib.sha256(json.dumps([groups,orders],sort_keys=True,
                                    allow_nan=False).encode()).hexdigest()


def opening_position(order, current, magic):
    result=order['result']
    if result.get('position'): return str(result['position'])
    matches=[str(p['ticket']) for p in current.values() if p.get('magic')==magic
             and p.get('comment')==order['id'] and p.get('side')==0
             and (not result.get('position_identifier') or str(p.get('identifier'))==str(result['position_identifier']))]
    if len(matches)>1: raise ValueError('MT5 原持仓未唯一确认')
    return matches[0] if matches else ''


def observed(config, mode, positions, mt5):
    if not isinstance(positions,list) or not isinstance(mt5,dict): raise ValueError('持仓数据不完整')
    account = mt5.get('account',{})
    if (str(account.get('account')) != str(config['mt5']['account']) or
            account.get('server') != config['mt5']['server']):
        raise ValueError('MT5 核对账户不符')
    if not isinstance(mt5.get('positions'),list) or not isinstance(mt5.get('orders'),list):
        raise ValueError('MT5 持仓或挂单数据不完整')
    if mode not in ('hedge','one_way'): raise ValueError('币安持仓模式未知')
    qty = 0.
    for p in positions:
        if p.get('symbol') != config['symbol']: raise ValueError('币安持仓品种不符')
        value = number(p['positionAmt']); side = p.get('positionSide')
        if mode=='hedge':
            if value and (side!='SHORT' or value>0): raise ValueError('币安存在其他方向持仓')
        elif side!='BOTH' or value>0:
            raise ValueError('币安存在其他方向持仓')
        qty -= value
    current = {}
    for p in mt5['positions']:
        if p.get('symbol') != config['mt5']['symbol']: raise ValueError('MT5 持仓品种不符')
        if number(p['lots'])<=0 or p.get('side') not in (0,1) or not p.get('identifier'):
            raise ValueError('MT5 持仓字段无效')
        ticket = str(p['ticket'])
        if ticket in current: raise ValueError('重复 MT5 持仓编号')
        current[ticket] = p
    return qty,current


def differences(state, config, mode, positions, mt5, open_orders):
    bqty,current = observed(config,mode,positions,mt5)
    if not isinstance(open_orders,list): raise ValueError('币安挂单数据不完整')
    if open_orders or mt5['orders']: raise ValueError('平台存在未完成委托，等待人工核对')
    groups = [g for g in state['groups'] if g['status']!='closed']
    changed = abs(bqty-sum(amounts(state,g)['binance'] for g in groups))>1e-7
    claimed = set()
    for g in groups:
        remaining = {}
        for o in state['orders']:
            if o['group']!=g['id'] or o['leg']!='mt5': continue
            ticket = str(o.get('position') or o['result'].get('position') or '')
            remaining[ticket] = remaining.get(ticket,0)+(1 if o['action']=='open' else -1)*number(o['result'].get('qty',0))
        for o in state['orders']:
            if o['group']!=g['id'] or o['leg']!='mt5' or o['action']!='open' or not o['result'].get('qty'): continue
            ticket = opening_position(o,current,config['execution']['magic'])
            if ticket and ticket in claimed: raise ValueError('MT5 原持仓归属尚未唯一确认')
            if ticket: claimed.add(ticket)
            p = current.get(ticket)
            expected = remaining.get(str(o['result'].get('position') or ''),0)
            actual = number(p['lots'])*number(g['contract']) if p else 0.
            if p and (p.get('side')!=0 or p.get('magic')!=config['execution']['magic'] or p.get('comment')!=o['id']):
                raise ValueError('MT5 原持仓方向或归属变化')
            changed = changed or abs(actual-expected)>1e-7
    if any(p.get('magic')==config['execution']['magic'] and ticket not in claimed for ticket,p in current.items()):
        raise ValueError('MT5 存在日志之外的策略持仓')
    return changed


def external_order(group, leg, rows, qty, price, fee, order_id, at, position=None, identifier=None, swap=0):
    ids = sorted(str(x['ticket'] if leg=='mt5' else x['id']) for x in rows)
    key = json.dumps([group['key'],leg,order_id,ids],ensure_ascii=False)
    result = dict(status='done',qty=qty,price=price,ticket=str(order_id),fee=fee,
                  platform_time_ms=at,reconcile_time_ms=int(time.time()*1000))
    if leg=='mt5': result.update(position=position,position_identifier=identifier,swap=swap)
    return dict(id='external-'+hashlib.sha256(key.encode()).hexdigest()[:24],group=group['id'],
                leg=leg,action='close',symbol=group['symbol'] if leg=='binance' else group['mt5_symbol'],
                source='external',external_deal_ids=ids,requested=qty,created_ms=at,
                fx=group['costs']['usdt_usd'],fx_source='saved_group',result=result,
                **({'position':position,'position_identifier':identifier} if leg=='mt5' else {}))


def mt5_backfill(state, config, group, current, deals):
    own = [o for o in state['orders'] if o['group']==group['id'] and o['leg']=='mt5' and o['result'].get('qty',0)>0]
    openings = [o for o in own if o['action']=='open']
    if not openings and abs(amounts(state,group)['mt5'])<1e-8: return []
    if len(openings)!=1: raise ValueError('MT5 开仓记录未唯一确认')
    opening = openings[0]; r = opening['result']
    position_ticket = opening_position(opening,current,config['execution']['magic'])
    # Prove the immutable position identifier from the original strategy fill.
    # The order ticket is the strongest link when it is available.  Some MT5
    # brokers omit it from the acknowledgement, while still returning the
    # immutable position identifier or the original strategy comment.  The
    # read-only history worker has already proved the account and symbol; use
    # the strongest identifier present and keep the volume/position checks
    # below as the final ownership proof.
    order_ticket=str(r.get('ticket') or '')
    identifier=str(r.get('position_identifier') or '')
    starts = [d for d in deals if d.get('entry')==0 and d.get('type')==0
              and (not d.get('symbol') or d.get('symbol')==group['mt5_symbol'])
              and ((identifier and str(d.get('position_id'))==identifier) or
                   (order_ticket and str(d.get('order'))==order_ticket) or
                   (not order_ticket and not identifier and d.get('comment')==opening['id']
                    and d.get('magic')==config['execution']['magic']))]
    identifiers = {str(d['position_id']) for d in starts}
    if len(identifiers)!=1 or abs(sum(number(d['volume'])*group['contract'] for d in starts)-r['qty'])>1e-7:
        raise ValueError('MT5 原开仓成交历史尚未完整返回')
    identifier = next(iter(identifiers))
    if r.get('position_identifier') and str(r['position_identifier'])!=identifier:
        raise ValueError('MT5 原持仓标识不符')
    history = [d for d in deals if str(d.get('position_id'))==identifier]
    if len({str(d['ticket']) for d in history})!=len(history): raise ValueError('重复 MT5 成交记录')
    if any(d.get('entry') not in (0,1,3) or d.get('type') not in (0,1) for d in history):
        raise ValueError('MT5 成交类型无法安全归属')
    incoming = [d for d in history if d['entry']==0]
    if {str(d['ticket']) for d in incoming}!={str(d['ticket']) for d in starts}:
        raise ValueError('MT5 持仓存在其他加仓成交')
    exits = [d for d in history if d['entry'] in (1,3)]
    if any(d['type']!=1 for d in exits): raise ValueError('MT5 平仓方向不符')
    used = set()
    for o in own:
        if o['action']!='close': continue
        if o.get('source')=='external':
            rows = [d for d in exits if str(d['ticket']) in o['external_deal_ids']]
        else:
            rows = [d for d in exits if str(d['order'])==str(o['result'].get('ticket'))
                    and d.get('comment')==o['id'] and d.get('magic')==config['execution']['magic']]
        if not rows or abs(sum(number(d['volume'])*group['contract'] for d in rows)-o['result']['qty'])>1e-7:
            raise ValueError('MT5 已记平仓与成交历史不符')
        used.update(str(d['ticket']) for d in rows)
    p = current.get(position_ticket)
    if p and str(p.get('identifier'))!=identifier: raise ValueError('MT5 当前持仓标识不符')
    actual = number(p['lots'])*group['contract'] if p else 0
    closed = sum(number(d['volume'])*group['contract'] for d in exits)
    if abs(r['qty']-closed-actual)>1e-7 or actual>amounts(state,group)['mt5']+1e-7:
        raise ValueError('MT5 平仓历史与剩余持仓尚未一致')
    result = []
    for d in exits:
        if str(d['ticket']) in used: continue
        if any(o['group']==group['id'] and o['leg']=='mt5' and o.get('source')!='external'
               and (d.get('comment')==o['id'] or str(d['order'])==str(o['result'].get('ticket')))
               for o in state['orders']):
            raise ValueError('MT5 程序订单成交须由原订单确认流程处理')
        qty = number(d['volume'])*group['contract']; price = number(d['price']); at = int(number(d['time_msc']))
        if qty<=0 or price<=0 or at<=0 or at<min(int(x['time_msc']) for x in starts):
            raise ValueError('MT5 平仓成交字段无效')
        record=external_order(group,'mt5',[d],qty,price,-number(d['commission'])-number(d['fee']),
                              d['order'],at,position_ticket,identifier,number(d['swap']))
        record['_opening_identity']=dict(id=opening['id'],position=position_ticket,position_identifier=identifier)
        result.append(record)
    return result


def binance_backfill(state, config, group, mode, actual, fills, commission):
    # A symbol position has no per-group identifier. Do not distribute manual
    # buys among groups, including closed groups overlapping this window.
    peers = [g for g in state['groups'] if g.get('key')==group['key'] and
             g.get('closed_ms',float('inf'))>=group['opened_ms']]
    if len(peers)!=1: raise ValueError('币安多组仓位合并，手动成交无法唯一归属；请人工核对')
    own = [o for o in state['orders'] if o['group']==group['id'] and o['leg']=='binance' and o['result'].get('qty',0)>0]
    if len({str(x['id']) for x in fills})!=len(fills): raise ValueError('重复币安成交记录')
    used = set(); signed = 0.
    for f in fills:
        if f.get('symbol')!=group['symbol'] or f.get('positionSide')!=('SHORT' if mode=='hedge' else 'BOTH'):
            raise ValueError('币安成交方向或品种不符')
        qty = number(f['qty']); price = number(f['price'])
        number(f['commission'])
        if not f.get('commissionAsset'): raise ValueError('币安成交手续费币种缺失')
        if qty<=0 or price<=0 or f.get('side') not in ('BUY','SELL'): raise ValueError('币安成交字段无效')
        signed += qty if f['side']=='SELL' else -qty
    for o in own:
        rows = [f for f in fills if str(f['orderId'])==str(o['result'].get('ticket'))]
        if not rows or abs(sum(number(f['qty']) for f in rows)-o['result']['qty'])>1e-7:
            raise ValueError('币安原订单成交历史尚未完整返回')
        if any(f['side']!=('SELL' if o['action']=='open' else 'BUY') for f in rows):
            raise ValueError('币安已记订单方向不符')
        used.update(str(f['id']) for f in rows)
    if abs(signed-actual)>1e-7 or actual>amounts(state,group)['binance']+1e-7:
        raise ValueError('币安平仓历史与剩余持仓尚未一致')
    extra = [f for f in fills if str(f['id']) not in used]
    if any(str(f['orderId'])==str(o['result'].get('ticket')) for f in extra for o in state['orders']
           if o['group']==group['id'] and o['leg']=='binance' and o.get('source')!='external'):
        raise ValueError('币安程序订单成交须由原订单确认流程处理')
    if any(f['side']!='BUY' for f in extra): raise ValueError('币安存在程序之外的加仓成交')
    by_order = {}
    for f in extra: by_order.setdefault(str(f['orderId']),[]).append(f)
    result = []
    for oid,rows in by_order.items():
        qty = sum(number(f['qty']) for f in rows)
        at = max(int(number(f['time'])) for f in rows)
        if at<group['opened_ms']-60000: raise ValueError('币安平仓成交时间无效')
        result.append(external_order(group,'binance',rows,qty,
                                     sum(number(f['qty'])*number(f['price']) for f in rows)/qty,
                                     number(commission(rows)),oid,at))
    return result


def propose(state, config, mode, positions, mt5, fills, deals, commission):
    bqty,current = observed(config,mode,positions,mt5)
    groups = [g for g in state['groups'] if g['status']!='closed']
    proposals = []; errors = []
    expected_b = sum(amounts(state,g)['binance'] for g in groups)
    for g in groups:
        try: proposals.extend(mt5_backfill(state,config,g,current,deals))
        except ValueError as exc:
            # The message is generated by our validation layer and contains
            # no account credentials or native ticket values.  Keeping it
            # makes the recovery panel useful when a broker's MT5 history
            # shape differs from the common dialect.
            errors.append(str(exc) if str(exc).startswith(('MT5 原','MT5 持仓','MT5 成交','MT5 平仓'))
                          else 'MT5 成交历史或归属待核对')
        except (KeyError,TypeError,OverflowError): errors.append('MT5 成交历史或归属待核对')
    if abs(expected_b-bqty)>1e-7:
        if len(groups)!=1: errors.append('币安多组仓位合并，手动成交无法唯一归属；请人工核对')
        else:
            try: proposals.extend(binance_backfill(state,config,groups[0],mode,bqty,fills,commission))
            except ValueError as exc: errors.append(str(exc))
            except (KeyError,TypeError,OverflowError): errors.append('币安成交历史字段待核对')
    return proposals,list(dict.fromkeys(errors))


class ExternalCloseMonitor:
    """Dedicated read clients/process. Never borrows the order terminal lock."""
    INTERVAL = 5.

    def __init__(self, runtime, client, terminal_factory, start=True):
        self.runtime,self.client,self.terminal_factory = runtime,client,terminal_factory
        self.terminal = None; self.stopped = threading.Event()
        self.wake_event=threading.Event();self.retry_requested=threading.Event()
        self.history_token = None; self.history_retry_at = 0.; self.history_attempts = 0
        if start: self.start()

    def start(self):
        self.thread = threading.Thread(target=self._loop,name='gold-pair-external-closes',daemon=True)
        self.thread.start()

    def close(self):
        self.stopped.set()
        self.wake_event.set()
        if self.terminal: self.terminal.close()

    def wake(self):
        self.retry_requested.set();self.wake_event.set()

    def step(self):
        capture = self.runtime._external_capture(self)
        if not capture: return
        state,config,token = capture
        try:
            if self.terminal is None: self.terminal = self.terminal_factory()
            unknown=[o for o in state['orders'] if o.get('result',{}).get('status')!='done']
            if unknown:
                # Query original intents first. Never cancel or submit through
                # this observer, and never treat a vanished position as a fill.
                resolved=[]
                for order in unknown:
                    try:
                        result=(self.client.query(order) if order['leg']=='binance' else
                                self.terminal.call('query',order=order))
                        if (Engine.result_valid(result,order['requested']) and result.get('status')=='done'
                                and result['qty']>=order['result'].get('qty',0)):
                            resolved.append(dict(id=order['id'],result=result))
                    except Exception: pass
                self.runtime._external_publish(self,token,'resolved',resolved,[])
                return
            mode = self.client.refresh_position_mode()
            positions = self.client.positions(config['symbol']); orders = self.client.open_orders(config['symbol'])
            mt5 = self.terminal.call('snapshot')
            if not differences(state,config,mode,positions,mt5,orders):
                self.runtime._external_publish(self,token,'checked',[],[])
                return
            if not self.runtime._external_publish(self,token,'detected',[],[]): return
            if self.history_token!=token:
                self.history_token=token;self.history_attempts=0;self.history_retry_at=0.
            if self.retry_requested.is_set():
                self.retry_requested.clear();self.history_retry_at=0.
            if time.monotonic()<self.history_retry_at:
                self.runtime._external_publish(self,token,'pending',[],['成交历史或归属待确认，后台稍后重试；交易保持暂停'])
                return
            start = min(g['opened_ms'] for g in state['groups'] if g['status']!='closed')-60000
            openings = [dict(order_ticket=o['result'].get('ticket'),
                             identifier=o['result'].get('position_identifier'),
                             comment=o['id'], magic=config['execution']['magic'],
                             created_ms=o.get('created_ms'),
                             requested=o.get('requested'),
                             symbol=o.get('symbol'))
                        for o in state['orders'] if o['leg']=='mt5' and o['action']=='open' and o['result'].get('qty',0)>0]
            history_error = []
            try: deals = self.terminal.call('external_history',start_ms=start,openings=openings)
            except Exception as exc:
                deals = []
                # Keep the UI actionable without exposing native/account data.
                message = str(exc)
                allowed = ('MT5 原', 'MT5 持仓', 'MT5 成交', 'MT5 平仓')
                history_error = [message if message.startswith(allowed) else 'MT5 成交历史读取失败，请确认终端历史可见']
            try:
                expected = sum(amounts(state,g)['binance'] for g in state['groups'] if g['status']!='closed')
                actual,_ = observed(config,mode,positions,mt5)
                # Order IDs alone are not an account identity. Prove the
                # strategy's random client IDs on this authenticated account.
                for o in state['orders']:
                    if o['group'] not in {g['id'] for g in state['groups'] if g['status']!='closed'} or o['leg']!='binance' or o.get('source')=='external' or o['result'].get('qty',0)<=0:
                        continue
                    verified = self.client.query(o)
                    if (verified.get('status')!='done' or str(verified.get('ticket'))!=str(o['result'].get('ticket'))
                            or abs(number(verified.get('qty'))-number(o['result']['qty']))>1e-7):
                        raise ValueError('币安原订单归属待确认')
                fills = self.client.trade_history(config['symbol'],start,int(time.time()*1000)) if abs(expected-actual)>1e-7 else []
                identity_error = False
            except Exception:
                fills = []; identity_error = True
            # Re-read after history retrieval; changes during the scan wait for
            # the next pass. Local order changes are checked separately by token.
            again = self.client.positions(config['symbol']); pending = self.client.open_orders(config['symbol'])
            latest = self.terminal.call('snapshot')
            a = observed(config,mode,positions,mt5); b = observed(config,mode,again,latest)
            def stable(value):
                qty,ps = value
                return qty,sorted((k,str(p.get('identifier')),p.get('side'),p.get('magic'),p.get('comment'),number(p['lots'])) for k,p in ps.items())
            if stable(a)!=stable(b) or pending or latest['orders']:
                self.runtime._external_publish(self,token,'pending',[],['平台持仓仍在变化，等待下一次核对'])
                return
            proposals,errors = propose(state,config,mode,again,latest,fills,deals,self.client.commissions_usdt)
            errors = history_error + errors
            if identity_error:
                # Keep proven MT5 facts, but never claim account-wide flatness
                # or resume management when Binance ownership is unconfirmed.
                proposals = [o for o in proposals if o['leg']=='mt5']
                errors.append('币安原订单归属或成交历史待确认')
            if errors:
                self.history_retry_at=time.monotonic()+(15,30,60)[min(self.history_attempts,2)]
                self.history_attempts+=1
            else:
                self.history_retry_at=0.;self.history_attempts=0
            self.runtime._external_publish(self,token,'backfill',proposals,errors,mt5_snapshot=latest)
        except (ValueError,KeyError,TypeError,OverflowError):
            self.runtime._external_publish(self,token,'blocked',[],[])
        except Exception:
            # Never publish raw API/native exception text (may contain secrets).
            self.runtime._external_publish(self,token,'unavailable',[],['平台只读核对不可用或存在委托、归属异常，请手动持仓对账'])

    def _loop(self):
        try:
            while not self.stopped.is_set():
                self.step()
                self.wake_event.wait(self.INTERVAL)
                self.wake_event.clear()
        finally:
            if self.terminal: self.terminal.close()
