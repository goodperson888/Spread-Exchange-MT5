"""Lighter mainnet perpetual adapter, official SDK signing only.

The engine's historical `binance` leg key means the exchange leg. Venue/account
identity lives in pair_key and the journaled pair definition, never in that key.
HTTP acceptance is pending; only matching terminal order evidence confirms fills.
"""
import asyncio
import hashlib
import json
import math
import re
import threading
import time
from collections import deque
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR
from urllib.parse import urlencode, urlsplit
from urllib.request import build_opener, ProxyHandler, Request
from trading_brokers import ApiError, BinanceBookTicker, Binance, websocket
from execution_evidence import capture_receipt

BASE='https://mainnet.zklighter.elliot.ai'


class RequestBudget:
    """Process-wide rolling budget; fail fast rather than delay a trade.

    Reserve headroom for SDK nonce recovery and writes. Fresh verification
    never uses a cached account/position response to evade the limit.
    """
    def __init__(self,clock=time.monotonic):
        self.clock=clock;self.lock=threading.Lock();self.calls=deque();self.blocked_until=0

    def take(self,priority=False):
        with self.lock:
            now=self.clock()
            while self.calls and self.calls[0]<=now-60:self.calls.popleft()
            if now<self.blocked_until or len(self.calls)>=(56 if priority else 46):
                raise ApiError('LIGHTER_RATE_LIMIT','Lighter 查询额度暂不足，请等待自动重试；本次未发送请求')
            self.calls.append(now)

    def backoff(self):
        with self.lock:self.blocked_until=max(self.blocked_until,self.clock()+60)


REQUEST_BUDGET=RequestBudget()


def decimal(value):
    n=Decimal(str(value))
    if not n.is_finite(): raise ValueError('Lighter 数值无效')
    return n


def as_dict(value): return value.to_dict() if hasattr(value,'to_dict') else value


def client_index(account,order_id):
    if not order_id: raise ValueError('Lighter 下单意图缺少持久化订单 ID')
    return int.from_bytes(hashlib.sha256(f'{account}:{order_id}'.encode()).digest()[:6],'big')


def order_plan(order,spec):
    qty=decimal(order['requested']);tick=decimal(spec['filters']['PRICE_FILTER']['tickSize'])
    scale=10**spec['size_decimals'];price_scale=10**spec['price_decimals']
    ask=order['action']=='open'
    price=(decimal(order['limit'])/tick).to_integral_value(rounding=ROUND_CEILING if ask else ROUND_FLOOR)*tick
    f=spec['filters']['LOT_SIZE']
    if (qty<=0 or qty*scale!=(qty*scale).to_integral_value() or qty<decimal(f['minQty']) or
            qty>decimal(f['maxQty']) or price<=0): raise ValueError('Lighter 订单数量或保护价不符合规格')
    if qty*price<decimal(spec['filters']['MIN_NOTIONAL']['notional']):
        raise ValueError('Lighter 订单金额低于最小名义金额')
    return dict(market_index=spec['market_id'],base_amount=int(qty*scale),price=int(price*price_scale),
                qty=qty,limit=price,is_ask=ask,reduce_only=not ask,
                deadline=order.get('_submit_deadline_monotonic',time.monotonic()+1.5))


def terminal_receipt(row,plan,account,index):
    """Reject mismatched, partial-schema, overfilled or price-violating receipts."""
    unknown=dict(status='unknown',qty=0,price=0,error='Lighter 成交回报待完整核验',error_code='INVALID_RESPONSE')
    unknown['receipt']=capture_receipt('lighter',row)
    if not isinstance(row,dict): return unknown
    try:
        for key,value in [('owner_account_index',account),('market_index',plan['market_index']),('client_order_index',index)]:
            if type(row.get(key)) is not int or row[key]!=value: return unknown
        if (type(row.get('is_ask')) is not bool or row['is_ask']!=plan['is_ask'] or
            type(row.get('reduce_only')) is not bool or row['reduce_only']!=plan['reduce_only'] or
            decimal(row['initial_base_amount'])!=plan['qty']): return unknown
        if type(row.get('order_index')) is not int or row['order_index']<=0 or not row.get('order_id'): return unknown
        status=row['status'];qty=decimal(row['filled_base_amount']);quote=decimal(row['filled_quote_amount'])
        if not isinstance(status,str) or qty<0 or qty>plan['qty'] or quote<0 or (qty==0)!=(quote==0): return unknown
        final=status=='filled' or status.startswith('canceled')
        if status=='filled' and qty!=plan['qty']: return unknown
        price=quote/qty if qty else Decimal(0)
        if qty and ((plan['is_ask'] and price<plan['limit']) or (not plan['is_ask'] and price>plan['limit'])): return unknown
        return dict(status='done' if final else 'pending',qty=float(qty),price=float(price),
                    ticket=str(row['order_index']),raw_status=status,
                    receipt=capture_receipt('lighter',row))
    except (ValueError,TypeError,KeyError,ArithmeticError): return unknown


class SDKSession:
    """One loop and mutation lock per account; reads do not wait for mutations."""
    def __init__(self,account,key_index,private_key,proxy,budget=None):
        self.account,self.key_index=account,key_index
        self.budget=budget or REQUEST_BUDGET
        self.loop=asyncio.new_event_loop();self.pending=set();self.closed=False
        self.thread=threading.Thread(target=self.loop.run_forever,name='lighter-sdk',daemon=True);self.thread.start()
        async def create():
            import lighter
            self.sdk=lighter.SignerClient(url=BASE,account_index=account,api_private_keys={key_index:private_key})
            self.write_lock=asyncio.Lock();self.sends=set()
            if proxy:self.sdk.api_client.rest_client.proxy=proxy
            # Fetch the initial nonce during connection, rather than add a
            # network read to the first signal's unhedged execution window.
            await self.warmup()
            from lighter_order_stream import LighterOrderStream
            self.stream=LighterOrderStream(BASE.replace('https:','wss:')+'/stream',account,self.auth,
                lambda row,plan,owner,index: terminal_receipt(row,plan,owner,index)['status']=='done',proxy_url=proxy or None)
            self.stream.start()
        try:self.call(create(),timeout=15)
        except Exception:
            self.close();raise ValueError('Lighter 官方签名库初始化失败，请检查安装、账户索引及 API 私钥') from None

    def call(self,coro,timeout=12,mutation=False):
        future=asyncio.run_coroutine_threadsafe(coro,self.loop)
        self.pending.add(future);future.add_done_callback(self.pending.discard)
        try:return future.result(timeout)
        except Exception:
            # A timed-out send may already be accepted. Leave it owned and
            # serialized until its bounded SDK request finishes; never resend.
            if not mutation:future.cancel()
            raise ApiError('LIGHTER_REQUEST','Lighter 请求未确认，请等待账户及订单核对',uncertain=mutation) from None

    def auth(self):
        token,error=self.sdk.create_auth_token_with_expiry(api_key_index=self.key_index)
        if error or not token:raise ValueError('Lighter 账户查询签名失败')
        return token

    async def warmup(self):
        async with self.write_lock:
            self.budget.take()
            await asyncio.wait_for(self.sdk.nonce_manager.async_hard_refresh_nonce(self.key_index),5)

    async def pages(self,method,params,field,max_pages=20,priority=False):
        rows=[];cursor=None
        for _ in range(max_pages):
            args=dict(params,authorization=self.auth(),account_index=self.account,_request_timeout=5)
            if cursor:args['cursor']=cursor
            api=self.sdk.order_api
            if method=='position_funding':
                import lighter
                api=lighter.AccountApi(self.sdk.api_client)
            self.budget.take(priority)
            try:response=as_dict(await getattr(api,method)(**args))
            except Exception as exc:
                if getattr(exc,'status',None)==429:self.budget.backoff()
                raise
            if response.get('code')!=200 or not isinstance(response.get(field),list):
                raise ValueError('Lighter 账户历史返回不完整')
            rows.extend(response[field]);cursor=response.get('next_cursor')
            if not cursor:return rows
        raise ValueError('Lighter 历史超过单次核验范围，请缩小查询范围')

    async def active(self,market,priority=False):
        self.budget.take(priority)
        try:r=as_dict(await self.sdk.order_api.account_active_orders(authorization=self.auth(),
            account_index=self.account,market_id=market,_request_timeout=5))
        except Exception as exc:
            if getattr(exc,'status',None)==429:self.budget.backoff()
            raise
        if r.get('code')!=200 or not isinstance(r.get('orders'),list):raise ValueError('Lighter 活动订单未确认')
        return r['orders']

    async def find(self,market,index):
        cached=self.stream.order(market,index)
        if cached is not None:return cached
        rest=asyncio.create_task(self.find_rest(market,index))
        pushed=asyncio.create_task(self.stream.wait_order(market,index))
        try:
            done,_=await asyncio.wait((rest,pushed),return_when=asyncio.FIRST_COMPLETED)
            if pushed in done and pushed.result() is not None:return pushed.result()
            return await rest
        finally:
            for task in (rest,pushed):
                if not task.done():task.cancel()
            await asyncio.gather(rest,pushed,return_exceptions=True)

    async def find_rest(self,market,index):
        rows=await self.pages('account_inactive_orders',dict(market_id=market,limit=100),'orders',10,priority=True)
        match=[r for r in rows if r.get('client_order_index')==index]
        if not match:match=[r for r in await self.active(market,priority=True) if r.get('client_order_index')==index]
        if len(match)>1:raise ValueError('Lighter 订单身份不唯一')
        return match[0] if match else None

    async def send(self,plan,index):
        self.stream.arm(plan,index)
        task=asyncio.create_task(self.send_locked(plan,index))
        self.sends.add(task)
        def finished(t):
            self.sends.discard(t)
            if not t.cancelled():t.exception()
        task.add_done_callback(finished)
        pushed=asyncio.create_task(self.stream.wait_order(plan['market_index'],index))
        try:
            done,_=await asyncio.wait((task,pushed),return_when=asyncio.FIRST_COMPLETED)
            if pushed in done and pushed.result() is not None:return dict(accepted=True,terminal=pushed.result())
            return await asyncio.shield(task)
        finally:
            if not pushed.done():pushed.cancel()
            await asyncio.gather(pushed,return_exceptions=True)

    async def send_locked(self,plan,index):
        async with self.write_lock:
            if time.monotonic()>=plan['deadline']:return dict(accepted=False,submitted=False)
            try:self.budget.take(priority=True)
            except ApiError:return dict(accepted=False,submitted=False,rate_limited=True)
            _,response,error=await asyncio.wait_for(self.sdk.create_order(market_index=plan['market_index'],
                client_order_index=index,base_amount=plan['base_amount'],price=plan['price'],is_ask=plan['is_ask'],
                order_type=self.sdk.ORDER_TYPE_LIMIT,time_in_force=self.sdk.ORDER_TIME_IN_FORCE_IMMEDIATE_OR_CANCEL,
                reduce_only=plan['reduce_only'],order_expiry=self.sdk.DEFAULT_IOC_EXPIRY),15)
            # Error strings and signed payloads never leave this session.
            return dict(accepted=bool(not error and response and as_dict(response).get('code')==200),submitted=True)

    async def cancel(self,market,index):
        async with self.write_lock:
            self.budget.take(priority=True)
            _,response,error=await asyncio.wait_for(self.sdk.cancel_order(market_index=market,
                order_index=index),15)
            if error or not response or as_dict(response).get('code')!=200:raise ValueError('Lighter 撤单未确认')

    def close(self):
        if self.closed:return
        self.closed=True
        if hasattr(self,'sdk'):
            async def finish():
                if hasattr(self,'sends') and self.sends:
                    await asyncio.wait(tuple(self.sends),timeout=15)
                if hasattr(self,'stream'):await self.stream.close()
                remaining=[t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
                for task in remaining:task.cancel()
                if remaining:await asyncio.gather(*remaining,return_exceptions=True)
                await self.sdk.close()
            try:self.call(finish(),timeout=18)
            except Exception:pass
        self.loop.call_soon_threadsafe(self.loop.stop);self.thread.join(timeout=3)
        if not self.thread.is_alive():self.loop.close()


class Lighter:
    name='Lighter';currency='USDC';position_mode='one_way'
    quote_rest_interval=5.;external_poll_interval=10.;entry_refresh=10.;entry_ttl=20.
    def __init__(self,settings):
        self.settings=dict(settings);self.proxy_url=settings.get('proxy_url','')
        self.account=settings.get('account_index');self.key_index=settings.get('api_key_index',3)
        self.key=f'lighter:{self.account}:{self.key_index}' # No credential in identity or journal.
        self.offset=0;self._clock_anchor=None;self.sync_rtt_ms=None
        self.session=None;self.specs={};self.session_lock=threading.Lock();self.closed=False
        self.budget=REQUEST_BUDGET
        self.opener=build_opener(ProxyHandler({'http':self.proxy_url,'https':self.proxy_url})) if self.proxy_url else build_opener()

    def public(self,path,params=None,base=BASE):
        is_lighter=base==BASE
        if is_lighter:self.budget.take()
        if is_lighter:
            stage={'/api/v1/orderBooks':'合约规格','/api/v1/orderBookOrders':'盘口',
                   '/api/v1/candles':'历史行情','/api/v1/account':'账户'}.get(path,'公开')
            source='Lighter '+stage;error_code='LIGHTER_PUBLIC'
        elif base=='https://api.kraken.com':
            source='Kraken USDC/USD 汇率';error_code='FX_PUBLIC'
        else:
            source='公开辅助';error_code='PUBLIC_AUXILIARY'
        data=None
        try:
            with self.opener.open(Request(base+path+('?' +urlencode(params,doseq=True) if params else ''),
                                          headers={'Accept':'application/json','User-Agent':'GoldPairLocal/1.0'}),timeout=5) as response:
                data=json.load(response)
            if not isinstance(data,dict) or data.get('code',200)!=200:raise ValueError('接口返回错误格式或状态')
            return data
        except Exception as exc:
            status=getattr(exc,'code',None)
            if is_lighter and status==429:self.budget.backoff()
            try:
                body=json.loads(exc.read(4096)) if hasattr(exc,'read') else data
            except Exception:body={}
            if is_lighter and isinstance(body,dict) and body.get('code')==20558:
                raise ApiError('LIGHTER_REGION','Lighter 返回地区限制（20558），请核对平台服务范围与账户资格',http_status=status) from None
            reason=(f'HTTP {status}' if isinstance(status,int) else
                    '响应格式无效' if isinstance(exc,(ValueError,TypeError)) else '网络连接失败')
            raise ApiError(error_code,f'{source}接口读取失败（{reason}）',http_status=status) from None

    def sync(self):
        # The SDK signs account nonces rather than Binance recvWindow timestamps.
        self.sync_rtt_ms=None

    def spec(self,symbol):
        if symbol in self.specs:return self.specs[symbol]
        rows=self.public('/api/v1/orderBooks').get('order_books',[])
        rows=[r for r in rows if r.get('symbol')==symbol and r.get('market_type')=='perp']
        if len(rows)!=1:raise ValueError('Lighter 未找到该永续合约，请填写平台原名，如 XAU、PAXG')
        r=rows[0]
        if r.get('status')!='active' or symbol not in ('XAU','PAXG') or decimal(r.get('multiplier',0))!=1:
            raise ValueError('Lighter 只支持已核验单位的 XAU / PAXG 黄金永续合约')
        sd=int(r['supported_size_decimals']);pd=int(r['supported_price_decimals'])
        step=format(Decimal(10)**-sd,'f');tick=format(Decimal(10)**-pd,'f')
        f=dict(minQty=r['min_base_amount'],maxQty=str((2**63-1)//(10**sd)),stepSize=step)
        s=dict(symbol=symbol,status='TRADING',baseAsset=symbol,quoteAsset='USDC',market_id=int(r['market_id']),
               size_decimals=sd,price_decimals=pd,filters=dict(LOT_SIZE=f,MARKET_LOT_SIZE=f,
               PRICE_FILTER=dict(tickSize=tick),MIN_NOTIONAL=dict(notional=r['min_quote_amount'])))
        self.specs[symbol]=s;return s

    def depth(self,symbol,limit=100):
        data=self.public('/api/v1/orderBookOrders',dict(market_id=self.spec(symbol)['market_id'],limit=min(100,limit)))
        result={}
        for side in ('bids','asks'):
            levels={}
            if not isinstance(data.get(side),list):raise ValueError('Lighter 盘口缺少买卖侧')
            for r in data[side]:
                price=decimal(r['price']);qty=decimal(r['remaining_base_amount'])
                if price<=0 or qty<0:raise ValueError('Lighter 盘口字段无效')
                if qty:levels[price]=levels.get(price,Decimal(0))+qty
            result[side]=[[float(p),float(levels[p])] for p in sorted(levels,reverse=side=='bids')]
        result['time_ms']=int(time.time()*1000);return result

    def quote(self,symbol):
        book=self.depth(symbol)
        if not book['bids'] or not book['asks']:raise ValueError('Lighter 盘口为空')
        b,a=book['bids'][0],book['asks'][0]
        if b[0]>a[0]:raise ValueError('Lighter 盘口交叉')
        return dict(bid=b[0],ask=a[0],bid_qty=b[1],ask_qty=a[1],time_ms=book['time_ms'],
                    received_ms=book['time_ms'],time_basis='receipt_only')

    def chart_bars(self,symbol,period,start,end,check):
        interval={'M1':60000,'M5':300000,'M15':900000}[period]
        cursor=start;rows=[]
        for _ in range(9):
            check()
            if cursor>=end:break
            stop=min(end,cursor+499*interval)
            page=self.public('/api/v1/candles',dict(market_id=self.spec(symbol)['market_id'],
                resolution={'M1':'1m','M5':'5m','M15':'15m'}[period],start_timestamp=cursor,
                end_timestamp=stop-1,count_back=(stop-cursor)//interval,set_timestamp_to_end='false'))
            data=page.get('c')
            if not isinstance(data,list) or len(data)>499:raise ValueError('Lighter 历史 K 线不完整')
            batch=[dict(t=int(r['t']),close=float(r['c'])) for r in data]
            if any(not cursor<=r['t']<stop for r in batch):raise ValueError('Lighter 历史 K 线超出请求区间')
            rows.extend(batch);cursor=stop
        if cursor<end:raise ValueError('Lighter 历史范围超过单次上限')
        return rows

    def usdt_usd(self):
        # Legacy engine field name; value explicitly means USDC/USD for Lighter.
        # Coinbase Exchange no longer has the USDC-USD product. Use a real
        # USD order book rather than a nominal 1:1 conversion or USDC/USDT.
        r=self.public('/0/public/Ticker',dict(pair='USDCUSD'),base='https://api.kraken.com')
        try:
            if r.get('error')!=[]:raise ValueError('汇率接口返回错误')
            row=r['result']['USDCUSD']
            if not isinstance(row,dict) or any(not isinstance(row.get(k),list) or not row[k] for k in ('a','b')):
                raise ValueError('汇率盘口字段异常')
            bid=decimal(row['b'][0]);ask=decimal(row['a'][0])
            if not Decimal('.9')<bid<=ask<Decimal('1.1'):raise ValueError('汇率盘口异常')
            value=float((bid+ask)/2)
        except (KeyError,IndexError,TypeError,ValueError,ArithmeticError):
            raise ValueError('Kraken USDC/USD 汇率盘口缺失或异常，请核对汇率设置') from None
        return dict(value=value,time_ms=int(time.time()*1000),source='Kraken USDC/USD bid/ask midpoint',
                    currency='USDC',time_basis='receipt_only')

    def sdk(self):
        with self.session_lock:
            if self.closed:raise ValueError('Lighter 连接已关闭')
            if self.session is None:
                key=str(self.settings.get('api_private_key','')).strip()
                if self.account is None or not re.fullmatch(r'(?:0x)?[0-9a-fA-F]{80}',key):
                    raise ValueError('请配置 Lighter 账户索引、API Key 索引及 80 位十六进制 API 私钥（不是钱包私钥）')
                self.session=SDKSession(int(self.account),int(self.key_index),key,self.proxy_url,self.budget)
            return self.session

    def account_info(self):
        if self.account is None:raise ValueError('请填写 Lighter 账户索引')
        rows=self.public('/api/v1/account',dict(by='index',value=self.account,active_only='false')).get('accounts',[])
        if len(rows)!=1 or rows[0].get('account_index')!=self.account:raise ValueError('Lighter 账户索引不匹配')
        return rows[0]

    def api_permissions(self):
        session=self.sdk();session.call(session.active(None))
        return dict(source='Lighter authenticated read',note='只读签名验证，不保证实际下单资格')

    def preflight(self,api_permissions=None,sync_clock=True):
        if sync_clock:self.sync()
        session=self.sdk();session.call(session.active(None))
        account=self.account_info()
        return dict(position_mode='one_way',position_mode_label='单向净持仓',available_balance=float(account['available_balance']),
                    available=float(account['available_balance']),wallet=float(account['collateral']),
                    asset_mode_label='USDC 保证金',can_trade=True,source='Lighter authenticated read')

    def refresh_position_mode(self):return 'one_way'

    def commission_rate(self,symbol):
        session=self.sdk()
        async def read():
            import lighter
            self.budget.take()
            return as_dict(await lighter.AccountApi(session.sdk.api_client).account_limits(
                account_index=self.account,authorization=session.auth(),_request_timeout=5))
        data=session.call(read())
        if data.get('code')!=200:raise ValueError('Lighter 账户费率未确认')
        tick=data.get('current_taker_fee_tick')
        if tick is None or decimal(tick)!=0:
            raise ValueError('Lighter 非零费率的计量单位尚未核验，请关闭自动费率并填写账户实际吃单费率')
        return dict(taker=0.,maker=None,source='Lighter account limits (zero fee verified)')

    def positions(self,symbol):
        return self._positions(symbol,self.account_info())

    def _positions(self,symbol,account,spec=None):
        rows=account.get('positions')
        if not isinstance(rows,list):raise ValueError('Lighter 持仓字段缺失')
        result=[];market=(spec if spec is not None else self.spec(symbol))['market_id']
        for r in rows:
            if r.get('market_id')!=market:continue
            qty=decimal(r['position']);sign=r['sign']
            if qty<0 or sign not in (-1,0,1) or (qty and sign==0):raise ValueError('Lighter 持仓方向未确认')
            value=decimal(r['position_value']);mark=abs(value/qty) if qty else Decimal(0)
            result.append(dict(symbol=symbol,positionAmt=float(qty*sign),positionSide='BOTH',
                entryPrice=float(r['avg_entry_price']),markPrice=float(mark),liquidationPrice=float(r['liquidation_price']),
                unRealizedProfit=float(r['unrealized_pnl'])))
        return result

    def risk_sample(self,symbol):
        a=self.account_info()
        return dict(account=self.risk_account(a),positions=self._positions(symbol,a))

    @staticmethod
    def risk_account(a):
        # Compare cross-account equity to cross maintenance. Deposited
        # collateral excludes the valuation context and is not a substitute.
        return dict(totalMarginBalance=float(a['cross_asset_value']) if a.get('cross_asset_value') is not None else None,
            totalMaintMargin=float(a['cross_maintenance_margin_requirement']) if a.get('cross_maintenance_margin_requirement') is not None else None,
            availableBalance=float(a['available_balance']))

    def entry_check(self,order,spec):
        # One fresh account read for the combined check, never a shared cache.
        a=self.account_info();s=self.sdk();s.call(s.active(spec['market_id']))
        self.test_order(order,spec,account=a)
        return dict(position_mode='one_way')

    def open_orders(self,symbol):
        s=self.sdk();rows=s.call(s.active(self.spec(symbol)['market_id']))
        return [dict(orderId=str(r['order_index']),side='SELL' if r['is_ask'] else 'BUY',type=r['type'],
                     origQty=r['initial_base_amount'],executedQty=r['filled_base_amount'],price=r['price'],status=r['status']) for r in rows]

    def request(self,path,params=None,**kwargs):
        # Narrow compatibility for the independent risk observer, never routes writes.
        if path=='/fapi/v3/positionRisk':return self.positions(params['symbol'])
        if path=='/fapi/v3/account':
            return self.risk_account(self.account_info())
        raise ValueError('Lighter 不支持该兼容查询')

    def test_order(self,order,spec,account=None):
        plan=order_plan(order,spec);a=self.account_info() if account is None else account
        positions=self._positions(order['symbol'],a,spec)
        if any(p['positionAmt']>0 for p in positions):raise ValueError('Lighter 已有相反方向持仓，请先人工核对')
        settings=[p for p in a.get('positions',[]) if p.get('market_id')==spec['market_id']]
        if len(settings)>1:raise ValueError('Lighter 市场保证金设置重复')
        if not settings:raise ValueError('Lighter 账户实际保证金设置未返回，请先在平台核对该市场杠杆')
        imf=decimal(settings[0]['initial_margin_fraction'])/100
        if not 0<imf<=1:raise ValueError('Lighter 最低保证金比例无效')
        if decimal(a['available_balance'])<plan['qty']*plan['limit']*imf:
            raise ValueError('Lighter 可用保证金不足以覆盖交易所最低要求')
        return dict(read_only=True,note='账户与本地规格校验；Lighter 无测试订单接口，未验证真实下单资格')

    def submit(self,order,spec):
        try:plan=order_plan(order,spec);index=client_index(self.account,order['id'])
        except Exception as exc:return dict(status='done',qty=0,price=0,submitted=False,error=str(exc))
        if order.get('depth_guard'):
            try:
                provider=getattr(self,'depth_provider',None)
                book=provider() if provider else self.depth(order['symbol'])
                if provider and (not book or not 0<=int(time.time()*1000)-book['received_ms']<=order.get('max_depth_age_ms',3000)):
                    if order.get('liquidity'):
                        return dict(status='done',qty=0,price=0,submitted=False,error_code='DEPTH_CHECK_FAILED',error='Lighter 盘口缓存已过期，未发送订单')
                    # Manual/protective reductions and legacy/disabled guards
                    # retain their original REST fallback when WS is absent.
                    book=self.depth(order['symbol'])
                available=Binance.executable_depth(order,
                    dict(side='SELL' if plan['is_ask'] else 'BUY',price=str(plan['limit'])),book)
                if available+1e-12<float(plan['qty']):
                    return dict(status='done',qty=0,price=0,submitted=False,error_code='DEPTH_INSUFFICIENT',error='Lighter 保护价内盘口数量不足')
            except Exception:return dict(status='done',qty=0,price=0,submitted=False,error_code='DEPTH_CHECK_FAILED',error='Lighter 盘口深度读取失败，未发送订单')
        try:
            session=self.sdk();remaining=plan['deadline']-time.monotonic()
            if remaining<=0:return dict(status='done',qty=0,price=0,submitted=False,error='Lighter 派发前报价或开仓时限已过期')
            accepted=session.call(session.send(plan,index),timeout=min(18,max(.05,remaining)),mutation=True)
            if isinstance(accepted,dict):
                if accepted.get('submitted') is False:return dict(status='done',qty=0,price=0,submitted=False,error=(
                    'Lighter 请求额度暂不足，未发送订单' if accepted.get('rate_limited') else 'Lighter 等待签名通道期间报价或开仓时限已过期'))
                if accepted.get('terminal') is not None:
                    return terminal_receipt(accepted['terminal'],plan,self.account,index)
                accepted=accepted.get('accepted',False)
            # Give the matching private push / REST history the remainder of
            # the original entry window; acceptance alone cannot hedge MT5.
            if accepted and plan['deadline']>time.monotonic():
                try:
                    row=session.call(session.find(plan['market_index'],index),timeout=min(1.,max(.05,plan['deadline']-time.monotonic())))
                    receipt=terminal_receipt(row,plan,self.account,index)
                    if receipt['status']=='done':return receipt
                except Exception:pass
            # Acceptance alone MUST NOT release the MT5 hedge as a full fill.
            return dict(status='pending' if accepted else 'unknown',qty=0,price=0,
                        raw_status='accepted' if accepted else 'unconfirmed')
        except Exception:return dict(status='unknown',qty=0,price=0,error='Lighter 下单回执未确认，等待按原订单 ID 核对')

    def query(self,order):
        try:
            spec=self.spec(order['symbol']);plan=order_plan(order,spec);index=client_index(self.account,order['id'])
            session=self.sdk();budget=max(.05,min(12,float(order.get('query_timeout_ms',12000))/1000))
            row=session.call(session.find(spec['market_id'],index),timeout=budget)
            return terminal_receipt(row,plan,self.account,index)
        except Exception:return dict(status='unknown',qty=0,price=0,error='Lighter 订单核对尚未完成；不会重复发送')

    def cancel(self,order):
        session=self.sdk();spec=self.spec(order['symbol']);index=client_index(self.account,order['id'])
        row=session.call(session.find(spec['market_id'],index))
        if not row:raise ValueError('Lighter 原订单未找到，不能猜测撤单')
        validated=terminal_receipt(row,order_plan(order,spec),self.account,index)
        if validated['status']=='unknown':return validated
        if validated['status']!='done':session.call(session.cancel(spec['market_id'],int(row['order_index'])),timeout=18,mutation=True)
        return self.query(order)

    def normalized_trades(self,symbol,rows):
        result=[];seen=set();market=self.spec(symbol)['market_id']
        for r in rows:
            if r.get('market_id')!=market:raise ValueError('Lighter 成交市场不匹配')
            sell=r.get('ask_account_id')==self.account;buy=r.get('bid_account_id')==self.account
            if sell==buy:raise ValueError('Lighter 成交账户方向无法唯一确认')
            tid=str(r['trade_id'])
            if tid in seen:raise ValueError('Lighter 成交分页重复')
            seen.add(tid)
            maker=r['is_maker_ask']==sell
            fee=r.get('maker_fee' if maker else 'taker_fee')
            integrator=r.get('integrator_maker_fee' if maker else 'integrator_taker_fee')
            # Never turn absent or unknown fee units into a verified zero.
            known=fee is not None and decimal(fee)==0 and integrator is not None and decimal(integrator)==0
            result.append(dict(id=tid,orderId=str(r['ask_id'] if sell else r['bid_id']),symbol=symbol,
                qty=float(decimal(r['size'])),price=float(decimal(r['price'])),side='SELL' if sell else 'BUY',
                buyer=buy,positionSide='BOTH',time=int(r['timestamp']),commission=0. if known else None,
                commissionAsset='USDC',realizedPnl=float(r.get('ask_account_pnl' if sell else 'bid_account_pnl',0))))
        return result

    def trades(self,symbol,ticket):
        session=self.sdk();rows=session.call(session.pages('trades',dict(market_id=self.spec(symbol)['market_id'],
            order_index=int(ticket),sort_by='timestamp',sort_dir='desc',limit=100),'trades'))
        result=self.normalized_trades(symbol,rows)
        if any(str(r['orderId'])!=str(ticket) for r in result):raise ValueError('Lighter 成交订单 ID 不匹配')
        return result

    def trade_history(self,symbol,start,end):
        session=self.sdk();rows=session.call(session.pages('trades',dict(market_id=self.spec(symbol)['market_id'],
            sort_by='timestamp',sort_dir='desc',limit=100),'trades'))
        return [r for r in self.normalized_trades(symbol,rows) if start<=r['time']<=end]

    @staticmethod
    def commissions_usdt(fills):
        if not fills or any(r.get('commission') is None or r.get('commissionAsset')!='USDC' for r in fills):
            raise ValueError('Lighter 实际成交手续费尚未核验，保留原估算')
        return sum(float(r['commission']) for r in fills)

    def income(self,symbol,start,end):
        session=self.sdk();rows=session.call(session.pages('position_funding',dict(market_ids=[self.spec(symbol)['market_id']],
            start_timestamp=int(start),end_timestamp=int(end),limit=100),'position_fundings'))
        result=[]
        for r in rows:
            if r.get('market_id')!=self.spec(symbol)['market_id']:raise ValueError('Lighter 资金费市场不匹配')
            # Official field is signed change, not a funding rate estimate.
            result.append(dict(incomeType='FUNDING_FEE',time=int(r['timestamp']),income=float(decimal(r['change'])),asset='USDC'))
        return result

    def force_orders(self,symbol,start,end):
        # No equivalent forced-order endpoint verified. Unknown is not manual.
        raise ValueError('Lighter 尚无已核验的强平成交分类，外部平仓原因保持未知')

    def public_ip(self):
        r=self.public('',base='https://api.ipify.org?format=json');return r['ip']

    def close(self):
        with self.session_lock:
            self.closed=True
            if self.session:self.session.close();self.session=None


class LighterBookTicker(BinanceBookTicker):
    def __init__(self,market_id,proxy_url=''):
        super().__init__('LIGHTER',True,proxy_url);self.market_id=market_id
        self.levels={'bids':{},'asks':{}};self.offset=None;self.nonce=None

    def handle(self,message):
        kind=message.get('type')
        if kind not in ('subscribed/order_book','update/order_book'):return False
        if message.get('channel')!=f'order_book:{self.market_id}':return False
        data=message['order_book'];offset=data['offset'];nonce=data['nonce']
        if type(offset) is not int or type(nonce) is not int:raise ValueError('盘口序列缺失')
        snapshot=kind=='subscribed/order_book'
        if not snapshot and (self.offset is None or offset<=self.offset or data.get('begin_nonce')!=self.nonce or nonce<=self.nonce):
            raise ValueError('盘口序列中断，重新订阅')
        levels={'bids':{},'asks':{}} if snapshot else {k:dict(v) for k,v in self.levels.items()}
        for side in ('bids','asks'):
            for r in data.get(side,[]):
                price=decimal(r['price']);size=decimal(r['size'])
                if price<=0 or size<0:raise ValueError('盘口数值无效')
                if size:levels[side][price]=size
                else:levels[side].pop(price,None)
        if not levels['bids'] or not levels['asks']:raise ValueError('盘口为空')
        b=max(levels['bids']);a=min(levels['asks'])
        if b>a:raise ValueError('盘口交叉')
        now=int(time.time()*1000);stamp=message.get('timestamp')
        if stamp is None:stamp=now
        if isinstance(stamp,bool) or not isinstance(stamp,(int,float)) or not math.isfinite(stamp):raise ValueError('盘口时间无效')
        q=dict(bid=float(b),ask=float(a),bid_qty=float(levels['bids'][b]),ask_qty=float(levels['asks'][a]),
               time_ms=int(stamp),received_ms=now,source_sequence=f'{offset}:{nonce}',time_basis='source' if message.get('timestamp') else 'receipt_only')
        with self.lock:
            self.levels=levels;self.offset=offset;self.nonce=nonce
            self.value=q;self.last_received_ms=now;self.connection_state='streaming';self.error=''
        self.ready.set()
        if self.on_quote:self.on_quote()
        return True

    def depth_snapshot(self):
        # Published maps are immutable: handle() replaces them, never edits them.
        with self.lock:
            if self.connection_state!='streaming': return None
            levels=self.levels;received=self.last_received_ms;offset=self.offset
        # Sorting outside the lock keeps quote publication independent.
        return dict(bids=tuple((float(p),float(n)) for p,n in sorted(levels['bids'].items(),reverse=True)[:100]),
                    asks=tuple((float(p),float(n)) for p,n in sorted(levels['asks'].items())[:100]),
                    received_ms=received,offset=offset)

    def _run(self):
        backoff=1
        while not self.stop_event.is_set():
            ws=None
            try:
                self.connection_state='connecting';self.offset=None;self.nonce=None
                options=dict(timeout=5,enable_multithread=True)
                if self.proxy_url:
                    p=urlsplit(self.proxy_url);options.update(http_proxy_host=p.hostname,http_proxy_port=p.port,proxy_type='http')
                ws=websocket.create_connection('wss://mainnet.zklighter.elliot.ai/stream',**options)
                with self.lock:self.socket=ws;self.connection_state='subscribed'
                ws.send(json.dumps(dict(type='subscribe',channel=f'order_book/{self.market_id}')));ws.settimeout(30)
                while not self.stop_event.is_set():
                    message=json.loads(ws.recv())
                    if message.get('type')=='ping':ws.send(json.dumps(dict(type='pong')));continue
                    if message.get('error') or message.get('type')=='error':raise ValueError('订阅失败')
                    if self.handle(message):backoff=1
            except Exception as exc:
                with self.lock:
                    self.value=None;self.ready.clear();self.connection_state='reconnecting';self.reconnects+=1
                    try:code=json.loads(getattr(exc,'resp_body',b'{}') or b'{}').get('code')
                    except Exception:code=None
                    self.error=('Lighter 返回地区限制（20558），请核对平台服务范围与账户资格' if code==20558 else
                                f'Lighter WebSocket 中断（{type(exc).__name__}），自动重新订阅')
            finally:
                with self.lock:self.socket=None
                try:
                    if ws:ws.close()
                except Exception:pass
            if self.stop_event.wait(backoff):break
            backoff=min(30,backoff*2)
