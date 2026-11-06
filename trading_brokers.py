"""Exchange/terminal adapters. All writes are invoked only by an armed engine."""
import copy
import hashlib
import hmac
import ipaddress
import json
import math
import queue
import subprocess
import sys
import threading
import time
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR
from pathlib import Path
from urllib.parse import urlencode, urlsplit
from urllib.request import ProxyHandler, Request, build_opener
from urllib.error import HTTPError
from liquidation_risk import mt5_risk_fields


def _utc_datetime_from_timestamp(seconds):
    """Build a UTC datetime without platform-specific timestamp limits.

    ``datetime.fromtimestamp`` delegates to the C runtime. On Windows it
    rejects timestamps before the Unix epoch, while Linux accepts them. The
    history fallback probes a day on either side of a journaled opening time,
    so old records can legitimately produce a pre-epoch lower bound. Pure
    datetime arithmetic is consistent everywhere.
    """
    return datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(seconds=float(seconds))

try:
    import websocket
except ImportError:  # REST fallback remains available for source deployments.
    websocket = None


class ApiError(Exception):
    def __init__(self, code, message, uncertain=False):
        super().__init__(message)
        self.code, self.uncertain = code, uncertain


class Binance:
    # recvWindow is Binance's signed-request validity window; it is not a
    # limit on the round-trip time of the public clock endpoint.  A 1,000 ms
    # recvWindow therefore must not make a perfectly usable 600 ms network
    # path fail clock sync.  Keep a separate upper bound below the HTTP
    # timeout so genuinely stalled routes are still rejected early.
    MAX_SYNC_RTT_MS = 4500

    def __init__(self, production=False, key='', secret='', recv_window_ms=5000, proxy_url=''):
        self.base = 'https://fapi.binance.com' if production else 'https://demo-fapi.binance.com'
        self.key, self.secret, self.offset = key, secret, 0
        self._clock_anchor = None
        self.sync_rtt_ms = None
        self.recv_window_ms = int(recv_window_ms)
        self.position_mode = 'one_way'
        self.proxy_url = str(proxy_url or '').strip()
        proxies = {'http': self.proxy_url, 'https': self.proxy_url} if self.proxy_url else {}
        self.opener = build_opener(ProxyHandler(proxies)) if proxies else build_opener()

    def request(self, path, params=None, method='GET', signed=False, base=None, _time_retry=True, timeout=5):
        # Keep the caller's parameters separate from the per-request timestamp
        # so a one-time -1021 retry can rebuild a fresh signature safely.
        original_params = dict(params or {})
        params = dict(original_params)
        if signed:
            if not self.key or not self.secret:
                raise ValueError('请先配置当前网络的币安凭据')
            anchor = self._clock_anchor
            timestamp = int(anchor[0]+(time.monotonic()-anchor[1])*1000) if anchor else int(time.time()*1000)+self.offset
            params.update(timestamp=timestamp, recvWindow=self.recv_window_ms)
        query = urlencode(params)
        if signed:
            query += '&signature='+hmac.new(self.secret.encode(), query.encode(), hashlib.sha256).hexdigest()
        req = Request((base or self.base)+path+('?' + query if query else ''), method=method,
                      headers={'X-MBX-APIKEY':self.key} if signed else {})
        try:
            with self.opener.open(req, timeout=timeout) as res:
                return json.load(res)
        except HTTPError as exc:
            try:
                payload=json.loads(exc.read()); code=payload.get('code', exc.code)
            except Exception:
                code=exc.code
            if signed and code == -1021 and _time_retry:
                # The lightweight connection check uses a temporary client and
                # may have corrected its own clock offset while the long-lived
                # trading client drifted.  Re-sync once, then rebuild the
                # signed request.  A -1021 response is rejected by Binance, so
                # this does not duplicate an accepted order.
                self.sync()
                return self.request(path, original_params, method, signed, base, _time_retry=False)
            timing = f'最近校时 RTT={self.sync_rtt_ms} ms，时钟偏移={self.offset} ms' if self.sync_rtt_ms is not None else '尚未取得校时 RTT'
            message = (f'币安接口错误 -1021：请求时间校验失败（已同步服务器时间并重试一次，recvWindow={self.recv_window_ms} ms，{timing}）；请检查 Windows 时间同步、代理和网络延迟'
                       if code == -1021 else f'币安接口错误 {code}')
            if code == -2015:
                message += ('：API Key、出口 IP 或权限校验未通过，币安未指明具体是哪一项。\n'
                            '1. 在币安「API 管理」确认项目填写的是当前有效的 API Key。\n'
                            '2. 确认这把 Key 已开启「允许合约」并保存。\n'
                            '3. 点击「获取币安出口 IP」，确认该 IP 已加入这把 Key 的白名单；更换代理或网络后需重新检测。\n'
                            '完成后再次点击「检查币安连接」。')
            if code == -4411:
                message += ('：币安要求先签署相关交易协议（黄金等 TradFi 永续合约）。\n'
                            '1. 用 API Key 所属账户登录币安 App 或网页版，打开对应合约（如 XAUUSDT）。\n'
                            '2. 阅读并确认页面提示的 TradFi 永续合约协议；无需为此实际下单。\n'
                            '3. 返回本程序等待下一次预检；仍失败时重新连接并对账。\n'
                            '若没有协议入口或签署后仍报错，请联系币安客服确认该账户的合约资格。')
            raise ApiError(code, message, exc.code >= 500 or code in (-1006, -1007)) from None
        except Exception:
            raise ApiError('NETWORK', '币安请求超时或网络不可用，交易结果需要查询确认', True) from None

    def sync(self):
        before=time.monotonic()
        payload=self.request('/fapi/v1/time')
        t=payload.get('serverTime') if isinstance(payload, dict) else None
        after=time.monotonic()
        rtt=(after-before)*1000
        try:
            server_time=float(t)
        except (TypeError, ValueError):
            server_time=float('nan')
        if not math.isfinite(server_time) or server_time < 1_000_000_000_000:
            raise ValueError('币安校时返回的服务器时间无效，请检查网络后重试')
        if rtt > self.MAX_SYNC_RTT_MS:
            raise ValueError(f'币安校时往返耗时 {round(rtt)} ms，超过安全上限 {self.MAX_SYNC_RTT_MS} ms；请检查网络或代理')
        server_now=server_time+rtt/2
        self._clock_anchor=(server_now,after)
        self.offset=round(server_now-time.time()*1000)
        self.sync_rtt_ms=round(rtt)

    def public_ip(self):
        """Return the public egress IP observed through this client's route."""
        req = Request('https://api.ipify.org?format=json',
                      headers={'Accept':'application/json','User-Agent':'GoldPairLocal/1'})
        try:
            with self.opener.open(req, timeout=8) as res:
                value = str(json.load(res).get('ip', '')).strip()
            ipaddress.ip_address(value)
            return value
        except Exception:
            raise ApiError('NETWORK', '无法检测公网出口 IP，请检查代理后重试', False) from None

    def spec(self, symbol):
        items=self.request('/fapi/v1/exchangeInfo').get('symbols', [])
        s=next((x for x in items if x['symbol']==symbol), None)
        if not s:
            raise ValueError('币安未找到该合约，请核对名称')
        return {**s, 'filters':{x['filterType']:x for x in s['filters']}}

    def quote(self, symbol):
        r=self.request('/fapi/v1/ticker/bookTicker', {'symbol':symbol})
        return dict(bid=float(r['bidPrice']), ask=float(r['askPrice']), time_ms=int(r['time']),
                    bid_qty=float(r['bidQty']), ask_qty=float(r['askQty']))

    def depth(self, symbol, limit=20):
        """Read a shallow public order book for an execution-side liquidity check."""
        r=self.request('/fapi/v1/depth', {'symbol':symbol, 'limit':int(limit)})
        return {
            'bids': [(float(price), float(qty)) for price, qty in (r.get('bids') or [])],
            'asks': [(float(price), float(qty)) for price, qty in (r.get('asks') or [])],
        }

    def usdt_usd(self):
        r=self.request('/fapi/v1/assetIndex', {'symbol':'USDTUSD'})
        value=float(r['index'])
        if not .5<=value<=1.5: raise ValueError('USDT/USD 指数超出安全范围')
        return dict(value=value,time_ms=int(r['time']),source='Binance USDTUSD asset index')

    def commission_rate(self, symbol):
        r=self.request('/fapi/v1/commissionRate', {'symbol':symbol}, signed=True)
        return dict(maker=float(r['makerCommissionRate'])*100,
                    taker=float(r['takerCommissionRate'])*100)

    def api_permissions(self):
        """Read safe permission flags for the current key; never returns credentials or IPs."""
        r=self.request('/sapi/v1/account/apiRestrictions', signed=True,
                       base='https://api.binance.com')
        return {
            'enable_reading':bool(r.get('enableReading')),
            'enable_futures':bool(r.get('enableFutures')),
            'ip_restricted':bool(r.get('ipRestrict')),
            'trading_authority_expiration_time':int(r.get('tradingAuthorityExpirationTime') or 0),
        }

    def preflight(self, api_permissions=None, sync_clock=True):
        if api_permissions and api_permissions.get('enable_futures') is False:
            raise ValueError('当前 API Key 未开启合约权限（enableFutures=false）。\n'
                             '请到币安「API 管理」，找到项目中填写的这把 Key，开启「允许合约」并保存。\n'
                             '同时点击「获取币安出口 IP」，确认该 IP 已加入这把 Key 的白名单，再点击「检查币安连接」。')
        if sync_clock: self.sync()
        a=self.request('/fapi/v3/account', signed=True)
        # V3 supplies balances/positions; permission and mode flags live in
        # accountConfig. Missing fields must never be reported as false.
        configuration=self.request('/fapi/v1/accountConfig', signed=True)
        if type(configuration.get('canTrade')) is not bool:
            raise ValueError('账户配置接口未返回有效 canTrade，暂不能确认交易权限；这不等同于 canTrade=false')
        if not configuration['canTrade']:
            if api_permissions and api_permissions.get('enable_futures'):
                raise ValueError('当前 API Key 已允许合约（enableFutures=true），但 /fapi/v1/accountConfig 明确返回 canTrade=false；具体原因未由接口提供，请向币安核实账户交易状态；本次不是 IP 白名单拒绝响应')
            raise ValueError('USDⓈ-M 合约账户返回 canTrade=false；API Key 权限明细未能读取，请检查主/子账户、组合保证金及账户风控状态')
        assets=list(a.get('assets') or [])
        usdt=next((x for x in assets if str(x.get('asset','')).upper()=='USDT'),None)
        if not usdt:
            raise ValueError('币安合约账户未返回 USDT 资产信息')
        if type(configuration.get('multiAssetsMargin')) is not bool:
            raise ValueError('账户配置接口未返回有效保证金模式，暂不能完成核验')
        multi=configuration['multiAssetsMargin']
        non_usdt=[]
        for item in assets:
            asset=str(item.get('asset','')).upper()
            if not asset or asset=='USDT': continue
            amounts=[float(item.get(key,0) or 0) for key in (
                'walletBalance','crossWalletBalance','unrealizedProfit','crossUnPnl','marginBalance',
                'initialMargin','maintMargin','positionInitialMargin','openOrderInitialMargin')]
            if any(not math.isfinite(value) or abs(value)>1e-8 for value in amounts): non_usdt.append(asset)
        if multi and non_usdt:
            names='、'.join(sorted(set(non_usdt)))
            raise ValueError(f'多资产模式检测到非 USDT 资产余额、盈亏或占用保证金：{names}；请清零后重试')
        dual_side=self.refresh_position_mode()=='hedge'
        return {
            'can_trade':True,
            'position_mode':self.position_mode,
            'position_mode_label':'双向持仓（对冲模式）' if dual_side else '单向持仓',
            'available':float(usdt.get('availableBalance',a.get('availableBalance',0)) or 0),
            'wallet':float(usdt.get('walletBalance',a.get('totalWalletBalance',0)) or 0),
            'asset_mode':'multi' if multi else 'single',
            'asset_mode_label':'多资产模式（已核验仅 USDT 非零）' if multi else '单资产模式',
            'non_usdt_assets':non_usdt,
            'account_available_usd':float(a.get('availableBalance',0) or 0),
            'account_wallet_usd':float(a.get('totalWalletBalance',0) or 0),
        }

    def refresh_position_mode(self):
        dual_flag=self.request('/fapi/v1/positionSide/dual', signed=True).get('dualSidePosition')
        if type(dual_flag) is not bool:
            raise ValueError('持仓模式接口未返回有效 dualSidePosition，暂不能安全判断订单参数；本次未下单')
        self.position_mode='hedge' if dual_flag else 'one_way'
        return self.position_mode

    def positions(self, symbol):
        return self.request('/fapi/v3/positionRisk', {'symbol':symbol}, signed=True)

    def open_orders(self, symbol):
        return self.request('/fapi/v1/openOrders', {'symbol':symbol}, signed=True)

    @staticmethod
    def normalize(r):
        status=r['status']; qty=float(r.get('executedQty',0)); price=float(r.get('avgPrice',0))
        if qty > 0 and price <= 0:
            price = float(r.get('cumQuote', 0)) / qty
        final=status in ('FILLED','CANCELED','EXPIRED','EXPIRED_IN_MATCH','REJECTED')
        return dict(status='done' if final else 'pending', qty=qty, price=price,
                    ticket=str(r.get('orderId','')), raw_status=status)

    @staticmethod
    def executable_depth(order, args, book):
        """Return quantity available at prices executable by this IOC order."""
        side=args['side']; limit=float(args['price'])
        rows=book['bids'] if side=='SELL' else book['asks']
        if side=='SELL':
            return sum(qty for price, qty in rows if price >= limit)
        return sum(qty for price, qty in rows if price <= limit)

    def order_args(self, order, spec):
        side='SELL' if order['action']=='open' else 'BUY'
        tick=Decimal(spec['filters']['PRICE_FILTER']['tickSize'])
        limit=Decimal(str(order['limit']))
        price=(limit/tick).to_integral_value(rounding=ROUND_CEILING if side=='SELL' else ROUND_FLOOR)*tick
        args=dict(symbol=order['symbol'],side=side,type='LIMIT',timeInForce='IOC',
                  quantity=format(Decimal(str(order['requested'])), 'f'),price=format(price,'f'),
                  newClientOrderId=order.get('id') or 'check'+uuid.uuid4().hex[:24],newOrderRespType='RESULT')
        if self.position_mode=='hedge':
            args['positionSide']=str(order.get('position_side') or 'SHORT').upper()
        elif side=='BUY':
            args['reduceOnly']='true'
        return args

    def test_order(self, order, spec):
        # Production validation endpoint; explicitly never use /order here.
        return self.request('/fapi/v1/order/test', self.order_args(order, spec), 'POST', True)

    def submit(self, order, spec):
        args=self.order_args(order,spec)
        try:
            if order.get('depth_guard'):
                try:
                    book=self.depth(order['symbol'], limit=20)
                    available=self.executable_depth(order,args,book)
                except Exception as exc:
                    return {'status':'done','qty':0,'price':0,'submitted':False,
                            'error':'币安下单前盘口深度检查失败，未发送订单：'+str(exc),
                            'error_code':'DEPTH_CHECK_FAILED'}
                if available + 1e-12 < float(order['requested']):
                    return {'status':'done','qty':0,'price':0,'submitted':False,
                            'raw_status':'DEPTH_INSUFFICIENT',
                            'error':f'滑点范围内盘口数量不足：可成交约 {available:g}，申请 {float(order["requested"]):g}',
                            'error_code':'DEPTH_INSUFFICIENT',
                            'depth_available':available}
            return self.normalize(self.request('/fapi/v1/order', args, 'POST', True))
        except ApiError as exc:
            if exc.uncertain: return {'status':'unknown','qty':0,'price':0,'error':str(exc)}
            return {'status':'done','qty':0,'price':0,'error':str(exc)}

    def query(self, order):
        try:
            options={}
            if order.get('query_timeout_ms'):
                options=dict(timeout=max(.001,min(5,order['query_timeout_ms']/1000)),_time_retry=False)
            return self.normalize(self.request('/fapi/v1/order',dict(symbol=order['symbol'],origClientOrderId=order['id']),signed=True,**options))
        except ApiError as exc:
            # "Not found" immediately after a timeout is not proof of rejection.
            return {'status':'unknown','qty':0,'price':0,'error':str(exc)}

    def cancel(self, order):
        self.request('/fapi/v1/order', dict(symbol=order['symbol'],origClientOrderId=order['id']), 'DELETE', True)

    def income(self, symbol, start, end):
        # Binance limits one income-history query to seven days. Split long
        # holdings into non-overlapping windows and paginate each window.
        rows=[]; cursor=int(start); end=int(end); seven_days=7*86400000-1
        while cursor <= end:
            window_end=min(end,cursor+seven_days)
            for page in range(1,101):
                r=self.request('/fapi/v1/income',dict(symbol=symbol,startTime=cursor,endTime=window_end,
                               limit=1000,page=page),signed=True)
                rows.extend(r)
                if len(r)<1000: break
            else: raise ValueError('费用记录超出单次对账范围')
            cursor=window_end+1
        unique={str(x.get('tranId',i)):x for i,x in enumerate(rows)}
        return list(unique.values())

    def trades(self, symbol, order_id):
        return self.request('/fapi/v1/userTrades',dict(symbol=symbol,orderId=order_id,limit=1000),signed=True)

    def trade_history(self, symbol, start, end):
        """Complete bounded time windows; a full page is split, never truncated."""
        start,end = int(start),int(end)
        if start>end or end-start>180*86400000:
            raise ValueError('币安成交历史超出自动回补范围')
        rows = []; remaining = 96
        def window(lo,hi):
            nonlocal remaining
            remaining -= 1
            if remaining<0: raise ValueError('币安成交记录过多，须人工核对')
            batch = self.request('/fapi/v1/userTrades',dict(symbol=symbol,startTime=lo,endTime=hi,limit=1000),signed=True)
            if not isinstance(batch,list): raise ValueError('币安成交历史数据不完整')
            if len(batch)>=1000:
                if lo==hi: raise ValueError('币安同毫秒成交过多，须人工核对')
                mid = (lo+hi)//2
                window(lo,mid); window(mid+1,hi)
            else: rows.extend(batch)
        while start<=end:
            stop = min(end,start+7*86400000-1)
            window(start,stop); start = stop+1
        return rows

    def commissions_usdt(self, fills):
        rates={'USDT':1.0};total=0.0
        for fill in fills:
            asset=str(fill.get('commissionAsset') or 'USDT').upper()
            if asset not in rates:
                ticker=self.quote(asset+'USDT')
                rates[asset]=(ticker['bid']+ticker['ask'])/2
            total+=float(fill.get('commission',0))*rates[asset]
        return total


class BinanceBookTicker:
    """Reconnectable best-bid/ask stream with a REST-compatible quote shape."""
    def __init__(self, symbol, production=True, proxy_url=''):
        self.symbol=symbol.upper();self.production=production
        self.proxy_url=str(proxy_url or '').strip()
        self.lock=threading.RLock();self.stop_event=threading.Event();self.ready=threading.Event()
        self.value=None;self.error='';self.socket=None;self.thread=None;self.on_quote=None

    def start(self):
        if websocket is None:
            self.error='websocket-client 未安装，已回退 REST 行情'
            return self
        self.thread=threading.Thread(target=self._run,name='binance-book-ticker',daemon=True)
        self.thread.start();return self

    def _run(self):
        host='wss://fstream.binance.com' if self.production else 'wss://fstream.binancefuture.com'
        url=f'{host}/ws/{self.symbol.lower()}@bookTicker';backoff=1
        while not self.stop_event.is_set():
            ws=None
            try:
                options=dict(timeout=10,enable_multithread=True)
                if self.proxy_url:
                    proxy=urlsplit(self.proxy_url)
                    options.update(http_proxy_host=proxy.hostname,http_proxy_port=proxy.port,proxy_type='http')
                ws=websocket.create_connection(url,**options)
                ws.settimeout(30)
                with self.lock: self.socket=ws;self.error=''
                backoff=1
                while not self.stop_event.is_set():
                    payload=json.loads(ws.recv())
                    if payload.get('s')!=self.symbol: continue
                    q=dict(bid=float(payload['b']),ask=float(payload['a']),
                           bid_qty=float(payload['B']),ask_qty=float(payload['A']),
                           time_ms=int(payload.get('E') or payload.get('T') or time.time()*1000),
                           received_ms=int(time.time()*1000))
                    if 0<q['bid']<=q['ask']:
                        with self.lock: self.value=q
                        self.ready.set()
                        if self.on_quote: self.on_quote()
            except Exception as exc:
                with self.lock: self.error=str(exc);self.socket=None
            finally:
                with self.lock:
                    ws=self.socket;self.socket=None
                try:
                    if ws: ws.close()
                except Exception: pass
            if self.stop_event.wait(backoff): break
            backoff=min(30,backoff*2)

    def quote(self, max_age_ms=5000, wait_ms=0):
        if wait_ms: self.ready.wait(wait_ms/1000)
        with self.lock: value=dict(self.value) if self.value else None
        if not value or int(time.time()*1000)-value.get('received_ms',value['time_ms'])>max_age_ms: return None
        return value

    def close(self):
        self.stop_event.set()
        with self.lock: ws=self.socket
        try:
            if ws: ws.close()
        except Exception: pass
        if self.thread and self.thread.is_alive(): self.thread.join(timeout=2)


class Terminal:
    """Persistent isolated MT5 session; a blocked native call cannot freeze the web UI."""
    def __init__(self, settings, magic, read_only=False):
        if sys.platform != 'win32':
            raise ValueError('真实 MT5 行情及执行需要 Windows 64 位')
        command=[sys.executable,'--mt5-worker'] if getattr(sys,'frozen',False) else [sys.executable,str(Path(__file__).resolve()),'--mt5-worker']
        self.process=subprocess.Popen(command,stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.DEVNULL,
            text=True,encoding='utf-8',creationflags=getattr(subprocess,'CREATE_NO_WINDOW',0))
        self.responses=queue.Queue(); self.lock=threading.Lock()
        threading.Thread(target=self._read,daemon=True).start()
        try: self.call('init',settings=settings,magic=magic,read_only=read_only)
        except Exception:
            self.close(); raise

    def _read(self):
        for line in self.process.stdout:
            try: self.responses.put(json.loads(line))
            except ValueError: pass
        self.responses.put({'ok':False,'error':'MT5 工作进程已退出'})

    def call(self, command, **args):
        with self.lock:
            if self.process.poll() is not None: raise ValueError('MT5 会话已断开，需要重新连接及对账')
            self.process.stdin.write(json.dumps(dict(command=command,**args))+'\n'); self.process.stdin.flush()
            try: result=self.responses.get(timeout=12)
            except queue.Empty:
                self.close(); raise ValueError('MT5 请求超时，若涉及下单则成交状态未知，必须对账') from None
            if not result.get('ok'): raise ValueError(result.get('error','MT5 请求失败'))
            return result['data']

    def close(self):
        if self.process.poll() is None:
            self.process.terminate()
            try: self.process.wait(timeout=2)
            except subprocess.TimeoutExpired: self.process.kill()


class NativeMT5Fills:
    """Resolve only this strategy's exact order; an empty history is not a rejection."""
    def __init__(self, mt, symbol, magic):
        self.mt, self.symbol, self.magic = mt, symbol, magic

    def positions(self, order):
        rows = self.mt.positions_get(symbol=self.symbol)
        if rows is None: raise ValueError('MT5 持仓读取失败')
        return [p for p in rows if p.magic == self.magic and p.type == 0
                and p.comment == order['id']]

    def matches(self, rows, order, ticket=None):
        side = 0 if order['action'] == 'open' else 1
        entry = (0,) if side == 0 else (1, 2, 3)
        return [d for d in rows or () if d.symbol == self.symbol and d.magic == self.magic
                and d.type == side and d.entry in entry
                and (str(d.order) == str(ticket) if ticket else d.comment == order['id'])]

    def aggregate(self, rows, order, positions=()):
        if not rows: return dict(status='unknown', qty=0, price=0)
        if len({str(d.order) for d in rows}) != 1 or len({str(d.position_id) for d in rows}) != 1:
            return dict(status='unknown', qty=0, price=0)
        # Never mix duplicate, zero-volume, or malformed native deal records.
        if len({d.ticket for d in rows}) != len(rows):
            return dict(status='unknown', qty=0, price=0)
        if any(not math.isfinite(d.volume) or d.volume <= 0 or not math.isfinite(d.price)
               or d.price <= 0 for d in rows):
            return dict(status='unknown', qty=0, price=0)
        lots = sum(d.volume for d in rows)
        position_id = str(rows[0].position_id)
        current = [p for p in positions if str(p.identifier) == position_id]
        position = (str(current[0].ticket) if len(current) == 1 else None) if order['action']=='open' else order.get('position')
        data = dict(status='done', qty=lots*self.mt.symbol_info(self.symbol).trade_contract_size,
                    price=sum(d.volume*d.price for d in rows)/lots, ticket=str(rows[0].order),
                    position_identifier=position_id,
                    fee=-sum(d.commission+d.fee for d in rows), swap=sum(d.swap for d in rows))
        if position: data['position'] = position
        return data

    def query(self, order):
        from datetime import datetime, timezone
        saved = order.get('result') or {}
        ticket = saved.get('ticket') or order.get('ticket')
        try: ticket = str(int(ticket)) if int(ticket)>0 else None
        except (TypeError,ValueError): ticket = None
        current = self.positions(order) if order['action']=='open' else ()
        rows = self.mt.history_deals_get(ticket=int(ticket)) if ticket else None
        if ticket:
            matches = self.matches(rows, order, ticket)
        else:
            # Older journals discarded the acknowledgement ticket. The exact
            # unique opening comment can recover its position without dates.
            matches = []
            if len(current)==1:
                rows = self.mt.history_deals_get(position=current[0].identifier)
                matches = self.matches(rows, order)
            if not matches:
                # Broker history clocks can differ from UTC. Widen only this
                # read, keeping exact comment/magic/symbol/direction matching.
                start = order['created_ms']/1000-86400
                end = order['created_ms']/1000+86400
                rows = self.mt.history_deals_get(_utc_datetime_from_timestamp(start),
                                                _utc_datetime_from_timestamp(end))
                matches = self.matches(rows, order)
            if len({str(d.order) for d in matches}) != 1:
                return dict(status='unknown',qty=0,price=0)
            ticket = str(matches[0].order)
        unknown = dict(status='unknown',qty=0,price=0,ticket=str(ticket))
        # A visible deal can be only part of an IOC fill or a still-live order.
        pending = self.mt.orders_get(ticket=int(ticket))
        terminal = self.mt.history_orders_get(ticket=int(ticket))
        if pending is None or pending or not terminal: return unknown
        terminal = [x for x in terminal if str(x.ticket)==str(ticket) and x.symbol==self.symbol
                    and x.magic==self.magic and x.type==(0 if order['action']=='open' else 1)]
        if len(terminal)!=1: return unknown
        item = terminal[0]
        if item.state not in (self.mt.ORDER_STATE_FILLED,self.mt.ORDER_STATE_CANCELED,
                              self.mt.ORDER_STATE_REJECTED,self.mt.ORDER_STATE_EXPIRED): return unknown
        filled = item.volume_initial-item.volume_current
        if not math.isfinite(filled) or filled<0: return unknown
        if abs(sum(d.volume for d in matches)-filled)>1e-9: return unknown
        if filled==0: return dict(status='done',qty=0,price=0,ticket=str(ticket))
        return {**unknown,**self.aggregate(matches,order,current)}

    def submitted(self, order, result, contract):
        ticket = str(result.order or '')
        data = dict(status='unknown',qty=0,price=0,retcode=result.retcode)
        if result.order: data['ticket'] = ticket
        if result.deal: data['deal'] = str(result.deal)
        for key, value in (('ack_volume',result.volume),('ack_price',result.price)):
            if isinstance(value,(int,float)) and math.isfinite(value): data[key] = value
        volume, price = result.volume, result.price
        qty = volume*contract if isinstance(volume,(int,float)) else float('nan')
        if not (math.isfinite(qty) and 0<qty<=order['requested']+1e-7
                and isinstance(price,(int,float)) and math.isfinite(price) and price>0):
            # Unusual acknowledgements use verified terminal order + deal data.
            if result.order:
                try: data.update(self.query({**order,'result':data}))
                except Exception: data['error']='MT5 成交回报待核对，保留订单编号继续查询'
            if data['status']!='done':
                data.setdefault('error','MT5 成交数量或价格待核对，正在查询订单及成交历史')
            return data
        # Keep the normal path to one position read; fees are verified later.
        # Do not assume that an order ticket or position identifier is its ticket.
        position = order.get('position')
        if order['action']=='open':
            try: current = self.positions(order)
            except Exception: current = ()  # Fill acknowledgement remains known.
            if len(current)==1:
                position = str(current[0].ticket)
                data['position_identifier'] = str(current[0].identifier)
        data.update(status='done',qty=qty,price=price)
        if position: data['position'] = position
        return data


def native_external_history(mt, symbol, openings):
    """Read immutable position history without assuming a broker clock offset."""
    if not isinstance(openings,list) or not 0<len(openings)<=100:
        raise ValueError('MT5 原开仓编号列表无效')
    from datetime import datetime, timezone
    result=[];seen=set()

    def read_history(*args, **kwargs):
        try: return mt.history_deals_get(*args, **kwargs)
        except Exception: return None

    for opening in openings:
        # A native acknowledgement can contain a deal/position identifier
        # while omitting the order ticket.  Prefer the immutable position
        # identifier in that case; it remains queryable after the position is
        # manually closed and is safer than guessing from a local timestamp.
        identifier = str(opening.get('identifier') or '').strip()
        try: ticket=int(opening.get('order_ticket') or 0)
        except (TypeError,ValueError): ticket=0
        rows = read_history(ticket=ticket) if ticket>0 else None
        ticket_ids={int(d.position_id) for d in (rows or ())
                    if d.symbol==symbol and int(d.order)==ticket}
        if ticket_ids:
            if len(ticket_ids)!=1: raise ValueError('MT5 原开仓持仓标识未唯一确认')
            found=str(next(iter(ticket_ids)))
            if identifier and identifier!=found: raise ValueError('MT5 原持仓标识不符')
            identifier=found

        # Position filtering is the preferred full-history query, but some
        # servers return an empty result after a manual close.  Keep the
        # result only if it contains the original opening deal; otherwise
        # continue to the bounded identity query below.
        full = read_history(position=int(identifier)) if identifier else None
        full_openings=[d for d in (full or ()) if d.symbol==symbol
                       and int(d.position_id)==int(identifier or 0)
                       and (not hasattr(d,'entry') or int(d.entry)==0)
                       and (not hasattr(d,'type') or int(d.type)==0)]
        if identifier and not full_openings:
            full=None

        # Last-resort recovery for older journals and broker servers that do
        # not implement ticket/position filters consistently.  The date range
        # bounds the native request; ownership still requires an exact,
        # unique opening deal on this authenticated account.
        if not full:
            try: created=float(opening.get('created_ms'))/1000
            except (TypeError,ValueError): created=0
            if created<=0: raise ValueError('MT5 原开仓编号未确认')
            rows=read_history(_utc_datetime_from_timestamp(created-86400),
                              _utc_datetime_from_timestamp(created+86400))
            if rows is None: raise ValueError('MT5 原开仓历史读取失败')
            comment=str(opening.get('comment') or '').strip()
            try: magic=int(opening.get('magic'))
            except (TypeError,ValueError): magic=None
            matches=[d for d in rows if d.symbol==symbol and int(d.entry)==0 and int(d.type)==0
                     and (identifier and str(d.position_id)==identifier or
                          ticket>0 and int(d.order)==ticket or
                          not identifier and not ticket and comment and magic is not None
                          and str(d.comment)==comment and int(d.magic)==magic)]
            ids={int(d.position_id) for d in matches}
            if len(ids)!=1: raise ValueError('MT5 原开仓成交未唯一确认')
            found=str(next(iter(ids)))
            if identifier and identifier!=found: raise ValueError('MT5 原持仓标识不符')
            identifier=found
            full=[d for d in rows if d.symbol==symbol and int(d.position_id)==int(identifier)]

        if identifier in seen: continue
        seen.add(identifier)
        deals=full
        if not deals: raise ValueError('MT5 持仓成交历史读取失败')
        result.extend(d._asdict() for d in deals if d.symbol==symbol)
        if len(result)>20000: raise ValueError('MT5 历史过多，须人工核对')
    return result


def native_external_history_probe(mt, symbol, openings):
    """Return a safe, relation-preserving MT5 history shape for diagnostics.

    Values that identify an account/order are replaced with D/O/P aliases;
    execution fields remain visible so broker-specific history differences can
    be diagnosed without exporting credentials or raw tickets.
    """
    from datetime import datetime, timezone
    allowed=('ticket','order','position_id','symbol','magic','entry','type','reason',
             'volume','price','time','time_msc','commission','fee','swap','comment')
    queries=[]; rows=[]; aliases={k:{} for k in ('ticket','order','position_id')}

    def alias(kind,value):
        if value in (None,''): return None
        key=str(value); mapping=aliases[kind]
        if key not in mapping: mapping[key]=f'{kind[0].upper()}{len(mapping)+1}'
        return mapping[key]

    def read(label,*args,**kwargs):
        try:
            value=mt.history_deals_get(*args,**kwargs)
            queries.append({'query':label,'status':'ok' if value is not None else 'empty'})
            return value or ()
        except Exception:
            queries.append({'query':label,'status':'error'})
            return ()

    for opening in openings if isinstance(openings,list) else []:
        ticket=opening.get('order_ticket'); identifier=opening.get('identifier')
        try: ticket=int(ticket or 0)
        except (TypeError,ValueError): ticket=0
        if identifier:
            rows.extend(read('position',position=int(identifier)))
        if ticket:
            rows.extend(read('ticket',ticket=ticket))
        try: created=float(opening.get('created_ms'))/1000
        except (TypeError,ValueError): created=0
        if created>0:
            rows.extend(read('range',_utc_datetime_from_timestamp(created-86400),
                             _utc_datetime_from_timestamp(created+86400)))
    unique={}
    for row in rows:
        data={}
        source=row._asdict() if hasattr(row,'_asdict') else getattr(row,'__dict__',{})
        for key in allowed:
            if key not in source: continue
            value=source[key]
            if key in aliases:
                data[key]=alias(key,value)
            elif key=='comment':
                text=str(value or '')
                data['comment_present']=bool(text)
                data['comment_kind']='strategy' if text.startswith('gp') else 'manual' if text else 'empty'
            elif isinstance(value,(str,int,float,bool)) or value is None:
                data[key]=value
        identity=(data.get('ticket'),data.get('order'),data.get('position_id'),
                  data.get('time_msc'),data.get('volume'),data.get('price'))
        unique[identity]=data
    clean=sorted(unique.values(),key=lambda x:(str(x.get('position_id')),x.get('time_msc',0)))
    fields=sorted({key for row in clean for key in row})
    return {'schema_version':1,'symbol':symbol,'queries':queries,'row_count':len(clean),
            'fields':fields,'rows':clean}


def worker_main():
    import MetaTrader5 as mt
    from datetime import datetime, timezone
    settings={}; magic=0; read_only=False
    def history(start):
        rows=mt.history_deals_get(_utc_datetime_from_timestamp(start/1000),datetime.now(timezone.utc))
        if rows is None: raise ValueError('MT5 成交历史读取失败')
        return rows
    def identity():
        a,t=mt.account_info(),mt.terminal_info()
        if not a or not t or not t.connected: raise ValueError('MT5 未连接')
        if str(a.login)!=str(settings['account']) or a.server!=settings['server']:
            raise ValueError('MT5 账户发生变化，已阻止交易')
        return a,t
    def query_order(o):
        return NativeMT5Fills(mt,settings['symbol'],magic).query(o)
    for line in sys.stdin:
        try:
            r=json.loads(line); cmd=r['command']
            if cmd=='init':
                settings=r['settings'];magic=int(r['magic']);read_only=bool(r.get('read_only',False))
                if not settings.get('account') or not settings.get('server') or not settings.get('symbol'):
                    raise ValueError('先检查并应用 MT5 账户和品种')
                path=settings.get('terminal_path','');args=[path] if path else []
                if not mt.initialize(*args,timeout=10000): raise ValueError('MT5 初始化失败')
                identity(); mt.symbol_select(settings['symbol'],True); data=True
            else:
                if read_only and cmd not in ('snapshot','quote','history','external_history','external_history_probe','query'):
                    raise ValueError('持仓核对会话只允许读取行情、持仓和成交')
                a,t=identity(); s=mt.symbol_info(settings['symbol'])
                if not s: raise ValueError('MT5 品种不可用')
                if cmd=='quote':
                    tick=mt.symbol_info_tick(s.name)
                    if not tick: raise ValueError('MT5 报价读取失败')
                    data=dict(quote=dict(bid=tick.bid,ask=tick.ask,time_ms=tick.time_msc))
                elif cmd=='snapshot':
                    tick=mt.symbol_info_tick(s.name)
                    positions=mt.positions_get(symbol=s.name)
                    pending=mt.orders_get(symbol=s.name)
                    if pending is None: raise ValueError('MT5 挂单读取失败')
                    if not tick or positions is None: raise ValueError('MT5 报价或持仓读取失败')
                    data=dict(quote=dict(bid=tick.bid,ask=tick.ask,time_ms=tick.time_msc),
                        spec=dict(name=s.name,contract_size_oz=s.trade_contract_size,volume_min=s.volume_min,
                                  volume_max=s.volume_max,volume_step=s.volume_step,trade_mode=s.trade_mode,
                                  currency_base=s.currency_base,currency_profit=s.currency_profit),
                        account=dict(account=str(a.login),server=a.server,currency=a.currency,margin_mode=a.margin_mode,
                                     trade_mode=a.trade_mode,free_margin=a.margin_free,**mt5_risk_fields(a)),
                        allowed=bool(t.trade_allowed and not t.tradeapi_disabled and a.trade_allowed and a.trade_expert),
                        orders=[dict(ticket=str(x.ticket),symbol=x.symbol,type=x.type,volume=x.volume_current) for x in pending],
                        positions=[dict(identifier=str(p.identifier),ticket=str(p.ticket),lots=p.volume,side=p.type,magic=p.magic,swap=p.swap,
                                        symbol=p.symbol,price_open=p.price_open,time_ms=p.time_msc,
                                        profit=p.profit,comment=p.comment) for p in positions])
                elif cmd=='history': data=[d._asdict() for d in history(r['start_ms']) if d.symbol==s.name and d.magic==magic]
                elif cmd=='external_history':
                    if int(time.time()*1000)-int(r['start_ms'])>180*86400000:
                        raise ValueError('MT5 成交历史超出自动回补范围')
                    # Manual exits usually use a different magic/comment. The
                    # caller proves ownership via the original opening deal's
                    # immutable position_id, not via the closing comment.
                    data=native_external_history(mt,s.name,r['openings'])
                elif cmd=='external_history_probe':
                    if int(time.time()*1000)-int(r.get('start_ms',0))>180*86400000:
                        raise ValueError('MT5 成交历史超出自动回补范围')
                    data=native_external_history_probe(mt,s.name,r['openings'])
                elif cmd=='rates':
                    tf={'M1':mt.TIMEFRAME_M1,'M5':mt.TIMEFRAME_M5,'H1':mt.TIMEFRAME_H1}[r['period']]
                    rates=mt.copy_rates_from_pos(s.name,tf,1,r['count'])
                    if rates is None: raise ValueError('MT5 历史数据读取失败')
                    data=[dict(t=int(x['time'])*1000,close=float(x['close'])) for x in rates]
                elif cmd=='query': data=query_order(r['order'])
                elif cmd=='margin':
                    value=mt.order_calc_margin(mt.ORDER_TYPE_BUY,s.name,r['lots'],mt.symbol_info_tick(s.name).ask)
                    if value is None: raise ValueError('无法核验 MT5 保证金')
                    data=dict(required=value,available=a.margin_free)
                elif cmd=='submit':
                    o=r['order']; opening=o['action']=='open'
                    if not (t.trade_allowed and not t.tradeapi_disabled and a.trade_allowed and a.trade_expert):
                        raise ValueError('MT5 自动交易权限未开启')
                    tick=mt.symbol_info_tick(s.name); price=tick.ask if opening else tick.bid
                    if (opening and price>o['limit']) or (not opening and price<o['limit']):
                        data=dict(status='done',qty=0,price=0,error='MT5 报价已超出滑点预算')
                    else:
                        filling=mt.ORDER_FILLING_IOC if s.filling_mode&2 else mt.ORDER_FILLING_FOK if s.filling_mode&1 else None
                        if filling is None: raise ValueError('该 MT5 合约未提供可用的 IOC/FOK 成交方式')
                        req=dict(action=mt.TRADE_ACTION_DEAL,symbol=s.name,volume=o['requested']/s.trade_contract_size,
                            type=mt.ORDER_TYPE_BUY if opening else mt.ORDER_TYPE_SELL,price=price,
                            deviation=max(1,math.floor(o['slippage']/s.point)),magic=magic,comment=o['id'],
                            type_time=mt.ORDER_TIME_GTC,type_filling=filling)
                        if not opening:
                            if not o.get('position') and o.get('opening_id'):
                                current=mt.positions_get(symbol=s.name)
                                if current is None: raise ValueError('MT5 持仓读取失败')
                                pos=[p for p in current if p.comment==o['opening_id'] and p.magic==magic
                                     and p.type==0 and (not o.get('position_identifier') or
                                         str(p.identifier)==str(o['position_identifier']))]
                                if len(pos)!=1: raise ValueError('MT5 持仓票据未唯一确认，请先对账')
                                o['position']=str(pos[0].ticket)
                            else:
                                pos=mt.positions_get(ticket=int(o['position']))
                            if not pos or not owned_close_position(pos[0],o,s.name,magic):
                                raise ValueError('MT5 待平仓位不属于本策略，已拒绝操作')
                            if req['volume']>pos[0].volume+1e-9: raise ValueError('MT5 平仓量大于实际持仓')
                            req['position']=int(o['position'])
                        check=mt.order_check(req)
                        if not check or check.retcode!=0:
                            data=dict(status='done',qty=0,price=0,error='MT5 订单预检查未通过')
                        else:
                            result=mt.order_send(req)
                            if result is None: data=dict(status='unknown',qty=0,price=0)
                            elif result.retcode in (mt.TRADE_RETCODE_DONE,mt.TRADE_RETCODE_DONE_PARTIAL):
                                data=NativeMT5Fills(mt,s.name,magic).submitted(o,result,s.trade_contract_size)
                            elif result.retcode in (mt.TRADE_RETCODE_TIMEOUT,mt.TRADE_RETCODE_CONNECTION,mt.TRADE_RETCODE_PLACED):
                                data=dict(status='unknown',qty=0,price=0,retcode=result.retcode)
                                if result.order: data['ticket']=str(result.order)
                                if result.deal: data['deal']=str(result.deal)
                            else: data=dict(status='done',qty=0,price=0,error=f'MT5 拒单 {result.retcode}')
                else: raise ValueError('未知 MT5 指令')
            print(json.dumps(dict(ok=True,data=data),allow_nan=False),flush=True)
        except Exception as exc:
            print(json.dumps(dict(ok=False,error=str(exc))),flush=True)
    mt.shutdown()


def owned_close_position(position, order, symbol, magic):
    if position.symbol != symbol or position.type != 0:
        return False
    adopted = order.get('adopted_position')
    if not adopted:
        return (position.magic == magic and
                (not order.get('opening_id') or position.comment == order['opening_id']) and
                (not order.get('position_identifier') or str(position.identifier)==str(order['position_identifier'])))
    return (str(position.ticket) == str(adopted['ticket']) == str(order.get('position'))
            and str(position.identifier) == str(adopted['identifier'])
            and position.magic == adopted['magic'] and position.time_msc == adopted['time_ms']
            and abs(position.price_open - adopted['price_open']) < 1e-8)


class LiveBroker:
    def __init__(self, config, exchange, terminal, spec):
        self.config,self.b,self.m,self.spec=config,exchange,terminal,spec

    def submit(self, order):
        try:
            wire_order=copy.deepcopy(order)
            if wire_order['leg']=='binance':
                try:
                    top_qty=float(wire_order.get('top_qty'))
                except (TypeError, ValueError):
                    top_qty=float('nan')
                # A sufficient top level is the fast path.  If it is absent
                # or insufficient, Binance.submit performs the deeper REST
                # check before sending the IOC order.
                wire_order['depth_guard']=not math.isfinite(top_qty) or top_qty+1e-12 < float(wire_order['requested'])
            result=self.b.submit(wire_order,self.spec) if order['leg']=='binance' else self.m.call('submit',order=wire_order)
            # Return fills immediately so the other leg can hedge them. Fee
            # history is verified after closing, outside paired execution.
            return result
        except Exception as exc:
            return dict(status='unknown',qty=0,price=0,error=str(exc))

    def query(self, order):
        return self.b.query(order) if order['leg']=='binance' else self.m.call('query',order=order)

    def cancel(self, order):
        if order['leg']=='binance': self.b.cancel(order)


class PaperBroker:
    def submit(self, order):
        return dict(status='done',qty=order['requested'],price=order['reference'],
                    ticket=order['id'],position=order.get('position') or order['id'])
    def query(self, order):
        return order.get('result',dict(status='unknown',qty=0,price=0))
    def cancel(self, order): pass


if __name__=='__main__': worker_main()
