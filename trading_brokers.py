"""Exchange/terminal adapters. All writes are invoked only by an armed engine."""
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
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR
from pathlib import Path
from urllib.parse import urlencode, urlsplit
from urllib.request import ProxyHandler, Request, build_opener
from urllib.error import HTTPError

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

    def request(self, path, params=None, method='GET', signed=False, base=None, _time_retry=True):
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
            with self.opener.open(req, timeout=5) as res:
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
            message = (f'币安接口错误 -1021：请求时间校验失败（已同步服务器时间并重试一次，recvWindow={self.recv_window_ms} ms）；请检查 Windows 时间同步和网络延迟'
                       if code == -1021 else f'币安接口错误 {code}')
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
        if sync_clock: self.sync()
        a=self.request('/fapi/v3/account', signed=True)
        # V3 supplies balances/positions; permission and mode flags live in
        # accountConfig. Missing fields must never be reported as false.
        configuration=self.request('/fapi/v1/accountConfig', signed=True)
        if api_permissions and api_permissions.get('enable_futures') is False:
            raise ValueError('当前 API Key 权限返回 enableFutures=false；请确认修改并保存的是页面中这把 Key 的“允许合约”权限')
        if type(configuration.get('canTrade')) is not bool:
            raise ValueError('账户配置接口未返回有效 canTrade，暂不能确认交易权限；这不等同于 canTrade=false')
        if not configuration['canTrade']:
            if api_permissions and not api_permissions.get('enable_futures'):
                raise ValueError('当前 API Key 权限返回 enableFutures=false；请确认修改并保存的是页面中这把 Key 的“允许合约”权限')
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
            return self.normalize(self.request('/fapi/v1/order', args, 'POST', True))
        except ApiError as exc:
            if exc.uncertain: return {'status':'unknown','qty':0,'price':0,'error':str(exc)}
            return {'status':'done','qty':0,'price':0,'error':str(exc)}

    def query(self, order):
        try:
            return self.normalize(self.request('/fapi/v1/order',dict(symbol=order['symbol'],origClientOrderId=order['id']),signed=True))
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
    def __init__(self, settings, magic):
        if sys.platform != 'win32':
            raise ValueError('真实 MT5 行情及执行需要 Windows 64 位')
        command=[sys.executable,'--mt5-worker'] if getattr(sys,'frozen',False) else [sys.executable,str(Path(__file__).resolve()),'--mt5-worker']
        self.process=subprocess.Popen(command,stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.DEVNULL,
            text=True,encoding='utf-8',creationflags=getattr(subprocess,'CREATE_NO_WINDOW',0))
        self.responses=queue.Queue(); self.lock=threading.Lock()
        threading.Thread(target=self._read,daemon=True).start()
        try: self.call('init',settings=settings,magic=magic)
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


def worker_main():
    import MetaTrader5 as mt
    from datetime import datetime, timezone
    settings={}; magic=0
    def history(start):
        rows=mt.history_deals_get(datetime.fromtimestamp(start/1000,timezone.utc),datetime.now(timezone.utc))
        if rows is None: raise ValueError('MT5 成交历史读取失败')
        return rows
    def identity():
        a,t=mt.account_info(),mt.terminal_info()
        if not a or not t or not t.connected: raise ValueError('MT5 未连接')
        if str(a.login)!=str(settings['account']) or a.server!=settings['server']:
            raise ValueError('MT5 账户发生变化，已阻止交易')
        return a,t
    def query_order(o):
        matches=[d for d in history(o['created_ms']-60000) if d.symbol==settings['symbol'] and d.magic==magic
                 and (d.comment==o['id'] or (o.get('ticket') and str(d.order)==str(o['ticket'])))]
        if not matches: return dict(status='unknown',qty=0,price=0)
        lots=sum(d.volume for d in matches); spec=mt.symbol_info(settings['symbol'])
        return dict(status='done',qty=lots*spec.trade_contract_size,
                    price=sum(d.volume*d.price for d in matches)/lots,
                    ticket=str(matches[0].order),position=str(matches[0].position_id),
                    fee=-sum(d.commission+d.fee for d in matches),swap=sum(d.swap for d in matches))
    for line in sys.stdin:
        try:
            r=json.loads(line); cmd=r['command']
            if cmd=='init':
                settings=r['settings'];magic=int(r['magic'])
                if not settings.get('account') or not settings.get('server') or not settings.get('symbol'):
                    raise ValueError('先检查并应用 MT5 账户和品种')
                path=settings.get('terminal_path','');args=[path] if path else []
                if not mt.initialize(*args,timeout=10000): raise ValueError('MT5 初始化失败')
                identity(); mt.symbol_select(settings['symbol'],True); data=True
            else:
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
                                     trade_mode=a.trade_mode,free_margin=a.margin_free),
                        allowed=bool(t.trade_allowed and not t.tradeapi_disabled and a.trade_allowed and a.trade_expert),
                        orders=[dict(ticket=str(x.ticket),symbol=x.symbol,type=x.type,volume=x.volume_current) for x in pending],
                        positions=[dict(identifier=str(p.identifier),ticket=str(p.ticket),lots=p.volume,side=p.type,magic=p.magic,swap=p.swap,
                                        symbol=p.symbol,price_open=p.price_open,time_ms=p.time_msc,
                                        profit=p.profit,comment=p.comment) for p in positions])
                elif cmd=='history': data=[d._asdict() for d in history(r['start_ms']) if d.symbol==s.name and d.magic==magic]
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
                                position=o.get('position')
                                deal_rows=mt.history_deals_get(ticket=result.deal) if result.deal else ()
                                if opening:
                                    # In hedging accounts an order ticket is not
                                    # a reliable position ticket. Resolve the
                                    # position created with our unique comment.
                                    if deal_rows:
                                        position=str(deal_rows[0].position_id)
                                    else:
                                        current=mt.positions_get(symbol=s.name) or ()
                                        owned=[p for p in current if p.magic==magic and p.comment==o['id']
                                               and p.type==mt.POSITION_TYPE_BUY]
                                        position=str(owned[0].ticket) if len(owned)==1 else str(result.order or '')
                                data=dict(status='done',qty=result.volume*s.trade_contract_size,price=result.price,
                                          ticket=str(result.order),position=position)
                                if deal_rows:
                                    data['fee']=-sum(d.commission+d.fee for d in deal_rows)
                                    data['swap']=sum(d.swap for d in deal_rows)
                            elif result.retcode in (mt.TRADE_RETCODE_TIMEOUT,mt.TRADE_RETCODE_CONNECTION,mt.TRADE_RETCODE_PLACED):
                                data=dict(status='unknown',qty=0,price=0,ticket=str(result.order))
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
        return position.magic == magic
    return (str(position.ticket) == str(adopted['ticket']) == str(order.get('position'))
            and str(position.identifier) == str(adopted['identifier'])
            and position.magic == adopted['magic'] and position.time_msc == adopted['time_ms']
            and abs(position.price_open - adopted['price_open']) < 1e-8)


class LiveBroker:
    def __init__(self, config, exchange, terminal, spec):
        self.config,self.b,self.m,self.spec=config,exchange,terminal,spec

    def submit(self, order):
        try:
            result=self.b.submit(order,self.spec) if order['leg']=='binance' else self.m.call('submit',order=order)
            return self._fees(order,result)
        except Exception as exc:
            return dict(status='unknown',qty=0,price=0,error=str(exc))

    def query(self, order):
        result=self.b.query(order) if order['leg']=='binance' else self.m.call('query',order=order)
        return self._fees(order,result)

    def _fees(self,order,result):
        if order['leg']=='binance' and result.get('ticket') and result.get('qty',0)>0:
            try:
                fills=self.b.trades(order['symbol'],result['ticket'])
                if fills: result['fee']=self.b.commissions_usdt(fills)
            except Exception: pass
        return result

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
