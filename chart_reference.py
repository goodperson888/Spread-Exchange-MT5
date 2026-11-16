"""Bounded, display-only bar history. Never uses the order journal or broker."""
import copy
import hashlib
import json
import math
import os
import threading
import time
from collections import OrderedDict
from pathlib import Path

from pair_instruments import native_pair, short_symbol
from trading_config import pair_key
from trading_store import storage_error_message


PERIODS={'M1':60000,'M5':300000,'M15':900000}
CACHE_TTL=900
MAX_POINTS=4000


class Cancelled(Exception): pass


def period_for(minutes):
    return 'M1' if minutes<=1440 else 'M5' if minutes<=10080 else 'M15'


def positive(value):
    return isinstance(value,(int,float)) and not isinstance(value,bool) and math.isfinite(value) and value>0


def normalized_bars(rows,start,end,interval):
    bars={}
    if not isinstance(rows,list) or len(rows)>MAX_POINTS: raise ValueError('历史 K 线数量或格式异常')
    for row in rows:
        at=row.get('t');close=row.get('close')
        if not positive(at) or int(at)!=at or int(at)%interval or not positive(close):
            raise ValueError('历史 K 线时间或价格无效')
        if at in bars: raise ValueError('历史 K 线时间重复，无法安全配对')
        if start<=at and at+interval<=end: bars[int(at)]=float(close)
    return bars


def align_bars(short,long,start,end,interval):
    a=normalized_bars(short,start,end,interval);b=normalized_bars(long,start,end,interval)
    # No nearest-price joins, forward filling, interpolation, or high-low subtraction.
    return [dict(time_ms=at,short_close=a[at],long_close=b[at]) for at in sorted(a.keys() & b.keys())]


def fetch_bars(config,period,start,end,cancel):
    """Dedicated native worker and public HTTP client; no trading credentials."""
    from trading_brokers import Terminal, Binance
    interval=PERIODS[period];deadline=time.monotonic()+45
    def check():
        if cancel.is_set(): raise Cancelled()
        remaining=deadline-time.monotonic()
        if remaining<=0: raise ValueError('历史补取超时，可稍后重试')
        return min(12,remaining)
    settings=copy.deepcopy(config['mt5'])
    settings.pop('mcp_token',None)
    if native_pair(config): settings['pair_symbols']=[short_symbol(config),settings['symbol']]
    terminal=Terminal(settings,config['execution']['magic'],read_only=True,timeout_seconds=check())
    try:
        def read(symbol):
            result=terminal.call('chart_rates',symbol=symbol,period=period,start_ms=start,end_ms=end,
                                 timeout_seconds=check())
            check()
            if result.get('symbol')!=symbol: raise ValueError('历史品种与当前组合不符')
            tick=result.get('tick_time_ms');observed=result.get('observed_ms')
            if not positive(tick) or not positive(observed) or abs(tick-observed)>120000:
                raise ValueError('MT5 最新报价过旧或原始时间与本机明显错位，历史时基尚未确认；暂不配对历史，不自动加减时区')
            return result['bars']
        long=read(settings['symbol'])
        if native_pair(config): short=read(short_symbol(config))
        else:
            client=Binance(production=True,proxy_url=config.get('binance',{}).get('proxy_url',''))
            short=[];cursor=start
            for _ in range(9):
                if cursor>=end: break
                page=client.request('/fapi/v1/klines',dict(symbol=config['symbol'],
                    interval={'M1':'1m','M5':'5m','M15':'15m'}[period],startTime=cursor,endTime=end-1,limit=499),
                    timeout=min(5,check()))
                if not isinstance(page,list) or len(page)>499: raise ValueError('币安历史 K 线格式异常')
                if not page: break
                try: rows=[dict(t=int(row[0]),close=float(row[4])) for row in page]
                except (ValueError,TypeError,IndexError): raise ValueError('币安历史 K 线字段异常') from None
                if any(row['t']<cursor or row['t']>=end for row in rows): raise ValueError('币安历史返回了请求区间之外的数据')
                short.extend(rows)
                next_cursor=max(row['t'] for row in rows)+interval
                if next_cursor<=cursor: raise ValueError('币安历史分页未推进')
                cursor=next_cursor
                if len(short)>MAX_POINTS: raise ValueError('历史 K 线超过显示上限')
            if cursor<end and len(page)==499: raise ValueError('历史分页超过单次上限')
        check()
        return align_bars(short,long,start,end,interval)
    finally:
        terminal.close()


def percentile(values,fraction):
    at=(len(values)-1)*fraction;left=int(at);right=min(len(values)-1,left+1)
    return values[left]+(values[right]-values[left])*(at-left)


class ReferenceHistory:
    def __init__(self,directory,fetch=fetch_bars):
        self.directory=Path(directory);self.fetch=fetch
        self.lock=threading.Lock();self.entries=OrderedDict();self.pending=None
        self.thread=None;self.cancel=threading.Event();self.stopped=False;self.active_key=None

    def close(self):
        with self.lock:
            self.stopped=True;self.pending=None;self.cancel.set()

    def suspend(self):
        with self.lock:
            if self.pending: self.entries[self.pending[0]]['state']='idle'
            self.pending=None;self.cancel.set()

    def request(self,config,minutes,local_samples,connected=True):
        minutes=max(1,min(43200,int(minutes)));interval=PERIODS[period_for(minutes)]
        now=int(time.time()*1000);start=now-minutes*60000
        if not config or config['mt5']['adapter']!='native':
            return dict(state='unavailable',message='历史参考补取需要 Windows 原生 MT5；当前仍显示本机采样。',samples=[])
        # Only fill periods for which local executable quote sampling is missing.
        covered={int(q['time_ms'])//interval for q in local_samples
                 if q.get('valid') is not False and start<=q.get('time_ms',0)<=now}
        expected=max(1,minutes*60000//interval)
        if len(covered)>=expected*.95:
            return dict(state='local',message='所选窗口已有本机采样，无需补取历史参考。',samples=[])
        key=hashlib.sha256((pair_key(config)+'|'+str(minutes)+'|reference-v1').encode()).hexdigest()
        with self.lock:
            entry=self.entries.get(key)
            if entry: self.entries.move_to_end(key)
            if entry is None:
                entry=dict(state='idle',rows=[],updated_at=0)
                self.entries[key]=entry
            if not self.stopped and entry['state'] not in ('loading','queued'):
                stale=not entry.get('updated_at') or time.monotonic()-entry['updated_at']>(60 if entry['state']=='error' else CACHE_TTL)
                if (connected and stale) or (not connected and not entry.get('cache_checked')):
                    if self.pending and self.pending[0]!=key:
                        self.entries[self.pending[0]]['state']='idle'
                    entry['state']='queued'
                    # Secrets are not retained by the display worker or cache.
                    clean={k:copy.deepcopy(config[k]) for k in ('mt5','execution','pair','symbol') if k in config}
                    clean['mt5'].pop('mcp_token',None)
                    clean['binance']={'proxy_url':config.get('binance',{}).get('proxy_url','')}
                    end=now-now%interval
                    self.pending=(key,clean,period_for(minutes),start-start%interval,end,connected)
                    if self.active_key and self.active_key!=key: self.cancel.set()
                    if self.thread is None or not self.thread.is_alive():
                        self.thread=threading.Thread(target=self._run,name='chart-reference-history',daemon=True)
                        self.thread.start()
            while len(self.entries)>8:
                oldest=next(iter(self.entries))
                if oldest==self.active_key: self.entries.move_to_end(oldest);continue
                self.entries.pop(oldest)
            rows=list(entry['rows']);state=entry['state'];error=entry.get('error','')
            warning=entry.get('cache_warning','')
        fx=1. if native_pair(config) else float(config['costs']['usdt_usd'])
        if not positive(fx): return dict(state='error',message='参考汇率无效，无法计算历史价差。',samples=[])
        values=[dict(time_ms=row['time_ms'],spread=row['short_close']*fx-row['long_close'])
                for row in rows if start<=row['time_ms'] and row['time_ms']+interval<=now]
        values=[row for row in values if math.isfinite(row['spread'])]
        ordered=sorted(row['spread'] for row in values)
        stats=(dict(count=len(ordered),minimum=ordered[0],maximum=ordered[-1],median=percentile(ordered,.5),
                    p10=percentile(ordered,.1),p90=percentile(ordered,.9)) if ordered else None)
        messages={'queued':'正在排队补取平台历史…','loading':'正在后台补取平台历史…',
                  'ready':'已补取同周期收盘价参考；只供观察，不参与交易判断。',
                  'error':error,'idle':'连接双边行情后可后台补取历史参考。'}
        message=messages.get(state,'历史参考尚未就绪')
        if not connected: message='未连接；保留已缓存的历史参考，不发起平台请求。'
        if state=='ready' and not values: message='平台未返回可按同一时间配对的完整 K 线，保留空缺。'
        return dict(state=state,message=message,samples=[row for row in values if row['time_ms']//interval not in covered],
                    stats=stats,interval_ms=interval,period=period_for(minutes),fx=fx,reference_only=True,
                    source='MT5 双品种收盘价' if native_pair(config) else '币安合约成交收盘价 × 当前参考汇率 − MT5 收盘价',
                    coverage_start=values[0]['time_ms'] if values else None,
                    coverage_end=values[-1]['time_ms']+interval if values else None,cache_warning=warning)

    def _read_cache(self,key,start,end,interval):
        try:
            path=self.directory/(key+'.json')
            if path.stat().st_size>1024*1024: return None
            data=json.loads(path.read_text(encoding='utf-8'))
            if (data.get('schema')!=1 or data.get('start')>start or data.get('end')>end or
                    end-data.get('end',0)>CACHE_TTL*1000 or
                    not 0<=time.time()-data.get('written_at',0)<=CACHE_TTL): return None
            rows=data['rows']
            if (not isinstance(rows,list) or len(rows)>MAX_POINTS or
                    any(not positive(r.get('time_ms')) or r['time_ms']%interval or not positive(r.get('short_close')) or
                        not positive(r.get('long_close')) for r in rows)): return None
            times=[r['time_ms'] for r in rows]
            if times!=sorted(set(times)) or any(t<data['start'] or t+interval>data['end'] for t in times): return None
            return rows
        except (OSError,ValueError,TypeError,KeyError,AttributeError): return None

    def _write_cache(self,key,rows,start,end):
        self.directory.mkdir(parents=True,exist_ok=True)
        path=self.directory/(key+'.json');temporary=path.with_suffix('.tmp')
        try:
            temporary.write_text(json.dumps(dict(schema=1,start=start,end=end,written_at=time.time(),rows=rows),allow_nan=False),encoding='utf-8')
            os.replace(temporary,path)
            files=sorted(self.directory.glob('*.json'),key=lambda p:p.stat().st_mtime,reverse=True)
            for old in files[8:]: old.unlink(missing_ok=True)
        finally:
            temporary.unlink(missing_ok=True)

    def _run(self):
        while True:
            with self.lock:
                if self.stopped or self.pending is None:
                    self.active_key=None;self.thread=None;return
                key,config,period,start,end,allow_fetch=self.pending;self.pending=None
                self.active_key=key;self.cancel=threading.Event();cancel=self.cancel
                self.entries[key]['state']='loading'
            try:
                rows=self._read_cache(key,start,end,PERIODS[period]);from_cache=rows is not None
                if rows is None and not allow_fetch:
                    with self.lock: self.entries[key].update(state='idle',cache_checked=True)
                    continue
                if rows is None: rows=self.fetch(config,period,start,end,cancel)
                if cancel.is_set(): raise Cancelled()
                warning=''
                try:
                    if not from_cache: self._write_cache(key,rows,start,end)
                except OSError as exc: warning='参考历史缓存未写入：'+storage_error_message(exc)
                with self.lock: self.entries[key].update(state='ready',rows=rows,updated_at=time.monotonic(),error='',cache_warning=warning,cache_checked=True)
            except Cancelled:
                with self.lock: self.entries[key]['state']='idle'
            except Exception as exc:
                with self.lock: self.entries[key].update(state='error',error='历史参考补取失败：'+storage_error_message(exc),updated_at=time.monotonic())
