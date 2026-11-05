"""On-demand, allowlisted execution export using an independent WAL reader."""
import json
import math
import re
import sqlite3
import time
from pathlib import Path


def numbers(row, fields):
    return {key:value for key in fields.split() if isinstance((value:=row.get(key)),(int,float))
            and not isinstance(value,bool) and math.isfinite(value)}


def choice(row, key, allowed):
    return row.get(key) if row.get(key) in allowed else 'unknown'


def symbol(value):
    return value if isinstance(value,str) and re.fullmatch(r'[\w.#+/-]{1,40}',value) else None


def build_report(state, events):
    groups=state.get('groups',[])[-200:]
    group_ids={g['id']:'G'+str(i+1) for i,g in enumerate(groups)}
    orders=[o for o in state.get('orders',[]) if o.get('group') in group_ids]
    order_ids={o['id']:'O'+str(i+1) for i,o in enumerate(orders)}
    report={'schema_version':1,'exported_ms':int(time.time()*1000),
        'scope':{'group_limit':200,'event_limit':2000,'groups':len(groups),'orders':len(orders)},
        'notes':[
            '仅含最近200组已持久化执行记录和最近2000条中关联订单的白名单事件；导出时正在执行的记录可能尚未完成。',
            '组和订单ID已替换；不含API凭据、账户、服务器、票据、路径或原始错误文本。仍包含价格、数量和交易时间。',
            'signal_spread为组触发价差；quote_spread及quote_context为各腿调用前报价；actual_spread为双边成交均价差，非同时市场报价。',
            'actual_spread=币安成交均价×记录汇率−MT5成交均价；spread_slippage=实际−触发，平仓正偏移通常更不利。',
            'dispatch_ms是调用交易适配器的本机时间；request_ms包括适配器内部校验及请求，并非纯网络或撮合耗时。',
            'response_received_ms/fill_time_ms为本机收到直接回报时间；reconcile_time_ms为后续查询确认时间。',
            'execution_open/close.elapsed_ms使用单调时钟，含本机同步日志、双腿串行处理和可能的查询等待，截止组完成确认前；不含最后一次组状态提交。',
            '缺少的字段表示旧版未记录或无法确定，不能当作零；重启后不推算单调耗时；纸面模式不可用于评估实盘速度。',
            '导出按需读取本机数据库，不调用交易平台、不取得交易线程或写日志的锁。'
        ],'groups':[],'orders':[],'events':[]}
    for g in groups:
        row={'id':group_ids[g['id']], 'mode':choice(g,'mode',('paper','live')),
             'status':choice(g,'status',('opening','open','closing','unwinding','attention','closed')),
             'imported':bool(g.get('imported')), 'symbol':symbol(g.get('symbol')),
             'mt5_symbol':symbol(g.get('mt5_symbol')),
             'first_leg':choice(g,'first_leg',('binance','mt5')),
             'second_leg':choice(g,'second_leg',('binance','mt5')),
             **numbers(g,'opened_ms closed_ms qty lots contract entry entry_signal exit exit_signal entry_threshold grid_index')}
        row['parameters']=numbers(g.get('parameters',{}),'entry_spread_usd exit_spread_usd take_contraction_usd max_slippage_usd unwind_slippage_usd max_unhedged_ms max_quote_age_ms max_clock_skew_ms')
        row['costs']=numbers(g.get('costs',{}),'usdt_usd')
        for action in ('open','close'):
            if 'execution_'+action in g:
                row['execution_'+action]=numbers(g['execution_'+action],'started_ms signal_time_ms signal_spread confirmed_ms elapsed_ms')
        report['groups'].append(row)
    for o in orders:
        result=o.get('result',{})
        row={'id':order_ids[o['id']],'group':group_ids[o['group']],
             'leg':choice(o,'leg',('binance','mt5')),'action':choice(o,'action',('open','close')),
             'symbol':symbol(o.get('symbol')),'imported':bool(o.get('imported')),
             **numbers(o,'requested reference limit slippage fx top_qty created_ms dispatch_ms intent_journal_ms signal_spread signal_time_ms quote_spread actual_spread spread_slippage')}
        row['result']={'status':choice(result,'status',('done','pending','unknown')),
                       **numbers(result,'qty price fee request_ms response_received_ms fill_time_ms reconcile_time_ms')}
        code=result.get('error_code')
        if isinstance(code,int) or code in ('DEPTH_INSUFFICIENT','DEPTH_CHECK_FAILED'):
            row['result']['error_code']=code
        row['quote_context']={leg:numbers(o.get('quote_context',{}).get(leg,{}),
            'bid ask bid_qty ask_qty time_ms observed_ms source_time_ms') for leg in ('binance','mt5')}
        row['reconcile']=numbers(o.get('reconcile',{}),'attempts started_ms next_ms')
        report['orders'].append(row)
    kinds={'order_intent','order_result','order_reconciled','order_reconcile_retry','order_reconcile_exhausted',
           'group_opening','group_opened','group_closed','exit_signal','entry_skipped'}
    for event in events:
        data=event.get('data',{})
        if event.get('kind') not in kinds: continue
        oid=order_ids.get(data.get('id'));gid=group_ids.get(data.get('group'))
        if oid is None and gid is None: continue
        row={'kind':event['kind'],**numbers(event,'time'),
             'data':numbers(data,'attempt query_ms signal_spread entry qty net_estimate')}
        if oid: row['data']['order']=oid
        if gid: row['data']['group']=gid
        report['events'].append(row)
    return report


def export_report(path):
    # Separate read connection: never enter Store.lock or TradingRuntime.lock.
    # WAL readers do not block order-intent commits. Only one click reads data.
    db=sqlite3.connect(Path(path).resolve().as_uri()+'?mode=ro',uri=True,timeout=1)
    try:
        db.execute('BEGIN')
        row=db.execute('SELECT value FROM state WHERE id=1').fetchone()
        events=[{'time':r[0],'kind':r[1],'data':json.loads(r[2])} for r in db.execute(
            'SELECT time,kind,data FROM events ORDER BY id DESC LIMIT 2000')]
        state=json.loads(row[0]) if row else {}
    finally:
        db.close()
    return build_report(state,events)
