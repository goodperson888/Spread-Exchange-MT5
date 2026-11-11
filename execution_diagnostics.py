"""On-demand, allowlisted execution export using an independent WAL reader."""
import json
import math
import re
import sqlite3
import time
from pathlib import Path
from execution_evidence import reason_code, REASON_LABELS, result_evidence


def numbers(row, fields):
    return {key:value for key in fields.split() if isinstance((value:=row.get(key)),(int,float))
            and not isinstance(value,bool) and math.isfinite(value)}


def choice(row, key, allowed):
    return row.get(key) if row.get(key) in allowed else 'unknown'


def symbol(value):
    return value if isinstance(value,str) and re.fullmatch(r'[\w.#+/-]{1,40}',value) else None


def exit_parameters(row):
    result=numbers(row,'take_contraction_usd exit_spread_usd min_net_profit_usd group_max_loss_usd max_hold_minutes')
    for key in ('require_net_profit','group_loss_enabled'):
        if type(row.get(key)) is bool:result[key]=row[key]
    if row.get('target_mode') in ('contraction','absolute'):result['target_mode']=row['target_mode']
    return result


def safe_reason(row, key='reason'):
    code=row.get('reason_code')
    if not isinstance(code,str) or code not in REASON_LABELS: code=reason_code(row.get(key))
    return {'reason_code':code,'reason_label':REASON_LABELS[code]}


def pause_evidence(row):
    data={**numbers(row,'time_ms'),**safe_reason(row)}
    for key in ('enabled_before','keep_entry'):
        if type(row.get(key)) is bool: data[key]=row[key]
    return data


def build_report(state, events):
    groups=state.get('groups',[])[-200:]
    group_ids={g['id']:'G'+str(i+1) for i,g in enumerate(groups)}
    orders=[o for o in state.get('orders',[]) if o.get('group') in group_ids]
    order_ids={o['id']:'O'+str(i+1) for i,o in enumerate(orders)}
    report={'schema_version':2,'exported_ms':int(time.time()*1000),
        'scope':{'group_limit':200,'event_limit':2000,'groups':len(groups),'orders':len(orders)},
        'notes':[
            '仅含最近200组已持久化执行记录及最近2000条中的执行、启动、暂停白名单事件；导出时正在执行的记录可能尚未完成。',
            '组和订单ID已替换；不含API凭据、账户、服务器、票据、路径或原始错误文本。仍包含价格、数量和交易时间。',
            'signal_spread为组触发价差；quote_spread及quote_context为各腿调用前报价；actual_spread为双边成交均价差，非同时市场报价。',
            'actual_spread=币安成交均价×记录汇率−MT5成交均价；spread_slippage=实际−触发，平仓正偏移通常更不利。',
            'dispatch_ms是调用交易适配器的本机时间；request_ms包括适配器内部校验及请求，并非纯网络或撮合耗时。',
            'response_received_ms/fill_time_ms为本机收到直接回报时间；reconcile_time_ms为后续查询确认时间。',
            'execution_open/close.elapsed_ms使用单调时钟，含本机同步日志、双腿串行处理和可能的查询等待，截止组完成确认前；不含最后一次组状态提交。',
            '缺少的字段表示旧版未记录或无法确定，不能当作零；重启后不推算单调耗时；纸面模式不可用于评估实盘速度。',
            '导出按需读取本机数据库，不调用交易平台、不取得交易线程或写日志的锁。',
            'source=external 表示平台外部平仓回补；platform_time_ms是未经时区推算的平台原始时间，reconcile_time_ms是本机发现时间，组closed_ms为本机确认完成时间；两者差不代表程序下单耗时。',
            'estimated_rebate为用户按手续费比例预计的返佣，非实际到账；net_before_rebate不含该项，net含该项并参与退出判断。'
        ],'groups':[],'orders':[],'events':[]}
    report['runtime']={**numbers(state,'boot_ms last_start_ms'),**safe_reason(state,'alarm')}
    for key in ('enabled','recovery'):
        if type(state.get(key)) is bool: report['runtime'][key]=state[key]
    if state.get('mode') in ('live','paper'): report['runtime']['mode']=state['mode']
    if isinstance(state.get('last_pause'),dict): report['runtime']['last_pause']=pause_evidence(state['last_pause'])
    report['notes'].extend([
        'runtime为本机最后已提交状态，不代表导出时实时持仓；启动/暂停历史受最近2000条事件上限约束。',
        'initial_result保留下单直接回报，valid仅指通过本机字段校验，不代表已确认成交；reconcile.last_query及事件保留查询预算、耗时、固定错误分类和平台数字错误码，不导出原始错误或账号标识。',
        'query_timeout_ms是适配器请求超时参数，并非严格总耗时上限；多阶段网络请求可能超出该值。1.5秒等敞口配置仍单独决定能否开第二腿。',
        'confirmation为MT5成交核对阶段、各查询返回状态和数量校验字段；has_ticket/has_deal等只表示是否取得编号，不含编号值。first_unconfirmed_query保留第一次未确认的查询；稳定重复事件限频，attempt仍累计实际查询次数。',
    ])
    probe=state.get('mt5_history_probe')
    if isinstance(probe,dict):
        report['mt5_history_probe']=probe
        report['notes'].append('mt5_history_probe为只读历史字段探针：订单、成交和持仓编号已替换为别名；用于比较经纪商返回的字段、类型和关联关系。')
    for g in groups:
        row={'id':group_ids[g['id']], 'mode':choice(g,'mode',('paper','live')),
             'status':choice(g,'status',('opening','open','closing','unwinding','attention','closed')),
             'imported':bool(g.get('imported')), 'symbol':symbol(g.get('symbol')),
             'external_close':bool(g.get('external_close')),
             'mt5_symbol':symbol(g.get('mt5_symbol')),
             'first_leg':choice(g,'first_leg',('binance','mt5')),
             'second_leg':choice(g,'second_leg',('binance','mt5')),
             **numbers(g,'opened_ms closed_ms qty lots contract entry entry_signal exit exit_signal entry_threshold grid_index')}
        if g.get('reason'): row.update(safe_reason(g))
        row['parameters']=numbers(g.get('parameters',{}),'entry_spread_usd exit_spread_usd take_contraction_usd max_slippage_usd unwind_slippage_usd max_unhedged_ms max_quote_age_ms max_clock_skew_ms')
        row['parameters'].update(exit_parameters(g.get('parameters',{})))
        row.update(numbers(g,'exit_rules_revision'))
        row['costs']=numbers(g.get('costs',{}),'usdt_usd binance_rebate_percent mt5_rebate_percent')
        row['valuation']=numbers(g.get('valuation',{}),'gross fees estimated_exit_fee net_before_rebate estimated_rebate estimated_rebate_binance estimated_rebate_mt5 net')
        for action in ('open','close'):
            if 'execution_'+action in g:
                row['execution_'+action]=numbers(g['execution_'+action],'started_ms signal_time_ms signal_spread confirmed_ms elapsed_ms')
        report['groups'].append(row)
    for o in orders:
        result=o.get('result',{})
        row={'id':order_ids[o['id']],'group':group_ids[o['group']],
             'leg':choice(o,'leg',('binance','mt5')),'action':choice(o,'action',('open','close')),
             'symbol':symbol(o.get('symbol')),'imported':bool(o.get('imported')),
             'source':'external' if o.get('source')=='external' else 'adopted' if o.get('imported') else 'program',
             **numbers(o,'requested reference limit slippage fx top_qty created_ms dispatch_ms intent_journal_ms signal_spread signal_time_ms quote_spread actual_spread spread_slippage')}
        row['result']={'status':choice(result,'status',('done','pending','unknown')),
                       **numbers(result,'qty price fee swap request_ms response_received_ms fill_time_ms reconcile_time_ms platform_time_ms retcode ack_volume ack_price')}
        row['result'].update(result_evidence(result))
        if isinstance(o.get('initial_result'),dict): row['initial_result']=result_evidence(o['initial_result'])
        row['quote_context']={leg:numbers(o.get('quote_context',{}).get(leg,{}),
            'bid ask bid_qty ask_qty time_ms observed_ms source_time_ms') for leg in ('binance','mt5')}
        row['reconcile']=numbers(o.get('reconcile',{}),'attempts started_ms next_ms')
        rec=o.get('reconcile',{})
        if type(rec.get('exhausted')) is bool: row['reconcile']['exhausted']=rec['exhausted']
        if isinstance(rec.get('last_query'),dict):
            row['reconcile']['last_query']={**result_evidence(rec['last_query']),
                                          **numbers(rec['last_query'],'query_ms query_timeout_ms')}
        if isinstance(rec.get('first_unconfirmed_query'),dict):
            first=rec['first_unconfirmed_query']
            row['reconcile']['first_unconfirmed_query']={**result_evidence(first),
                                                        **numbers(first,'time_ms query_ms query_timeout_ms')}
        report['orders'].append(row)
    kinds={'order_intent','order_result','order_reconciled','order_reconcile_retry','order_reconcile_exhausted',
           'group_opening','group_opened','group_closed','exit_signal','entry_skipped','close_attempt','entry_recovered'}
    for event in events:
        data=event.get('data') or {}
        if event.get('kind') in ('boot','start','pause','reconciled','paper_reconciled','auto_reconciled_flat','close_requested'):
            row={'kind':event['kind'],**numbers(event,'time'),'data':{}}
            if event['kind']=='pause': row['data'].update(pause_evidence(data))
            elif event['kind']=='boot': row['data'].update(safe_reason({'reason':'程序重启'}))
            elif data.get('reason'): row['data'].update(safe_reason(data))
            if data.get('mode') in ('live','paper'): row['data']['mode']=data['mode']
            gid=group_ids.get(data.get('group'))
            if gid: row['data']['group']=gid
            report['events'].append(row)
            continue
        if event.get('kind') in ('external_position_changed','external_close_backfilled','external_orders_resolved','recovery_flat_confirmed'):
            related=[group_ids[x] for x in data.get('groups',[]) if x in group_ids]
            if related: report['events'].append({'kind':event['kind'],**numbers(event,'time'),'data':{'groups':related}})
            continue
        if event.get('kind')=='exit_rules_updated':
            changes=[]
            for change in data.get('changes',[])[:50]:
                gid=group_ids.get(change.get('group'))
                if gid:changes.append({'group':gid,**numbers(change,'revision'),
                                       'before':exit_parameters(change.get('before',{})),
                                       'after':exit_parameters(change.get('after',{})),
                                       'rebates_before':numbers(change.get('rebates_before',{}),'binance_rebate_percent mt5_rebate_percent'),
                                       'rebates_after':numbers(change.get('rebates_after',{}),'binance_rebate_percent mt5_rebate_percent')})
            if changes:report['events'].append({'kind':'exit_rules_updated',**numbers(event,'time'),'data':{'changes':changes}})
            continue
        if event.get('kind') not in kinds: continue
        oid=order_ids.get(data.get('id'));gid=group_ids.get(data.get('group'))
        if oid is None and gid is None: continue
        row={'kind':event['kind'],**numbers(event,'time'),
             'data':numbers(data,'attempt query_ms query_timeout_ms signal_spread entry qty net_estimate')}
        if isinstance(data.get('result'),dict): row['data']['result']=result_evidence(data['result'])
        if data.get('reason'): row['data'].update(safe_reason(data))
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
