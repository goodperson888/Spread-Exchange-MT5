"""On-demand, allowlisted execution export using an independent WAL reader."""
import json
import math
import re
import sqlite3
import time
from pathlib import Path
from execution_latency import PROCESS_LATENCY
from exit_observations import PROCESS_EXIT_OBSERVATIONS
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
    if row.get('exit_mode') in ('group','basket'):result['exit_mode']=row['exit_mode']
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
    report={'schema_version':2,'receipt_schema_version':1,'exported_ms':int(time.time()*1000),
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
    margin=state.get('entry_margin')
    if isinstance(margin,dict):
        clean=numbers(margin,'checked_ms lots total_lots managed_lots required available')
        if type(margin.get('ready')) is bool: clean['ready']=margin['ready']
        if margin.get('state') in ('ready','insufficient','unavailable','capacity_full'): clean['state']=margin['state']
        if margin.get('phase') in ('connect','start'): clean['phase']=margin['phase']
        report['runtime']['entry_margin']=clean
    report['notes'].extend([
        'result.close_cause为已确认外部成交的固定原因证据：MT5保留原reason数字码，6表示Stop Out；币安仅将已签名LIQUIDATION强平订单与原成交编号、方向、数量、均价精确匹配后标记强平。liquidation_protection_armed仅表示已安排保护性平仓，不证明已经双平；liquidation_recovery记录本机发现/安排时间和当时剩余数量。缺失原因不等于手动平仓，平台原始时间不用于推算本机耗时。',
        'quote_source=cache表示下单前使用后台缓存报价，refresh表示走原同步报价读取路径；不是纯网络耗时。开仓第二腿缓存需两边本机接收时间均在250毫秒内，同时满足用户配置的更严格行情时效和时差限制；缓存不足仍走原查询，不放宽开仓敞口时限。',
        'pnl_schema_version=2将币安线性合约价差盈亏先以USDT计算后换汇；已平仓的换算汇率固定，不对开平两笔名义本金分别换汇。旧记录的价差变化不能直接当作净收益。',
        'exit_decision记录下单前止盈判断的预估收益、预计返佣及阈值，不是成交保证；exit_deferred表示第一腿前报价已不满足逐组止盈条件。adverse_slippage按各订单原报价和成交价计算，正值为不利，slippage_exceeded提示超出预算但不否定真实成交。',
        '币安平仓直接回报为完整FILLED数量但缺少均价时，会做一次有请求超时预算的原订单补查；补查失败保持暂停，已暂停状态不自动恢复。',
        'runtime为本机最后已提交状态，不代表导出时实时持仓；启动/暂停历史受最近2000条事件上限约束。',
        'initial_result保留下单直接回报，valid仅指通过本机字段校验，不代表已确认成交；reconcile.last_query及事件保留查询预算、耗时、固定错误分类和平台数字错误码，不导出原始错误或账号标识。',
        'query_timeout_ms是适配器请求超时参数，并非严格总耗时上限；多阶段网络请求可能超出该值。1.5秒等敞口配置仍单独决定能否开第二腿。',
        'confirmation为MT5成交核对阶段、各查询返回状态和数量校验字段；has_ticket/has_deal等只表示是否取得编号，不含编号值。first_unconfirmed_query保留第一次未确认的查询；稳定重复事件限频，attempt仍累计实际查询次数。',
        'receipt为已有平台回执的白名单字段与原字段类型；missing、null、零值和无法解析分别保留。币安values含avgPrice、cumQuote、executedQty及平台时间，price_source说明归一化价格来源；MT5含直接order_send回执，precheck_receipt单列order_check结果，即使同次调用已由历史确认也保留原回执。编号仅保留有无，不导出请求回显、账户、注释和错误原文。',
        'validation_issues为本机校验失败的固定分类；reconcile.first_cancel/last_cancel记录首次及最近一次撤单调用返回与耗时，returned仅代表调用正常返回，不证明撤单成功；撤单后仍查询原订单。ack_received_ms为MT5原生下单调用返回的本机时间，早于可能的历史补查。所有新增采集复用已有调用和日志提交，不增加查询；平台原始时间不用于推断时区或本机延迟。',
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
        if isinstance(g.get('liquidation_recovery'),dict):
            row['liquidation_recovery']=numbers(g['liquidation_recovery'],'detected_ms armed_ms remaining_mt5 remaining_binance')
        row['costs']=numbers(g.get('costs',{}),'usdt_usd binance_rebate_percent mt5_rebate_percent')
        row['valuation']=numbers(g.get('valuation',{}),'pnl_schema_version binance_pnl_usdt mt5_pnl_usd valuation_fx gross fees estimated_exit_fee net_before_rebate estimated_rebate estimated_rebate_binance estimated_rebate_mt5 net')
        if isinstance(g.get('exit_decision'),dict):
            decision=g['exit_decision']
            row['exit_decision']=numbers(decision,'signal_spread entry_spread contraction target_contraction target_spread min_net_profit estimated_net estimated_net_before_rebate estimated_gross estimated_exit_fee estimated_rebate valuation_fx basket_estimated_net')
            if type(decision.get('require_net_profit')) is bool: row['exit_decision']['require_net_profit']=decision['require_net_profit']
            if decision.get('target_mode') in ('contraction','absolute'): row['exit_decision']['target_mode']=decision['target_mode']
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
             **numbers(o,'requested reference limit slippage fx top_qty created_ms dispatch_ms intent_journal_ms signal_spread signal_time_ms quote_spread actual_spread spread_slippage adverse_slippage')}
        if o.get('quote_source') in ('cache','refresh'): row['quote_source']=o['quote_source']
        if type(o.get('slippage_exceeded')) is bool: row['slippage_exceeded']=o['slippage_exceeded']
        row['result']={'status':choice(result,'status',('done','pending','unknown')),
                       **numbers(result,'qty price fee fee_verified_ms swap request_ms response_received_ms fill_time_ms reconcile_time_ms platform_time_ms retcode ack_volume ack_price')}
        row['result'].update(result_evidence(result))
        if isinstance(o.get('initial_result'),dict): row['initial_result']=result_evidence(o['initial_result'])
        row['quote_context']={leg:numbers(o.get('quote_context',{}).get(leg,{}),
            'bid ask bid_qty ask_qty time_ms observed_ms source_time_ms') for leg in ('binance','mt5')}
        row['reconcile']=numbers(o.get('reconcile',{}),'attempts started_ms next_ms cancel_attempts')
        rec=o.get('reconcile',{})
        if type(rec.get('cancel_sent')) is bool: row['reconcile']['cancel_sent']=rec['cancel_sent']
        for key in ('first_cancel','last_cancel'):
            if isinstance(rec.get(key),dict):
                cancel=rec[key]
                row['reconcile'][key]={**numbers(cancel,'time_ms elapsed_ms'),
                                       'result':result_evidence(cancel.get('result',{}))}
                if type(cancel.get('returned')) is bool: row['reconcile'][key]['returned']=cancel['returned']
        if type(rec.get('exhausted')) is bool: row['reconcile']['exhausted']=rec['exhausted']
        if isinstance(rec.get('last_query'),dict):
            row['reconcile']['last_query']={**result_evidence(rec['last_query']),
                                          **numbers(rec['last_query'],'time_ms query_ms query_timeout_ms')}
        if isinstance(rec.get('first_unconfirmed_query'),dict):
            first=rec['first_unconfirmed_query']
            row['reconcile']['first_unconfirmed_query']={**result_evidence(first),
                                                        **numbers(first,'time_ms query_ms query_timeout_ms')}
        report['orders'].append(row)
    kinds={'order_intent','order_result','order_reconciled','order_reconcile_retry','order_reconcile_exhausted',
           'group_opening','group_opened','group_closed','exit_signal','exit_deferred','entry_skipped','close_attempt','entry_recovered','opening_fee_verified'}
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
        if event.get('kind') in ('external_position_changed','external_close_backfilled','external_orders_resolved','recovery_flat_confirmed','liquidation_protection_armed'):
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
             'data':numbers(data,'attempt query_ms query_timeout_ms signal_spread entry qty net_estimate fee entry_spread contraction target_contraction target_spread min_net_profit estimated_net estimated_net_before_rebate estimated_gross estimated_exit_fee estimated_rebate valuation_fx basket_estimated_net')}
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
    report=build_report(state,events)
    aliases={g['id']:'G'+str(i+1) for i,g in enumerate(state.get('groups',[])[-200:])}
    report['exit_observations']=PROCESS_EXIT_OBSERVATIONS.snapshot(aliases)
    report['notes'].append('exit_observations为本次连接内策略实际检查过的退出条件，最多512段；原因、规则或报价有效性改变及每30秒开启新段，first/latest为该段首末证据，samples累计检查次数，min_exit_spread/max_net为段内极值，极值未必同时出现。仅用限量内存，不增加平台请求或日志提交；重启/重新连接清空。报价无效、恢复等待等提前返回不重新估值，缺少net不能当作零。规则中revision对应当时锁定版本，整篮子按加权入场与篮子净收益检查。图表记录与策略取样独立；未被策略取用的瞬时行情、导出前旧版未采集的瞬间及已淘汰段无法还原。内存与持久化快照不是原子快照，未映射组仅统计数量。')
    report['latency']=PROCESS_LATENCY.snapshot()
    report['notes'].append('latency为当前进程内最近最多512个阶段样本的统计，重启或重新连接清空，mode区分纸面与实盘；不增加平台查询或日志提交。strategy_pass为一次engine.tick，含执行与确认，decision为条件评估及估值；quote_wait为最新一侧报价首次被策略取用时距本机接收的时间，两边quote_age分别为策略取用时的本机报价年龄，不能当作跨平台网络延迟。')
    return report
