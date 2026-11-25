"""On-demand, allowlisted execution export using an independent WAL reader."""
import json
import math
import re
import sqlite3
import time
from pathlib import Path
from execution_latency import PROCESS_LATENCY
from exit_observations import PROCESS_EXIT_OBSERVATIONS
from entry_observations import PROCESS_ENTRY_OBSERVATIONS
from execution_evidence import reason_code, REASON_LABELS, result_evidence
from commission_reference import safe_references
from reference_diagnostics import PROCESS_REFERENCE_DIAGNOSTICS
from liquidity_guard import safe_evidence as liquidity_evidence


_CURRENT_CONNECTION = {}


def register_connection(sample):
    """Publish a tiny immutable connection outcome; never retain config/errors."""
    global _CURRENT_CONNECTION
    clean=numbers(sample,'started_ms elapsed_ms requested_lots effective_lots effective_qty')
    for key in ('requested_mode','effective_mode'):
        if sample.get(key) in ('paper','live'):clean[key]=sample[key]
    if sample.get('state') in ('connecting','connected','failed'):clean['state']=sample['state']
    if sample.get('error_code') in ('NONE','QUOTE_TIME_REGRESSION','STORAGE_FULL','TIMEOUT','CONNECTION_ERROR'):
        clean['error_code']=sample['error_code']
    if type(sample.get('connected')) is bool:clean['connected']=sample['connected']
    _CURRENT_CONNECTION=clean
    return dict(clean)


def numbers(row, fields):
    return {key:value for key in fields.split() if isinstance((value:=row.get(key)),(int,float))
            and not isinstance(value,bool) and math.isfinite(value)}


def choice(row, key, allowed):
    return row.get(key) if row.get(key) in allowed else 'unknown'


def symbol(value):
    return value if isinstance(value,str) and re.fullmatch(r'[\w.#+/-]{1,40}',value) else None


def exit_parameters(row):
    result=numbers(row,'take_contraction_usd exit_spread_usd min_net_profit_usd group_max_loss_usd max_hold_minutes exit_confirmation_ticks')
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
    terminal_ids={}
    def terminal_alias(ref):
        identity=ref.get('identity')
        if not isinstance(identity,str) or not identity:return None
        if identity not in terminal_ids:terminal_ids[identity]='T'+str(len(terminal_ids)+1)
        return terminal_ids[identity]
    report={'schema_version':2,'receipt_schema_version':1,'exported_ms':int(time.time()*1000),
        'scope':{'group_limit':200,'event_limit':2000,'groups':len(groups),'orders':len(orders)},
        'notes':[
            '仅含最近200组已持久化执行记录及最近2000条中的执行、启动、暂停白名单事件；导出时正在执行的记录可能尚未完成。',
            '组和订单ID已替换；不含API凭据、账户、服务器、票据、路径或原始错误文本。仍包含价格、数量和交易时间。',
            'signal_spread为组触发价差；quote_spread及quote_context为各腿调用前报价；actual_spread为双边成交均价差，非同时市场报价。',
            'actual_spread=币安成交均价×记录汇率−MT5成交均价；MT5 配对时分别表示 A、B 两腿。spread_slippage=实际−触发，平仓正偏移通常更不利。跨终端配对的 endpoints 使用 T1、T2 等本次导出内一致的匿名代号，已有组始终在原终端核对和平仓。',
            'dispatch_ms是调用交易适配器的本机时间；request_ms包括适配器内部校验及请求，并非纯网络或撮合耗时。',
            'response_received_ms/fill_time_ms为本机收到直接回报时间；reconcile_time_ms为后续查询确认时间。',
            'execution_open/close.elapsed_ms使用单调时钟，含本机同步日志、双腿执行和可能的查询等待，截止组完成确认前；不含最后一次组状态提交。',
            '缺少的字段表示旧版未记录或无法确定，不能当作零；重启后不推算单调耗时；纸面模式不可用于评估实盘速度。',
            '导出按需读取本机数据库，不调用交易平台、不取得交易线程或写日志的锁。',
            'source=external 表示平台外部平仓回补；platform_time_ms是未经时区推算的平台原始时间，reconcile_time_ms是本机发现时间，组closed_ms为本机确认完成时间；两者差不代表程序下单耗时。',
            'estimated_rebate为用户按手续费比例预计的返佣，非实际到账；net_before_rebate不含该项，net含该项并参与退出判断。'
        ],'groups':[],'orders':[],'events':[]}
    report.update(PROCESS_REFERENCE_DIAGNOSTICS.snapshot())
    report['notes'].append('chart_reference 与 mt5_commission_reference 是本进程最近后台读取的脱敏快照，不代表实时平台状态；历史失败请重试后导出。组内 commission_reference 和 commission_estimates 为开仓时锁定的历史费用参考及每手开仓／平仓估算，实际成交费用优先；未取得可靠样本时使用备用配置，返佣仍为用户预计。')
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
        'pair.exchange=lighter 表示 Lighter 主网 USDC 永续；兼容字段 leg=binance 代表交易所腿，usdt_usd/commissions_usdt 在该组按 USDC/USD 和 USDC 费用解释。未核验的非零 fee tick 保留估算，不当作实付手续费。',
        'pair.type=mt5_mt5 表示同一 MT5 的相同手数组合；兼容字段 leg=binance 在此类组中代表 MT5 品种 A，leg=mt5 代表 MT5 品种 B；direction=forward 为卖 A 买 B，reverse 为买 A 卖 B，旧组缺失时为 forward。执行价差按实际卖出腿−买入腿归一化，反向组为 B−A，图表统一显示 A−B；parameters.exit_spread_usd 和 exit_decision.target_spread 仍为配置的 A−B 绝对阈值。quantity_unit=lot 时 requested/qty/remaining 均为手数，leg_contracts 为各自每手合约大小；platform=mt5，native_position_side=1 为空头、0 为多头。价差使用原始同报价单位价格相减，盈亏各乘各自合约大小。并发适配器调用不代表同一 MT5 终端能并发撮合。',
        'result.close_cause为已确认外部成交的固定原因证据：MT5保留原reason数字码，6表示Stop Out；币安仅将已签名LIQUIDATION强平订单与原成交编号、方向、数量、均价精确匹配后标记强平。liquidation_protection_armed仅表示已安排保护性平仓，不证明已经双平；liquidation_recovery记录本机发现/安排时间和当时剩余数量。缺失原因不等于手动平仓，平台原始时间不用于推算本机耗时。',
        'quote_source=cache表示下单前使用后台缓存报价，refresh表示走原同步报价读取路径；不是纯网络耗时。开仓第二腿缓存需两边本机接收时间均在250毫秒内，同时满足用户配置的更严格行情时效和时差限制；缓存不足仍走原查询，不放宽开仓敞口时限。',
        'pnl_schema_version=2将币安线性合约价差盈亏先以USDT计算后换汇；已平仓的换算汇率固定，不对开平两笔名义本金分别换汇。旧记录的价差变化不能直接当作净收益。',
        'exit_decision记录下单前止盈判断的预估收益、预计返佣及阈值，不是成交保证；confirmation记录正常止盈触发时连续达标次数count及要求次数required，旧记录缺失不补值。exit_confirmation_ticks默认1，多次时同一源报价重复读取不计数；手动、止损、超时及保护退出不等待次数。exit_deferred表示第一腿前报价已不满足逐组止盈条件。adverse_slippage按各订单原报价和成交价计算，正值为不利，slippage_exceeded提示超出预算但不否定真实成交。',
        'execution_request复用MT5下单前已读取的报价与请求：price为请求价格，bid/ask为发单前终端报价，point为最小报价单位，deviation_points为请求允许偏差点数，trade_execution_mode为MT5执行模式数字码，filling为成交方式。只保留数值白名单，不含请求账号、终端路径、编号或注释，不额外查询。MT5市价执行的偏差参数不等于保证成交价格上限。MT5_SLIPPAGE_PRECHECK表示本地滑点检查拦截，submitted=false，未调用order_check/order_send；execution_precheck记录已有终端报价、可执行价price、允许边界limit及滑点预算，tick_time_ms仅为平台原始时间。',
        '币安平仓直接回报为完整FILLED数量但缺少均价时，会做一次有请求超时预算的原订单补查；补查失败保持暂停，已暂停状态不自动恢复。',
        'runtime为本机最后已提交状态，不代表导出时实时持仓；启动/暂停历史受最近2000条事件上限约束。',
        'initial_result保留下单直接回报，valid仅指通过本机字段校验，不代表已确认成交；reconcile.last_query及事件保留查询预算、耗时、固定错误分类和平台数字错误码，不导出原始错误或账号标识。',
        'query_timeout_ms是适配器请求超时参数，并非严格总耗时上限；多阶段网络请求可能超出该值。1.5秒等敞口配置仍单独决定能否开第二腿。',
        'confirmation为MT5成交核对阶段、各查询返回状态和数量校验字段；has_ticket/has_deal等只表示是否取得编号，不含编号值。first_unconfirmed_query保留第一次未确认的查询；稳定重复事件限频，attempt仍累计实际查询次数。',
        'close_attempts为各平台实际平仓请求次数，retry_limit为每个平台的请求上限；等待报价或查询原订单不占次数。订单close_attempt和slippage_mode记录该次请求计数及普通/保护滑点。close_protection记录发现单边剩余敞口的本机时间和数量，close_halt保留达到重试上限时的平台及剩余数量；后续外部平仓不会抹去这些证据。旧版整轮计数不能还原为每个平台请求次数。',
        'dispatch_mode=parallel表示双边开仓或已配平程序组平仓的两腿意图在一个事务中持久化，再并发调用适配器；同一组close_batch标识同批平仓请求。serial表示串行开仓或逐笔减仓。dispatch_queue_ms为意图全部落盘至该线程调用的单调耗时，last_dispatch_gap_ms为最近一批两个适配器调用的本机时间间隔，不是同时成交保证。未发出的本地拦截记录submitted=false，不计实际请求耗时；线程创建失败须手动重试，报价在落盘后过期则等待新报价。',
        '原生MT5平仓成功回执缺价格、原订单短暂待处理时最多提供300ms本机核对期，期间平仓组继续阻止开新仓；超时或部分成交仍暂停，不恢复已暂停的程序。cost_verification_stage为已平仓后台费用核验的固定步骤分类；缺失或待核验不能视为实际费用为零。',
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
             **numbers(g,'opened_ms closed_ms qty lots contract entry entry_signal exit exit_signal entry_threshold grid_index retry_limit')}
        if g.get('reason'): row.update(safe_reason(g))
        if g.get('commission_reference'):
            row['commission_reference']=safe_references(g['commission_reference'])
        if g.get('commission_estimates'):
            row['commission_estimates']=numbers(g['commission_estimates'],'binance_open binance_close mt5_open mt5_close')
        if g.get('pair',{}).get('type') in ('mt5_mt5','exchange_mt5'):
            row['pair']={'type':g['pair']['type']}
            if g['pair']['type']=='exchange_mt5': row['pair']['exchange']=g['pair'].get('exchange','binance') if g['pair'].get('exchange','binance') in ('binance','lighter') else 'unknown'
            if g['pair']['type']=='mt5_mt5': row['direction']=g.get('direction','forward') if g.get('direction','forward') in ('forward','reverse') else 'unknown'
            if g['pair'].get('endpoints'):
                row['pair'].update(mode='terminals',endpoints={leg:terminal_alias(ref) for leg,ref in g['pair']['endpoints'].items() if leg in ('binance','mt5')})
        if g.get('quantity_unit')=='lot':
            row['quantity_unit']='lot';row['leg_contracts']=numbers(g.get('leg_contracts',{}),'binance mt5')
        row['parameters']=numbers(g.get('parameters',{}),'entry_spread_usd entry_lower_usd exit_spread_usd take_contraction_usd max_slippage_usd unwind_slippage_usd max_unhedged_ms max_quote_age_ms max_clock_skew_ms')
        row['parameters'].update(exit_parameters(g.get('parameters',{})))
        row['parameters'].update(numbers(g.get('parameters',{}),'entry_confirmation_decimals entry_forward_confirmation_ticks entry_reverse_confirmation_ticks'))
        for direction in ('forward','reverse'):
            field=f'entry_{direction}_confirmation_mode'
            if g.get('parameters',{}).get(field) in ('equal','at_or_better'):
                row['parameters'][field]=g['parameters'][field]
        if type(g.get('parameters',{}).get('entry_spread_guard_enabled')) is bool:
            row['parameters']['entry_spread_guard_enabled']=g['parameters']['entry_spread_guard_enabled']
        row['parameters'].update(numbers(g.get('parameters',{}),'entry_spread_slippage_usd'))
        row['parameters'].update(numbers(g.get('parameters',{}),'liquidity_max_usage_percent liquidity_buffer_usd'))
        if type(g.get('parameters',{}).get('liquidity_guard_enabled')) is bool:
            row['parameters']['liquidity_guard_enabled']=g['parameters']['liquidity_guard_enabled']
        for action in ('open','close'):
            if isinstance(g.get('liquidity_'+action),dict):row['liquidity_'+action]=liquidity_evidence(g['liquidity_'+action])
        if isinstance(g.get('entry_spread_guard'),dict):
            guard=g['entry_spread_guard']
            row['entry_spread_guard']=numbers(guard,'signal entry_threshold actual slippage limit time_ms')
            row['entry_spread_guard']['action']=choice(guard,'action',('pause_and_protective_close',))
        if g.get('parameters',{}).get('direction_mode') in ('auto','forward','reverse'):
            row['parameters']['direction_mode']=g['parameters']['direction_mode']
        row.update(numbers(g,'exit_rules_revision'))
        if isinstance(g.get('liquidation_recovery'),dict):
            row['liquidation_recovery']=numbers(g['liquidation_recovery'],'detected_ms armed_ms remaining_mt5 remaining_binance')
        if isinstance(g.get('close_attempts'),dict): row['close_attempts']=numbers(g['close_attempts'],'binance mt5')
        if isinstance(g.get('close_protection'),dict):
            row['close_protection']=numbers(g['close_protection'],'started_ms remaining_mt5 remaining_binance')
        if isinstance(g.get('close_halt'),dict):
            row['close_halt']={**numbers(g['close_halt'],'time_ms retry_limit remaining_mt5 remaining_binance'),
                               'leg':choice(g['close_halt'],'leg',('binance','mt5'))}
        row.update(numbers(g,'cost_check_ms'))
        if type(g.get('costs_verified')) is bool: row['costs_verified']=g['costs_verified']
        if g.get('cost_verification_stage') in ('binance_fees','mt5_fees','binance_funding','verified'):
            row['cost_verification_stage']=g['cost_verification_stage']
        row['costs']=numbers(g.get('costs',{}),'usdt_usd binance_rebate_percent mt5_rebate_percent mt5_short_rebate_percent mt5_short_commission_per_lot_side mt5_commission_per_lot_side')
        for key in ('mt5_fee_auto','mt5_short_fee_auto'):
            if type(g.get('costs',{}).get(key)) is bool: row['costs'][key]=g['costs'][key]
        row['valuation']=numbers(g.get('valuation',{}),'pnl_schema_version binance_pnl_usdt mt5_pnl_usd valuation_fx gross fees estimated_exit_fee net_before_rebate estimated_rebate estimated_rebate_binance estimated_rebate_mt5 net')
        if isinstance(g.get('exit_decision'),dict):
            decision=g['exit_decision']
            row['exit_decision']=numbers(decision,'signal_spread entry_spread contraction target_contraction target_spread min_net_profit estimated_net estimated_net_before_rebate estimated_gross estimated_exit_fee estimated_rebate valuation_fx basket_estimated_net')
            if type(decision.get('require_net_profit')) is bool: row['exit_decision']['require_net_profit']=decision['require_net_profit']
            if isinstance(decision.get('basket_members'),list):
                row['exit_decision']['basket_members']=[group_ids[x] for x in decision['basket_members'][:50] if x in group_ids]
            if decision.get('target_mode') in ('contraction','absolute'): row['exit_decision']['target_mode']=decision['target_mode']
            if isinstance(decision.get('confirmation'),dict):
                row['exit_decision']['confirmation']=numbers(decision['confirmation'],'count required')
                if type(decision['confirmation'].get('ready')) is bool:
                    row['exit_decision']['confirmation']['ready']=decision['confirmation']['ready']
        for action in ('open','close'):
            if 'execution_'+action in g:
                row['execution_'+action]=numbers(g['execution_'+action],'started_ms signal_time_ms signal_spread confirmed_ms elapsed_ms parallel_batches last_dispatch_gap_ms dispatch_gap_ms quote_to_execution_ms')
        if g.get('entry_dispatch') in ('serial','parallel'):row['entry_dispatch']=g['entry_dispatch']
        report['groups'].append(row)
    for o in orders:
        result=o.get('result',{})
        row={'id':order_ids[o['id']],'group':group_ids[o['group']],
             'leg':choice(o,'leg',('binance','mt5')),'action':choice(o,'action',('open','close')),
             'symbol':symbol(o.get('symbol')),'imported':bool(o.get('imported')),
             'source':'external' if o.get('source')=='external' else 'adopted' if o.get('imported') else 'program',
             **numbers(o,'requested reference limit slippage fx top_qty created_ms dispatch_ms intent_journal_ms dispatch_queue_ms execution_to_dispatch_ms dispatch_quote_age_ms dispatch_quote_alignment_ms signal_spread signal_time_ms quote_spread actual_spread spread_slippage adverse_slippage close_attempt close_batch')}
        if o.get('dispatch_mode') in ('serial','parallel'): row['dispatch_mode']=o['dispatch_mode']
        if isinstance(o.get('liquidity'),dict):row['liquidity']=liquidity_evidence(o['liquidity'])
        if o.get('platform')=='mt5': row['platform']='mt5'
        if isinstance(o.get('endpoint'),dict):row['endpoint']=terminal_alias(o['endpoint'])
        if o.get('quantity_unit')=='lot': row['quantity_unit']='lot'
        row.update(numbers(o,'contract_size native_position_side'))
        if o.get('slippage_mode') in ('normal','protective'): row['slippage_mode']=o['slippage_mode']
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
    kinds={'order_intent','order_result','order_reconciled','order_reconcile_retry','order_reconcile_exhausted','costs_verified','cost_verification_pending',
           'group_opening','group_opened','group_closed','exit_signal','exit_deferred','entry_skipped','close_attempt','entry_recovered','opening_fee_verified'}
    for event in events:
        data=event.get('data') or {}
        if event.get('kind')=='native_pair_connect':
            stages=('准备连接','初始化 MT5 执行会话','核验卖出品种规格','核验买入品种规格',
                    '读取账户与交易权限','初始化 MT5 行情会话','读取双品种首轮报价','启用行情与后台观察','双品种行情已连接')
            clean={**numbers(data,'started_ms elapsed_ms'),'state':choice(data,'state',('connecting','connected','failed'))}
            if data.get('stage') in stages: clean['stage']=data['stage']
            clean['timings']=[{'stage':row['stage'],**numbers(row,'elapsed_ms')}
                              for row in data.get('timings',[])[:10] if isinstance(row,dict) and row.get('stage') in stages]
            report['events'].append({'kind':event['kind'],**numbers(event,'time'),'data':clean})
            continue
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
        if event.get('kind')=='entry_spread_guard_triggered':
            gid=group_ids.get(data.get('group'))
            if gid:report['events'].append({'kind':event['kind'],**numbers(event,'time'),
                'data':dict(group=gid,**numbers(data,'signal entry_threshold actual slippage limit time_ms'),
                            **safe_reason(data))})
            continue
        if event.get('kind') not in kinds: continue
        oid=order_ids.get(data.get('id'));gid=group_ids.get(data.get('group'))
        if oid is None and gid is None: continue
        row={'kind':event['kind'],**numbers(event,'time'),
             'data':numbers(data,'attempt close_attempt close_batch query_ms query_timeout_ms signal_spread entry qty net_estimate fee entry_spread contraction target_contraction target_spread min_net_profit estimated_net estimated_net_before_rebate estimated_gross estimated_exit_fee estimated_rebate valuation_fx basket_estimated_net')}
        if data.get('dispatch_mode') in ('serial','parallel'): row['data']['dispatch_mode']=data['dispatch_mode']
        if data.get('leg') in ('binance','mt5'): row['data']['leg']=data['leg']
        if data.get('slippage_mode') in ('normal','protective'): row['data']['slippage_mode']=data['slippage_mode']
        if isinstance(data.get('result'),dict): row['data']['result']=result_evidence(data['result'])
        if data.get('reason'): row['data'].update(safe_reason(data))
        if oid: row['data']['order']=oid
        if gid: row['data']['group']=gid
        report['events'].append(row)
    report['entry_observations']=PROCESS_ENTRY_OBSERVATIONS.snapshot(terminal_alias)
    report['notes'].append('entry_observations为本次连接内实际策略评估的开仓证据，内存最多512段；每段记录对应方向的真实可成交价差、当前生效阈值、到价与阻拦原因、首末样本和方向极值。pool_rank表示候选筛选，engine表示执行条件检查，preflight表示下单前复查；ENTRY_SUBMIT仅表示进入开仓流程，不保证成交。反向仍统一使用A Ask−B Bid≤下限；P为本段内匿名配对，端点T与组内别名一致。连续报价确认的规则、当前计数及要求次数随评估样本导出；仅统计策略实际取用的新源报价，不代表所有平台 tick 均被处理。记录不写盘、不新增平台查询；重新连接/重启清空，图表触线但未被策略取用的瞬时行情不能还原。')
    return report


_CURRENT_TRANSPORT=None


def register_transport(stream):
    global _CURRENT_TRANSPORT
    _CURRENT_TRANSPORT=stream


def transport_snapshot(stream):
    """Only fixed transport labels; never export exception text or URLs."""
    if stream is None or not callable(getattr(stream,'status',None)):return {}
    sample=stream.status();result={}
    for key,allowed in [('state',('idle','connecting','subscribed','streaming','reconnecting','unavailable','closed')),
                        ('route',('proxy','direct'))]:
        if sample.get(key) in allowed:result[key]=sample[key]
    for key in ('reconnects','last_received_ms'):
        value=sample.get(key)
        if type(value) is int and value>=0:result[key]=value
    error=str(sample.get('error') or '')
    result['error_code']=('REGION_RESTRICTED' if '20558' in error else 'CLIENT_MISSING' if '未安装' in error else
        'TIMEOUT' if 'Timeout' in error or '超时' in error else 'CONNECTION_ERROR' if error else 'NONE')
    result['exchange']='lighter' if hasattr(stream,'market_id') else 'binance'
    return result


def export_report(path):
    from trading_store import storage_snapshot
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
    report['market_transport']=transport_snapshot(_CURRENT_TRANSPORT)
    report['connection']=dict(_CURRENT_CONNECTION)
    report['notes'].append('connection为本进程最近一次交易所/MT5连接尝试的请求模式、手数及完成后的实际模式和数量；失败可保留旧连接。runtime是数据库最后提交状态，两者含义不同；不含配置原文、凭据或错误原文，不增加平台查询。')
    report['notes'].append('market_transport为导出时当前连接的内存行情通道状态、重连次数及固定错误分类；不新增平台请求，不含原始错误、代理地址或凭据，不能还原过去每次断线。')
    report['storage']=storage_snapshot(path)
    report['notes'].append('storage为导出时数据所在磁盘的可用空间及数据库/WAL/SHM大小，单位字节；按需读取，不增加下单路径的磁盘检查，不含数据目录路径。')
    aliases={g['id']:'G'+str(i+1) for i,g in enumerate(state.get('groups',[])[-200:])}
    report['exit_observations']=PROCESS_EXIT_OBSERVATIONS.snapshot(aliases)
    report['notes'].append('exit_observations为本次连接内策略实际检查过的退出条件，最多512段；原因、规则或报价有效性改变及每30秒开启新段，first/latest为该段首末证据，samples累计检查次数，min_exit_spread/max_net为段内极值，极值未必同时出现。仅用限量内存，不增加平台请求或日志提交；重启/重新连接清空。报价无效、恢复等待等提前返回不重新估值，缺少net不能当作零。规则中revision对应当时锁定版本，整篮子按加权入场与篮子净收益检查。图表记录与策略取样独立；未被策略取用的瞬时行情、导出前旧版未采集的瞬间及已淘汰段无法还原。内存与持久化快照不是原子快照，未映射组仅统计数量。')
    report['latency']=PROCESS_LATENCY.snapshot()
    report['notes'].append('latency为当前进程内每阶段最近最多512个样本的中位数、P95、P99、最大值和最新值，重启或重新连接清空，mode区分纸面与实盘；P99少于100个样本时标记p99_low_sample，不可视为稳定结论。不增加平台查询或日志提交。strategy_cycle包含锁内整轮调度，strategy_lock_wait为调度线程取得共享锁的等待；strategy_pass为一次engine.tick，含执行与确认，decision为条件评估及估值。')
    report['notes'].append('quote_pickup为本进程发布双边报价至首次策略取用的单调时钟耗时；quote_wait及两边quote_age沿用本机接收墙钟计算，不能当作跨平台网络延迟。counters中quote_push/poll/unknown为策略首次取用的报价样本传输方式，非收包率；binance/mt5在双MT5模式分别代表A/B腿。')
    report['notes'].append('mt5_push_parse/accept/notify分别为Python读到完整一行后解析校验、缓存更新及唤醒通知耗时；不含操作系统或Socket缓冲区中读出前的排队。binance/mt5_push_to_publish及push_to_strategy为对应腿完整消息读出至组合发布及首次策略取用，复用未变化腿时包含其缓存停留；均使用Python单调时钟。mt5_ea_prepare/send由EA自己的单调时钟测量，随下一条报价报告上一次成功发送，send含编码及SocketSend调用；不是经纪商到终端或跨进程传输延迟，旧EA缺少时留空。')
    report['notes'].append('execution_to_dispatch_ms为本进程本次开/平仓流程开始至该腿调用适配器的单调耗时，execution_open.dispatch_gap_ms为两腿发送间隔；quote_to_execution_ms为触发报价发布至该流程开始的单调耗时。重启缺少计时起点则留空。receipt_handling_wait为收到回报到主线程处理前的等待；result_journal为原有回报落盘耗时。每笔初始回执及其timings可对照actual_spread/spread_slippage，不把后续核验替换后的回执当作初始请求。')
    report['notes'].append('dispatch_quote_age_ms为调用适配器时该腿参考报价的本机年龄，dispatch_quote_alignment_ms为双边参考报价的本机收到时间差；缺少 observed_ms 则留空。两者不使用平台原始时钟，不代表网络传输延迟或成交延迟。')
    report['notes'].append('MT5的terminal_roundtrip包含工作进程内部阶段；worker_submit包含preflight、order_check、order_send和fill_normalize；request还包含适配器处理。上述计时以及strategy_cycle/strategy_pass是包含关系，不能相加；order_send也不等于纯网络耗时。counters中的full_fill/partial_fill/zero_fill/unconfirmed仅统计直接回执，不表示后续对账的最终结果。')
    return report
