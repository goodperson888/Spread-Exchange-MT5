"""Small allowlists shared by execution and its private-data-free export."""
import math
import close_cause


REASON_LABELS = {
    'NONE': '无当前告警',
    'USER_PAUSE': '手动暂停新开仓',
    'ORDER_UNCONFIRMED': '订单成交待确认，暂停新开仓',
    'ENTRY_WINDOW_EXPIRED': '开仓确认超过单边敞口时限，保护性撤回',
    'FIRST_LEG_INCOMPLETE': '第一腿拒单或未完全成交，保护性撤回',
    'SECOND_LEG_INCOMPLETE': '第二腿拒单、部分成交或状态未知，保护性撤回',
    'QUOTE_UNAVAILABLE': '报价过期或不可用',
    'CHANNEL_ERROR': '行情或交易通道异常',
    'POSITION_MISMATCH': '平台持仓与程序记录不一致',
    'RECONCILE_REQUIRED': '须核对持仓后手动恢复',
    'RESTART': '程序重启，自动开仓保持暂停',
    'CLOSE_RETRIES_EXHAUSTED': '平仓重试达到上限，仍须处理敞口',
    'CLOSE_RESIDUAL': '单边平仓未完成，继续保护性减仓，新开仓暂停',
    'CLOSE_DISPATCH_FAILED': '并发平仓调度未启动，请检查后重试平仓',
    'RISK_LIMIT': '策略亏损上限触发',
    'RECOVERED_PAUSED': '异常或延迟回报已确认，新开仓仍暂停',
    'PERMISSION_DENIED': '平台交易权限不可用',
    'ENTRY_MARGIN': '新开仓保证金不足或未能核验；已有仓位仍可管理',
    'MANUAL_CLOSE': '用户请求平仓，新开仓暂停',
    'TARGET_REACHED': '价差达到止盈条件且通过配置的净收益检查',
    'TARGET_DEFERRED': '下单前止盈条件已不满足，继续持仓等待',
    'HOLD_TIMEOUT': '达到最长持仓时间',
    'LIQUIDATION': '平台确认强平，保护性平仓剩余敞口',
    'OTHER': '其他原因；原始文本未导出',
}


def reason_code(reason):
    text=str(reason or '')
    if not text: return 'NONE'
    # Classify only into fixed labels. Never export arbitrary upstream text.
    rules=(
        ('已暂停新开仓；已有仓位继续管理', 'USER_PAUSE'),
        ('平台确认强平', 'LIQUIDATION'),
        ('程序重启', 'RESTART'),
        ('开仓确认窗口已结束', 'ENTRY_WINDOW_EXPIRED'),
        ('超出敞口期限', 'ENTRY_WINDOW_EXPIRED'),
        ('第一腿成交后报价过期', 'ENTRY_WINDOW_EXPIRED'),
        ('开仓拒单或部分成交', 'FIRST_LEG_INCOMPLETE'),
        ('拒单、部分成交或状态未知', 'SECOND_LEG_INCOMPLETE'),
        ('第二腿前无法取得新报价', 'QUOTE_UNAVAILABLE'),
        ('行情或交易通道异常', 'CHANNEL_ERROR'),
        ('减仓重试达到上限', 'CLOSE_RETRIES_EXHAUSTED'),
        ('单边平仓未完成', 'CLOSE_RESIDUAL'),
        ('并发平仓调度失败', 'CLOSE_DISPATCH_FAILED'),
        ('亏损上限', 'RISK_LIMIT'),
        ('逐组止盈', 'TARGET_REACHED'),
        ('止盈报价已变化', 'TARGET_DEFERRED'),
        ('整篮子止盈', 'TARGET_REACHED'),
        ('接管篮子收窄并盈利', 'TARGET_REACHED'),
        ('持仓超时', 'HOLD_TIMEOUT'),
        ('自动交易权限已关闭', 'PERMISSION_DENIED'),
        ('新开仓保证金', 'ENTRY_MARGIN'),
        ('外部平仓记录已回补', 'RECOVERED_PAUSED'),
        ('新开仓仍暂停', 'RECOVERED_PAUSED'),
        ('持仓变化', 'POSITION_MISMATCH'),
        ('持仓与本策略日志不一致', 'POSITION_MISMATCH'),
        ('持仓数量与日志一致', 'RECONCILE_REQUIRED'),
        ('持仓对账', 'RECONCILE_REQUIRED'),
        ('用户平仓', 'MANUAL_CLOSE'),
        ('用户请求全部平仓', 'MANUAL_CLOSE'),
        ('状态未知', 'ORDER_UNCONFIRMED'),
        ('成交状态未确定', 'ORDER_UNCONFIRMED'),
        ('报价过期', 'QUOTE_UNAVAILABLE'),
    )
    return next((code for needle,code in rules if needle in text),'OTHER')


PLATFORM_STATUSES = {'NEW','PARTIALLY_FILLED','FILLED','CANCELED','PENDING_CANCEL',
                     'REJECTED','EXPIRED','EXPIRED_IN_MATCH'}
ERROR_CODES = {'NETWORK','TIMEOUT','INVALID_RESPONSE','DEPTH_INSUFFICIENT','DEPTH_CHECK_FAILED',
               'DISPATCH_NOT_STARTED','STALE_EXECUTION_QUOTE'}
ERROR_KINDS = {'timeout','network','exchange','invalid_response','local_validation','unknown'}
MT5_STAGES = {'positions_unavailable','identity_unresolved','deals_missing','pending_unavailable',
              'order_pending','order_history_missing','order_identity_mismatch','order_not_terminal',
              'fill_volume_invalid','fill_volume_mismatch','deals_ambiguous','deal_fields_invalid',
              'confirmed','query_exception'}
MT5_READS = {'deals_ticket','deals_position','deals_range','pending_ticket','orders_ticket','orders_range'}

# Capture only fixed fields from an already received reply. Never retain the
# arbitrary payload, native request echo, broker comment, or platform IDs.
RECEIPT_NUMBERS = {
    'binance': ('executedQty','avgPrice','cumQuote','origQty','price','stopPrice','time','updateTime'),
    'mt5': ('retcode','retcode_external','volume','price','bid','ask'),
}
RECEIPT_ENUMS = {
    'status': PLATFORM_STATUSES,
    'side': {'BUY','SELL'}, 'positionSide': {'BOTH','LONG','SHORT'},
    'type': {'LIMIT','MARKET','STOP','STOP_MARKET','TAKE_PROFIT','TAKE_PROFIT_MARKET','TRAILING_STOP_MARKET'},
    'timeInForce': {'GTC','IOC','FOK','GTX','GTD'},
}
RECEIPT_BOOLS = ('reduceOnly','closePosition','priceProtect')
RECEIPT_IDS = {'binance': ('orderId','clientOrderId'), 'mt5': ('order','deal','request_id')}
FIELD_TYPES = {'missing','null','boolean','number','string','object','array','other'}


def field_type(value):
    if value is None: return 'null'
    if isinstance(value,bool): return 'boolean'
    if isinstance(value,(int,float)): return 'number'
    if isinstance(value,str): return 'string'
    if isinstance(value,dict): return 'object'
    if isinstance(value,(list,tuple)): return 'array'
    return 'other'


def finite_number(value):
    if isinstance(value,bool) or not isinstance(value,(int,float,str)): return None
    if isinstance(value,str) and len(value)>80: return None
    try:
        n=float(value)
        return n if math.isfinite(n) else None
    except (ValueError,TypeError,OverflowError): return None


def receipt_evidence(value):
    """Re-apply the allowlist at export, including to persisted nested data."""
    if not isinstance(value,dict) or value.get('platform') not in ('binance','mt5'): return {}
    platform=value['platform']; data={'platform':platform}
    if isinstance(value.get('payload_type'),str) and value['payload_type'] in FIELD_TYPES: data['payload_type']=value['payload_type']
    keys=RECEIPT_NUMBERS[platform]+(tuple(RECEIPT_ENUMS)+RECEIPT_BOOLS if platform=='binance' else ())
    shapes=value.get('field_types'); values=value.get('values')
    if isinstance(shapes,dict):
        data['field_types']={k:shapes[k] for k in keys if isinstance(shapes.get(k),str) and shapes[k] in FIELD_TYPES}
    if isinstance(values,dict):
        clean={}
        for k in RECEIPT_NUMBERS[platform]:
            n=values.get(k)
            if isinstance(n,(int,float)) and not isinstance(n,bool) and math.isfinite(n): clean[k]=n
        if platform=='binance':
            for k,allowed in RECEIPT_ENUMS.items():
                if isinstance(values.get(k),str) and values[k] in allowed: clean[k]=values[k]
            for k in RECEIPT_BOOLS:
                if type(values.get(k)) is bool: clean[k]=values[k]
        data['values']=clean
    for k in RECEIPT_IDS[platform]:
        if type(value.get('has_'+k)) is bool: data['has_'+k]=value['has_'+k]
    if value.get('price_source') in ('avgPrice','cumQuote','unavailable'): data['price_source']=value['price_source']
    return data


def capture_receipt(platform, payload):
    """Small bounded snapshot; no I/O and no recursive payload serialization."""
    if platform not in RECEIPT_NUMBERS: return {}
    if platform=='mt5' and payload is not None and not isinstance(payload,dict):
        keys=RECEIPT_NUMBERS[platform]+RECEIPT_IDS[platform]
        payload={k:getattr(payload,k) for k in keys if hasattr(payload,k)}
    data={'platform':platform,'payload_type':field_type(payload),'field_types':{},'values':{}}
    if not isinstance(payload,dict): return data
    keys=RECEIPT_NUMBERS[platform]+(tuple(RECEIPT_ENUMS)+RECEIPT_BOOLS if platform=='binance' else ())
    for k in keys:
        data['field_types'][k]=field_type(payload[k]) if k in payload else 'missing'
        if k in RECEIPT_NUMBERS[platform]:
            n=finite_number(payload.get(k))
            if n is not None: data['values'][k]=n
        elif k in RECEIPT_ENUMS:
            v=payload.get(k)
            if isinstance(v,str) and v in RECEIPT_ENUMS[k]: data['values'][k]=v
        elif type(payload.get(k)) is bool: data['values'][k]=payload[k]
    for k in RECEIPT_IDS[platform]:
        if k in payload: data['has_'+k]=payload[k] not in (None,'',0,'0')
    return data


VALIDATION_ISSUES = ('not_object','invalid_status','invalid_quantity','quantity_exceeds_request',
                     'invalid_price','nonpositive_fill_price','quantity_regressed','identity_mismatch')


def validation_issues(result, requested):
    """Explain the existing fill-shape check without changing its decisions."""
    if not isinstance(result,dict): return ['not_object']
    issues=[]
    if result.get('status') not in ('done','pending','unknown'): issues.append('invalid_status')
    qty=finite_number(result.get('qty',0));price=finite_number(result.get('price',0))
    if qty is None or qty<0: issues.append('invalid_quantity')
    elif qty>requested+1e-7: issues.append('quantity_exceeds_request')
    if price is None: issues.append('invalid_price')
    elif qty is not None and qty!=0 and price<=0: issues.append('nonpositive_fill_price')
    return issues


def confirmation_evidence(value):
    if not isinstance(value,dict): return {}
    data={}
    if isinstance(value.get('stage'),str) and value['stage'] in MT5_STAGES: data['stage']=value['stage']
    for key in ('position_matches','deal_matches','order_matches','order_state','volume_initial',
                'volume_current','deal_lots','filled_lots','candidate_rows','ticket_matches',
                'symbol_matches','magic_matches','side_matches','entry_matches'):
        n=value.get(key)
        if isinstance(n,(int,float)) and not isinstance(n,bool) and math.isfinite(n): data[key]=n
    reads=[]
    for row in value.get('reads',[])[:7] if isinstance(value.get('reads'),list) else []:
        if not isinstance(row,dict) or not isinstance(row.get('api'),str) or row['api'] not in MT5_READS: continue
        if row.get('status') not in ('ok','empty','unavailable','error'): continue
        clean={'api':row['api'],'status':row['status']}
        for key in ('rows','error_code'):
            if isinstance(row.get(key),int) and not isinstance(row[key],bool): clean[key]=row[key]
        reads.append(clean)
    if reads: data['reads']=reads
    return data


def result_evidence(result):
    if not isinstance(result,dict): return {'error_kind':'invalid_response'}
    data={}
    cause=close_cause.evidence(result.get('close_cause'))
    if cause: data['close_cause']=cause
    if type(result.get('valid')) is bool: data['valid']=result['valid']
    if type(result.get('fee_verified')) is bool: data['fee_verified']=result['fee_verified']
    for key in ('ticket','deal','position','position_identifier'):
        if key in result: data['has_'+key]=result[key] not in (None,'',0,'0')
        elif type(result.get('has_'+key)) is bool: data['has_'+key]=result['has_'+key]
    for key in ('qty','price','request_ms','response_received_ms','ack_received_ms','retcode','ack_volume','ack_price','http_status','native_error_code'):
        value=result.get(key)
        if isinstance(value,(int,float)) and not isinstance(value,bool) and math.isfinite(value): data[key]=value
    if result.get('status') in ('done','pending','unknown'): data['status']=result['status']
    if isinstance(result.get('raw_status'),str) and result['raw_status'] in PLATFORM_STATUSES: data['raw_status']=result['raw_status']
    code=result.get('error_code')
    if (isinstance(code,int) and not isinstance(code,bool)) or (isinstance(code,str) and code in ERROR_CODES): data['error_code']=code
    if isinstance(result.get('error_kind'),str) and result['error_kind'] in ERROR_KINDS: data['error_kind']=result['error_kind']
    elif result.get('error'): data['error_kind']='unknown'
    confirmation=confirmation_evidence(result.get('confirmation'))
    if confirmation: data['confirmation']=confirmation
    if type(result.get('submitted')) is bool: data['submitted']=result['submitted']
    for key in ('receipt','precheck_receipt'):
        receipt=receipt_evidence(result.get(key))
        if receipt: data[key]=receipt
    if isinstance(result.get('validation_issues'),list):
        data['validation_issues']=[key for key in VALIDATION_ISSUES if key in result['validation_issues']]
    return data
