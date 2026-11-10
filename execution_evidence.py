"""Small allowlists shared by execution and its private-data-free export."""
import math


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
    'RISK_LIMIT': '策略亏损上限触发',
    'RECOVERED_PAUSED': '异常或延迟回报已确认，新开仓仍暂停',
    'PERMISSION_DENIED': '平台交易权限不可用',
    'MANUAL_CLOSE': '用户请求平仓，新开仓暂停',
    'OTHER': '其他原因；原始文本未导出',
}


def reason_code(reason):
    text=str(reason or '')
    if not text: return 'NONE'
    # Classify only into fixed labels. Never export arbitrary upstream text.
    rules=(
        ('已暂停新开仓；已有仓位继续管理', 'USER_PAUSE'),
        ('程序重启', 'RESTART'),
        ('开仓确认窗口已结束', 'ENTRY_WINDOW_EXPIRED'),
        ('超出敞口期限', 'ENTRY_WINDOW_EXPIRED'),
        ('第一腿成交后报价过期', 'ENTRY_WINDOW_EXPIRED'),
        ('开仓拒单或部分成交', 'FIRST_LEG_INCOMPLETE'),
        ('拒单、部分成交或状态未知', 'SECOND_LEG_INCOMPLETE'),
        ('第二腿前无法取得新报价', 'QUOTE_UNAVAILABLE'),
        ('行情或交易通道异常', 'CHANNEL_ERROR'),
        ('减仓重试达到上限', 'CLOSE_RETRIES_EXHAUSTED'),
        ('亏损上限', 'RISK_LIMIT'),
        ('自动交易权限已关闭', 'PERMISSION_DENIED'),
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
ERROR_CODES = {'NETWORK','TIMEOUT','INVALID_RESPONSE','DEPTH_INSUFFICIENT','DEPTH_CHECK_FAILED'}
ERROR_KINDS = {'timeout','network','exchange','invalid_response','local_validation','unknown'}


def result_evidence(result):
    if not isinstance(result,dict): return {'error_kind':'invalid_response'}
    data={}
    if type(result.get('valid')) is bool: data['valid']=result['valid']
    for key in ('qty','price','request_ms','response_received_ms','retcode','ack_volume','ack_price'):
        value=result.get(key)
        if isinstance(value,(int,float)) and not isinstance(value,bool) and math.isfinite(value): data[key]=value
    if result.get('status') in ('done','pending','unknown'): data['status']=result['status']
    if isinstance(result.get('raw_status'),str) and result['raw_status'] in PLATFORM_STATUSES: data['raw_status']=result['raw_status']
    code=result.get('error_code')
    if (isinstance(code,int) and not isinstance(code,bool)) or (isinstance(code,str) and code in ERROR_CODES): data['error_code']=code
    if isinstance(result.get('error_kind'),str) and result['error_kind'] in ERROR_KINDS: data['error_kind']=result['error_kind']
    elif result.get('error'): data['error_kind']='unknown'
    return data
