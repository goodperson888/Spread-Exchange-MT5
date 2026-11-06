"""Local-only XAU perpetual × MT5 paired-trading application.

The default is paper mode; live execution is deliberately armed only by the
user after connection and reconciliation checks pass.
"""
from __future__ import annotations

import copy
import json
import math
import hashlib
import os
import sys
import tempfile
import secrets
import socket
from decimal import Decimal
import threading
import time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse
from mt5_connector import run_probe
from mt5_mcp import DEFAULT_URL as DEFAULT_MCP_URL, McpTerminal, inspect_mcp_terminal, validate_mcp_url
from mt5_paper import inspect_paper_terminal
from paper_engine import default_state, step as paper_step
from trading_brokers import Binance, BinanceBookTicker, Terminal, LiveBroker, PaperBroker
from trading_config import validate as validate_trading, plan as executable_plan, pair_key
from trading_engine import Engine
from trading_store import Store
from execution_diagnostics import export_report
import position_adoption
import exit_rules
import external_closes
from market_push import PushBridge, QuoteEvents, QuotePump, TickFreshness, observed_quote
from entry_preflight import EntryPreflight
from liquidation_risk import BinanceRiskMonitor, risk_snapshot

ROOT = Path(__file__).resolve().parent
DATA = (Path(os.environ.get('LOCALAPPDATA', str(Path.home()))) / 'GoldPairLocal') if getattr(sys, 'frozen', False) else ROOT / 'data'
DATA = Path(os.environ.get('GOLD_PAIR_DATA_DIR', str(DATA)))
DATA.mkdir(parents=True, exist_ok=True)
CONFIG_PATH = DATA / "config.json"
STATE_PATH = DATA / "state.json"
EVENTS_PATH = DATA / "events.jsonl"
TRADING_DB_PATH = DATA / "trading.sqlite3"
HOST = "127.0.0.1"
PORT = int(os.environ.get("GOLD_PAIR_PORT", "8766"))
LOCK = threading.RLock()
PROBE_LOCK = threading.Lock()
LAST_PROBE = None
DIAGNOSTIC_EXPORT_LOCK = threading.Lock()


def atomic_write_json(path, value):
    fd, name = tempfile.mkstemp(dir=path.parent, prefix=path.stem + '-', suffix='.tmp')
    try:
        with os.fdopen(fd, 'w', encoding='utf-8') as handle:
            json.dump(value, handle, ensure_ascii=False, indent=2, allow_nan=False)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(name, path)
    finally:
        if os.path.exists(name):
            os.unlink(name)


def stop_legacy_paper_on_boot(path=STATE_PATH):
    """The legacy paper engine advances only on a button click, not in background.

    Preserve its simulated positions for inspection, but never carry a stale
    "running" label across application restarts.
    """
    state = read_json(path, default_state())
    state["running"] = False
    state["updated_ms"] = now_ms()
    atomic_write_json(path, state)
    return state


class TradingRuntime:
    """The only component allowed to poll or submit paired trades.

    Credentials are loaded from the local config only when needed. They are
    never copied into SQLite events or trading records.
    """
    def __init__(self):
        self.lock = threading.RLock()
        self.store = Store(TRADING_DB_PATH)
        self.engine = Engine(self.store, PaperBroker())
        self.binance = None
        self.market_stream = None
        self.terminal = None
        self.spec = None
        self.config = None
        self.plan = None
        self.quote = None
        self.connected = False
        self.reconciled = False
        self.last_error = ''
        self.market_meta = {}
        self.quote_events = QuoteEvents()
        self.mt5_freshness = TickFreshness()
        self.market_wake = threading.Event()
        self.pump = None
        self.push_bridge = None
        self._last_mt5_snapshot = None
        self._last_mt5_snapshot_at = 0
        self._mt5_risk_error = ''
        self.position_report = None
        self.adoption_preview = None
        self.engine.before_import_close = self._guard_import_close
        self._last_fx_refresh = 0
        self._last_carry_refresh = 0
        self._last_auto_reconcile_at = 0
        self._last_rest_market = None
        self._last_rest_market_at = 0
        self.entry_preflight = None
        self.risk_monitor = None
        self.external_monitor = None
        self.external_close_status = None
        self.stop_event = threading.Event()
        self.thread = threading.Thread(target=self._loop, name='gold-pair-market', daemon=True)
        self.thread.start()

    def close(self):
        self.stop_event.set()
        self.market_wake.set()
        if self.external_monitor: self.external_monitor.close()
        if self.pump: self.pump.close()
        if self.push_bridge: self.push_bridge.close()
        if self.terminal:
            self.terminal.close()
        if self.market_stream:
            self.market_stream.close()
        if self.entry_preflight:
            self.entry_preflight.close()
        if self.risk_monitor:
            self.risk_monitor.close()
        self.store.close()

    def connect(self, config, api_key='', api_secret='', mt5_mcp_token=''):
        config = copy.deepcopy(config)
        validate_trading(config)
        mode = config['execution']['mode']
        
        # 只支持两种模式：paper（纸上交易）和live（实盘交易）
        if mode not in ('paper', 'live'):
            raise ValueError('只支持纸上交易(paper)和实盘交易(live)两种模式')
        
        # 实盘模式需要API密钥，纸面模式使用正式环境行情但不需要密钥
        if mode == 'live' and (not api_key or not api_secret):
            raise ValueError('实盘连接需要币安 API Key 和 Secret Key；请先保存本机连接配置')
        
        with self.lock:
            if self.engine.state.get('enabled'):
                raise ValueError('自动开仓运行中，请先暂停新开仓，再保存并重新连接；已有仓位继续按原规则管理')
            new_key = pair_key(config)
            key_fingerprint=hashlib.sha256(api_key.encode()).hexdigest()
            if any(g.get('imported') and g.get('binance_key_fingerprint')!=key_fingerprint for g in self.engine.active()):
                raise ValueError('接管仓位绑定的币安 API Key 已变化；请使用接管时的连接配置')
            if any(g.get('key') != new_key for g in self.engine.active()):
                raise ValueError('存在其他账户或品种的未平交易组，禁止切换连接')

            # Build and validate a replacement connection before touching the
            # current one. A failed reconnect must not leave a half-live runtime.
            new_binance = Binance(production=True, key=api_key, secret=api_secret,
                                   recv_window_ms=config['binance']['recv_window_ms'],
                                   proxy_url=config['binance'].get('proxy_url', ''))
            new_terminal = None
            new_stream = None
            new_preflight = None
            new_risk_monitor = None
            mt5_snapshot_started = None
            meta = {'mt5_transport':'工作进程持久会话 + tick 轮询'}
            try:
                new_spec = new_binance.spec(config['symbol'])
                if config['costs']['usdt_usd_auto']:
                    fx = new_binance.usdt_usd()
                    config['costs']['usdt_usd'] = fx['value']
                    meta['fx'] = fx
                if config['mt5']['adapter'] == 'paper':
                    mt5 = inspect_paper_terminal(config['mt5'])
                    if not mt5.get('connected'):
                        raise ValueError(mt5.get('message', '本地 MT5 模拟器不可用'))
                    snapshot = {'quote': self.mt5_freshness.observe(mt5['quote'],config['mt5']),
                                'spec': mt5['symbol'], 'allowed': True, 'positions': []}
                elif config['mt5']['adapter'] == 'mcp':
                    if mode != 'paper':
                        raise ValueError('MT5 MCP 只读适配器只能用于纸面模式')
                    checked_spec(config['mt5'], require_applied=True)
                    new_terminal = McpTerminal(config['mt5'], mt5_mcp_token)
                    snapshot = new_terminal.call('snapshot')
                    snapshot['quote'] = self.mt5_freshness.observe(snapshot['quote'],config['mt5'])
                    meta['mt5_transport'] = 'MT5 MCP 真实行情（只读轮询）'
                else:
                    checked_spec(config['mt5'], require_applied=True)
                    new_terminal = Terminal(config['mt5'], config['execution']['magic'])
                    mt5_snapshot_started = time.monotonic()
                    snapshot = new_terminal.call('snapshot')
                    snapshot['quote'] = self.mt5_freshness.observe(snapshot['quote'],config['mt5'])
                    if not snapshot['allowed']:
                        raise ValueError('MT5 未允许程序交易；请按页面提示完成权限检查')
                    if snapshot['spec']['name'] != config['mt5']['symbol']:
                        raise ValueError('MT5 当前品种与已应用配置不一致')
                    if mode == 'live':
                        if str(snapshot['account'].get('currency','')).upper()!='USD':
                            raise ValueError('当前 MT5 账户币种不是 USD，本版无法准确换算佣金和持仓费')
                        if int(snapshot['account'].get('margin_mode', -1)) != 2:
                            raise ValueError('当前 MT5 账户不是对冲模式；本版按逐组持仓管理，只允许 MT5 对冲账户实盘')
                        try:
                            permissions = new_binance.api_permissions()
                        except Exception as exc:
                            permissions = None
                            meta['binance_permission_warning'] = str(exc)
                        new_binance.preflight(permissions)
                        if config['costs']['binance_fee_auto']:
                            fees = new_binance.commission_rate(config['symbol'])
                            config['costs']['binance_taker_percent'] = fees['taker']
                            meta['binance_fee'] = {**fees, 'source':'Binance account commission rate'}
                        # Check the configured worst-case MT5 exposure, not just
                        # the first group.  This covers repeated entries and
                        # grid additions before any live order is sent.
                        max_live_lots = float(config['strategy']['max_total_lots'])
                        margin = new_terminal.call('margin', lots=max_live_lots)
                        meta['mt5_margin_check'] = {
                            'lots': max_live_lots,
                            'required': float(margin['required']),
                            'available': float(margin['available']),
                        }
                        if margin['required'] > margin['available']:
                            raise ValueError(f'按最大总持仓 {max_live_lots:g} 手核算，MT5 可用保证金不足，不能启动')

                new_stream = BinanceBookTicker(config['symbol'], production=True,
                                               proxy_url=config['binance'].get('proxy_url', '')).start()
                market = new_stream.quote(max_age_ms=config['strategy']['max_quote_age_ms'],wait_ms=2500)
                if market is None:
                    market = new_binance.quote(config['symbol'])
                    meta['binance_transport'] = 'REST 回退（WebSocket 正在重连）'
                else:
                    meta['binance_transport'] = 'WebSocket bookTicker'
                new_plan = executable_plan(config, snapshot['spec'], new_spec, market['bid'])
                new_quote = self._quote(snapshot['quote'], market, config)
                broker = PaperBroker() if mode == 'paper' else LiveBroker(config, new_binance, new_terminal, new_spec)
                if mode == 'live':
                    new_preflight = EntryPreflight(new_binance, new_binance, new_spec)
                    risk_client = Binance(production=True, key=api_key, secret=api_secret,
                                          recv_window_ms=config['binance']['recv_window_ms'],
                                          proxy_url=config['binance'].get('proxy_url', ''))
                    new_risk_monitor = BinanceRiskMonitor(risk_client, config['symbol'])
            except Exception:
                if new_stream:
                    new_stream.close()
                if new_terminal:
                    new_terminal.close()
                if new_preflight:
                    new_preflight.close()
                if new_risk_monitor:
                    new_risk_monitor.close()
                raise

            if self.pump: self.pump.close()
            old_terminal = self.terminal
            old_stream = self.market_stream
            old_preflight = self.entry_preflight
            old_risk_monitor = getattr(self, 'risk_monitor', None)
            old_external_monitor = getattr(self, 'external_monitor', None)
            self.binance, self.market_stream, self.terminal, self.spec = new_binance, new_stream, new_terminal, new_spec
            self.entry_preflight = new_preflight
            self.risk_monitor = new_risk_monitor
            self.external_monitor = None
            self.external_close_status = None
            self.plan, self.config, self.engine.broker = new_plan, config, broker
            self.engine.quote_provider = self._execution_quote
            self.engine.entry_preflight = self._entry_preflight_guard if mode == 'live' else None
            self._last_mt5_snapshot = snapshot if config['mt5']['adapter']=='native' else None
            self._last_mt5_snapshot_at = mt5_snapshot_started
            self._mt5_risk_error = ''
            self.quote, self.connected, self.last_error = new_quote, True, ''
            self.market_meta = meta
            self.position_report = None
            self.adoption_preview = None
            for g in self.engine.active():
                if g.get('imported'): g['management_enabled']=False
            self._last_fx_refresh = time.monotonic()
            self._last_carry_refresh = 0
            self._last_rest_market = market
            self._last_rest_market_at = time.monotonic()
            self.reconciled = mode == 'paper'
            if mode == 'live':
                self.engine.pause('实盘连接已建立，启动前必须完成持仓对账')
            self.store.sample(self.quote)
            self.quote_events.publish(self.quote)
            # Intake persists/pushes quotes without taking the order execution lock.
            def publish(q):
                self.store.sample(q)
                self.quote_events.publish(q)
            if self.push_bridge is None and (DATA / 'quote-push.json').exists():
                try: self._start_push_bridge()
                except OSError as exc: self.market_meta['push_warning']='EA 接收器未启动：'+str(exc)
            self.pump = QuotePump(config, new_binance, new_stream, new_terminal, self.push_bridge,
                                  inspect_paper_terminal, publish, self.market_wake, self.mt5_freshness)
            self.pump.last_stamp = self.quote['time_ms']
            self.pump.start()
            if old_terminal and old_terminal is not new_terminal:
                old_terminal.close()
            if old_stream and old_stream is not new_stream:
                old_stream.close()
            if old_preflight and old_preflight is not new_preflight:
                old_preflight.close()
            if old_risk_monitor and old_risk_monitor is not new_risk_monitor:
                old_risk_monitor.close()
            if old_external_monitor: old_external_monitor.close()
            if mode=='live':
                observer = Binance(production=True,key=api_key,secret=api_secret,
                                   recv_window_ms=config['binance']['recv_window_ms'],
                                   proxy_url=config['binance'].get('proxy_url',''))
                self.external_monitor = external_closes.ExternalCloseMonitor(
                    self,observer,lambda: Terminal(config['mt5'],config['execution']['magic'],read_only=True),start=False)
                self.external_monitor.start()
            return self.snapshot()

    def _external_capture(self, monitor):
        # A background observer never waits for paired execution or borrows its
        # terminal/client. Recovery groups also need read-only history repair.
        if not self.lock.acquire(blocking=False): return None
        try:
            if (self.stop_event.is_set() or monitor.stopped.is_set() or monitor is not getattr(self,'external_monitor',None)
                    or not self.connected or self.config['execution']['mode']!='live'
                    or self.config['mt5']['adapter']!='native'): return None
            active = self.engine.active()
            if not active or any(g.get('imported') or g.get('key')!=pair_key(self.config)
                    or g.get('status') not in ('open','attention','opening','closing','unwinding') for g in active):
                return None
            state = copy.deepcopy(external_closes.observation_state(self.engine.state))
            return state,copy.deepcopy(self.config),external_closes.fingerprint(state)
        finally: self.lock.release()

    def _external_publish(self, monitor, token, phase, proposals, errors, mt5_snapshot=None, history_probe=None):
        if not self.lock.acquire(blocking=False): return False
        try:
            if (self.stop_event.is_set() or monitor.stopped.is_set() or not self.connected
                    or monitor is not getattr(self,'external_monitor',None)
                    or token!=external_closes.fingerprint(self.engine.state)): return False
            active = self.engine.active(); at = now_ms()
            waiting = any(g.get('external_attention') for g in active)
            probe_changed = history_probe is not None and history_probe != self.engine.state.get('mt5_history_probe')
            if probe_changed:
                self.engine.state['mt5_history_probe'] = copy.deepcopy(history_probe)
            if phase=='resolved':
                before=copy.deepcopy(self.engine.state)
                changed=[]
                try:
                    for proposal in proposals:
                        order=next((o for o in self.engine.state['orders'] if o['id']==proposal['id']),None)
                        result=proposal.get('result',{})
                        if (not order or order['result'].get('status')=='done'
                                or not self.engine.result_valid(result,order['requested']) or result.get('status')!='done'
                                or result['qty']<order['result'].get('qty',0)): continue
                        order['result']={**order['result'],**result,'reconcile_time_ms':at}
                        order['result'].pop('error',None)
                        changed.append(order['id'])
                    self.engine._entry_deadlines.clear()
                    self.engine.state.update(enabled=False,recovery=True,
                        alarm='正在核对原订单与平台平仓成交；完成前禁止自动处理')
                    self.reconciled=False
                    for group in active:group['external_attention']=True
                    if changed:self.engine.save('external_orders_resolved',{'groups':[g['id'] for g in active],'orders':changed})
                except Exception:
                    self.engine.state=before
                    self.engine.state.update(enabled=False,recovery=True)
                    raise
                self.external_close_status=dict(time_ms=at,level='warning',message=(
                    '已确认原订单成交，下一轮核对平台平仓历史；新开仓保持暂停' if changed else
                    '原订单成交仍待平台确认；不会按空仓推算成交，请稍后重试或导出执行诊断'))
                return True
            if phase=='checked':
                if active and all(not self.engine.uncertain(g) and max(self.engine.amounts(g).values())<1e-8 for g in active):
                    before=copy.deepcopy(self.engine.state)
                    try:
                        for group in active:
                            group.update(status='closed',closed_ms=at,reason='原订单成交已核对，已确认双边空仓')
                            group['exit']=self.engine._record_spread(group,'close')
                            group.pop('external_attention',None)
                            self.engine._complete_execution(group,'close')
                            group['valuation']=self.engine.valuation(group,self.quote or dict(binance=dict(ask=0),mt5=dict(bid=0),usdt_usd=group['costs']['usdt_usd']))
                        self.engine.state.update(enabled=False,recovery=False,alarm='已确认旧组双边平仓；可手动启动自动开仓')
                        self.engine.save('recovery_flat_confirmed',{'groups':[g['id'] for g in active]})
                        self.reconciled=True
                    except Exception:
                        self.engine.state=before;self.reconciled=False
                        raise
                    self.external_close_status=dict(time_ms=at,level='normal',message='已确认旧组双边平仓；新开仓保持暂停，可手动启动')
                    return True
                if waiting:
                    message = ('已回补可确认成交；仍有剩余仓位，请核对后处理；不会自动重新开仓'
                               if any(g.get('external_close') for g in active) else
                               '持仓数量与日志一致；上次异常仍待点击持仓对账确认，交易保持暂停')
                else: message = '后台每约 5 秒核对程序持仓数量；平台手动平仓查到成交后回补记录'
                self.external_close_status = dict(time_ms=at,level='warning' if waiting else 'normal',message=message)
                return True
            if phase=='unavailable':
                self.external_close_status = dict(time_ms=at,level='unknown',message='后台持仓核对暂不可用；请检查连接或点击持仓对账')
                return True
            if phase in ('detected','blocked'):
                reason = '发现平台持仓变化，已暂停交易并核对真实成交；不会自动重新开仓'
                if phase=='blocked': reason = '平台委托或持仓归属异常，已暂停交易；请点击持仓对账'
                changed = not waiting or self.engine.state.get('enabled')
                self.engine._entry_deadlines.clear()
                self.engine.state.update(enabled=False,recovery=True,alarm=reason)
                self.reconciled = False
                for g in active: g['external_attention'] = True
                self.external_close_status = dict(time_ms=at,level='warning',message=reason)
                if changed: self.engine.save('external_position_changed',{'groups':[g['id'] for g in active]})
                return True
            if phase=='pending':
                message='；'.join(errors)
                if probe_changed: message += '；已记录 MT5 历史字段探针，请重新导出执行诊断'
                self.external_close_status = dict(time_ms=at,level='warning',message=message)
                if probe_changed:
                    self.engine.save('mt5_history_probe', {'row_count': history_probe.get('row_count',0),
                                                           'query_count': len(history_probe.get('queries',[]))})
                return True
            if phase!='backfill': return False
            before = copy.deepcopy(self.engine.state)
            previous_status = self.external_close_status
            known = {o['id'] for o in self.engine.state['orders']}
            new = [copy.deepcopy(o) for o in proposals if o['id'] not in known]
            touched = {o['group'] for o in new}
            self.engine.state['orders'].extend(new)
            try:
                for order in new:
                    identity=order.pop('_opening_identity',None)
                    if identity:
                        opening=next(o for o in self.engine.state['orders'] if o['id']==identity['id'] and o['group']==order['group'] and o['leg']=='mt5' and o['action']=='open')
                        opening['result']['position_identifier']=identity['position_identifier']
                        if identity.get('position'):opening['result']['position']=identity['position']
                for g in active:
                    if g['id'] not in touched: continue
                    g['external_attention'] = True
                    g['external_close'] = True
                    g['costs_verified'] = False
                    own = [o for o in self.engine.state['orders'] if o['group']==g['id']]
                    realized_swap = sum(o['result'].get('swap',0) for o in own if o.get('source')=='external' and o['leg']=='mt5')
                    if mt5_snapshot is not None:
                        tickets = self.engine.mt5_tickets(g)
                        g['live_mt5_swap_usd'] = realized_swap+sum(float(p.get('swap',0)) for p in mt5_snapshot['positions'] if str(p['ticket']) in tickets)
                    remaining = self.engine.amounts(g)
                    if max(remaining.values())<1e-8:
                        g.update(status='closed',reason='平台外部平仓，已按真实成交回补记录',
                                 closed_ms=at)
                        actual = self.engine._record_spread(g,'close')
                        if actual is not None: g['exit'] = actual
                        g.pop('external_attention',None)
                    else:
                        g.update(status='attention',reason='平台外部减仓已回补；仍有剩余仓位，请持仓对账后处理')
                    q = self.quote or dict(binance=dict(ask=0),mt5=dict(bid=0),usdt_usd=g['costs']['usdt_usd'])
                    g['valuation'] = self.engine.valuation(g,q)
                remaining = self.engine.active()
                completed = bool(new) and not remaining and not errors
                if completed:
                    self.engine.state.update(enabled=False,recovery=False,alarm='平台外部平仓记录已回补；新开仓保持暂停')
                    self.reconciled = True
                message = ('平台外部平仓记录已回补；新开仓保持暂停' if completed else
                           '已回补可确认成交；仍有剩余仓位或待确认记录，请持仓对账后处理')
                if not new and not any(g.get('external_close') for g in active):
                    message = '尚无可安全回补的完整成交；保留原记录，等待平台历史或人工对账'
                if errors: message += '；'+'；'.join(errors)
                if probe_changed: message += '；已记录 MT5 历史字段探针，请重新导出执行诊断'
                self.external_close_status = dict(time_ms=at,level='normal' if completed else 'warning',message=message)
                if new or probe_changed:
                    self.engine.save('external_close_backfilled' if new else 'mt5_history_probe',
                                     ({'groups':sorted(touched),'orders':[o['id'] for o in new]} if new else
                                      {'row_count': history_probe.get('row_count',0),
                                       'query_count': len(history_probe.get('queries',[]))}))
            except Exception:
                self.engine.state = before
                self.external_close_status = previous_status
                self.reconciled = False
                raise
            return True
        finally: self.lock.release()

    def _quote(self, mt5, binance, config=None):
        c = config or self.config
        fx = c['costs']['usdt_usd']
        observed_at = now_ms()
        mt5 = self.mt5_freshness.observe(mt5, c['mt5'], observed_at)
        binance = observed_quote(binance, observed_at)
        return {
            'key': pair_key(c), 'time_ms': now_ms(),
            'binance': binance, 'mt5': mt5,
            'usdt_usd': fx,
            'entry': binance['bid'] * fx - mt5['ask'],
            'exit': binance['ask'] * fx - mt5['bid'],
        }

    def _start_push_bridge(self):
        if self.push_bridge: return
        path = DATA / 'quote-push.json'
        settings = read_json(path, {})
        if not settings.get('token'):
            settings = {'token': secrets.token_hex(32), 'port': 8767}
            atomic_write_json(path, settings)
            try: path.chmod(0o600)
            except OSError: pass
        def notify():
            if self.pump: self.pump.changed.set()
        self.push_bridge = PushBridge(settings['token'], int(settings.get('port',8767)), notify).start()
        if self.pump: self.pump.bridge = self.push_bridge

    def push_setup(self):
        with self.lock:
            self._start_push_bridge()
            c = self.config or load_config()
            return dict(port=self.push_bridge.port, token=self.push_bridge.token,
                        account=c['mt5'].get('account',''), server=c['mt5'].get('server',''),
                        symbol=c['mt5'].get('symbol',''), source='/GoldPairQuotes.mq5',
                        binary='/GoldPairQuotes.ex5' if (ROOT/'mt5/GoldPairQuotes.ex5').exists() else None)

    def _execution_quote(self):
        """Use the latest push tick between legs, falling back to a fresh native read."""
        c=self.config
        mt5=self.push_bridge.quote(c['mt5'], c['strategy']['max_quote_age_ms']) if self.push_bridge else None
        if c['mt5']['adapter']=='paper': mt5=None
        if mt5 is None:
            if c['mt5']['adapter']=='paper': mt5=inspect_paper_terminal(c['mt5'])['quote']
            elif c['mt5']['adapter']=='mcp': mt5=self.terminal.call('snapshot')['quote']
            else: mt5=self.terminal.call('quote')['quote']
        mt5=self.mt5_freshness.observe(mt5,c['mt5'])
        market=self.market_stream.quote(max_age_ms=c['strategy']['max_quote_age_ms']) if self.market_stream else None
        if market is None: market=self.binance.quote(c['symbol'])
        return self._quote(mt5,market,c)

    def _entry_preflight_guard(self, q, threshold):
        preflight = self.entry_preflight
        if preflight is None:
            return True
        near = float(q.get('entry', -float('inf'))) >= float(threshold) - preflight.NEAR_USD
        if near:
            preflight.update(dict(key=q.get('key'), symbol=self.config['symbol'],
                                  position_mode=self.binance.position_mode,
                                  price=float(q['binance']['bid']), qty=float(self.plan['qty'])))
        status = preflight.status()
        if near and not status['ready']:
            labels={'waiting':'等待开仓预检','checking':'开仓预检中','expired':'开仓预检已过期','failed':'开仓预检失败'}
            reason=labels.get(status['state'],'开仓预检未通过')+'：'+status['message']
            self.engine.entry_preflight_detail={'state':status['state'],'reason':reason}
            self.engine.state['alarm'] = reason
            self._preflight_alarm=reason
            return False
        if status['ready'] and self.engine.state.get('alarm')==getattr(self,'_preflight_alarm',None):
            self.engine.state['alarm']=''
        return True

    def _poll(self):
        c = self.config
        q, version, error = self.pump.read()
        if error or q is None:
            self.engine.reconcile_pending(c)
        if error: raise ValueError(error)
        if q is None: return
        self.quote = q
        self.market_meta['mt5_transport'] = self.pump.mt5_transport
        self.market_meta['binance_transport'] = self.pump.binance_transport
        self.market_meta['strategy_trigger'] = '报价事件驱动；订单串行执行'
        if self.entry_preflight:
            self.market_meta['entry_preflight'] = self.entry_preflight.status()
        if c['execution']['mode']=='live' and not self.reconciled:
            self.engine.reconcile_pending(c)
            self._auto_reconcile_flat()
        if c['execution']['mode']=='paper' or self.reconciled:
            if any(g.get('imported') for g in self.engine.active()) and time.monotonic()-getattr(self,'_last_import_check',0)>=2:
                self._guard_import_close()
                self._last_import_check=time.monotonic()
            # Slow account reads above must not leave the strategy using an old sample.
            q, version, error = self.pump.read()
            if error: raise ValueError(error)
            self.quote = q
            if self.entry_preflight and q.get('entry', -float('inf')) >= c['strategy']['entry_spread_usd'] - self.entry_preflight.NEAR_USD:
                self.entry_preflight.update(dict(key=q.get('key'), symbol=c['symbol'],
                                                  position_mode=self.binance.position_mode,
                                                  price=float(q['binance']['bid']), qty=float(self.plan['qty'])))
            self.engine.tick(c, self.plan, q)
        # Maintenance is separate from quote intake. It can delay a strategy pass,
        # but cannot freeze push reception or the chart; the next pass reads latest.
        if c['mt5']['adapter']=='native' and time.monotonic()-(self._last_mt5_snapshot_at or 0)>=2:
            snapshot_started = time.monotonic()
            try:
                self._last_mt5_snapshot = self.terminal.call('snapshot')
            except Exception:
                self._mt5_risk_error = 'MT5 账户快照读取失败'
                raise
            self._last_mt5_snapshot_at = snapshot_started
            self._mt5_risk_error = ''
            if not self._last_mt5_snapshot['allowed']:
                raise ValueError('MT5 自动交易权限已关闭；已暂停开仓')
            self._update_live_carry(self._last_mt5_snapshot['positions'])
        if c['costs']['usdt_usd_auto'] and time.monotonic()-self._last_fx_refresh>=30:
            try:
                fx=self.binance.usdt_usd();c['costs']['usdt_usd']=fx['value'];self.market_meta['fx']=fx
                self.market_meta.pop('fx_warning',None)
            except Exception as exc:
                self.market_meta['fx_warning']='USDT/USD 更新失败，暂用上次数值：'+str(exc)
            self._last_fx_refresh=time.monotonic()
        self._verify_closed_costs()

    def _auto_reconcile_flat(self):
        """Reconcile confirmed closes while the account is in recovery.

        During a two-leg close one platform can briefly be flat while the
        other is still closing. Check at a slow cadence until both are flat,
        or validate adopted residual positions so closing can continue.
        """
        now = time.monotonic()
        if now-self._last_auto_reconcile_at < 2: return False
        self._last_auto_reconcile_at = now
        active = self.engine.active()
        if not active or any(g.get('status') not in ('closing','unwinding') for g in active): return False
        if any(g.get('external_attention') for g in active): return False
        try:
            for group in active: self.engine.resolve(group)
            if any(self.engine.uncertain(group) for group in active):
                return False
            if any(max(self.engine.amounts(group).values())>=1e-8 for group in active):
                # Revalidate both accounts and exact MT5 ownership before
                # continuing an interrupted close; never rearm entries.
                self.engine.pause('平仓回报已确认，正在核验剩余持仓')
                self.reconcile()
                self.engine.save('auto_reconciled_close')
                return True
            positions = self.binance.positions(self.config['symbol'])
            if self.binance.open_orders(self.config['symbol']) or any(abs(float(x.get('positionAmt',0)))>1e-8 for x in positions):
                return False
            mt5 = self.terminal.call('snapshot')
            imported_tickets={ticket for group in active if group.get('imported') for ticket in self.engine.mt5_tickets(group)}
            managed=[x for x in mt5.get('positions',[]) if x.get('magic')==self.config['execution']['magic']
                     or str(x.get('ticket')) in imported_tickets]
            if managed: return False
            if self.quote:
                for group in list(active): self.engine.finalize_flat(group,self.quote)
            self.engine.state['recovery']=False
            self.engine.state['alarm']=''
            self.reconciled=True
            self.engine.save('auto_reconciled_flat')
            return True
        except Exception:
            return False

    def _update_live_carry(self, positions):
        if self.config['execution']['mode']!='live': return
        active=self.engine.active();orders=self.engine.state['orders']
        for group in active:
            if group.get('imported'):
                tickets=self.engine.mt5_tickets(group)
                current=sum(float(p.get('swap',0)) for p in positions if str(p['ticket']) in tickets)
                realized=sum(float(o.get('result',{}).get('swap',0)) for o in orders
                             if o['group']==group['id'] and o['leg']=='mt5' and o['action']=='close')
                group['live_mt5_swap_usd']=current+realized
                continue
            opened=next((o for o in orders if o['group']==group['id'] and o['leg']=='mt5'
                         and o['action']=='open' and o.get('result',{}).get('qty',0)>0),None)
            ticket=str(opened.get('result',{}).get('position','')) if opened else ''
            comment=opened['id'] if opened else ''
            swap=sum(float(p.get('swap',0)) for p in positions
                     if str(p.get('ticket',''))==ticket or p.get('comment')==comment)
            swap+=sum(float(o.get('result',{}).get('swap',0)) for o in orders
                      if o['group']==group['id'] and o.get('source')=='external' and o['leg']=='mt5')
            group['live_mt5_swap_usd']=swap
            group['live_carry_usd']=swap+group.get('live_binance_funding_usd',0)
        if not active or time.monotonic()-self._last_carry_refresh<60: return
        try:
            start=min(g['opened_ms'] for g in active);end=now_ms()
            rows=self.binance.income(self.config['symbol'],start,end)
            for group in active: group['live_binance_funding_usd']=0
            for item in rows:
                if item.get('incomeType')!='FUNDING_FEE': continue
                at=int(item.get('time',0));peers=[g for g in self.engine.state['groups']
                    if g.get('mode')=='live' and g.get('symbol')==self.config['symbol']
                    and g.get('open_binance') is not None and g['opened_ms']<=at<=g.get('closed_ms',at)]
                total=sum(float(g.get('qty',0)) for g in peers)
                if not total: continue
                for group in active:
                    if group in peers:
                        group['live_binance_funding_usd']+=float(item.get('income',0))*group['qty']/total*self.config['costs']['usdt_usd']
            for group in active:
                group['live_carry_usd']=group.get('live_mt5_swap_usd',0)+group.get('live_binance_funding_usd',0)
            self._last_carry_refresh=time.monotonic();self.market_meta.pop('carry_warning',None)
        except Exception as exc:
            self.market_meta['carry_warning']='实盘持仓费更新待重试：'+str(exc)
            self._last_carry_refresh=time.monotonic()

    def _verify_closed_costs(self):
        """Replace estimates with broker deal records after a completed live group.

        Binance commissions are matched to the exact order IDs. MT5 deals are
        matched to our unique comments and magic number. Funding has no order
        ID in Binance's income feed, so it is only attributed while this app
        is the sole managed position for this symbol (enforced by reconcile).
        
        Funding fees are calculated by exact time window to avoid double counting
        when multiple groups have overlapping holding periods.
        """
        if self.config['execution']['mode'] != 'live':
            return
        if any(g['status']!='open' or self.engine.uncertain(g) for g in self.engine.active()):
            # Even a different group's fee history must not delay a pending
            # hedge confirmation or protective close.
            return
        for group in self.engine.state['groups']:
            if group.get('mode') != 'live' or group.get('key') != pair_key(self.config):
                continue
            if group.get('imported'):
                if (group['status']=='closed' and not group.get('import_closing_costs_checked')
                        and now_ms()-group.get('import_cost_check_ms',0)>=60000):
                    group['import_cost_check_ms']=now_ms()
                    try:
                        self._verify_import_costs(group)
                    except Exception as exc:
                        group['cost_verification_error']=str(exc)
                continue
            if group['status'] != 'closed' or group.get('costs_verified'):
                continue
            if now_ms()-group.get('cost_check_ms',0)<60000:
                continue
            group['cost_check_ms']=now_ms()
            try:
                by_id = {o['id']: o for o in self.engine.state['orders'] if o['group'] == group['id']}
                fees = {}
                for order in by_id.values():
                    if order['leg'] != 'binance' or order['result'].get('qty',0)<=0:
                        continue
                    if order.get('source')=='external':
                        # Already read and position-validated by the independent
                        # observer; no additional history on the execution lane.
                        if not order.get('external_deal_ids') or not math.isfinite(order['result']['fee']):
                            raise ValueError('外部成交费用记录待核验')
                        fees[order['id']] = order['result']['fee']
                        continue
                    if not order['result'].get('ticket'):
                        raise ValueError('币安成交订单号尚未确认')
                    fills = self.binance.trades(order['symbol'], order['result']['ticket'])
                    if not fills or abs(sum(float(x.get('qty',0)) for x in fills)-order['result']['qty'])>1e-7:
                        raise ValueError('币安成交费用记录尚未完整返回')
                    fees[order['id']] = self.binance.commissions_usdt(fills)
                mt5_orders = [o for o in by_id.values() if o['leg']=='mt5' and o['result'].get('qty',0)>0]
                deals = self.terminal.call('history', start_ms=group['opened_ms'] - 60000) if mt5_orders else []
                mt_swap = 0
                for order in mt5_orders:
                    if order.get('source')=='external':
                        if not order.get('external_deal_ids') or not math.isfinite(order['result']['fee']):
                            raise ValueError('外部成交费用记录待核验')
                        fees[order['id']] = order['result']['fee']
                        mt_swap += order['result'].get('swap',0)
                        continue
                    rows = [x for x in deals if x.get('comment') == order['id']]
                    if not rows or abs(sum(float(x.get('volume',0))*group['contract'] for x in rows)-order['result']['qty'])>1e-7:
                        raise ValueError('MT5 成交费用记录尚未完整返回')
                    fees[order['id']] = -sum(float(x.get('commission', 0)) + float(x.get('fee', 0)) for x in rows)
                    mt_swap += sum(float(x.get('swap', 0)) for x in rows)
                
                # Funding is reported for the whole symbol position. Allocate
                # each event by group quantity among all groups alive then, so
                # overlapping groups do not each claim the full account fee.
                window_start = group['opened_ms']
                window_end = group['closed_ms'] + 1000
                income = self.binance.income(group['symbol'], window_start, window_end)
                funding = 0
                for item in income:
                    if item.get('incomeType') == 'FUNDING_FEE':
                        fee_time = item.get('time', 0)
                        if window_start <= fee_time <= window_end:
                            peers = [x for x in self.engine.state['groups']
                                     if x.get('mode') == 'live' and x.get('symbol') == group['symbol']
                                     and x.get('open_binance') is not None
                                     and x['opened_ms'] <= fee_time <= x.get('closed_ms', fee_time)]
                            total_qty = sum(float(x.get('qty', 0)) for x in peers)
                            share = float(group['qty']) / total_qty if total_qty else 1
                            funding += float(item.get('income', 0)) * share
                
                if not all(math.isfinite(x) for x in (*fees.values(),mt_swap,funding)):
                    raise ValueError('成交费用记录包含无效数字')
                # Publish only a complete set. A delayed MT5/funding record
                # must not partially overwrite already known commissions.
                for order_id, fee in fees.items():
                    by_id[order_id]['result']['fee'] = fee
                group['mt5_swap_usd'] = mt_swap
                group['binance_funding_usd'] = funding * group['costs']['usdt_usd']
                group['carry_usd'] = group['mt5_swap_usd'] + group['binance_funding_usd']
                group['costs_verified'] = True
                group.pop('cost_verification_error',None)
                group['valuation'] = self.engine.valuation(group, self.quote)
                self.engine.save('costs_verified', {'group': group['id'], 'net': group['valuation']['net']})
            except Exception as exc:
                # Position is already flat; retain estimates and surface the
                # verification failure without treating it as a trade failure.
                group['cost_verification_error'] = str(exc)
                self.engine.save('cost_verification_pending', {'group': group['id']})

    def _verify_import_costs(self, group):
        """Verify trades sent after adoption, while retaining the explicit original cost estimates."""
        orders=[o for o in self.engine.state['orders'] if o['group']==group['id'] and o['action']=='close']
        deals=self.terminal.call('history',start_ms=group['opened_ms']-60000)
        for o in orders:
            if o['result'].get('qty',0)<=0: continue
            if o['leg']=='binance':
                fills=self.binance.trades(o['symbol'],o['result']['ticket'])
                if not fills: raise ValueError('币安接管平仓成交费用尚未返回')
                o['result']['fee']=self.binance.commissions_usdt(fills)
            else:
                rows=[d for d in deals if d.get('comment')==o['id']]
                if abs(sum(float(d['volume'])*group['contract'] for d in rows)-o['result']['qty'])>1e-7:
                    raise ValueError('MT5 接管平仓成交记录尚未完整返回')
                o['result']['fee']=-sum(float(d.get('commission',0))+float(d.get('fee',0)) for d in rows)
                o['result']['swap']=sum(float(d.get('swap',0)) for d in rows)
        group['live_mt5_swap_usd']=sum(o['result'].get('swap',0) for o in orders if o['leg']=='mt5')
        funding=0
        for item in self.binance.income(group['symbol'],group['opened_ms'],group['closed_ms']+1000):
            at=int(item.get('time',0))
            if item.get('incomeType')!='FUNDING_FEE' or not group['opened_ms']<=at<=group['closed_ms']: continue
            peers=[g for g in self.engine.state['groups'] if g.get('mode')=='live' and g['symbol']==group['symbol']
                   and g['opened_ms']<=at<=g.get('closed_ms',at)]
            total=sum(g['qty'] for g in peers)
            if total: funding+=float(item['income'])*group['qty']/total*group['costs']['usdt_usd']
        group['live_binance_funding_usd']=funding
        group['import_closing_costs_checked']=True
        group['costs_verified']=False
        group.pop('cost_verification_error',None)
        group['valuation']=self.engine.valuation(group,self.quote)
        self.engine.save('adoption_closing_costs_checked',{'group':group['id']})

    def _loop(self):
        while not self.stop_event.is_set():
            changed = self.market_wake.wait(.1)
            self.market_wake.clear()
            with self.lock:
                if not self.connected or not self.config:
                    continue
                poll = self.config['execution']['poll_ms'] / 1000
                last = getattr(self, '_last_poll', 0)
                if not changed and time.monotonic() - last < poll:
                    continue
                self._last_poll = time.monotonic()
                try:
                    self._poll()
                    self.last_error = ''
                except Exception as exc:
                    self.last_error = str(exc)
                    # Fail closed even when there is no current position:
                    # a transient channel failure must not leave an armed
                    # live engine that resumes opening by itself later.
                    if self.engine.state.get('enabled'):
                        self.engine.pause('行情或交易通道异常：' + self.last_error)

    def config_changed(self, current_config):
        incoming=copy.deepcopy(current_config);effective=copy.deepcopy(self.config)
        if not effective: return False
        incoming.pop('risk_alerts', None);effective.pop('risk_alerts', None)
        if effective['costs']['usdt_usd_auto']:
            incoming['costs']['usdt_usd']=effective['costs']['usdt_usd']
        if effective['costs']['binance_fee_auto'] and effective['execution']['mode']=='live':
            incoming['costs']['binance_taker_percent']=effective['costs']['binance_taker_percent']
        return incoming != effective

    def start(self, current_config=None):
        with self.lock:
            if not self.connected:
                raise ValueError('请先连接并完成双边规格校验')
            if current_config is not None:
                if self.config_changed(current_config):
                    raise ValueError('配置已修改，请重新点击“保存并连接”')
            if self.config['execution']['mode'] == 'live' and not self.reconciled:
                raise ValueError('实盘启动前必须先完成持仓对账')
            if self.config['execution']['mode'] == 'live':
                mode_before=self.binance.position_mode
                mode_now=self.binance.refresh_position_mode()
                if mode_now != mode_before:
                    self.reconciled=False
                    self.engine.pause('币安持仓模式在连接后发生变化，请重新持仓对账')
                    raise ValueError('币安持仓模式在连接后发生变化，请重新连接并完成持仓对账')
            self.engine.start(self.config)
            self.external_close_status = None
            return self.snapshot()

    def pause(self):
        with self.lock:
            self.engine.pause()
            return self.snapshot()

    def apply_exit_rules(self, body):
        with self.lock:
            if not self.connected:raise ValueError('请先连接双边行情')
            if not self.reconciled:raise ValueError('请先完成持仓对账')
            ids=exit_rules.apply(self.engine,self.config,body)
            return {**self.snapshot(),'message':f'已更新 {len(ids)} 组的退出规则及所填返佣比例；预计返佣计入净收益，下一次有效行情检查起生效，满足条件可能立即平仓。'}

    def apply_entry_threshold(self, value):
        """Apply only the live opening threshold without reconnecting.

        Connection/account changes remain gated by reconnect and reconciliation;
        this setting is safe to change while the quote stream is running.
        """
        with self.lock:
            if not self.connected:
                raise ValueError('请先连接双边行情，再应用开仓阈值')
            if value is None or isinstance(value, bool) or not str(value).strip():
                raise ValueError('请填写开仓阈值')
            try:
                threshold = float(value)
            except (TypeError, ValueError):
                raise ValueError('开仓阈值必须是数字')
            if not math.isfinite(threshold) or threshold < 0 or threshold > 100000:
                raise ValueError('开仓阈值必须在 0 到 100000 USD/盎司之间')
            # Persist only this field; never overwrite newer saved credentials
            # or other pending edits with the older runtime configuration.
            saved = load_config()
            saved['strategy']['entry_spread_usd'] = threshold
            write_config(saved)
            self.config['strategy']['entry_spread_usd'] = threshold
            self.engine.save('entry_threshold_applied', {'value': threshold})
            self.market_meta['strategy_trigger'] = '报价事件驱动；开仓阈值已应用'
            return self.snapshot()

    def close_group(self, group=None, reason='用户请求平仓'):
        with self.lock:
            if self.config and self.config['execution']['mode']=='live' and not self.reconciled:
                raise ValueError('请先完成持仓对账，再请求平仓')
            if not self.quote:
                raise ValueError('尚无可用报价，不能请求平仓')
            for g in self.engine.active():
                if group is None or g['id']==group:
                    g['exit_signal']=self.quote.get('exit')
            self.engine.request_close(group, reason)
            # One immediate attempt; the background loop performs retries.
            self.engine.tick(self.config, self.plan, self.quote)
            result=self.snapshot()
            targets=[g for g in self.engine.state['groups'] if group is None or g['id']==group]
            if any(self.engine.uncertain(g) for g in targets):
                result['message']='平仓请求已记录；正在核对未确认成交，确认后才继续双边平仓。当前尚未双平，请勿把未知数量当作零。'
            elif any(g['status']=='attention' for g in targets):
                result['message']='平仓未完成：重试已达上限，请核对平台仓位后重试。'
            elif any(g['status']!='closed' for g in targets):
                result['message']='平仓请求已记录，尚未双平；请查看行情有效性、对账状态及交易组原因。'
            else:
                result['message']='本次请求涉及的交易组已确认双边平仓。'
            return result

    def _validate_import_exposure(self, mt5, positions, pending):
        """Compare account quantities to the journal, including each adopted MT5 identity."""
        if pending or mt5.get('orders'):
            raise ValueError('当前品种存在挂单，暂停接管管理，请处理挂单后重新对账')
        if mt5.get('orders') is None:
            raise ValueError('未取得 MT5 挂单检查结果')
        expected=sum(self.engine.amounts(g)['binance'] for g in self.engine.active())
        rows=[p for p in positions if float(p.get('positionAmt',0))]
        if any(float(p['positionAmt'])>0 or p.get('positionSide') not in ('SHORT','BOTH') for p in rows):
            raise ValueError('币安持仓方向改变，请重新对账')
        if abs(sum(abs(float(p['positionAmt'])) for p in rows)-expected)>1e-7:
            raise ValueError('币安实际数量已变化，与接管及策略记录不符；暂停处理，请人工核对')
        current={str(p['ticket']):p for p in mt5['positions']}
        for g in self.engine.active():
            if not g.get('imported'): continue
            remaining=self.engine.mt5_tickets(g)
            for o in self.engine.state['orders']:
                if o['group']!=g['id'] or not o.get('original_position'): continue
                original=o['original_position'];ticket=str(original['ticket']);actual=current.get(ticket)
                qty=remaining.get(ticket,0)
                if qty<=1e-8:
                    if actual: raise ValueError('已平接管票据仍有持仓，请人工核对')
                    continue
                if (not actual or actual['side']!=0 or
                    any(str(actual.get(k))!=str(original.get(k)) for k in ('identifier','magic','time_ms','price_open')) or
                    abs(actual['lots']*g['contract']-qty)>1e-7):
                    raise ValueError('接管 MT5 票据 '+ticket+' 的身份或数量已变化，请人工核对')

    def _guard_import_close(self):
        try:
            if not self.reconciled: raise ValueError('请先完成持仓对账')
            for g in self.engine.active():
                if self.engine.uncertain(g): self.engine.resolve(g)
            if any(self.engine.uncertain(g) for g in self.engine.active()):
                # A pending status is not an ownership mismatch. Keep polling
                # without sending another order or disabling close recovery.
                return False
            mt5=self.terminal.call('snapshot')
            self._validate_import_exposure(mt5,self.binance.positions(self.config['symbol']),
                                           self.binance.open_orders(self.config['symbol']))
        except Exception:
            self.reconciled=False
            self.engine.state['recovery']=True
            for g in self.engine.active():
                if g.get('imported'): g['management_enabled']=False
            self.engine.pause('接管仓位核验未通过，请查看错误并重新对账')
            raise

    def _adoption_read(self):
        if not self.connected or self.config['execution']['mode']!='live':
            raise ValueError('请先连接实盘账户；接管登记本身不会下单')
        if self.engine.active():
            raise ValueError('首次接管需没有未平策略组；已接管仓位请在下方独立管理，不能重复导入')
        if self.engine.state['enabled']:
            raise ValueError('请先暂停新开仓，再读取接管预览')
        self.binance.refresh_position_mode()
        return (self.terminal.call('snapshot'), self.binance.positions(self.config['symbol']),
                self.binance.open_orders(self.config['symbol']))

    def adoption_plan(self, body):
        with self.lock:
            mt5,positions,pending=self._adoption_read()
            tickets=body.get('tickets',[])
            if not isinstance(tickets,list) or any(not isinstance(t,str) for t in tickets):
                raise ValueError('MT5 票据列表无效')
            p=position_adoption.preview(self.config,mt5,positions,pending,tickets,body,self.spec,now_ms())
            p['binance_key_fingerprint']=hashlib.sha256(self.binance.key.encode()).hexdigest()
            self.adoption_preview=p
            return {'preview':{k:v for k,v in p.items() if k not in ('fingerprint','binance_key_fingerprint')}}

    def adoption_confirm(self, body):
        with self.lock:
            token=body.get('preview_id')
            # A lost HTTP response can be retried without importing twice.
            if any(g.get('imported') and g['id']==token for g in self.engine.state['groups']):
                return self.snapshot()
            p=self.adoption_preview
            if not p or p['id']!=token or now_ms()-p['created_ms']>120000:
                raise ValueError('预览已失效，请重新预览后确认')
            mt5,positions,pending=self._adoption_read()
            if (p['key']!=pair_key(self.config) or
                now_ms()-p['created_ms']>120000 or
                p['fingerprint']!=position_adoption.fingerprint(positions,mt5,pending) or
                p['binance_key_fingerprint']!=hashlib.sha256(self.binance.key.encode()).hexdigest()):
                self.adoption_preview=None
                raise ValueError('平台持仓或连接已变化，未接管；请重新预览')
            g=position_adoption.register(self.engine,self.config,p,now_ms())
            self.reconciled=False;self.adoption_preview=None
            result=self.reconcile()
            result['message']='已登记接管并完成对账，尚未启动已有仓位管理；本次没有下单'
            return result

    def adoption_manage(self, body):
        with self.lock:
            g=next((g for g in self.engine.active() if g.get('imported') and g['id']==body.get('group')),None)
            if not g: raise ValueError('未找到活动接管篮子')
            if g['status']!='open': raise ValueError('篮子正在平仓或有异常，请先处理订单状态')
            enabled=body.get('enabled')
            if type(enabled) is not bool: raise ValueError('启停参数无效')
            if enabled:
                if self._guard_import_close() is False:
                    raise ValueError('尚有成交状态未确定，请等待查询确认后再启动管理')
                if self.engine.state['recovery']: raise ValueError('请先完成持仓对账')
                g['parameters']['take_contraction_usd']=position_adoption.number(body,'take_contraction_usd',.000001,1e5)
                g['parameters']['min_net_profit_usd']=position_adoption.number(body,'min_net_profit_usd',0,1e9)
            g['management_enabled']=enabled
            self.engine.save('adoption_management',{'group':g['id'],'enabled':enabled})
            return self.snapshot()

    def _strategy_tickets_match(self, positions):
        """Validate each ordinary group's remaining MT5 ticket, not just totals."""
        current = {str(p['ticket']): p for p in positions}
        claimed = set()
        for group in self.engine.active():
            if group.get('imported'): continue
            remaining = self.engine.mt5_tickets(group)
            for order in self.engine.state['orders']:
                if (order['group']!=group['id'] or order['leg']!='mt5' or order['action']!='open'
                        or order['result'].get('qty',0)<=0):
                    continue
                ticket = str(order['result'].get('position') or '')
                if not ticket or ticket in claimed: return False
                claimed.add(ticket)
                qty = remaining.get(ticket,0)
                actual = current.get(ticket)
                if qty < -1e-8: return False
                if qty<=1e-8:
                    if actual: return False
                    continue
                if (not actual or actual.get('side')!=0
                        or actual.get('magic')!=self.config['execution']['magic']
                        or actual.get('comment')!=order['id']
                        or abs(float(actual['lots'])*group['contract']-qty)>1e-7):
                    return False
        return True

    def reconcile(self):
        with self.lock:
            # 必须先连接才能进行对账
            if not self.connected:
                raise ValueError('请先连接后再进行对账')
            self.position_report = None

            if self.config['execution']['mode'] == 'paper':
                # Paper state is deterministic and is reconciled from journal.
                for g in self.engine.active(): self.engine.resolve(g, force=True)
                self.engine.state['recovery'] = any(self.engine.uncertain(g) for g in self.engine.active())
                self.reconciled = not self.engine.state['recovery']
                self.engine.save('paper_reconciled')
                return self.snapshot()
            
            # 实盘模式对账：验证实际持仓与策略日志一致性
            self.reconciled = False
            self.binance.refresh_position_mode()
            for g in self.engine.active():
                self.engine.resolve(g, force=True)
            positions = self.binance.positions(self.config['symbol'])
            open_orders = self.binance.open_orders(self.config['symbol'])
            signed_bqty = sum(float(x.get('positionAmt', 0)) for x in positions)
            mt5 = self.terminal.call('snapshot')
            # A read-only snapshot is also useful when reconciliation fails.
            # Never turn external positions into managed groups here.
            self.position_report = {
                'time_ms': now_ms(), 'status': '已读取，核对尚未通过',
                'symbol': self.config['symbol'], 'mt5_symbol': self.config['mt5']['symbol'],
                'mt5_currency': mt5.get('account', {}).get('currency', 'USD'),
                'binance': [{k: x.get(k) for k in ('symbol', 'positionSide', 'positionAmt',
                            'entryPrice', 'markPrice', 'unRealizedProfit', 'updateTime')}
                            for x in positions if float(x.get('positionAmt', 0)) != 0],
                'binance_orders': [{k: x.get(k) for k in ('orderId', 'symbol', 'side',
                                    'positionSide', 'type', 'origQty', 'executedQty', 'price', 'status')}
                                   for x in open_orders],
                'mt5': [dict(x, managed=False) for x in mt5['positions']],
            }
            imported_tickets={ticket for g in self.engine.active() if g.get('imported') for ticket in self.engine.mt5_tickets(g)}
            managed_mt5 = [x for x in mt5['positions'] if x['magic'] == self.config['execution']['magic'] or str(x['ticket']) in imported_tickets]
            lots = sum(float(x['lots']) for x in managed_mt5)
            expected_b = sum(self.engine.amounts(g)['binance'] for g in self.engine.active())
            expected_m = sum(self.engine.amounts(g)['mt5'] / g['contract'] for g in self.engine.active())
            expected_comments = {o['id'] for o in self.engine.state['orders']
                                 if o['group'] in {g['id'] for g in self.engine.active()}
                                 and o['leg'] == 'mt5' and o['action'] == 'open'}
            for position in self.position_report['mt5']:
                position['managed'] = (position.get('magic') == self.config['execution']['magic']
                                       and position.get('comment') in expected_comments) or str(position['ticket']) in imported_tickets
            if imported_tickets:
                self._validate_import_exposure(mt5,positions,open_orders)
                # A close can be confirmed by the platform before the next
                # strategy tick. Finalize flat closing groups here so the UI
                # does not remain stuck in `closing` or offer a duplicate
                # close button.
                current_quote=getattr(self,'quote',None)
                for group in list(self.engine.active()):
                    if group.get('imported') and current_quote and self.engine.finalize_flat(group, current_quote):
                        group['import_closing_costs_checked'] = False
            by_comment = {x.get('comment'): x for x in managed_mt5 if x.get('comment')}
            for order in self.engine.state['orders']:
                if (order['id'] in by_comment and order['leg'] == 'mt5' and order['action'] == 'open'
                        and not order.get('result', {}).get('position')):
                    order['result']['position'] = str(by_comment[order['id']]['ticket'])

            # One-way mode returns a signed BOTH amount. Hedge Mode returns
            # separate LONG/SHORT rows; this strategy owns only SHORT.
            position_mode=getattr(self.binance, 'position_mode', 'one_way')
            if position_mode=='hedge':
                long_qty=sum(abs(float(x.get('positionAmt',0))) for x in positions if x.get('positionSide')=='LONG')
                short_qty=sum(abs(float(x.get('positionAmt',0))) for x in positions if x.get('positionSide')=='SHORT')
                other_qty=sum(abs(float(x.get('positionAmt',0))) for x in positions
                              if x.get('positionSide') not in ('LONG','SHORT'))
                signed_bqty=short_qty
            else:
                long_qty=0
                short_qty=0
                other_qty=0
                signed_bqty=sum(float(x.get('positionAmt', 0)) for x in positions)
            bad_mt5 = any(int(x.get('side', -1)) != 0 or (x.get('comment') not in expected_comments and str(x['ticket']) not in imported_tickets)
                          for x in managed_mt5)
            bad_mt5 = bad_mt5 or not self._strategy_tickets_match(mt5['positions'])
            binance_mismatch=(long_qty>0.000001 or other_qty>0.000001 or abs(short_qty-expected_b)>0.000001) if position_mode=='hedge' else abs(signed_bqty + expected_b)>0.000001
            if open_orders or binance_mismatch or abs(lots - expected_m) > .0000001 or bad_mt5:
                self.engine.pause('账户持仓与本策略日志不一致，禁止自动处理；请人工核对')
                monitor=getattr(self,'external_monitor',None)
                if monitor:monitor.wake()
                if (self.engine.active() and not any(abs(float(p.get('positionAmt',0)))>1e-8 for p in positions)
                        and not mt5['positions'] and not mt5['orders'] and not open_orders):
                    self.position_report['status']='平台当前空仓，本机未结束记录等待成交回补'
                    self.external_close_status=dict(time_ms=now_ms(),level='warning',
                        message='平台当前空仓；正在核对原订单与平仓历史，确认后结束旧组；不会重新开仓')
                    raise ValueError('双边平台当前空仓，但本机仍有未结束交易记录；已请求后台核对原订单和平仓成交。请等待状态刷新，回补完成后再启动；若持续等待，请导出执行诊断。')
                details = []
                if open_orders: details.append('币安存在未完成委托')
                if binance_mismatch: details.append('币安持仓模式、空头方向或数量不符')
                if abs(lots - expected_m) > .0000001 or bad_mt5: details.append('MT5 持仓数量、方向或注释不符')
                raise ValueError('实际持仓与本策略日志不一致：' + '；'.join(details) + '。手工仓位可在“已有持仓管理”中预览接管，无需先平仓')
            
            self.engine.state['recovery'] = any(self.engine.uncertain(g) for g in self.engine.active())
            if self.engine.state['recovery']:
                self.engine.pause('仍有订单成交状态未确定，禁止启动')
                raise ValueError('仍有订单成交状态未确定，请继续核对平台成交记录')
            if not self.engine.state['recovery']:
                self.engine.state['alarm'] = ''
            self.reconciled = True
            self.position_report['status'] = '本策略持仓核对通过；其他 MT5 持仓仅展示'
            self.engine.save('reconciled', {'binance_qty': signed_bqty, 'position_mode':position_mode, 'mt5_lots': lots})
            return self.snapshot()

    def snapshot(self):
        with self.lock:
            saved_config = load_config()
            # A valuation snapshot remains available while management is paused,
            # without invoking strategy decisions or changing durable state.
            st = copy.deepcopy(self.engine.state)
            quote = (self.pump.read()[0] if getattr(self,'pump',None) else None) or self.quote
            for group in st['groups']:
                if group['status'] != 'closed':
                    unverified=(not quote or group.get('key')!=quote.get('key') or not self.connected
                                or group['status']!='open' or self.engine.uncertain(group)
                                or group.get('external_attention') or st.get('recovery')
                                or (self.config and self.config['execution']['mode']=='live' and not self.reconciled))
                    group['valuation_status']='unverified' if unverified else 'estimate'
                    if not unverified:group['valuation'] = self.engine.valuation(group, quote)
            return {
                'connected': self.connected, 'last_error': self.last_error,
                'quote': quote, 'plan': self.plan,
                'state': st, 'events': self.store.events(),
                'auto_values': {**self.market_meta, **({'mt5_transport':self.pump.mt5_transport, 'binance_transport':self.pump.binance_transport} if getattr(self,'pump',None) else {})},
                'position_report': self.position_report,
                'external_close_status': copy.deepcopy(getattr(self,'external_close_status',None)),
                'strategy_runtime': {'entry_spread_usd': self.config['strategy']['entry_spread_usd'],
                                     'max_quote_age_ms': self.config['strategy']['max_quote_age_ms'],
                                     'mt5_symbol': self.config['mt5']['symbol'], 'symbol': self.config['symbol']} if self.config else None,
                'config_pending': bool(self.connected and self.config_changed(saved_config)),
                'liquidation_risk': risk_snapshot(
                    self.config['execution']['mode'] if self.config else 'paper', self.connected,
                    getattr(self, 'risk_monitor', None), getattr(self, '_last_mt5_snapshot', None),
                    getattr(self, '_last_mt5_snapshot_at', None), saved_config.get('risk_alerts', {}),
                    mt5_error=getattr(self, '_mt5_risk_error', '')),
                'entry_preflight': self.entry_preflight.status() if self.entry_preflight else None,
                'capabilities': {'live_orders': bool(self.config and self.config['execution']['mode'] == 'live'),
                                 'mode': self.config['execution']['mode'] if self.config else 'paper',
                                 'reconciled': self.reconciled,
                                 'position_mode': getattr(self.binance, 'position_mode', 'one_way') if self.connected else None},
            }

    def chart(self, since):
        with self.lock:
            # Keep history available after a page refresh or service restart,
            # before a new connection has established the runtime key.
            config = self.config or load_config()
            key = pair_key(config) if config else ''
        return self.store.read_samples(key, since)


TRADING = TradingRuntime()

DEFAULT = json.loads((ROOT / "config.example.json").read_text(encoding="utf-8"))


def now_ms() -> int:
    return int(time.time() * 1000)


def read_json(path: Path, fallback):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError):
        return fallback


def deep_merge(base, incoming):
    result = dict(base)
    for key, value in incoming.items():
        if isinstance(value, dict) and isinstance(result.get(key), dict):
            result[key] = deep_merge(result[key], value)
        else:
            result[key] = value
    return result


def load_config():
    config = deep_merge(DEFAULT, read_json(CONFIG_PATH, {}))
    # 1000 ms was the old example value and is too narrow for many stable
    # internet/proxy routes.  Migrate that untouched legacy default so an
    # existing installation does not keep failing signed requests after an
    # upgrade.  Other user-selected values remain unchanged.
    binance = config.setdefault('binance', {})
    if binance.get('recv_window_ms') == 1000:
        binance['recv_window_ms'] = 5000
    # Keep older saved configurations valid after adding the dedicated
    # protective-close budget.  A user who previously chose a larger normal
    # slippage must not get a validation error on the new default.
    strategy = config.get('strategy', {})
    try:
        normal_slippage = float(strategy.get('max_slippage_usd', 0))
        unwind_slippage = float(strategy.get('unwind_slippage_usd', 0))
        if math.isfinite(normal_slippage) and math.isfinite(unwind_slippage) and unwind_slippage < normal_slippage:
            strategy['unwind_slippage_usd'] = normal_slippage
    except (TypeError, ValueError):
        pass
    legacy_paper = config.get('mt5', {}).get('adapter') == 'paper' and (
        config['mt5'].get('account') == 'PAPER-MT5' or config['mt5'].get('server') == 'local-paper')
    if legacy_paper:
        config['mt5']['adapter'] = 'native' if sys.platform == 'win32' else 'mcp'
        config['mt5']['mcp_url'] = config['mt5'].get('mcp_url') or DEFAULT_MCP_URL
        if config['mt5'].get('account') == 'PAPER-MT5':
            config['mt5']['account'] = ''
        if config['mt5'].get('server') == 'local-paper':
            config['mt5']['server'] = ''
    return config


def write_config(config):
    # Atomic replace prevents a stopped app from leaving half a configuration.
    fd, name = tempfile.mkstemp(dir=DATA, prefix='config-', suffix='.tmp')
    try:
        with os.fdopen(fd, 'w', encoding='utf-8') as handle:
            json.dump(config, handle, ensure_ascii=False, indent=2, allow_nan=False)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(name, CONFIG_PATH)
    finally:
        if os.path.exists(name):
            os.unlink(name)


def connection_key(settings):
    return tuple(str(settings.get(k, '')).strip() for k in (
        'adapter', 'terminal_path', 'mcp_url', 'account', 'server', 'symbol',
        'contract_size_oz', 'volume_min', 'volume_step', 'volume_max',
    ))


def checked_spec(settings, require_applied=False):
    if not LAST_PROBE or LAST_PROBE['key'] != connection_key(settings):
        raise ValueError('请先检查 MT5 连接，并应用读取到的账户及规格。')
    result = LAST_PROBE['result']
    if require_applied and not LAST_PROBE.get('applied'):
        raise ValueError('请先核对并应用读取结果')
    if not result.get('identity_matches') or not result.get('symbol') or time.monotonic() - LAST_PROBE['at'] > 300:
        raise ValueError('MT5 检查已失效，请重新检查连接。')
    return result


def save_event(event):
    event = {"time_ms": now_ms(), **event}
    with EVENTS_PATH.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(event, ensure_ascii=False) + "\n")


def validate_config(config):
    errors = []
    if config.get('mode') != 'paper':
        errors.append('旧纸面接口固定为 paper；自动执行模式请选择 paper 或 live')
    symbol = str(config.get("symbol") or '').upper()
    if not 5 <= len(symbol) <= 24 or not symbol.isalnum():
        errors.append('币安合约名称无效')
    mt5 = config.get("mt5", {})
    strategy = config.get("strategy", {})
    lots = float(strategy.get("mt5_lots", 0) or 0)
    contract = float(mt5.get("contract_size_oz", 0) or 0)
    step = float(mt5.get("volume_step", 0) or 0)
    vmin = float(mt5.get("volume_min", 0) or 0)
    if not isinstance(mt5.get('symbol', ''), str):
        errors.append('MT5 品种名称应为文本')
    if mt5.get('adapter') == 'mcp':
        try:
            validate_mcp_url(mt5.get('mcp_url'))
        except ValueError as exc:
            errors.append(str(exc))
    if contract <= 0:
        errors.append("MT5 合约大小必须大于 0")
    if lots <= 0:
        errors.append("MT5 手数必须大于 0")
    if step <= 0 or vmin <= 0:
        errors.append("MT5 最小手数和步长必须大于 0")
    if not (0 <= float(strategy.get("max_quote_age_ms", 0)) <= 60000):
        errors.append("行情最大年龄范围应为 0 到 60000 毫秒")
    if not (0 <= float(strategy.get("max_unhedged_ms", 0)) <= 60000):
        errors.append("单边敞口时间范围应为 0 到 60000 毫秒")
    if not all(math.isfinite(x) for x in (lots, contract, step, vmin)):
        errors.append('数量和规格必须是有限数值')
    for key in ('entry_spread_usd', 'take_contraction_usd', 'max_slippage_usd', 'unwind_slippage_usd'):
        value = float(strategy.get(key, 0))
        if not math.isfinite(value) or value < 0:
            errors.append('价差、目标和滑点应为有限的非负数')
    if float(strategy.get('unwind_slippage_usd', 0)) < float(strategy.get('max_slippage_usd', 0)):
        errors.append('保护性平仓滑点不能小于普通最大滑点')
    window = float(config.get('binance', {}).get('recv_window_ms', 1000))
    if not math.isfinite(window) or not 1 <= window <= 60000:
        errors.append('请求有效窗口应为 1 到 60000 毫秒')
    try:
        validate_trading(config)
    except (ValueError, KeyError, TypeError) as exc:
        errors.append(str(exc))
    return errors


def order_plan(config, binance_bid=0.0):
    spec = checked_spec(config['mt5'], require_applied=True)['symbol']
    lots = Decimal(str(config['strategy']['mt5_lots']))
    minimum, maximum, step = (Decimal(str(spec[k])) for k in ('volume_min', 'volume_max', 'volume_step'))
    if not lots.is_finite() or lots < minimum or lots > maximum or lots % step:
        raise ValueError('MT5 手数不符合终端返回的最小值、最大值或步长。')
    qty = lots * Decimal(str(spec['contract_size_oz']))
    price = float(binance_bid or 0)
    if not math.isfinite(price) or price < 0:
        raise ValueError('参考价格必须是有限的非负数')
    return {
        "mt5_symbol": spec['name'],
        "mt5_lots": float(lots),
        "contract_size_oz": spec['contract_size_oz'],
        "gold_qty_oz": float(qty),
        "binance_symbol": config["symbol"],
        "binance_qty_xau": float(qty),
        "binance_notional_usdt": float(qty) * price if price > 0 else None,
        "note": "MT5 规格已读取；币安数量是 1 XAU 对应 1 盎司假设下的预览，尚未校验交易所合约过滤器，不是可执行订单。",
        "direction": "卖 XAUUSDT / 买 MT5",
    }


def inspect_binance(config, api_key='', api_secret=''):
    """Read-only connectivity and permission check; never submits an order."""
    mode = config['execution']['mode']
    if mode == 'live' and (not api_key or not api_secret):
        raise ValueError('实盘检查需要输入本次会话的币安 API Key 和 Secret Key')
    client = Binance(production=True, key=api_key, secret=api_secret,
                     recv_window_ms=config['binance']['recv_window_ms'],
                     proxy_url=config['binance'].get('proxy_url', ''))
    client.sync()
    spec = client.spec(config['symbol'])
    stream = BinanceBookTicker(config['symbol'], production=True,
                               proxy_url=config['binance'].get('proxy_url', '')).start()
    try:
        quote = stream.quote(max_age_ms=config['strategy']['max_quote_age_ms'], wait_ms=2500)
        transport = 'WebSocket bookTicker'
        warning = ''
        if quote is None:
            quote = client.quote(config['symbol'])
            transport = 'REST 回退'
            warning = 'WebSocket 未在等待时间内收到数据；正式连接会继续自动重连并使用 REST 回退'
    finally:
        stream.close()
    fx = client.usdt_usd()
    account = fees = permissions = None
    permission_warning = ''
    if mode == 'live':
        try:
            permissions = client.api_permissions()
        except Exception as exc:
            permission_warning = str(exc)
        account = client.preflight(permissions)
        fees = client.commission_rate(config['symbol'])
    return {
        'mode': mode, 'symbol': spec['symbol'], 'status': spec.get('status'),
        'base_asset': spec.get('baseAsset'), 'quote_asset': spec.get('quoteAsset'),
        'quote': quote, 'transport': transport, 'transport_warning': warning,
        'usdt_usd': fx, 'account': account, 'permissions': permissions,
        'permission_warning': permission_warning, 'fees': fees,
        'message': '币安公开行情及合约规格正常' if mode == 'paper'
                   else ('币安连接检查通过：行情、账户及基础权限正常。\n'
                         '本次未进行开仓预检；该品种下单资格以交易台的开仓预检结果为准。'),
        'orders_sent': False,
    }


def inspect_binance_public_ip(config):
    """Detect the current public IP using the same route as Binance REST."""
    proxy_url = config['binance'].get('proxy_url', '')
    client = Binance(production=True, recv_window_ms=config['binance']['recv_window_ms'],
                     proxy_url=proxy_url)
    return {
        'ip': client.public_ip(),
        'route': 'proxy' if proxy_url else 'direct',
        'route_label': '经配置代理' if proxy_url else '直连网络',
        'current_only': True,
    }


class Handler(BaseHTTPRequestHandler):
    server_version = "GoldPairLocal/0.1"

    def log_message(self, fmt, *args):
        print("[%s] %s" % (self.log_date_time_string(), fmt % args))

    def send_json(self, payload, status=200):
        raw = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def read_body(self):
        length = int(self.headers.get("Content-Length", "0"))
        if not 0 <= length <= 100_000:
            raise ValueError("请求过大")
        result = json.loads(self.rfile.read(length) or b"{}", parse_constant=lambda _: None)
        if not isinstance(result, dict):
            raise ValueError('请求应为配置对象')
        return result

    def local_request(self, mutation=False):
        port = self.server.server_port
        allowed = (f'127.0.0.1:{port}', f'localhost:{port}')
        if self.headers.get('Host') not in allowed:
            self.send_json({'ok': False, 'error': '仅接受本机访问'}, 403)
            return False
        if mutation and (self.headers.get('X-Local-App') != 'GoldPairLocal' or
                         self.headers.get('Origin') not in (None, *(f'http://{h}' for h in allowed))):
            self.send_json({'ok': False, 'error': '请从本机应用页面操作'}, 403)
            return False
        return True

    def quote_stream(self):
        origin = self.headers.get('Origin')
        if origin and origin not in (f'http://127.0.0.1:{self.server.server_port}', f'http://localhost:{self.server.server_port}'):
            self.send_json({'ok':False,'error':'仅接受本机页面'},403);return
        hub = TRADING.quote_events
        raw = self.headers.get('Last-Event-ID','')
        try:
            epoch, number = raw.split(':',1)
            cursor = int(number) if epoch==hub.epoch else -1
        except (ValueError, TypeError): cursor = -1
        self.send_response(200)
        self.send_header('Content-Type','text/event-stream; charset=utf-8')
        self.send_header('Cache-Control','no-cache, no-transform')
        self.send_header('X-Accel-Buffering','no')
        self.end_headers()
        self.connection.settimeout(15)
        self.connection.setsockopt(socket.IPPROTO_TCP,socket.TCP_NODELAY,1)
        try:
            while True:
                sequence, reset, rows = hub.read(cursor)
                if cursor < 0 or reset:
                    payload = {'reset':True,'samples':[q for _,q in rows[-1:]]}
                else: payload = {'reset':False,'samples':[q for _,q in rows]}
                if rows or cursor<0 or reset:
                    data = json.dumps(payload,ensure_ascii=False,allow_nan=False)
                    self.wfile.write(f'id: {hub.epoch}:{sequence}\nevent: quotes\ndata: {data}\n\n'.encode())
                    cursor=sequence
                else: self.wfile.write(b': heartbeat\n\n')
                self.wfile.flush()
        except (OSError, ValueError): pass
        finally: self.close_connection=True

    def do_GET(self):
        if not self.local_request():
            return
        path = urlparse(self.path).path
        if path == '/favicon.ico':
            self.send_response(204)
            self.end_headers()
            return
        if path in ("/", "/index.html"):
            data = (ROOT / "index.html").read_bytes()
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
            return
        if path in ("/app.js", "/workspace.js", "/trading-app.js", "/quote-stream.js", "/style.css", "/echarts.min.js"):
            suffix = path[1:]
            data = (ROOT / suffix).read_bytes()
            content_type = "text/javascript; charset=utf-8" if suffix.endswith("js") else "text/css; charset=utf-8"
            self.send_response(200)
            self.send_header("Content-Type", content_type)
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
            return
        if path == '/api/trading/stream':
            self.quote_stream(); return
        if path in ('/GoldPairQuotes.mq5','/GoldPairQuotes.ex5'):
            asset = ROOT / 'mt5' / path[1:]
            if not asset.exists():
                self.send_json({'ok':False,'error':'EA 尚未编译'},404);return
            data=asset.read_bytes()
            self.send_response(200)
            self.send_header('Content-Type','application/octet-stream')
            self.send_header('Content-Disposition','attachment; filename="'+path[1:]+'"')
            self.send_header('Content-Length',str(len(data)))
            self.end_headers();self.wfile.write(data);return
        if path == "/api/status":
            with LOCK:
                config = load_config()
                state = read_json(STATE_PATH, {"mode": "paper", "running": False, "position": None})
            self.send_json({"ok": True, "app": "GoldPairLocal", "host": HOST, "port": PORT, "time_ms": now_ms(), "capabilities": {"mt5_native": sys.platform == 'win32', "mt5_paper": True, "binance_testnet": False, "real_orders": False, "paired_live_orders": True, "paper_engine": True}, "config": {"mode": config.get("mode"), "symbol": config.get("symbol")}, "state": state, "config_errors": validate_config(config)})
            return
        if path == "/api/config":
            with LOCK:
                config = load_config()
            safe = json.loads(json.dumps(config))
            safe["binance"]["api_key_configured"] = bool(safe["binance"].get("api_key"))
            safe["binance"]["api_secret_configured"] = bool(safe["binance"].get("api_secret"))
            self.send_json({"ok": True, "config": safe, "errors": validate_config(config)})
            return
        if path == "/api/trading/status":
            self.send_json({"ok": True, **TRADING.snapshot()})
            return
        if path == '/api/trading/diagnostics':
            if not DIAGNOSTIC_EXPORT_LOCK.acquire(blocking=False):
                self.send_json({'ok':False,'error':'已有诊断导出正在生成，请稍后重试'},429);return
            try:
                try:
                    data=json.dumps(export_report(TRADING_DB_PATH),ensure_ascii=False,allow_nan=False).encode('utf-8')
                except Exception:
                    self.send_json({'ok':False,'error':'执行诊断暂时无法读取，请稍后重试'},503);return
                self.send_response(200)
                self.send_header('Content-Type','application/json; charset=utf-8')
                self.send_header('Content-Disposition','attachment; filename="execution-diagnostics-'+datetime.now().strftime('%Y%m%d-%H%M%S')+'.json"')
                self.send_header('Cache-Control','no-store')
                self.send_header('Content-Length',str(len(data)))
                self.end_headers();self.wfile.write(data)
            finally:
                DIAGNOSTIC_EXPORT_LOCK.release()
            return
        if path == "/api/trading/chart":
            query = urlparse(self.path).query
            try:
                minutes = max(1, min(43200, int(next((x.split('=', 1)[1] for x in query.split('&') if x.startswith('minutes=')), '1440'))))
            except ValueError:
                self.send_json({'ok': False, 'error': 'minutes 参数无效'}, 400)
                return
            self.send_json({'ok': True, 'samples': TRADING.chart(now_ms() - minutes * 60000)})
            return
        self.send_json({"ok": False, "error": "未找到"}, 404)

    def do_POST(self):
        global LAST_PROBE
        if not self.local_request(mutation=True):
            return
        path = urlparse(self.path).path
        try:
            body = self.read_body()
            if path == '/api/mt5/push/setup':
                self.send_json({'ok':True, **TRADING.push_setup()});return
            if path == '/api/mt5/check':
                if not PROBE_LOCK.acquire(blocking=False):
                    self.send_json({'ok': False, 'error': 'MT5 检查正在进行，请等待结果'}, 409)
                    return
                try:
                    with LOCK:
                        settings = load_config()['mt5']
                    LAST_PROBE = None
                    token = str(body.get('mcp_token') or settings.get('mcp_token', ''))
                    if settings.get('adapter', 'native') == 'paper':
                        result = inspect_paper_terminal(settings)
                    elif settings.get('adapter') == 'mcp':
                        result = inspect_mcp_terminal(settings, token)
                    else:
                        result = run_probe(settings)
                    with LOCK:
                        LAST_PROBE = dict(key=connection_key(settings), result=result, at=time.monotonic())
                    self.send_json({'ok': True, 'result': result})
                finally:
                    PROBE_LOCK.release()
                return
            if path == '/api/mt5/apply':
                with LOCK:
                    config = load_config()
                    result = checked_spec(config['mt5'])
                    spec = result['symbol']
                    config['mt5'].update(account=result['identity']['account'], server=result['identity']['server'],
                                         symbol=spec['name'], **{k: spec[k] for k in ('contract_size_oz', 'volume_min', 'volume_step', 'volume_max')})
                    config['mt5'].pop('bridge_url', None)
                    write_config(config)
                    LAST_PROBE['key'] = connection_key(config['mt5'])
                    LAST_PROBE['applied'] = True
                self.send_json({'ok': True, 'mt5': config['mt5']})
                return
            if path == '/api/binance/check':
                with LOCK:
                    config = load_config()
                result = inspect_binance(config, str(body.get('api_key') or config.get('binance', {}).get('api_key', '')),
                                         str(body.get('api_secret') or config.get('binance', {}).get('api_secret', '')))
                self.send_json({'ok': True, 'result': result})
                return
            if path == '/api/binance/public-ip':
                with LOCK:
                    config = load_config()
                self.send_json({'ok': True, 'result': inspect_binance_public_ip(config)})
                return
            if path == "/api/config":
                with LOCK:
                    current = load_config()
                    if body.get('clear_credentials'):
                        current.setdefault('binance', {})['api_key'] = ''
                        current.setdefault('binance', {})['api_secret'] = ''
                        current.setdefault('mt5', {})['mcp_token'] = ''
                    incoming = deep_merge({}, body)
                    incoming.pop('clear_credentials', None)
                    b = incoming.get("binance", {})
                    for name in ('api_key', 'api_secret'):
                        if name in b: b[name] = str(b[name] or '').strip()
                    if 'mt5' in incoming and 'mcp_token' in incoming['mt5']:
                        incoming['mt5']['mcp_token'] = str(incoming['mt5']['mcp_token'] or '').strip()
                    saved = deep_merge(current, incoming)
                    errors = validate_config(saved)
                    if errors:
                        self.send_json({'ok': False, 'errors': errors}, 400)
                        return
                    saved['mt5'].pop('bridge_url', None)
                    write_config(saved)
                self.send_json({"ok": True, "errors": validate_config(saved)})
                save_event({"type": "config_saved", "mode": saved.get("mode")})
                return

            if path == '/api/trading/connect':
                config = load_config()
                result = TRADING.connect(config,
                                         str(body.get('api_key') or config.get('binance', {}).get('api_key', '')),
                                         str(body.get('api_secret') or config.get('binance', {}).get('api_secret', '')),
                                         str(body.get('mt5_mcp_token') or config.get('mt5', {}).get('mcp_token', '')))
                self.send_json({'ok': True, **result})
                return
            if path == '/api/trading/start':
                self.send_json({'ok': True, **TRADING.start(load_config())})
                return
            if path == '/api/trading/pause':
                self.send_json({'ok': True, **TRADING.pause()})
                return
            if path == '/api/trading/exit-rules':
                self.send_json({'ok': True, **TRADING.apply_exit_rules(body)})
                return
            if path == '/api/trading/apply-entry':
                self.send_json({'ok': True, **TRADING.apply_entry_threshold(body.get('entry_spread_usd'))})
                return
            if path == '/api/trading/close':
                group = body.get('group')
                self.send_json({'ok': True, **TRADING.close_group(str(group) if group else None,
                                                                   str(body.get('reason') or '用户请求平仓'))})
                return
            if path == '/api/trading/reconcile':
                self.send_json({'ok': True, **TRADING.reconcile()})
                return
            if path == '/api/trading/adoption/preview':
                self.send_json({'ok': True, **TRADING.adoption_plan(body)})
                return
            if path == '/api/trading/adoption/confirm':
                self.send_json({'ok': True, **TRADING.adoption_confirm(body)})
                return
            if path == '/api/trading/adoption/manage':
                self.send_json({'ok': True, **TRADING.adoption_manage(body)})
                return

            if path == "/api/paper/plan":
                config = load_config()
                errors = validate_config(config)
                if errors:
                    self.send_json({"ok": False, "errors": errors}, 400)
                    return
                with LOCK:
                    plan = order_plan(config, body.get("binance_bid", 0))
                self.send_json({"ok": True, "plan": plan, "checks": {"local_only": True, "real_order_enabled": False, "mode": config.get("mode")}})
                save_event({"type": "paper_plan", "plan": plan})
                return
            if path == "/api/paper/start":
                with LOCK:
                    state = read_json(STATE_PATH, default_state())
                    state["running"] = True
                    state["updated_ms"] = now_ms()
                    STATE_PATH.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
                self.send_json({"ok": True, "state": state, "message": "纸面引擎已启动；只更新模拟持仓，不发送任何订单。"})
                save_event({"type": "paper_started"})
                return
            if path == "/api/paper/reset":
                state = default_state()
                STATE_PATH.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
                save_event({"type": "paper_reset"})
                self.send_json({"ok": True, "state": state})
                return
            if path == "/api/paper/step":
                config = load_config()
                errors = validate_config(config)
                if errors:
                    self.send_json({"ok": False, "errors": errors}, 400)
                    return
                with LOCK:
                    plan = order_plan(config, body.get("binance_bid", 0))
                    state = read_json(STATE_PATH, default_state())
                    result = paper_step(state, config, plan, body)
                    STATE_PATH.write_text(json.dumps(result["state"], ensure_ascii=False, indent=2), encoding="utf-8")
                self.send_json({"ok": True, **result, "checks": {"real_order_enabled": False, "mode": "paper"}})
                save_event({"type": "paper_step", "action": result["action"], "metrics": result["metrics"]})
                return
            if path == "/api/paper/stop":
                state = read_json(STATE_PATH, {"mode": "paper", "position": None})
                state["running"] = False
                state["stopped_at_ms"] = now_ms()
                STATE_PATH.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
                save_event({"type": "paper_stopped"})
                self.send_json({"ok": True, "state": state})
                return
            self.send_json({"ok": False, "error": "未找到"}, 404)
        except Exception as exc:
            self.send_json({"ok": False, "error": str(exc)}, 400)


if __name__ == "__main__":
    stop_legacy_paper_on_boot()
    print(f"黄金双边自动交易：{HOST}:{PORT}（仅本机，默认模拟模式）")
    ThreadingHTTPServer((HOST, PORT), Handler).serve_forever()
