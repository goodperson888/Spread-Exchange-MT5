import copy
import json
import sys
import tempfile
import threading
import time
import unittest
import urllib.error
import urllib.request
from pathlib import Path
from types import SimpleNamespace as NS
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import mt5_connector as connector
import mt5_mcp
import server


class FakeMT5:
    def __init__(self):
        self.terminal = NS(connected=True, trade_allowed=True, tradeapi_disabled=False, path='C:/MT5')
        self.account = NS(login=123, server='Demo', currency='USD', margin_mode=2, trade_allowed=True, trade_expert=True)
        self.symbol = NS(name='XAUUSDm', currency_base='XAU', currency_profit='USD', custom=False,
                         trade_contract_size=100, volume_min=.01, volume_step=.01, volume_max=10,
                         digits=2, trade_mode=4)
        self.symbols = [self.symbol]
        self.tick = NS(bid=4300, ask=4300.2, time_msc=int(time.time()*1000))
        self.closed = False

    def initialize(self, *args, **kwargs):
        assert 'login' not in kwargs and 'password' not in kwargs  # never switch accounts
        return True

    def terminal_info(self): return self.terminal
    def account_info(self): return self.account
    def symbols_get(self): return self.symbols
    def symbol_info(self, name): return next((s for s in self.symbols if s.name == name), None)
    def symbol_select(self, name, enable): return True
    def symbol_info_tick(self, name): return self.tick
    def shutdown(self): self.closed = True


class ConnectorTests(unittest.TestCase):
    def test_legacy_paper_does_not_resume_after_restart(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'state.json'
            path.write_text(json.dumps({'running': True, 'groups': [{'id': 1}]}), encoding='utf-8')
            state = server.stop_legacy_paper_on_boot(path)
            self.assertFalse(state['running'])
            self.assertEqual(state['groups'], [{'id': 1}])
            self.assertFalse(json.loads(path.read_text(encoding='utf-8'))['running'])

    def test_auto_detect_and_spec(self):
        fake = FakeMT5()
        r = connector.inspect_terminal({}, fake)
        self.assertEqual(r['symbol']['contract_size_oz'], 100)
        self.assertEqual(r['identity']['account'], '123')
        self.assertEqual(r['blockers'], [])
        self.assertFalse(r['real_order_enabled'])
        self.assertTrue(fake.closed)

    def test_mismatched_account_and_server(self):
        for settings in ({'account':'456'}, {'server':'Live'}):
            r = connector.inspect_terminal(settings, FakeMT5())
            self.assertEqual(r['code'], 'ACCOUNT_MISMATCH')
            self.assertIsNone(r['symbol'])

    def test_multiple_gold_requires_selection(self):
        fake = FakeMT5()
        other = copy.copy(fake.symbol); other.name = 'XAUUSD'
        fake.symbols.append(other)
        self.assertEqual(connector.inspect_terminal({}, fake)['code'], 'SELECT_SYMBOL')

    def test_non_gold_and_missing_spec(self):
        for field, value in (('currency_base','EUR'), ('trade_contract_size',float('nan')), ('volume_step',0)):
            fake = FakeMT5(); setattr(fake.symbol, field, value)
            self.assertIsNone(connector.inspect_terminal({'symbol':'XAUUSDm'}, fake)['symbol'])

    def test_permissions_and_stale_quote(self):
        fake = FakeMT5(); fake.terminal.tradeapi_disabled = True
        fake.account.trade_allowed = False; fake.account.margin_mode = 0
        fake.tick.time_msc -= 60000
        r = connector.inspect_terminal({}, fake)
        self.assertTrue(r['connected'])
        self.assertEqual(len(r['blockers']), 4)

    def test_disconnected(self):
        fake = FakeMT5(); fake.terminal.connected = False
        self.assertFalse(connector.inspect_terminal({}, fake)['connected'])
        self.assertTrue(fake.closed)

    def test_unsupported_os(self):
        self.assertEqual(connector.inspect_terminal({}, platform='darwin')['code'], 'WINDOWS_REQUIRED')

    def test_timeout(self):
        import subprocess
        with patch.object(connector.sys, 'platform', 'win32'), patch.object(connector.subprocess, 'CREATE_NO_WINDOW', 0, create=True), patch.object(connector.subprocess, 'run', side_effect=subprocess.TimeoutExpired('probe', 18)):
            self.assertEqual(connector.run_probe({})['code'], 'PROBE_TIMEOUT')

    def test_mcp_real_quote_adapter_is_read_only(self):
        class FakeMcp:
            def __init__(self): self.calls = []
            def call_tool(self, name, arguments=None):
                self.calls.append((name, arguments or {}))
                if name == 'get_trading_account_info':
                    return {'account': {'login': 123, 'server': 'Demo', 'currency': 'USD', 'margin_mode': 'hedging'},
                            'terminal': {'server_connected': True}}
                return {'symbols': [{'symbol':'XAUUSDm','currency_base':'XAU','currency_profit':'USD',
                    'trade_mode':4,'contract_size':100,'volume_min':.01,'volume_step':.01,'volume_max':200,
                    'digits':3,'bid':4357.1,'ask':4357.3,'update_time':time.strftime('%Y.%m.%d %H:%M:%S', time.gmtime())}]}
            def close(self): pass
        client = FakeMcp()
        result = mt5_mcp.inspect_mcp_terminal({'symbol':'XAUUSDm'}, client=client)
        self.assertTrue(result['connected'])
        self.assertEqual(result['symbol']['contract_size_oz'], 100)
        self.assertEqual(result['quote']['bid'], 4357.1)
        self.assertFalse(result['real_order_enabled'])
        self.assertEqual([x[0] for x in client.calls], ['get_trading_account_info', 'get_marketwatch_symbols'])
        self.assertFalse(any(name.startswith('trade_') for name, _ in client.calls))

    def test_mcp_rejects_non_local_url(self):
        with self.assertRaises(ValueError):
            mt5_mcp.validate_mcp_url('https://example.com/mcp')


class HttpTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        data = Path(self.temp.name)
        self.patches = [patch.object(server, name, value) for name, value in {
            'DATA':data, 'CONFIG_PATH':data/'config.json', 'STATE_PATH':data/'state.json',
            'EVENTS_PATH':data/'events.jsonl', 'LAST_PROBE':None,
        }.items()]
        for p in self.patches: p.start()
        self.http = server.ThreadingHTTPServer(('127.0.0.1', 0), server.Handler)
        self.thread = threading.Thread(target=self.http.serve_forever, daemon=True)
        self.thread.start()
        self.url = 'http://127.0.0.1:' + str(self.http.server_port)

    def tearDown(self):
        self.http.shutdown(); self.http.server_close(); self.thread.join()
        for p in reversed(self.patches): p.stop()
        self.temp.cleanup()

    def request(self, path, body=None, headers=None):
        req = urllib.request.Request(self.url+path, data=json.dumps(body).encode() if body is not None else None,
                                     headers=headers if headers is not None else {'Content-Type':'application/json','X-Local-App':'GoldPairLocal'})
        try:
            with urllib.request.urlopen(req) as r: return r.status, json.load(r)
        except urllib.error.HTTPError as e: return e.code, json.load(e)

    def test_check_apply_plan_and_restart(self):
        self.request('/api/config', {'mt5': {'adapter': 'native'}})
        result = connector.inspect_terminal({}, FakeMT5())
        with patch.object(server, 'run_probe', return_value=result):
            self.assertEqual(self.request('/api/mt5/check', {})[0], 200)
        self.assertEqual(self.request('/api/paper/plan', {})[0], 400)
        code, body = self.request('/api/mt5/apply', {})
        self.assertEqual(code, 200); self.assertEqual(body['mt5']['account'], '123')
        code, body = self.request('/api/paper/plan', {'binance_bid':4300})
        self.assertEqual(code, 200); self.assertEqual(body['plan']['binance_notional_usdt'], 8600)
        server.LAST_PROBE = None
        self.assertEqual(self.request('/api/paper/plan', {})[0], 400)
        self.assertEqual(self.request('/api/config')[1]['config']['mt5']['account'], '123')

    def test_changed_account_invalidates_check(self):
        self.request('/api/config', {'mt5': {'adapter': 'native'}})
        result = connector.inspect_terminal({}, FakeMT5())
        with patch.object(server, 'run_probe', return_value=result): self.request('/api/mt5/check', {})
        self.request('/api/config', {'mt5':{'account':'456'}})
        self.assertEqual(self.request('/api/mt5/apply', {})[0], 400)

    def test_invalid_lot_and_expired_specs(self):
        self.request('/api/config', {'mt5': {'adapter': 'native'}})
        result = connector.inspect_terminal({}, FakeMT5())
        with patch.object(server, 'run_probe', return_value=result): self.request('/api/mt5/check', {})
        self.request('/api/mt5/apply', {})
        self.request('/api/config', {'strategy':{'mt5_lots':.025}})
        self.assertEqual(self.request('/api/paper/plan', {})[0], 400)
        self.request('/api/config', {'strategy':{'mt5_lots':.02}})
        server.LAST_PROBE['at'] -= 301
        self.assertEqual(self.request('/api/paper/plan', {})[0], 400)

    def test_api_no_false_start_or_live_mode(self):
        code, result = self.request('/api/paper/start', {})
        self.assertEqual(code, 200)
        self.assertTrue(result['state']['running'])
        self.assertFalse(self.request('/api/status')[1]['capabilities']['real_orders'])
        self.assertEqual(self.request('/api/config', {'mode':'live'})[0], 400)

    def test_persisted_secrets_and_clear(self):
        self.request('/api/config', {'binance':{'api_key':'test-key', 'api_secret':'test-secret'},
                                     'mt5':{'mcp_token':'mcp-secret'}})
        response = self.request('/api/config')[1]
        self.assertEqual(response['config']['binance']['api_key'], 'test-key')
        self.assertEqual(response['config']['binance']['api_secret'], 'test-secret')
        self.assertEqual(response['config']['mt5']['mcp_token'], 'mcp-secret')
        self.assertTrue(response['config']['binance']['api_key_configured'])
        self.request('/api/config', {'clear_credentials': True})
        self.assertEqual(server.load_config()['binance']['api_secret'], '')
        self.assertEqual(server.load_config()['mt5']['mcp_token'], '')

    def test_binance_check_is_read_only(self):
        result = {'message':'ok','orders_sent':False,'quote':{'bid':4300,'ask':4301}}
        with patch.object(server, 'inspect_binance', return_value=result) as inspect:
            code, body = self.request('/api/binance/check', {'api_key':'session-key','api_secret':'session-secret'})
        self.assertEqual(code, 200)
        self.assertFalse(body['result']['orders_sent'])
        inspect.assert_called_once()
        self.assertEqual(server.load_config()['binance']['api_secret'], '')

    def test_binance_public_ip_endpoint(self):
        with patch.object(server, 'inspect_binance_public_ip', return_value={
                'ip':'203.0.113.8','route':'proxy','route_label':'经配置代理','current_only':True}) as inspect:
            code, body = self.request('/api/binance/public-ip', {})
        self.assertEqual(code, 200)
        self.assertEqual(body['result']['ip'], '203.0.113.8')
        inspect.assert_called_once()

    def test_inspect_binance_checks_market_and_live_permissions(self):
        instances = []
        class FakeBinance:
            def __init__(self, **kwargs): self.kwargs=kwargs; instances.append(self)
            def sync(self): pass
            def spec(self, symbol): return {'symbol':symbol,'status':'TRADING','baseAsset':'XAU','quoteAsset':'USDT'}
            def quote(self, symbol): return {'bid':4300.,'ask':4300.2,'time_ms':123}
            def usdt_usd(self): return {'value':.9998,'time_ms':123,'source':'test'}
            def api_permissions(self): return {'enable_futures':True,'enable_reading':True,'ip_restricted':True}
            def preflight(self, permissions=None):
                self.permissions=permissions
                return {'can_trade':True,'available':1000.,'wallet':1200.}
            def commission_rate(self, symbol): return {'maker':.01,'taker':.04}
        class FakeStream:
            def start(self): return self
            def quote(self, **kwargs): return {'bid':4300.,'ask':4300.2,'time_ms':123}
            def close(self): pass
        c=copy.deepcopy(server.load_config());c['execution']['mode']='live';c['mt5']['adapter']='native'
        c['binance']['proxy_url']='http://127.0.0.1:7890'
        with patch.object(server,'Binance',FakeBinance), patch.object(server,'BinanceBookTicker',return_value=FakeStream()) as stream:
            with self.assertRaises(ValueError): server.inspect_binance(c)
            result=server.inspect_binance(c,'key','secret')
        self.assertFalse(result['orders_sent'])
        self.assertEqual(result['fees']['taker'],.04)
        self.assertTrue(result['permissions']['enable_futures'])
        self.assertTrue(instances[-1].permissions['enable_futures'])
        self.assertEqual(result['transport'],'WebSocket bookTicker')
        self.assertEqual(instances[-1].kwargs['proxy_url'],'http://127.0.0.1:7890')
        stream.assert_called_with('XAUUSDT',production=True,proxy_url='http://127.0.0.1:7890')

    def test_binance_proxy_validation(self):
        c=copy.deepcopy(server.load_config())
        for invalid in ('socks5://127.0.0.1:7890','http://user:pass@127.0.0.1:7890','http://127.0.0.1'):
            c['binance']['proxy_url']=invalid
            self.assertTrue(any('代理' in error for error in server.validate_config(c)))
        c['binance']['proxy_url']='http://127.0.0.1:7890'
        self.assertFalse(any('代理' in error for error in server.validate_config(c)))

    def test_paper_open_close_and_stop(self):
        self.request('/api/config', {'mt5': {'adapter':'paper', 'account':'PAPER', 'server':'test',
                                              'symbol':'XAUUSD.paper', 'paper_bid':4300, 'paper_ask':4300.3}})
        self.request('/api/mt5/check', {})
        self.assertEqual(self.request('/api/mt5/apply', {})[0], 200)
        quote = dict(binance_bid=4306, binance_ask=4306.1, mt5_bid=4300, mt5_ask=4300.3)
        self.assertEqual(self.request('/api/paper/step', quote)[0], 400)
        self.request('/api/paper/start', {})
        code, opened = self.request('/api/paper/step', quote)
        self.assertEqual(code, 200)
        self.assertEqual(opened['action'], 'open_paper_group')
        self.assertAlmostEqual(opened['state']['groups'][0]['gross_pnl_usdt'], -.8)
        quote.update(binance_bid=4302.5, binance_ask=4302.6)
        code, closed = self.request('/api/paper/step', quote)
        self.assertEqual(code, 200)
        self.assertEqual(closed['action'], 'close_paper_group')
        self.assertAlmostEqual(closed['closed'][0]['gross_pnl_usdt'], 6.2)
        self.assertEqual(closed['state']['groups'], [])
        self.request('/api/paper/stop', {})
        self.assertEqual(self.request('/api/paper/step', quote)[0], 400)

    def test_cross_site_mutation_and_host_rejected(self):
        self.assertEqual(self.request('/api/config', {}, headers={'Content-Type':'application/json'})[0], 403)
        self.assertEqual(self.request('/api/config', {}, headers={'X-Local-App':'GoldPairLocal','Origin':'https://example.com'})[0], 403)
        self.assertEqual(self.request('/api/config', headers={'Host':'evil.test'})[0], 403)


if __name__ == '__main__': unittest.main()
