import copy
import unittest
from unittest.mock import patch
import server
import test_pair_profiles_http as profiles_http
from test_mt5_pool import config
from trading_config import pair_key
from mt5_pool_config import candidates


class PoolHttpTests(unittest.TestCase):
    request=profiles_http.PairProfilesHttpTests.request
    setUp=profiles_http.PairProfilesHttpTests.setUp
    tearDown=profiles_http.PairProfilesHttpTests.tearDown
    def install(self):
        c=config(2);server.write_config(c);return server.load_config()
    def test_create_pool_and_activate_does_not_start_trading(self):
        code,result=self.request('/api/pairs/save',dict(name='多终端',type='mt5_mt5',mode='pool',short_symbol='XAUUSD',long_symbol='XAUUSD'))
        self.assertEqual(code,200,result)
        code,result=self.request('/api/pairs/activate',dict(id=result['saved_id']))
        self.assertEqual(code,200,result);self.assertFalse(self.runtime.engine.state['enabled'])
        self.assertEqual(server.load_config()['pair']['mode'],'pool')

    def test_renaming_inactive_pool_retains_its_own_endpoints_and_rules(self):
        c=config(4);server.write_config(c)
        code,result=self.request('/api/pairs/save',dict(name='新池',type='mt5_mt5',mode='pool',short_symbol='XAUUSD',long_symbol='XAUUSD'))
        self.assertEqual(code,200,result);saved=result['saved_id']
        p=next(x for x in result['profiles'] if x['id']==saved)
        self.assertEqual(len(p['mt5_pool']['endpoints']),2)
        code,result=self.request('/api/pairs/save',dict(id=saved,name='改名',type='mt5_mt5',mode='pool',short_symbol='XAUUSD',long_symbol='XAUUSD'))
        self.assertEqual(code,200,result)
        self.assertEqual(next(x for x in result['profiles'] if x['id']==saved)['mt5_pool'],p['mt5_pool'])
        self.assertEqual(len(server.load_config()['mt5_pool']['endpoints']),4)

    def test_selected_rules_save_atomically_without_overwriting_other_pairs(self):
        c=config(4);server.write_config(c);pool=copy.deepcopy(c['mt5_pool'])
        chosen={'t1~t2','t1~t4'}
        for key in chosen:pool['rules'][key]['entry_spread_usd']=7.5
        code,result=self.request('/api/mt5/pool/save',dict(expected_pair_id=c['pair']['id'],pool=pool))
        self.assertEqual(code,200,result);saved=server.load_config()['mt5_pool']
        for key,rule in saved['rules'].items():
            self.assertEqual(rule['entry_spread_usd'],7.5 if key in chosen else 2)
        bad=copy.deepcopy(saved);bad['rules']['t1~t2']['entry_spread_usd']=9
        bad['rules']['t1~t4']['min_net_profit_usd']='invalid'
        code,_=self.request('/api/mt5/pool/save',dict(expected_pair_id=c['pair']['id'],pool=bad))
        self.assertEqual(code,400);self.assertEqual(server.load_config()['mt5_pool'],saved)
    def test_invalid_or_stale_form_cannot_replace_terminal_pool(self):
        c=self.install();pool=copy.deepcopy(c['mt5_pool']);pool['rules']['t1~t2']['entry_lower_usd']=100
        code,_=self.request('/api/mt5/pool/save',dict(expected_pair_id=c['pair']['id'],pool=pool))
        self.assertEqual(code,400);self.assertEqual(server.load_config(),c)
        code,_=self.request('/api/mt5/pool/save',dict(expected_pair_id='other',pool=c['mt5_pool']))
        self.assertEqual(code,400);self.assertEqual(server.load_config(),c)
    def test_owned_endpoint_cannot_be_rebound_but_new_rules_can_be_saved(self):
        c=self.install();fixed=candidates(c)['t1~t2']
        self.runtime.engine.active.return_value=[dict(key=pair_key(fixed))]
        pool=copy.deepcopy(c['mt5_pool']);pool['endpoints'][0]['account']='99999'
        code,_=self.request('/api/mt5/pool/save',dict(expected_pair_id=c['pair']['id'],pool=pool));self.assertEqual(code,400)
        pool=copy.deepcopy(c['mt5_pool']);pool['rules']['t1~t2']['entry_spread_usd']=6
        code,result=self.request('/api/mt5/pool/save',dict(expected_pair_id=c['pair']['id'],pool=pool));self.assertEqual(code,200,result)
        self.assertEqual(server.load_config()['mt5_pool']['rules']['t1~t2']['entry_spread_usd'],6)
    def test_check_only_applies_successful_matching_identity(self):
        c=self.install();e=c['mt5_pool']['endpoints'][0]
        with patch.object(server,'run_probe',return_value=dict(connected=True,identity_matches=False,message='身份不符')):
            code,result=self.request('/api/mt5/pool/check',dict(expected_pair_id=c['pair']['id'],endpoint_id=e['id']))
        self.assertEqual(code,200,result);self.assertFalse(result['applied']);self.assertEqual(server.load_config(),c)
        c['mt5_pool']['endpoints'][0].update(account='',server='');server.write_config(c)
        probe=dict(connected=True,identity_matches=True,endpoint_ready=True,blockers=[],identity=dict(account='123456',server='Demo-A'),symbol=dict(name='XAUUSD'))
        with patch.object(server,'run_probe',return_value=probe):
            code,result=self.request('/api/mt5/pool/check',dict(expected_pair_id=c['pair']['id'],endpoint_id=e['id']))
        self.assertEqual(code,200,result);self.assertTrue(result['applied'])
        self.assertEqual(server.load_config()['mt5_pool']['endpoints'][0]['account'],'123456')
    def test_pool_check_uses_real_single_endpoint_probe_and_current_lots(self):
        import mt5_connector as connector
        from test_native_pair_probe import PairTerminal
        c=self.install();e=c['mt5_pool']['endpoints'][1]
        e.update(account='',server='',symbol='XAUUSD.s')
        server.write_config(c);fake=PairTerminal();fake.symbols[1].name='XAUUSD.s'
        def probe(settings):
            self.assertTrue(settings['pool_endpoint_check'])
            self.assertNotIn('pair_symbols',settings)
            self.assertEqual(settings['pair_lots'],c['strategy']['mt5_lots'])
            self.assertEqual(settings['pair_execution_mode'],c['execution']['mode'])
            self.assertEqual(settings['terminal_path'],e['terminal_path'])
            return connector.inspect_terminal(settings,fake)
        with patch.object(server,'run_probe',side_effect=probe),patch.object(connector.Path,'is_file',return_value=True):
            code,result=self.request('/api/mt5/pool/check',dict(expected_pair_id=c['pair']['id'],endpoint_id=e['id']))
        self.assertEqual(code,200,result);self.assertTrue(result['applied'],result)
        self.assertEqual(result['result']['symbol']['name'],'XAUUSD.s')
        self.assertEqual(server.load_config()['mt5_pool']['endpoints'][1]['account'],'123')
        self.assertEqual(server.load_config()['mt5_pool']['endpoints'][0],c['mt5_pool']['endpoints'][0])
        fake.symbol_info.assert_called_once_with('XAUUSD.s')
        fake.order_send.assert_not_called();fake.order_check.assert_not_called();fake.order_calc_margin.assert_not_called()
    def test_incomplete_endpoint_validation_does_not_apply_identity(self):
        c=self.install();e=c['mt5_pool']['endpoints'][0]
        for extra in ({},{'endpoint_ready':False},{'endpoint_ready':True,'blockers':['权限未开启']}):
            probe=dict(connected=True,identity_matches=True,identity=dict(account='123456',server='Demo-A'),symbol=dict(name='XAUUSD'),**extra)
            with patch.object(server,'run_probe',return_value=probe):
                code,result=self.request('/api/mt5/pool/check',dict(expected_pair_id=c['pair']['id'],endpoint_id=e['id']))
            self.assertEqual(code,200,result);self.assertFalse(result['applied']);self.assertEqual(server.load_config(),c)
    def test_view_switch_requires_connected_pool_and_never_changes_saved_strategy(self):
        c=self.install();code,_=self.request('/api/mt5/pool/chart',dict(edge='t1~t2'))
        self.assertEqual(code,400);self.assertEqual(server.load_config(),c)
