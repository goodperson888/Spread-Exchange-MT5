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
        probe=dict(connected=True,identity_matches=True,identity=dict(account='123456',server='Demo-A'),symbol=dict(name='XAUUSD'))
        with patch.object(server,'run_probe',return_value=probe):
            code,result=self.request('/api/mt5/pool/check',dict(expected_pair_id=c['pair']['id'],endpoint_id=e['id']))
        self.assertEqual(code,200,result);self.assertTrue(result['applied'])
        self.assertEqual(server.load_config()['mt5_pool']['endpoints'][0]['account'],'123456')
    def test_view_switch_requires_connected_pool_and_never_changes_saved_strategy(self):
        c=self.install();code,_=self.request('/api/mt5/pool/chart',dict(edge='t1~t2'))
        self.assertEqual(code,400);self.assertEqual(server.load_config(),c)
