import copy
import json
import threading
import unittest
from types import SimpleNamespace as NS
from unittest.mock import Mock, patch

import _test_runtime
import server
import test_connection


class PairProfilesHttpTests(unittest.TestCase):
    request=test_connection.HttpTests.request

    def setUp(self):
        test_connection.HttpTests.setUp(self)
        self.runtime=NS(lock=threading.RLock(),engine=NS(state={'enabled':False,'recovery':False},active=Mock(return_value=[])),
                        _disconnect_pair_resources=Mock(),config=None,plan=None,quote=None,position_report=None,adoption_preview=None)
        self.runtime_patch=patch.object(server,'TRADING',self.runtime);self.runtime_patch.start()

    def tearDown(self):
        self.runtime_patch.stop();test_connection.HttpTests.tearDown(self)

    def save(self,**fields):
        return self.request('/api/pairs/save',dict(name='同终端测试',type='mt5_mt5',short_symbol='GOLD-FUT',long_symbol='GOLD-SPOT',**fields))

    def test_presets_save_without_activation_and_share_only_account_config(self):
        config=server.load_config();config['binance'].update(api_key='SECRET-KEY',api_secret='SECRET-TOKEN')
        config['mt5']['account']='12345';config['mt5']['mcp_token']='PRIVATE-TOKEN';server.write_config(config)
        code,result=self.save();self.assertEqual(code,200,result)
        self.assertEqual(result['active_id'],'legacy');self.assertEqual(server.load_config(),config)
        text=json.dumps(result)+ (server.DATA/'combinations.json').read_text(encoding='utf-8')
        for secret in ('SECRET-KEY','SECRET-TOKEN','PRIVATE-TOKEN','12345'):self.assertNotIn(secret,text)

    def test_activation_persists_pair_and_retains_credentials_without_arming(self):
        config=server.load_config();config['binance']['api_key']='local-key';server.write_config(config)
        _,result=self.save();profile=next(p for p in result['profiles'] if p['id']!='legacy')
        code,result=self.request('/api/pairs/activate',{'id':profile['id']});self.assertEqual(code,200,result)
        saved=server.load_config();self.assertEqual(saved['pair']['id'],profile['id'])
        self.assertEqual(saved['symbol'],'GOLD-FUT');self.assertEqual(saved['mt5']['symbol'],'GOLD-SPOT')
        self.assertEqual(saved['mt5']['adapter'],'native');self.assertEqual(saved['binance']['api_key'],'local-key')
        self.assertFalse(self.runtime.engine.state['enabled']);self.runtime._disconnect_pair_resources.assert_called_once()
        self.assertEqual(self.request('/api/pairs')[1]['active_id'],profile['id'])

    def test_active_or_uncertain_groups_block_switch_without_writing_config(self):
        _,result=self.save();selected=next(p['id'] for p in result['profiles'] if p['id']!='legacy')
        original=server.load_config()
        for enabled,recovery,groups in ((True,False,[]),(False,True,[]),(False,False,[{'status':'open'}])):
            self.runtime.engine.state.update(enabled=enabled,recovery=recovery);self.runtime.engine.active.return_value=groups
            code,_=self.request('/api/pairs/activate',{'id':selected});self.assertEqual(code,400)
            self.assertEqual(server.load_config(),original)
        self.runtime._disconnect_pair_resources.assert_not_called()

    def test_edit_inactive_preset_preserves_its_parameters_and_magic(self):
        _,result=self.save();profile=next(p for p in result['profiles'] if p['id']!='legacy')
        config=server.load_config();config['strategy']['mt5_lots']=.03;server.write_config(config)
        code,result=self.save(id=profile['id']);self.assertEqual(code,200,result)
        edited=next(p for p in result['profiles'] if p['id']==profile['id'])
        for key in ('strategy','execution','costs'):self.assertEqual(edited[key],profile[key])

    def test_validation_failure_keeps_config_and_saved_presets(self):
        _,result=self.save();selected=next(p for p in result['profiles'] if p['id']!='legacy')
        original=copy.deepcopy(server.load_config());rows=(server.DATA/'combinations.json').read_bytes()
        with patch.object(server,'validate_config',return_value=['invalid test setting']):
            code,_=self.request('/api/pairs/activate',{'id':selected['id']});self.assertEqual(code,400)
        self.assertEqual(server.load_config(),original);self.assertEqual((server.DATA/'combinations.json').read_bytes(),rows)


if __name__=='__main__':unittest.main()
