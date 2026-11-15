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

    def activate_native(self):
        _, result = self.save()
        selected = next(p['id'] for p in result['profiles'] if p['id'] != 'legacy')
        self.assertEqual(self.request('/api/pairs/activate', {'id':selected})[0], 200)
        self.assertEqual(self.request('/api/config', {'mt5':{'adapter':'native'}, 'strategy':{'mt5_lots':.1}})[0], 200)

    def test_native_check_uses_one_bounded_probe_for_both_symbols_and_current_lots(self):
        from test_native_pair_probe import PairTerminal
        self.activate_native()
        with patch.object(server, 'run_probe', side_effect=lambda s: test_connection.connector.inspect_terminal(s, PairTerminal())) as probe, \
             patch.object(server, 'Terminal') as terminal:
            code, body = self.request('/api/mt5/check', {})
        self.assertEqual(code, 200); self.assertTrue(body['result']['pair_ready'], body)
        probe.assert_called_once(); terminal.assert_not_called()
        args = probe.call_args.args[0]
        self.assertEqual(args['pair_symbols'], ['GOLD-FUT','GOLD-SPOT']); self.assertEqual(args['pair_lots'], .1)
        self.assertEqual(self.request('/api/mt5/apply', {})[0], 200)
        saved = server.load_config()
        self.assertEqual(saved['mt5']['account'], '123'); self.assertEqual(saved['pair']['short_symbol'], 'GOLD-FUT')
        self.assertEqual(saved['mt5']['contract_size_oz'], 10)

    def test_failed_either_leg_cannot_apply_only_the_other_leg(self):
        from test_native_pair_probe import PairTerminal
        self.activate_native(); fake = PairTerminal(); fake.symbols.pop(0)
        with patch.object(server, 'run_probe', side_effect=lambda s: test_connection.connector.inspect_terminal(s, fake)):
            code, body = self.request('/api/mt5/check', {})
        self.assertEqual(code, 200); self.assertFalse(body['result']['pair_ready'])
        self.assertTrue(body['result']['pair_checks']['mt5']['passed'])
        before = server.load_config()
        self.assertEqual(self.request('/api/mt5/apply', {})[0], 400)
        self.assertEqual(server.load_config(), before)

    def test_short_symbol_or_lots_change_invalidates_native_probe(self):
        from test_native_pair_probe import PairTerminal
        self.activate_native()
        for change in ('short','lots','mode'):
            with patch.object(server, 'run_probe', side_effect=lambda s: test_connection.connector.inspect_terminal(s, PairTerminal())):
                self.request('/api/mt5/check', {})
            original = server.load_config(); changed = copy.deepcopy(original)
            if change == 'short': changed['pair']['short_symbol'] = 'GOLD-OTHER'; changed['symbol'] = 'GOLD-OTHER'
            elif change == 'lots': changed['strategy']['mt5_lots'] = .2
            else: changed['execution']['mode'] = 'live' if original['execution']['mode'] == 'paper' else 'paper'
            server.write_config(changed)
            self.assertEqual(self.request('/api/mt5/apply', {})[0], 400, change)
            server.write_config(original)

    def test_native_pair_never_treats_legacy_paper_probe_as_dual_symbol_check(self):
        self.activate_native()
        self.assertEqual(self.request('/api/config', {'mt5':{'adapter':'paper'}})[0], 200)
        with patch.object(server, 'run_probe') as native, patch.object(server, 'inspect_paper_terminal') as paper:
            code, body = self.request('/api/mt5/check', {})
        self.assertEqual(code, 200); self.assertFalse(body['result']['pair_ready'])
        self.assertIn('Windows 原生', body['result']['message'])
        native.assert_not_called(); paper.assert_not_called()
        self.assertEqual(self.request('/api/mt5/apply', {})[0], 400)


if __name__=='__main__':unittest.main()
