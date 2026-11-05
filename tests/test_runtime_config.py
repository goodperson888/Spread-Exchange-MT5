import copy
import threading
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

import _test_runtime
from server import TradingRuntime
from test_trading_engine import config


class RuntimeConfigTests(unittest.TestCase):
    def fixture(self):
        runtime=TradingRuntime.__new__(TradingRuntime)
        runtime.config=config();runtime.config['execution']['mode']='live'
        runtime.config['mt5']['adapter']='native'
        runtime.config['costs'].update(usdt_usd_auto=True,binance_fee_auto=True)
        runtime.lock=threading.RLock();runtime.connected=True;runtime.reconciled=False;runtime.quote=None
        runtime.engine=SimpleNamespace(state={'enabled':False,'groups':[],'orders':[]})
        runtime.plan={'lots':.02,'qty':2};runtime.last_error='';runtime.market_meta={}
        runtime.store=SimpleNamespace(events=lambda:[]);runtime.position_report=None
        runtime.entry_preflight=None;runtime.binance=SimpleNamespace(position_mode='hedge')
        return runtime

    def test_auto_cost_updates_do_not_require_reconnection_or_mutate_input(self):
        runtime=self.fixture();saved=copy.deepcopy(runtime.config)
        saved['costs']['usdt_usd']=1.001;saved['costs']['binance_taker_percent']=.02
        before=copy.deepcopy(saved)
        self.assertFalse(runtime.config_changed(saved));self.assertEqual(saved,before)
        saved['strategy']['mt5_lots']=.01
        self.assertTrue(runtime.config_changed(saved))

    def test_snapshot_keeps_effective_plan_when_saved_quantity_changes(self):
        runtime=self.fixture();saved=copy.deepcopy(runtime.config)
        saved['strategy']['mt5_lots']=.01
        saved['binance'].update(api_key='never-display-key',api_secret='never-display-secret')
        with patch('server.load_config',return_value=saved):
            result=runtime.snapshot()
        self.assertTrue(result['config_pending']);self.assertEqual(result['plan']['lots'],.02)
        self.assertNotIn('never-display',str(result))

    def test_risk_thresholds_apply_without_reconnecting_or_network_calls(self):
        runtime=self.fixture();saved=copy.deepcopy(runtime.config)
        saved['risk_alerts']={'binance_warning_distance_percent':8,'mt5_warning_margin_level_percent':300}
        self.assertFalse(runtime.config_changed(saved))
        monitor=SimpleNamespace(status=Mock(return_value={'level':'warning'}))
        runtime.risk_monitor=monitor
        runtime._last_mt5_snapshot={'account':{'equity':1000,'margin':100,'margin_so_mode':0,'margin_so_call':100,'margin_so_so':50}}
        import time
        runtime._last_mt5_snapshot_at=time.monotonic()
        with patch('server.load_config',return_value=saved): result=runtime.snapshot()
        self.assertFalse(result['config_pending'])
        self.assertEqual(result['liquidation_risk']['alerts']['mt5_warning_margin_level_percent'],300)
        self.assertEqual(result['liquidation_risk']['mt5']['level'],'normal')
        monitor.status.assert_called_once()

    def test_reconnecting_an_enabled_engine_is_rejected_before_network_calls(self):
        runtime=self.fixture();runtime.engine.state['enabled']=True
        with patch('server.Binance',Mock()) as client:
            with self.assertRaisesRegex(ValueError,'先暂停新开仓'):
                runtime.connect(runtime.config,'test-key','test-secret')
            client.assert_not_called()


if __name__=='__main__':unittest.main()
