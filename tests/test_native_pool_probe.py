import copy
import ntpath
import unittest
from unittest.mock import patch

import mt5_connector as connector
from mt5_pool_config import endpoint_probe_settings, endpoint_settings
from test_mt5_pool import config
from test_native_pair_probe import PairTerminal


class NativePoolProbeTests(unittest.TestCase):
    def setUp(self):
        self.config = config(2)
        self.fake = PairTerminal()
        self.fake.symbols[0].name = 'XAUUSDm'
        self.fake.symbols[0].volume_min = .01
        self.fake.symbols[0].volume_step = .01
        self.fake.symbols[1].name = 'XAUUSD.s'
        for e, name in zip(self.config['mt5_pool']['endpoints'], ('XAUUSDm', 'XAUUSD.s')):
            e.update(symbol=name, account='', server='')
        self.config['execution']['mode'] = 'live'

    def inspect(self, index=0, **changes):
        settings = endpoint_probe_settings(self.config, self.config['mt5_pool']['endpoints'][index])
        self.fake.terminal.path = changes.pop('actual_path', ntpath.dirname(settings['terminal_path']))
        with patch.object(connector.Path, 'is_file', return_value=True):
            result = connector.inspect_terminal({**settings, **changes}, self.fake)
        if changes.get('terminal_path') != '': self.assertTrue(self.fake.closed)
        self.fake.order_send.assert_not_called()
        self.fake.order_check.assert_not_called()
        self.fake.order_calc_margin.assert_not_called()
        return result

    def test_each_endpoint_checks_only_its_own_symbol_and_reads_blank_identity(self):
        for index, name in enumerate(('XAUUSDm', 'XAUUSD.s')):
            with self.subTest(symbol=name):
                self.setUp()
                result = self.inspect(index)
                self.assertTrue(result['endpoint_ready'], result)
                self.assertEqual(result['code'], 'ENDPOINT_CHECKED')
                self.assertEqual(result['symbol']['name'], name)
                self.assertEqual(result['identity']['account'], '123')
                self.assertEqual(result['endpoint_lots'], .01)
                self.assertNotIn('pair_check', result)
                self.fake.symbol_info.assert_called_once_with(name)
                self.fake.symbols_get.assert_not_called()
                self.assertEqual(self.fake.order_calc_profit.call_count, 4)
                self.assertFalse(result['real_order_enabled'])

    def test_wrong_actual_directory_blocks_before_reading_symbols(self):
        result = self.inspect(actual_path=r'C:\MT5-previous')
        self.assertFalse(result['connected'])
        self.assertEqual(result['code'], 'TERMINAL_MISMATCH')
        self.assertFalse(result['terminal_binding']['path_matches'])
        self.assertEqual(result['terminal_binding']['actual_path'], r'C:\MT5-previous')
        self.fake.symbol_info.assert_not_called()
        self.fake.symbols_get.assert_not_called()

    def test_missing_pool_path_never_auto_discovers_an_old_terminal(self):
        from unittest.mock import Mock
        self.fake.initialize = Mock(return_value=True)
        result = self.inspect(terminal_path='')
        self.assertEqual(result['code'], 'TERMINAL_PATH')
        self.fake.initialize.assert_not_called()

    def test_explicit_portable_option_and_failed_initialization_diagnostics(self):
        from unittest.mock import Mock
        self.fake.initialize = Mock(return_value=False)
        self.fake.last_error = Mock(return_value=(-10001, 'private platform text'))
        result = self.inspect(portable=True)
        self.assertEqual(result['code'], 'INITIALIZE_FAILED')
        self.assertIn('本机通信发送失败', result['message'])
        self.assertNotIn('private platform text', str(result))
        self.fake.initialize.assert_called_once_with(self.config['mt5_pool']['endpoints'][0]['terminal_path'], timeout=10000, portable=True)
        self.assertTrue(result['terminal_binding']['portable'])
        self.fake.symbol_info.assert_not_called()

    def test_probe_settings_keep_endpoint_identity_lots_mode_and_worker_scope(self):
        before = copy.deepcopy(self.config)
        self.config['strategy']['mt5_lots'] = .02
        self.config['execution']['mode'] = 'paper'
        e = self.config['mt5_pool']['endpoints'][1]
        settings = endpoint_probe_settings(self.config, e)
        self.assertNotIn('pair_symbols', settings)
        for key in ('terminal_path', 'account', 'server', 'symbol'):
            self.assertEqual(settings[key], e[key])
        self.assertEqual(settings['pair_lots'], .02)
        self.assertEqual(settings['pair_execution_mode'], 'paper')
        self.assertEqual(endpoint_settings(self.config, e)['pair_symbols'], ['XAUUSD.s'])
        self.assertEqual(self.config['mt5_pool'], before['mt5_pool'])

    def test_missing_symbol_reports_endpoint_not_same_terminal_pair(self):
        for name in ('', 'XAUUSDmissing'):
            with self.subTest(symbol=name):
                self.setUp()
                result = self.inspect(symbol=name)
                self.assertFalse(result['endpoint_ready'])
                self.assertIsNone(result['symbol'])
                self.assertNotIn('品种 A', str(result))
                self.assertTrue(result['blockers'])

    def test_lots_permissions_account_and_profit_formula_still_block(self):
        for case in ('lots', 'permissions', 'currency', 'netting', 'formula', 'quote', 'not_gold'):
            with self.subTest(case=case):
                self.setUp()
                changes = {}
                if case == 'lots': changes['pair_lots'] = .015
                if case == 'permissions': self.fake.terminal.trade_allowed = False
                if case == 'currency': self.fake.account.currency = 'EUR'
                if case == 'netting': self.fake.account.margin_mode = 0
                if case == 'formula': self.fake.order_calc_profit.side_effect = lambda *args: 999
                if case == 'quote': self.fake.tick.bid = 0
                if case == 'not_gold':
                    self.fake.symbols[0].name = 'EURUSD'
                    self.fake.symbols[0].currency_base = 'EUR'
                    changes['symbol'] = 'EURUSD'
                result = self.inspect(**changes)
                self.assertFalse(result['endpoint_ready'], result)
                self.assertIsNone(result['symbol'])
                self.assertTrue(result['blockers'])

    def test_paper_permissions_warn_and_identity_mismatch_never_binds(self):
        self.fake.terminal.trade_allowed = False
        result = self.inspect(pair_execution_mode='paper')
        self.assertTrue(result['endpoint_ready'], result)
        self.assertTrue(result['warnings'])
        self.setUp()
        result = self.inspect(account='different')
        self.assertFalse(result['identity_matches'])
        self.assertIsNone(result['symbol'])


if __name__ == '__main__': unittest.main()
