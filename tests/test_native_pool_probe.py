import copy
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
        with patch.object(connector.Path, 'is_file', return_value=True):
            result = connector.inspect_terminal({**settings, **changes}, self.fake)
        self.assertTrue(self.fake.closed)
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
