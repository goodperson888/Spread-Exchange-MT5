"""Broker-style USD-base gold CFD validation using read-only MT5 substitutes."""
import copy
import json
import unittest
from unittest.mock import patch

from test_connection import FakeMT5
import mt5_connector as connector


class FakeGoldCFD(FakeMT5):
    ORDER_TYPE_BUY = 0
    ORDER_TYPE_SELL = 1

    def __init__(self):
        super().__init__()
        self.symbol.name = 'XAUUSD'
        self.symbol.description = 'Gold vs US Dollar / Spot'
        self.symbol.currency_base = 'USD'
        self.symbol.currency_margin = 'USD'
        self.symbol.trade_calc_mode = 4
        self.profit_calls = []

    def order_calc_profit(self, action, symbol, lots, opening, closing):
        self.profit_calls.append((action, symbol, lots, opening, closing))
        return (1 if action == self.ORDER_TYPE_BUY else -1) * lots * self.symbol.trade_contract_size * (closing - opening)

    def order_send(self, *args):
        raise AssertionError('Connection checks must never send orders')

    def order_check(self, *args):
        raise AssertionError('Profit calculation needs no order request')


class GoldCFDTests(unittest.TestCase):
    def inspect(self, fake, automatic=False):
        return connector.inspect_terminal({} if automatic else {'symbol':fake.symbol.name}, fake)

    def test_screenshot_style_metadata_passes_four_profit_checks_and_preserves_raw_usd(self):
        fake = FakeGoldCFD()
        result = self.inspect(fake)
        self.assertEqual(result['code'], 'CONNECTED')
        self.assertEqual(result['blockers'], [])
        self.assertEqual(result['symbol']['contract_size_oz'], 100)
        self.assertEqual(result['symbol']['currency_base'], 'USD')
        self.assertEqual(result['symbol_diagnostics']['currency_base'], 'USD')
        self.assertEqual(result['symbol_diagnostics']['reasons'], [])
        verification = result['symbol_verification']
        self.assertTrue(verification['passed'])
        self.assertEqual([r['actual_usd'] for r in verification['checks']], [1, -1, -1, 1])
        self.assertEqual(len(fake.profit_calls), 4)
        self.assertFalse(result['real_order_enabled'])
        self.assertTrue(fake.closed)

    def test_single_compatible_symbol_is_auto_selected_only_after_verification(self):
        fake = FakeGoldCFD()
        fake.symbol.name = 'XAUUSD.m'
        result = self.inspect(fake, automatic=True)
        self.assertEqual(result['symbol']['name'], 'XAUUSD.m')
        self.assertEqual(result['candidates'], ['XAUUSD.m'])
        self.assertEqual(len(fake.profit_calls), 4)

    def test_uses_actual_contract_size_and_valid_volume_grid(self):
        for contract, minimum, step in ((1, .01, .01), (10, .1, .1), (100, .015, .01)):
            with self.subTest(contract=contract, minimum=minimum):
                fake = FakeGoldCFD()
                fake.symbol.trade_contract_size = contract
                fake.symbol.volume_min = minimum
                fake.symbol.volume_step = step
                result = self.inspect(fake)
                self.assertTrue(result['symbol_verification']['passed'])
                self.assertEqual(result['symbol']['contract_size_oz'], contract)
                self.assertGreaterEqual(fake.profit_calls[0][2], minimum)
                self.assertAlmostEqual(fake.profit_calls[0][2] / step, round(fake.profit_calls[0][2] / step))

    def test_standard_xau_metadata_adds_no_profit_calls(self):
        fake = FakeGoldCFD()
        fake.symbol.currency_base = 'XAU'
        self.assertEqual(self.inspect(fake)['code'], 'CONNECTED')
        self.assertEqual(fake.profit_calls, [])

    def test_ambiguous_gold_symbols_require_exact_selection_without_calculating_all(self):
        for standard in (False, True):
            fake = FakeGoldCFD()
            other = copy.copy(fake.symbol)
            other.name = 'XAUUSDm'
            if standard:
                other.currency_base = 'XAU'
            fake.symbols.append(other)
            result = self.inspect(fake, automatic=True)
            self.assertEqual(result['code'], 'SELECT_SYMBOL')
            self.assertIsNone(result['symbol'])
            self.assertEqual(fake.profit_calls, [])

    def test_wrong_identity_or_product_never_reaches_profit_calculation(self):
        for field, value in (('name', 'XAGUSD'), ('description', 'Silver vs US Dollar'),
                             ('description', 'Gold vs Euro'),
                             ('currency_base', 'EUR'), ('currency_base', ''),
                             ('currency_profit', 'EUR'), ('custom', True),
                             ('trade_calc_mode', 0), ('trade_calc_mode', 2)):
            with self.subTest(field=field, value=value):
                fake = FakeGoldCFD()
                setattr(fake.symbol, field, value)
                result = self.inspect(fake)
                self.assertIsNone(result['symbol'])
                self.assertEqual(fake.profit_calls, [])
        fake = FakeGoldCFD()
        result = connector.inspect_terminal({'account':'456', 'symbol':'XAUUSD'}, fake)
        self.assertEqual(result['code'], 'ACCOUNT_MISMATCH')
        self.assertEqual(fake.profit_calls, [])

    def test_non_usd_account_requires_separate_conversion_support(self):
        fake = FakeGoldCFD()
        fake.account.currency = 'EUR'
        result = self.inspect(fake)
        self.assertIsNone(result['symbol'])
        self.assertIn('美元账户', result['symbol_verification']['reason'])
        self.assertEqual(fake.profit_calls, [])

    def test_invalid_or_unavailable_profit_is_rejected_with_json_safe_diagnostics(self):
        for value in (None, float('nan'), float('inf'), True, 0, 100):
            with self.subTest(value=value):
                fake = FakeGoldCFD()
                with patch.object(fake, 'order_calc_profit', return_value=value):
                    result = self.inspect(fake)
                self.assertEqual(result['code'], 'UNSUPPORTED_SYMBOL')
                self.assertIsNone(result['symbol'])
                self.assertEqual(result['candidates'], [])
                self.assertFalse(result['symbol_verification']['passed'])
                json.dumps(result, allow_nan=False)

    def test_all_sides_and_both_price_moves_are_required(self):
        for failed_index in (1, 2, 3, 4):
            fake = FakeGoldCFD()
            calculate = fake.order_calc_profit
            def wrong(*args):
                actual = calculate(*args)
                return -actual if len(fake.profit_calls) == failed_index else actual
            with patch.object(fake, 'order_calc_profit', side_effect=wrong):
                result = self.inspect(fake)
            self.assertIsNone(result['symbol'])
            self.assertEqual(len(fake.profit_calls), failed_index)

    def test_api_errors_remain_actionable_without_raw_exception_details(self):
        fake = FakeGoldCFD()
        with patch.object(fake, 'order_calc_profit', side_effect=RuntimeError('private-account-detail')):
            result = self.inspect(fake)
        self.assertEqual(result['code'], 'UNSUPPORTED_SYMBOL')
        self.assertIn('盈亏预估', result['symbol_verification']['reason'])
        self.assertNotIn('private-account-detail', json.dumps(result))

    def test_quote_volume_and_visibility_failures_do_not_accept_contract(self):
        for failure in ('stale', 'missing', 'nan', 'volume', 'visibility'):
            with self.subTest(failure=failure):
                fake = FakeGoldCFD()
                if failure == 'stale': fake.tick.time_msc -= 60000
                if failure == 'missing': fake.tick = None
                if failure == 'nan': fake.tick.bid = float('nan')
                if failure == 'volume': fake.symbol.volume_step = 0
                with patch.object(fake, 'symbol_select', return_value=failure != 'visibility'):
                    result = self.inspect(fake)
                self.assertIsNone(result['symbol'])
                self.assertEqual(fake.profit_calls, [])

    def test_numeric_overflow_or_unrepresentable_price_change_is_rejected(self):
        for huge_quote in (False, True):
            fake = FakeGoldCFD()
            if huge_quote:
                fake.tick.bid = fake.tick.ask = 1e100
            else:
                fake.symbol.trade_contract_size = 1e308
                fake.symbol.volume_min = fake.symbol.volume_max = 100
            result = self.inspect(fake)
            self.assertIsNone(result['symbol'])
            self.assertEqual(fake.profit_calls, [])
            json.dumps(result, allow_nan=False)


if __name__ == '__main__':
    unittest.main()
