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

    def test_unique_case_insensitive_name_uses_actual_terminal_name_without_more_requests(self):
        fake = FakeGoldCFD()
        fake.symbol.name = 'XAUUSD.m'
        with patch.object(fake, 'symbol_info', wraps=fake.symbol_info) as info, \
             patch.object(fake, 'symbols_get', wraps=fake.symbols_get) as scan, \
             patch.object(fake, 'symbol_info_tick', wraps=fake.symbol_info_tick) as tick:
            result = connector.inspect_terminal({'symbol':'  xauusd.M  '}, fake)
        info.assert_called_once_with('XAUUSD.m')
        scan.assert_called_once_with()
        tick.assert_called_once_with('XAUUSD.m')
        self.assertEqual(result['symbol']['name'], 'XAUUSD.m')
        self.assertEqual(result['symbol_resolution'], {'requested':'xauusd.M', 'resolved':'XAUUSD.m'})
        self.assertEqual(len(fake.profit_calls), 4)

    def test_exact_case_wins_but_ambiguous_case_is_never_guessed(self):
        for requested, expected in (('XAUUSD', 'XAUUSD'), ('xauusd', 'xauusd'), ('xAuUsD', None)):
            with self.subTest(requested=requested):
                fake = FakeGoldCFD()
                other = copy.copy(fake.symbol)
                other.name = 'xauusd'
                fake.symbols.append(other)
                result = connector.inspect_terminal({'symbol':requested}, fake)
                if expected:
                    self.assertEqual(result['symbol']['name'], expected)
                    self.assertNotIn('symbol_resolution', result)
                else:
                    self.assertEqual(result['code'], 'SELECT_SYMBOL')
                    self.assertIsNone(result['symbol'])
                    self.assertIn('大小写', result['message'])
                    self.assertEqual(fake.profit_calls, [])

    def test_partial_name_does_not_expand_to_another_contract(self):
        fake = FakeGoldCFD()
        result = connector.inspect_terminal({'symbol':'xau'}, fake)
        self.assertIsNone(result['symbol'])
        self.assertNotIn('symbol_resolution', result)
        self.assertEqual(fake.profit_calls, [])

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

    def test_volume_and_visibility_failures_do_not_accept_contract(self):
        for failure in ('volume', 'visibility'):
            with self.subTest(failure=failure):
                fake = FakeGoldCFD()
                if failure == 'volume': fake.symbol.volume_step = 0
                with patch.object(fake, 'symbol_select', return_value=failure != 'visibility'):
                    result = self.inspect(fake)
                self.assertIsNone(result['symbol'])
                self.assertEqual(fake.profit_calls, [])

    def test_missing_market_quote_can_calibrate_units_without_manufacturing_a_quote(self):
        for failure in ('missing', 'nan', 'zero', 'crossed'):
            with self.subTest(failure=failure):
                fake = FakeGoldCFD()
                if failure == 'missing': fake.tick = None
                if failure == 'nan': fake.tick.bid = float('nan')
                if failure == 'zero': fake.tick.bid = fake.tick.ask = 0
                if failure == 'crossed': fake.tick.bid = fake.tick.ask + 1
                result = self.inspect(fake)
                self.assertTrue(result['symbol_verification']['passed'])
                self.assertEqual(result['symbol_verification']['price_source'], 'hypothetical_prices')
                self.assertEqual(len(fake.profit_calls), 4)
                self.assertIsNone(result['quote'])
                self.assertFalse(result['quote_diagnostics']['valid_price'])
                self.assertIn('尚无有效 Bid/Ask', ' '.join(result['blockers']))
                self.assertFalse(result['real_order_enabled'])
                self.assertEqual(result['symbol_diagnostics']['reasons'], [])
                json.dumps(result, allow_nan=False)

    def test_unavailable_quote_does_not_bypass_failed_profit_check(self):
        fake = FakeGoldCFD()
        fake.tick = None
        with patch.object(fake, 'order_calc_profit', return_value=None):
            result = self.inspect(fake)
        self.assertIsNone(result['symbol'])
        self.assertFalse(result['symbol_verification']['passed'])
        self.assertIn('未返回有效的盈亏预估', result['symbol_verification']['reason'])
        self.assertNotIn('currency_base 不是 XAU', ' '.join(result['symbol_diagnostics']['reasons']))

    def test_reference_quote_clock_does_not_block_contract_calibration_or_hide_market_warning(self):
        for offset in (-60000, 60000):
            with self.subTest(offset=offset):
                fake = FakeGoldCFD()
                fake.tick.time_msc += offset
                original = fake.tick.time_msc
                result = self.inspect(fake)
                self.assertEqual(result['code'], 'CONNECTED')
                self.assertTrue(result['symbol_verification']['passed'])
                self.assertEqual(len(fake.profit_calls), 4)
                self.assertEqual(result['quote']['time_ms'], original)
                self.assertEqual(result['quote_diagnostics']['time_ms'], original)
                self.assertGreater(abs(result['quote']['age_ms']), 59000)
                self.assertEqual(result['blockers'], [])
                self.assertIn('不能据此认定可立即成交', ' '.join(result['warnings']))
                self.assertFalse(result['real_order_enabled'])

    def test_screenshot_offset_is_only_a_hint_and_never_applied_to_quote_time(self):
        fake = FakeGoldCFD()
        local = 1793880142263
        fake.tick.time_msc = local + 7191593
        with patch.object(connector.time, 'time', return_value=local/1000):
            result = self.inspect(fake)
        diagnostic = result['quote_diagnostics']
        self.assertEqual(diagnostic['raw_time_delta_ms'], -7191593)
        self.assertEqual(diagnostic['possible_offset_hours'], 2)
        self.assertEqual(diagnostic['freshness_status'], 'needs_live_updates')
        self.assertEqual(result['quote']['time_ms'], fake.tick.time_msc)
        self.assertTrue(result['symbol_verification']['passed'])
        self.assertEqual(result['blockers'], [])
        self.assertIn('不作为行情延迟', result['warnings'][0])
        self.assertFalse(result['real_order_enabled'])

    def test_clock_hints_distinguish_past_and_future_without_rounding_arbitrary_delays(self):
        local = 1793880142263
        for delta, expected in ((-7191593, 2), (7191593, -2), (-230000, None), (60000, None)):
            with self.subTest(delta=delta), patch.object(connector.time, 'time', return_value=local/1000):
                fake = FakeGoldCFD()
                fake.tick.time_msc = local-delta
                diagnostic = connector.quote_diagnostics(fake.tick)
                self.assertEqual(diagnostic['raw_time_delta_ms'], delta)
                self.assertEqual(diagnostic['possible_offset_hours'], expected)

    def test_missing_timestamp_allows_only_contract_calibration_with_market_warning(self):
        fake = FakeGoldCFD()
        fake.tick.time_msc = 0
        result = self.inspect(fake)
        self.assertTrue(result['symbol_verification']['passed'])
        self.assertIsNone(result['quote'])
        self.assertFalse(result['quote_diagnostics']['valid_time'])
        self.assertIn('报价时间戳缺失或无效', ' '.join(result['blockers']))
        self.assertFalse(result['real_order_enabled'])

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
