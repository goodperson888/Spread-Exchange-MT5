import copy
import time
import unittest
from unittest.mock import Mock

import _test_runtime
import mt5_connector as connector
from test_connection import FakeMT5


class PairTerminal(FakeMT5):
    ORDER_TYPE_BUY = 0
    ORDER_TYPE_SELL = 1

    def __init__(self):
        super().__init__()
        a = copy.copy(self.symbol); a.name = 'GOLD-FUT'; a.volume_step = .1; a.volume_min = .1
        b = copy.copy(self.symbol); b.name = 'GOLD-SPOT'; b.trade_contract_size = 10
        self.symbols = [a, b]
        self.symbol_info = Mock(wraps=self.symbol_info)
        self.symbols_get = Mock(wraps=self.symbols_get)
        self.order_send = Mock(side_effect=AssertionError('Probe must never trade'))
        self.order_check = Mock(side_effect=AssertionError('Probe must never check an order'))
        self.order_calc_margin = Mock(side_effect=AssertionError('Connecting must not require spare margin'))
        self.order_calc_profit = Mock(side_effect=self.profit)

    def profit(self, side, name, lots, opening, closing):
        s = next(s for s in self.symbols if s.name == name)
        return (1 if side == self.ORDER_TYPE_BUY else -1) * lots * s.trade_contract_size * (closing-opening)


def settings():
    return dict(adapter='native', symbol='GOLD-SPOT', pair_symbols=['GOLD-FUT', 'GOLD-SPOT'],
                pair_lots=.1, pair_execution_mode='live')


class NativePairProbeTests(unittest.TestCase):
    def inspect(self, fake=None, **changes):
        fake = fake or PairTerminal()
        result = connector.inspect_terminal({**settings(), **changes}, fake)
        self.assertTrue(fake.closed)
        fake.order_send.assert_not_called(); fake.order_check.assert_not_called(); fake.order_calc_margin.assert_not_called()
        return result

    def test_checks_both_symbols_and_four_profit_directions_each_without_gold_scan(self):
        fake = PairTerminal(); r = self.inspect(fake)
        self.assertTrue(r['pair_ready'], r)
        self.assertEqual(r['pair_specs']['binance']['contract_size_oz'], 100)
        self.assertEqual(r['pair_specs']['mt5']['contract_size_oz'], 10)
        self.assertEqual(r['pair_lot_rules'], dict(min=.1, max=10., step=.1))
        self.assertEqual(r['pair_lots'], .1)
        self.assertEqual(fake.order_calc_profit.call_count, 8)
        self.assertEqual([x.args[0] for x in fake.symbol_info.call_args_list], ['GOLD-FUT', 'GOLD-SPOT'])
        fake.symbols_get.assert_not_called()
        self.assertFalse(r['real_order_enabled'])

    def test_missing_short_symbol_still_checks_long_and_blocks_apply(self):
        fake = PairTerminal(); fake.symbols.pop(0)
        r = self.inspect(fake)
        self.assertFalse(r['pair_ready']); self.assertIsNone(r['symbol'])
        self.assertTrue(r['pair_checks']['mt5']['passed'])
        self.assertIn('完整品种名', r['pair_checks']['binance']['errors'][0])
        self.assertEqual(fake.symbol_info.call_count, 2)

    def test_current_lots_must_match_both_steps_and_still_returns_common_rules(self):
        r = self.inspect(pair_lots=.02)
        self.assertFalse(r['pair_ready']); self.assertIsNone(r['symbol'])
        self.assertFalse(r['pair_checks']['binance']['passed'])
        self.assertTrue(r['pair_checks']['mt5']['passed'])
        self.assertEqual(r['pair_lot_rules']['step'], .1)
        self.assertIn('0.02', r['pair_checks']['binance']['errors'][0])

    def test_common_step_is_lcm_not_just_larger_step(self):
        fake = PairTerminal(); fake.symbols[0].volume_step = .02; fake.symbols[1].volume_step = .03
        r = self.inspect(fake, pair_lots=.12)
        self.assertTrue(r['pair_ready'], r)
        self.assertEqual(r['pair_lot_rules'], dict(min=.12, max=9.96, step=.06))

    def test_bad_profit_formula_is_reported_on_its_own_leg(self):
        fake = PairTerminal()
        fake.order_calc_profit.side_effect = lambda side, name, lots, opening, closing: (
            fake.profit(side,name,lots,opening,closing)*(2 if name == 'GOLD-FUT' else 1))
        r = self.inspect(fake)
        self.assertFalse(r['pair_ready'])
        self.assertFalse(r['pair_checks']['binance']['verification']['passed'])
        self.assertTrue(r['pair_checks']['mt5']['passed'])

    def test_usd_hedging_and_live_permissions_are_required_but_paper_does_not_trade(self):
        for currency, mode in [('EUR', 2), ('USD', 0)]:
            fake = PairTerminal(); fake.account.currency = currency; fake.account.margin_mode = mode
            r = self.inspect(fake)
            self.assertFalse(r['pair_ready']); self.assertTrue(any('USD 对冲' in x for x in r['blockers']))
        fake = PairTerminal(); fake.terminal.trade_allowed = False
        r = self.inspect(fake); self.assertFalse(r['pair_ready']); self.assertFalse(r['trade_allowed'])
        fake = PairTerminal(); fake.terminal.trade_allowed = False
        r = self.inspect(fake, pair_execution_mode='paper')
        self.assertTrue(r['pair_ready']); self.assertTrue(r['warnings'])

    def test_raw_clock_offset_warns_and_never_claims_realtime_ready(self):
        fake = PairTerminal(); fake.tick.time_msc += 2*3600000
        r = self.inspect(fake)
        self.assertTrue(r['pair_ready']); self.assertEqual(len(r['warnings']), 2)
        for row in r['pair_checks'].values():
            self.assertEqual(row['quote']['freshness_status'], 'needs_live_updates')

    def test_expired_or_invalid_quote_is_attributed_and_other_leg_is_checked(self):
        fake = PairTerminal(); fake.symbols[0].expiration_time = int(time.time()) - 60
        r = self.inspect(fake); self.assertFalse(r['pair_ready'])
        self.assertIn('到期', r['pair_checks']['binance']['errors'][0]); self.assertTrue(r['pair_checks']['mt5']['passed'])
        fake = PairTerminal(); old_tick = fake.symbol_info_tick
        fake.symbol_info_tick = lambda name: None if name == 'GOLD-SPOT' else old_tick(name)
        r = self.inspect(fake); self.assertTrue(r['pair_checks']['binance']['passed'])
        self.assertFalse(r['pair_ready']); self.assertIn('Bid/Ask', r['pair_checks']['mt5']['errors'][0])


if __name__ == '__main__': unittest.main()
