import copy
import threading
import unittest
from types import SimpleNamespace as NS
from unittest.mock import Mock

from liquidation_risk import (DEFAULT_ALERTS, BinanceRiskMonitor, binance_risk,
                              mt5_risk, mt5_risk_fields, risk_snapshot, validate_alerts)


def account():
    return {'totalMarginBalance': '1000', 'totalMaintMargin': '20', 'availableBalance': '800'}


def position(qty=-1, mark=4000, liquidation=4400, **extra):
    return dict(symbol='XAUUSDT', positionAmt=str(qty), markPrice=str(mark),
                liquidationPrice=str(liquidation), **extra)


def mt5():
    return dict(currency='USD', equity=1000, margin=100, free_margin=900,
                margin_level=1000, margin_so_mode=0, margin_so_call=100, margin_so_so=50)


class RiskCalculationTests(unittest.TestCase):
    def test_hedged_positions_choose_nearest_direction_using_mark_price(self):
        rows = [position(-1, 4000, 4400), position(2, 4000, 3920),
                dict(position(1, 4000, 3999), symbol='BTCUSDT')]
        result = binance_risk(account(), rows, 'XAUUSDT', DEFAULT_ALERTS)
        self.assertEqual(result['distance_percent'], 2)
        self.assertEqual(result['level'], 'danger')
        self.assertEqual([x['side'] for x in result['positions']], ['short', 'long'])
        self.assertEqual(len(result['positions']), 2)

    def test_warning_boundary_and_passed_liquidation_price(self):
        for price, level in ((4400, 'normal'), (4200, 'warning'), (4080, 'danger'), (3990, 'danger')):
            with self.subTest(price=price):
                result = binance_risk(account(), [position(liquidation=price)], 'XAUUSDT', DEFAULT_ALERTS)
                self.assertEqual(result['level'], level)

    def test_invalid_or_zero_liquidation_price_never_claims_normal(self):
        for price in (0, None, 'NaN', '-Infinity', -1):
            with self.subTest(price=price):
                result = binance_risk(account(), [position(liquidation=price)], 'XAUUSDT', DEFAULT_ALERTS)
                self.assertEqual(result['level'], 'unknown')
                self.assertIsNone(result['distance_percent'])

    def test_partial_fields_do_not_hide_known_danger_or_claim_safe(self):
        result = binance_risk(account(), [position(liquidation=0), position(liquidation=4080)], 'XAUUSDT', DEFAULT_ALERTS)
        self.assertEqual(result['level'], 'danger')
        self.assertIn('字段缺失', result['message'])
        self.assertEqual(binance_risk({}, [], 'XAUUSDT', DEFAULT_ALERTS)['level'], 'unknown')
        self.assertEqual(binance_risk(account(), [], 'XAUUSDT', DEFAULT_ALERTS)['level'], 'idle')
        low = dict(account(), totalMarginBalance='10')
        self.assertEqual(binance_risk(low, [], 'XAUUSDT', DEFAULT_ALERTS)['level'], 'danger')

    def test_mt5_uses_account_equity_and_respects_broker_stopout(self):
        for equity, level in ((1000, 'normal'), (200, 'warning'), (100, 'danger'), (50, 'danger')):
            data = dict(mt5(), equity=equity, margin_level=9999)
            result = mt5_risk(data, DEFAULT_ALERTS)
            self.assertEqual(result['level'], level)
            self.assertEqual(result['margin_level'], equity)
        alerts = dict(DEFAULT_ALERTS, mt5_warning_margin_level_percent=20, mt5_danger_margin_level_percent=10)
        result = mt5_risk(dict(mt5(), equity=40), alerts)
        self.assertEqual(result['level'], 'danger')
        self.assertIn('经纪商强平线', result['message'])

    def test_mt5_money_mode_and_missing_stopout_remain_unclassified(self):
        result = mt5_risk(dict(mt5(), margin_so_mode=1), DEFAULT_ALERTS)
        self.assertEqual(result['level'], 'unknown')
        self.assertIn('金额强平模式', result['message'])
        self.assertEqual(result['margin_so_so'], 50)
        for key in ('equity', 'margin', 'margin_so_so', 'margin_so_mode'):
            data = mt5(); data.pop(key)
            self.assertEqual(mt5_risk(data, DEFAULT_ALERTS)['level'], 'unknown')

    def test_mt5_zero_margin_and_native_missing_fields(self):
        result = mt5_risk(dict(mt5(), margin=0), DEFAULT_ALERTS)
        self.assertEqual(result['level'], 'idle')
        self.assertEqual(mt5_risk(dict(mt5(), margin=0, equity=-1), DEFAULT_ALERTS)['level'], 'danger')
        fields = mt5_risk_fields(NS(equity=1000, margin=100, margin_so_mode=0))
        self.assertEqual(fields['margin_so_mode'], 0)
        self.assertIsNone(fields['margin_so_so'])

    def test_thresholds_reject_nonfinite_booleans_and_inverted_levels(self):
        validate_alerts({})
        for value in (None, [], {'binance_warning_distance_percent': True},
                      {'binance_warning_distance_percent': float('nan')},
                      {'mt5_danger_margin_level_percent': 300},
                      {'binance_danger_distance_percent': 5}):
            with self.subTest(value=value), self.assertRaises(ValueError): validate_alerts(value)


class RiskCollectionTests(unittest.TestCase):
    def fixture(self):
        clock = [100.0]
        client = NS(sync=Mock(), request=Mock(side_effect=[account(), [position()]]))
        monitor = BinanceRiskMonitor(client, 'XAUUSDT', clock=lambda: clock[0], start=False)
        return clock, client, monitor

    def test_collector_only_uses_read_endpoints_and_status_never_requests(self):
        clock, client, monitor = self.fixture()
        monitor._step()
        client.sync.assert_called_once()
        self.assertEqual([x.args[0] for x in client.request.call_args_list], ['/fapi/v3/account', '/fapi/v3/positionRisk'])
        self.assertTrue(all(x.kwargs == {'signed': True} for x in client.request.call_args_list))
        client.request.reset_mock()
        self.assertEqual(monitor.status(DEFAULT_ALERTS)['level'], 'normal')
        clock[0] += 20
        self.assertEqual(monitor.status(DEFAULT_ALERTS)['level'], 'stale')
        client.request.assert_not_called()

    def test_failures_revoke_normal_immediately_and_expire_old_data(self):
        clock, client, monitor = self.fixture()
        monitor._step(); clock[0] += 1
        client.request.side_effect = RuntimeError('secret key in unexpected error')
        monitor._step()
        result = monitor.status(DEFAULT_ALERTS)
        self.assertEqual(result['level'], 'unknown')
        self.assertEqual(result['age_ms'], 1000)
        self.assertNotIn('secret', str(result))
        clock[0] += 20
        self.assertEqual(monitor.status(DEFAULT_ALERTS)['level'], 'stale')

    def test_close_discards_a_late_response(self):
        clock, client, monitor = self.fixture()
        def close_in_request(*args, **kwargs):
            monitor.close()
            return account()
        client.request.side_effect = close_in_request
        monitor._step()
        self.assertEqual(monitor.samples, {})
        client.request.assert_called_once()

    def test_paper_and_disconnected_never_query_accounts(self):
        monitor = NS(status=Mock(side_effect=AssertionError('Must not read account')))
        for mode, connected, level in (('paper', True, 'paper'), ('live', False, 'disconnected')):
            result = risk_snapshot(mode, connected, monitor, None, None, {})
            self.assertEqual(result['binance']['level'], level)
            self.assertEqual(result['mt5']['level'], level)
        monitor.status.assert_not_called()

    def test_readonly_snapshot_ages_mt5_and_applies_new_alerts(self):
        clock, client, monitor = self.fixture(); monitor._step()
        alerts = dict(DEFAULT_ALERTS, binance_warning_distance_percent=12)
        before = copy.deepcopy(alerts)
        result = risk_snapshot('live', True, monitor, {'account': mt5()}, 80, alerts, clock=lambda: clock[0])
        self.assertEqual(result['mt5']['level'], 'stale')
        self.assertEqual(result['binance']['level'], 'warning')
        self.assertEqual(alerts, before)

    def test_mt5_read_failure_revokes_cached_normal_state_immediately(self):
        clock, client, monitor = self.fixture(); monitor._step()
        result = risk_snapshot('live', True, monitor, {'account': mt5()}, 99, {},
                               clock=lambda: clock[0], mt5_error='MT5 账户快照读取失败')
        self.assertEqual(result['mt5']['level'], 'unknown')
        self.assertEqual(result['mt5']['age_ms'], 1000)
        self.assertIn('读取失败', result['mt5']['message'])


if __name__ == '__main__': unittest.main()
