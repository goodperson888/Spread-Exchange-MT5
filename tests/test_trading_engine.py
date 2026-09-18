import copy
import io
import json
import tempfile
import time
import unittest
import unittest.mock
from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from trading_brokers import Binance, PaperBroker
from trading_config import pair_key, validate
from trading_engine import Engine
from trading_store import Store


ROOT = Path(__file__).resolve().parents[1]


def config():
    c = json.loads((ROOT / 'config.example.json').read_text())
    c['mt5'].update(adapter='paper', symbol='XAUUSD.paper', account='PAPER', server='local')
    c['strategy'].update(entry_spread_usd=5, take_contraction_usd=2, cooldown_seconds=1, max_total_lots=.1)
    c['costs']['binance_taker_percent'] = 0
    return c


def quote(c, bid=4306, ask=4306.1, mt_bid=4300, mt_ask=4300.3):
    now = int(time.time() * 1000)
    return dict(key=pair_key(c), time_ms=now, binance=dict(bid=bid, ask=ask, time_ms=now),
                mt5=dict(bid=mt_bid, ask=mt_ask, time_ms=now), entry=bid-mt_ask, exit=ask-mt_bid)


class UnknownBroker(PaperBroker):
    def submit(self, order):
        return dict(status='unknown', qty=0, price=0, error='network timeout')

    def query(self, order):
        return dict(status='unknown', qty=0, price=0, error='not yet queryable')


class RejectMt5CloseBroker(PaperBroker):
    def submit(self, order):
        if order['leg'] == 'mt5' and order['action'] == 'close':
            return dict(status='done', qty=0, price=0, error='rejected')
        return super().submit(order)


class RejectBinanceOpenBroker(PaperBroker):
    def submit(self, order):
        if order['leg'] == 'binance' and order['action'] == 'open':
            return dict(status='done', qty=0, price=0, error='rejected')
        return super().submit(order)


class EngineTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.store = Store(Path(self.temp.name) / 'state.sqlite3')
        self.c = config()
        validate(self.c)
        self.plan = dict(lots=.02, qty=2, contract=100)

    def tearDown(self):
        self.store.close()
        self.temp.cleanup()

    def test_paper_pair_opens_then_closes_on_contraction(self):
        engine = Engine(self.store, PaperBroker())
        engine.start(self.c)
        engine.tick(self.c, self.plan, quote(self.c))
        g = engine.active()[0]
        self.assertEqual(g['status'], 'open')
        self.assertEqual(engine.amounts(g), {'binance': 2, 'mt5': 2})
        engine.tick(self.c, self.plan, quote(self.c, bid=4302.9, ask=4303))
        self.assertEqual(engine.active(), [])
        closed = engine.state['groups'][0]
        self.assertEqual(closed['status'], 'closed')
        self.assertAlmostEqual(closed['exit'], 3)
        self.assertGreater(closed['valuation']['net'], 0)
        self.assertIn('binance_funding', closed['valuation'])
        self.assertIn('mt5_swap', closed['valuation'])
        self.assertEqual(closed['valuation']['remaining'], {'binance': 0, 'mt5': 0})

    def test_grid_adds_only_after_each_spread_interval(self):
        self.c['strategy'].update(grid_enabled=True, grid_spacing_usd=2, grid_max_adds=2, max_groups=3)
        validate(self.c)
        engine = Engine(self.store, PaperBroker())
        engine.start(self.c)
        engine.tick(self.c, self.plan, quote(self.c, bid=4305.3, ask=4305.4))
        self.assertEqual([g['grid_index'] for g in engine.active()], [0])
        engine.state['last_open_ms']=0
        engine.tick(self.c, self.plan, quote(self.c, bid=4306.9, ask=4307.0))
        self.assertEqual(len(engine.active()), 1)
        engine.state['last_open_ms']=0
        engine.tick(self.c, self.plan, quote(self.c, bid=4307.4, ask=4307.5))
        self.assertEqual([g['grid_index'] for g in engine.active()], [0, 1])
        engine.state['last_open_ms']=0
        engine.tick(self.c, self.plan, quote(self.c, bid=4309.4, ask=4309.5))
        self.assertEqual([g['grid_index'] for g in engine.active()], [0, 1, 2])

    def test_unknown_first_leg_pauses_without_resubmission(self):
        engine = Engine(self.store, UnknownBroker())
        engine.start(self.c)
        engine.tick(self.c, self.plan, quote(self.c))
        self.assertFalse(engine.state['enabled'])
        self.assertEqual(len(engine.state['orders']), 1)
        engine.tick(self.c, self.plan, quote(self.c))
        self.assertEqual(len(engine.state['orders']), 1)
        self.assertIn('状态未知', engine.state['alarm'])

    def test_changed_key_rejects_quote(self):
        engine = Engine(self.store, PaperBroker())
        engine.start(self.c)
        q = quote(self.c)
        q['key'] = 'other'
        engine.tick(self.c, self.plan, q)
        self.assertEqual(engine.state['orders'], [])

    def test_dynamic_usdt_index_does_not_change_account_pair_key(self):
        changed = copy.deepcopy(self.c)
        changed['costs']['usdt_usd'] = .997
        self.assertEqual(pair_key(self.c), pair_key(changed))

    def test_mt5_close_rejection_does_not_unhedge_binance(self):
        engine = Engine(self.store, RejectMt5CloseBroker())
        engine.start(self.c)
        engine.tick(self.c, self.plan, quote(self.c))
        group = engine.active()[0]
        engine.request_close(group['id'])
        engine.tick(self.c, self.plan, quote(self.c))
        self.assertEqual(engine.amounts(group), {'binance': 2, 'mt5': 2})
        binance_closes = [o for o in engine.state['orders']
                          if o['leg'] == 'binance' and o['action'] == 'close']
        self.assertEqual(binance_closes, [])

    def test_second_leg_rejection_immediately_unwinds_mt5(self):
        engine = Engine(self.store, RejectBinanceOpenBroker())
        engine.start(self.c)
        engine.tick(self.c, self.plan, quote(self.c))
        self.assertEqual(engine.active(), [])
        group = engine.state['groups'][0]
        self.assertEqual(group['status'], 'closed')
        self.assertEqual(engine.amounts(group), {'binance': 0, 'mt5': 0})


class BinanceCostTests(unittest.TestCase):
    @staticmethod
    def account_request(account, dual=False, configuration=None):
        def request(path, params=None, method='GET', signed=False):
            if path.endswith('/accountConfig'):
                return configuration if configuration is not None else {k:account[k] for k in ('canTrade','multiAssetsMargin') if k in account}
            if path.endswith('/account'): return account
            if path.endswith('/dual'): return {'dualSidePosition':dual}
            if path.endswith('/time'): return {'serverTime':int(time.time()*1000)}
            raise AssertionError(path)
        return request

    def test_v3_without_permission_fields_uses_account_configuration(self):
        account={'assets':[{'asset':'USDT','walletBalance':'100','availableBalance':'90'}]}
        broker=Binance(production=True)
        broker.request=self.account_request(account, configuration={'canTrade':True,'multiAssetsMargin':False})
        self.assertTrue(broker.preflight({'enable_futures':True})['can_trade'])

    def test_missing_permission_is_unknown_not_false(self):
        broker=Binance(production=True)
        for flag in (None, 'false', 0):
            broker.request=self.account_request({}, configuration={'canTrade':flag})
            with self.assertRaisesRegex(ValueError,'未返回有效 canTrade'):
                broker.preflight({'enable_futures':True})

    def test_multi_asset_mode_allows_usdt_only_and_reports_usdt_balance(self):
        account={'canTrade':True,'multiAssetsMargin':True,'availableBalance':'900',
                 'totalWalletBalance':'1000','assets':[
                     {'asset':'USDT','walletBalance':'120','availableBalance':'95',
                      'crossWalletBalance':'120','unrealizedProfit':'0'},
                     {'asset':'USDC','walletBalance':'0','availableBalance':'900',
                      'crossWalletBalance':'0','unrealizedProfit':'0'}]}
        broker=Binance(production=True);broker.request=self.account_request(account)
        result=broker.preflight()
        self.assertEqual(result['asset_mode'],'multi')
        self.assertEqual(result['wallet'],120.)
        self.assertEqual(result['available'],95.)
        self.assertEqual(result['non_usdt_assets'],[])

    def test_multi_asset_mode_rejects_non_usdt_collateral_or_pnl(self):
        for field,value in (('walletBalance','1'),('crossWalletBalance','-1'),
                            ('unrealizedProfit','0.01'),('crossUnPnl','0.01'),('initialMargin','0.01')):
            other={'asset':'USDC','walletBalance':'0','crossWalletBalance':'0','unrealizedProfit':'0'}
            other[field]=value
            account={'canTrade':True,'multiAssetsMargin':True,'assets':[
                {'asset':'USDT','walletBalance':'100','availableBalance':'90'},other]}
            broker=Binance(production=True);broker.request=self.account_request(account)
            with self.assertRaisesRegex(ValueError,'USDC'): broker.preflight()

    def test_preflight_separates_trade_permission_and_position_mode_errors(self):
        account={'canTrade':False,'multiAssetsMargin':False,'assets':[
            {'asset':'USDT','walletBalance':'100','availableBalance':'90'}]}
        broker=Binance(production=True);broker.request=self.account_request(account)
        with self.assertRaisesRegex(ValueError,'enableFutures=false'):
            broker.preflight({'enable_futures':False})
        with self.assertRaisesRegex(ValueError,'enableFutures=true.*canTrade=false'):
            broker.preflight({'enable_futures':True})
        account['canTrade']=True
        broker.request=self.account_request(account,dual=True)
        self.assertEqual(broker.preflight()['position_mode'],'hedge')

    def test_hedge_mode_order_uses_position_side_without_reduce_only(self):
        broker=Binance(production=True)
        broker.position_mode='hedge'
        seen={}
        def request(path, params=None, method='GET', signed=False):
            seen.update(params or {})
            return {'status':'FILLED','executedQty':'1','avgPrice':'100','orderId':123}
        broker.request=request
        spec={'filters':{'PRICE_FILTER':{'tickSize':'0.1'}}}
        result=broker.submit({'action':'open','symbol':'XAUUSDT','requested':1,'limit':100,
                              'id':'gphedge123','position_side':'SHORT'},spec)
        self.assertEqual(result['status'],'done')
        self.assertEqual(seen['positionSide'],'SHORT')
        self.assertNotIn('reduceOnly',seen)

    def test_public_ip_uses_configured_opener_and_validates_response(self):
        broker = Binance(production=True, proxy_url='http://127.0.0.1:7890')
        broker.opener.open = unittest.mock.Mock(return_value=io.BytesIO(b'{"ip":"203.0.113.8"}'))
        self.assertEqual(broker.public_ip(), '203.0.113.8')
        request = broker.opener.open.call_args.args[0]
        self.assertEqual(request.full_url, 'https://api.ipify.org?format=json')

    def test_asset_index_and_account_commission_are_normalized(self):
        broker = Binance(production=True, key='k', secret='s')
        def request(path, params=None, method='GET', signed=False):
            if path.endswith('assetIndex'):
                return {'index':'0.9992','time':123}
            return {'makerCommissionRate':'0.0001','takerCommissionRate':'0.00045'}
        broker.request = request
        self.assertEqual(broker.usdt_usd()['value'], .9992)
        self.assertEqual(broker.commission_rate('XAUUSDT')['taker'], .045)

    def test_non_usdt_commission_is_converted(self):
        broker = Binance(production=True)
        broker.quote = lambda symbol: {'bid':600,'ask':602}
        fee = broker.commissions_usdt([
            {'commission':'1.5','commissionAsset':'USDT'},
            {'commission':'0.01','commissionAsset':'BNB'},
        ])
        self.assertAlmostEqual(fee, 7.51)


if __name__ == '__main__':
    unittest.main()
