"""Public market/FX requests must be usable without any account credentials."""
import io
import json
import unittest
from urllib.error import HTTPError
from unittest.mock import Mock, patch

import _test_runtime
from lighter_broker import BASE, Lighter, RequestBudget
from trading_brokers import ApiError


def response(data):
    return io.BytesIO(json.dumps(data).encode())


def rejected(status, body):
    return HTTPError('https://private.example/secret', status, 'private-message', {}, response(body))


def ticker(bid='0.9998', ask='0.9999'):
    return dict(error=[], result={'USDCUSD': dict(b=[bid,'1','1'], a=[ask,'1','1'])})


class LighterPublicTests(unittest.TestCase):
    def broker(self):
        client=Lighter({})
        client.budget=RequestBudget()
        client.opener=Mock()
        return client

    def test_public_requests_have_app_identity_and_no_credentials(self):
        client=self.broker()
        client.opener.open.return_value=response(dict(code=200, order_books=[]))
        self.assertEqual(client.public('/api/v1/orderBooks')['code'],200)
        request=client.opener.open.call_args.args[0]
        self.assertEqual(request.full_url,BASE+'/api/v1/orderBooks')
        self.assertEqual(request.get_header('User-agent'),'GoldPairLocal/1.0')
        self.assertIsNone(request.get_header('Authorization'))
        self.assertIsNone(request.data)
        self.assertIsNone(client.session)

    def test_http_errors_identify_stage_and_never_echo_response_or_url(self):
        for path, label in [('/api/v1/orderBooks','合约规格'),('/api/v1/orderBookOrders','盘口'),
                            ('/api/v1/candles','历史行情')]:
            with self.subTest(path=path):
                client=self.broker()
                client.opener.open.side_effect=rejected(403,dict(message='raw-private-secret'))
                with self.assertRaises(ApiError) as caught:client.public(path)
                error=caught.exception
                self.assertEqual(error.code,'LIGHTER_PUBLIC')
                self.assertEqual(error.http_status,403)
                self.assertIn('Lighter '+label,str(error))
                self.assertIn('HTTP 403',str(error))
                self.assertNotIn('private',str(error))
                self.assertNotIn('地区限制',str(error))

    def test_region_classification_requires_explicit_lighter_code(self):
        for reply in [response(dict(code=20558)),rejected(403,dict(code=20558))]:
            client=self.broker()
            if isinstance(reply,Exception):client.opener.open.side_effect=reply
            else:client.opener.open.return_value=reply
            with self.assertRaises(ApiError) as caught:client.public('/api/v1/orderBooks')
            self.assertEqual(caught.exception.code,'LIGHTER_REGION')

    def test_malformed_error_body_still_returns_safe_stage(self):
        client=self.broker();client.opener.open.side_effect=rejected(403,['private-message'])
        with self.assertRaises(ApiError) as caught:client.public('/api/v1/orderBooks')
        self.assertIn('合约规格接口读取失败（HTTP 403）',str(caught.exception))

    def test_actual_usdc_usd_book_is_used_without_account_or_signer(self):
        client=self.broker();client.opener.open.return_value=response(ticker())
        with patch.object(client,'sdk',side_effect=AssertionError('must stay public')):
            result=client.usdt_usd()
        request=client.opener.open.call_args.args[0]
        self.assertEqual(request.full_url,'https://api.kraken.com/0/public/Ticker?pair=USDCUSD')
        self.assertAlmostEqual(result['value'],.99985)
        self.assertEqual(result['currency'],'USDC')
        self.assertEqual(result['time_basis'],'receipt_only')
        self.assertIn('Kraken USDC/USD',result['source'])
        self.assertGreater(result['time_ms'],0)
        self.assertEqual(len(client.budget.calls),0)
        self.assertIsNone(client.session)

    def test_fx_403_is_not_reported_as_lighter_or_missing_credentials(self):
        client=self.broker();client.opener.open.side_effect=rejected(403,dict(code=20558,message='private-secret'))
        with self.assertRaises(ApiError) as caught:client.usdt_usd()
        error=caught.exception
        self.assertEqual(error.code,'FX_PUBLIC')
        self.assertIn('Kraken USDC/USD 汇率接口读取失败（HTTP 403）',str(error))
        self.assertNotIn('Lighter',str(error))
        self.assertNotIn('private',str(error))

    def test_auxiliary_limits_do_not_consume_or_block_lighter_trade_budget(self):
        client=self.broker()
        for _ in range(56):client.budget.take(priority=True)
        client.opener.open.return_value=response(ticker())
        self.assertGreater(client.usdt_usd()['value'],.9)
        client.opener.open.side_effect=rejected(429,{})
        with self.assertRaises(ApiError):client.usdt_usd()
        self.assertEqual(client.budget.blocked_until,0)
        self.assertEqual(len(client.budget.calls),56)
        client.budget=RequestBudget()
        client.opener.open.side_effect=rejected(429,{})
        with self.assertRaises(ApiError):client.public('/api/v1/orderBooks')
        self.assertGreater(client.budget.blocked_until,0)

    def test_invalid_fx_never_falls_back_to_one(self):
        bad=[{},dict(error=['private-message'],result={}),dict(error=[],result={'USDTUSD':ticker()['result']['USDCUSD']}),
             ticker('NaN'),ticker('-1'),ticker('0.9999','0.9998'),ticker('1.2','1.3'),
             dict(error=[],result={'USDCUSD':dict(b=['.99'],a='1.01')}),
             dict(error=[],result={'USDCUSD':dict(b=[],a=['1'])})]
        for data in bad:
            with self.subTest(data=data):
                client=self.broker();client.opener.open.return_value=response(data)
                with self.assertRaisesRegex(ValueError,'Kraken USDC/USD 汇率盘口缺失或异常'):
                    client.usdt_usd()

    def test_paper_connection_checks_market_and_fx_without_any_key(self):
        import server
        from test_exchange_venues import lighter_config
        config=lighter_config();config['execution']['mode']='paper'
        config['lighter'].update(account_index=None,api_private_key='')
        raw=dict(symbol='XAU',market_type='perp',status='active',market_id=92,multiplier='1',
                 supported_size_decimals=4,supported_price_decimals=2,min_base_amount='.0015',min_quote_amount='10')
        client=self.broker()
        client.opener.open.side_effect=[response(dict(code=200,order_books=[raw])),response(ticker())]
        stream=Mock();stream.start.return_value=stream
        stream.quote.return_value=dict(bid=3500,ask=3501)
        stream.status.return_value=dict(state='streaming')
        with patch('server.exchange_venues.client',return_value=client), \
             patch('server.exchange_venues.stream',return_value=stream), \
             patch.object(client,'sdk',side_effect=AssertionError('must stay public')):
            result=server.inspect_binance(config)
        self.assertFalse(result['orders_sent'])
        self.assertIsNone(result['account'])
        self.assertIsNone(result['fees'])
        self.assertAlmostEqual(result['usdt_usd']['value'],.99985)
        self.assertIn('Lighter',result['message'])
        self.assertEqual(client.opener.open.call_count,2)
        stream.close.assert_called_once()


class LighterHistoryPaginationTests(unittest.TestCase):
    def test_short_window_and_last_page_request_exact_count_back(self):
        for period,count in [('M1',2),('M1',360),('M1',499),('M5',500),('M15',2880)]:
            with self.subTest(period=period,count=count):
                client=Lighter({});client.spec=Mock(return_value=dict(market_id=92))
                interval={'M1':60000,'M5':300000,'M15':900000}[period]
                end=1795177980000//interval*interval;start=end-count*interval
                def page(path,params):
                    self.assertEqual(path,'/api/v1/candles')
                    page_end=params['end_timestamp']+1
                    self.assertEqual(params['count_back'],(page_end-params['start_timestamp'])//interval)
                    self.assertLessEqual(params['count_back'],499)
                    # count_back is a lookback count, not a page-size ceiling:
                    # the real API may return older candles to satisfy it.
                    return dict(c=[dict(t=t,c=3500) for t in
                        range(page_end-params['count_back']*interval,page_end,interval)])
                client.public=Mock(side_effect=page)
                rows=client.chart_bars('XAU',period,start,end,lambda:5)
                self.assertEqual(len(rows),count)
                self.assertEqual([r['t'] for r in rows],list(range(start,end,interval)))
                self.assertEqual(client.public.call_count,(count+498)//499)
                self.assertIsNone(client.session)

    def test_outside_window_response_is_still_rejected(self):
        client=Lighter({});client.spec=Mock(return_value=dict(market_id=92))
        client.public=Mock(return_value=dict(c=[dict(t=1795177800000,c=3500)]))
        with self.assertRaisesRegex(ValueError,'超出请求区间'):
            client.chart_bars('XAU','M1',1795177860000,1795177980000,lambda:5)


if __name__=='__main__':unittest.main()
