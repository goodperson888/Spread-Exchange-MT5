import asyncio
import copy
import json
import threading
import time
import unittest
from decimal import Decimal
from types import SimpleNamespace as NS
from unittest.mock import Mock,patch,AsyncMock
import _test_runtime
from test_trading_engine import config
from trading_config import pair_key,validate,plan
from pair_profiles import clean_profile,activate,capture
from exchange_venues import venue,client
from lighter_broker import Lighter,LighterBookTicker,order_plan,terminal_receipt,client_index,SDKSession,RequestBudget
from entry_confirmation import source_token
from trading_brokers import BinanceBookTicker


def lighter_config():
    c=config();c['pair']=dict(id='light',name='Lighter XAU',type='exchange_mt5',short_symbol='XAU',exchange='lighter')
    c['symbol']='XAU';c['lighter']=dict(account_index=123,api_key_index=3,api_private_key='',proxy_url='')
    return c


def spec():
    return dict(symbol='XAU',status='TRADING',baseAsset='XAU',quoteAsset='USDC',market_id=92,size_decimals=4,price_decimals=2,
        filters=dict(LOT_SIZE=dict(minQty='.0015',maxQty='10000',stepSize='.0001'),
        MARKET_LOT_SIZE=dict(minQty='.0015',maxQty='10000',stepSize='.0001'),
        PRICE_FILTER=dict(tickSize='.01'),MIN_NOTIONAL=dict(notional='10')))


def intent(**values):return dict(dict(id='persisted-123',symbol='XAU',action='open',requested=1,limit=3500),**values)


def row(**values):
    return dict(dict(owner_account_index=123,market_index=92,client_order_index=client_index(123,'persisted-123'),
        is_ask=True,reduce_only=False,initial_base_amount='1',filled_base_amount='1',filled_quote_amount='3501',
        status='filled',order_index=998,order_id='998'),**values)


class VenueTests(unittest.TestCase):
    def test_legacy_key_stays_unchanged_and_new_account_changes_key(self):
        c=config();expected='|'.join(str(x) for x in (c['execution']['mode'],c['execution']['quote_source'],c['symbol'],
            c['mt5']['adapter'],c['mt5'].get('terminal_path'),c['mt5'].get('account'),c['mt5'].get('server'),c['mt5']['symbol'],c['execution']['magic']))
        self.assertEqual(pair_key(c),expected)
        c=lighter_config();base=pair_key(c);self.assertTrue(base.startswith('lighter|123|3|'))
        c['lighter']['account_index']=124;self.assertNotEqual(pair_key(c),base)
        c['lighter']['account_index']=123;c['lighter']['api_private_key']='a'*80;self.assertEqual(pair_key(c),base)

    def test_profile_venue_and_parameters_are_independent_and_secret_free(self):
        c=config();c['lighter']=dict(account_index=123,api_private_key='secret-private')
        p=clean_profile(dict(name='Lighter 黄金',type='exchange_mt5',exchange='lighter',short_symbol='XAU',long_symbol='XAUUSD'),c)
        self.assertEqual(p['exchange'],'lighter');self.assertNotIn('secret-private',json.dumps(p))
        saved=activate(c,p);self.assertEqual(venue(saved),'lighter');self.assertEqual(saved['lighter'],c['lighter'])
        self.assertEqual(capture(saved)['exchange'],'lighter')
        self.assertTrue(p['costs']['usdt_usd_auto']);self.assertEqual(p['costs']['usdt_usd'],1)
        c['costs'].update(usdt_usd=.9,binance_rebate_percent=80)
        same=clean_profile(dict(id=capture(c)['id'],name='编辑原组合',type='exchange_mt5',exchange='lighter',short_symbol='XAU',long_symbol='XAU'),c)
        self.assertEqual(same['costs']['usdt_usd'],1);self.assertEqual(same['costs']['binance_rebate_percent'],0)

    def test_receipt_retains_missing_zero_and_null_without_private_fields(self):
        from execution_evidence import capture_receipt
        payload=row(price=None,filled_base_amount=0,api_private_key='private-secret',authorization='bearer-secret')
        receipt=capture_receipt('lighter',payload);text=json.dumps(receipt)
        for private in ('private-secret','bearer-secret','owner_account_index','123','998'):
            self.assertNotIn(private,text)
        self.assertEqual(receipt['field_types']['price'],'null')
        self.assertEqual(receipt['values']['filled_base_amount'],0)
        self.assertEqual(receipt['field_types']['remaining_base_amount'],'missing')

    def test_transport_export_drops_error_text_urls_and_credentials(self):
        from execution_diagnostics import transport_snapshot
        s=NS(market_id=92,status=lambda:dict(state='reconnecting',route='proxy',reconnects=3,last_received_ms=10,
            error='20558 secret-key http://private.example',proxy_url='http://user:pass@proxy'))
        r=transport_snapshot(s)
        self.assertEqual(r['error_code'],'REGION_RESTRICTED');self.assertEqual(r['exchange'],'lighter')
        self.assertNotIn('secret',json.dumps(r));self.assertNotIn('http',json.dumps(r))

    def test_provider_and_credential_validation(self):
        c=lighter_config();validate(c)
        for field,value in [('account_index',-1),('account_index',True),('api_key_index',1),('api_key_index',255),('api_private_key','a'*64)]:
            bad=copy.deepcopy(c);bad['lighter'][field]=value
            with self.assertRaises(ValueError):validate(bad)
        c['pair']['exchange']='fake'
        with self.assertRaises(ValueError):venue(c)

    def test_gold_plan_uses_correct_settlement_and_equal_ounces(self):
        c=lighter_config();mt=dict(volume_min=.01,volume_max=100,volume_step=.01,contract_size_oz=100)
        result=plan(c,mt,spec(),3500);self.assertEqual(result['qty'],c['strategy']['mt5_lots']*100)
        bad=spec();bad['quoteAsset']='USDT'
        with self.assertRaises(ValueError):plan(c,mt,bad,3500)

    def test_specs_resolve_live_ids_and_reject_spot_or_unverified_units(self):
        l=Lighter({});raw=dict(symbol='XAU',market_type='perp',status='active',market_id=1234,multiplier='1',
            supported_size_decimals=4,supported_price_decimals=2,min_base_amount='.0015',min_quote_amount='10')
        l.public=Mock(return_value=dict(order_books=[raw]))
        self.assertEqual(l.spec('XAU')['market_id'],1234)
        for values in [dict(market_type='spot'),dict(multiplier='100'),dict(status='inactive')]:
            l.specs.clear();l.public.return_value=dict(order_books=[dict(raw,**values)])
            with self.assertRaises(ValueError):l.spec('XAU')

    def test_rounding_never_expands_protection_and_no_silent_quantity_resize(self):
        s=spec();self.assertEqual(order_plan(intent(limit=3500.001),s)['price'],350001)
        self.assertEqual(order_plan(intent(action='close',limit=3500.009),s)['price'],350000)
        with self.assertRaises(ValueError):order_plan(intent(requested=.12345),s)
        with self.assertRaises(ValueError):client_index(123,'')
        self.assertLess(client_index(123,'x'),2**48);self.assertNotEqual(client_index(123,'x'),client_index(124,'x'))

    def test_terminal_fill_and_partial_cancel(self):
        p=order_plan(intent(),spec());i=client_index(123,'persisted-123')
        self.assertEqual(terminal_receipt(row(),p,123,i)['status'],'done')
        r=terminal_receipt(row(status='canceled',filled_base_amount='.4',filled_quote_amount='1400.4'),p,123,i)
        self.assertEqual(r['status'],'done');self.assertEqual(r['qty'],.4)
        r=terminal_receipt(row(status='open',filled_base_amount='.4',filled_quote_amount='1400.4'),p,123,i)
        self.assertEqual(r['status'],'pending')

    def test_receipt_identity_amount_and_protection_violations_stay_unknown(self):
        p=order_plan(intent(),spec());i=client_index(123,'persisted-123')
        for changes in [dict(owner_account_index=124),dict(market_index=93),dict(client_order_index=i+1),
            dict(is_ask=1),dict(reduce_only=True),dict(initial_base_amount='2'),dict(filled_base_amount='1.1'),
            dict(filled_base_amount='.5'),dict(filled_quote_amount='3499'),dict(order_id=''),dict(order_index=0)]:
            self.assertEqual(terminal_receipt(row(**changes),p,123,i)['status'],'unknown',changes)

    def test_acceptance_and_ambiguous_submit_never_mean_filled_or_resend(self):
        l=Lighter(dict(account_index=123));sdk=Mock();l.sdk=Mock(return_value=sdk)
        sdk.send=Mock(return_value=object());sdk.call.return_value=True
        r=l.submit(intent(),spec());self.assertEqual(r['status'],'pending');self.assertEqual(r['qty'],0)
        sdk.call.side_effect=TimeoutError()
        self.assertEqual(l.submit(intent(),spec())['status'],'unknown')
        self.assertEqual(sdk.send.call_count,2) # Exactly one attempt per explicit submit.
        l.spec=Mock(return_value=spec());sdk.find=Mock(return_value=object())
        self.assertEqual(l.query(intent())['status'],'unknown');self.assertEqual(sdk.send.call_count,2)

    def test_unknown_fee_units_and_absent_fees_never_mark_zero_verified(self):
        l=Lighter(dict(account_index=123));l.spec=Mock(return_value=spec())
        t=dict(market_id=92,ask_account_id=123,bid_account_id=42,is_maker_ask=False,trade_id=9,ask_id=998,bid_id=3,
               size='1',price='3500',timestamp=1000,taker_fee=0,integrator_taker_fee=0)
        self.assertEqual(l.commissions_usdt(l.normalized_trades('XAU',[t])),0)
        for changes in [dict(taker_fee=5),dict(taker_fee=None),dict(integrator_taker_fee=None)]:
            with self.assertRaises(ValueError):l.commissions_usdt(l.normalized_trades('XAU',[dict(t,**changes)]))
        with self.assertRaises(ValueError):l.normalized_trades('XAU',[dict(t,ask_account_id=9)])
        with self.assertRaises(ValueError):l.normalized_trades('XAU',[t,t])

    def test_read_only_margin_check_uses_effective_account_imf_not_exchange_minimum(self):
        l=Lighter(dict(account_index=123));l.account_info=Mock(return_value=dict(available_balance='100',
            positions=[dict(market_id=92,initial_margin_fraction='20.00',position='0',sign=0,position_value='0',
                avg_entry_price='0',liquidation_price='0',unrealized_pnl='0')]))
        l.positions=Mock(return_value=[]);l.public=Mock(return_value=dict(order_book_details=[dict(market_id=92,
            min_initial_margin_fraction=100,default_initial_margin_fraction=200)]))
        with self.assertRaisesRegex(ValueError,'保证金不足'):l.test_order(intent(),spec())
        l.account_info.return_value['available_balance']='800'
        self.assertTrue(l.test_order(intent(),spec())['read_only'])
        self.assertEqual(l.account_info.call_count,2)
        l.public.assert_not_called()
        l.account_info.return_value['positions']=[]
        with self.assertRaisesRegex(ValueError,'实际保证金'):l.test_order(intent(),spec())

    def test_budget_reserves_confirmation_and_write_headroom_and_expires(self):
        now=[100.];b=RequestBudget(lambda:now[0])
        for _ in range(46):b.take()
        with self.assertRaisesRegex(Exception,'额度'):b.take()
        for _ in range(10):b.take(priority=True)
        with self.assertRaisesRegex(Exception,'额度'):b.take(priority=True)
        now[0]=160.;b.take();b.backoff()
        with self.assertRaisesRegex(Exception,'额度'):b.take(priority=True)
        now[0]=220.;b.take(priority=True)

    def test_risk_uses_cross_equity_not_deposited_collateral(self):
        account=dict(collateral='1000',cross_asset_value='50',cross_maintenance_margin_requirement='45',available_balance='5')
        self.assertEqual(Lighter.risk_account(account)['totalMarginBalance'],50)
        account.pop('cross_asset_value')
        self.assertIsNone(Lighter.risk_account(account)['totalMarginBalance'])

    def test_account_identity_mismatch_is_not_flat(self):
        l=Lighter(dict(account_index=123));l.public=Mock(return_value=dict(accounts=[dict(account_index=124,positions=[])]))
        with self.assertRaises(ValueError):l.positions('XAU')

    def test_book_snapshot_deltas_and_gap_never_reuse_corrupt_data(self):
        s=LighterBookTicker(92)
        message=dict(type='subscribed/order_book',channel='order_book:92',timestamp=1000,
            order_book=dict(offset=1,nonce=10,bids=[dict(price='3500',size='2')],asks=[dict(price='3501',size='3')]))
        self.assertTrue(s.handle(message));self.assertEqual(s.value['bid'],3500)
        delta=dict(type='update/order_book',channel='order_book:92',timestamp=1001,
            order_book=dict(offset=3,begin_nonce=10,nonce=11,bids=[dict(price='3500',size='0'),dict(price='3499',size='4')],asks=[]))
        self.assertTrue(s.handle(delta));self.assertEqual(s.value['bid'],3499)
        with self.assertRaises(ValueError):s.handle(delta)
        self.assertEqual(s.offset,3)
        bad=copy.deepcopy(delta);bad['order_book'].update(offset=4,begin_nonce=11,nonce=12,bids=[dict(price='3502',size='1')])
        with self.assertRaises(ValueError):s.handle(bad)
        self.assertEqual(s.value['bid'],3499)

    def test_identical_source_updates_count_but_heartbeats_and_rest_repeats_do_not(self):
        q=dict(binance=dict(bid=1,ask=2,time_ms=100,time_basis='receipt_only',source_sequence='1:2'),mt5={})
        before=source_token(q);q['binance']['time_ms']=200;self.assertEqual(source_token(q),before)
        q['binance']['source_sequence']='2:3';self.assertNotEqual(source_token(q),before)
        q['binance'].pop('source_sequence');before=source_token(q);q['binance']['time_ms']=300
        self.assertEqual(source_token(q),before)

    def test_binance_public_stream_path_and_failed_socket_cleanup(self):
        stream=BinanceBookTicker('XAUUSDT',True);socket=Mock()
        socket.recv.side_effect=TimeoutError('proxy-secret-url')
        def connect(url,**options):self.assertEqual(url,'wss://fstream.binance.com/public/ws/xauusdt@bookTicker');return socket
        def stop(delay):return True
        stream.stop_event.wait=stop
        with patch('trading_brokers.websocket',NS(create_connection=connect)):stream._run()
        socket.close.assert_called_once();self.assertNotIn('proxy-secret',stream.error)
        self.assertEqual(stream.status()['reconnects'],1);self.assertIsNone(stream.value)

    def test_lighter_candles_are_display_only_and_bounded(self):
        l=Lighter({});l.spec=Mock(return_value=spec());l.public=Mock(return_value=dict(c=[dict(t=60000,c=3500)]))
        rows=l.chart_bars('XAU','M1',60000,120000,lambda:1)
        self.assertEqual(rows,[dict(t=60000,close=3500)])
        l.public.return_value=dict(c=[dict(t=0,c=3500)])
        with self.assertRaises(ValueError):l.chart_bars('XAU','M1',60000,120000,lambda:1)


class PrivatePushTests(unittest.IsolatedAsyncioTestCase):
    def session(self):
        s=SDKSession.__new__(SDKSession);s.account=123;s.key_index=3;s.write_lock=asyncio.Lock()
        s.budget=RequestBudget();s.sends=set();s.sdk=NS(create_order=AsyncMock(return_value=(None,dict(code=200),None)),
            ORDER_TYPE_LIMIT=0,ORDER_TIME_IN_FORCE_IMMEDIATE_OR_CANCEL=0,DEFAULT_IOC_EXPIRY=0)
        return s

    async def test_nonce_warmup_is_read_only_and_precedes_first_send(self):
        s=self.session();s.sdk.nonce_manager=NS(async_hard_refresh_nonce=AsyncMock())
        await s.warmup()
        s.sdk.nonce_manager.async_hard_refresh_nonce.assert_awaited_once_with(3)
        s.sdk.create_order.assert_not_awaited();self.assertEqual(len(s.budget.calls),1)

    async def test_push_can_win_while_nonce_lock_owns_inflight_send(self):
        s=self.session();started=asyncio.Event();finish=asyncio.Event();armed=[]
        async def create(**kwargs):
            started.set();await finish.wait();return None,dict(code=200),None
        async def pushed(market,index):await started.wait();return row()
        s.sdk.create_order=AsyncMock(side_effect=create);s.stream=NS(arm=lambda p,i:armed.append(i),wait_order=pushed)
        p=order_plan(intent(),spec());i=client_index(123,'persisted-123')
        result=await s.send(p,i)
        self.assertEqual(armed,[i]);self.assertEqual(result['terminal'],row())
        self.assertTrue(s.write_lock.locked());self.assertEqual(len(s.sends),1)
        finish.set();await asyncio.gather(*s.sends)
        self.assertFalse(s.write_lock.locked());s.sdk.create_order.assert_awaited_once()
        # In SDK 1.1.4 an explicit key with nonce=-1 bypasses its manager.
        # Omit both so the sole configured key uses the SDK nonce lock/cache.
        self.assertNotIn('api_key_index',s.sdk.create_order.await_args.kwargs)
        self.assertNotIn('nonce',s.sdk.create_order.await_args.kwargs)

    async def test_cancel_also_uses_sdk_managed_nonce(self):
        s=self.session();s.sdk.cancel_order=AsyncMock(return_value=(None,dict(code=200),None))
        await s.cancel(92,998)
        s.sdk.cancel_order.assert_awaited_once_with(market_index=92,order_index=998)

    async def test_expiry_in_signer_queue_never_sends_and_budget_does_not_wait(self):
        s=self.session();p=order_plan(intent(),spec())
        await s.write_lock.acquire();task=asyncio.create_task(s.send_locked(p,7));await asyncio.sleep(0)
        p['deadline']=0;s.write_lock.release();result=await task
        self.assertFalse(result['submitted']);s.sdk.create_order.assert_not_awaited()
        for _ in range(56):s.budget.take(priority=True)
        p['deadline']=time.monotonic()+1
        result=await s.send_locked(p,7)
        self.assertFalse(result['submitted']);self.assertTrue(result['rate_limited'])
        s.sdk.create_order.assert_not_awaited()

    async def test_private_confirmation_wins_slow_rest_and_cancels_only_read(self):
        s=self.session();cancelled=asyncio.Event()
        async def rest(market,index):
            try:await asyncio.Event().wait()
            finally:cancelled.set()
        async def pushed(market,index):await asyncio.sleep(0);return row()
        s.stream=NS(order=lambda *a:None,wait_order=pushed);s.find_rest=rest
        self.assertEqual(await s.find(92,7),row());self.assertTrue(cancelled.is_set())
        s.sdk.create_order.assert_not_awaited()

    async def test_terminal_push_is_armed_and_validated_before_releasing_hedge(self):
        from lighter_order_stream import LighterOrderStream
        s=LighterOrderStream('wss://example',123,lambda:'token',
            lambda r,p,a,i:terminal_receipt(r,p,a,i)['status']=='done')
        i=client_index(123,'persisted-123');p=order_plan(intent(),spec());s.arm(p,i)
        s.handle(dict(type='update/account_all_orders',channel='account_all_orders:123',orders={'92':[row(reduce_only=True)]}))
        self.assertIsNone(s.order(92,i))
        s.handle(dict(type='update/account_all_orders',channel='account_all_orders:123',orders={'92':[row()]}))
        self.assertEqual(s.order(92,i)['order_index'],998)
        s.handle(dict(type='update/account_all_orders',channel='account_all_orders:123',orders={'92':[row(status='open')]}))
        self.assertEqual(s.order(92,i)['status'],'filled')


class VenueHttpTests(unittest.TestCase):
    def setUp(self):
        import test_connection
        self.base=test_connection.HttpTests();self.base.setUp();self.addCleanup(self.base.tearDown)
        self.request=self.base.request

    def test_private_key_write_only_preserve_blank_and_clear_explicitly(self):
        import server
        key='f'*80
        code,response=self.request('/api/config',dict(lighter=dict(account_index=123,api_key_index=3,api_private_key=key)))
        self.assertEqual(code,200,response)
        code,response=self.request('/api/config')
        self.assertTrue(response['config']['lighter']['api_private_key_configured'])
        self.assertNotIn(key,json.dumps(response));self.assertNotIn('api_private_key',response['config']['lighter'])
        code,response=self.request('/api/config',dict(lighter=dict(api_private_key='',proxy_url='')))
        self.assertEqual(code,200,response);self.assertEqual(server.load_config()['lighter']['api_private_key'],key)
        self.request('/api/config',dict(clear_credentials=True))
        self.assertEqual(server.load_config()['lighter']['api_private_key'],'')

    def test_lighter_profile_activation_uses_xau_without_binance_credentials(self):
        import server
        runtime=NS(lock=threading.RLock(),engine=NS(state=dict(enabled=False,recovery=False),active=Mock(return_value=[])),
                   _disconnect_pair_resources=Mock(),_native_connection_pending=Mock(return_value=False))
        with patch.object(server,'TRADING',runtime):
            code,response=self.request('/api/pairs/save',dict(name='Lighter XAU',exchange='lighter',type='exchange_mt5',
                short_symbol='XAU',long_symbol='XAUUSD'))
            self.assertEqual(code,200,response)
            code,response=self.request('/api/pairs/activate',dict(id=response['saved_id']))
            self.assertEqual(code,200,response);self.assertEqual(venue(server.load_config()),'lighter')
            self.assertEqual(server.load_config()['symbol'],'XAU')


class IntakeSpeedTests(unittest.TestCase):
    def test_native_exchange_fallback_polls_without_waiting_for_slow_main_interval(self):
        import time
        from market_push import QuotePump
        c=config();c['mt5']['adapter']='native';c['execution']['poll_ms']=2000;c['execution']['mt5_quote_poll_ms']=20
        arrived=threading.Event();reads=[]
        def quote(*args,**kwargs):
            reads.append(time.monotonic())
            if len(reads)>=4:arrived.set()
            return dict(quote=dict(bid=3500,ask=3500.1,time_ms=int(time.time()*1000)))
        terminal=NS(call=quote)
        stream=NS(quote=lambda **kwargs:dict(bid=3505,ask=3505.1,time_ms=int(time.time()*1000)),on_quote=None)
        pump=QuotePump(c,NS(),stream,terminal,None,None,lambda q:None,threading.Event())
        try:
            pump.start();self.assertTrue(arrived.wait(.8),'native intake stalled behind 2-second main-loop interval')
            self.assertLess(reads[3]-reads[0],.8)
        finally:pump.close()
