import copy
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import pair_profiles
from pair_instruments import equal_lot_plan, native_pair
from native_pair import NativePairBroker, leg_view
from trading_config import pair_key, validate
from trading_engine import Engine
from trading_brokers import PaperBroker, NativeMT5Fills, owned_close_position
from trading_store import Store
from execution_diagnostics import build_report
from test_trading_engine import config, quote


def native_config():
    c=config();c['pair']=dict(id='same-terminal',name='期现同手数',type='mt5_mt5',short_symbol='GOLD-FUT')
    c['symbol']='GOLD-FUT';c['mt5'].update(symbol='GOLD-SPOT',adapter='native')
    c['costs'].update(usdt_usd=1,usdt_usd_auto=False,binance_fee_auto=False,
                      mt5_short_commission_per_lot_side=3,mt5_commission_per_lot_side=5,
                      mt5_short_rebate_percent=50,mt5_rebate_percent=0)
    return c


def specs():
    return {leg:dict(name=name,contract_size_oz=contract,volume_min=.01,volume_max=100,volume_step=.01,
                     currency_profit='USD',trade_mode=4) for leg,name,contract in
            (('binance','GOLD-FUT',100),('mt5','GOLD-SPOT',10))}


class PairDefinitionTests(unittest.TestCase):
    def test_equal_lots_with_different_multipliers(self):
        c=native_config();c['strategy']['mt5_lots']=.01
        p=equal_lot_plan(c,specs());self.assertEqual(p['qty'],.01);self.assertEqual(p['contract'],1)
        self.assertEqual(p['leg_contracts'],dict(binance=100,mt5=10));validate(c)

    def test_both_lot_steps_are_enforced(self):
        c=native_config();c['strategy']['mt5_lots']=.02;s=specs();s['binance']['volume_step']=.1
        with self.assertRaisesRegex(ValueError,'同时符合'): equal_lot_plan(c,s)

    def test_unsupported_currency_and_same_symbol_are_rejected(self):
        c=native_config();s=specs();s['binance']['currency_profit']='EUR'
        with self.assertRaises(ValueError):equal_lot_plan(c,s)
        s=specs();s['binance']['name']=s['mt5']['name']
        with self.assertRaises(ValueError):equal_lot_plan(c,s)

    def test_pair_key_binds_type_direction_account_symbols_and_magic(self):
        c=native_config();original=pair_key(c)
        for section,key,value in [('pair','short_symbol','GOLD-OTHER'),('mt5','symbol','GOLD-OTHER'),
                                  ('mt5','account','other-account'),('execution','magic',12345)]:
            changed=copy.deepcopy(c);changed[section][key]=value;self.assertNotEqual(original,pair_key(changed))
        self.assertNotEqual(pair_key(config()),original)

    def test_legacy_key_does_not_change_when_named(self):
        c=config();before=pair_key(c);c['pair']=dict(id='legacy',name='旧组合',type='exchange_mt5')
        self.assertEqual(before,pair_key(c))

    def test_presets_never_copy_credentials_or_terminal_identity(self):
        c=native_config();c['binance'].update(api_key='PRIVATE',api_secret='SECRET');c['mt5']['mcp_token']='TOKEN'
        p=pair_profiles.capture(c);text=json.dumps(p)
        for value in ('PRIVATE','SECRET','TOKEN','mcp_token','api_key','terminal_path','account','server'):
            self.assertNotIn(value,text)

    def test_native_activation_preserves_shared_credentials_and_uses_unit_fx(self):
        c=config();c['binance']['api_key']='shared-key'
        p=pair_profiles.capture(native_config());p['costs']['usdt_usd']=.99
        switched=pair_profiles.activate(c,p)
        self.assertTrue(native_pair(switched));self.assertEqual(switched['costs']['usdt_usd'],1)
        self.assertEqual(switched['binance']['api_key'],'shared-key')
        self.assertEqual(switched['mt5']['symbol'],'GOLD-SPOT')

    def test_new_profile_gets_distinct_magic_but_does_not_change_source(self):
        c=config();before=copy.deepcopy(c)
        p=pair_profiles.clean_profile(dict(name='PAXG',type='exchange_mt5',short_symbol='PAXGUSDT',long_symbol='XAUUSD'),c)
        self.assertNotEqual(p['execution']['magic'],c['execution']['magic']);self.assertEqual(c,before)
        self.assertEqual(p['symbol'],'PAXGUSDT')


class NativePairEngineTests(unittest.TestCase):
    def setUp(self):
        tmp=tempfile.TemporaryDirectory();self.addCleanup(tmp.cleanup)
        self.store=Store(Path(tmp.name)/'test.db');self.addCleanup(self.store.close)
        self.c=native_config();self.c['strategy']['mt5_lots']=.01
        self.p=equal_lot_plan(self.c,specs());self.b=PaperBroker();self.e=Engine(self.store,self.b)
        self.e.start(self.c)

    def market(self,a=110,b=100):
        q=quote(self.c);q['binance'].update(bid=a,ask=a+.1);q['mt5'].update(bid=b-.1,ask=b)
        q.update(entry=a-b,exit=a+.1-(b-.1));return q

    def opened(self):
        self.e.tick(self.c,self.p,self.market());return self.e.active()[0]

    def test_orders_are_equal_lots_and_keep_actual_multipliers(self):
        g=self.opened();self.assertEqual(g['status'],'open')
        orders=self.e.state['orders'];self.assertEqual([o['requested'] for o in orders],[.01,.01])
        self.assertEqual([o['native_position_side'] for o in orders],[1,0])
        self.assertEqual([o['contract_size'] for o in orders],[100,10])
        self.assertTrue(all(o['platform']=='mt5' and o['quantity_unit']=='lot' for o in orders))

    def test_pnl_and_commission_use_each_instrument_contract(self):
        g=self.opened();v=self.e.valuation(g,self.market(107,102))
        # Short 0.01*100: +2.9; long 0.01*10: +0.19.
        self.assertAlmostEqual(v['gross'],3.09);self.assertAlmostEqual(v['fees'],.08)
        self.assertAlmostEqual(v['estimated_exit_fee'],.08)
        self.assertAlmostEqual(v['estimated_rebate'],.03);self.assertAlmostEqual(v['net'],2.96)

    def test_actual_fees_replace_estimates(self):
        g=self.opened();a,b=self.e.state['orders']
        g['mode']='live'
        a['result'].update(fee=.123,fee_verified=True);b['result'].update(fee=.02,fee_verified=True)
        v=self.e.valuation(g,self.market());self.assertAlmostEqual(v['fees'],.143)
        self.assertTrue(v['fees_verified'])

    def test_both_native_tickets_are_bound_on_close(self):
        g=self.opened();opening={o['leg']:o['id'] for o in self.e.state['orders']}
        self.e.request_close(g['id']);self.e.close_group(g,self.market())
        self.assertEqual(g['status'],'closed');self.assertEqual(self.e.amounts(g),dict(binance=0,mt5=0))
        closes=[o for o in self.e.state['orders'] if o['action']=='close']
        self.assertEqual(len(closes),2)
        for o in closes:
            self.assertEqual(o['opening_id'],opening[o['leg']]);self.assertEqual(o['requested'],.01)

    def test_first_leg_partial_never_opens_second_and_reduces_only_fill(self):
        calls=[]
        def submit(o):
            calls.append(copy.deepcopy(o));r=PaperBroker.submit(self.b,o)
            if o['action']=='open':r['qty']=.005
            return r
        self.b.submit=submit;self.e.tick(self.c,self.p,self.market());g=self.e.state['groups'][0]
        self.assertEqual(g['status'],'closed');self.assertEqual(len(calls),2)
        self.assertEqual(calls[1]['requested'],.005);self.assertTrue(all(o['leg']=='binance' for o in calls))

    def test_second_leg_partial_reduces_excess_and_then_the_matched_remainder(self):
        calls=[]
        def submit(o):
            calls.append(copy.deepcopy(o));r=PaperBroker.submit(self.b,o)
            if o['action']=='open' and o['leg']=='mt5':r['qty']=.005
            return r
        self.b.submit=submit;self.e.tick(self.c,self.p,self.market());g=self.e.state['groups'][0]
        self.assertEqual(g['status'],'closed');self.assertFalse(self.e.state['enabled'])
        self.assertEqual(sum(o['requested'] for o in calls if o['action']=='close' and o['leg']=='binance'),.01)
        self.assertEqual(sum(o['requested'] for o in calls if o['action']=='close' and o['leg']=='mt5'),.005)

    def test_unknown_result_is_not_resubmitted(self):
        calls=[]
        def submit(o):calls.append(o);return dict(status='unknown',qty=0,price=0)
        self.b.submit=submit
        self.e.tick(self.c,self.p,self.market());self.e.tick(self.c,self.p,self.market())
        self.assertEqual(len(calls),1);self.assertFalse(self.e.state['enabled'])

    @staticmethod
    def pending_native_ack(o):
        return dict(status='unknown',qty=0,price=0,retcode=10009,ack_volume=o['requested'],ack_price=0,ticket='500',
                    confirmation=dict(stage='order_pending',position_matches=1,deal_matches=1,
                                      ticket_matches=1,symbol_matches=1,magic_matches=1,side_matches=1,entry_matches=1))

    def test_native_short_second_leg_uses_original_confirmation_window(self):
        from test_confirmation_diagnostics import Clock
        clock=Clock();self.c['execution']['entry_leg']='mt5';self.c['strategy']['max_unhedged_ms']=1500
        calls=[]
        def submit(o):
            calls.append(o)
            return self.pending_native_ack(o) if o['leg']=='binance' else PaperBroker.submit(self.b,o)
        self.b.submit=submit;self.b.query=lambda o:{**PaperBroker.submit(self.b,o),'ticket':'500'}
        with patch('trading_engine.time.monotonic',lambda:clock.now),patch('trading_engine.stamp',clock.wall):
            self.e.tick(self.c,self.p,self.market());g=self.e.active()[0]
            self.assertEqual(g['status'],'opening');self.assertTrue(self.e.state['enabled'])
            self.assertEqual(self.e._entry_deadlines[g['id']],101.5)
            clock.advance(.11);self.e.tick(self.c,self.p,self.market())
            self.assertEqual(g['status'],'open');self.assertEqual(len(calls),2)

    def test_native_short_close_ack_gets_same_bounded_grace_as_long_leg(self):
        from test_confirmation_diagnostics import Clock
        clock=Clock()
        self.b.submit=lambda o:self.pending_native_ack(o) if o['action']=='close' and o['leg']=='binance' else PaperBroker.submit(self.b,o)
        with patch('trading_engine.time.monotonic',lambda:clock.now),patch('trading_engine.stamp',clock.wall):
            g=self.opened();self.e.tick(self.c,self.p,self.market(108,102))
            self.assertEqual(g['status'],'closing');self.assertTrue(self.e.state['enabled'])
            self.assertTrue(self.e._waiting_close_confirmation(g))

    def test_reboot_retains_pair_unit_and_never_arms(self):
        g=self.opened();loaded=Engine(self.store,PaperBroker())
        found=next(x for x in loaded.state['groups'] if x['id']==g['id'])
        self.assertEqual(found['leg_contracts'],g['leg_contracts']);self.assertEqual(found['quantity_unit'],'lot')
        self.assertFalse(loaded.state['enabled']);self.assertTrue(loaded.state['recovery'])

    def test_diagnostic_export_explains_native_legs_and_units_without_identity(self):
        self.opened();report=build_report(self.e.state,self.store.events())
        row=report['groups'][0];self.assertEqual(row['pair']['type'],'mt5_mt5')
        self.assertEqual(row['quantity_unit'],'lot');self.assertEqual(row['leg_contracts']['binance'],100)
        for o in report['orders']:
            self.assertEqual(o['platform'],'mt5');self.assertEqual(o['quantity_unit'],'lot')
        self.assertNotIn('same-terminal',json.dumps(report))

    def test_external_verifier_view_keeps_leg_ownership_and_lot_unit(self):
        g=self.opened();state,view=leg_view(self.e.state,g,'binance')
        self.assertEqual(view['mt5_symbol'],'GOLD-FUT');self.assertEqual(view['contract'],1)
        self.assertEqual(len(state['orders']),1);self.assertEqual(state['orders'][0]['native_position_side'],1)


class NativePairChartTests(unittest.TestCase):
    def test_native_history_relocation_retains_only_same_account_and_symbols(self):
        from trading_store import same_chart_market
        key='mt5_mt5|live|native|C:/old.exe|123|broker|GOLD-FUT|GOLD-SPOT|42|p1'
        self.assertTrue(same_chart_market(key,key.replace('C:/old.exe','D:/new.exe')))
        for index in (1,2,4,5,6,7,8,9):
            parts=key.split('|');parts[index]+='-different'
            self.assertFalse(same_chart_market(key,'|'.join(parts)))


class NativePairBrokerTests(unittest.TestCase):
    def test_physical_quantity_is_converted_only_at_native_boundary(self):
        terminal=Mock();terminal.call.return_value=dict(status='done',qty=1.,price=4300,fee=.04)
        broker=NativePairBroker(terminal,dict(binance=100,mt5=10))
        order=dict(id='gp-test',leg='binance',symbol='GOLD-FUT',action='open',requested=.01,
                   native_position_side=1,result=dict(status='unknown',qty=0,price=0))
        saved=copy.deepcopy(order);r=broker.submit(order)
        wire=terminal.call.call_args.kwargs['order']
        self.assertEqual(wire['requested'],1);self.assertEqual(wire['native_position_side'],1)
        self.assertEqual(r['qty'],.01);self.assertEqual(r['price'],4300);self.assertEqual(r['fee'],.04)
        self.assertEqual(order,saved)

    def test_query_normalizes_original_order_without_changing_id_or_price(self):
        terminal=Mock();terminal.call.return_value=dict(status='done',qty=.1,price=4300)
        broker=NativePairBroker(terminal,dict(binance=100,mt5=10))
        order=dict(id='gp-original',leg='mt5',symbol='GOLD-SPOT',requested=.01,
                   result=dict(status='unknown',qty=.005,price=4300))
        r=broker.query(order);wire=terminal.call.call_args.kwargs['order']
        self.assertEqual(wire['id'],'gp-original');self.assertAlmostEqual(wire['result']['qty'],.05)
        self.assertEqual(r['qty'],.01);self.assertEqual(r['price'],4300)


class NativeShortFillTests(unittest.TestCase):
    def fixture(self):
        from test_native_mt5_fills import NativeMT5FillTests
        f=NativeMT5FillTests();f.setUp();f.o['native_position_side']=1
        f.p.type=1;f.d.type=1;f.h.type=1
        return f

    def test_short_opening_history_requires_sell_deals_and_entry_in(self):
        f=self.fixture();r=f.f.query({**f.o,'result':dict(ticket='500')})
        self.assertEqual(r['status'],'done');self.assertEqual(r['qty'],1)
        f.d.type=0;r=f.f.query({**f.o,'result':dict(ticket='500')})
        self.assertEqual(r['status'],'unknown')

    def test_short_close_uses_buy_deal_and_entry_out(self):
        f=self.fixture();o={**f.o,'action':'close','position':'900','opening_id':f.o['id']}
        f.d.type=0;f.d.entry=1;f.h.type=0
        r=f.f.query({**o,'result':dict(ticket='500')});self.assertEqual(r['status'],'done')
        self.assertTrue(owned_close_position(f.p,o,'XAUUSD',42))
        f.p.type=0;self.assertFalse(owned_close_position(f.p,o,'XAUUSD',42))

    def test_worker_opens_native_short_and_preserves_ioc_and_original_id(self):
        f=self.fixture();f.o.update(limit=3998,symbol='XAUUSD')
        f.ack.volume=.01;f.ack.price=3999.9
        result=f.run_worker(f.o)
        self.assertTrue(result['ok'],result)
        req=f.mt.order_send.call_args.args[0]
        self.assertEqual(req['type'],1);self.assertEqual(req['volume'],.01)
        self.assertEqual(req['comment'],f.o['id']);self.assertEqual(req['type_filling'],1)

    def test_worker_closes_native_short_with_buy_and_owned_ticket(self):
        f=self.fixture();f.o.update(action='close',limit=4001,symbol='XAUUSD',position='900',opening_id=f.o['id'])
        f.ack.volume=.01;f.ack.price=4000
        result=f.run_worker(f.o);self.assertTrue(result['ok'],result)
        req=f.mt.order_send.call_args.args[0]
        self.assertEqual(req['type'],0);self.assertEqual(req['position'],900)


if __name__=='__main__':unittest.main()
