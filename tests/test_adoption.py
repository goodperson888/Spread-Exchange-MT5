import copy
import hashlib
import json
import sys
import tempfile
import threading
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import position_adoption as adoption
import server
from trading_brokers import PaperBroker, owned_close_position
from trading_engine import Engine, stamp
from trading_store import Store
from test_trading_engine import config, quote


class AdoptionTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.store=Store(Path(self.temp.name)/'test.sqlite3');self.addCleanup(self.store.close)
        self.c=config();self.c['execution']['mode']='live';self.c['mt5']['adapter']='native'
        self.c['costs']['usdt_usd']=1
        self.broker=Mock(wraps=PaperBroker())
        self.engine=Engine(self.store,self.broker)
        self.at=stamp()
        self.positions=[dict(ticket=str(i),identifier=str(i+100),symbol=self.c['mt5']['symbol'],
            lots=.01,side=0,magic=0,comment='manual',price_open=4300.,time_ms=self.at-86400000,swap=-.1,profit=0.) for i in (1,2)]
        self.mt5=dict(account=dict(account='123',server='Demo',currency='USD',margin_mode=2),
            allowed=True,orders=[],positions=self.positions,
            spec=dict(name=self.c['mt5']['symbol'],contract_size_oz=100,volume_min=.01,volume_step=.01,volume_max=100))
        self.exchange=[dict(symbol=self.c['symbol'],positionSide='SHORT',positionAmt='-2',entryPrice='4306')]
        self.spec=dict(status='TRADING',baseAsset='XAU',quoteAsset='USDT',filters={
            k:dict(minQty='.001',maxQty='1000',stepSize='.001') for k in ('LOT_SIZE','MARKET_LOT_SIZE')})
        self.inputs=dict(entry_fx=1,history_fees_usd=1,history_funding_usd=-.5,
                         take_contraction_usd=2,min_net_profit_usd=0,costs_confirmed=True)

    def proposal(self, tickets=None):
        p=adoption.preview(self.c,self.mt5,self.exchange,[],tickets or ['1','2'],self.inputs,self.spec,self.at)
        p['binance_key_fingerprint']=hashlib.sha256(b'test-key').hexdigest()
        return p

    def registered(self):
        return adoption.register(self.engine,self.c,self.proposal(),self.at)

    def runtime(self):
        r=server.TradingRuntime.__new__(server.TradingRuntime)
        r.lock=threading.RLock();r.config=self.c;r.engine=self.engine;r.spec=self.spec
        r.connected=True;r.reconciled=False;r.adoption_preview=None;r.position_report=None
        r.terminal=Mock();r.terminal.call.return_value=self.mt5
        r.binance=Mock(key='test-key',position_mode='hedge')
        r.binance.positions.return_value=self.exchange;r.binance.open_orders.return_value=[]
        r.snapshot=lambda:dict(state=r.engine.state,position_report=r.position_report)
        return r

    def test_preview_is_read_only_and_requires_full_pair(self):
        p=self.proposal()
        self.assertEqual((p['qty'],p['entry']),(2,6))
        self.assertEqual(self.engine.state['groups'],[])
        self.broker.submit.assert_not_called()
        with self.assertRaisesRegex(ValueError,'配平'):self.proposal(['1'])
        with self.assertRaisesRegex(ValueError,'重复'):self.proposal(['1','1'])
        self.exchange[0]['positionAmt']='2'
        with self.assertRaisesRegex(ValueError,'空头'):self.proposal()

    def test_unknown_costs_and_pending_orders_are_blocked(self):
        for field in ('entry_fx','history_fees_usd','history_funding_usd'):
            before=self.inputs[field];self.inputs[field]=''
            with self.assertRaises(ValueError):self.proposal()
            self.inputs[field]=before
        self.inputs['costs_confirmed']=False
        with self.assertRaises(ValueError):self.proposal()
        self.inputs['costs_confirmed']=True;self.mt5['orders']=[{'ticket':'99'}]
        with self.assertRaisesRegex(ValueError,'挂单'):self.proposal()

    def test_confirm_is_atomic_idempotent_and_does_not_trade(self):
        r=self.runtime();p=r.adoption_plan(dict(self.inputs,tickets=['1','2']))['preview']
        r.adoption_confirm({'preview_id':p['id']})
        r.adoption_confirm({'preview_id':p['id']})
        self.assertEqual(len(self.engine.state['groups']),1)
        self.assertEqual(len(self.engine.state['orders']),3)
        self.assertTrue(r.reconciled)
        self.assertFalse(self.engine.state['groups'][0]['management_enabled'])
        self.assertFalse(self.engine.state['enabled'])
        self.broker.submit.assert_not_called()
        self.assertEqual([x.args for x in r.terminal.call.call_args_list],[('snapshot',)]*3)

    def test_expired_or_changed_preview_cannot_commit(self):
        r=self.runtime();p=r.adoption_plan(dict(self.inputs,tickets=['1','2']))['preview']
        r.adoption_preview['created_ms']-=121000
        with self.assertRaisesRegex(ValueError,'失效'):r.adoption_confirm({'preview_id':p['id']})
        p=r.adoption_plan(dict(self.inputs,tickets=['1','2']))['preview']
        self.positions[0]['price_open']=4301.
        with self.assertRaisesRegex(ValueError,'变化'):r.adoption_confirm({'preview_id':p['id']})
        self.assertEqual(self.engine.state['groups'],[])

    def test_persist_failure_rolls_back_registration(self):
        p=self.proposal();self.store.commit=Mock(side_effect=OSError('disk full'))
        with self.assertRaises(OSError):adoption.register(self.engine,self.c,p,self.at)
        self.assertEqual(self.engine.state['groups'],[])
        self.assertEqual(self.engine.state['orders'],[])

    def test_original_basis_and_explicit_costs_are_retained(self):
        g=self.registered();v=self.engine.valuation(g,quote(self.c,ask=4303,mt_bid=4300))
        self.assertAlmostEqual(v['gross'],6)
        self.assertAlmostEqual(v['net'],4.3)  # 6 - 1 historic fees - .5 funding - .2 swap
        restored=Engine(self.store,self.broker)
        self.assertEqual(restored.state['groups'][0]['entry'],6)
        self.assertFalse(restored.state['groups'][0]['management_enabled'])
        self.assertTrue(restored.state['recovery'])

    def test_independent_management_and_multiple_ticket_close(self):
        g=self.registered();self.engine.state['recovery']=False
        p=dict(lots=.01,qty=1,contract=100)
        q=quote(self.c,bid=4302.9,ask=4303,mt_bid=4300,mt_ask=4300.1)
        self.engine.tick(self.c,p,q)
        self.broker.submit.assert_not_called()  # adoption remains paused even when profitable
        g['management_enabled']=True
        self.engine.tick(self.c,p,q)
        self.engine.tick(self.c,p,q)
        self.assertEqual(g['status'],'closed')
        calls=[x.args[0] for x in self.broker.submit.call_args_list]
        self.assertEqual([(o['leg'],o['requested']) for o in calls],[('mt5',1),('binance',1)]*2)
        self.assertEqual([o['position'] for o in calls if o['leg']=='mt5'],['1','2'])
        self.assertTrue(all(o['action']=='close' for o in calls))

    def test_both_contraction_and_profit_are_required(self):
        g=self.registered();g['management_enabled']=True;self.engine.state['recovery']=False
        q=quote(self.c,bid=4304.9,ask=4305,mt_bid=4300,mt_ask=4300.1)
        self.engine.tick(self.c,{},q)  # profitable, but not contracted enough
        self.broker.submit.assert_not_called()
        g['history_fees_usd']=100
        self.engine.tick(self.c,{},quote(self.c,bid=4302.9,ask=4303,mt_bid=4300,mt_ask=4300.1))
        self.broker.submit.assert_not_called()  # contracted, but not profitable

    def test_manual_position_change_disarms_management(self):
        r=self.runtime();g=self.registered();r.reconcile();g['management_enabled']=True
        self.positions[0]['lots']=.02
        with self.assertRaisesRegex(ValueError,'票据'):r._guard_import_close()
        self.assertFalse(r.reconciled);self.assertFalse(g['management_enabled'])
        self.assertTrue(self.engine.state['recovery'])
        self.broker.submit.assert_not_called()

    def test_native_close_requires_adopted_identity_not_just_ticket(self):
        original=self.positions[0]
        p=SimpleNamespace(ticket=1,identifier=101,symbol=original['symbol'],type=0,magic=0,
                          price_open=4300.,time_msc=original['time_ms'])
        order=dict(position='1',adopted_position=original)
        self.assertTrue(owned_close_position(p,order,p.symbol,9121701))
        self.assertFalse(owned_close_position(p,{},p.symbol,9121701))
        p.identifier=999
        self.assertFalse(owned_close_position(p,order,p.symbol,9121701))

    def test_management_toggle_does_not_enable_new_entries(self):
        r=self.runtime();g=self.registered();r.reconcile()
        r.adoption_manage(dict(group=g['id'],enabled=True,take_contraction_usd=4,min_net_profit_usd=8))
        self.assertTrue(g['management_enabled']);self.assertFalse(self.engine.state['enabled'])
        self.assertEqual(g['parameters']['min_net_profit_usd'],8)
        r.adoption_manage(dict(group=g['id'],enabled=False))
        self.assertFalse(g['management_enabled']);self.broker.submit.assert_not_called()

    def test_mt5_rejection_does_not_close_binance_hedge(self):
        g=self.registered();g['management_enabled']=True;self.engine.state['recovery']=False
        self.broker.submit.side_effect=lambda o:dict(status='done',qty=0,price=0,error='rejected')
        q=quote(self.c,bid=4302.9,ask=4303,mt_bid=4300,mt_ask=4300.1)
        self.engine.tick(self.c,{},q)
        self.assertEqual([c.args[0]['leg'] for c in self.broker.submit.call_args_list],['mt5'])
        self.assertEqual(self.engine.amounts(g),{'mt5':2,'binance':2})

    def test_imported_basket_stays_out_of_new_entry_basket(self):
        g=self.registered();self.engine.state['recovery']=False
        self.c['strategy'].update(exit_mode='basket',min_net_profit_usd=0)
        q=quote(self.c)
        self.engine.open(self.c,dict(qty=1,lots=.01,contract=100),q)
        self.broker.submit.reset_mock()
        self.engine.tick(self.c,{},quote(self.c,bid=4302.9,ask=4303,mt_bid=4300,mt_ask=4300.1))
        self.assertEqual(g['status'],'open')
        self.assertTrue(all(c.args[0]['group']!=g['id'] for c in self.broker.submit.call_args_list))

    def test_closing_fee_verification_does_not_replace_original_costs(self):
        r=self.runtime();g=self.registered();self.engine.state['recovery']=False
        g['management_enabled']=True
        q=quote(self.c,bid=4302.9,ask=4303,mt_bid=4300,mt_ask=4300.1)
        self.engine.tick(self.c,{},q);self.engine.tick(self.c,{},q);r.quote=q
        closes=[o for o in self.engine.state['orders'] if o['action']=='close']
        r.terminal.call.return_value=[dict(comment=o['id'],volume=.01,commission=-.1,fee=0,swap=-.1)
            for o in closes if o['leg']=='mt5']
        r.binance.trades.return_value=[{'commission':'.1'}];r.binance.commissions_usdt.return_value=.1
        r.binance.income.return_value=[dict(incomeType='FUNDING_FEE',time=g['opened_ms'],income='-.3')]
        r._verify_import_costs(g)
        self.assertEqual(r.binance.trades.call_count,2)  # only newly sent close orders, no synthetic IDs
        self.assertAlmostEqual(g['valuation']['fees'],1.4)
        self.assertAlmostEqual(g['valuation']['binance_funding'],-.8)
        self.assertFalse(g['costs_verified'])  # old fees/FX remain user-confirmed estimates
        self.assertTrue(g['import_closing_costs_checked'])


if __name__=='__main__':unittest.main()
