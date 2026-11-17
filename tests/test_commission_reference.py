import copy
import json
import tempfile
import threading
import time
import unittest
from pathlib import Path
from types import SimpleNamespace as NS
from unittest.mock import Mock

from commission_reference import summarize, native_commission_reference, fee_rate, refresh, attach, safe_references
from test_pair_combinations import native_config, specs
from test_trading_engine import config, quote
from pair_instruments import equal_lot_plan
from trading_config import pair_key, validate
from trading_engine import Engine
from trading_store import Store
from trading_brokers import PaperBroker


class CommissionTests(unittest.TestCase):
    def setUp(self):self.now=int(time.time()*1000)

    def rows(self, symbol='A', action=0, side=0, charge=.06):
        return [dict(order=i, symbol=symbol,entry=action,type=side,volume=.01,commission=-charge,fee=0,
                     time_msc=self.now-i*1000) for i in range(3)]

    def test_upfront_round_turn_charge_is_not_averaged_into_close_fee(self):
        rows=self.rows(charge=.06)+self.rows(action=1,side=1,charge=0)
        ref=summarize(rows,'A',.01,self.now)
        self.assertEqual(ref['profiles']['open_0']['rate'],6)
        self.assertEqual(ref['profiles']['close_1']['rate'],0)
        self.assertEqual(ref['profiles']['open_1']['state'],'insufficient')

    def test_missing_costs_credits_tiered_lots_and_variable_charges_are_not_automatic(self):
        rows=self.rows();rows[0].pop('fee')
        self.assertEqual(summarize(rows,'A',.01,self.now)['profiles']['open_0']['state'],'insufficient')
        rows=self.rows();rows[0]['commission']=.06
        self.assertEqual(summarize(rows,'A',.01,self.now)['profiles']['open_0']['state'],'insufficient')
        self.assertEqual(summarize(self.rows(),'A',.02,self.now)['profiles']['open_0']['samples'],0)
        rows=self.rows();rows[0]['commission']=-.1
        self.assertEqual(summarize(rows,'A',.01,self.now)['profiles']['open_0']['state'],'variable')

    def test_partial_fills_are_combined_by_order_and_symbol(self):
        rows=self.rows()
        extra=copy.deepcopy(rows)
        for r in rows+extra:r['volume']=.005;r['commission']=-.015
        rows+=extra+self.rows(symbol='UNRELATED',charge=9)
        self.assertEqual(summarize(rows,'A',.01,self.now)['profiles']['open_0']['rate'],3)

    def test_all_zero_samples_do_not_silently_confirm_a_commission_free_account(self):
        result=summarize(self.rows(charge=0)+self.rows(action=1,side=1,charge=0),'A',.01,self.now)
        self.assertEqual(result['profiles']['open_0']['state'],'zero_unconfirmed')
        self.assertEqual(result['profiles']['close_1']['state'],'zero_unconfirmed')
        self.assertNotIn('rate',result['profiles']['close_1'])

    def group(self):
        return dict(costs=dict(mt5_fee_auto=True,mt5_short_fee_auto=True,mt5_commission_per_lot_side=9,
                               mt5_short_commission_per_lot_side=8),pair=dict(type='mt5_mt5'),
                    direction='forward',symbol='A',mt5_symbol='B',lots=.01,commission_reference={
                        'binance':summarize(self.rows(side=1,charge=.03)+self.rows(action=1,charge=0),'A',.01,self.now),
                        'mt5':summarize(self.rows(symbol='B',charge=.05)+self.rows(symbol='B',action=1,side=1,charge=.01),'B',.01,self.now)})

    def test_rates_respect_physical_legs_reverse_direction_and_manual_override(self):
        g=self.group()
        self.assertEqual(fee_rate(g,'binance','open'),3);self.assertEqual(fee_rate(g,'binance','close'),0)
        self.assertEqual(fee_rate(g,'mt5','open'),5);self.assertEqual(fee_rate(g,'mt5','close'),1)
        g['direction']='reverse'
        self.assertEqual(fee_rate(g,'mt5','open'),9)
        g['direction']='forward';g['costs']['mt5_fee_auto']=False
        self.assertEqual(fee_rate(g,'mt5','close'),9)
        g['costs']['mt5_fee_auto']=True;g['commission_reference']['mt5']['checked_ms']-=86400001
        self.assertEqual(fee_rate(g,'mt5','close'),9)

    def test_group_snapshot_does_not_expire_into_a_different_fee_while_holding(self):
        g=self.group();g['opened_ms']=self.now
        g['commission_reference']['mt5']['checked_ms']=self.now-1000
        from unittest.mock import patch
        with patch('commission_reference.time.time',return_value=(self.now+86400000*2)/1000):
            self.assertEqual(fee_rate(g,'mt5','close'),1)

    def test_new_group_locks_references_without_changing_existing_group_or_actual_fee(self):
        with tempfile.TemporaryDirectory() as folder:
            store=Store(Path(folder)/'db');self.addCleanup(store.close)
            e=Engine(store,PaperBroker());c=native_config();c['strategy']['mt5_lots']=.01
            c['costs'].update(mt5_fee_auto=True,mt5_short_fee_auto=True)
            refs=self.group()['commission_reference']
            refs['mt5']['symbol']=c['mt5']['symbol'];refs['binance']['symbol']=c['symbol']
            e.commission_reference=dict(key=pair_key(c),legs=refs)
            e.start(c);e.tick(c,equal_lot_plan(c,specs()),quote(c))
            g=e.active()[0];self.assertEqual(fee_rate(g,'mt5','close'),1)
            e.commission_reference['legs']['mt5']['profiles']['close_1']['rate']=100
            self.assertEqual(fee_rate(g,'mt5','close'),1)
            for o in e.state['orders']:o['result'].update(fee=.07,fee_verified=True)
            v=e.valuation(g,quote(c));self.assertAlmostEqual(v['fees'],.14)
            self.assertAlmostEqual(v['estimated_exit_fee'],.01)
            c['mt5']['account']='OTHER';fresh={'costs':c['costs']};attach(e,c,fresh)
            self.assertNotIn('commission_reference',fresh)

    def test_native_read_does_not_export_deal_identifiers_and_requires_usd(self):
        mt=Mock();mt.account_info.return_value=NS(currency='USD')
        mt.history_deals_get.return_value=[NS(_asdict=lambda r=r:r) for r in self.rows()]
        ref=native_commission_reference(mt,'A',.01)
        self.assertEqual(ref['profiles']['open_0']['samples'],3)
        mt.order_send.assert_not_called();mt.account_info.return_value=NS(currency='EUR')
        with self.assertRaises(ValueError):native_commission_reference(mt,'A',.01)
        ref['account']='PRIVATE';ref['profiles']['open_0']['order']='PRIVATE'
        self.assertNotIn('PRIVATE',json.dumps(safe_references({'mt5':ref})))

    def test_background_read_releases_lock_and_rejects_account_switch(self):
        c=config();c['mt5']['adapter']='native';c['costs']['mt5_fee_auto']=True
        runtime=NS(lock=threading.Lock(),config=c,connected=True,engine=NS(),market_meta={})
        monitor=NS(runtime=runtime,stopped=threading.Event());runtime.maintenance=monitor
        terminal=Mock()
        def call(*args,**kw):
            self.assertTrue(runtime.lock.acquire(blocking=False));runtime.lock.release()
            runtime.config=copy.deepcopy(c);runtime.config['mt5']['account']='OTHER'
            return summarize(self.rows(symbol=c['mt5']['symbol']),c['mt5']['symbol'],.01,self.now)
        terminal.call.side_effect=call
        self.assertFalse(refresh(monitor,lambda:terminal));self.assertFalse(hasattr(runtime.engine,'commission_reference'))

    def test_auto_switches_are_boolean_config_fields(self):
        c=config();validate(c);c['costs']['mt5_fee_auto']=1
        with self.assertRaisesRegex(ValueError,'开关'):validate(c)

    def test_history_diagnostics_export_only_known_fields(self):
        from reference_diagnostics import ReferenceDiagnostics
        cache=ReferenceDiagnostics()
        cache.set_chart([dict(state='error',stage='clock',error_code='missing_time',error='PRIVATE',
            account='PRIVATE',clock_samples=[dict(leg='mt5',tick_time_ms=0,tick_time_valid=False,server='PRIVATE')])])
        output=cache.snapshot()
        self.assertNotIn('PRIVATE',json.dumps(output))
        self.assertIs(output['chart_reference'][0]['clock_samples'][0]['tick_time_valid'],False)

    def test_failed_commission_reader_is_discarded_without_pausing_or_journal_write(self):
        c=config();c['mt5']['adapter']='native';c['costs']['mt5_fee_auto']=True
        engine=NS(save=Mock(),pause=Mock())
        runtime=NS(lock=threading.Lock(),config=c,connected=True,engine=engine,market_meta={})
        terminal=Mock();terminal.call.side_effect=TimeoutError()
        monitor=NS(runtime=runtime,stopped=threading.Event(),session_lock=threading.Lock(),terminals={'commission':terminal})
        runtime.maintenance=monitor
        self.assertTrue(refresh(monitor,lambda:terminal))
        terminal.close.assert_called_once();self.assertIsNone(monitor.terminals['commission'])
        engine.save.assert_not_called();engine.pause.assert_not_called()
        self.assertEqual(runtime.market_meta['mt5_commission_reference']['mt5']['state'],'unavailable')
