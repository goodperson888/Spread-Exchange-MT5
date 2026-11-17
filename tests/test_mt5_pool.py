import copy
import json
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import Mock,patch

import _test_runtime
from server import TradingRuntime
from mt5_pool_config import clean_pool,candidates,seed_rules,endpoint_ref
from mt5_pool import PoolOwner,PoolBroker,endpoint_exposure_matches
from pair_instruments import equal_lot_plan,pair_info
from trading_config import validate,pair_key
from trading_engine import Engine,stamp
from trading_brokers import PaperBroker
from trading_store import Store
from native_pair import NativePairObserver,leg_view
from external_closes import external_order
from execution_latency import LatencyStats
from market_push import QuoteEvents
from execution_diagnostics import build_report
from test_pair_combinations import native_config
import pair_profiles


def config(n=3):
    c=native_config();c['pair'].update(mode='pool',id='gold-pool',name='黄金终端池',short_symbol='XAUUSD')
    c['symbol']='XAUUSD';c['mt5'].update(symbol='XAUUSD',adapter='native')
    c['strategy'].update(mt5_lots=.01,max_groups=5,max_total_lots=.1,entry_spread_usd=2,
                         entry_lower_usd=-2,direction_mode='auto',grid_enabled=False,exit_mode='group')
    c['mt5_pool']=seed_rules(dict(endpoints=[dict(id='t'+str(i),name='MT5 '+str(i),
        terminal_path=f'C:\\MT5-{i}\\terminal64.exe',account=str(1000+i),server='Broker-'+str(i),symbol='XAUUSD',
        max_lots=.05,commission=0,rebate=0,fee_auto=False) for i in range(1,n+1)]),c['strategy'])
    return c


def spec():
    return dict(name='XAUUSD',contract_size_oz=100,volume_min=.01,volume_max=100,volume_step=.01,
                currency_base='XAU',currency_profit='USD',trade_mode=4,linear_usd_verified=True)


class DefinitionTests(unittest.TestCase):
    def test_three_terminals_are_three_fixed_pairs_and_six_directions(self):
        c=config();validate(c);rows=candidates(c)
        self.assertEqual(set(rows),{'t1~t2','t1~t3','t2~t3'})
        self.assertEqual(len({pair_key(x) for x in rows.values()}),3)
        self.assertEqual({x['pair']['id'] for x in rows.values()},{'gold-pool'})
    def test_same_symbol_requires_different_verified_terminal_and_account(self):
        c=config(2);validate(c)
        for key in ['terminal_path','account']:
            changed=copy.deepcopy(c);changed['mt5_pool']['endpoints'][1][key]=changed['mt5_pool']['endpoints'][0][key]
            if key=='account':changed['mt5_pool']['endpoints'][1]['server']=changed['mt5_pool']['endpoints'][0]['server']
            with self.assertRaises(ValueError):validate(changed)
        changed=copy.deepcopy(c);changed['mt5_pool']['endpoints'][1]['terminal_path']='c:/mt5-1/terminal64.exe'
        with self.assertRaises(ValueError):validate(changed)
    def test_each_fixed_pair_keeps_independent_defaults_and_identity(self):
        c=config();before={k:pair_key(v) for k,v in candidates(c).items()}
        c['strategy']['entry_spread_usd']=99
        c['mt5_pool']['rules']['t1~t2']['entry_spread_usd']=10
        rows=candidates(c)
        self.assertEqual(rows['t1~t2']['strategy']['entry_spread_usd'],10)
        self.assertEqual(rows['t1~t3']['strategy']['entry_spread_usd'],2)
        self.assertEqual(before,{k:pair_key(v) for k,v in rows.items()})
        c['mt5_pool']['endpoints'][0]['account']='123456'
        self.assertNotEqual(before['t1~t2'],pair_key(candidates(c)['t1~t2']))
        self.assertEqual(before['t2~t3'],pair_key(candidates(c)['t2~t3']))
    def test_equal_lots_pool_requires_matching_notional_contract(self):
        c=candidates(config(2))['t1~t2'];s={leg:spec() for leg in ('binance','mt5')}
        self.assertEqual(equal_lot_plan(c,s)['lots'],.01)
        s['mt5']['contract_size_oz']=10
        with self.assertRaisesRegex(ValueError,'相同合约大小'):equal_lot_plan(c,s)
    def test_create_activate_pool_keeps_local_paths_but_no_api_credentials(self):
        base=native_config();base['binance']['api_secret']='SECRET'
        p=pair_profiles.clean_profile(dict(name='池',type='mt5_mt5',mode='pool',short_symbol='XAUUSD',long_symbol='XAUUSD'),base)
        c=pair_profiles.activate(base,p);validate(c)
        self.assertEqual(c['pair']['mode'],'pool');self.assertEqual(len(c['mt5_pool']['endpoints']),2)
        self.assertNotIn('SECRET',json.dumps(pair_profiles.capture(c)))
    def test_incomplete_account_can_be_saved_but_cannot_connect(self):
        c=config(2);c['mt5_pool']['endpoints'][0]['account']='';validate(c)
        with self.assertRaises(ValueError):clean_pool(c['mt5_pool'],complete=True)


class PoolTests(unittest.TestCase):
    def setUp(self):
        tmp=tempfile.TemporaryDirectory();self.addCleanup(tmp.cleanup)
        self.store=Store(Path(tmp.name)/'pool.db');self.addCleanup(self.store.close)
        r=self.r=TradingRuntime.__new__(TradingRuntime)
        r.lock=threading.RLock();r.store=self.store;r.engine=Engine(self.store,PaperBroker());r.config=config()
        r.market_wake=threading.Event();r.quote_events=QuoteEvents();r.connected=True;r.reconciled=True;r.latency=LatencyStats()
        r.pool=None;r.quote=None;r.last_error='';r.position_report=None
        endpoints=r.config['mt5_pool']['endpoints'];sessions={e['id']:Mock() for e in endpoints}
        self.owner=r.pool=PoolOwner(r,r.config,sessions,{e['id']:spec() for e in endpoints},{})
        self.addCleanup(self.owner.close)
        r.pump=self.owner.pump;r.engine.multi_pair=True;r.engine.quote_provider=r._pool_execution_quote
        r.engine.close_quote_provider=r._pool_close_quote;r.engine.entry_preflight=r._pool_entry_guard
        r.engine.start(r.config)
        self.books([4005,4000,3998])
    def books(self,prices,ages=None):
        now=stamp()
        for i,(e,p) in enumerate(zip(self.owner.endpoints,prices)):
            self.owner.pump.ingest(e,dict(bid=p,ask=p+.1,time_ms=now,observed_ms=now-(ages[i] if ages else 0)))
    def test_selects_best_executable_pair_and_binds_both_original_endpoints(self):
        self.r._pool_poll();groups=self.r.engine.active();self.assertEqual(len(groups),1)
        g=groups[0];self.assertEqual(g['status'],'open')
        self.assertEqual([e['id'] for e in g['pair']['endpoints'].values()],['t1','t3'])
        self.assertEqual({o['endpoint']['id'] for o in self.r.engine.state['orders']},{'t1','t3'})
        self.assertEqual(len(self.owner.ranking),6)
    def test_stale_third_terminal_does_not_hide_healthy_pair(self):
        self.books([4005,4000,3990],[0,0,10000])
        winner=self.owner.rank(self.owner.pump.all())
        self.assertEqual(winner['edge'],'t1~t2')
        self.assertTrue(all(not row['ready'] for row in self.owner.ranking if 't3' in row['edge']))
    def test_selected_chart_never_changes_order_ownership_or_other_pair_valuation(self):
        self.r._pool_poll();g=self.r.engine.active()[0];key=g['key']
        self.owner.chart_key='t2~t3';self.books([4005,4000,3998]);self.r._pool_poll()
        self.assertEqual(g['key'],key);self.assertEqual(g['status'],'open')
        self.assertEqual(len(self.r.engine.active()),1)
        self.assertNotEqual(self.owner.pump.read()[0]['key'],g['key'])
    def test_close_uses_original_pair_after_best_candidate_changes(self):
        self.r._pool_poll();g=self.r.engine.active()[0]
        self.r.engine.pause();self.owner.chart_key='t1~t2'
        self.books([4001,3990,3998]);self.r.engine.request_close(g['id']);self.r._pool_poll()
        self.assertEqual(g['status'],'closed')
        closes=[o for o in self.r.engine.state['orders'] if o['action']=='close']
        self.assertEqual({o['endpoint']['id'] for o in closes},{'t1','t3'})
        self.assertEqual({o['endpoint']['id'] for o in closes if o['leg']=='mt5'},{'t3'})
    def test_quota_applies_to_shared_terminal_across_fixed_pairs(self):
        self.r._pool_poll();g=self.r.engine.active()[0]
        self.owner.contexts['t1~t2'].config['pair']['endpoints']['binance']['max_lots']=.01
        self.assertIn('额度',self.owner.block(self.owner.contexts['t1~t2'],'forward'))

    def test_reverse_group_quote_refresh_preserves_direction_and_original_sessions(self):
        from pair_instruments import direction_quote
        self.books([3998,4000,4005]);self.r._pool_poll();g=self.r.engine.active()[0]
        self.assertEqual(g['direction'],'reverse');self.r.engine.pause()
        q=self.r._pool_group_quote(g);projected=direction_quote(q,g)
        self.assertEqual(self.r._pool_close_quote(projected)['exit'],projected['exit'])
        self.r.engine.request_close(g['id']);self.r._pool_poll()
        self.assertEqual(g['status'],'closed')
        self.assertEqual({o['endpoint']['id'] for o in self.r.engine.state['orders']},{'t1','t3'})
    def test_fees_change_ranking_and_are_not_an_unconditional_zero(self):
        ctx=self.owner.contexts['t1~t3'];ctx.config['costs']['mt5_short_commission_per_lot_side']=1000
        self.assertEqual(self.owner.rank(self.owner.pump.all())['edge'],'t1~t2')
    def test_quote_ingestion_wakes_strategy_without_disk_io(self):
        with patch.object(self.r.store,'sample',side_effect=AssertionError('disk on quote thread')):
            self.books([4006,4000,3998]);self.assertTrue(self.r.market_wake.is_set())

    def test_valid_ea_quote_uses_endpoint_identity_without_native_poll(self):
        pump=self.owner.pump;e=self.owner.endpoints[0];at=stamp()
        self.r.push_bridge=Mock();self.r.push_bridge.quote.return_value=dict(bid=4001,ask=4002,time_ms=at,observed_ms=at)
        ingest=pump.ingest
        def receive(endpoint,book):ingest(endpoint,book);pump.stopped.set()
        with patch.object(pump,'ingest',side_effect=receive),patch('mt5_pool.Terminal') as terminal:
            pump._loop(e)
        terminal.assert_not_called()
        settings=self.r.push_bridge.quote.call_args.args[0]
        self.assertEqual((settings['account'],settings['server'],settings['symbol']),(e['account'],e['server'],e['symbol']))

    def test_external_close_history_with_identical_ticket_numbers_routes_to_each_original_terminal(self):
        self.r._pool_poll();g=self.r.engine.active()[0];g['mode']='live'
        ctx=next(x for x in self.owner.contexts.values() if x.engine.key==g['key'])
        by_endpoint={}
        for leg in ('binance','mt5'):
            o=next(x for x in self.r.engine.state['orders'] if x['group']==g['id'] and x['leg']==leg)
            o['result'].update(ticket='500',position='900',position_identifier='700')
            original=dict(ticket='600',order='500',position_id='700',symbol=o['symbol'],magic=g['magic'],
                entry=0,type=o['native_position_side'],comment=o['id'],volume=.01,price=o['result']['price'],
                commission=-.03,fee=0,swap=0,time_msc=g['opened_ms'],reason=3)
            deals=[original,{**original,'ticket':'800','order':'1000','entry':1,'type':1-original['type'],
                'magic':0,'comment':'manual','time_msc':g['opened_ms']+100,'reason':0}]
            def call(command,rows=deals,**kw):
                if command=='snapshot':return dict(positions=[],orders=[],allowed=True)
                if command=='external_history':return copy.deepcopy(rows)
                if command=='query':return dict(status='done',qty=kw['order']['requested'],price=4000,fee=.03,fee_verified=True)
                raise AssertionError(command)
            terminal=Mock();terminal.call.side_effect=call
            ctx.maintenance.terminals[('history',leg)]=terminal;by_endpoint[o['endpoint']['id']]=terminal
        ctx.maintenance.step('history')
        self.assertEqual(g['status'],'closed')
        rows=[o for o in self.r.engine.state['orders'] if o.get('source')=='external']
        self.assertEqual(len(rows),2);self.assertEqual(len({o['id'] for o in rows}),2)
        self.assertEqual({o['endpoint']['id'] for o in rows},{'t1','t3'})
        for terminal in by_endpoint.values():self.assertTrue(any(c.args[0]=='external_history' for c in terminal.call.call_args_list))
    def test_restart_recovers_immutable_identity_and_requires_reconciliation(self):
        self.r._pool_poll();g=self.r.engine.active()[0]
        restored=Engine(self.store,PaperBroker());rg=restored.active()[0]
        self.assertEqual(rg['pair']['endpoints'],g['pair']['endpoints']);self.assertTrue(restored.state['recovery'])
        self.assertFalse(restored.state['enabled'])
    def test_same_ticket_at_different_endpoints_backfills_distinct_external_orders(self):
        self.r._pool_poll();g=self.r.engine.active()[0]
        results=[]
        for leg in ('binance','mt5'):
            _,view=leg_view(self.r.engine.state,g,leg)
            results.append(external_order(view,'mt5',[dict(ticket='123')],.01,4000,0,'456',stamp()))
        self.assertNotEqual(results[0]['id'],results[1]['id'])
    def test_scope_cannot_clear_other_groups_or_mix_quote_keys(self):
        self.r._pool_poll();g=self.r.engine.active()[0]
        ctx=self.owner.contexts['t1~t2'];self.assertEqual(ctx.engine.active(),[])
        self.r.engine.entry_candidate_key=None
        self.r.engine.tick(ctx.config,ctx.plan,self.owner.pump.all()['t1~t2'])
        self.assertEqual(g['status'],'open')
    def test_global_uncertain_group_blocks_new_entries_across_pool(self):
        self.r._pool_poll();g=self.r.engine.active()[0];g['status']='attention'
        self.r.engine.state['last_open_ms']=0
        winner=self.owner.rank(self.owner.pump.all());self.assertIsNone(winner)
    def test_original_session_routing_rejects_mutated_endpoint_even_with_same_symbol(self):
        self.r._pool_poll();order=self.r.engine.state['orders'][0]
        sessions={e['id']:Mock() for e in self.owner.endpoints}
        sessions['t1'].call.return_value=dict(status='done',qty=1.,price=4000)
        broker=PoolBroker(sessions,self.owner.endpoints)
        result=broker.submit(order);self.assertEqual(result['qty'],.01)
        self.assertEqual(sessions['t1'].call.call_count,1)
        changed=copy.deepcopy(order);changed['endpoint']['identity']='different-account'
        with self.assertRaises(ValueError):broker.query(changed)
        self.assertFalse(sessions['t2'].call.called)

    def test_original_two_sessions_close_concurrently_after_both_intents_are_durable(self):
        self.r._pool_poll();g=self.r.engine.active()[0];self.r.engine.pause()
        gate=threading.Barrier(2);sent=[]
        def call(command,**kw):
            self.assertEqual(command,'submit');o=kw['order']
            durable=[x for x in self.store.load()['orders'] if x['action']=='close']
            self.assertEqual({x['endpoint']['id'] for x in durable},{'t1','t3'})
            sent.append(o);gate.wait(timeout=2)
            return dict(status='done',qty=o['requested'],price=4000,fee=0)
        sessions={e['id']:Mock() for e in self.owner.endpoints}
        for s in sessions.values():s.call.side_effect=call
        self.r.engine.broker=PoolBroker(sessions,self.owner.endpoints)
        self.r.engine.request_close(g['id']);self.r._pool_poll()
        self.assertEqual(g['status'],'closed');self.assertEqual(len(sent),2)
        sessions['t2'].call.assert_not_called()

    def test_unknown_peer_close_blocks_new_entry_without_duplicate_submit(self):
        self.r._pool_poll();g=self.r.engine.active()[0];self.r.engine.pause()
        sessions={e['id']:Mock() for e in self.owner.endpoints}
        sessions['t1'].call.return_value=dict(status='done',qty=1.,price=4000,fee=0)
        sessions['t3'].call.return_value=dict(status='unknown',qty=0)
        self.r.engine.broker=PoolBroker(sessions,self.owner.endpoints)
        self.r.engine.request_close(g['id']);self.r._pool_poll()
        self.assertNotEqual(g['status'],'closed');self.assertFalse(self.r.engine.state['enabled'])
        self.assertIsNone(self.owner.rank(self.owner.pump.all()))
        self.r._pool_poll()
        self.assertEqual(sum(c.args[0]=='submit' for c in sessions['t3'].call.call_args_list),1)
        sessions['t2'].call.assert_not_called()

    def test_export_replaces_terminal_identities_with_consistent_aliases(self):
        self.r._pool_poll();report=build_report(self.r.engine.state,[])
        refs=report['groups'][0]['pair']['endpoints']
        self.assertEqual(set(refs.values()),{'T1','T2'})
        for o in report['orders']:self.assertEqual(o['endpoint'],refs[o['leg']])
        encoded=json.dumps(report)
        for e in self.owner.endpoints:
            for private in (e['terminal_path'],e['server'],endpoint_ref(e)['identity']):
                self.assertNotIn(private,encoded)


class PoolHistoryTests(unittest.TestCase):
    def test_same_millisecond_retains_every_fixed_pair_and_duplicates_are_immutable(self):
        with tempfile.TemporaryDirectory() as d:
            s=Store(Path(d)/'history.db')
            try:
                at=stamp()-1000
                for key in ('mt5_edge|paper|1|pool|one','mt5_edge|paper|1|pool|two'):
                    s.sample(dict(time_ms=at,key=key,entry=2))
                    s.sample(dict(time_ms=at,key=key,entry=99))
                    self.assertEqual([q['entry'] for q in s.read_samples(key,at-1)],[2])
                old=at-2*86400000
                s.sample(dict(time_ms=old,key=key,entry=3))
                self.assertEqual([q['entry'] for q in s.read_samples(key,old-1,until=old+1)],[3])
            finally:s.close()

    def test_each_history_request_uses_its_own_terminal_clock(self):
        from chart_reference import fetch_terminal_bars
        c=candidates(config(2))['t1~t2'];start=1790000040000;end=start+120000
        offsets=[7200000,10800000];sessions=[Mock(),Mock()]
        for session,offset in zip(sessions,offsets):
            session.call.return_value=dict(symbol='XAUUSD',tick_time_ms=end+offset,observed_ms=end,
                current_bar_ms=end+offset,bars=[dict(t=start+offset,close=4000)])
        with patch('trading_brokers.Terminal',side_effect=sessions) as factory, \
                patch('chart_reference.native_clock',side_effect=offsets):
            rows=fetch_terminal_bars(c,'M1',start,end,threading.Event())
        self.assertEqual(len(rows),1);self.assertEqual(rows[0]['time_ms'],start)
        self.assertNotEqual(factory.call_args_list[0].args[0]['terminal_path'],factory.call_args_list[1].args[0]['terminal_path'])
        for session,offset in zip(sessions,offsets):
            self.assertEqual(session.call.call_args.kwargs['start_ms'],start+offset)
            session.close.assert_called_once()


if __name__=='__main__':unittest.main()
