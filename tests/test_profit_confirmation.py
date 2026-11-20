import copy
import tempfile
import time
import unittest
from pathlib import Path

from trading_engine import Engine
from trading_brokers import PaperBroker
from trading_store import Store
from trading_config import validate
from execution_diagnostics import build_report
from exit_observations import ExitObservations
from exit_rules import normalize, FIELDS
from test_trading_engine import config, quote


class ProfitConfirmationTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.store=Store(Path(self.tmp.name)/'profit.db');self.addCleanup(self.store.close)
        self.c=config();self.c['strategy']['exit_confirmation_ticks']=3
        self.e=Engine(self.store,PaperBroker());self.e.exit_observations=ExitObservations()
        self.p=dict(qty=2,lots=.02,contract=100)
        self.e.start(self.c);self.e.tick(self.c,self.p,quote(self.c))
        self.g=self.e.active()[0];self.e.pause()
        self.seq=int(time.time()*1000)

    def market(self,hit=True):
        q=quote(self.c,bid=4302 if hit else 4306,ask=4302.1 if hit else 4306.1)
        self.seq+=1
        for book in ('binance','mt5'): q[book]['source_time_ms']=self.seq
        return q

    def tick(self,hit=True):
        q=self.market(hit);self.e.tick(self.c,self.p,q);return q

    def progress(self):
        return self.e.exit_confirmations.snapshot(self.g['id'])['forward']

    def test_new_timestamps_at_same_prices_count_but_poll_replays_do_not(self):
        q=self.tick();self.assertEqual(self.progress()['count'],1)
        before=len(self.store.events())
        for _ in range(5):self.e.tick(self.c,self.p,q)
        self.assertEqual(self.progress()['count'],1)
        self.assertEqual(len(self.store.events()),before)  # No per-quote durable writes.
        self.tick();self.assertEqual(self.progress()['count'],2)
        self.assertEqual(self.g['status'],'open')
        self.tick();self.assertEqual(self.g['status'],'closed')
        self.assertFalse(self.e.state['enabled'])  # Pause only stops new entries.
        self.assertEqual(self.g['exit_decision']['confirmation'],dict(count=3,required=3,ready=True))
        row=build_report(self.e.state,[])['groups'][0]
        self.assertEqual(row['exit_decision']['confirmation']['count'],3)
        self.assertEqual(row['parameters']['exit_confirmation_ticks'],3)

    def test_target_or_net_miss_restarts_sequence(self):
        self.tick();self.tick(False);self.assertEqual(self.progress()['count'],0)
        self.tick();self.g['parameters']['min_net_profit_usd']=100
        self.tick();self.assertEqual(self.progress()['count'],0)
        self.g['parameters']['min_net_profit_usd']=0
        self.tick();self.assertEqual(self.progress()['count'],1)
        episodes=self.e.exit_observations.snapshot({self.g['id']:'G1'})['episodes']
        self.assertEqual(episodes[-1]['code'],'WAIT_CONFIRMATION')
        self.assertEqual(episodes[-1]['latest']['confirmation']['required'],3)

    def test_invalid_quotes_reset_without_observer_and_do_not_recount_cached_tick(self):
        q=self.tick();self.e.exit_observations=None
        self.e.observe_exit_wait(self.c,None,'QUOTE_UNAVAILABLE')
        self.assertEqual(self.progress()['count'],0)
        self.e.tick(self.c,self.p,q);self.assertEqual(self.progress()['count'],0)
        self.tick();self.assertEqual(self.progress()['count'],1)
        invalid=self.market();invalid['mt5']['time_ms']=0
        self.e.tick(self.c,self.p,invalid);self.assertEqual(self.progress()['count'],0)

    def test_recovery_rule_revision_cost_change_and_reconnect_reset(self):
        self.tick();self.e.state['recovery']=True
        self.e.tick(self.c,self.p,self.market());self.assertEqual(self.progress()['count'],0)
        self.e.state['recovery']=False;self.tick()
        self.g['exit_rules_revision']=1
        self.tick();self.assertEqual(self.progress()['count'],1)
        self.tick();self.g['costs']['binance_rebate_percent']=20
        self.tick();self.assertEqual(self.progress()['count'],1)
        self.e.exit_confirmations.clear();self.tick();self.assertEqual(self.progress()['count'],1)

    def test_one_sample_legacy_exit_reevaluates_current_quote_after_cost_update(self):
        self.g['parameters'].pop('exit_confirmation_ticks')
        self.g['parameters']['min_net_profit_usd']=100
        q=self.tick();self.assertEqual(self.g['status'],'open')
        self.g['parameters']['min_net_profit_usd']=0
        self.e.tick(self.c,self.p,q);self.assertEqual(self.g['status'],'closed')

    def test_preflight_miss_requires_fresh_confirmation_again(self):
        worse=self.market(False);self.e.close_quote_provider=lambda:worse
        self.tick();self.tick();self.tick()
        self.assertEqual(self.g['status'],'open');self.assertEqual(self.progress()['count'],0)
        self.e.close_quote_provider=None
        self.tick();self.tick();self.assertEqual(self.g['status'],'open')
        self.tick();self.assertEqual(self.g['status'],'closed')

    def test_manual_loss_and_timeout_exits_bypass_profit_confirmation(self):
        # Separate engines avoid reusing a closed group's source watermarks.
        for kind in ('manual','group_loss','total_loss','timeout','unwind'):
            with self.subTest(kind=kind), tempfile.TemporaryDirectory() as tmp:
                store=Store(Path(tmp)/'bypass.db')
                try:
                    e=Engine(store,PaperBroker());c=copy.deepcopy(self.c)
                    c['strategy']['exit_confirmation_ticks']=1000
                    e.start(c);e.tick(c,self.p,quote(c));g=e.active()[0]
                    if kind=='manual':e.request_close(g['id'])
                    elif kind=='unwind':g.update(status='unwinding',reason='开仓异常保护')
                    elif kind=='group_loss':g['parameters'].update(group_loss_enabled=True,group_max_loss_usd=.01)
                    elif kind=='total_loss':c['strategy'].update(total_loss_enabled=True,total_max_loss_usd=.01)
                    else:g['parameters']['max_hold_minutes']=1;g['opened_ms']-=61000
                    e.tick(c,self.p,quote(c,bid=4310,ask=4310.1))
                    self.assertEqual(g['status'],'closed')
                    self.assertNotIn('confirmation',g.get('exit_decision',{}))
                finally:store.close()

    def test_basket_waits_for_every_member_and_membership_change_restarts_count(self):
        self.g['parameters'].update(exit_mode='basket',exit_confirmation_ticks=2)
        self.tick();self.assertEqual(self.progress()['count'],1)
        self.c['strategy'].update(exit_mode='basket',exit_confirmation_ticks=3)
        second=self.e.open(self.c,self.p,quote(self.c))
        second=self.e.active()[-1]
        self.tick();self.assertEqual(self.progress()['count'],1)
        self.tick();self.assertTrue(all(g['status']=='open' for g in (self.g,second)))
        episodes=self.e.exit_observations.snapshot({self.g['id']:'G1',second['id']:'G2'})['episodes']
        self.assertEqual(next(row for row in reversed(episodes) if row['group']=='G1')['code'],'WAIT_BASKET_CONFIRMATION')
        self.tick();self.assertTrue(all(g['status']=='closed' for g in (self.g,second)))
        self.assertTrue(all(g['reason']=='整篮子止盈' for g in (self.g,second)))

    def test_count_validation_and_backward_rule_payload(self):
        rules={key:self.g['parameters'][key] for key in FIELDS}
        rules.pop('exit_confirmation_ticks');self.assertEqual(normalize(rules)['exit_confirmation_ticks'],1)
        for bad in (0,1001,True,1.5,'3'):
            with self.subTest(bad=bad):
                self.c['strategy']['exit_confirmation_ticks']=bad
                with self.assertRaises(ValueError):validate(self.c)
                with self.assertRaises(ValueError):normalize({**rules,'exit_confirmation_ticks':bad})


if __name__=='__main__':unittest.main()
