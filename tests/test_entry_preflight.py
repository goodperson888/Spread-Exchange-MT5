import time
import unittest
from types import SimpleNamespace as NS

from entry_preflight import EntryPreflight


class FakeBinance:
    offset = 17
    _clock_anchor = (1000000, 0)
    sync_rtt_ms = 4
    position_mode = 'hedge'

    def __init__(self, fail=None):
        self.fail = fail
        self.synced = 0
        self.tested = 0

    def sync(self): self.synced += 1
    def preflight(self, sync_clock=True):
        if self.fail == 'account': raise ValueError('canTrade=false')
        return {'position_mode':'hedge','available':1000}
    def test_order(self, order, spec):
        self.tested += 1
        if self.fail == 'test': raise ValueError('测试订单失败')
        return {}


class EntryPreflightTests(unittest.TestCase):
    def context(self):
        return {'key':'pair','symbol':'XAUUSDT','position_mode':'hedge','price':4350,'qty':1}

    def test_near_threshold_runs_test_order_and_expires(self):
        b=FakeBinance();p=EntryPreflight(b,b,{},clock=time.monotonic,start=False)
        p.update(self.context());p._step()
        self.assertTrue(p.status()['ready']);self.assertEqual(b.tested,1);self.assertEqual(b.synced,1)
        p.result['started']-=p.TTL+1
        self.assertFalse(p.status()['ready']);self.assertEqual(p.status()['state'],'expired')

    def test_failed_test_blocks_readiness_without_retrying_order(self):
        b=FakeBinance('test');p=EntryPreflight(b,b,{},clock=time.monotonic,start=False)
        p.update(self.context());p._step()
        self.assertFalse(p.status()['ready']);self.assertEqual(p.status()['state'],'failed')
        self.assertIn('测试订单失败',p.status()['message'])

    def test_context_change_clears_previous_result(self):
        b=FakeBinance();p=EntryPreflight(b,b,{},clock=time.monotonic,start=False)
        p.update(self.context());p._step();self.assertTrue(p.status()['ready'])
        p.update(dict(self.context(),key='other'))
        self.assertFalse(p.status()['ready']);self.assertEqual(p.status()['state'],'waiting')

    def test_quantity_or_mode_change_invalidates_ready_check_immediately(self):
        for update in ({'qty':2},{'position_mode':'one_way'},{'symbol':'OTHER'}):
            b=FakeBinance();p=EntryPreflight(b,b,{},clock=lambda:100.0,start=False)
            context=self.context();p.update(context);p._step();self.assertTrue(p.status()['ready'])
            p.update(dict(context,**update));self.assertFalse(p.status()['ready'])
            # Context changes need no extra five-second wait before rechecking.
            p._step()
            if 'position_mode' in update:
                self.assertEqual(p.status()['state'],'failed')
                self.assertIn('持仓模式变化',p.status()['message'])
            else:self.assertTrue(p.status()['ready'])

    def test_price_updates_reuse_ready_check_and_valid_clock(self):
        now=[100.0];b=FakeBinance();p=EntryPreflight(b,b,{},clock=lambda:now[0],start=False)
        p.update(self.context());p._step()
        p.update(dict(self.context(),price=4351));self.assertTrue(p.status()['ready'])
        now[0]+=5;p._step()
        self.assertEqual(b.tested,2);self.assertEqual(b.synced,1)
        now[0]+=30;p._step();self.assertEqual(b.synced,2)


if __name__ == '__main__': unittest.main()
