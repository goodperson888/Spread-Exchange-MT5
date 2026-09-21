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


if __name__ == '__main__': unittest.main()
