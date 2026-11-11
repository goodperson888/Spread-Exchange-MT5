"""Exercise native-worker JSON and MT5 API arguments, without any terminal."""
import io
import json
import sys
import time
import unittest
from pathlib import Path
from types import SimpleNamespace as NS
from unittest.mock import Mock, patch

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from trading_brokers import NativeMT5Fills, worker_main


class NativeMT5FillTests(unittest.TestCase):
    def setUp(self):
        self.o=dict(id='gptest-opening',action='open',requested=1,created_ms=int(time.time()*1000),
                    limit=4010,slippage=1)
        self.p=NS(ticket=900,identifier=700,magic=42,type=0,comment=self.o['id'],symbol='XAUUSD',volume=.01)
        self.d=NS(ticket=600,order=500,position_id=700,symbol='XAUUSD',magic=42,
                  type=0,entry=0,comment=self.o['id'],volume=.01,price=4000,commission=-.03,fee=0,swap=0)
        self.h=NS(ticket=500,symbol='XAUUSD',magic=42,type=0,state=4,volume_initial=.01,volume_current=0)
        self.mt=Mock(ORDER_STATE_FILLED=4,ORDER_STATE_CANCELED=2,ORDER_STATE_REJECTED=5,ORDER_STATE_EXPIRED=6)
        self.mt.symbol_info.return_value=NS(name='XAUUSD',trade_contract_size=100,point=.01,filling_mode=2)
        self.mt.positions_get.return_value=(self.p,)
        self.mt.history_deals_get.return_value=(self.d,)
        self.mt.orders_get.return_value=()
        self.mt.history_orders_get.return_value=(self.h,)
        self.f=NativeMT5Fills(self.mt,'XAUUSD',42)
        self.ack=NS(retcode=10009,order=500,deal=600,volume=0,price=0)

    def test_zero_ack_recovers_from_order_ticket_not_deal_and_maps_actual_position(self):
        r=self.f.submitted(self.o,self.ack,100)
        self.assertEqual((r['status'],r['qty'],r['price']),('done',1,4000))
        self.assertEqual((r['ticket'],r['position'],r['position_identifier']),('500','900','700'))
        self.mt.history_deals_get.assert_called_once_with(ticket=500)
        self.assertAlmostEqual(r['fee'],.03)

    def test_normal_ack_uses_one_position_read_and_no_history_requests(self):
        self.ack.volume=.01;self.ack.price=4000
        r=self.f.submitted(self.o,self.ack,100)
        self.assertEqual(r['status'],'done');self.assertEqual(r['position'],'900')
        self.mt.positions_get.assert_called_once_with(symbol='XAUUSD')
        self.mt.history_deals_get.assert_not_called();self.mt.history_orders_get.assert_not_called()
        self.mt.orders_get.assert_not_called()

    def test_history_error_preserves_identifiers_and_continues_reading(self):
        self.mt.history_deals_get.side_effect=TimeoutError('delayed')
        r=self.f.submitted(self.o,self.ack,100)
        self.assertEqual(r['status'],'unknown');self.assertEqual(r['ticket'],'500')
        self.assertEqual(r['deal'],'600');self.assertEqual(r['retcode'],10009)
        self.mt.history_deals_get.side_effect=None
        self.assertEqual(self.f.query({**self.o,'result':r})['qty'],1)

    def test_position_read_failure_does_not_discard_a_valid_fill_acknowledgement(self):
        self.ack.volume=.01;self.ack.price=4000
        self.mt.positions_get.return_value=None
        r=self.f.submitted(self.o,self.ack,100)
        self.assertEqual((r['status'],r['qty'],r['ticket']),('done',1,'500'))
        self.assertNotIn('position',r)
        self.mt.history_deals_get.assert_not_called()

    def test_missing_ack_volume_is_recovered_without_native_json_failure(self):
        self.ack.volume=None
        r=self.run_worker(self.o)
        self.assertTrue(r['ok']);self.assertEqual(r['data']['qty'],1)

    def test_legacy_lost_ticket_uses_exact_position_history_without_dates(self):
        self.o['created_ms']-=2*3600000
        r=self.f.query(self.o)
        self.assertEqual(r['qty'],1)
        self.mt.history_deals_get.assert_called_once_with(position=700)

    def test_zero_ticket_from_timeout_is_not_treated_as_a_real_order_id(self):
        r=self.f.query({**self.o,'result':{'ticket':'0','status':'unknown'}})
        self.assertEqual((r['status'],r['qty'],r['ticket']),('done',1,'500'))
        self.mt.history_deals_get.assert_called_once_with(position=700)

    def test_missing_position_uses_bounded_dates_and_exact_comment(self):
        self.mt.positions_get.return_value=()
        r=self.f.query(self.o)
        self.assertEqual(r['qty'],1);self.assertNotIn('position',r)
        args=self.mt.history_deals_get.call_args.args
        self.assertEqual(len(args),2)
        self.assertLessEqual(args[1].timestamp()-args[0].timestamp(),172801)
        self.d.comment='foreign';self.assertEqual(self.f.query(self.o)['status'],'unknown')

    def test_pending_or_incomplete_history_never_confirms_partial_as_final(self):
        o={**self.o,'result':{'ticket':'500'}}
        self.mt.orders_get.return_value=(NS(ticket=500),)
        self.assertEqual(self.f.query(o)['status'],'unknown')
        self.mt.orders_get.return_value=();self.h.volume_initial=.02
        self.assertEqual(self.f.query(o)['status'],'unknown')
        self.h.state=2;self.h.volume_current=.01
        self.assertEqual(self.f.query(o)['qty'],1)

    def test_empty_history_is_unknown_unless_terminal_order_proves_zero_fill(self):
        self.mt.history_deals_get.return_value=()
        o={**self.o,'result':{'ticket':'500'}}
        self.assertEqual(self.f.query(o)['status'],'unknown')
        self.h.state=2;self.h.volume_current=.01
        r=self.f.query(o);self.assertEqual((r['status'],r['qty']),('done',0))

    def test_foreign_direction_magic_or_duplicate_deals_do_not_confirm(self):
        for attr,value in (('type',1),('entry',1),('magic',77),('symbol','OTHER')):
            old=getattr(self.d,attr);setattr(self.d,attr,value)
            self.assertEqual(self.f.query({**self.o,'result':{'ticket':'500'}})['status'],'unknown')
            setattr(self.d,attr,old)
        self.mt.history_deals_get.return_value=(self.d,self.d)
        self.h.volume_initial=.02
        self.assertEqual(self.f.query({**self.o,'result':{'ticket':'500'}})['status'],'unknown')

    def test_split_deals_aggregate_only_after_terminal_total_matches(self):
        second=NS(**vars(self.d));second.ticket=601;second.volume=.004;second.price=4001
        self.d.volume=.006;self.mt.history_deals_get.return_value=(self.d,second)
        r=self.f.query({**self.o,'result':{'ticket':'500'}})
        self.assertEqual(r['qty'],1);self.assertAlmostEqual(r['price'],4000.4)

    def test_close_uses_sell_exit_deals_and_original_position_ticket(self):
        self.d.type=1;self.d.entry=1;self.h.type=1
        o={**self.o,'id':'gptest-close','action':'close','position':'900','result':{'ticket':'500'}}
        r=self.f.query(o)
        self.assertEqual((r['status'],r['position'],r['qty']),('done','900',1))
        self.mt.positions_get.assert_not_called()

    def test_missing_ticket_index_recovers_by_exact_position_history(self):
        self.mt.history_deals_get.side_effect=lambda *a,**kw: () if 'ticket' in kw else (self.d,)
        r=self.f.query({**self.o,'result':{'ticket':'500'}})
        self.assertEqual((r['status'],r['qty']),('done',1))
        self.assertEqual([c.kwargs for c in self.mt.history_deals_get.call_args_list],
                         [{'ticket':500},{'position':700}])
        self.assertEqual(r['confirmation']['stage'],'confirmed')

    def test_missing_indexes_use_bounded_range_and_exact_order_identity(self):
        foreign=NS(**vars(self.d));foreign.order=501;foreign.ticket=601
        self.mt.history_deals_get.side_effect=lambda *a,**kw: () if kw else (foreign,self.d)
        r=self.f.query({**self.o,'result':{'ticket':'500'}})
        self.assertEqual((r['status'],r['qty'],r['ticket']),('done',1,'500'))
        args=self.mt.history_deals_get.call_args.args
        self.assertLessEqual(args[1].timestamp()-args[0].timestamp(),172800)
        self.assertEqual(r['confirmation']['ticket_matches'],1)

    def test_missing_terminal_ticket_index_recovers_same_order_from_range(self):
        foreign=NS(**vars(self.h));foreign.ticket=501
        self.mt.history_orders_get.side_effect=lambda *a,**kw: () if kw else (foreign,self.h)
        r=self.f.query({**self.o,'result':{'ticket':'500'}})
        self.assertEqual((r['status'],r['qty']),('done',1))
        self.assertEqual(r['confirmation']['reads'][-1]['api'],'orders_range')

    def test_fallback_never_claims_other_order_even_with_same_comment(self):
        self.d.order=501
        r=self.f.query({**self.o,'result':{'ticket':'500'}})
        self.assertEqual((r['status'],r['qty']),('unknown',0))
        self.assertEqual(r['confirmation']['stage'],'deals_missing')
        self.assertEqual(r['confirmation']['ticket_matches'],0)
        self.mt.order_send.assert_not_called()

    def test_exact_deal_recovers_missing_order_id_despite_rewritten_comment(self):
        self.ack.order=0;self.ack.volume=.01;self.d.comment='rewritten';self.p.comment='rewritten'
        r=self.f.submitted(self.o,self.ack,100)
        self.assertEqual((r['status'],r['qty'],r['ticket'],r['deal']),('done',1,'500','600'))
        self.assertFalse(any(c.kwargs.get('ticket')==600 for c in self.mt.history_deals_get.call_args_list))

    def test_failed_history_reports_safe_api_code_and_stage(self):
        self.mt.history_deals_get.return_value=None;self.mt.history_orders_get.return_value=None
        self.mt.last_error.return_value=(-4,'PRIVATE-DO-NOT-EXPORT')
        r=self.f.query({**self.o,'result':{'ticket':'500'}})
        self.assertEqual(r['status'],'unknown')
        self.assertEqual(r['confirmation']['stage'],'order_history_missing')
        self.assertTrue(all(x['error_code']==-4 for x in r['confirmation']['reads'] if x['status']=='unavailable'))
        self.assertNotIn('PRIVATE-DO-NOT-EXPORT',json.dumps(r['confirmation']))

    def test_missing_magic_match_is_visible_without_relaxing_ownership(self):
        self.d.magic=0
        r=self.f.query({**self.o,'result':{'ticket':'500'}})
        self.assertEqual(r['status'],'unknown')
        self.assertEqual(r['confirmation']['stage'],'deals_missing')
        self.assertEqual(r['confirmation']['magic_matches'],0)
        self.assertEqual(r['confirmation']['ticket_matches'],1)

    def test_volume_mismatch_stage_preserves_the_two_values_for_diagnosis(self):
        self.h.volume_initial=.02
        r=self.f.query({**self.o,'result':{'ticket':'500'}})
        self.assertEqual(r['status'],'unknown')
        self.assertEqual(r['confirmation']['stage'],'fill_volume_mismatch')
        self.assertEqual(r['confirmation']['filled_lots'],.02)
        self.assertEqual(r['confirmation']['deal_lots'],.01)

    def run_worker(self, order, command='submit'):
        self.mt.TRADE_RETCODE_DONE=10009;self.mt.TRADE_RETCODE_DONE_PARTIAL=10010
        self.mt.ORDER_FILLING_IOC=1;self.mt.ORDER_FILLING_FOK=0
        self.mt.ORDER_TYPE_BUY=0;self.mt.ORDER_TYPE_SELL=1
        self.mt.TRADE_ACTION_DEAL=1;self.mt.ORDER_TIME_GTC=0
        self.mt.account_info.return_value=NS(login=123,server='TEST',trade_allowed=True,trade_expert=True)
        self.mt.terminal_info.return_value=NS(connected=True,trade_allowed=True,tradeapi_disabled=False)
        self.mt.symbol_info_tick.return_value=NS(ask=4000,bid=3999.9)
        self.mt.order_check.return_value=NS(retcode=0);self.mt.order_send.return_value=self.ack
        messages=[dict(command='init',settings=dict(account='123',server='TEST',symbol='XAUUSD'),magic=42),
                  dict(command=command,order=order)]
        output=io.StringIO()
        with patch.dict(sys.modules,{'MetaTrader5':self.mt}),patch('sys.stdin',io.StringIO(
                '\n'.join(json.dumps(x) for x in messages))),patch('sys.stdout',output):
            worker_main()
        return [json.loads(x) for x in output.getvalue().splitlines()][-1]

    def test_actual_worker_json_recovers_malformed_success_without_losing_ticket(self):
        r=self.run_worker(self.o)
        self.assertTrue(r['ok']);self.assertEqual(r['data']['qty'],1)
        self.mt.history_deals_get.assert_called_once_with(ticket=500)
        self.mt.order_send.assert_called_once()

    def test_original_zero_ack_and_precheck_survive_history_recovery(self):
        self.ack.retcode_external=7;self.ack.bid=3999.9;self.ack.ask=4000
        self.ack.request_id=123;self.ack.comment='PRIVATE-DO-NOT-EXPORT'
        r=self.run_worker(self.o)['data']
        self.assertEqual(r['price'],4000)
        self.assertEqual(r['receipt']['values']['price'],0)
        self.assertEqual(r['receipt']['values']['volume'],0)
        self.assertEqual(r['receipt']['values']['retcode_external'],7)
        self.assertTrue(r['receipt']['has_request_id'])
        self.assertEqual(r['precheck_receipt']['values']['retcode'],0)
        self.assertGreater(r['ack_received_ms'],0)
        self.assertNotIn('PRIVATE-DO-NOT-EXPORT',json.dumps(r))

    def test_precheck_rejection_keeps_numeric_code_and_never_sends(self):
        self.mt.order_check.side_effect=lambda req:NS(retcode=10019,comment='private')
        r=self.run_worker(self.o)['data']
        self.assertFalse(r['submitted'])
        self.assertEqual(r['precheck_receipt']['values']['retcode'],10019)
        self.mt.order_send.assert_not_called()

    def test_native_rejection_and_none_preserve_safe_failure_details(self):
        self.ack.retcode=10030
        r=self.run_worker(self.o)['data']
        self.assertEqual(r['retcode'],10030)
        self.assertEqual(r['receipt']['values']['retcode'],10030)
        self.mt.reset_mock();self.ack=None;self.mt.last_error.return_value=(-10005,'private')
        r=self.run_worker(self.o)['data']
        self.assertEqual(r['status'],'unknown')
        self.assertEqual(r['native_error_code'],-10005)
        self.assertEqual(r['receipt']['payload_type'],'null')
        self.assertNotIn('private',json.dumps(r))

    def test_actual_worker_queries_terminal_resolved_symbol_without_sending(self):
        self.mt.symbol_info.return_value.name='xauusd'
        self.p.symbol=self.d.symbol=self.h.symbol='xauusd'
        r=self.run_worker({**self.o,'result':{'ticket':'500'}},command='query')
        self.assertTrue(r['ok']);self.assertEqual(r['data']['qty'],1)
        self.mt.positions_get.assert_called_once_with(symbol='xauusd')
        self.mt.order_send.assert_not_called()

    def test_malformed_deal_volume_stays_unknown_with_diagnosis(self):
        self.d.volume='bad'
        r=NativeMT5Fills(self.mt,'XAUUSD',42).query({**self.o,'result':{'ticket':'500'}})
        self.assertEqual(r['status'],'unknown')
        self.assertEqual(r['confirmation']['stage'],'deal_fields_invalid')

    def test_manual_flat_still_recovers_exact_opening_from_range_history(self):
        self.mt.positions_get.return_value=()
        exit_deal=NS(**{**vars(self.d),'ticket':601,'order':501,'type':1,'entry':1,
                       'comment':'manual','magic':0,'price':4001})
        self.mt.history_deals_get.side_effect=lambda *args,**kwargs: () if kwargs else (self.d,exit_deal)
        r=self.f.query({**self.o,'result':{'ticket':'500'}})
        self.assertEqual((r['status'],r['qty'],r['price']),('done',1,4000))
        self.assertEqual(r['position_identifier'],'700')
        self.assertNotIn('position',r)
        self.assertEqual(r['confirmation']['stage'],'confirmed')
        self.mt.order_send.assert_not_called()

    def test_invalid_native_commission_is_not_marked_verified(self):
        self.d.commission=float('nan')
        r=self.f.query({**self.o,'result':{'ticket':'500'}})
        self.assertEqual(r['status'],'unknown');self.assertFalse(r.get('fee_verified',False))

    def test_actual_worker_resolves_close_ticket_by_exact_opening_identity(self):
        self.ack.volume=.01;self.ack.price=3999.9
        o={**self.o,'id':'gptest-close','action':'close','position':None,'opening_id':self.o['id'],
           'position_identifier':'700','limit':3990}
        r=self.run_worker(o)
        self.assertTrue(r['ok']);self.assertEqual(r['data']['position'],'900')
        self.assertEqual(self.mt.order_send.call_args.args[0]['position'],900)

    def test_actual_worker_refuses_another_strategy_position_even_same_magic(self):
        o={**self.o,'action':'close','position':'900','opening_id':'different-opening','limit':3990}
        r=self.run_worker(o)
        self.assertFalse(r['ok']);self.mt.order_send.assert_not_called()


if __name__=='__main__': unittest.main()
