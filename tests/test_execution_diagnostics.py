import json
import tempfile
import threading
import unittest
from pathlib import Path

from execution_diagnostics import build_report, export_report
from trading_store import Store


class DiagnosticsTests(unittest.TestCase):
    def test_profit_confirmation_export_keeps_only_count_evidence(self):
        state={'groups':[{'id':'private','parameters':{'exit_confirmation_ticks':5,
            'entry_forward_confirmation_ticks':3,'entry_reverse_confirmation_mode':'equal'},
            'exit_decision':{'confirmation':{'count':5,'required':5,'ready':True,'account':'SECRET'}}},
            {'id':'old','parameters':{},'exit_decision':{}}],'orders':[]}
        rows=build_report(state,[])['groups']
        self.assertEqual(rows[0]['exit_decision']['confirmation'],{'count':5,'required':5,'ready':True})
        self.assertEqual(rows[0]['parameters']['entry_forward_confirmation_ticks'],3)
        self.assertEqual(rows[0]['parameters']['entry_reverse_confirmation_mode'],'equal')
        self.assertNotIn('confirmation',rows[1]['exit_decision'])
        self.assertNotIn('exit_confirmation_ticks',rows[1]['parameters'])
        self.assertNotIn('SECRET',json.dumps(rows))

    def test_local_mt5_slippage_rejection_keeps_unsent_evidence_and_redacts_fields(self):
        result=dict(status='done',qty=0,price=0,submitted=False,
            error_code='MT5_SLIPPAGE_PRECHECK',error_kind='local_validation',error='SECRET',
            execution_precheck=dict(bid=3999.9,ask=4000,price=4000,limit=3999,slippage=1,
                point=.01,tick_time_ms=1234,account='SECRET',comment='SECRET',invalid=float('nan')))
        state={'groups':[{'id':'private'}],'orders':[{'id':'order','group':'private',
            'result':result,'initial_result':result}]}
        row=build_report(state,[])['orders'][0]
        for field in ('result','initial_result'):
            evidence=row[field]
            self.assertFalse(evidence['submitted'])
            self.assertEqual(evidence['error_code'],'MT5_SLIPPAGE_PRECHECK')
            self.assertEqual(evidence['error_kind'],'local_validation')
            self.assertEqual(evidence['execution_precheck'],{k:result['execution_precheck'][k]
                for k in ('bid','ask','price','limit','slippage','point','tick_time_ms')})
        self.assertNotIn('SECRET',json.dumps(row,allow_nan=False))

    def test_mt5_execution_request_exports_only_finite_numeric_evidence(self):
        evidence={'price':3720.1,'bid':3720.1,'ask':3720.3,'point':.001,'deviation_points':300,
                  'trade_execution_mode':2,'filling':1,'account':'SECRET','comment':'SECRET','magic':123}
        state={'groups':[{'id':'private'}],'orders':[{'id':'order','group':'private','result':{
            'execution_request':{**evidence,'point':float('nan'),'filling':True}},
            'initial_result':{'execution_request':evidence}}]}
        report=build_report(state,[])
        row=report['orders'][0]
        self.assertEqual(row['result']['execution_request'],{k:evidence[k] for k in ('price','bid','ask','deviation_points','trade_execution_mode')})
        self.assertEqual(row['initial_result']['execution_request']['point'],.001)
        self.assertEqual(row['initial_result']['execution_request']['filling'],1)
        self.assertNotIn('SECRET',json.dumps(report));self.assertNotIn('magic',row['initial_result']['execution_request'])

    def test_stage_timings_export_is_bounded_and_rejects_arbitrary_fields(self):
        state={'groups':[{'id':'private'}],'orders':[{'id':'private-order','group':'private','result':{
            'timings':{'terminal_queue_ms':0,'order_send_ms':875,'private-account':'SECRET',
                       'order_check_ms':float('nan'),'worker_submit_ms':-1,'fill_normalize_ms':True}}}]}
        report=build_report(state,[])
        self.assertEqual(report['orders'][0]['result']['timings'],{'terminal_queue_ms':0,'order_send_ms':875})
        self.assertNotIn('SECRET',json.dumps(report))

    def test_allowlist_excludes_private_fields_and_keeps_execution_evidence(self):
        secret='PRIVATE-DO-NOT-EXPORT'
        state={'key':secret,'config':{'api_secret':secret},'groups':[
            {'id':secret,'mode':'live','status':'closed','key':secret,'account':secret,
             'entry':5.2,'entry_signal':5.7,'parameters':{'api_key':secret,'max_unhedged_ms':1500},
             'costs':{'usdt_usd':1,'account':secret},'reason':secret}],
            'orders':[{'id':secret,'group':secret,'leg':'binance','action':'open',
                'requested':1,'quote_spread':5.5,'actual_spread':5.2,'spread_slippage':-.5,
                'result':{'status':'done','qty':1,'price':4305.5,'ticket':secret,'deal':secret,
                          'position_identifier':secret,'error':secret,'request_ms':20,
                          'retcode':10009,'ack_volume':0,'ack_price':0},
                'quote_context':{'binance':{'bid':4305.5,'api_key':secret,'observed_ms':1000}}}]}
        events=[{'kind':'order_reconciled','time':1200,'data':{'id':secret,'query_ms':15,'error':secret}},
                {'kind':'arbitrary_private_event','data':{'group':secret,'error':secret}}]
        report=build_report(state,events)
        self.assertNotIn(secret,json.dumps(report))
        self.assertEqual(report['orders'][0]['group'],'G1')
        self.assertEqual(report['orders'][0]['spread_slippage'],-.5)
        self.assertEqual(report['orders'][0]['result']['request_ms'],20)
        self.assertEqual(report['orders'][0]['result']['retcode'],10009)
        self.assertEqual(report['orders'][0]['result']['ack_volume'],0)
        self.assertEqual(report['events'][0]['data'],{'order':'O1','query_ms':15})
        self.assertEqual(len(report['events']),1)

    def test_native_connection_failure_exports_fixed_phase_timings_without_private_error(self):
        secret='PRIVATE-DO-NOT-EXPORT'
        report=build_report({'groups':[],'orders':[]},[{'kind':'native_pair_connect','time':123,'data':{
            'state':'failed','stage':'读取双品种首轮报价','started_ms':10,'elapsed_ms':25000,
            'message':secret,'account':secret,'timings':[{'stage':'初始化 MT5 执行会话','elapsed_ms':200,'path':secret},
                {'stage':secret,'elapsed_ms':300}]}}])
        self.assertNotIn(secret,json.dumps(report))
        data=report['events'][0]['data']
        self.assertEqual(data['state'],'failed');self.assertEqual(data['stage'],'读取双品种首轮报价')
        self.assertEqual(data['timings'],[{'stage':'初始化 MT5 执行会话','elapsed_ms':200}])

    def test_group_limit_and_missing_legacy_fields(self):
        report=build_report({'groups':[{'id':str(i)} for i in range(205)],
                             'orders':[{'id':'old','group':'0'},{'id':'new','group':'204'}]},[])
        self.assertEqual(len(report['groups']),200)
        self.assertEqual(len(report['orders']),1)
        self.assertNotIn('request_ms',report['orders'][0]['result'])
        self.assertNotIn('elapsed_ms',report['groups'][0])

    def test_mt5_history_probe_is_exported_without_raw_identifiers(self):
        report=build_report({'groups':[],'orders':[], 'mt5_history_probe':{
            'schema_version':1,'symbol':'XAUUSD','queries':[{'query':'position','status':'empty'}],
            'row_count':1,'fields':['entry','position_id'],
            'rows':[{'position_id':'P1','entry':1,'comment_present':True,'comment_kind':'manual'}]}},[])
        self.assertEqual(report['mt5_history_probe']['rows'][0]['position_id'],'P1')
        self.assertNotIn('ticket',json.dumps(report['mt5_history_probe']))

    def test_exit_rule_audit_exports_only_safe_fields_and_anonymous_group_ids(self):
        secret='PRIVATE-DO-NOT-EXPORT'
        rules={'target_mode':'absolute','exit_spread_usd':1,'require_net_profit':False,
               'min_net_profit_usd':0,'api_secret':secret,'account':secret}
        state={'groups':[{'id':secret,'parameters':rules,'exit_rules_revision':2}]}
        events=[{'kind':'exit_rules_updated','time':123,'data':{'changes':[
            {'group':secret,'revision':2,'before':rules,'after':{**rules,'exit_spread_usd':2},'ticket':secret}]}}]
        report=build_report(state,events)
        self.assertNotIn(secret,json.dumps(report))
        change=report['events'][0]['data']['changes'][0]
        self.assertEqual(change['group'],'G1');self.assertEqual(change['revision'],2)
        self.assertEqual(change['before']['exit_spread_usd'],1)
        self.assertEqual(change['after']['exit_spread_usd'],2)
        self.assertFalse(change['after']['require_net_profit'])

    def test_export_uses_committed_snapshot_without_store_lock_or_writer_wait(self):
        with tempfile.TemporaryDirectory() as tmp:
            path=Path(tmp)/'trading.sqlite3';store=Store(path)
            try:
                store.commit({'groups':[{'id':'committed'}],'orders':[]},'boot')
                # Keep the writer transaction and Store.lock held while a reader exports.
                with store.lock:
                    store.db.execute('BEGIN IMMEDIATE')
                    store.db.execute('UPDATE state SET value=?', (json.dumps({'groups':[]}),))
                    reports=[];errors=[]
                    def read():
                        try: reports.append(export_report(path))
                        except Exception as exc: errors.append(exc)
                    thread=threading.Thread(target=read);thread.start();thread.join(2)
                    self.assertFalse(thread.is_alive())
                    self.assertFalse(errors)
                    self.assertEqual(len(reports[0]['groups']),1)
                    store.db.rollback()
            finally:
                store.close()


if __name__=='__main__':unittest.main()
