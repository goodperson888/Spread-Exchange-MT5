import copy
import unittest
from unittest.mock import Mock
from opening_costs import verify


class OpeningCostTests(unittest.TestCase):
    def setUp(self):
        self.order=dict(id='gp-own',action='open',leg='binance',symbol='XAUUSDT',
                        result=dict(status='done',qty=1,price=4000,ticket='500'))
        self.row=dict(id=600,orderId=500,symbol='XAUUSDT',side='SELL',positionSide='SHORT',
                      qty='1',price='4000',commission='.16',commissionAsset='USDT')
        self.client=Mock();self.terminal=Mock()
        self.client.query.return_value=copy.deepcopy(self.order['result'])
        self.client.trades.return_value=[self.row]
        self.client.commissions_usdt.side_effect=lambda rows:sum(float(x['commission']) for x in rows)

    def test_exact_split_fills_and_zero_commissions(self):
        self.client.trades.return_value=[{**self.row,'qty':'.4','commission':'0'},
                                        {**self.row,'id':601,'qty':'.6','commission':'0'}]
        self.assertEqual(verify(self.order,self.client,self.terminal),0)
        self.terminal.call.assert_not_called()

    def test_partial_duplicate_foreign_and_missing_fees_never_replace_estimate(self):
        missing={k:v for k,v in self.row.items() if k!='commission'}
        for rows in ([],[self.row,self.row],[{**self.row,'qty':'.5'}],[{**self.row,'orderId':501}],
                     [{**self.row,'symbol':'OTHER'}],[{**self.row,'side':'BUY'}],[missing],
                     [{**self.row,'commission':'nan'}]):
            with self.subTest(rows=rows):
                self.client.trades.return_value=rows
                with self.assertRaises((ValueError,KeyError,TypeError)): verify(self.order,self.client,self.terminal)

    def test_order_identity_must_match_before_reading_fees(self):
        self.client.query.return_value={**self.order['result'],'ticket':'foreign'}
        with self.assertRaises(ValueError): verify(self.order,self.client,self.terminal)
        self.client.trades.assert_not_called()

    def test_native_fee_requires_verified_original_quantity_and_price(self):
        self.order['leg']='mt5';self.order['symbol']='XAUUSD'
        self.terminal.call.return_value={**self.order['result'],'fee':.04}
        self.assertEqual(verify(self.order,self.client,self.terminal),.04)
        self.client.query.assert_not_called()
        self.terminal.call.return_value={**self.order['result'],'qty':.5,'fee':.02}
        with self.assertRaises(ValueError): verify(self.order,self.client,self.terminal)
