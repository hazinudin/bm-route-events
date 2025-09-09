import unittest
import json
import numpy.testing as np_test
from src.route_events.bridge.inventory import BridgeInventory

with open('tests/domain/bridge/inventory/test_inventory_invij.json') as jf:
    input_dict = json.load(jf)

class TestBridgeInventoryMethod(unittest.TestCase):
    def test_inv_year(self):
        inv = BridgeInventory.from_invij(input_dict)

        self.assertTrue(inv.inv_year == 2023)

    def test_has_unique_span_number(self):
        inv = BridgeInventory.from_invij(input_dict)

        self.assertTrue(inv.has_unique_span_number()[('UTAMA', 1)])

    def test_has_monotonic_span_number(self):
        inv = BridgeInventory.from_invij(input_dict)

        self.assertTrue(inv.has_monotonic_span_number('UTAMA'))

    def test_has_monotonic_span_seq_number(self):
        inv = BridgeInventory.from_invij(input_dict)

        self.assertTrue(inv.has_monotonic_span_seq_number()['UTAMA'])

    def test_has_monotonic_subs_number(self):
        inv = BridgeInventory.from_invij(input_dict)

        self.assertTrue(inv.has_monotonic_subs_number()[('UTAMA', 1)])

    def test_total_span_length(self):
        inv = BridgeInventory.from_invij(input_dict)

        np_test.assert_almost_equal(inv.total_span_length('utama'), 200.83)

    def test_select_span_structure(self):
        inv = BridgeInventory.from_invij(input_dict)

        self.assertTrue(inv.select_span_structure('B').shape[0] == 2)

        self.assertTrue(inv.select_span_structure('X').is_empty())

    def test_get_span_numbers(self):
        inv = BridgeInventory.from_invij(input_dict)

        self.assertTrue(
            inv.get_span_numbers('utama')[inv._sups._span_num_col].to_list()[0] == [1,2]
            )
        
    def test_span_subs_count(self):
        inv = BridgeInventory.from_invij(input_dict)
        result = inv.span_subs_count()

        self.assertTrue(result[('UTAMA', 1)]['SPAN_NUMBER'] == 2)
        self.assertTrue(result[('UTAMA', 1)]['SUBS_NUMBER'] == 3)

    def test_span_type(self):
        inv = BridgeInventory.from_invij(input_dict)
        span = inv.span_type

        self.assertTrue(type(span) is str)

    def test_get_main_span_structure(self):
        inv = BridgeInventory.from_invij(input_dict)
        types = inv.get_main_span_structure()

        self.assertTrue(type(types) is list)
        self.assertTrue(len(types) > 0)
        self.assertTrue(type(types[0]) is str)
