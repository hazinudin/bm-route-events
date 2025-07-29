from src.route_events.bridge.inventory import BridgeInventory, Superstructure, Substructure
import unittest
import json
import pyarrow as pa


class TestSuperstructureFactory(unittest.TestCase):
    def test_from_invij(self):
        with open('tests/domain/bridge/inventory/test_inventory_invij.json') as jf:
            input_dict = json.load(jf)

        sups_dict = input_dict['bangunan_atas']
        sups = Superstructure.from_invij(bridge_id='x', inv_year=2024, data=sups_dict)

        self.assertTrue(type(sups.artable) == pa.Table)

    def test_from_invij_popup(self):
        with open('tests/domain/bridge/inventory/test_inventory_invij.json') as jf:
            input_dict = json.load(jf)

        sups_dict = input_dict['bangunan_atas']
        sups = Superstructure.from_invij_popup(bridge_id='x', inv_year=2024, data=sups_dict)

        self.assertTrue(type(sups.artable) == pa.Table)


class TestSubStructureFactory(unittest.TestCase):
    def test_from_invij(self):
        with open('tests/domain/bridge/inventory/test_inventory_invij.json') as jf:
            input_dict = json.load(jf)

        subs_dict = input_dict['bangunan_bawah']
        subs = Substructure.from_invij(
            bridge_id='x', 
            inv_year=2025, 
            data=subs_dict
        )

        self.assertTrue(type(subs.artable) == pa.Table)


class TestBridgeInventoryFactory(unittest.TestCase):
    def test_from_invij(self):
        with open('tests/domain/bridge/inventory/test_inventory_invij.json') as jf:
            input_dict = json.load(jf)

        inv = BridgeInventory.from_invij(input_dict)
        
        self.assertTrue(type(inv.artable) == pa.Table)
        self.assertTrue(len(inv.artable) == 1)

        self.assertTrue(type(inv.sups.artable) == pa.Table)
        self.assertTrue(len(inv.sups.artable) == 2)

        self.assertTrue(type(inv.subs.artable) == pa.Table)
        self.assertTrue(len(inv.subs.artable) == 3)

        self.assertTrue(type(inv.sups.elements.artable) == pa.Table)
        self.assertTrue(len(inv.sups.elements.artable) == 38)

        self.assertTrue(type(inv.subs.elements.artable) == pa.Table)
        self.assertTrue(len(inv.subs.elements.artable) == 30)
    
    def test_from_invij_pop_up(self):
        with open('tests/domain/bridge/inventory/test_inventory_popup_invij.json') as jf:
            input_dict = json.load(jf)
        
        inv = BridgeInventory.from_invij_popup(input_dict)

        self.assertTrue(len(inv.artable) == 1)
        self.assertTrue(len(inv.sups.artable) == 1)
        self.assertIsNone(inv.sups.subs)
