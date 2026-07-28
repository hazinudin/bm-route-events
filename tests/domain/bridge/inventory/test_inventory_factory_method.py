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

    def test_from_sups_only_update(self):
        bridge_id = "0100019"

        # Build the "existing" inventory that the update merges into.
        # The fixture uses placeholder 0.0 coordinates that fail INVIJ profile
        # validation, so fix them for this unit test only.
        with open(f'tests/domain/bridge/inventory/full_inventory_{bridge_id}_2025.json') as jf:
            existing_dict = json.load(jf)
        existing_dict['longitude'] = 120.0
        existing_dict['latitude'] = -5.0
        existing_inv = BridgeInventory.from_invij(existing_dict)

        # Partial superstructure-only payload (snake_case API format).
        with open(f'tests/domain/bridge/inventory/sups_only_payload_{bridge_id}_2025.json') as jf:
            payload = json.load(jf)

        # Normalize to the format expected by SupsOnlyProfileSchema /
        # SuperstructureOnlySchema: uppercase INVIJ keys, and the construction
        # year is accepted under the TAHUN_BANGUN alias (not CONS_YEAR).
        data = json.loads(json.dumps(payload).upper().replace('NULL', 'null'))
        if 'CONS_YEAR' in data:
            data['TAHUN_BANGUN'] = data.pop('CONS_YEAR')

        inv = BridgeInventory.from_sups_only_update(data, existing_inv)

        # Result is a valid BridgeInventory.
        self.assertTrue(type(inv.artable) == pa.Table)

        # Profile fields are overridden from the payload.
        self.assertEqual(inv.id, bridge_id)
        self.assertEqual(inv.length, 13.0)
        self.assertEqual(inv.span_type, 'MBP')

        # Superstructure is merged, with the payload overrides applied.
        self.assertIsNotNone(inv.sups)
        sups = inv.sups.pl_df.to_dicts()
        self.assertEqual(len(sups), 1)
        self.assertEqual(sups[0]['SUPERSTRUCTURE'], 'MBP')
        self.assertEqual(sups[0]['SPAN_LENGTH'], 13.0)

        # Substructure presence is carried over from the existing inventory.
        self.assertEqual(inv.subs is None, existing_inv.subs is None)

    def test_from_sups_only_update_no_existing(self):
        bridge_id = "0100019"

        # Partial superstructure-only payload (snake_case API format).
        with open(f'tests/domain/bridge/inventory/sups_only_payload_{bridge_id}_2025.json') as jf:
            payload = json.load(jf)

        # Normalize to the format expected by SupsOnlyProfileSchema /
        # SuperstructureOnlySchema: uppercase INVIJ keys, and the construction
        # year is accepted under the TAHUN_BANGUN alias (not CONS_YEAR).
        data = json.loads(json.dumps(payload).upper().replace('NULL', 'null'))
        if 'CONS_YEAR' in data:
            data['TAHUN_BANGUN'] = data.pop('CONS_YEAR')

        # No existing inventory -> the factory must build directly from the
        # payload (INSERT case) without any merge.
        inv = BridgeInventory.from_sups_only_update(data, None)

        # Result is a valid BridgeInventory.
        self.assertTrue(type(inv.artable) == pa.Table)

        # Profile fields come straight from the payload.
        self.assertEqual(inv.id, bridge_id)
        self.assertEqual(inv.length, 13.0)
        self.assertEqual(inv.span_type, 'MBP')
        self.assertEqual(inv.inventory_state, 'POPUP')

        # Superstructure is built from the payload only.
        self.assertIsNotNone(inv.sups)
        sups = inv.sups.pl_df.to_dicts()
        self.assertEqual(len(sups), 1)
        self.assertEqual(sups[0]['SUPERSTRUCTURE'], 'MBP')
        self.assertEqual(sups[0]['SPAN_LENGTH'], 13.0)

        # No merge happened: only the partial superstructure columns exist,
        # and no substructure is attached.
        self.assertNotIn('GIRDER_COUNT', inv.sups.pl_df.columns)
        self.assertIsNone(inv.subs)
