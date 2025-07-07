from src.route_events.bridge.inventory import BridgeInventory, BridgeInventoryRepo
import unittest
import pyarrow as pa
import json
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os


load_dotenv('tests/dev.env')
HOST = os.getenv('DB_HOST')
USER = os.getenv('MISC_USER')
PWD = os.getenv('MISC_PWD')

class TestInventoryRepo(unittest.TestCase):
    def test_get_by_bridge_id(self):

        engine = create_engine(f"oracle+oracledb://{USER}:{PWD}@{HOST}:1521/geodbbm")
        
        # Bridge ID for query
        bridge_id = '3500725'

        repo = BridgeInventoryRepo(sql_engine=engine)
        repo.inv_table_name = 'NAT_BRIDGE_PROFILE_DEV'
        repo.sups_table_name = 'NAT_BRIDGE_SPAN_DEV'
        repo.subs_table_name = 'NAT_BRIDGE_ABT_DEV' 
        repo.sups_el_table_name = 'NAT_BRIDGE_SPAN_L3L4_DEV'
        repo.subs_el_table_name = 'NAT_BRIDGE_ABT_L3L4_DEV'

        # Get by Bridge ID
        inv = repo.get_by_bridge_id(bridge_id=bridge_id, inv_year=2023)

        self.assertTrue(type(inv.artable) == pa.Table)
        self.assertTrue(type(inv.sups.artable) == pa.Table)
        self.assertTrue(type(inv.subs.artable) == pa.Table)
        self.assertTrue(type(inv.sups.elements.artable) == pa.Table)
        self.assertTrue(type(inv.subs.elements.artable) == pa.Table)

    def test_get_by_bridge_id_no_data(self):

        engine = create_engine(f"oracle+oracledb://{USER}:{PWD}@{HOST}:1521/geodbbm")
        
        # Bridge ID for query
        bridge_id = 'ABCD'

        repo = BridgeInventoryRepo(sql_engine=engine)
        repo.inv_table_name = 'NAT_BRIDGE_PROFILE_DEV'
        repo.sups_table_name = 'NAT_BRIDGE_SPAN_DEV'
        repo.subs_table_name = 'NAT_BRIDGE_ABT_DEV' 
        repo.sups_el_table_name = 'NAT_BRIDGE_SPAN_L3L4_DEV'
        repo.subs_el_table_name = 'NAT_BRIDGE_ABT_L3L4_DEV'

        # Get by Bridge ID
        inv = repo.get_by_bridge_id(bridge_id=bridge_id, inv_year=2023)

        self.assertIsNone(inv)

    def test_put(self):
        with open('tests/domain/bridge/inventory/test_inventory_invij.json') as jf:
            input_dict = json.load(jf)

        inv = BridgeInventory.from_invij(input_dict)

        engine = create_engine(f"oracle+oracledb://{USER}:{PWD}@{HOST}:1521/geodbbm")

        repo = BridgeInventoryRepo(sql_engine=engine)
        repo.inv_table_name = 'NAT_BRIDGE_PROFILE_DEV'
        repo.sups_table_name = 'NAT_BRIDGE_SPAN_DEV'
        repo.subs_table_name = 'NAT_BRIDGE_ABT_DEV' 
        repo.sups_el_table_name = 'NAT_BRIDGE_SPAN_L3L4_DEV'
        repo.subs_el_table_name = 'NAT_BRIDGE_ABT_L3L4_DEV'

        repo.put(inv)

        self.assertTrue(True)

    def test_get_available_years(self):
        engine = create_engine(f"oracle+oracledb://{USER}:{PWD}@{HOST}:1521/geodbbm")

        repo = BridgeInventoryRepo(sql_engine=engine)
        repo.inv_table_name = 'NAT_BRIDGE_PROFILE_DEV'
        result = repo.get_available_years("3500725")
        
        self.assertTrue(type(result) == list)

        self.assertTrue(max(result) == 2023)
