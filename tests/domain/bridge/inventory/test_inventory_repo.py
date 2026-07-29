from src.route_events.bridge.inventory import (
    BridgeInventory,
    BridgeInventoryRepo,
    Superstructure,
)
import unittest
import pyarrow as pa
import polars as pl
import json
import datetime
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os


load_dotenv('tests/dev.env')
HOST = os.getenv('GDB_HOST')
USER = os.getenv('MISC_USER')
PWD = os.getenv('MISC_PWD')

class TestInventoryRepo(unittest.TestCase):
    def test_get_by_bridge_id(self):

        engine = create_engine(f"oracle+oracledb://{USER}:{PWD}@{HOST}:1521/geodbbm")
        
        # Bridge ID for query
        bridge_id = '3400080'

        repo = BridgeInventoryRepo(sql_engine=engine)
        repo.inv_table_name = 'NAT_BRIDGE_PROFILE'
        repo.sups_table_name = 'NAT_BRIDGE_SPAN'
        repo.subs_table_name = 'NAT_BRIDGE_ABT' 
        repo.sups_el_table_name = 'NAT_BRIDGE_SPAN_L3L4'
        repo.subs_el_table_name = 'NAT_BRIDGE_ABT_L3L4'

        # Get by Bridge ID
        inv = repo.get_by_bridge_id(bridge_id=bridge_id, inv_year=2025)

        self.assertTrue(type(inv.artable) == pa.Table)
        self.assertTrue(type(inv.sups.artable) == pa.Table)

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

    def test_put_sups(self):
        bridge_id = "99999"

        # Profile (one row per bridge) used to populate NAT_BRIDGE_PROFILE_POPUP
        profile_tbl = pa.table(
            {
                "BRIDGE_ID": pa.array([bridge_id], pa.string()),
                "INV_YEAR": pa.array([2025], pa.int64()),
                "BRIDGE_LENGTH": pa.array([120.5], pa.float64()),
                "MAIN_SPAN_TYPE": pa.array(["UTAMA"], pa.string()),
                "INV_DATE": pa.array(
                    [datetime.datetime(2025, 6, 1)], pa.timestamp("us")
                ),
            }
        )
        inv = BridgeInventory(profile_tbl, state="POPUP")

        # Superstructure spans used to populate NAT_BRIDGE_SPAN_POPUP
        sups_tbl = pa.table(
            {
                "BRIDGE_ID": pa.array([bridge_id, bridge_id], pa.string()),
                "INV_YEAR": pa.array([2025, 2025], pa.int64()),
                "SPAN_NUMBER": pa.array([1, 2], pa.int64()),
                "SPAN_TYPE": pa.array(["UTAMA", "UTAMA"], pa.string()),
                "SPAN_SEQ": pa.array([1, 1], pa.int64()),
                "SPAN_LENGTH": pa.array([60.0, 60.5], pa.float64()),
                "SUPERSTRUCTURE": pa.array(["RC SLAB", "PRESTRESS"], pa.string()),
            }
        )
        sups = Superstructure(sups_tbl, validate=False)
        inv.add_superstructure(sups, replace=True)

        engine = create_engine(
            f"oracle+oracledb://{USER}:{PWD}@{HOST}:1521/geodbbm"
        )

        repo = BridgeInventoryRepo(sql_engine=engine)
        # Use DEV-suffixed popup tables to avoid touching production data
        repo.sups_popup_profile_table_name = "NAT_BRIDGE_PROFILE_POPUP"
        repo.sups_popup_span_table_name = "NAT_BRIDGE_SPAN_POPUP"

        repo.put_sups(inv, val_note=["sample validation message"], source="VV")

        # Verify the rows were written
        df_profile = pl.read_database(
            f"select * from {repo.sups_popup_profile_table_name} where BRIDGE_ID = '{bridge_id}'",
            connection=engine,
        )
        df_span = pl.read_database(
            f"select * from {repo.sups_popup_span_table_name} where BRIDGE_ID = '{bridge_id}' and INV_YEAR = 2025",
            connection=engine,
        )

        self.assertEqual(df_profile.height, 1)
        self.assertEqual(df_span.height, 2)
        self.assertEqual(df_profile["BRIDGE_ID"][0], bridge_id)
        self.assertEqual(df_profile["SOURCE"][0], "VV")
        self.assertEqual(sorted(df_span["SPAN_NUMBER"].to_list()), [1, 2])
