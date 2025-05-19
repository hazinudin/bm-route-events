import unittest
from src.service.bridge.inventory_validation import BridgeInventoryValidation
from sqlalchemy import create_engine
import json
import cProfile
import pstats
from dotenv import load_dotenv
import os


load_dotenv('tests/dev.env')
HOST = os.getenv('DB_HOST')
USER = os.getenv('MISC_USER')
PWD = os.getenv('MISC_PWD')

engine = create_engine(f"oracle+oracledb://{USER}:{PWD}@{HOST}:1521/geodbbm")

with open('tests/service/bridge_inventory_validation/test_inventory_data_1.json') as jf:
    input_dict = json.load(jf)

class TestBridgeInventoryValidation(unittest.TestCase):
    def test_init(self):
        with cProfile.Profile() as profile:
            check = BridgeInventoryValidation(
                data=input_dict,
                validation_mode='INSERT',
                lrs_grpc_host='localhost:50052',
                sql_engine=engine,
                dev = True
            )

            res = pstats.Stats(profile)
            res.sort_stats(pstats.SortKey.TIME)

        self.assertTrue(True)