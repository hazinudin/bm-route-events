import unittest
from src.service.bridge.master_validation import BridgeMasterValidation
from sqlalchemy import create_engine
import cProfile 
import json
import pstats
import os
from dotenv import load_dotenv


load_dotenv('tests/dev.env')
HOST = os.getenv('DB_HOST')
USER = os.getenv('MISC_USER')
PWD = os.getenv('MISC_PWD')

engine = create_engine(f"oracle+oracledb://{USER}:{PWD}@{HOST}:1521/geodbbm")

class TestBridgeMasterValidation(unittest.TestCase):
    def test_insert_check(self):
        with open('tests/service/bridge_master_validation/test_master_data.json') as jf:
            input_dict = json.load(jf)

        check = BridgeMasterValidation(
            data=input_dict, 
            validation_mode='INSERT',
            lrs_grpc_host='localhost:50052',
            sql_engine=engine
        )
        
        check.insert_check()

        self.assertTrue(True)

    def test_init(self):
        with open('tests/service/bridge_master_validation/test_master_data.json') as jf:
            input_dict = json.load(jf)

        check = BridgeMasterValidation(
            data=input_dict, 
            validation_mode='UPDATE',
            lrs_grpc_host='localhost:50052',
            sql_engine=engine
        )
        
        self.assertTrue(True)

    def test_init_from_dict(self):
        input_dict = {'ID_JBT': '5201515', 'NO_JBT': '52.056.006.0', 'NAMA_JBT': 'MARONGI', 'LATITUDE': -1.7777920001, 'LONGITUDE': 120.736225, 'LINKID': '52056', 'ID_PROV': '52', 'TAHUN_BANGUN': 1989, 'PJG_TOTAL': 4000.5, 'TGL_UPDATE': '28/3/2024', 'STATUS_JBT': 'N', 'MODE': 'UPDATE', 'TIPE_JBT': 'S', 'JENIS_JBT': None}
        
        check = BridgeMasterValidation(
                data=input_dict, 
                validation_mode='UPDATE',
                lrs_grpc_host='localhost:50052',
                sql_engine=engine
        )

        self.assertTrue(check.get_status() == 'review')

    def test_init_from_empty_dict(self):
        check = BridgeMasterValidation(
                data={}, 
                validation_mode='UPDATE',
                lrs_grpc_host='localhost:50052',
                sql_engine=engine
        )
        
        self.assertTrue(check.get_status() == 'rejected')

    def test_update_check(self):
        with open('tests/service/bridge_master_validation/test_master_data.json') as jf:
            input_dict = json.load(jf)

        with cProfile.Profile() as profile:
            check = BridgeMasterValidation(
                data=input_dict, 
                validation_mode='UPDATE',
                lrs_grpc_host='localhost:50052',
                sql_engine=engine
            )
            
            check.update_check()

            res = pstats.Stats(profile)
            res.sort_stats(pstats.SortKey.TIME)
