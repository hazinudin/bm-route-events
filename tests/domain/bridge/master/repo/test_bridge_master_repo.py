from route_events.bridge import BridgeMasterRepo, BridgeMaster
from route_events.bridge.master.repo.db_repo import BridgeMasterRepoDB
from sqlalchemy import create_engine
import unittest
import pyarrow as pa
import json
import copy
from dotenv import load_dotenv
import os


class TestBridgeMasterRepo(unittest.TestCase):
    def test_repo_by_bridge_id(self):
        """
        Load BridgeMaster data from a Bridge ID query.
        """

        repo = BridgeMasterRepo('localhost:50051')
        bm = repo.get_by_bridge_id('6300311')

        self.assertTrue(type(bm.artable) == pa.Table)
        self.assertTrue(bm.artable.num_rows == 1)

    def test_repo_by_bridge_number(self):
        """
        Load the BridgeMaster data from a Bridge number query.
        """

        repo = BridgeMasterRepo('localhost:50051')
        bm = repo.get_by_bridge_number('61.024.017.0', return_count_only=False)

        self.assertTrue(type(bm[0].artable) == pa.Table)
        self.assertTrue(bm[0].artable.num_rows == 1)

    def test_get_nearest_bridge(self):
        """
        Load the nearest BridgeMaster from an inputted BridgeMaster
        """
        repo = BridgeMasterRepo('localhost:50051')
        ref = repo.get_by_bridge_id('2200650')

        # The nearest bridge would have bridge_id = '2200651'
        result = repo.get_nearest(ref, radius=50, return_count_only=False)

        self.assertTrue(type(result[0].artable) == pa.Table)
        self.assertTrue(result[0].id == '2200651')
        self.assertTrue(len(result) == 1)

    def test_get_nearest_bridge_count_only(self):
        """
        Get the count of nearest BridgeMaster from an inputted BridgeMaster
        """
        repo = BridgeMasterRepo('localhost:50051')
        ref = repo.get_by_bridge_id('2200650')

        # The nearest bridge would have bridge_id = '2200651'
        result = repo.get_nearest(ref, radius=50, return_count_only=True)

        self.assertTrue(result == 1)

    def test_insert(self):
        """
        Insert BridgeMaster data.
        """
        repo = BridgeMasterRepo('localhost:50051')
        
        with open('tests/bridge/test_master_data_crud.json') as jf:
            input_dict = json.load(jf)

        bm =  BridgeMaster.from_invij(input_dict)
        insert_oid = repo.insert(bridge=bm).add_results[0].objectid

        bm_repo = repo.get_by_bridge_id(bm.id)

        repo.delete([insert_oid])
        self.assertTrue(bm_repo.id == bm.id)

    def test_update(self):
        """
        Update BridgeMaster data.
        """
        repo = BridgeMasterRepo('localhost:50051')
        
        with open('tests/bridge/test_master_data_crud.json') as jf:
            data_dict = json.load(jf)
            update_dict = copy.deepcopy(data_dict)

            update_dict['no_jbt'] = 'test_update'

        insert_bm = BridgeMaster.from_invij(data_dict)
        update_bm =  BridgeMaster.from_invij(update_dict)

        insert_oid = repo.insert(bridge=insert_bm).add_results[0].objectid
        repo.update(bridge=update_bm)

        updated_bm = repo.get_by_bridge_id(insert_bm.id)

        repo.delete([insert_oid])
        self.assertTrue(updated_bm.id == update_bm.id)


load_dotenv('tests/dev.env')
HOST = os.getenv('DB_HOST')
USER = os.getenv('MISC_USER')
PWD = os.getenv('MISC_PWD')

engine = create_engine(f"oracle+oracledb://{USER}:{PWD}@{HOST}:1521/geodbbm")

class TestBridgeMasterRepoDB(unittest.TestCase):
    def test_repo_get_by_bridge_id(self):
        """
        Get BridgeMaster from Database.
        """
        repo = BridgeMasterRepoDB(sql_engine=engine)

        bm = repo.get_by_bridge_id('2200553')

        self.assertTrue(type(bm.artable) == pa.Table)

    def test_repo_get_by_bridge_id_count_only(self):
        """
        Get count of requested bridge from Database.
        """
        repo = BridgeMasterRepoDB(sql_engine=engine)

        count = repo.get_by_bridge_id('0100004', return_count_only=True)
        
        self.assertTrue(count == 1)

    def test_repo_get_nearest_count_only(self):
        """
        Get count of requested bridge from Database.
        """
        repo = BridgeMasterRepoDB(sql_engine=engine)

        bm = repo.get_by_bridge_id('0101100')
        count = repo.get_nearest(bm, radius=10000, return_count_only=True)
        
        self.assertTrue(count == 2)

    def test_insert(self):
        """
        Insert BridgeMaster data.
        """
        repo = BridgeMasterRepoDB(sql_engine=engine)

        with open('tests/domain/bridge/master/test_master_data_crud.json') as jf:
            data_dict = json.load(jf)
        
        bm = BridgeMaster.from_invij(data_dict)

        repo.insert(bm)

        repo.get_by_bridge_id(bm.id)

        repo.delete(bm)

    def test_update(self):
        """
        Test update BridgeMaster data.
        """
        repo = BridgeMasterRepoDB(sql_engine=engine)

        with open('tests/domain/bridge/master/test_master_data_crud.json') as jf:
            data_dict = json.load(jf)
        
        bm = BridgeMaster.from_invij(data_dict)

        repo.insert(bm)

        # Change the data
        data_dict['nama_jbt'] = 'Jembatan Updated'

        # Update and delete
        bm_update = BridgeMaster.from_invij(data_dict)
        repo.update(bm_update)
        bm_test = repo.get_by_bridge_id(bm_update.id)
        repo.delete(bm)

        self.assertTrue(bm_update.name == bm_test.name)

    def test_update_existing(self):
        """
        Test update existing data on GeoDatabase.
        """
        repo = BridgeMasterRepoDB(sql_engine=engine)

        data_dict = {'ID_JBT': '0900080', 'NO_JBT': '09.017.011.0', 'NAMA_JBT': 'S.BAJENUNG', 'LATITUDE': -0.846254, 'LONGITUDE': 102.622554, 'LINKID': '09017', 'ID_PROV': '09', 'TAHUN_BANGUN': 1991, 'PJG_TOTAL': 8.3, 'TGL_UPDATE': '04/06/2024', 'STATUS_JBT': 'N', 'MODE': 'UPDATE', 'TIPE_JBT': 'S', 'JENIS_JBT': ''}

        # Update the data
        bm = BridgeMaster.from_invij(data_dict)
        repo.update(bm)

        self.assertTrue(True)

    def test_retire(self):
        """
        Test retire BridgeMaster data.
        """
        repo = BridgeMasterRepoDB(sql_engine=engine)

        with open('tests/bridge/master/test_master_data_crud.json') as jf:
            data_dict = json.load(jf)
        
        bm = BridgeMaster.from_invij(data_dict)

        repo.insert(bm)

        # Retire
        repo.retire(bm)

        self.assertTrue(repo.get_by_bridge_id(bm.id) is None)

    def test_get_oid(self):
        """
        Test get objectid from bridge master table.
        """
        repo = BridgeMasterRepoDB(sql_engine=engine)

        self.assertTrue(type(repo.get_oid()) == int) 
