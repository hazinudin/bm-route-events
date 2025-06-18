from route_events.bridge import BridgeMaster
import json
import unittest
import pyarrow as pa
import cProfile
import pstats
from pydantic import ValidationError
from copy import deepcopy


with open('tests/domain/bridge/master/test_master_data.json') as jf:
    input_dict = json.load(jf)

with open('tests/domain/bridge/master/test_master_data_missing_column.json') as jf:
    invalid_dict = json.load(jf)

class TestBridgeMasterFactory(unittest.TestCase):
    def test_bridge_master_from_json(self):
        """
        Generate from JSON file.
        """
        with cProfile.Profile() as profile:
            bm =  BridgeMaster.from_invij(input_dict)
        
        results = pstats.Stats(profile)
        results.sort_stats(pstats.SortKey.TIME)

        self.assertTrue(type(bm.artable) == pa.Table, msg=results.print_stats(10))

    def test_bridge_master_from_invalid_data_type(self):
        """
        Test input data with invalid data type.
        """
        test_dict = deepcopy(input_dict)
        test_dict['pjg_total'] = 'ABCD'  # Should be number

        with self.assertRaises(ValidationError):
            BridgeMaster.from_invij(test_dict)

    def test_bridge_master_from_json_invalid_data(self):
        """
        Generate from JSON file.
        """
        with self.assertRaises(ValidationError):
            BridgeMaster.from_invij(invalid_dict)

    def test_bridge_master_from_empty_json(self):
        """
        Test from emtpy json.
        """
        with self.assertRaises(ValidationError):
            BridgeMaster.from_invij({})

