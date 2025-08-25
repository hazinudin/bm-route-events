from route_events.bridge import BridgeMaster
import json
import unittest


class BridgeMasterProperty(unittest.TestCase):
    def test_has_correct_num_format(self):
        with open('tests/bridge/master/test_master_data.json') as jf:
            input_dict = json.load(jf)

        bm = BridgeMaster.from_invij(input_dict)
        self.assertTrue(bm.has_correct_num_format)

    def test_has_correct_prov_in_num(self):
        with open('tests/bridge/master/test_master_data.json') as jf:
            input_dict = json.load(jf)

        bm = BridgeMaster.from_invij(input_dict)
        self.assertTrue(bm.has_correct_prov_in_num)

    def test_master_survey_year(self):
        with open('tests/bridge/master/test_master_data.json') as jf:
            input_dict = json.load(jf)

        bm = BridgeMaster.from_invij(input_dict)
        year = bm.master_survey_year

        self.assertTrue(year == 2024)

    def test_bridge_length(self):
        with open('tests/domain/bridge/master/test_master_data.json') as jf:
            input_dict = json.load(jf)

        bm = BridgeMaster.from_invij(input_dict)
        length = bm.length

        self.assertTrue(type(length) is float)

    def test_master_as_pb(self):
        # with open('tests/bridge/test_master_data.json') as jf:
        #     input_dict = json.load(jf)

        input_dict = {'ID_JBT': '5200296', 'NO_JBT': '52.012.002.0', 'NAMA_JBT': 'TAIPA', 'LATITUDE': 0.47160299987859844, 'LONGITUDE': 119.993136, 'LINKID': '52012', 'ID_PROV': '52', 'TAHUN_BANGUN': 1999, 'PJG_TOTAL': 31.2, 'TGL_UPDATE': '25/03/2024', 'STATUS_JBT': 'N', 'MODE': 'UPDATE', 'TIPE_JBT': 'S', 'JENIS_JBT': ''}

        bm = BridgeMaster.from_invij(input_dict) 
        pb = bm.as_pb()

        self.assertTrue(True)
