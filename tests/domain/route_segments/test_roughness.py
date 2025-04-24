import unittest
from src.route_events.segments.roughness import RouteRoughness, RouteRoughnessRepo
from pyarrow import Table


class TestRoughness(unittest.TestCase):
    def test_from_excel(self):
        """
        Load data from Excel file with correct schema.
        """
        excel_path = "tests/domain/route_segments/input_excels/iri_1_010362.xlsx"

        iri = RouteRoughness.from_excel(
            excel_path=excel_path,
            linkid='010362',
            ignore_review=True
        )

        self.assertTrue(type(iri.artable) == Table)

    def test_all_base_method(self):
        """
        Test all inherited base method.
        """
        excel_path = "tests/domain/route_segments/input_excels/iri_1_010362.xlsx"

        iri = RouteRoughness.from_excel(
            excel_path=excel_path,
            linkid='010362',
            ignore_review=True
        )

        iri.incorrect_sta_diff()
        iri.incorrect_lane_sequence()
        iri.correct_data_year()
        iri.incorrect_segment_length()
        iri.is_duplicate_segment()
        iri.overlapping_segments()
        iri.sta_gap()

        self.assertTrue(True)

