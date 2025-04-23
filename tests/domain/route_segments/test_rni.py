import unittest
from src.route_events.segments.rni import RouteRNI
from pyarrow import Table


class TestRNI(unittest.TestCase):
    def test_from_excel(self):
        """
        Load data from Excel file with correct schema.
        """
        excel_path = "tests/domain/route_segments/input_excels/balai_5_15010.xlsx"

        rni = RouteRNI.from_excel(
            excel_path=excel_path,
            linkid = '15010',
            ignore_review=True
        )

        self.assertTrue(type(rni.artable) == Table)

    def test_all_base_method(self):
        """
        Load data from Excel file and test all available method from RouteSegmentEvents.
        """
        excel_path = "tests/domain/route_segments/input_excels/balai_5_15010.xlsx"

        rni = RouteRNI.from_excel(
            excel_path=excel_path,
            linkid = '15010',
            ignore_review=True,
            data_year=2023
        )

        rni.incorrect_sta_diff()
        rni.incorrect_lane_sequence()
        rni.correct_data_year()
        rni.incorrect_segment_length()
        rni.is_duplicate_segment()
        rni.overlapping_segments()
        rni.sta_gap()
        rni.incorrect_side_columns()

        self.assertTrue(True)

    def test_incorrect_side_columns(self):
        """
        Test incorrect side columns fill pattern check function.
        """
        excel_path = "tests/domain/route_segments/input_excels/balai_5_15010.xlsx"

        rni = RouteRNI.from_excel(
            excel_path=excel_path,
            linkid = '15010',
            ignore_review=True,
            data_year=2023
        )
        
        rni.incorrect_side_columns()

        self.assertTrue(True)

    def test_incorrect_road_type_spec(self):
        """
        Test segment with incorrect road type specification.
        """
        excel_path = "tests/domain/route_segments/input_excels/balai_5_15010.xlsx"

        rni = RouteRNI.from_excel(
            excel_path=excel_path,
            linkid = '15010',
            ignore_review=True,
            data_year=2023
        )

        rni.incorrect_road_type_spec()

        self.assertTrue(True)

    def test_incorrect_inner_shoulder(self):
        """
        Test segment with incorrect inner shoulder placement.
        """
        excel_path = "tests/domain/route_segments/input_excels/balai_5_15010.xlsx"

        rni = RouteRNI.from_excel(
            excel_path=excel_path,
            linkid = '15010',
            ignore_review=True,
            data_year=2023
        )

        rni.incorrect_inner_shoulder()

        self.assertTrue(True)

    def test_surface_types_mapping(self):
        """
        Surface types DataFrame mapping.
        """
        excel_path = "tests/domain/route_segments/input_excels/balai_5_15010.xlsx"

        rni = RouteRNI.from_excel(
            excel_path=excel_path,
            linkid='15010',
            ignore_review=True,
        )

        self.assertEqual(rni.surface_types_mapping.shape[0], 2)
