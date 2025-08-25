import unittest
from src.route_events.segments.rni import RouteRNI, RouteRNIRepo
from pyarrow import Table
import polars as pl
from dotenv import load_dotenv
import os
from sqlalchemy import create_engine, text


load_dotenv('tests/dev.env')
HOST = os.getenv('DB_HOST')
USER = os.getenv('SMD_USER')
PWD = os.getenv('SMD_PWD')

engine = create_engine(f"oracle+oracledb://{USER}:{PWD}@{HOST}:1521/geodbbm")


class TestRNIRepo(unittest.TestCase):
    def test_put_data(self):
        """
        Load data from Excel file and write it to database
        """
        excel_path = "tests/domain/route_segments/input_excels/balai_5_15010.xlsx"

        rni = RouteRNI.from_excel(
            excel_path=excel_path,
            linkid = '15010',
            ignore_review=True
        )

        repo = RouteRNIRepo(
            sql_engine=engine
        )

        rni._pl_df = rni.pl_df.with_columns(pl.lit('TEST_LINKID').alias('LINKID'))
        
        # Put the test data into RNI_1_2024
        repo.put(
            events=rni,
            year=2024,
            semester=1
        )

        # Test the inserted data
        df = pl.read_database(
            "select linkid, objectid, copied, update_date from rni_1_2024 where linkid = 'TEST_LINKID'",
            connection=engine
        )

        self.assertFalse(df.is_empty())
        self.assertTrue(df['COPIED'].eq(0).all())
        self.assertTrue(df['UPDATE_DATE'].is_not_null().all())

        # Delete the test data
        with engine.connect() as conn:
            conn.execute(text("DELETE FROM RNI_1_2024 WHERE LINKID = 'TEST_LINKID'"))
            conn.commit()

class TestRNI(unittest.TestCase):
    def test_from_excel(self):
        """
        Load data from Excel file with correct schema.
        """
        excel_path = "C:/Users/hazin/Downloads/rni_25_08-08-2025_105655_5855.xlsx"

        rni = RouteRNI.from_excel(
            excel_path=excel_path,
            linkid = '13006',
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
        excel_path = "tests/route_segments/input_excels/rni_7_04-08-2025_055329_2970.xlsx"

        rni = RouteRNI.from_excel(
            excel_path=excel_path,
            linkid = '2400512',
            ignore_review=True,
            data_year=2025
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
        excel_path = "tests/route_segments/input_excels/rni_7_01-08-2025_095037_4760.xlsx"

        rni = RouteRNI.from_excel(
            excel_path=excel_path,
            linkid = '2402416',
            ignore_review=True,
            data_year=2025
        )

        result = rni.incorrect_inner_shoulder()

        self.assertTrue(len(result) == 0)

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

    def test_surface_width_check(self):
        """
        Surface width check test.
        """
        excel_path = "tests/route_segments/input_excels/rni_7_01-08-2025_080000_2492.xlsx"

        rni = RouteRNI.from_excel(
            excel_path=excel_path,
            linkid = '241092',
            ignore_review=True
        )

        result = rni.incorrect_surface_width()

        self.assertTrue(len(result) == 0)

    def test_incorrect_surface_year_check(self):
        """
        Surface year check test.
        """

        excel_path = "tests/route_segments/input_excels/rni_7_01-08-2025_080000_2492.xlsx"

        rni = RouteRNI.from_excel(
            excel_path=excel_path,
            linkid='241092',
            ignore_review=True,
            data_year=2020  # For testing purpose
        )

        result = rni.incorrect_surf_year()

        self.assertTrue(len(result) > 0)