import polars as pl
import unittest
from src.route_events.points.rtc import RouteRTC
from dotenv import load_dotenv
import os
from sqlalchemy import create_engine


load_dotenv('tests/dev.env')
HOST = os.getenv('GDB_HOST')
USER = os.getenv('SMD_USER')
PWD = os.getenv('SMD_PWD')

engine = create_engine(f"oracle+oracledb://{USER}:{PWD}@{HOST}:1521/geodbbm")

class TestRouteRTC(unittest.TestCase):
    def test_from_excel(self):
        """
        Test from_excel factory method.
        """
        excel_path = "~/Downloads/rtc_6_16-10-2025_091412_6344.xlsx"
        route_id = "22040"

        events = RouteRTC.from_excel(
            excel_path=excel_path,
            linkid=route_id,
        )

        self.assertTrue(True)

    def test_df_with_timestamp(self):
        """
        Test df_with_timestamp property.
        """
        excel_path = "~/Downloads/rtc_6_16-10-2025_091412_6344.xlsx"
        route_id = "22040"

        events = RouteRTC.from_excel(
            excel_path=excel_path,
            linkid=route_id,
        )

        self.assertTrue(
            events.df_with_timestamp.filter(pl.col(events._timestamp_col).is_null()).is_empty()
        )

    def test_invalid_interval(self):
        excel_path = "~/Downloads/rtc_6_16-10-2025_091412_6344.xlsx"
        route_id = "22040"

        events = RouteRTC.from_excel(
            excel_path=excel_path,
            linkid=route_id,
        )

        result = events.invalid_interval()

        self.assertTrue(type(result) is pl.DataFrame)
        self.assertTrue(result.is_empty())

    def test_get_all_survey_directions(self):
        excel_path = "~/Downloads/rtc_6_16-10-2025_091412_6344.xlsx"
        route_id = "22040"

        events = RouteRTC.from_excel(
            excel_path=excel_path,
            linkid=route_id,
        )

        dirs = events.get_all_survey_directions()
        
        self.assertTrue(len(dirs) == 2)
        self.assertTrue('O' in dirs)

    def test_survey_duration(self):
        excel_path = "~/Downloads/rtc_6_16-10-2025_091412_6344.xlsx"
        route_id = "22040"

        events = RouteRTC.from_excel(
            excel_path=excel_path,
            linkid=route_id,
        )

        duration = events.survey_duration()

        self.assertTrue(duration == 7)

    def test_has_sta(self):
        excel_path = "~/Downloads/rtc_6_16-10-2025_091412_6344.xlsx"
        route_id = "22040"

        events = RouteRTC.from_excel(
            excel_path=excel_path,
            linkid=route_id,
        )
        
        self.assertFalse(events.has_sta())

from src.route_events.points.rtc.repo import RouteRTCRepo

class TestRouteRTCRepo(unittest.TestCase):
    def test_get_by_linkid(self):
        route_id = '01001'
        year = 2024

        repo = RouteRTCRepo(sql_engine=engine)
        events = repo.get_by_linkid(linkid=route_id, year=year)

        self.assertTrue(True)
        self.assertFalse(events.pl_df.is_empty())
