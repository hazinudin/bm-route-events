import unittest
from dotenv import load_dotenv
import os
from sqlalchemy import create_engine
from route_events import RouteDefects, LRSRoute
from src.service.points.validation.defects import RouteDefectsValidation
from src.service.validation_result.result import ValidationResult


load_dotenv('tests/dev.env')
HOST = os.getenv('DB_HOST')
USER = os.getenv('SMD_USER')
PWD = os.getenv('SMD_PWD')

engine = create_engine(f"oracle+oracledb://{USER}:{PWD}@{HOST}:1521/geodbbm")


class TestRouteDefectsValidation(unittest.TestCase):
    def test_init(self):
        excel_path = "tests/domain/route_points/defect_010362.xlsx"

        events = RouteDefects.from_excel(
            excel_path,
            '010362',
            data_year=2024
        )

        lrs = LRSRoute.from_feature_service('localhost:50052', '010362')
        results = ValidationResult('010362')

        check = RouteDefectsValidation(
            route='010362',
            events=events,
            lrs=lrs,
            sql_engine=engine,
            results = results
        )

        self.assertFalse(check.df_lrs_mv.is_empty())
        self.assertTrue(True)

    def test_validation(self):
        excel_path = "tests/domain/route_points/defect_010362.xlsx"

        events = RouteDefects.from_excel(
            excel_path,
            '010362',
            data_year=2024
        )

        lrs = LRSRoute.from_feature_service('localhost:50052', '010362')
        results = ValidationResult('010362')

        check = RouteDefectsValidation(
            route='010362',
            events=events,
            lrs=lrs,
            sql_engine=engine,
            results = results,
            survey_year=2024
        )

        check.lrs_sta_check()
        check.lrs_distance_check()
        check.route_has_rni_check()
        check.sta_not_in_rni_check()

        self.assertTrue(check.get_status() == 'error')

    def test_survey_photos(self):
        """
        Test survey photo object conversion.
        """
        excel_path = "tests/domain/route_points/defect_010362.xlsx"

        events = RouteDefects.from_excel(
            excel_path,
            '010362',
            data_year=2024
        )

        lrs = LRSRoute.from_feature_service('localhost:50052', '010362')
        results = ValidationResult('010362')

        check = RouteDefectsValidation(
            route='010362',
            events=events,
            lrs=lrs,
            sql_engine=engine,
            results = results,
            survey_year=2024
        )

        check.survey_photos

        self.assertTrue(True)