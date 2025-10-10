import unittest
from dotenv import load_dotenv
import os
from sqlalchemy import create_engine
from route_events import RouteDefects, LRSRoute
from src.service.points.validation.defects import RouteDefectsValidation
from src.service.validation_result.result import ValidationResult
from src.service.photo import gs
from google.cloud import storage


load_dotenv('tests/dev.env')
HOST = os.getenv('DB_HOST')
USER = os.getenv('SMD_USER')
PWD = os.getenv('SMD_PWD')

engine = create_engine(f"oracle+oracledb://{USER}:{PWD}@{HOST}:1521/geodbbm")


class TestRouteDefectsValidation(unittest.TestCase):
    def test_init(self):
        excel_path = "tests/domain/route_points/defect_010362.xlsx"
        route_id = '010362'

        events = RouteDefects.from_excel(
            excel_path,
            route_id,
            data_year=2025
        )

        lrs = LRSRoute.from_feature_service('localhost:50052', route_id)
        results = ValidationResult(route_id)

        client = storage.Client()
        sp = gs.SurveyPhotoStorage(bucket_name='sidako-bucket', sql_engine=engine, gs_client=client)

        check = RouteDefectsValidation(
            route=route_id,
            events=events,
            lrs=lrs,
            sql_engine=engine,
            results = results,
            photo_storage=sp,
            survey_year=2025,
        )

        check.base_validation()

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

        client = storage.Client()
        sp = gs.SurveyPhotoStorage(bucket_name='sidako-bucket', sql_engine=engine, gs_client=client)

        check = RouteDefectsValidation(
            route='010362',
            events=events,
            lrs=lrs,
            sql_engine=engine,
            results = results,
            survey_year=2024,
            photo_storage=sp
        )

        check.lrs_sta_check()
        check.lrs_distance_check()
        check.route_has_rni_check()
        check.sta_not_in_rni_check()
        check.survey_photo_url_check()
        check.surface_type_check()
        check.damage_surface_type_check()

        self.assertTrue(check.get_status() == 'error')

    def test_survey_photos(self):
        """
        Test survey photo object conversion.
        """
        excel_path = "C:/Users/hazin/Downloads/defect_21_08-09-2025_021224_9367.xlsx"
        linkid = '56027'

        events = RouteDefects.from_excel(
            excel_path,
            linkid,
            data_year=2025
        )

        lrs = LRSRoute.from_feature_service('localhost:50052', linkid)
        results = ValidationResult(linkid)

        client = storage.Client()
        sp = gs.SurveyPhotoStorage(bucket_name='sidako-bucket', sql_engine=engine, gs_client=client)

        check = RouteDefectsValidation(
            route=linkid,
            events=events,
            lrs=lrs,
            sql_engine=engine,
            results = results,
            survey_year=2025,
            photo_storage=sp
        )

        check.sta_not_in_rni_check()
        check.survey_photos
        check.survey_photo_url_check()

        self.assertTrue(True)

    def test_put_data(self):
        """
        Test put data in geodatabase.
        """
        excel_path = "tests/domain/route_points/defect_010362.xlsx"

        events = RouteDefects.from_excel(
            excel_path,
            '010362',
            data_year=2024
        )

        check = RouteDefectsValidation(
            route='010362',
            events=events,
            lrs=None,
            sql_engine=engine,
            results=None,
            survey_year=2024,
            photo_storage=None
        )

        check.put()

        self.assertTrue(True)