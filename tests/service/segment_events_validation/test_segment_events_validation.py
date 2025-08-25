import unittest
from src.service import RouteSegmentEventsValidation
from src.route_events import RouteSegmentEventsRepo, RouteSegmentEvents
from src.route_events import LRSRoute
from src.service.validation_result.result import ValidationResult
from sqlalchemy import create_engine
import json
import cProfile
import pstats
from dotenv import load_dotenv
import os


load_dotenv('tests/dev.env')
HOST = os.getenv('DB_HOST')
USER = os.getenv('SMD_USER')
PWD = os.getenv('SMD_PWD')

engine = create_engine(f"oracle+oracledb://{USER}:{PWD}@{HOST}:1521/geodbbm")


class TestRouteSegmentEventsValidation(unittest.TestCase):
    def test_init(self):
        repo = RouteSegmentEventsRepo(engine, 'smd.rni_2_2024')
        events = repo.get_by_linkid('44082')
        results = ValidationResult('44082')
        lrs = LRSRoute.from_feature_service('localhost:50052', '44082')

        check = RouteSegmentEventsValidation(
            events=events,
            lrs=lrs,
            sql_engine=engine,
            results=results
        )

        check.df_lrs_mv
        check.df_lrs_dist

        self.assertTrue(True)

    def test_lrs_check(self):        
        events = RouteSegmentEvents.from_excel(
            'tests/domain/route_segments/input_excels/balai_5_15010.xlsx',
            config_path='tests/domain/route_segments/input_config/rni_config.json',
            linkid='15010',
            ignore_review=True
        )
        
        results = ValidationResult('15010')
        lrs = LRSRoute.from_geojson_file('tests/domain/lrs/lrs_15010.json')

        check = RouteSegmentEventsValidation(
            events=events,
            lrs=lrs,
            sql_engine=engine,
            results=results
        )

        check.lrs_monotonic_check()
        check.lrs_distance_check()
        check.lrs_direction_check()
        check.lrs_segment_length_check()
        check.lrs_sta_check()

        self.assertTrue(True)


from src.route_events import RouteRNI, RouteRNIRepo
from src.service import RouteRNIValidation

class TestRouteRNIEventsValidation(unittest.TestCase):
    def test_init(self):
        routeid = '44082'
        repo = RouteRNIRepo(engine)
        events = repo.get_by_linkid(routeid, 2024, raise_if_table_does_not_exists=True)
        results = ValidationResult(routeid)

        check = RouteRNIValidation(
            routeid,
            events,
            lrs=None,
            sql_engine=engine,
            results=results
        )

        self.assertTrue(True)

    def test_side_columns_check(self):
        routeid = '15010'
        events = RouteRNI.from_excel(
            'tests/domain/route_segments/input_excels/balai_5_15010.xlsx',
            linkid='15010',
            ignore_review=True
        )
        results = ValidationResult(routeid)

        check = RouteRNIValidation(
            routeid,
            events,
            lrs=None,
            sql_engine=engine,
            results=results
        )

        check.side_columns_check()

        self.assertTrue(True)

    def test_prev_year_comparison(self):
        routeid = '22025'
        events = RouteRNI.from_excel(
            'C:/Users/hazin/Downloads/rni_6_14-08-2025_170221_6756 (1).xlsx',
            linkid='22025',
            ignore_review=True
        )
        results = ValidationResult(routeid)

        check = RouteRNIValidation(
            routeid,
            events,
            lrs=None,
            sql_engine=engine,
            results=results,
            survey_year=2025
        )

        check.decreasing_lane_width_check()
        check.decreasing_lane_count()
        check.decreasing_surf_width_check()

        self.assertTrue(True)

    def test_excel_validation(self):
        routeid = '13006'
        lrs = LRSRoute.from_feature_service('localhost:50052', routeid)

        check = RouteRNIValidation.validate_excel(
            excel_path="C:/Users/hazin/Downloads/rni_25_08-08-2025_105655_5855.xlsx",
            route=routeid,
            survey_year=2025,
            sql_engine=engine,
            lrs=lrs,
            ignore_review=False
        )

        check.base_validation()
        self.assertFalse(check.get_status() == 'rejected')
        self.assertTrue(check.get_status() == 'error')
        self.assertFalse(check._events.pl_df.is_empty())

    def test_paved_to_unpaved_check(self):
        routeid = '15010'
        check = RouteRNIValidation.validate_excel(
            excel_path='tests/domain/route_segments/input_excels/balai_5_15010.xlsx',
            route=routeid,
            survey_year=2024,
            sql_engine=engine,
            lrs=None,
            ignore_review=False
        )

        check.paved_to_unpaved_check()

        self.assertTrue(True)


from src.route_events import RouteRoughness, RouteRoughnessRepo
from src.service import RouteRoughnessValidation

class TestRouteRoughnessEventsValidation(unittest.TestCase):
    def test_prev_data(self):
        routeid = '44082'
        repo = RouteRoughnessRepo(engine)
        events = repo.get_by_linkid(
            routeid,
            2024,
            2,
            True
        )

        results = ValidationResult(routeid)

        c = RouteRoughnessValidation(
            routeid,
            events,
            lrs=None,
            sql_engine=engine,
            results=results,
            survey_year=2024,
            survey_semester=2
        )

        self.assertTrue(
            c.prev_data.year == 2024 and 
            c.prev_data.semester == 1
        )
        self.assertFalse(c.prev_data.no_data)

    def test_rni_data(self):
        routeid = '44082'
        repo = RouteRoughnessRepo(engine)
        events = repo.get_by_linkid(
            routeid,
            2024,
            2,
            True
        )

        results = ValidationResult(routeid)

        c = RouteRoughnessValidation(
            routeid,
            events,
            lrs=None,
            sql_engine=engine,
            results=results,
            survey_year=2024,
            survey_semester=2
        )

        self.assertTrue(c.rni.year == 2024)
        self.assertFalse(c.rni.no_data)
        self.assertTrue(type(c.rni) is RouteRNI)

    def test_prev_rni(self):
        routeid = '44082'
        repo = RouteRoughnessRepo(engine)
        events = repo.get_by_linkid(
            routeid,
            2024,
            2,
            True
        )

        results = ValidationResult(routeid)

        c = RouteRoughnessValidation(
            routeid,
            events,
            lrs=None,
            sql_engine=engine,
            results=results,
            survey_year=2024,
            survey_semester=2
        )

        self.assertTrue(c.prev_rni.year == 2023)
        self.assertFalse(c.prev_rni.no_data)
        self.assertTrue(type(c.prev_rni) is RouteRNI)

        self.assertTrue(True)

    def test_kemantapan_comparison(self):
        """
        Test kemantapan_comparison function.
        """
        routeid = '44082'
        repo = RouteRoughnessRepo(engine)
        events = repo.get_by_linkid(
            routeid,
            2024,
            2,
            True
        )

        results = ValidationResult(routeid)

        c = RouteRoughnessValidation(
            routeid,
            events,
            lrs=None,
            sql_engine=engine,
            results=results,
            survey_year=2024,
            survey_semester=2
        )

        c.kemantapan_comparison_check()

        self.assertTrue(c.get_status() == 'review')

    def test_rni_segments_comparison(self):
        """
        Test RNI segments comparison.
        """
        routeid = '44082'
        repo = RouteRoughnessRepo(engine)
        events = repo.get_by_linkid(
            routeid,
            2024,
            2,
            True
        )

        results = ValidationResult(routeid)

        c = RouteRoughnessValidation(
            routeid,
            events,
            lrs=None,
            sql_engine=engine,
            results=results,
            survey_year=2024,
            survey_semester=2
        )

        c.rni_segments_comparison()

        self.assertTrue(True)

    def test_pok_iri_check(self):
        routeid = '01001'

        repo = RouteRoughnessRepo(engine)
        events = repo.get_by_linkid(
            routeid,
            2024,
            2,
            True
        )

        results = ValidationResult(routeid)

        c = RouteRoughnessValidation(
            routeid,
            events,
            lrs=None,
            sql_engine=engine,
            results=results,
            survey_year=2024,
            survey_semester=2
        )

        c.pok_iri_check()

        self.assertTrue(True)

    def test_validate_excel(self):
        routeid = '44039'

        lrs = LRSRoute.from_feature_service('localhost:50052', routeid)
        results = ValidationResult(routeid)

        c = RouteRoughnessValidation.validate_excel(
            "tests/domain/route_segments/input_excels/iri_10_21-08-2025_034607_1157 44039.xlsx",
            route=routeid,
            survey_year=2025,
            survey_semester=1,
            sql_engine=engine,
            lrs=lrs
        )

        c.base_validation()

        self.assertTrue(True)


from src.route_events import RoutePCI, RoutePCIRepo
from src.service import RoutePCIValidation


class TestRoutePCIEventsValidation(unittest.TestCase):
    def test_pci_base_validation(self):
        """
        Test the base validation.
        """
        # routeid = '52013'
        routeid = '56039'

        lrs = LRSRoute.from_feature_service("localhost:50052", routeid)
        file_name = "tests/domain/route_segments/input_excels/pci_14_31-07-2025_020817_9581.xlsx"

        check = RoutePCIValidation.validate_excel(
            file_name,
            route=routeid,
            survey_year=2025,
            sql_engine=engine,
            lrs=lrs
        )

        check.base_validation()

        self.assertTrue(True)

    def test_pci_rni_surf_type_comparison(self):
        """
        Test PCI and RNI comparison.
        """
        routeid = '52051'

        lrs = LRSRoute.from_feature_service("localhost:50052", routeid)

        check = RoutePCIValidation.validate_excel(
            "tests/domain/route_segments/input_excels/pci_14_17-08-2025_133137_8988.xlsx",
            route=routeid,
            survey_year=2025,
            sql_engine=engine,
            lrs=lrs
        )

        check.rni_surf_type_comparison()
        self.assertTrue(True)

    def test_pci_rni_surf_type_segment_length_check(self):
        """
        Test PCI and RNI comparison for segment length.
        """
        routeid = '52051'

        lrs = LRSRoute.from_feature_service("localhost:50052", routeid)

        check = RoutePCIValidation.validate_excel(
            "tests/domain/route_segments/input_excels/pci_14_19-08-2025_052614_7961.xlsx",
            route=routeid,
            survey_year=2025,
            sql_engine=engine,
            lrs=lrs
        )

        check.rni_surf_type_segment_length_check()
        self.assertTrue(check.get_status() == 'verified')
        
    def test_defects_point_check(self):
        """
        Check the consistency between damages in PCI and Defects data.
        """
        routeid = '010362'
        repo = RoutePCIRepo(engine)
        results = ValidationResult(routeid)
        pci = repo.get_by_linkid(
            linkid=routeid,
            year=2024
        )

        check = RoutePCIValidation(
            routeid,
            pci,
            lrs=None,
            sql_engine=engine,
            results=results,
            survey_year=2024
        )

        check.defects_point_check()

        self.assertTrue(check.get_status() == 'error')