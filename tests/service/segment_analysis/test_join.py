import unittest
from src.service.segments.analysis.join import segments_join, CompareRNISegments, segments_coverage_join
from route_events import RouteSegmentEvents, RouteRNI, RouteRoughness
from src.route_events.segments.pok import RoutePOKRepo, RoutePOK
import polars as pl
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv


load_dotenv('tests/dev.env')
HOST = os.getenv('DB_HOST')
USER = os.getenv('SMD_USER')
PWD = os.getenv('SMD_PWD')

engine = create_engine(f"oracle+oracledb://{USER}:{PWD}@{HOST}:1521/geodbbm")


class TestSegmentsCoverageJoin(unittest.TestCase):
    def test_join_target_agg(self):
        pok_repo = RoutePOKRepo(engine)

        pok = pok_repo.get_by_comp_name(
            linkid = '15010',
            budget_year = 2024,
            comp_name_keywords = ['Pemeliharaan']
        )

        rni = RouteRNI.from_excel(
            'tests/domain/route_segments/input_excels/balai_5_15010.xlsx',
            linkid='15010',
            ignore_review=True
        )

        joined = segments_coverage_join(
            covering=pok,
            target=rni,
            covering_select=['COMP_NAME'],
            target_select=['SURF_TYPE'],
            target_agg = [pl.col('SURF_TYPE').max()]
        )

        self.assertFalse(joined.is_empty())

    def test_join_target_no_agg(self):
        pok_repo = RoutePOKRepo(engine)

        pok = pok_repo.get_by_comp_name(
            linkid = '010362',
            budget_year = 2024,
            comp_name_keywords = ['Pemeliharaan']
        )

        iri = RouteRoughness.from_excel(
            'tests/domain/route_segments/input_excels/iri_1_010362.xlsx',
            linkid='010362',
            ignore_review=True
        )

        joined = segments_coverage_join(
            covering=pok,
            target=iri,
            covering_select=['COMP_NAME'],
            target_select=['IRI']
        )

        self.assertFalse(joined.is_empty())


class TestSegmentsJoin(unittest.TestCase):
    def test_join_agg(self):
        events = RouteSegmentEvents.from_excel(
            'tests/domain/route_segments/input_excels/balai_5_15010.xlsx',
            config_path='tests/domain/route_segments/input_config/rni_config.json',
            linkid='15010',
            ignore_review=True
        )

        joined = segments_join(
            left=events,
            right=events,
            l_select=['SURF_TYPE'],
            r_select=['LANE_WIDTH'],
            l_agg=[pl.col('SURF_TYPE').max()],
            r_agg=[pl.col('LANE_WIDTH').sum()]
        )

        self.assertTrue(joined.shape[0] == 924)

    def test_rni_segments_comparison(self):
        events = RouteRNI.from_excel(
            'tests/domain/route_segments/input_excels/balai_5_15010.xlsx',
            linkid='15010',
            ignore_review=True
        )

        comp = CompareRNISegments(
            rni=events,
            other=events
        )

        self.assertTrue(comp.rni_with_no_match().is_empty())

    def test_rni_segments_comparison_mismatch(self):
        rni = RouteRNI.from_excel(
            'tests/domain/route_segments/input_excels/balai_5_15010.xlsx',
            linkid='15010',
            ignore_review=True
        )

        other = RouteRoughness.from_excel(
            'tests/domain/route_segments/input_excels/iri_1_010362.xlsx',
            linkid='010362',
            ignore_review=True
        )

        comp = CompareRNISegments(
            rni=rni,
            other=other
        )

        self.assertFalse(comp.rni_with_no_match().is_empty())
        self.assertFalse(comp.other_with_no_match().is_empty())
