import unittest
from src.service import Kemantapan
from src.route_events import (
    RouteRNI,
    RouteRNIRepo,
    RouteRoughness,
    RouteRoughnessRepo
)
from dotenv import load_dotenv
import os
from sqlalchemy import create_engine


load_dotenv('tests/dev.env')
HOST = os.getenv('DB_HOST')
USER = os.getenv('SMD_USER')
PWD = os.getenv('SMD_PWD')

engine = create_engine(f"oracle+oracledb://{USER}:{PWD}@{HOST}:1521/geodbbm")

class TestKemantapanRoughness(unittest.TestCase):
    def test_join(self):
        """
        Test data join between RNI and Roughness.
        """
        routeid = '01001'
        iri_repo = RouteRoughnessRepo(engine)
        rni_repo = RouteRNIRepo(engine)

        iri = iri_repo.get_by_linkid(routeid, year=2024, semester=2)
        iri.sta_unit = 'km'  # Change the unit first
        
        rni = rni_repo.get_by_linkid(
            routeid, 
            year=2024, 
            columns=[
            'LINKID', 'FROM_STA', 'TO_STA', 
            'LANE_CODE', 'SURF_TYPE', 'SEGMENT_LENGTH'
        ])

        k = Kemantapan(
            iri=iri,
            rni=rni,
        )

        k.joined

        self.assertTrue(True)

    def test_grading_query_segment(self):
        """
        Test grading query.
        """
        routeid = '01001'
        iri_repo = RouteRoughnessRepo(engine)
        rni_repo = RouteRNIRepo(engine)

        iri = iri_repo.get_by_linkid(routeid, year=2024, semester=2)
        iri.sta_unit = 'km'  # Change the unit first
        
        rni = rni_repo.get_by_linkid(
            routeid, 
            year=2024, 
            columns=[
            'LINKID', 'FROM_STA', 'TO_STA', 'LANE_CODE', 
            'SURF_TYPE', 'SEGMENT_LENGTH'
        ])

        k = Kemantapan(
            iri=iri,
            rni=rni,
        )

        iri_k = k.segment(summary_type='iri_kemantapan')

        self.assertTrue(iri_k['grade'].is_not_null().all())
