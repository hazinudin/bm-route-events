from route_events.route.lrs import LRSRoute
import unittest
import pyarrow as pa
import polars as pl
from route_events.geometry.point import Points
from route_events.geometry import LAMBERT_WKT


class TestLRSFactory(unittest.TestCase):
    def test_lrs_from_feature_service(self):
        """
        Generate LRSRoute object from GRPC feature service.
        """
        lrs = LRSRoute.from_feature_service('localhost:50052', '2813415')  # This one is 8MB in size

        self.assertTrue(type(lrs.gjson) == dict)
        self.assertTrue(type(lrs.artable) == pa.Table)

    def test_lrs_from_feature_service_empty_result(self):
        """
        Test LRSRoute factory method when the response is empty (route does not exists on LRS Network).
        """
        lrs = LRSRoute.from_feature_service('localhost:50052', 'ABCD')  # Invalid route

        self.assertTrue(lrs is None)


class TestLRSMethod(unittest.TestCase):
    def test_lrs_road_properties(self):
        """
        Get road function of a route.
        """
        lrs = LRSRoute.from_feature_service('localhost:50052', '01001')

        self.assertTrue(lrs.function == 'A')
        self.assertTrue(lrs.status == 'N')

    def test_lrs_h3_index(self):
        """
        Test LRS H3 index.
        """
        lrs = LRSRoute.from_feature_service('localhost:50052', '01001')
        row = lrs.dconn.sql("select h3 from lrs_point_table")

        self.assertTrue(row.fetchone()[0] == '8c6553530c24bff')

    def test_lrs_distance_to_point(self):
        """
        Test calculate nearest distance from input point to LRS Geometry
        """
        lrs = LRSRoute.from_feature_service('localhost:50052', '01001')
        dist = lrs.distance_to_point(long=95.42103999972832, lat=5.647860000331377)

        self.assertTrue(0.31114639588873244 == dist)
    
    def test_lrs_distance_to_points(self):
        """
        Test calculate nearest distance from input points to LRS Geometry.
        """
        df = pl.read_parquet('tests/domain/lrs/lambert_15010.parquet')

        # Inverted
        points = Points(
            df, 
            'TO_STA_LAT', 
            'TO_STA_LONG', 
            wkt=LAMBERT_WKT
            )
        
        lrs = LRSRoute.from_feature_service('localhost:50052', '15010')
        dist = lrs.distance_to_points(points)

        self.assertTrue(dist.filter(pl.col('dist') > 1).is_empty())

    def test_get_points_m_value(self):
        """
        Test calculate M-Value from input points.
        """
        df = pl.read_parquet('tests/domain/lrs/lambert_15010.parquet')

        # Inverted
        points = Points(
            df, 
            'TO_STA_LAT', 
            'TO_STA_LONG', 
            wkt=LAMBERT_WKT
            )
        
        lrs = LRSRoute.from_feature_service('localhost:50052', '15010')

        lrs.get_points_m_value(points=points)

    def test_max_m_value(self):
        """
        Test LRS max M-Value property.
        """
        lrs = LRSRoute.from_geojson_file('tests/domain/lrs/lrs_15010.json')

        self.assertTrue(type(lrs.max_m_value) == float)
        self.assertTrue(lrs.max_m_value == 92.30999999999767)
