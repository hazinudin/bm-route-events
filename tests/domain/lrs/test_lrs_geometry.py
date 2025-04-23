from route_events.route.lrs import LRSRoute
import unittest


class TestLRSGeoemtry(unittest.TestCase):
    def test_lrs_distance_to_point(self):
        """
        Test calcuate nearest distance from input point to LRS Geometry
        """
        lrs = LRSRoute.from_feature_service('localhost:50052', '01001')
        dist = lrs.distance_to_point(long=95.42103999972832, lat=5.647860000331377)

        self.assertTrue(0.31114639588873244 == dist)
    
    def test_get_point_m_value(self):
        """
        Test get M value of a point on LRS geometry
        """
        lrs = LRSRoute.from_feature_service('localhost:50052', '01001')
        mval = lrs.get_point_m_value(long=95.42103999972832, lat=5.647860000331377)

        self.assertTrue(10897.934312577387 == mval)