import unittest
from route_events.geometry import Point
from route_events.geometry import LAMBERT_WKT


class TestPoint(unittest.TestCase):
    def test_transform(self):
        """
        Transform Point coordinate from EPSG:4326 to Lambert projection.
        """

        pt = Point(long=95.42103999972832, lat=5.647860000331377, wkt='EPSG:4326')
        transformed = pt.transform(LAMBERT_WKT, invert=True)

        self.assertTrue(transformed.Y == -2184157.971000001)
        self.assertTrue(transformed.X == 609253.9258999936)

    def test_create_buffer(self):
        """
        Create buffer around Point at the target distance.
        """
        pt = Point(long=107.79631039982367, lat=-6.2886600492326155, wkt='EPSG:4326')
        transformed = pt.transform(LAMBERT_WKT, invert=True)

        # Create 30m buffer
        gjson = transformed.create_buffer(30)

        self.assertTrue(type(gjson) == str)

    def test_distance_to(self):
        """
        Calculate distance between two Points.
        """
        pt = Point(long=-2184157.971000001, lat=609253.9258999936, wkt=LAMBERT_WKT)
        other_pt = Point(long=pt.X+30, lat=pt.Y, wkt=LAMBERT_WKT)  # Shift 30 meters right

        dist = pt.distance_to(other_pt)

        self.assertTrue(dist==30)
